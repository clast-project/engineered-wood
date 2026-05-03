// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.list</c>: variable-length list of elements with
/// per-row offsets.
///
/// <para>Wire format: 0 buffers, 2-3 children (elements, offsets, optional
/// validity). Metadata <c>ListMetadata { elements_len, offset_ptype }</c>.
/// Offsets has <c>parent_len + 1</c> entries; row i covers
/// <c>elements[offsets[i]..offsets[i+1]]</c>.</para>
///
/// <para>Produces <see cref="ListArray"/> (i32 offsets) when the schema field
/// type is <see cref="ListType"/>, or <see cref="LargeListArray"/> (i64 offsets)
/// when the schema field type is <see cref="LargeListType"/>. The schema choice
/// is controlled by <c>VortexFileReader.OpenAsync(useLargeList: ...)</c>.</para>
/// </summary>
internal static class ListArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not ListType and not LargeListType)
            throw new VortexFormatException(
                $"vortex.list requires ListType or LargeListType, got {expectedType}.");
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.list expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount is not 2 and not 3)
            throw new VortexFormatException(
                $"vortex.list expects 2 or 3 children, got {node.ChildCount}.");

        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException("vortex.list ArrayNode has empty metadata.");
        var (elementsLen, offsetPtype) = ParseListMetadata(metaVec.RawBytes(metaVec.Length));

        var rowCount = checked((int)expectedRowCount);
        var elementType = expectedType is ListType lt
            ? lt.Fields[0].DataType
            : ((LargeListType)expectedType).Fields[0].DataType;
        var elements = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, elementType, checked((long)elementsLen));
        var offsets = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs,
            PtypeIntToArrowType(offsetPtype), expectedRowCount + 1);

        ArrowBuffer nullBuffer; int nullCount;
        if (node.ChildCount == 2)
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }
        else
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(2), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }

        var elementsData = ((Apache.Arrow.Array)elements).Data;

        if (expectedType is ListType)
        {
            // Arrow ListArray: i32 cumulative offsets. Throws if any offset doesn't fit.
            var offsetBytes = new byte[(rowCount + 1) * 4];
            for (int i = 0; i <= rowCount; i++)
                BinaryPrimitives.WriteInt32LittleEndian(
                    offsetBytes.AsSpan(i * 4), checked((int)GetLongAtIndex(offsets, i)));
            var data = new ArrayData(
                expectedType, rowCount, nullCount, offset: 0,
                new[] { nullBuffer, new ArrowBuffer(offsetBytes) },
                new[] { elementsData });
            return new ListArray(data);
        }
        else
        {
            // Arrow LargeListArray: i64 cumulative offsets.
            var offsetBytes = new byte[(rowCount + 1) * 8];
            for (int i = 0; i <= rowCount; i++)
                BinaryPrimitives.WriteInt64LittleEndian(
                    offsetBytes.AsSpan(i * 8), GetLongAtIndex(offsets, i));
            var data = new ArrayData(
                expectedType, rowCount, nullCount, offset: 0,
                new[] { nullBuffer, new ArrowBuffer(offsetBytes) },
                new[] { elementsData });
            return new LargeListArray(data);
        }
    }

    private static (ulong ElementsLen, int OffsetPtype) ParseListMetadata(ReadOnlySpan<byte> bytes)
    {
        ulong elementsLen = 0;
        int offsetPtype = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                elementsLen = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                offsetPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else
            {
                switch (wireType)
                {
                    case 0: Varint.ReadUnsigned(bytes, ref pos); break;
                    case 1: pos += 8; break;
                    case 2:
                        var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                        pos += len; break;
                    case 5: pos += 4; break;
                    default:
                        throw new VortexFormatException(
                            $"Unsupported wire type {wireType} in ListMetadata.");
                }
            }
        }
        return (elementsLen, offsetPtype);
    }

    private static long GetLongAtIndex(IArrowArray array, int i) => array switch
    {
        UInt8Array u8 => u8.GetValue(i)!.Value,
        UInt16Array u16 => u16.GetValue(i)!.Value,
        UInt32Array u32 => u32.GetValue(i)!.Value,
        UInt64Array u64 => unchecked((long)u64.GetValue(i)!.Value),
        Int8Array i8 => i8.GetValue(i)!.Value,
        Int16Array i16 => i16.GetValue(i)!.Value,
        Int32Array i32 => i32.GetValue(i)!.Value,
        Int64Array i64 => i64.GetValue(i)!.Value,
        _ => throw new VortexFormatException(
            $"vortex.list offsets type {array.GetType().Name} not supported."),
    };

    private static IArrowType PtypeIntToArrowType(int ptype) => ptype switch
    {
        0 => UInt8Type.Default,
        1 => UInt16Type.Default,
        2 => UInt32Type.Default,
        3 => UInt64Type.Default,
        4 => Int8Type.Default,
        5 => Int16Type.Default,
        6 => Int32Type.Default,
        7 => Int64Type.Default,
        _ => throw new VortexFormatException(
            $"Unsupported ptype {ptype} in ListMetadata."),
    };
}
