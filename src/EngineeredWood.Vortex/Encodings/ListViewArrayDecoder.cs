// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.listview</c>: variable-length list with separate
/// per-row offsets and sizes (no contiguity invariant — row i covers
/// <c>elements[offsets[i] .. offsets[i] + sizes[i]]</c>).
///
/// <para>Wire format: 0 buffers, 3-4 children (elements, offsets [len=N],
/// sizes [len=N], optional validity). Metadata <c>ListViewMetadata { elements_len,
/// offset_ptype, size_ptype }</c>.</para>
///
/// <para>Apache.Arrow .NET 22.1 doesn't have ListViewArray, so we materialize
/// to a regular <see cref="ListArray"/> (i32 offsets) or <see cref="LargeListArray"/>
/// (i64 offsets) depending on the schema field type. Output type matches
/// <c>expectedType</c>: <see cref="ListType"/> → ListArray,
/// <see cref="LargeListType"/> → LargeListArray. When offsets are already
/// contiguous (the common case from the writer's serialize path) we keep the
/// elements buffer; otherwise we re-pack into a fresh contiguous elements array.</para>
/// </summary>
internal static class ListViewArrayDecoder
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
                $"vortex.listview requires ListType or LargeListType, got {expectedType}.");
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.listview expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount is not 3 and not 4)
            throw new VortexFormatException(
                $"vortex.listview expects 3 or 4 children, got {node.ChildCount}.");

        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException("vortex.listview ArrayNode has empty metadata.");
        var (elementsLen, offsetPtype, sizePtype) = ParseMetadata(metaVec.RawBytes(metaVec.Length));

        var rowCount = checked((int)expectedRowCount);
        var elementType = expectedType is ListType lt
            ? lt.Fields[0].DataType
            : ((LargeListType)expectedType).Fields[0].DataType;
        var elements = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, elementType, checked((long)elementsLen));
        var offsets = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs, PtypeIntToArrowType(offsetPtype), expectedRowCount);
        var sizes = ArrayDecoder.DecodeNode(
            node.Child(2), serialized, arraySpecs, PtypeIntToArrowType(sizePtype), expectedRowCount);

        ArrowBuffer nullBuffer; int nullCount;
        if (node.ChildCount == 3)
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }
        else
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(3), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }

        bool useI64 = expectedType is LargeListType;
        int offsetSize = useI64 ? 8 : 4;
        var arrowOffsetBytes = new byte[(rowCount + 1) * offsetSize];
        long total = 0;
        for (int i = 0; i < rowCount; i++)
        {
            WriteOffset(arrowOffsetBytes.AsSpan(i * offsetSize, offsetSize), total, useI64);
            total += GetLongAtIndex(sizes, i);
        }
        WriteOffset(arrowOffsetBytes.AsSpan(rowCount * offsetSize, offsetSize), total, useI64);

        // Detect the contiguous case: each row's start equals the previous row's end.
        bool contiguous = true;
        long expectedStart = 0;
        for (int i = 0; i < rowCount && contiguous; i++)
        {
            if (GetLongAtIndex(offsets, i) != expectedStart) contiguous = false;
            expectedStart += GetLongAtIndex(sizes, i);
        }

        var elementsData = ((Apache.Arrow.Array)elements).Data;
        if (!contiguous)
        {
            // Materialize: copy each row's slice into a fresh contiguous element array.
            elementsData = MaterializeContiguous(elements, offsets, sizes, rowCount, total);
        }

        var data = new ArrayData(
            expectedType, rowCount, nullCount, offset: 0,
            new[] { nullBuffer, new ArrowBuffer(arrowOffsetBytes) },
            new[] { elementsData });
        return useI64 ? new LargeListArray(data) : new ListArray(data);
    }

    private static void WriteOffset(Span<byte> dst, long value, bool useI64)
    {
        if (useI64)
            BinaryPrimitives.WriteInt64LittleEndian(dst, value);
        else
            BinaryPrimitives.WriteInt32LittleEndian(dst, checked((int)value));
    }

    /// <summary>
    /// Re-packs the elements into a contiguous Arrow array: one gather of every row's
    /// view, in row order, so it serves any element type <see cref="ArrowCompute"/>
    /// can take from and carries the elements' nulls with them.
    /// </summary>
    private static ArrayData MaterializeContiguous(
        IArrowArray elements, IArrowArray offsets, IArrowArray sizes,
        int rowCount, long totalElements)
    {
        var indices = new int[checked((int)totalElements)];
        int pos = 0;
        for (int i = 0; i < rowCount; i++)
        {
            int start = checked((int)GetLongAtIndex(offsets, i));
            int size = checked((int)GetLongAtIndex(sizes, i));
            for (int j = 0; j < size; j++)
                indices[pos++] = start + j;
        }
        return ArrowCompute.Take(elements, indices).Data;
    }

    private static (ulong ElementsLen, int OffsetPtype, int SizePtype) ParseMetadata(ReadOnlySpan<byte> bytes)
    {
        ulong elementsLen = 0;
        int offsetPtype = 0, sizePtype = 0;
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
            else if (fieldNum == 3 && wireType == 0)
                sizePtype = (int)Varint.ReadUnsigned(bytes, ref pos);
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
                            $"Unsupported wire type {wireType} in ListViewMetadata.");
                }
            }
        }
        return (elementsLen, offsetPtype, sizePtype);
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
            $"vortex.listview offsets/sizes type {array.GetType().Name} not supported."),
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
            $"Unsupported ptype {ptype} in ListViewMetadata."),
    };
}
