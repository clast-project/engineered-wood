// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.varbin</c>: classic Arrow-style variable-length binary
/// (offsets + concatenated bytes).
///
/// <para>Wire format: 1 buffer (the bytes), 1-2 children (offsets primitive +
/// optional validity bool), metadata <c>VarBinMetadata { offsets_ptype }</c>.
/// row i = <c>bytes[offsets[i] .. offsets[i+1]]</c> (offsets has N+1 entries).</para>
/// </summary>
internal static class VarBinArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not StringType and not BinaryType)
            throw new NotSupportedException(
                $"vortex.varbin only supports StringType / BinaryType, got {expectedType}.");
        if (node.BufferRefCount != 1)
            throw new VortexFormatException(
                $"vortex.varbin expects 1 buffer, got {node.BufferRefCount}.");
        if (node.ChildCount is not 1 and not 2)
            throw new VortexFormatException(
                $"vortex.varbin expects 1 or 2 children (offsets, optional validity), got {node.ChildCount}.");

        var bufferRef = node.BufferRef(0);
        var bufferDesc = serialized.Message.Buffer(bufferRef);
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.varbin buffer compression {bufferDesc.Compression} not yet implemented.");
        var bytes = serialized.BufferBytes(bufferRef);

        var metaVec = node.Metadata;
        var offsetsPtype = ParseVarBinMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));
        var offsetsType = PtypeIntToArrowType(offsetsPtype);
        var rowCount = checked((int)expectedRowCount);
        var offsets = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, offsetsType, expectedRowCount + 1);

        ArrowBuffer nullBuffer; int nullCount;
        if (node.ChildCount == 1)
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }
        else
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(1), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }

        // Convert offsets to Arrow's i32 form. Arrow's StringArray/BinaryArray
        // require i32 offsets; if the source is u64 we'd need LargeStringArray
        // (deferred — punt for now).
        var i32Offsets = OffsetsToInt32(offsets, rowCount + 1);
        var offsetBytes = new byte[i32Offsets.Length * 4];
        for (int i = 0; i < i32Offsets.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4), i32Offsets[i]);

        // Slice the value buffer to the actual end. Vortex may include extra
        // padding/trailing bytes beyond offsets[n]; we copy exactly what's used.
        var totalLen = i32Offsets[rowCount];
        var valueBytes = new byte[totalLen];
        bytes.Slice(0, totalLen).CopyTo(valueBytes);

        return expectedType is StringType
            ? new StringArray(rowCount, new ArrowBuffer(offsetBytes), new ArrowBuffer(valueBytes), nullBuffer, nullCount, 0)
            : new BinaryArray(BinaryType.Default, rowCount, new ArrowBuffer(offsetBytes), new ArrowBuffer(valueBytes), nullBuffer, nullCount, 0);
    }

    private static int[] OffsetsToInt32(IArrowArray offsets, int count) => offsets switch
    {
        Int32Array i32 => CopyInt32(i32, count),
        UInt32Array u32 => CopyUInt32(u32, count),
        Int64Array i64 => CopyInt64(i64, count),
        UInt64Array u64 => CopyUInt64(u64, count),
        _ => throw new NotSupportedException(
            $"vortex.varbin offsets type {offsets.GetType().Name} not supported."),
    };

    private static int[] CopyInt32(Int32Array a, int count)
    {
        var r = new int[count];
        for (int i = 0; i < count; i++) r[i] = a.GetValue(i)!.Value;
        return r;
    }

    private static int[] CopyUInt32(UInt32Array a, int count)
    {
        var r = new int[count];
        for (int i = 0; i < count; i++) r[i] = checked((int)a.GetValue(i)!.Value);
        return r;
    }

    private static int[] CopyInt64(Int64Array a, int count)
    {
        var r = new int[count];
        for (int i = 0; i < count; i++) r[i] = checked((int)a.GetValue(i)!.Value);
        return r;
    }

    private static int[] CopyUInt64(UInt64Array a, int count)
    {
        var r = new int[count];
        for (int i = 0; i < count; i++) r[i] = checked((int)a.GetValue(i)!.Value);
        return r;
    }

    private static int ParseVarBinMetadata(ReadOnlySpan<byte> bytes)
    {
        // VarBinMetadata { offsets_ptype: PType enum at field 1 }
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                return (int)Varint.ReadUnsigned(bytes, ref pos);
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
                        $"Unsupported wire type {wireType} in VarBinMetadata.");
            }
        }
        return 0; // proto3 default = U8
    }

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
        _ => throw new VortexFormatException($"Unsupported ptype {ptype} in VarBinMetadata."),
    };
}
