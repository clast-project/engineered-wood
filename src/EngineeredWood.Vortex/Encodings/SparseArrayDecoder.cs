// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.sparse</c>: most rows take a single <c>fill_value</c>;
/// non-default rows are stored as patches (indices + values).
///
/// <para>Wire format: 1 buffer (fill_value as ScalarValue proto), 2-3 children
/// (indices, values, optional chunk_offsets). Metadata
/// <c>SparseMetadata { patches: PatchesMetadata }</c> (required, includes
/// indices_ptype, len, offset).</para>
///
/// <para>Phase 1 scope: integer/float fill values only.</para>
/// </summary>
internal static class SparseArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.BufferRefCount != 1)
            throw new VortexFormatException(
                $"vortex.sparse expects 1 buffer (fill_value), got {node.BufferRefCount}.");
        if (node.ChildCount is < 2 or > 3)
            throw new VortexFormatException(
                $"vortex.sparse expects 2-3 children, got {node.ChildCount}.");

        var bufferRef = node.BufferRef(0);
        var bufferDesc = serialized.Message.Buffer(bufferRef);
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.sparse buffer compression {bufferDesc.Compression} not yet implemented.");
        var fillValue = ScalarValueProto.Parse(serialized.BufferBytes(bufferRef));

        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException("vortex.sparse ArrayNode has empty metadata.");
        var (patchesLen, patchesOffset, indicesPtype) =
            ParseSparseMetadata(metaVec.RawBytes(metaVec.Length));

        var indicesType = PtypeIntToArrowType(indicesPtype);
        var indices = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, indicesType, (long)patchesLen);
        var values = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs, expectedType, (long)patchesLen);

        var rowCount = checked((int)expectedRowCount);
        return Materialize(expectedType, rowCount, fillValue, indices, values, (int)patchesOffset);
    }

    private static IArrowArray Materialize(
        IArrowType type, int rowCount, ScalarValueProto fill,
        IArrowArray indices, IArrowArray values, int patchesOffset)
    {
        return (type, fill.Kind, values) switch
        {
            (Int8Type, ScalarValueKind.Int64, Int8Array v) => Build<sbyte>(rowCount,
                (sbyte)fill.Int64Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new Int8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (Int16Type, ScalarValueKind.Int64, Int16Array v) => Build<short>(rowCount,
                (short)fill.Int64Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new Int16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (Int32Type, ScalarValueKind.Int64, Int32Array v) => Build<int>(rowCount,
                (int)fill.Int64Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new Int32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (Int64Type, ScalarValueKind.Int64, Int64Array v) => Build<long>(rowCount,
                fill.Int64Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new Int64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt8Type, ScalarValueKind.UInt64, UInt8Array v) => Build<byte>(rowCount,
                (byte)fill.UInt64Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new UInt8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt16Type, ScalarValueKind.UInt64, UInt16Array v) => Build<ushort>(rowCount,
                (ushort)fill.UInt64Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new UInt16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt32Type, ScalarValueKind.UInt64, UInt32Array v) => Build<uint>(rowCount,
                (uint)fill.UInt64Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new UInt32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt64Type, ScalarValueKind.UInt64, UInt64Array v) => Build<ulong>(rowCount,
                fill.UInt64Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new UInt64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (FloatType, ScalarValueKind.F32, FloatArray v) => Build<float>(rowCount,
                fill.F32Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new FloatArray(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (DoubleType, ScalarValueKind.F64, DoubleArray v) => Build<double>(rowCount,
                fill.F64Value, indices, i => v.GetValue(i)!.Value, patchesOffset,
                (data, len) => new DoubleArray(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            _ => throw new NotSupportedException(
                $"vortex.sparse: unsupported (type={type}, fill={fill.Kind}, values={values.GetType().Name})."),
        };
    }

    private static IArrowArray Build<T>(
        int rowCount, T fill, IArrowArray indices, Func<int, T> getValue,
        int patchesOffset, Func<byte[], int, IArrowArray> ctor)
        where T : unmanaged
    {
        var bytes = new byte[(long)rowCount * Marshal.SizeOf<T>()];
        var span = MemoryMarshal.Cast<byte, T>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++) span[i] = fill;

        for (int k = 0; k < indices.Length; k++)
        {
            var rowIdx = GetIntAtIndex(indices, k) - patchesOffset;
            if ((uint)rowIdx >= (uint)rowCount)
                throw new VortexFormatException(
                    $"vortex.sparse: patch index {rowIdx} out of range [0, {rowCount}).");
            span[rowIdx] = getValue(k);
        }

        return ctor(bytes, rowCount);
    }

    private static (ulong Len, ulong Offset, int IndicesPtype) ParseSparseMetadata(ReadOnlySpan<byte> bytes)
    {
        // SparseMetadata { patches: PatchesMetadata at field 1 }
        // PatchesMetadata { len@1, offset@2, indices_ptype@3, ... }
        ulong len = 0, offset = 0;
        int indicesPtype = 2;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 2)
            {
                var l = (int)Varint.ReadUnsigned(bytes, ref pos);
                ParsePatches(bytes.Slice(pos, l), out len, out offset, out indicesPtype);
                pos += l;
            }
            else
            {
                switch (wireType)
                {
                    case 0: Varint.ReadUnsigned(bytes, ref pos); break;
                    case 1: pos += 8; break;
                    case 2:
                        var fl = (int)Varint.ReadUnsigned(bytes, ref pos);
                        pos += fl; break;
                    case 5: pos += 4; break;
                    default:
                        throw new VortexFormatException(
                            $"Unsupported wire type {wireType} in SparseMetadata.");
                }
            }
        }
        return (len, offset, indicesPtype);
    }

    private static void ParsePatches(
        ReadOnlySpan<byte> bytes, out ulong len, out ulong offset, out int indicesPtype)
    {
        len = 0; offset = 0; indicesPtype = 2;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0) len = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0) offset = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 0) indicesPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else
            {
                switch (wireType)
                {
                    case 0: Varint.ReadUnsigned(bytes, ref pos); break;
                    case 1: pos += 8; break;
                    case 2:
                        var l = (int)Varint.ReadUnsigned(bytes, ref pos);
                        pos += l; break;
                    case 5: pos += 4; break;
                }
            }
        }
    }

    private static int GetIntAtIndex(IArrowArray array, int i) => array switch
    {
        UInt8Array u8 => u8.GetValue(i)!.Value,
        UInt16Array u16 => u16.GetValue(i)!.Value,
        UInt32Array u32 => checked((int)u32.GetValue(i)!.Value),
        UInt64Array u64 => checked((int)u64.GetValue(i)!.Value),
        Int8Array i8 => i8.GetValue(i)!.Value,
        Int16Array i16 => i16.GetValue(i)!.Value,
        Int32Array i32 => i32.GetValue(i)!.Value,
        Int64Array i64 => checked((int)i64.GetValue(i)!.Value),
        _ => throw new VortexFormatException(
            $"vortex.sparse indices type {array.GetType().Name} not supported."),
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
        _ => throw new VortexFormatException($"Unsupported ptype {ptype} in SparseMetadata."),
    };
}
