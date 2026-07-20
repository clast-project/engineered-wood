// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>fastlanes.rle</c>: per-chunk run-length-encoded primitive
/// arrays. Despite the name, this encoding has no FastLanes UTL transposition —
/// it's plain dictionary-and-indices RLE applied to fixed 1024-element chunks.
///
/// <para>Per upstream <c>encodings/fastlanes/src/rle/array</c>:
/// <list type="bullet">
///   <item>0 buffers</item>
///   <item>3 children (in order):
///     <list type="number">
///       <item><c>values</c> — unique run values from all chunks (concatenated)</item>
///       <item><c>indices</c> — chunk-local indices, length = num_chunks × 1024;
///         u8 or u16</item>
///       <item><c>values_idx_offsets</c> — absolute starts into <c>values</c>
///         for each chunk; length = num_chunks; unsigned int (typically u64)</item>
///     </list>
///   </item>
///   <item>Metadata <c>RLEMetadata { values_len, indices_len, indices_ptype,
///     values_idx_offsets_len, values_idx_offsets_ptype, offset = 0 }</c></item>
/// </list></para>
///
/// <para>Decode (per chunk c):
///   <c>chunk_values = values[values_idx_offsets[c] − values_idx_offsets[0]..]</c>;
///   <c>output[c·1024 + i] = chunk_values[indices[c·1024 + i]]</c>.
/// Slicing drops the first <c>offset</c> rows from chunk 0 (offset must be &lt; 1024).</para>
///
/// <para>Scope: non-nullable values and indices; values child decodes to any of
/// the supported primitive types. Slicing offset (offset != 0) is supported.</para>
/// </summary>
internal static class RleArrayDecoder
{
    private const int ElementsPerChunk = 1024;

    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"fastlanes.rle expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount != 3)
            throw new VortexFormatException(
                $"fastlanes.rle expects 3 children (values, indices, values_idx_offsets), got {node.ChildCount}.");

        var metaVec = node.Metadata;
        var meta = ParseMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));
        if (meta.Offset >= (ulong)ElementsPerChunk)
            throw new VortexFormatException(
                $"fastlanes.rle offset {meta.Offset} must be < 1024 (sub-chunk slice).");

        var rowCount = checked((int)expectedRowCount);
        int offset = (int)meta.Offset;

        var values = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, expectedType, checked((long)meta.ValuesLen));

        var indicesType = PtypeIntToArrowType(meta.IndicesPtype);
        var indices = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs, indicesType, checked((long)meta.IndicesLen));

        var offsetsType = PtypeIntToArrowType(meta.ValuesIdxOffsetsPtype);
        var valuesIdxOffsets = ArrayDecoder.DecodeNode(
            node.Child(2), serialized, arraySpecs, offsetsType,
            checked((long)meta.ValuesIdxOffsetsLen));

        return Gather(expectedType, rowCount, offset, meta.IndicesPtype,
            values, indices, valuesIdxOffsets);
    }

    private static IArrowArray Gather(
        IArrowType type, int rowCount, int offset, int indicesPtype,
        IArrowArray values, IArrowArray indices, IArrowArray valuesIdxOffsets)
    {
        return type switch
        {
            UInt8Type => GatherTo<byte>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new UInt8Array(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            Int8Type => GatherTo<sbyte>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new Int8Array(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            UInt16Type => GatherTo<ushort>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new UInt16Array(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            Int16Type => GatherTo<short>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new Int16Array(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            UInt32Type => GatherTo<uint>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new UInt32Array(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            Int32Type => GatherTo<int>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new Int32Array(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            UInt64Type => GatherTo<ulong>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new UInt64Array(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            Int64Type => GatherTo<long>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new Int64Array(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            FloatType => GatherTo<float>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new FloatArray(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            DoubleType => GatherTo<double>(rowCount, offset, indicesPtype, values, indices, valuesIdxOffsets,
                (data, nullBuf, nullCount, len) => new DoubleArray(new ArrowBuffer(data), nullBuf, len, nullCount, 0)),
            _ => throw new NotSupportedException(
                $"fastlanes.rle currently supports primitive value types only, got {type}."),
        };
    }

    private static IArrowArray GatherTo<TValue>(
        int rowCount, int offset, int indicesPtype,
        IArrowArray values, IArrowArray indices, IArrowArray valuesIdxOffsets,
        Func<byte[], ArrowBuffer, int, int, IArrowArray> ctor)
        where TValue : unmanaged
    {
        var elementSize = Marshal.SizeOf<TValue>();
        var dst = new byte[(long)rowCount * elementSize];
        var dstSpan = MemoryMarshal.Cast<byte, TValue>(dst.AsSpan());

        var valuesData = ((Apache.Arrow.Array)values).Data;
        var valuesSpan = MemoryMarshal.Cast<byte, TValue>(valuesData.Buffers[1].Span);

        // First chunk's absolute offset — used to make subsequent chunk offsets
        // relative when the array has been sliced.
        long firstChunkBase = ReadOffsetAtIndex(valuesIdxOffsets, 0);

        // Indices' raw value buffer (u8 or u16) and validity bitmap. Per
        // upstream rle/vtable/validity.rs, an RLE array's validity = its
        // INDICES child's validity sliced by `offset`. Read raw bytes so null
        // positions don't trip GetValue() — they may contain garbage indices
        // that would OOB into `values`.
        var indicesData = ((Apache.Arrow.Array)indices).Data;
        var indicesBitmap = indicesData.Buffers[0].Span;     // empty if non-nullable
        var indicesValueBytes = indicesData.Buffers[1].Span;
        bool indicesAreU8 = indicesPtype == 0;
        int indexBytes = indicesAreU8 ? 1 : 2;

        // Slicing: indices buffer covers numChunks * 1024 rows; we only emit
        // logical positions [offset, offset + rowCount). For each output row p,
        // global_pos = offset + p, chunk_id = global_pos / 1024, position
        // within chunk = global_pos % 1024.
        int sliceEnd = offset + rowCount;
        int numChunks = (sliceEnd + ElementsPerChunk - 1) / ElementsPerChunk;
        for (int c = 0; c < numChunks; c++)
        {
            int chunkBase = checked((int)(ReadOffsetAtIndex(valuesIdxOffsets, c) - firstChunkBase));
            int chunkStart = c * ElementsPerChunk;
            int chunkEnd = chunkStart + ElementsPerChunk;
            int overlapStart = Math.Max(chunkStart, offset);
            int overlapEnd = Math.Min(chunkEnd, sliceEnd);
            if (overlapEnd <= overlapStart) continue;

            for (int g = overlapStart; g < overlapEnd; g++)
            {
                if (!indicesBitmap.IsEmpty && !GetBit(indicesBitmap, g))
                    continue; // null row — leave zero in output; bitmap masks it.
                int localIdx = indicesAreU8
                    ? indicesValueBytes[g]
                    : System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(
                        indicesValueBytes.Slice(g * 2, 2));
                dstSpan[g - offset] = valuesSpan[chunkBase + localIdx];
            }
        }

        // Build the output null bitmap by copying indices' validity bits
        // [offset, offset + rowCount) into a fresh byte-aligned bitmap.
        ArrowBuffer nullBuffer = ArrowBuffer.Empty;
        int nullCount = 0;
        if (!indicesBitmap.IsEmpty)
        {
            int byteLen = (rowCount + 7) / 8;
            var outBitmap = new byte[byteLen];
            for (int p = 0; p < rowCount; p++)
            {
                if (GetBit(indicesBitmap, offset + p))
                    outBitmap[p >> 3] |= (byte)(1 << (p & 7));
                else
                    nullCount++;
            }
            nullBuffer = new ArrowBuffer(outBitmap);
        }

        return ctor(dst, nullBuffer, nullCount, rowCount);
    }

    private static bool GetBit(ReadOnlySpan<byte> bitmap, int bitIndex) =>
        (bitmap[bitIndex >> 3] & (1 << (bitIndex & 7))) != 0;

    private static long ReadOffsetAtIndex(IArrowArray array, int i) => array switch
    {
        UInt8Array u8 => u8.GetValue(i)!.Value,
        UInt16Array u16 => u16.GetValue(i)!.Value,
        UInt32Array u32 => u32.GetValue(i)!.Value,
        UInt64Array u64 => unchecked((long)u64.GetValue(i)!.Value),
        _ => throw new VortexFormatException(
            $"fastlanes.rle: values_idx_offsets type {array.GetType().Name} not supported."),
    };

    private static int ReadIndexAt(IArrowArray array, int i) => array switch
    {
        UInt8Array u8 => u8.GetValue(i)!.Value,
        UInt16Array u16 => u16.GetValue(i)!.Value,
        _ => throw new VortexFormatException(
            $"fastlanes.rle: indices type {array.GetType().Name} not supported (expected u8 or u16)."),
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
        _ => throw new VortexFormatException($"Unsupported ptype {ptype} in RLEMetadata."),
    };

    private readonly struct RleMeta
    {
        public ulong ValuesLen { get; init; }
        public ulong IndicesLen { get; init; }
        public int IndicesPtype { get; init; }
        public ulong ValuesIdxOffsetsLen { get; init; }
        public int ValuesIdxOffsetsPtype { get; init; }
        public ulong Offset { get; init; }
    }

    /// <summary>
    /// Parses <c>RLEMetadata</c>:
    ///   field 1 (varint): values_len (u64)
    ///   field 2 (varint): indices_len (u64)
    ///   field 3 (varint): indices_ptype (PType enum)
    ///   field 4 (varint): values_idx_offsets_len (u64)
    ///   field 5 (varint): values_idx_offsets_ptype (PType enum)
    ///   field 6 (varint, optional): offset (u64, default 0)
    /// </summary>
    private static RleMeta ParseMetadata(ReadOnlySpan<byte> bytes)
    {
        ulong valuesLen = 0, indicesLen = 0, valuesIdxOffsetsLen = 0, offset = 0;
        // proto3 omits enum fields with the default value (= 0 = U8), so default
        // both ptype fields to U8 here.
        int indicesPtype = 0, valuesIdxOffsetsPtype = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                valuesLen = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                indicesLen = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 0)
                indicesPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 4 && wireType == 0)
                valuesIdxOffsetsLen = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 5 && wireType == 0)
                valuesIdxOffsetsPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 6 && wireType == 0)
                offset = (ulong)Varint.ReadUnsigned(bytes, ref pos);
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
                            $"Unsupported wire type {wireType} in RLEMetadata.");
                }
            }
        }
        return new RleMeta
        {
            ValuesLen = valuesLen,
            IndicesLen = indicesLen,
            IndicesPtype = indicesPtype,
            ValuesIdxOffsetsLen = valuesIdxOffsetsLen,
            ValuesIdxOffsetsPtype = valuesIdxOffsetsPtype,
            Offset = offset,
        };
    }
}
