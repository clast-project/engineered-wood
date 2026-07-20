// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>fastlanes.bitpacked</c>: integer arrays bit-packed using
/// the FastLanes layout (1024 elements per chunk, transposed for SIMD-friendly
/// unpack).
///
/// <para>Wire format (per <c>encodings/fastlanes/src/bitpacking/vtable/mod.rs</c>):
/// <list type="bullet">
///   <item>1 buffer: <c>packed</c> — the bit-packed bytes, sized as
///     <c>num_chunks × 1024 × bit_width / 8</c>.</item>
///   <item>0+ children: optional patches (indices + values, when some values
///     don't fit in <c>bit_width</c>), optional validity (last child).</item>
///   <item>Metadata proto <c>BitPackedMetadata { bit_width, offset, patches }</c>.</item>
/// </list></para>
///
/// <para>Phase 1 scope: non-nullable, non-patched, offset=0 arrays of integer
/// types. Patches and slicing offsets land alongside fixtures that exercise them.</para>
/// </summary>
internal static class BitPackedArrayDecoder
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
                $"fastlanes.bitpacked expects 1 buffer, got {node.BufferRefCount}.");

        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException("fastlanes.bitpacked ArrayNode has empty metadata.");
        var meta = ParseBitPackedMetadata(metaVec.RawBytes(metaVec.Length));
        if (meta.Offset >= 1024)
            throw new VortexFormatException(
                $"fastlanes.bitpacked offset {meta.Offset} must be < 1024 (sub-chunk slice).");

        // Child layout (per upstream's validity_idx logic):
        //   no patches:                            validity at child[0] (or none)
        //   patches without chunk_offsets:         children = [indices, values, optional validity]
        //   patches with chunk_offsets:            children = [indices, values, chunk_offsets, optional validity]
        int patchChildCount;
        if (!meta.HasPatches) patchChildCount = 0;
        else if (meta.PatchHasChunkOffsets) patchChildCount = 3;
        else patchChildCount = 2;

        var rowCount = checked((int)expectedRowCount);

        // Optional trailing validity child.
        ArrowBuffer nullBuffer;
        int nullCount;
        if (node.ChildCount == patchChildCount)
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }
        else if (node.ChildCount == patchChildCount + 1)
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(patchChildCount), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }
        else
        {
            throw new VortexFormatException(
                $"fastlanes.bitpacked: expected {patchChildCount} or {patchChildCount + 1} children, got {node.ChildCount}.");
        }

        var bufferRef = node.BufferRef(0);
        var bufferDesc = serialized.Message.Buffer(bufferRef);
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"fastlanes.bitpacked buffer compression {bufferDesc.Compression} not yet implemented.");
        var packed = serialized.BufferBytes(bufferRef);

        // Unpack into a raw byte buffer, optionally apply patches, then wrap.
        var (rawBytes, elementSize) = UnpackBytes(
            expectedType, rowCount, (int)meta.Offset, (int)meta.BitWidth, packed);

        if (meta.HasPatches)
        {
            var patchIndicesType = PtypeIntToArrowType(meta.PatchIndicesPtype);
            var patchIndices = ArrayDecoder.DecodeNode(
                node.Child(0), serialized, arraySpecs, patchIndicesType, (long)meta.PatchesLen);
            var patchValues = ArrayDecoder.DecodeNode(
                node.Child(1), serialized, arraySpecs, expectedType, (long)meta.PatchesLen);
            ApplyPatchesInPlace(expectedType, rawBytes, elementSize,
                patchIndices, patchValues, (int)meta.PatchesOffset);
        }

        return Wrap(expectedType, rawBytes, rowCount, nullBuffer, nullCount);
    }

    private static IArrowArray Wrap(
        IArrowType type, byte[] data, int rowCount, ArrowBuffer nullBuffer, int nullCount)
    {
        var valueBuf = new ArrowBuffer(data);
        return type switch
        {
            Int8Type => new Int8Array(valueBuf, nullBuffer, rowCount, nullCount, 0),
            UInt8Type => new UInt8Array(valueBuf, nullBuffer, rowCount, nullCount, 0),
            Int16Type => new Int16Array(valueBuf, nullBuffer, rowCount, nullCount, 0),
            UInt16Type => new UInt16Array(valueBuf, nullBuffer, rowCount, nullCount, 0),
            Int32Type => new Int32Array(valueBuf, nullBuffer, rowCount, nullCount, 0),
            UInt32Type => new UInt32Array(valueBuf, nullBuffer, rowCount, nullCount, 0),
            Int64Type => new Int64Array(valueBuf, nullBuffer, rowCount, nullCount, 0),
            UInt64Type => new UInt64Array(valueBuf, nullBuffer, rowCount, nullCount, 0),
            _ => throw new NotSupportedException(
                $"fastlanes.bitpacked: cannot wrap Arrow type {type}."),
        };
    }

    /// <summary>
    /// Mutates <paramref name="rawBytes"/> in place: for each k,
    /// <c>output[indices[k] - patchesOffset] = values[k]</c>. Patch values
    /// always go through <see cref="GetLongAtIndex"/> (not <see cref="GetIntAtIndex"/>)
    /// because a u32 column's patch values can hold the full unsigned range,
    /// and the bitpattern would overflow a checked-int cast for values ≥ 2^31.
    /// Indices stay with <see cref="GetIntAtIndex"/> — they're bounded by rowCount.
    /// </summary>
    private static void ApplyPatchesInPlace(
        IArrowType type, byte[] rawBytes, int elementSize,
        IArrowArray indices, IArrowArray values, int patchesOffset)
    {
        var patchCount = indices.Length;
        switch (elementSize)
        {
            case 1:
                for (int k = 0; k < patchCount; k++)
                {
                    var rowIdx = GetIntAtIndex(indices, k) - patchesOffset;
                    rawBytes[rowIdx] = (byte)GetLongAtIndex(values, k);
                }
                break;
            case 2:
                {
                    var s = MemoryMarshal.Cast<byte, ushort>(rawBytes.AsSpan());
                    for (int k = 0; k < patchCount; k++)
                    {
                        var rowIdx = GetIntAtIndex(indices, k) - patchesOffset;
                        s[rowIdx] = (ushort)GetLongAtIndex(values, k);
                    }
                    break;
                }
            case 4:
                {
                    var s = MemoryMarshal.Cast<byte, uint>(rawBytes.AsSpan());
                    for (int k = 0; k < patchCount; k++)
                    {
                        var rowIdx = GetIntAtIndex(indices, k) - patchesOffset;
                        s[rowIdx] = (uint)GetLongAtIndex(values, k);
                    }
                    break;
                }
            case 8:
                {
                    var s = MemoryMarshal.Cast<byte, ulong>(rawBytes.AsSpan());
                    for (int k = 0; k < patchCount; k++)
                    {
                        var rowIdx = GetIntAtIndex(indices, k) - patchesOffset;
                        s[rowIdx] = (ulong)GetLongAtIndex(values, k);
                    }
                    break;
                }
            default:
                throw new NotSupportedException(
                    $"fastlanes.bitpacked: unsupported element size {elementSize} for patch application.");
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
            $"fastlanes.bitpacked patch indices/values type {array.GetType().Name} not supported."),
    };

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
            $"fastlanes.bitpacked: long patch values type {array.GetType().Name} not supported."),
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
        _ => throw new VortexFormatException($"Unsupported ptype {ptype}."),
    };

    private static (byte[] Bytes, int ElementSize) UnpackBytes(
        IArrowType type, int rowCount, int offset, int bitWidth, ReadOnlySpan<byte> packed)
    {
        return type switch
        {
            Int8Type or UInt8Type => (UnpackTo<byte>(rowCount, offset, bitWidth, packed), 1),
            Int16Type or UInt16Type => (UnpackTo<ushort>(rowCount, offset, bitWidth, packed), 2),
            Int32Type or UInt32Type => (UnpackTo<uint>(rowCount, offset, bitWidth, packed), 4),
            Int64Type or UInt64Type => (UnpackTo<ulong>(rowCount, offset, bitWidth, packed), 8),
            _ => throw new NotSupportedException(
                $"fastlanes.bitpacked decoder doesn't support Arrow type {type}."),
        };
    }

    /// <summary>
    /// Unpacks the bit-packed buffer covering <paramref name="offset"/> +
    /// <paramref name="rowCount"/> logical rows, then copies the slice
    /// <c>[offset, offset + rowCount)</c> into the output. The packed buffer
    /// always covers <c>numChunks × 1024</c> rows where <c>numChunks =
    /// ceil((offset + rowCount) / 1024)</c>.
    /// </summary>
    private static byte[] UnpackTo<T>(
        int rowCount, int offset, int bitWidth, ReadOnlySpan<byte> packed)
        where T : unmanaged
    {
        const int ElementsPerChunk = 1024;
        var elementSize = Marshal.SizeOf<T>();
        var bytes = new byte[(long)rowCount * elementSize];

        if (bitWidth == 0)
        {
            // bit_width=0 means all values are 0 (FastLanes special case).
            return bytes;
        }

        int packedBytesPerChunk = ElementsPerChunk * bitWidth / 8;
        int numChunks = (offset + rowCount + ElementsPerChunk - 1) / ElementsPerChunk;
        long requiredBytes = (long)numChunks * packedBytesPerChunk;
        if (packed.Length < requiredBytes)
            throw new VortexFormatException(
                $"fastlanes.bitpacked buffer is {packed.Length} bytes but needs {requiredBytes} for {numChunks} chunks at bit_width={bitWidth}.");

        var chunkBytes = new byte[ElementsPerChunk * elementSize];
        var chunkSpan = MemoryMarshal.Cast<byte, T>(chunkBytes.AsSpan());

        int sliceEnd = offset + rowCount;
        for (int c = 0; c < numChunks; c++)
        {
            var chunkPacked = packed.Slice(c * packedBytesPerChunk, packedBytesPerChunk);
            UnpackChunk(bitWidth, chunkPacked, chunkSpan);

            // Per-chunk overlap with the slice [offset, offset+rowCount).
            int chunkStart = c * ElementsPerChunk;
            int chunkEnd = chunkStart + ElementsPerChunk;
            int overlapStart = Math.Max(chunkStart, offset);
            int overlapEnd = Math.Min(chunkEnd, sliceEnd);
            if (overlapEnd <= overlapStart) continue;

            int srcStart = overlapStart - chunkStart;
            int rowsToCopy = overlapEnd - overlapStart;
            int dstStart = overlapStart - offset;
            var srcBytes = MemoryMarshal.AsBytes(chunkSpan.Slice(srcStart, rowsToCopy));
            srcBytes.CopyTo(bytes.AsSpan(dstStart * elementSize));
        }
        return bytes;
    }

    private static void UnpackChunk<T>(int bitWidth, ReadOnlySpan<byte> packed, Span<T> destination)
        where T : unmanaged
    {
        if (typeof(T) == typeof(byte))
            Clast.FastLanes.BitPacking.UnpackChunk<byte>(bitWidth, packed, MemoryMarshal.Cast<T, byte>(destination));
        else if (typeof(T) == typeof(ushort))
            Clast.FastLanes.BitPacking.UnpackChunk<ushort>(bitWidth, packed, MemoryMarshal.Cast<T, ushort>(destination));
        else if (typeof(T) == typeof(uint))
            Clast.FastLanes.BitPacking.UnpackChunk<uint>(bitWidth, packed, MemoryMarshal.Cast<T, uint>(destination));
        else if (typeof(T) == typeof(ulong))
            Clast.FastLanes.BitPacking.UnpackChunk<ulong>(bitWidth, packed, MemoryMarshal.Cast<T, ulong>(destination));
        else
            throw new InvalidOperationException(
                $"fastlanes.bitpacked: UnpackChunk not supported for {typeof(T)}.");
    }

    internal readonly struct BitPackedMeta
    {
        public uint BitWidth { get; init; }
        public uint Offset { get; init; }
        public bool HasPatches { get; init; }
        public ulong PatchesLen { get; init; }
        public ulong PatchesOffset { get; init; }
        public int PatchIndicesPtype { get; init; }
        public bool PatchHasChunkOffsets { get; init; }
    }

    /// <summary>
    /// Parses <c>BitPackedMetadata</c> proto:
    ///   field 1 (varint): bit_width (u32)
    ///   field 2 (varint): offset (u32)
    ///   field 3 (length-delim, optional): patches (PatchesMetadata embedded message)
    /// And nested PatchesMetadata: 1=len, 2=offset, 3=indices_ptype, 4=chunk_offsets_len, 5=chunk_offsets_ptype.
    /// </summary>
    private static BitPackedMeta ParseBitPackedMetadata(ReadOnlySpan<byte> bytes)
    {
        uint bitWidth = 0, offset = 0;
        bool hasPatches = false;
        ulong patchesLen = 0, patchesOffset = 0;
        int patchIndicesPtype = 2; // default U32
        bool patchHasChunkOffsets = false;

        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                bitWidth = (uint)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                offset = (uint)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 2)
            {
                hasPatches = true;
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                ParsePatchesMetadata(bytes.Slice(pos, len),
                    out patchesLen, out patchesOffset, out patchIndicesPtype, out patchHasChunkOffsets);
                pos += len;
            }
            else SkipField(bytes, ref pos, wireType);
        }
        return new BitPackedMeta
        {
            BitWidth = bitWidth, Offset = offset, HasPatches = hasPatches,
            PatchesLen = patchesLen, PatchesOffset = patchesOffset,
            PatchIndicesPtype = patchIndicesPtype, PatchHasChunkOffsets = patchHasChunkOffsets,
        };
    }

    private static void ParsePatchesMetadata(
        ReadOnlySpan<byte> bytes,
        out ulong len, out ulong offset, out int indicesPtype, out bool hasChunkOffsets)
    {
        len = 0; offset = 0; indicesPtype = 2; hasChunkOffsets = false;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0) len = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0) offset = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 0) indicesPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 4 && wireType == 0) { Varint.ReadUnsigned(bytes, ref pos); hasChunkOffsets = true; }
            else if (fieldNum == 5 && wireType == 0) { Varint.ReadUnsigned(bytes, ref pos); hasChunkOffsets = true; }
            else SkipField(bytes, ref pos, wireType);
        }
    }

    private static void SkipField(ReadOnlySpan<byte> bytes, ref int pos, uint wireType)
    {
        switch (wireType)
        {
            case 0: Varint.ReadUnsigned(bytes, ref pos); break;
            case 1: pos += 8; break;
            case 2:
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                pos += len;
                break;
            case 5: pos += 4; break;
            default:
                throw new VortexFormatException(
                    $"Unsupported protobuf wire type {wireType} in BitPackedMetadata.");
        }
    }
}
