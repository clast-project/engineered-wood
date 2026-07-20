// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Clast.Pcodec;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.pco</c>: Pcodec-compressed numeric arrays.
///
/// <para>Vortex uses pco's <em>wrapped</em> format: it manages chunking and
/// paging itself with a shared 1- or 2-byte header per array. Per upstream
/// <c>encodings/pco/src/array.rs</c>:
/// <list type="bullet">
///   <item>Buffers (interleaved per chunk):
///     <c>[chunk0_meta, chunk0_page0, …, chunk0_pageN0, chunk1_meta, chunk1_page0, …]</c></item>
///   <item>0-1 children (optional validity)</item>
///   <item>Metadata <c>PcoMetadata { header: bytes, chunks: [{ pages: [{ n_values: u32 }] }] }</c></item>
/// </list></para>
///
/// <para>Decode delegates to <c>Clast.Pcodec.PcoWrappedDecoder&lt;T&gt;</c>.
/// Phase 1 scope: non-nullable; pco supports f32/f64/i32/i64/i16/u16/u32/u64
/// (numeric types matching Apache Arrow primitives).</para>
/// </summary>
internal static class PcoArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.ChildCount > 1)
            throw new VortexFormatException(
                $"vortex.pco expects 0 or 1 children, got {node.ChildCount}.");

        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException("vortex.pco ArrayNode has empty metadata.");
        var meta = ParsePcoMetadata(metaVec.RawBytes(metaVec.Length));

        // Buffers expected: sum over chunks of (1 + pages_in_chunk).
        int expectedBuffers = 0;
        for (int c = 0; c < meta.Chunks.Count; c++)
            expectedBuffers += 1 + meta.Chunks[c].PageNValues.Count;
        if (node.BufferRefCount != expectedBuffers)
            throw new VortexFormatException(
                $"vortex.pco expects {expectedBuffers} buffers from metadata "
                + $"({meta.Chunks.Count} chunks), got {node.BufferRefCount}.");

        var rowCount = checked((int)expectedRowCount);

        // Optional validity child. Vortex compresses ONLY VALID values, so the
        // pages decompress to a dense buffer of valid values that we then
        // splice into the sparse output by walking the validity bitmap.
        ArrowBuffer nullBuffer; int nullCount;
        if (node.ChildCount == 0) { nullBuffer = ArrowBuffer.Empty; nullCount = 0; }
        else
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(0), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }
        int validCount = rowCount - nullCount;

        return expectedType switch
        {
            FloatType => DecodeTo<float>(node, serialized, meta, rowCount, validCount, nullBuffer, nullCount,
                (data, nb, nc, len) => new FloatArray(new ArrowBuffer(data), nb, len, nc, 0)),
            DoubleType => DecodeTo<double>(node, serialized, meta, rowCount, validCount, nullBuffer, nullCount,
                (data, nb, nc, len) => new DoubleArray(new ArrowBuffer(data), nb, len, nc, 0)),
            Int16Type => DecodeTo<short>(node, serialized, meta, rowCount, validCount, nullBuffer, nullCount,
                (data, nb, nc, len) => new Int16Array(new ArrowBuffer(data), nb, len, nc, 0)),
            UInt16Type => DecodeTo<ushort>(node, serialized, meta, rowCount, validCount, nullBuffer, nullCount,
                (data, nb, nc, len) => new UInt16Array(new ArrowBuffer(data), nb, len, nc, 0)),
            Int32Type => DecodeTo<int>(node, serialized, meta, rowCount, validCount, nullBuffer, nullCount,
                (data, nb, nc, len) => new Int32Array(new ArrowBuffer(data), nb, len, nc, 0)),
            UInt32Type => DecodeTo<uint>(node, serialized, meta, rowCount, validCount, nullBuffer, nullCount,
                (data, nb, nc, len) => new UInt32Array(new ArrowBuffer(data), nb, len, nc, 0)),
            Int64Type => DecodeTo<long>(node, serialized, meta, rowCount, validCount, nullBuffer, nullCount,
                (data, nb, nc, len) => new Int64Array(new ArrowBuffer(data), nb, len, nc, 0)),
            UInt64Type => DecodeTo<ulong>(node, serialized, meta, rowCount, validCount, nullBuffer, nullCount,
                (data, nb, nc, len) => new UInt64Array(new ArrowBuffer(data), nb, len, nc, 0)),
            _ => throw new NotSupportedException(
                $"vortex.pco doesn't support Arrow type {expectedType}."),
        };
    }

    private static IArrowArray DecodeTo<T>(
        ArrayNode node,
        SerializedArray serialized,
        PcoMeta meta,
        int rowCount, int validCount,
        ArrowBuffer nullBuffer, int nullCount,
        Func<byte[], ArrowBuffer, int, int, IArrowArray> ctor)
        where T : unmanaged
    {
        var elementSize = Marshal.SizeOf<T>();
        // Dense buffer holds only valid values; we splice into the sparse final
        // output afterward (or use it directly when there are no nulls).
        bool dense = nullCount == 0;
        int targetCount = dense ? rowCount : validCount;
        var dst = new byte[(long)rowCount * elementSize];
        var denseBuf = dense ? dst : new byte[(long)validCount * elementSize];
        var denseSpan = MemoryMarshal.Cast<byte, T>(denseBuf.AsSpan());

        var decoder = new PcoWrappedDecoder<T>(meta.Header);

        int bufferIdx = 0;
        int rowsWritten = 0;
        for (int c = 0; c < meta.Chunks.Count; c++)
        {
            var chunkMetaBytes = ReadBuffer(node, serialized, bufferIdx++);
            var chunkHandle = decoder.BeginChunk(chunkMetaBytes);

            var pageCounts = meta.Chunks[c].PageNValues;
            for (int p = 0; p < pageCounts.Count; p++)
            {
                int pageRows = checked((int)pageCounts[p]);
                var pageBytes = ReadBuffer(node, serialized, bufferIdx++);
                int wrote = decoder.DecodePage(
                    chunkHandle, pageBytes, pageRows, denseSpan.Slice(rowsWritten, pageRows));
                if (wrote != pageRows)
                    throw new VortexFormatException(
                        $"vortex.pco: page {p} of chunk {c} declared {pageRows} values but decoder wrote {wrote}.");
                rowsWritten += pageRows;
            }
        }

        if (rowsWritten != targetCount)
            throw new VortexFormatException(
                $"vortex.pco: metadata pages sum to {rowsWritten} values but expected {targetCount} (rowCount={rowCount}, validCount={validCount}).");

        // Splice dense → sparse using the validity bitmap when there are nulls.
        if (!dense)
        {
            var dstSpan = MemoryMarshal.Cast<byte, T>(dst.AsSpan());
            var bitmap = nullBuffer.Span;
            int dIdx = 0;
            for (int r = 0; r < rowCount; r++)
            {
                if ((bitmap[r >> 3] & (1 << (r & 7))) != 0)
                    dstSpan[r] = denseSpan[dIdx++];
            }
        }

        return ctor(dst, nullBuffer, nullCount, rowCount);
    }

    private static ReadOnlySpan<byte> ReadBuffer(ArrayNode node, SerializedArray serialized, int idx)
    {
        var bufferRef = node.BufferRef(idx);
        var bufferDesc = serialized.Message.Buffer(bufferRef);
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.pco buffer compression {bufferDesc.Compression} not yet implemented.");
        return serialized.BufferBytes(bufferRef);
    }

    private readonly struct PcoMeta
    {
        public byte[] Header { get; init; }
        public IReadOnlyList<PcoChunkInfo> Chunks { get; init; }
    }

    private sealed class PcoChunkInfo
    {
        public List<uint> PageNValues { get; } = new();
    }

    /// <summary>
    /// Parses <c>PcoMetadata</c>:
    ///   field 1 (length-delimited): header (bytes)
    ///   field 2 (length-delimited, repeated): chunks (PcoChunkInfo messages)
    ///     PcoChunkInfo: field 1 (length-delim, repeated): pages (PcoPageInfo messages)
    ///       PcoPageInfo: field 1 (varint, u32): n_values
    /// </summary>
    private static PcoMeta ParsePcoMetadata(ReadOnlySpan<byte> bytes)
    {
        byte[] header = System.Array.Empty<byte>();
        var chunks = new List<PcoChunkInfo>();
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 2)
            {
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                header = bytes.Slice(pos, len).ToArray();
                pos += len;
            }
            else if (fieldNum == 2 && wireType == 2)
            {
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                chunks.Add(ParseChunkInfo(bytes.Slice(pos, len)));
                pos += len;
            }
            else SkipField(bytes, ref pos, wireType);
        }
        return new PcoMeta { Header = header, Chunks = chunks };
    }

    private static PcoChunkInfo ParseChunkInfo(ReadOnlySpan<byte> bytes)
    {
        var info = new PcoChunkInfo();
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 2)
            {
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                info.PageNValues.Add(ParsePageNValues(bytes.Slice(pos, len)));
                pos += len;
            }
            else SkipField(bytes, ref pos, wireType);
        }
        return info;
    }

    private static uint ParsePageNValues(ReadOnlySpan<byte> bytes)
    {
        uint nValues = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                nValues = (uint)Varint.ReadUnsigned(bytes, ref pos);
            else SkipField(bytes, ref pos, wireType);
        }
        return nValues;
    }

    private static void SkipField(ReadOnlySpan<byte> bytes, ref int pos, uint wireType)
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
                    $"Unsupported wire type {wireType} in PcoMetadata.");
        }
    }
}
