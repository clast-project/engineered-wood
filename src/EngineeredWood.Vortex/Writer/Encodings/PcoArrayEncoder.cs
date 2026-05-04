// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Clast.Pcodec;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.PcoArrayDecoder"/>:
/// emits a <c>vortex.pco</c> ArrayNode subtree by Pcodec-compressing the
/// column's <em>valid</em> values (nulls are filtered out and ride as a
/// separate validity child).
///
/// <para>Wire shape per upstream <c>encodings/pco/src/array.rs</c>:
/// <list type="bullet">
///   <item>Buffers (interleaved per chunk):
///     <c>[chunk0_meta, chunk0_page0, …, chunk0_pageN0, chunk1_meta, chunk1_page0, …]</c></item>
///   <item>0-1 children (optional validity bitmap)</item>
///   <item>Metadata <c>PcoMetadata { header: bytes, chunks: [{ pages: [{ n_values: u32 }] }] }</c></item>
/// </list>
/// <c>Clast.Pcodec.PcoWrappedEncoder&lt;T&gt;</c> emits one page per
/// <c>EncodeChunk</c> call, so each <see cref="PcoArrayEncoder"/> chunk has
/// exactly one page and the buffer layout simplifies to <c>[meta0, page0,
/// meta1, page1, …]</c>.</para>
///
/// <para>Scope: i16/u16/i32/u32/i64/u64/f32/f64. Non-sliced inputs only;
/// nullable + non-nullable supported. Empty / all-null columns produce zero
/// chunks (header + validity child only).</para>
///
/// <para>Opt-in via <c>VortexFileWriter(preferPco: true)</c>. When set, Pco
/// supersedes the float/integer compressing chain (ALP / ALP-RD / RLE / FoR /
/// bitpacked) for the types it covers — pco's mode-search auto-selects
/// Classic / IntMult / FloatMult / FloatQuant per chunk, which tends to beat
/// the format-specific encoders on noisy real-world data at the cost of
/// slower decode.</para>
/// </summary>
internal static class PcoArrayEncoder
{
    /// <summary>
    /// Per-chunk row cap. Matches upstream's <c>VALUES_PER_CHUNK =
    /// pco::DEFAULT_MAX_PAGE_N = 1 &lt;&lt; 18</c>. Larger inputs split into
    /// multiple chunks, each with its own meta + page buffer pair.
    /// </summary>
    private const int ValuesPerChunk = 1 << 18;

    /// <summary>
    /// True iff <paramref name="array"/> is a supported numeric type, has
    /// <c>offset = 0</c>, has at least one valid (non-null) value, and is
    /// long enough that pco's overhead (header + per-chunk meta) doesn't
    /// dominate. Profitability isn't probed — caller is expected to opt-in
    /// via <c>preferPco</c>.
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is not (Int16Array or Int32Array or Int64Array
                          or UInt16Array or UInt32Array or UInt64Array
                          or FloatArray or DoubleArray)) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        if (data.Offset != 0) return false;
        if (array.Length < 16) return false;
        // All-null columns produce zero pco chunks. Cheaper to fall through
        // to plain primitive (validity bitmap + zeroed data buffer) than
        // emit a pco header for nothing.
        if ((int)data.GetNullCount() >= array.Length) return false;
        return true;
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        var data = ((Apache.Arrow.Array)array).Data;
        if (data.Offset != 0)
            throw new NotSupportedException("vortex.pco writer doesn't yet support sliced inputs.");

        int rowCount = array.Length;
        int nullCount = (int)data.GetNullCount();
        bool hasNulls = nullCount > 0;
        int validCount = rowCount - nullCount;

        // 1. Compress valid values via pco — type-dispatched; PcoWrappedEncoder<T>
        //    is generic on the element type so we go through a typed helper.
        byte[] header;
        var chunkMetas = new List<byte[]>();
        var pages = new List<byte[]>();
        var pageNValues = new List<uint>();
        switch (array)
        {
            case Int16Array a:
                header = EncodeTyped<short>(GetDense<short>(a, hasNulls, validCount), chunkMetas, pages, pageNValues);
                break;
            case UInt16Array a:
                header = EncodeTyped<ushort>(GetDense<ushort>(a, hasNulls, validCount), chunkMetas, pages, pageNValues);
                break;
            case Int32Array a:
                header = EncodeTyped<int>(GetDense<int>(a, hasNulls, validCount), chunkMetas, pages, pageNValues);
                break;
            case UInt32Array a:
                header = EncodeTyped<uint>(GetDense<uint>(a, hasNulls, validCount), chunkMetas, pages, pageNValues);
                break;
            case Int64Array a:
                header = EncodeTyped<long>(GetDense<long>(a, hasNulls, validCount), chunkMetas, pages, pageNValues);
                break;
            case UInt64Array a:
                header = EncodeTyped<ulong>(GetDense<ulong>(a, hasNulls, validCount), chunkMetas, pages, pageNValues);
                break;
            case FloatArray a:
                header = EncodeTyped<float>(GetDense<float>(a, hasNulls, validCount), chunkMetas, pages, pageNValues);
                break;
            case DoubleArray a:
                header = EncodeTyped<double>(GetDense<double>(a, hasNulls, validCount), chunkMetas, pages, pageNValues);
                break;
            default:
                throw new NotSupportedException(
                    $"vortex.pco writer doesn't support Arrow array {array.GetType().Name}.");
        }

        // 2. Register buffers in interleaved order: [meta0, page0, meta1, page1, ...].
        var bufferIdxs = new List<ushort>(chunkMetas.Count * 2);
        for (int c = 0; c < chunkMetas.Count; c++)
        {
            bufferIdxs.Add(sb.AddBuffer(chunkMetas[c], alignmentExponent: 0));
            bufferIdxs.Add(sb.AddBuffer(pages[c], alignmentExponent: 0));
        }

        // 3. Optional validity child at children[0]. Match the pattern other
        //    encoders use: vortex.bool wrapping a raw bitmap buffer.
        var childTickets = new List<int>(1);
        if (hasNulls)
        {
            var bitmap = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: rowCount);
            ushort bitmapBufIdx = sb.AddBuffer(bitmap, alignmentExponent: 0);
            int validityNode = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, idx.Bool, bitmapBufIdx);
            childTickets.Add(validityNode);
        }

        // 4. Serialize PcoMetadata proto and emit ArrayNode.
        var metadataBytes = SerializePcoMetadata(header, pageNValues);
        int metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var bufIdxArr = bufferIdxs.ToArray();
        var children = childTickets.ToArray();
        return (statsTicket, hasNulls) switch
        {
            (null, false) => ArrayNodeEmitter.EmitWithMetadataBuffersAndChildren(
                sb.Builder, idx.Pco, bufIdxArr, metadataTicket, children),
            (null, true) => ArrayNodeEmitter.EmitWithMetadataBuffersAndChildren(
                sb.Builder, idx.Pco, bufIdxArr, metadataTicket, children),
            ({ } stat, _) => ArrayNodeEmitter.EmitWithMetadataBuffersChildrenAndStats(
                sb.Builder, idx.Pco, bufIdxArr, metadataTicket, children, stat),
        };
    }

    /// <summary>
    /// Builds a dense <typeparamref name="T"/> array of the column's valid
    /// values. For non-nullable inputs we reinterpret the existing buffer
    /// (no copy); for nullable inputs we walk the validity bitmap and
    /// compact valid rows into a fresh array.
    /// </summary>
    private static T[] GetDense<T>(IArrowArray array, bool hasNulls, int validCount)
        where T : unmanaged
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int elemSize = Marshal.SizeOf<T>();
        var raw = MemoryMarshal.Cast<byte, T>(data.Buffers[1].Span.Slice(0, n * elemSize));

        if (!hasNulls)
        {
            var copy = new T[n];
            raw.CopyTo(copy);
            return copy;
        }

        var dense = new T[validCount];
        var validity = data.Buffers[0].Span;
        int j = 0;
        for (int i = 0; i < n; i++)
            if ((validity[i >> 3] & (1 << (i & 7))) != 0)
                dense[j++] = raw[i];
        return dense;
    }

    /// <summary>
    /// Drives <see cref="PcoWrappedEncoder{T}"/> over <paramref name="dense"/>:
    /// allocates the encoder, snapshots its <c>Header</c>, then chunks the
    /// dense input at <see cref="ValuesPerChunk"/> boundaries and accumulates
    /// per-chunk meta + page buffers + page row counts. Returns the header
    /// bytes for inclusion in PcoMetadata.
    /// </summary>
    private static byte[] EncodeTyped<T>(
        T[] dense,
        List<byte[]> chunkMetas, List<byte[]> pages, List<uint> pageNValues)
        where T : unmanaged
    {
        var encoder = new PcoWrappedEncoder<T>();
        byte[] header = encoder.Header.ToArray();

        int pos = 0;
        while (pos < dense.Length)
        {
            int take = Math.Min(ValuesPerChunk, dense.Length - pos);
            var slice = dense.AsSpan(pos, take);
            var wrapped = encoder.EncodeChunk(slice);
            chunkMetas.Add(wrapped.ChunkMeta);
            pages.Add(wrapped.Page);
            pageNValues.Add((uint)take);
            pos += take;
        }
        return header;
    }

    /// <summary>
    /// Inline PcoMetadata proto bytes:
    ///   field 1 (length-delim): header (bytes)
    ///   field 2 (length-delim, repeated): chunks (PcoChunkInfo messages)
    ///     PcoChunkInfo: field 1 (length-delim, repeated): pages (PcoPageInfo messages)
    ///       PcoPageInfo: field 1 (varint, u32): n_values
    /// One page per chunk with this encoder, so each PcoChunkInfo has a
    /// single PcoPageInfo entry.
    /// </summary>
    private static byte[] SerializePcoMetadata(byte[] header, List<uint> pageNValues)
    {
        // Pre-size generously: header field ≤ 1 + 5 + header_len bytes;
        // per-chunk record ≤ 12 bytes (outer tag(1) + outer len(1) + inner
        // tag(1) + inner len(1) + page tag(1) + page len(1) + n_values(≤5)
        // ≈ 11). Round to 16 / chunk.
        var buf = new byte[8 + header.Length + pageNValues.Count * 16];
        int pos = 0;

        // Field 1: header (bytes).
        buf[pos++] = 0x0A; // tag 1, wire-type 2 (length-delim)
        pos += Varint.WriteUnsigned(buf.AsSpan(pos), (ulong)header.Length);
        Buffer.BlockCopy(header, 0, buf, pos, header.Length);
        pos += header.Length;

        // Field 2 (repeated): one PcoChunkInfo per chunk. Reuse two stack
        // buffers across iterations — pageNValues.Count is bounded by
        // (rowCount / ValuesPerChunk) but can still be large for billion-row
        // columns, so allocating per-iteration would risk stack overflow.
        Span<byte> page = stackalloc byte[6];
        Span<byte> info = stackalloc byte[12];
        for (int c = 0; c < pageNValues.Count; c++)
        {
            // Inner PcoPageInfo: field 1 = n_values (varint).
            int pageLen = 0;
            page[pageLen++] = 0x08; // tag 1, varint
            pageLen += Varint.WriteUnsigned(page.Slice(pageLen), pageNValues[c]);

            // PcoChunkInfo: field 1 = pages (length-delim, repeated, one entry).
            int infoLen = 0;
            info[infoLen++] = 0x0A; // tag 1, wire-type 2
            infoLen += Varint.WriteUnsigned(info.Slice(infoLen), (ulong)pageLen);
            page.Slice(0, pageLen).CopyTo(info.Slice(infoLen));
            infoLen += pageLen;

            // Outer field 2: PcoChunkInfo (length-delim).
            buf[pos++] = 0x12; // tag 2, wire-type 2
            pos += Varint.WriteUnsigned(buf.AsSpan(pos), (ulong)infoLen);
            info.Slice(0, infoLen).CopyTo(buf.AsSpan(pos));
            pos += infoLen;
        }

        var result = new byte[pos];
        System.Array.Copy(buf, result, pos);
        return result;
    }
}
