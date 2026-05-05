// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.RleArrayDecoder"/>:
/// emits a <c>fastlanes.rle</c> ArrayNode subtree. Despite the name, this
/// encoding has no FastLanes UTL transposition — it's plain dict-and-indices
/// RLE applied to fixed 1024-element chunks.
///
/// <para>Pipeline (per 1024-row chunk):
/// <list type="number">
///   <item>Find unique values in input order; assign each a chunk-local index.</item>
///   <item>Append the unique values to the global <c>values</c> child.</item>
///   <item>Append the per-row local indices to the global <c>indices</c> child.</item>
///   <item>Append the chunk's starting offset (cumulative unique-count) to
///     <c>values_idx_offsets</c>.</item>
/// </list></para>
///
/// <para>Wire shape: 0 buffers, 3 children (values, indices, values_idx_offsets),
/// metadata <c>RLEMetadata { values_len, indices_len, indices_ptype,
/// values_idx_offsets_len, values_idx_offsets_ptype, offset = 0 }</c>. Same
/// vtable as vortex.list / fastlanes.for / fastlanes.delta / vortex.dict
/// (slots 0+1+2, with optional slot 4 for stats).</para>
///
/// <para>Scope: nullable + non-nullable, sliced + non-sliced primitive
/// numeric columns (Int8..Int64, UInt8..UInt64, Float32, Float64), length
/// ≥ 1024. Indices are u8 when every chunk has ≤ 256 distinct values, u16
/// otherwise. Offsets are u64 for simplicity. For nullable inputs the
/// indices child carries the validity bitmap (matching upstream's
/// <c>rle/vtable/validity.rs</c>: "RLE array's validity = its INDICES
/// child's validity"). Null rows write index 0 — the reader skips lookups
/// at null positions so the placeholder index never resolves.</para>
/// </summary>
internal static class RleArrayEncoder
{
    private const int ElementsPerChunk = 1024;

    /// <summary>
    /// Structural + profitability check: non-null, non-sliced
    /// <see cref="FloatArray"/> or <see cref="DoubleArray"/> column of length
    /// ≥ 1024 where the total per-chunk-unique count is at most <c>n / 4</c>.
    /// The probe doesn't materialize per-chunk dicts — it does O(n) work and
    /// bails early when the threshold is crossed.
    ///
    /// <para>Integer columns are intentionally excluded — bitpacked/FoR/delta
    /// almost always beat RLE on ints, and putting RLE earlier in the dispatch
    /// chain steals from those better encodings. RLE shines for floats, where
    /// no other compressing encoding currently applies.</para>
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is null) return false;
        if (array is not FloatArray and not DoubleArray) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n < ElementsPerChunk) return false;
        if (ElementSize(array) is not int elemSize) return false;

        int off = data.Offset;
        int numChunks = n / ElementsPerChunk; // ignore trailing partial chunk for the probe
        int cap = n / 4;
        int total = 0;
        var src = data.Buffers[1].Span;
        for (int c = 0; c < numChunks; c++)
        {
            int chunkStart = (off + c * ElementsPerChunk) * elemSize;
            int chunkBytes = ElementsPerChunk * elemSize;
            // Count distinct via a HashSet over byte sequences. Per-chunk
            // allocation is fine — bounded to the chunk's row count.
            var seen = new HashSet<long>(); // collapse N-byte values into a 64-bit key when possible (elemSize <= 8)
            for (int i = 0; i < ElementsPerChunk; i++)
            {
                long key = ReadLongKey(src, chunkStart + i * elemSize, elemSize);
                seen.Add(key);
            }
            total += seen.Count;
            if (total > cap) return false;
        }
        return true;
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        var data = ((Apache.Arrow.Array)array).Data;
        if (ElementSize(array) is not int elemSize)
            throw new NotSupportedException(
                $"fastlanes.rle writer doesn't support Arrow {array.GetType().Name}.");

        int n = array.Length;
        int numChunks = (n + ElementsPerChunk - 1) / ElementsPerChunk;

        // Run pipeline: build values bytes, indices buffer, valuesIdxOffsets.
        // Returned arrays are typed Arrow arrays for recursive encoding.
        var (valuesArray, indicesArray, indicesPtype, offsetsArray) =
            RunPipeline(array, elemSize, n, numChunks);

        // Encode children.
        int valuesNodeTicket = ArrayEncoderDispatch.Emit(sb, valuesArray, idx);
        int indicesNodeTicket = ArrayEncoderDispatch.Emit(sb, indicesArray, idx);
        int offsetsNodeTicket = ArrayEncoderDispatch.Emit(sb, offsetsArray, idx);

        // Metadata.
        var metadataBytes = SerializeMetadata(
            valuesLen: (ulong)valuesArray.Length,
            indicesLen: (ulong)indicesArray.Length,
            indicesPtype: indicesPtype,
            offsetsLen: (ulong)offsetsArray.Length,
            offsetsPtype: 3); // 3 = U64
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var children = new[] { valuesNodeTicket, indicesNodeTicket, offsetsNodeTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, idx.Rle, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, idx.Rle, metadataTicket, children, statsTicket.Value);
    }

    private static int? ElementSize(IArrowArray array) => array switch
    {
        Int8Array or UInt8Array => 1,
        Int16Array or UInt16Array => 2,
        Int32Array or UInt32Array or FloatArray => 4,
        Int64Array or UInt64Array or DoubleArray => 8,
        _ => null,
    };

    /// <summary>
    /// Reads a fixed-size little-endian primitive at <paramref name="byteOffset"/>
    /// in <paramref name="src"/> and zero-extends to long for use as a hash key.
    /// Sign of source data doesn't matter — the bit pattern uniquely identifies the value.
    /// </summary>
    private static long ReadLongKey(ReadOnlySpan<byte> src, int byteOffset, int elemSize) => elemSize switch
    {
        1 => src[byteOffset],
        2 => BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(byteOffset, 2)),
        4 => BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(byteOffset, 4)),
        8 => unchecked((long)BinaryPrimitives.ReadUInt64LittleEndian(src.Slice(byteOffset, 8))),
        _ => throw new NotSupportedException(),
    };

    /// <summary>
    /// Walks each 1024-row chunk, builds per-chunk unique-value dicts, and
    /// produces the three children's Arrow arrays. Trailing partial chunk is
    /// padded with zero bytes; the trailing rows in the indices buffer must
    /// still resolve to a valid chunk-local index, so we ensure index 0 (the
    /// first unique value) exists for every chunk by inserting "0 bytes" as
    /// the first lookup if needed.
    /// </summary>
    private static (IArrowArray Values, IArrowArray Indices, byte IndicesPtype, IArrowArray Offsets)
        RunPipeline(IArrowArray array, int elemSize, int n, int numChunks)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        var src = data.Buffers[1].Span;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validityBitmap = hasNulls ? data.Buffers[0].Span : default;

        // Phase 1: discover per-chunk unique-value lists. We need to know the
        // total values_len before allocating, AND the max per-chunk distinct
        // count to pick u8 vs u16 for the indices. Null rows are excluded
        // from the dict — the index there is always 0 (placeholder; the
        // reader skips lookups at null rows via the indices' validity bitmap).
        var perChunkDicts = new Dictionary<long, int>[numChunks];
        var perChunkValueKeys = new List<long>[numChunks];
        int totalValues = 0;
        int maxPerChunkDistinct = 0;
        for (int c = 0; c < numChunks; c++)
        {
            int chunkStart = (off + c * ElementsPerChunk) * elemSize;
            int rowsInChunk = Math.Min(ElementsPerChunk, n - c * ElementsPerChunk);
            var dict = new Dictionary<long, int>();
            var keys = new List<long>();
            for (int i = 0; i < rowsInChunk; i++)
            {
                int absRow = off + c * ElementsPerChunk + i;
                if (hasNulls && (validityBitmap[absRow >> 3] & (1 << (absRow & 7))) == 0)
                    continue;
                long key = ReadLongKey(src, chunkStart + i * elemSize, elemSize);
                if (!dict.ContainsKey(key))
                {
                    dict[key] = keys.Count;
                    keys.Add(key);
                }
            }
            // Two cases need a placeholder:
            //   1. Trailing partial chunk's padding rows still get an index
            //      value packed into the buffer (the decoder won't read them
            //      but the bytes must be in-range for any future tooling).
            //   2. All-null chunk: dict is empty, but every row's index slot
            //      still needs a valid in-range value. Insert 0.
            if ((rowsInChunk < ElementsPerChunk || keys.Count == 0) && keys.Count == 0)
            {
                dict[0] = 0;
                keys.Add(0);
            }
            perChunkDicts[c] = dict;
            perChunkValueKeys[c] = keys;
            totalValues += keys.Count;
            if (keys.Count > maxPerChunkDistinct) maxPerChunkDistinct = keys.Count;
        }

        // Phase 2: pick indices type. u8 if every chunk has ≤ 256 distinct values.
        bool indicesAreU8 = maxPerChunkDistinct <= byte.MaxValue + 1;
        byte indicesPtype = indicesAreU8 ? (byte)0 : (byte)1;
        int indicesElemSize = indicesAreU8 ? 1 : 2;

        // Phase 3: allocate output buffers.
        var valuesBytes = new byte[(long)totalValues * elemSize];
        var indicesBytes = new byte[(long)numChunks * ElementsPerChunk * indicesElemSize];
        var offsetsBytes = new byte[numChunks * 8]; // u64 each
        // Indices validity bitmap: covers the FULL numChunks × 1024 buffer.
        // Initialised all-bits-set so non-nullable inputs need no further
        // work; for nullable inputs we clear bits at null positions below.
        // Padding rows in the trailing partial chunk are also marked valid —
        // the decoder bounds the visible window to the column's row count
        // so those bits are unobservable.
        var indicesValidityBytes = hasNulls
            ? new byte[((long)numChunks * ElementsPerChunk + 7) >> 3]
            : System.Array.Empty<byte>();
        if (hasNulls)
        {
            // Mark all bits set; we'll clear nulls in Phase 4.
            for (int i = 0; i < indicesValidityBytes.Length; i++) indicesValidityBytes[i] = 0xFF;
        }
        int indicesNullCount = 0;

        // Phase 4: fill them. For each chunk:
        //  - copy the chunk's unique-value bytes into the values buffer.
        //  - record the chunk's starting offset (cumulative unique count).
        //  - re-walk the chunk's source rows to compute and store the local index.
        int valuesPos = 0;
        for (int c = 0; c < numChunks; c++)
        {
            // Record offset BEFORE appending this chunk's values.
            BinaryPrimitives.WriteUInt64LittleEndian(
                offsetsBytes.AsSpan(c * 8, 8), (ulong)valuesPos);

            // Append unique values.
            var keys = perChunkValueKeys[c];
            for (int k = 0; k < keys.Count; k++)
                WriteKey(valuesBytes.AsSpan(valuesPos * elemSize + k * elemSize, elemSize), keys[k], elemSize);
            valuesPos += keys.Count;

            // Fill local indices for this chunk's 1024 rows.
            int chunkStart = (off + c * ElementsPerChunk) * elemSize;
            int rowsInChunk = Math.Min(ElementsPerChunk, n - c * ElementsPerChunk);
            var dict = perChunkDicts[c];
            int chunkIndicesStart = c * ElementsPerChunk * indicesElemSize;
            for (int i = 0; i < rowsInChunk; i++)
            {
                int absRow = off + c * ElementsPerChunk + i;
                if (hasNulls && (validityBitmap[absRow >> 3] & (1 << (absRow & 7))) == 0)
                {
                    // Null row: write placeholder index 0; clear validity bit
                    // at logical position c*1024+i so the reader skips lookup.
                    int logical = c * ElementsPerChunk + i;
                    indicesValidityBytes[logical >> 3] &= (byte)~(1 << (logical & 7));
                    indicesNullCount++;
                    continue;
                }
                long key = ReadLongKey(src, chunkStart + i * elemSize, elemSize);
                int localIdx = dict[key];
                WriteIndex(indicesBytes.AsSpan(chunkIndicesStart + i * indicesElemSize, indicesElemSize),
                    localIdx, indicesAreU8);
            }
            // Padding rows in the trailing partial chunk keep validity = 1
            // (the placeholder set above) and index = 0 (default zero in
            // freshly allocated buffer); the reader's row-count limit hides them.
        }

        // Build Arrow arrays for the children.
        IArrowArray valuesArray = BuildPrimitiveArray(array, valuesBytes, totalValues);
        var indicesValidityBuf = hasNulls
            ? new ArrowBuffer(indicesValidityBytes)
            : ArrowBuffer.Empty;
        IArrowArray indicesArray = indicesAreU8
            ? new UInt8Array(new ArrowBuffer(indicesBytes), indicesValidityBuf, indicesBytes.Length, indicesNullCount, 0)
            : new UInt16Array(new ArrowBuffer(indicesBytes), indicesValidityBuf, indicesBytes.Length / 2, indicesNullCount, 0);
        IArrowArray offsetsArray = new UInt64Array(
            new ArrowBuffer(offsetsBytes), ArrowBuffer.Empty, numChunks, 0, 0);

        return (valuesArray, indicesArray, indicesPtype, offsetsArray);
    }

    private static void WriteKey(Span<byte> dest, long key, int elemSize)
    {
        switch (elemSize)
        {
            case 1: dest[0] = (byte)key; break;
            case 2: BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)key); break;
            case 4: BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)key); break;
            case 8: BinaryPrimitives.WriteUInt64LittleEndian(dest, unchecked((ulong)key)); break;
            default: throw new NotSupportedException();
        }
    }

    private static void WriteIndex(Span<byte> dest, int localIdx, bool u8)
    {
        if (u8)
            dest[0] = checked((byte)localIdx);
        else
            BinaryPrimitives.WriteUInt16LittleEndian(dest, checked((ushort)localIdx));
    }

    private static IArrowArray BuildPrimitiveArray(IArrowArray template, byte[] valuesBytes, int totalValues)
    {
        var buf = new ArrowBuffer(valuesBytes);
        return template switch
        {
            Int8Array => new Int8Array(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            UInt8Array => new UInt8Array(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            Int16Array => new Int16Array(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            UInt16Array => new UInt16Array(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            Int32Array => new Int32Array(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            UInt32Array => new UInt32Array(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            FloatArray => new FloatArray(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            Int64Array => new Int64Array(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            UInt64Array => new UInt64Array(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            DoubleArray => new DoubleArray(buf, ArrowBuffer.Empty, totalValues, 0, 0),
            _ => throw new NotSupportedException(),
        };
    }

    /// <summary>
    /// Inline RLEMetadata proto bytes:
    ///   field 1 (varint, u64): values_len
    ///   field 2 (varint, u64): indices_len
    ///   field 3 (varint, PType): indices_ptype
    ///   field 4 (varint, u64): values_idx_offsets_len
    ///   field 5 (varint, PType): values_idx_offsets_ptype
    ///   field 6 (varint, u64): offset (omitted when 0 per proto3 default).
    /// </summary>
    private static byte[] SerializeMetadata(
        ulong valuesLen, ulong indicesLen, byte indicesPtype,
        ulong offsetsLen, byte offsetsPtype)
    {
        // Worst case: 5 × (1 tag + 10 varint) ≈ 55 bytes.
        Span<byte> tmp = stackalloc byte[64];
        int pos = 0;
        tmp[pos++] = 0x08; // tag 1
        pos += Varint.WriteUnsigned(tmp.Slice(pos), valuesLen);
        tmp[pos++] = 0x10; // tag 2
        pos += Varint.WriteUnsigned(tmp.Slice(pos), indicesLen);
        tmp[pos++] = 0x18; // tag 3
        tmp[pos++] = indicesPtype;
        tmp[pos++] = 0x20; // tag 4
        pos += Varint.WriteUnsigned(tmp.Slice(pos), offsetsLen);
        tmp[pos++] = 0x28; // tag 5
        tmp[pos++] = offsetsPtype;
        return tmp.Slice(0, pos).ToArray();
    }
}
