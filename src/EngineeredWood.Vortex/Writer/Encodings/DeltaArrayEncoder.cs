// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.DeltaArrayDecoder"/>:
/// emits a <c>fastlanes.delta</c> ArrayNode subtree. Encodes each 1024-row
/// chunk as
/// <list type="number">
///   <item>FastLanes <em>transpose</em>: <c>transposed[transpose(i)] = input[i]</c>
///     using the type-independent 16×8×8 permutation
///     <c>(idx%16)*64 + FL_ORDER[(idx/16)%8]*8 + (idx/128)</c>;</item>
///   <item><c>bases = transposed[0..LANES]</c>;</item>
///   <item><c>deltas = Delta::delta::&lt;LANES&gt;(transposed, bases)</c> via
///     <see cref="Clast.FastLanes.Delta.DeltaChunk{T}"/>.</item>
/// </list>
///
/// <para>Wire shape: 0 buffers, 2 children: <c>bases</c> (numChunks × LANES,
/// vortex.primitive) and <c>deltas</c> (numChunks × 1024, typically
/// fastlanes.bitpacked); metadata <c>DeltaMetadata { deltas_len: u64, offset: 0 }</c>
/// (proto3 default omits offset). Same vtable as vortex.list / fastlanes.for
/// (slots 0+1+2 with optional slot 4 for stats).</para>
///
/// <para>Scope: non-nullable, non-sliced unsigned-integer columns (UInt8..UInt64),
/// length ≥ 1024. Vortex's writer fill-forwards null positions before delta
/// encoding to keep within-lane deltas small; we defer that to a later chunk
/// and reject nullable inputs. Caller (dispatch) gates application on
/// <c>stats.IsStrictSorted</c> so we only run the O(n) pipeline on columns
/// where deltas will compress well.</para>
/// </summary>
internal static class DeltaArrayEncoder
{
    private const int ElementsPerChunk = 1024;
    private static readonly int[] FL_ORDER = new int[] { 0, 4, 2, 6, 1, 5, 3, 7 };

    private static int TransposeIndex(int idx)
    {
        int lane = idx & 0xF;
        int order = (idx >> 4) & 0x7;
        int row = idx >> 7;
        return (lane << 6) | (FL_ORDER[order] << 3) | row;
    }

    /// <summary>
    /// Structural check (unsigned int, non-nullable, non-sliced, length ≥
    /// 1024) AND profitability probe — within-lane successive differences
    /// must fit in fewer bits than native, otherwise the bitpacked deltas
    /// child would be no smaller than the raw values.
    ///
    /// <para>Important: a column being <em>strictly sorted in linear order</em>
    /// is NOT sufficient. The FastLanes layout permutes values via FL_ORDER
    /// within each lane, so a linearly-monotonic sequence becomes
    /// zig-zag-monotonic within a lane and the unsigned deltas wrap. We
    /// probe the actual within-lane MaxBits here and accept only when it's
    /// at most half the native bit width — the threshold below which
    /// delta+bitpacked reliably beats FoR+bitpacked on the same column.</para>
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is null) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        if (data.GetNullCount() > 0) return false;
        if (array.Length < ElementsPerChunk) return false;
        int? native = NativeBits(array);
        if (native is not int nativeBits) return false;
        int deltaMaxBits = ProbeDeltaMaxBits(array);
        return deltaMaxBits * 2 <= nativeBits;
    }

    private static int? NativeBits(IArrowArray array) => array switch
    {
        UInt8Array => 8,
        UInt16Array => 16,
        UInt32Array => 32,
        UInt64Array => 64,
        _ => null,
    };

    /// <summary>
    /// Computes <c>MaxBits</c> over within-lane successive differences across
    /// all chunks WITHOUT materializing the transposed buffer. For each output
    /// position p in <c>[LANES, 1024)</c>, the value is <c>input[chunk_start +
    /// transpose(p)]</c> and the previous lane element is <c>input[chunk_start
    /// + transpose(p - LANES)]</c>. The unsigned difference (with natural
    /// wrap) is what the bitpacked encoder will see, so its MaxBits drives
    /// profitability.
    /// </summary>
    private static int ProbeDeltaMaxBits(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int off = data.Offset;
        int numChunks = (n + ElementsPerChunk - 1) / ElementsPerChunk;

        return array switch
        {
            UInt8Array => ProbeMaxBitsTyped<byte>(data.Buffers[1].Span.Slice(off, n),
                n, numChunks, Clast.FastLanes.Delta.LaneCount<byte>(), (a, b) => (byte)(a - b)),
            UInt16Array => ProbeMaxBitsTyped<ushort>(MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(off * 2, n * 2)),
                n, numChunks, Clast.FastLanes.Delta.LaneCount<ushort>(), (a, b) => (ushort)(a - b)),
            UInt32Array => ProbeMaxBitsTyped<uint>(MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(off * 4, n * 4)),
                n, numChunks, Clast.FastLanes.Delta.LaneCount<uint>(), (a, b) => a - b),
            UInt64Array => ProbeMaxBitsTyped<ulong>(MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(off * 8, n * 8)),
                n, numChunks, Clast.FastLanes.Delta.LaneCount<ulong>(), (a, b) => a - b),
            _ => int.MaxValue,
        };
    }

    private static int ProbeMaxBitsTyped<T>(
        ReadOnlySpan<T> values, int n, int numChunks, int lanes, Func<T, T, T> sub)
        where T : unmanaged, IComparable<T>
    {
        // For each chunk, walk output positions p in [LANES, 1024) and compare
        // the underlying input value at transpose(p) to the lane-prior value
        // at transpose(p - LANES). Trailing chunks are zero-padded — treat
        // out-of-range source positions as 0.
        ulong maxResidual = 0;
        for (int c = 0; c < numChunks; c++)
        {
            int chunkStart = c * ElementsPerChunk;
            for (int p = lanes; p < ElementsPerChunk; p++)
            {
                int srcCur = chunkStart + TransposeIndex(p);
                int srcPrev = chunkStart + TransposeIndex(p - lanes);
                T cur = srcCur < n ? values[srcCur] : default;
                T prev = srcPrev < n ? values[srcPrev] : default;
                ulong r = ToUInt64(sub(cur, prev));
                if (r > maxResidual) maxResidual = r;
            }
        }
        return maxResidual == 0 ? 0 : 64 - System.Numerics.BitOperations.LeadingZeroCount(maxResidual);
    }

    private static ulong ToUInt64<T>(T value) where T : unmanaged
    {
        // Reinterpret without sign extension. Works for byte/ushort/uint/ulong.
        return value switch
        {
            byte b => b,
            ushort u => u,
            uint u => u,
            ulong u => u,
            _ => throw new InvalidOperationException(),
        };
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array,
        ushort deltaEncodingIdx, ushort primitiveEncodingIdx,
        ushort bitpackedEncodingIdx, ushort boolEncodingIdx,
        int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        if (!IsApplicable(array))
            throw new InvalidOperationException(
                $"fastlanes.delta requires a non-nullable unsigned-int column with length ≥ {ElementsPerChunk} and within-lane MaxBits ≤ native/2; {array.GetType().Name} doesn't qualify.");

        // 1. Run the FastLanes delta pipeline. Returns same-typed Arrow arrays
        //    for bases (numChunks × LANES) and deltas (numChunks × 1024).
        var (basesArray, deltasArray, deltasLen) = RunPipeline(array);

        // 2. Encode bases via primitive (small — at most LANES * numChunks
        //    values, with no obvious bit-pattern structure to exploit).
        int basesNodeTicket = PrimitiveArrayEncoder.Emit(
            sb, basesArray, primitiveEncodingIdx, boolEncodingIdx);

        // 3. Encode deltas via bitpacked when applicable; otherwise fall back to
        //    plain primitive. This is exactly what vortex's bitpack_compress
        //    does on the deltas child.
        int deltasNodeTicket = BitPackedArrayEncoder.IsApplicable(deltasArray)
            ? BitPackedArrayEncoder.Emit(sb, deltasArray, bitpackedEncodingIdx, primitiveEncodingIdx, boolEncodingIdx)
            : PrimitiveArrayEncoder.Emit(sb, deltasArray, primitiveEncodingIdx, boolEncodingIdx);

        // 4. DeltaMetadata: field 1 (deltas_len, varint u64). Field 2 (offset)
        //    is 0 and omitted (proto3 default).
        var metadataBytes = SerializeMetadata(deltasLen);
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var children = new[] { basesNodeTicket, deltasNodeTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, deltaEncodingIdx, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, deltaEncodingIdx, metadataTicket, children, statsTicket.Value);
    }

    private static int LaneCount(IArrowArray array) => array switch
    {
        UInt8Array => Clast.FastLanes.Delta.LaneCount<byte>(),
        UInt16Array => Clast.FastLanes.Delta.LaneCount<ushort>(),
        UInt32Array => Clast.FastLanes.Delta.LaneCount<uint>(),
        UInt64Array => Clast.FastLanes.Delta.LaneCount<ulong>(),
        _ => throw new NotSupportedException(),
    };

    /// <summary>
    /// Runs the transpose + delta pipeline. Returns the bases and deltas
    /// children as same-typed Arrow arrays plus the actual deltas length
    /// (= padded length to multiple of 1024).
    /// </summary>
    private static (IArrowArray Bases, IArrowArray Deltas, int DeltasLen) RunPipeline(IArrowArray array)
    {
        return array switch
        {
            UInt8Array a => RunTyped<byte>(a, (data, len) => new UInt8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            UInt16Array a => RunTyped<ushort>(a, (data, len) => new UInt16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            UInt32Array a => RunTyped<uint>(a, (data, len) => new UInt32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            UInt64Array a => RunTyped<ulong>(a, (data, len) => new UInt64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            _ => throw new NotSupportedException(
                $"fastlanes.delta doesn't support Arrow {array.GetType().Name}."),
        };
    }

    private static (IArrowArray Bases, IArrowArray Deltas, int DeltasLen) RunTyped<T>(
        IArrowArray array, Func<byte[], int, IArrowArray> ctor)
        where T : unmanaged
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int elemSize = Marshal.SizeOf<T>();
        // Slice value bytes by data.Offset so src[i] is the logical row i.
        var srcBytes = data.Buffers[1].Span.Slice(data.Offset * elemSize, n * elemSize);
        var src = MemoryMarshal.Cast<byte, T>(srcBytes);

        int numChunks = (n + ElementsPerChunk - 1) / ElementsPerChunk;
        int paddedLen = numChunks * ElementsPerChunk;
        int lanes = LaneCount(array);
        int basesLen = numChunks * lanes;

        var basesBytes = new byte[basesLen * elemSize];
        var deltasBytes = new byte[paddedLen * elemSize];
        // Force Span<T> (not ReadOnlySpan<T>) so the per-chunk slices can be
        // written via CopyTo / DeltaChunk. byte[] → MemoryMarshal.Cast picks
        // the ReadOnlySpan overload by default.
        var basesSpan = MemoryMarshal.Cast<byte, T>(basesBytes.AsSpan());
        var deltasSpan = MemoryMarshal.Cast<byte, T>(deltasBytes.AsSpan());

        Span<T> chunk = stackalloc T[ElementsPerChunk];
        Span<T> transposed = stackalloc T[ElementsPerChunk];
        for (int c = 0; c < numChunks; c++)
        {
            // Copy the chunk — pad the trailing chunk with zeros if needed.
            int chunkStart = c * ElementsPerChunk;
            int rowsInChunk = Math.Min(ElementsPerChunk, n - chunkStart);
            chunk.Clear();
            src.Slice(chunkStart, rowsInChunk).CopyTo(chunk);

            // Transpose (per fastlanes::transpose::transpose):
            // transposed[i] = chunk[transpose(i)]. The decoder runs the
            // inverse via untranspose: output[transpose(i)] = transposed[i].
            for (int i = 0; i < ElementsPerChunk; i++)
                transposed[i] = chunk[TransposeIndex(i)];

            // Bases = first LANES values of transposed.
            transposed.Slice(0, lanes).CopyTo(basesSpan.Slice(c * lanes, lanes));

            // Deltas = Delta::delta::<LANES>(transposed, bases).
            DeltaChunkTyped<T>(
                transposed,
                transposed.Slice(0, lanes),
                deltasSpan.Slice(c * ElementsPerChunk, ElementsPerChunk));
        }

        return (ctor(basesBytes, basesLen), ctor(deltasBytes, paddedLen), paddedLen);
    }

    private static void DeltaChunkTyped<T>(ReadOnlySpan<T> input, ReadOnlySpan<T> baseValues, Span<T> output)
        where T : unmanaged
    {
        if (typeof(T) == typeof(byte))
            Clast.FastLanes.Delta.DeltaChunk<byte>(
                MemoryMarshal.Cast<T, byte>(input),
                MemoryMarshal.Cast<T, byte>(baseValues),
                MemoryMarshal.Cast<T, byte>(output));
        else if (typeof(T) == typeof(ushort))
            Clast.FastLanes.Delta.DeltaChunk<ushort>(
                MemoryMarshal.Cast<T, ushort>(input),
                MemoryMarshal.Cast<T, ushort>(baseValues),
                MemoryMarshal.Cast<T, ushort>(output));
        else if (typeof(T) == typeof(uint))
            Clast.FastLanes.Delta.DeltaChunk<uint>(
                MemoryMarshal.Cast<T, uint>(input),
                MemoryMarshal.Cast<T, uint>(baseValues),
                MemoryMarshal.Cast<T, uint>(output));
        else if (typeof(T) == typeof(ulong))
            Clast.FastLanes.Delta.DeltaChunk<ulong>(
                MemoryMarshal.Cast<T, ulong>(input),
                MemoryMarshal.Cast<T, ulong>(baseValues),
                MemoryMarshal.Cast<T, ulong>(output));
        else
            throw new InvalidOperationException(
                $"fastlanes.delta: DeltaChunk not implemented for {typeof(T)}.");
    }

    private static byte[] SerializeMetadata(int deltasLen)
    {
        // field 1 (deltas_len, varint): tag 0x08.
        Span<byte> tmp = stackalloc byte[1 + 10];
        tmp[0] = 0x08;
        int n = 1 + Varint.WriteUnsigned(tmp.Slice(1), (ulong)deltasLen);
        return tmp.Slice(0, n).ToArray();
    }
}
