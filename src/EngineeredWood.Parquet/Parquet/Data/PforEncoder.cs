// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes INT32 and INT64 values as PFOR (Patched Frame of Reference) pages, choosing per
/// vector whether to pack the values or the differences between them.
/// </summary>
/// <remarks>
/// <para>Two decisions are made per vector. The first — whether to difference — is entirely the
/// writer's: this one runs the cost model both ways and keeps the cheaper, charging the delta
/// side for the start value it has to carry. The second, the bit width, follows from the first:
/// given the residuals, the model picks the width that minimizes packed bits plus exception
/// bits, which is the whole point of PFOR over plain FOR.</para>
/// <para>Specified in apache/parquet-format#617 (<c>PforEncoding.md</c>), which is a work in
/// progress. See <see cref="PforDecoder"/>.</para>
/// </remarks>
internal static class PforEncoder
{
    public const int DefaultLogVectorSize = 10;

    private const int PageHeaderSize = 7;
    private const int Int32VectorInfoSize = 7;
    private const int Int64VectorInfoSize = 11;
    private const int ExceptionPositionSize = 2;
    private const int DeltaFlag = 0x80;

    /// <summary>
    /// What one exception costs beyond its slot in the packed stream: a uint16 position plus a
    /// full-width value. This is the number that makes a narrow width with a few outliers beat a
    /// wide width with none.
    /// </summary>
    private const int Int32ExceptionBits = 16 + 32;

    /// <inheritdoc cref="Int32ExceptionBits"/>
    private const int Int64ExceptionBits = 16 + 64;

    /// <summary>
    /// Buckets the frame search divides a vector's range into, as a power of two so the bucket of
    /// a value is a shift rather than a divide.
    /// </summary>
    private const int FrameSearchBits = 8;

    /// <inheritdoc cref="FrameSearchBits"/>
    private const int FrameSearchBuckets = 1 << FrameSearchBits;

    // Scratch, reused across pages. Encoding a page needs several arrays sized to the vector
    // (differences, residuals, exception positions) plus two small fixed ones, and allocating
    // them per page put about eleven kilobytes of garbage on the write path for every page of
    // every integer column. Thread-static rather than pooled because column chunks are encoded in
    // parallel and each thread encodes one page at a time — the same reason
    // ColumnChunkWriter.t_valuesBuffer is thread-static.

    [ThreadStatic]
    private static int[]? t_int32Differences;

    [ThreadStatic]
    private static uint[]? t_int32Residuals;

    [ThreadStatic]
    private static long[]? t_int64Differences;

    [ThreadStatic]
    private static ulong[]? t_int64Residuals;

    [ThreadStatic]
    private static ushort[]? t_positions;

    /// <summary>Sized for INT64; the INT32 paths clear and read only the low 33 entries.</summary>
    [ThreadStatic]
    private static int[]? t_histogram;

    [ThreadStatic]
    private static int[]? t_buckets;

    private static T[] Ensure<T>(ref T[]? cache, int length)
    {
        if (cache == null || cache.Length < length)
            cache = new T[length];
        return cache;
    }

    /// <summary>Encodes a span of INT32 values into a single PFOR page.</summary>
    public static byte[] EncodeInt32s(ReadOnlySpan<int> values, int logVectorSize = DefaultLogVectorSize)
    {
        int vectorSize = ValidateLogVectorSize(logVectorSize);
        int numVectors = (values.Length + vectorSize - 1) / vectorSize;

        var differences = Ensure(ref t_int32Differences, vectorSize);
        var residuals = Ensure(ref t_int32Residuals, vectorSize);
        var positions = Ensure(ref t_positions, vectorSize);
        var histogram = Ensure(ref t_histogram, 65);
        var buckets = Ensure(ref t_buckets, FrameSearchBuckets + 1);

        var vectorBytes = new byte[numVectors][];
        int totalVectorSize = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int start = v * vectorSize;
            int n = Math.Min(vectorSize, values.Length - start);
            vectorBytes[v] = EncodeInt32Vector(
                values.Slice(start, n), differences, residuals, positions, histogram, buckets);
            totalVectorSize += vectorBytes[v].Length;
        }

        return AssemblePage(numVectors, values.Length, logVectorSize, sizeof(int), vectorBytes, totalVectorSize);
    }

    /// <summary>Encodes a span of INT64 values into a single PFOR page.</summary>
    public static byte[] EncodeInt64s(ReadOnlySpan<long> values, int logVectorSize = DefaultLogVectorSize)
    {
        int vectorSize = ValidateLogVectorSize(logVectorSize);
        int numVectors = (values.Length + vectorSize - 1) / vectorSize;

        var differences = Ensure(ref t_int64Differences, vectorSize);
        var residuals = Ensure(ref t_int64Residuals, vectorSize);
        var positions = Ensure(ref t_positions, vectorSize);
        var histogram = Ensure(ref t_histogram, 65);
        var buckets = Ensure(ref t_buckets, FrameSearchBuckets + 1);

        var vectorBytes = new byte[numVectors][];
        int totalVectorSize = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int start = v * vectorSize;
            int n = Math.Min(vectorSize, values.Length - start);
            vectorBytes[v] = EncodeInt64Vector(
                values.Slice(start, n), differences, residuals, positions, histogram, buckets);
            totalVectorSize += vectorBytes[v].Length;
        }

        return AssemblePage(numVectors, values.Length, logVectorSize, sizeof(long), vectorBytes, totalVectorSize);
    }

    private static int ValidateLogVectorSize(int logVectorSize)
    {
        if (logVectorSize < 3 || logVectorSize > 15)
            throw new ArgumentOutOfRangeException(nameof(logVectorSize), logVectorSize,
                "log_vector_size must be in [3, 15].");

        // The ceiling is what keeps num_exceptions inside its uint16 field and every exception
        // position addressable: 2^15 elements, so at worst 32,768 exceptions at index 32,767.
        return 1 << logVectorSize;
    }

    private static byte[] AssemblePage(
        int numVectors, int numElements, int logVectorSize, int valueByteWidth,
        byte[][] vectorBytes, int totalVectorSize)
    {
        int offsetArraySize = numVectors * 4;
        var page = new byte[PageHeaderSize + offsetArraySize + totalVectorSize];

        page[0] = 0; // packing_mode = FOR + bit-packing
        page[1] = (byte)logVectorSize;
        page[2] = (byte)valueByteWidth;
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(3, 4), numElements);

        // Offsets are measured from the start of the offset array, so the first one points just
        // past the offset array rather than at the start of the page.
        uint runningOffset = (uint)offsetArraySize;
        int writePos = PageHeaderSize + offsetArraySize;
        for (int v = 0; v < numVectors; v++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(PageHeaderSize + v * 4, 4), runningOffset);
            vectorBytes[v].CopyTo(page.AsSpan(writePos));
            writePos += vectorBytes[v].Length;
            runningOffset += (uint)vectorBytes[v].Length;
        }

        return page;
    }

    // ───── INT32 ─────

    private static byte[] EncodeInt32Vector(
        ReadOnlySpan<int> values, int[] differences, uint[] residuals, ushort[] positions,
        int[] histogram, int[] buckets)
    {
        int n = values.Length;

        var plain = PlanInt32(values, histogram, buckets);

        // d[0] is 0 rather than values[0] so that the first element does not force a wide frame
        // or an exception of its own; the decoder adds the start value back to it. The
        // subtraction is modular, and what it produces is an ordinary signed value — negative
        // differences are handled by the frame, not by zigzag.
        differences[0] = 0;
        for (int i = 1; i < n; i++)
            differences[i] = unchecked(values[i] - values[i - 1]);
        var delta = PlanInt32(differences.AsSpan(0, n), histogram, buckets);

        // The plans price only the packed bits and the exceptions, so the start value that a
        // delta vector has to carry is charged here. Ties go to the plain vector: it is the
        // cheaper one to decode, having no prefix sum.
        bool useDelta = delta.CostBits + 32 < plain.CostBits;

        var plan = useDelta ? delta : plain;
        ReadOnlySpan<int> source = useDelta ? differences.AsSpan(0, n) : values;

        uint frame = unchecked((uint)plan.FrameOfReference);

        // At the maximum width every residual fits by definition, and the mask has to be built
        // without shifting by 32: C# masks the shift count to 5 bits, so (1u << 32) is 1 and the
        // mask would come out 0, making every value an exception.
        uint mask = plan.BitWidth == 32 ? uint.MaxValue : (1u << plan.BitWidth) - 1u;

        int numExceptions = 0;
        for (int i = 0; i < n; i++)
        {
            uint residual = unchecked((uint)source[i] - frame);
            if (residual > mask)
            {
                positions[numExceptions++] = (ushort)i;
                residuals[i] = 0;
            }
            else
            {
                residuals[i] = residual;
            }
        }

        int headerSize = Int32VectorInfoSize + (useDelta ? sizeof(int) : 0);
        int packedSize = PackedByteLength(n, plan.BitWidth);
        var vector = new byte[headerSize + packedSize +
            numExceptions * (ExceptionPositionSize + sizeof(int))];

        BinaryPrimitives.WriteInt32LittleEndian(vector.AsSpan(0, 4), (int)plan.FrameOfReference);
        vector[4] = (byte)(plan.BitWidth | (useDelta ? DeltaFlag : 0));
        BinaryPrimitives.WriteUInt16LittleEndian(vector.AsSpan(5, 2), (ushort)numExceptions);

        int at = Int32VectorInfoSize;
        if (useDelta)
        {
            BinaryPrimitives.WriteInt32LittleEndian(vector.AsSpan(at, sizeof(int)), values[0]);
            at += sizeof(int);
        }

        if (plan.BitWidth > 0)
            PackBits(vector.AsSpan(at, packedSize), residuals.AsSpan(0, n), plan.BitWidth);
        at += packedSize;

        for (int j = 0; j < numExceptions; j++)
            BinaryPrimitives.WriteUInt16LittleEndian(
                vector.AsSpan(at + j * ExceptionPositionSize, ExceptionPositionSize), positions[j]);
        at += numExceptions * ExceptionPositionSize;

        // Never residuals: each is the value the packed stream would have carried had it fitted,
        // which in a delta vector is the difference, so that patching before the prefix sum sums
        // it like any other difference.
        for (int j = 0; j < numExceptions; j++)
            BinaryPrimitives.WriteInt32LittleEndian(
                vector.AsSpan(at + j * sizeof(int), sizeof(int)), source[positions[j]]);

        return vector;
    }

    /// <summary>
    /// Costs the vector at the two frames worth considering: the minimum, and the base of the
    /// densest window of values the bucket scan can find.
    /// </summary>
    /// <remarks>
    /// PforEncoding.md says the frame of reference is the minimum, and on a column with a low
    /// sentinel that is the wrong answer by a wide margin: the sentinel becomes the frame, every
    /// ordinary value sits tens of thousands above it, and the width is set by the gap rather than
    /// by the cluster. The spec's own worked example for that shape quotes a width that only a
    /// frame above the minimum can reach. So the minimum is a candidate here, not the rule --
    /// which is also what apache/arrow-rs#10977 does.
    /// </remarks>
    private static VectorPlan PlanInt32(ReadOnlySpan<int> source, int[] histogram, int[] buckets)
    {
        int n = source.Length;

        int min = source[0];
        int max = source[0];
        for (int i = 1; i < n; i++)
        {
            if (source[i] < min) min = source[i];
            if (source[i] > max) max = source[i];
        }

        uint range = unchecked((uint)max - (uint)min);
        if (range == 0)
            return new VectorPlan(min, 0, 0, 0);

        int shift = Math.Max(0, BitsRequired(range) - FrameSearchBits);

        // One walk fills both: the bit-width histogram prices the minimum as a frame, the bucket
        // counts price every other frame.
        Array.Clear(histogram, 0, 33);
        Array.Clear(buckets, 0, FrameSearchBuckets + 1);
        uint minFrame = unchecked((uint)min);
        for (int i = 0; i < n; i++)
        {
            uint offset = unchecked((uint)source[i] - minFrame);
            histogram[BitsRequired(offset)]++;
            buckets[offset >> shift]++;
        }

        var (bitWidth, numExceptions, costBits) = ChooseWidth(histogram, 32, n, Int32ExceptionBits);
        var best = new VectorPlan(min, bitWidth, numExceptions, costBits);

        // Already at the floor: a frame above the minimum buys a narrower width at the price of
        // patches, and there is no width below zero to buy.
        if (bitWidth == 0)
            return best;

        int numBuckets = (int)(range >> shift) + 1;
        if (!TryFindDenseWindow(buckets, numBuckets, shift, n, 32, Int32ExceptionBits, costBits,
                out int windowStart, out int windowEnd))
            return best;

        // Lower the frame from the winning window's bucket boundary onto the smallest value the
        // window actually covers. Boundaries stand 2^shift apart, which on a wide column is
        // thousands, and a cluster sitting just above one would pay those bits for nothing.
        uint windowLo = (uint)windowStart << shift;
        bool boundedAbove = windowEnd < numBuckets;
        uint windowHi = boundedAbove ? (uint)windowEnd << shift : 0;

        uint frameOffset = 0;
        bool covered = false;
        for (int i = 0; i < n; i++)
        {
            uint offset = unchecked((uint)source[i] - minFrame);
            if (offset < windowLo || (boundedAbove && offset >= windowHi))
                continue;
            if (!covered || offset < frameOffset)
            {
                frameOffset = offset;
                covered = true;
            }
        }

        if (!covered || frameOffset == 0)
            return best;

        // The scan works at bucket granularity and cannot see a window narrower than one bucket,
        // which is where the answers worth having tend to be. This pass is not bookkeeping: it is
        // where the width and the exception count are actually decided.
        int candidate = unchecked((int)(minFrame + frameOffset));
        Array.Clear(histogram, 0, 33);
        for (int i = 0; i < n; i++)
            histogram[BitsRequired(unchecked((uint)source[i] - (uint)candidate))]++;

        var (candidateWidth, candidateExceptions, candidateCost) =
            ChooseWidth(histogram, 32, n, Int32ExceptionBits);

        return candidateCost < costBits
            ? new VectorPlan(candidate, candidateWidth, candidateExceptions, candidateCost)
            : best;
    }

    // ───── INT64 ─────

    private static byte[] EncodeInt64Vector(
        ReadOnlySpan<long> values, long[] differences, ulong[] residuals, ushort[] positions,
        int[] histogram, int[] buckets)
    {
        int n = values.Length;

        var plain = PlanInt64(values, histogram, buckets);

        // See EncodeInt32Vector.
        differences[0] = 0;
        for (int i = 1; i < n; i++)
            differences[i] = unchecked(values[i] - values[i - 1]);
        var delta = PlanInt64(differences.AsSpan(0, n), histogram, buckets);

        bool useDelta = delta.CostBits + 64 < plain.CostBits;

        var plan = useDelta ? delta : plain;
        ReadOnlySpan<long> source = useDelta ? differences.AsSpan(0, n) : values;

        ulong frame = unchecked((ulong)plan.FrameOfReference);

        // See EncodeInt32Vector: (1UL << 64) is 1, not 0, so width 64 cannot build its mask by
        // shifting either.
        ulong mask = plan.BitWidth == 64 ? ulong.MaxValue : (1UL << plan.BitWidth) - 1UL;

        int numExceptions = 0;
        for (int i = 0; i < n; i++)
        {
            ulong residual = unchecked((ulong)source[i] - frame);
            if (residual > mask)
            {
                positions[numExceptions++] = (ushort)i;
                residuals[i] = 0;
            }
            else
            {
                residuals[i] = residual;
            }
        }

        int headerSize = Int64VectorInfoSize + (useDelta ? sizeof(long) : 0);
        int packedSize = PackedByteLength(n, plan.BitWidth);
        var vector = new byte[headerSize + packedSize +
            numExceptions * (ExceptionPositionSize + sizeof(long))];

        BinaryPrimitives.WriteInt64LittleEndian(vector.AsSpan(0, 8), plan.FrameOfReference);
        vector[8] = (byte)(plan.BitWidth | (useDelta ? DeltaFlag : 0));
        BinaryPrimitives.WriteUInt16LittleEndian(vector.AsSpan(9, 2), (ushort)numExceptions);

        int at = Int64VectorInfoSize;
        if (useDelta)
        {
            BinaryPrimitives.WriteInt64LittleEndian(vector.AsSpan(at, sizeof(long)), values[0]);
            at += sizeof(long);
        }

        if (plan.BitWidth > 0)
            PackBits(vector.AsSpan(at, packedSize), residuals.AsSpan(0, n), plan.BitWidth);
        at += packedSize;

        for (int j = 0; j < numExceptions; j++)
            BinaryPrimitives.WriteUInt16LittleEndian(
                vector.AsSpan(at + j * ExceptionPositionSize, ExceptionPositionSize), positions[j]);
        at += numExceptions * ExceptionPositionSize;

        for (int j = 0; j < numExceptions; j++)
            BinaryPrimitives.WriteInt64LittleEndian(
                vector.AsSpan(at + j * sizeof(long), sizeof(long)), source[positions[j]]);

        return vector;
    }

    /// <inheritdoc cref="PlanInt32"/>
    private static VectorPlan PlanInt64(ReadOnlySpan<long> source, int[] histogram, int[] buckets)
    {
        int n = source.Length;

        long min = source[0];
        long max = source[0];
        for (int i = 1; i < n; i++)
        {
            if (source[i] < min) min = source[i];
            if (source[i] > max) max = source[i];
        }

        ulong range = unchecked((ulong)max - (ulong)min);
        if (range == 0)
            return new VectorPlan(min, 0, 0, 0);

        int shift = Math.Max(0, BitsRequired(range) - FrameSearchBits);

        Array.Clear(histogram, 0, 65);
        Array.Clear(buckets, 0, FrameSearchBuckets + 1);
        ulong minFrame = unchecked((ulong)min);
        for (int i = 0; i < n; i++)
        {
            ulong offset = unchecked((ulong)source[i] - minFrame);
            histogram[BitsRequired(offset)]++;
            buckets[(int)(offset >> shift)]++;
        }

        var (bitWidth, numExceptions, costBits) = ChooseWidth(histogram, 64, n, Int64ExceptionBits);
        var best = new VectorPlan(min, bitWidth, numExceptions, costBits);

        if (bitWidth == 0)
            return best;

        int numBuckets = (int)(range >> shift) + 1;
        if (!TryFindDenseWindow(buckets, numBuckets, shift, n, 64, Int64ExceptionBits, costBits,
                out int windowStart, out int windowEnd))
            return best;

        ulong windowLo = (ulong)windowStart << shift;
        bool boundedAbove = windowEnd < numBuckets;
        ulong windowHi = boundedAbove ? (ulong)windowEnd << shift : 0;

        ulong frameOffset = 0;
        bool covered = false;
        for (int i = 0; i < n; i++)
        {
            ulong offset = unchecked((ulong)source[i] - minFrame);
            if (offset < windowLo || (boundedAbove && offset >= windowHi))
                continue;
            if (!covered || offset < frameOffset)
            {
                frameOffset = offset;
                covered = true;
            }
        }

        if (!covered || frameOffset == 0)
            return best;

        long candidate = unchecked((long)(minFrame + frameOffset));
        Array.Clear(histogram, 0, 65);
        for (int i = 0; i < n; i++)
            histogram[BitsRequired(unchecked((ulong)source[i] - (ulong)candidate))]++;

        var (candidateWidth, candidateExceptions, candidateCost) =
            ChooseWidth(histogram, 64, n, Int64ExceptionBits);

        return candidateCost < costBits
            ? new VectorPlan(candidate, candidateWidth, candidateExceptions, candidateCost)
            : best;
    }

    /// <summary>
    /// Finds the densest window of buckets, when one is cheaper than packing from the minimum.
    /// </summary>
    /// <remarks>
    /// <para>Approximate by design. An exact answer needs the values sorted; instead the range is
    /// bucketed and a window is slid over the bucket counts for each candidate width. Only whole
    /// buckets count as covered, so the exception estimate is an upper bound, never optimistic,
    /// and the width is an upper bound too. What comes out is a frame, not a plan; the caller
    /// costs it exactly.</para>
    /// <para>Seeded with the incumbent's cost, so a window only registers if it beats the minimum
    /// as a frame, and everything after it is skipped entirely on a column a frame cannot help --
    /// which is most of them. The seeding errs in the safe direction: the scan over-counts
    /// exceptions, so it can decline a frame whose exact cost would have won, but it cannot
    /// accept one that loses.</para>
    /// </remarks>
    private static bool TryFindDenseWindow(
        int[] buckets, int numBuckets, int shift, int n, int maxBits, int exceptionBits,
        long incumbentCost, out int windowStart, out int windowEnd)
    {
        Span<int> prefix = stackalloc int[FrameSearchBuckets + 2];
        prefix[0] = 0;
        for (int b = 0; b < numBuckets; b++)
            prefix[b + 1] = prefix[b] + buckets[b];

        long scanCost = incumbentCost;
        windowStart = -1;
        windowEnd = 0;

        // A width below the bucket size cannot be resolved at this granularity, and a window
        // spanning every bucket has no exceptions left to remove, so this covers only the
        // FrameSearchBits-odd widths in between. numBuckets never exceeds FrameSearchBuckets, so
        // the loop always breaks with (w - shift) at most FrameSearchBits, which is what keeps the
        // shift below well away from a count of 64.
        for (int w = shift; w <= maxBits; w++)
        {
            long wholeBuckets = 1L << (w - shift);
            int k = (int)Math.Min(wholeBuckets, numBuckets);

            for (int s = 0; s < numBuckets; s++)
            {
                int end = Math.Min(s + k, numBuckets);
                long exceptions = n - (prefix[end] - prefix[s]);
                long cost = (long)n * w + exceptions * exceptionBits;
                if (cost < scanCost)
                {
                    scanCost = cost;
                    windowStart = s;
                    windowEnd = end;
                }
            }

            if (k >= numBuckets)
                break;
        }

        return windowStart >= 0;
    }

    // ───── Cost model ─────

    /// <summary>
    /// Picks the width that minimizes <c>n * width + exceptions(width) * exceptionBits</c>.
    /// </summary>
    /// <remarks>
    /// Walking down from the maximum makes the exception count accumulate as each bucket is
    /// passed, so the whole search is one pass over the histogram rather than one pass over the
    /// values per candidate width. Ties keep the wider width, which has fewer exceptions and so
    /// decodes faster for the same number of bytes.
    /// </remarks>
    private static (int BitWidth, int NumExceptions, long CostBits) ChooseWidth(
        int[] histogram, int maxBits, int n, int exceptionBits)
    {
        int exceptions = 0;
        int bestWidth = maxBits;
        int bestExceptions = 0;
        long bestCost = long.MaxValue;

        for (int b = maxBits; b >= 0; b--)
        {
            // On entry `exceptions` is the number of residuals needing more than b bits, which
            // is exactly the number that would be patched at this width.
            long cost = (long)n * b + (long)exceptions * exceptionBits;
            if (cost < bestCost)
            {
                bestCost = cost;
                bestWidth = b;
                bestExceptions = exceptions;
            }

            exceptions += histogram[b];
        }

        return (bestWidth, bestExceptions, bestCost);
    }

    private readonly struct VectorPlan(long frameOfReference, int bitWidth, int numExceptions, long costBits)
    {
        public long FrameOfReference { get; } = frameOfReference;

        public int BitWidth { get; } = bitWidth;

        public int NumExceptions { get; } = numExceptions;

        /// <summary>Packed bits plus exception bits. Excludes the vector header and start value.</summary>
        public long CostBits { get; } = costBits;
    }

    // ───── Bit packing ─────

    private static int PackedByteLength(int count, int bitWidth) =>
        (int)(((long)count * bitWidth + 7) / 8);

    /// <summary>
    /// Packs residuals LSB-first, <paramref name="bitWidth"/> bits each, in the same order as
    /// the RLE/bit-packing hybrid encoding.
    /// </summary>
    /// <remarks>
    /// The accumulator is a register that whole words are flushed out of, rather than a
    /// read-modify-write into the destination; that shape is
    /// <see cref="AlpEncoder.PackBits(Span{byte}, ReadOnlySpan{long}, long, int)"/>'s, where it
    /// was measured. This takes residuals that are already frame-subtracted, unlike ALP's, for
    /// two reasons: an INT64 residual can exceed <see cref="long"/>'s range, and exception slots
    /// have to be zero <em>after</em> the subtraction rather than before it.
    /// </remarks>
    private static void PackBits(Span<byte> dest, ReadOnlySpan<uint> residuals, int bitWidth)
    {
        ulong accumulator = 0;
        int held = 0;
        int offset = 0;

        for (int i = 0; i < residuals.Length; i++)
        {
            ulong residual = residuals[i];
            accumulator |= residual << held;
            held += bitWidth;

            if (held >= 64)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(offset, 8), accumulator);
                offset += 8;
                held -= 64;
                accumulator = held == 0 ? 0UL : residual >> (bitWidth - held);
            }
        }

        WriteTail(dest, offset, accumulator, held);
    }

    /// <inheritdoc cref="PackBits(Span{byte}, ReadOnlySpan{uint}, int)"/>
    private static void PackBits(Span<byte> dest, ReadOnlySpan<ulong> residuals, int bitWidth)
    {
        ulong accumulator = 0;
        int held = 0;
        int offset = 0;

        for (int i = 0; i < residuals.Length; i++)
        {
            ulong residual = residuals[i];
            accumulator |= residual << held;
            held += bitWidth;

            if (held >= 64)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(offset, 8), accumulator);
                offset += 8;
                held -= 64;
                accumulator = held == 0 ? 0UL : residual >> (bitWidth - held);
            }
        }

        WriteTail(dest, offset, accumulator, held);
    }

    /// <summary>
    /// Writes the bits still in the accumulator, a byte at a time. The whole words above always
    /// fit, but the destination is sized to the exact packed length, so the last partial word
    /// cannot be written eight bytes at once.
    /// </summary>
    private static void WriteTail(Span<byte> dest, int offset, ulong accumulator, int held)
    {
        for (int k = 0; held > 0; k++, held -= 8)
            dest[offset + k] = (byte)(accumulator >> (k * 8));
    }

    private static int BitsRequired(uint value) =>
#if NET8_0_OR_GREATER
        32 - System.Numerics.BitOperations.LeadingZeroCount(value);
#else
        32 - BitPolyfills.LeadingZeroCount(value);
#endif

    private static int BitsRequired(ulong value) =>
#if NET8_0_OR_GREATER
        64 - System.Numerics.BitOperations.LeadingZeroCount(value);
#else
        64 - BitPolyfills.LeadingZeroCount(value);
#endif
}
