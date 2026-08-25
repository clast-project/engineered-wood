// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes ALP (Adaptive Lossless floating-Point, encoding number 10) data pages
/// for FLOAT and DOUBLE columns. See parquet-format Encodings.md §ALP for the layout.
/// </summary>
internal static class AlpEncoder
{
    /// <summary>log2 of the default vector size (1024 elements).</summary>
    public const int DefaultLogVectorSize = 10;

    private const int MaxFloatExponent = 10;
    private const int MaxDoubleExponent = 18;

    /// <summary>Values sampled out of each vector when scoring candidate (exponent, factor) pairs.</summary>
    private const int SamplesPerVector = 32;

    /// <summary>How many of a page's vectors are sampled to build the page-level shortlist.</summary>
    private const int SampledVectorsPerPage = 8;

    /// <summary>How many candidate combinations survive the page-level pass into the per-vector one.</summary>
    private const int ShortlistSize = 5;

    /// <summary>What one exception costs on the wire: the raw value plus its uint16 position.</summary>
    private const int DoubleExceptionBits = 64 + 16;

    /// <summary>What one exception costs on the wire: the raw value plus its uint16 position.</summary>
    private const int FloatExceptionBits = 32 + 16;

    private const float FloatMagic = (1u << 22) + (1u << 23);
    private const double DoubleMagic = (1L << 51) + (1L << 52);

    private static readonly float[] FloatPow10 =
    [
        1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f,
    ];

    private static readonly float[] FloatNegPow10 =
    [
        1e0f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f,
    ];

    /// <summary>Encodes a span of DOUBLE values into a single ALP-encoded page byte sequence.</summary>
    public static byte[] EncodeDoubles(ReadOnlySpan<double> values, int logVectorSize = DefaultLogVectorSize)
    {
        if (logVectorSize < 3 || logVectorSize > 15)
            throw new ArgumentOutOfRangeException(nameof(logVectorSize), logVectorSize,
                "log_vector_size must be in [3, 15].");

        int vectorSize = 1 << logVectorSize;
        int numVectors = (values.Length + vectorSize - 1) / vectorSize;

        var shortlist = BuildDoubleShortlist(values, vectorSize, numVectors);

        var vectorBytes = new byte[numVectors][];
        int totalVectorSize = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int start = v * vectorSize;
            int n = Math.Min(vectorSize, values.Length - start);
            vectorBytes[v] = EncodeDoubleVector(values.Slice(start, n), shortlist);
            totalVectorSize += vectorBytes[v].Length;
        }

        return AssemblePage(numVectors, values.Length, logVectorSize, vectorBytes, totalVectorSize);
    }

    /// <summary>Encodes a span of FLOAT values into a single ALP-encoded page byte sequence.</summary>
    public static byte[] EncodeFloats(ReadOnlySpan<float> values, int logVectorSize = DefaultLogVectorSize)
    {
        if (logVectorSize < 3 || logVectorSize > 15)
            throw new ArgumentOutOfRangeException(nameof(logVectorSize), logVectorSize,
                "log_vector_size must be in [3, 15].");

        int vectorSize = 1 << logVectorSize;
        int numVectors = (values.Length + vectorSize - 1) / vectorSize;

        var shortlist = BuildFloatShortlist(values, vectorSize, numVectors);

        var vectorBytes = new byte[numVectors][];
        int totalVectorSize = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int start = v * vectorSize;
            int n = Math.Min(vectorSize, values.Length - start);
            vectorBytes[v] = EncodeFloatVector(values.Slice(start, n), shortlist);
            totalVectorSize += vectorBytes[v].Length;
        }

        return AssemblePage(numVectors, values.Length, logVectorSize, vectorBytes, totalVectorSize);
    }

    private static byte[] AssemblePage(
        int numVectors, int numElements, int logVectorSize, byte[][] vectorBytes, int totalVectorSize)
    {
        int headerSize = 7;
        int offsetArraySize = numVectors * 4;
        int totalSize = headerSize + offsetArraySize + totalVectorSize;

        var page = new byte[totalSize];
        page[0] = 0; // compression_mode = ALP
        page[1] = 0; // integer_encoding = FOR + bit-packing
        page[2] = (byte)logVectorSize;
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(3, 4), numElements);

        // Offsets are measured from the start of the offset array; the first offset
        // points just past the offset array.
        uint runningOffset = (uint)offsetArraySize;
        int writePos = headerSize + offsetArraySize;
        for (int v = 0; v < numVectors; v++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(headerSize + v * 4, 4), runningOffset);
            vectorBytes[v].CopyTo(page.AsSpan(writePos));
            writePos += vectorBytes[v].Length;
            runningOffset += (uint)vectorBytes[v].Length;
        }

        return page;
    }

    // ───── DOUBLE vector encoding ─────

    private static byte[] EncodeDoubleVector(
        ReadOnlySpan<double> values, (byte Exponent, byte Factor)[] shortlist)
    {
        var (exponent, factor) = ChooseDoubleCombination(values, shortlist, exact: false);
        var vector = EncodeDoubleVectorWith(values, exponent, factor);

        // A 32-value sample can badly misjudge a vector where nearly everything ends up an
        // exception — raw radian coordinates are the canonical case — and land on a combination
        // that costs more than storing the doubles outright. That is rare enough to detect after
        // the fact: when it happens, re-score the shortlist against every value and keep whichever
        // of the two is smaller. Data that ALP suits never takes this path.
        if (values.Length == 0 || (long)vector.Length * 8 <= (long)values.Length * 64)
            return vector;

        var (exactExponent, exactFactor) = ChooseDoubleCombination(values, shortlist, exact: true);
        if (exactExponent == exponent && exactFactor == factor)
            return vector;

        var retry = EncodeDoubleVectorWith(values, exactExponent, exactFactor);
        return retry.Length < vector.Length ? retry : vector;
    }

    private static byte[] EncodeDoubleVectorWith(ReadOnlySpan<double> values, int exponent, int factor)
    {
        int n = values.Length;

        // Encode each value, recording exceptions and substituting placeholders so the
        // FOR range stays tight.
        long[] encoded = new long[n];
        EncodeDoubleVectorWithParams(values, exponent, factor, encoded,
            out int[] exceptionPositions, out double[] exceptionValues);

        long min = encoded.Length > 0 ? encoded[0] : 0;
        long max = min;
        for (int i = 1; i < n; i++)
        {
            long e = encoded[i];
            if (e < min) min = e;
            if (e > max) max = e;
        }

        ulong range = (n == 0) ? 0UL : unchecked((ulong)(max - min));
        int bitWidth = BitsRequired64(range);

        int packedSize = (n * bitWidth + 7) / 8;
        int totalSize = 4 + 9 + packedSize + exceptionPositions.Length * 2 + exceptionValues.Length * 8;
        var buf = new byte[totalSize];

        buf[0] = (byte)exponent;
        buf[1] = (byte)factor;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2, 2), checked((ushort)exceptionPositions.Length));
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(4, 8), min);
        buf[12] = (byte)bitWidth;

        if (bitWidth > 0)
            PackBits(buf.AsSpan(13, packedSize), encoded.AsSpan(0, n), min, bitWidth);

        int posOffset = 13 + packedSize;
        int valOffset = posOffset + exceptionPositions.Length * 2;
        for (int i = 0; i < exceptionPositions.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(
                buf.AsSpan(posOffset + i * 2, 2), checked((ushort)exceptionPositions[i]));
        for (int i = 0; i < exceptionValues.Length; i++)
        {
            long bits = BitConverter.DoubleToInt64Bits(exceptionValues[i]);
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(valOffset + i * 8, 8), bits);
        }

        return buf;
    }

    /// <summary>
    /// Builds the page-level shortlist of candidate (exponent, factor) pairs, ALP's "first level"
    /// sampling: a handful of the page's vectors are sampled, every combination the Parquet spec
    /// allows is scored on those samples by the size it would produce, and the cheapest few go
    /// forward. Only those survivors are re-scored per vector, which is what keeps the search off
    /// the critical path.
    /// </summary>
    private static (byte Exponent, byte Factor)[] BuildDoubleShortlist(
        ReadOnlySpan<double> values, int vectorSize, int numVectors)
    {
        const int Combinations = (MaxDoubleExponent + 1) * (MaxDoubleExponent + 2) / 2;

        Span<long> totals = stackalloc long[Combinations];
        totals.Clear();
        Span<double> sample = stackalloc double[SamplesPerVector];

        // Spread the budget over the page rather than striding by a floor-divided step, which
        // overshoots whenever the vector count is not a multiple of it — a 15-vector page sampled
        // all 15, and this pass is the most expensive thing the encoder does on a short page.
        int sampled = Math.Min(SampledVectorsPerPage, numVectors);
        for (int s = 0; s < sampled; s++)
        {
            int v = (int)((long)s * numVectors / sampled);
            int start = v * vectorSize;
            int n = Math.Min(vectorSize, values.Length - start);
            int taken = Sample(values.Slice(start, n), sample);

            int c = 0;
            for (int e = 0; e <= MaxDoubleExponent; e++)
                for (int f = 0; f <= e; f++, c++)
                    totals[c] += EstimateDoubleBits(sample.Slice(0, taken), e, f);
        }

        return TakeCheapest(totals, MaxDoubleExponent);
    }

    /// <summary>
    /// ALP's "second level" pass: scores only the shortlisted combinations against this vector —
    /// against a sample of it normally, against every value when <paramref name="exact"/> — and
    /// keeps the cheapest. The choice is an optimization, never a correctness requirement:
    /// whatever it picks, values that do not round-trip are stored as exceptions.
    /// </summary>
    private static (int Exponent, int Factor) ChooseDoubleCombination(
        ReadOnlySpan<double> values, (byte Exponent, byte Factor)[] shortlist, bool exact)
    {
        Span<double> sample = stackalloc double[SamplesPerVector];
        ReadOnlySpan<double> scored = exact
            ? values
            : sample.Slice(0, Sample(values, sample));

        long bestBits = long.MaxValue;
        (int Exponent, int Factor) best = (shortlist[0].Exponent, shortlist[0].Factor);

        foreach (var candidate in shortlist)
        {
            long bits = EstimateDoubleBits(scored, candidate.Exponent, candidate.Factor);
            if (bits < bestBits)
            {
                bestBits = bits;
                best = (candidate.Exponent, candidate.Factor);
            }
        }

        return best;
    }

    /// <summary>
    /// The bits one vector of <paramref name="sample"/> would occupy under this combination: every
    /// value bit-packed at the frame-of-reference width, plus the full width of each exception.
    /// Scoring by size rather than by exception count alone is the whole point — a combination that
    /// round-trips everything but needs 55-bit integers loses to one that takes a few exceptions
    /// and needs 11.
    /// </summary>
    private static long EstimateDoubleBits(ReadOnlySpan<double> sample, int exponent, int factor)
    {
        long min = long.MaxValue;
        long max = long.MinValue;
        int exceptions = 0;

        for (int i = 0; i < sample.Length; i++)
        {
            double v = sample[i];

            if (!IsFiniteDouble(v) || IsNegativeZero(v))
            {
                exceptions++;
                continue;
            }

            long encoded = Clast.Alp.AlpEncoder.EncodeValue(v, exponent, factor);
            if (Clast.Alp.AlpDecoder.DecodeValue(encoded, exponent, factor) != v)
            {
                exceptions++;
                continue;
            }

            if (encoded < min) min = encoded;
            if (encoded > max) max = encoded;
        }

        int bitWidth = exceptions == sample.Length
            ? 0
            : BitsRequired64(unchecked((ulong)(max - min)));

        return ((long)sample.Length * bitWidth) + ((long)exceptions * DoubleExceptionBits);
    }

    /// <summary>
    /// Encodes a vector under the chosen combination, substituting the first value that did encode
    /// for every one that did not so the frame-of-reference range stays tight, and handing back the
    /// exceptions to be stored verbatim.
    /// </summary>
    private static void EncodeDoubleVectorWithParams(
        ReadOnlySpan<double> values, int exponent, int factor,
        Span<long> destination,
        out int[] exceptionPositions, out double[] exceptionValues)
    {
        long fillValue = 0;
        bool hasFill = false;
        List<int>? exceptions = null;

        for (int i = 0; i < values.Length; i++)
        {
            double v = values[i];
            bool special = !IsFiniteDouble(v) || IsNegativeZero(v);
            long encoded = 0;

            if (!special)
            {
                encoded = Clast.Alp.AlpEncoder.EncodeValue(v, exponent, factor);
                special = Clast.Alp.AlpDecoder.DecodeValue(encoded, exponent, factor) != v;
            }

            if (special)
            {
                (exceptions ??= new List<int>()).Add(i);
                continue;
            }

            destination[i] = encoded;
            if (!hasFill)
            {
                fillValue = encoded;
                hasFill = true;
            }
        }

        if (exceptions is null)
        {
            exceptionPositions = [];
            exceptionValues = [];
            return;
        }

        exceptionPositions = exceptions.ToArray();
        exceptionValues = new double[exceptions.Count];
        for (int j = 0; j < exceptions.Count; j++)
        {
            int i = exceptions[j];
            exceptionValues[j] = values[i];
            destination[i] = fillValue;
        }
    }

    private static bool IsFiniteDouble(double v) => !double.IsNaN(v) && !double.IsInfinity(v);

    // ───── FLOAT vector encoding ─────

    private static byte[] EncodeFloatVector(
        ReadOnlySpan<float> values, (byte Exponent, byte Factor)[] shortlist)
    {
        var (exponent, factor) = ChooseFloatCombination(values, shortlist, exact: false);
        var vector = EncodeFloatVectorWith(values, exponent, factor);

        // See EncodeDoubleVector for why the sampled choice is double-checked here.
        if (values.Length == 0 || (long)vector.Length * 8 <= (long)values.Length * 32)
            return vector;

        var (exactExponent, exactFactor) = ChooseFloatCombination(values, shortlist, exact: true);
        if (exactExponent == exponent && exactFactor == factor)
            return vector;

        var retry = EncodeFloatVectorWith(values, exactExponent, exactFactor);
        return retry.Length < vector.Length ? retry : vector;
    }

    private static byte[] EncodeFloatVectorWith(ReadOnlySpan<float> values, int exponent, int factor)
    {
        int n = values.Length;

        int[] encoded = new int[n];
        EncodeFloatVectorWithParams(values, exponent, factor, encoded,
            out int[] exceptionPositions, out float[] exceptionValues);

        long min = n > 0 ? encoded[0] : 0;
        long max = min;
        for (int i = 1; i < n; i++)
        {
            long e = encoded[i];
            if (e < min) min = e;
            if (e > max) max = e;
        }

        ulong range = (n == 0) ? 0UL : unchecked((ulong)(max - min));
        int bitWidth = BitsRequired32(range);

        int packedSize = (n * bitWidth + 7) / 8;
        int totalSize = 4 + 5 + packedSize + exceptionPositions.Length * 2 + exceptionValues.Length * 4;
        var buf = new byte[totalSize];

        buf[0] = (byte)exponent;
        buf[1] = (byte)factor;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2, 2), checked((ushort)exceptionPositions.Length));
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(4, 4), checked((int)min));
        buf[8] = (byte)bitWidth;

        if (bitWidth > 0)
            PackBits(buf.AsSpan(9, packedSize), encoded.AsSpan(0, n), min, bitWidth);

        int posOffset = 9 + packedSize;
        int valOffset = posOffset + exceptionPositions.Length * 2;
        for (int i = 0; i < exceptionPositions.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(
                buf.AsSpan(posOffset + i * 2, 2), checked((ushort)exceptionPositions[i]));
        for (int i = 0; i < exceptionValues.Length; i++)
        {
            byte[] fb = BitConverter.GetBytes(exceptionValues[i]);
            fb.CopyTo(buf.AsSpan(valOffset + i * 4, 4));
        }

        return buf;
    }

    /// <summary>Page-level shortlist for FLOAT columns. See <see cref="BuildDoubleShortlist"/>.</summary>
    private static (byte Exponent, byte Factor)[] BuildFloatShortlist(
        ReadOnlySpan<float> values, int vectorSize, int numVectors)
    {
        const int Combinations = (MaxFloatExponent + 1) * (MaxFloatExponent + 2) / 2;

        Span<long> totals = stackalloc long[Combinations];
        totals.Clear();
        Span<float> sample = stackalloc float[SamplesPerVector];

        // See BuildDoubleShortlist for why this spreads a fixed budget rather than striding.
        int sampled = Math.Min(SampledVectorsPerPage, numVectors);
        for (int s = 0; s < sampled; s++)
        {
            int v = (int)((long)s * numVectors / sampled);
            int start = v * vectorSize;
            int n = Math.Min(vectorSize, values.Length - start);
            int taken = Sample(values.Slice(start, n), sample);

            int c = 0;
            for (int e = 0; e <= MaxFloatExponent; e++)
                for (int f = 0; f <= e; f++, c++)
                    totals[c] += EstimateFloatBits(sample.Slice(0, taken), e, f);
        }

        return TakeCheapest(totals, MaxFloatExponent);
    }

    /// <summary>Per-vector choice for FLOAT columns. See <see cref="ChooseDoubleCombination"/>.</summary>
    private static (int Exponent, int Factor) ChooseFloatCombination(
        ReadOnlySpan<float> values, (byte Exponent, byte Factor)[] shortlist, bool exact)
    {
        Span<float> sample = stackalloc float[SamplesPerVector];
        ReadOnlySpan<float> scored = exact
            ? values
            : sample.Slice(0, Sample(values, sample));

        long bestBits = long.MaxValue;
        (int Exponent, int Factor) best = (shortlist[0].Exponent, shortlist[0].Factor);

        foreach (var candidate in shortlist)
        {
            long bits = EstimateFloatBits(scored, candidate.Exponent, candidate.Factor);
            if (bits < bestBits)
            {
                bestBits = bits;
                best = (candidate.Exponent, candidate.Factor);
            }
        }

        return best;
    }

    /// <summary>The bits one vector of FLOAT samples would occupy. See <see cref="EstimateDoubleBits"/>.</summary>
    private static long EstimateFloatBits(ReadOnlySpan<float> sample, int exponent, int factor)
    {
        long min = long.MaxValue;
        long max = long.MinValue;
        int exceptions = 0;

        for (int i = 0; i < sample.Length; i++)
        {
            if (!TryEncodeFloat(sample[i], exponent, factor, out int encoded))
            {
                exceptions++;
                continue;
            }

            if (encoded < min) min = encoded;
            if (encoded > max) max = encoded;
        }

        int bitWidth = exceptions == sample.Length
            ? 0
            : BitsRequired32(unchecked((ulong)(max - min)));

        return ((long)sample.Length * bitWidth) + ((long)exceptions * FloatExceptionBits);
    }

    /// <summary>
    /// Encodes one FLOAT under a candidate combination, reporting <see langword="false"/> when the
    /// value has to be stored as an exception instead: non-finite, negative zero, out of int32
    /// range, or simply not recoverable by the inverse transform.
    /// </summary>
    private static bool TryEncodeFloat(float value, int exponent, int factor, out int encoded)
    {
        encoded = 0;

        if (!IsFiniteFloat(value) || IsNegativeZero(value))
            return false;

        float scaled = value * FloatPow10[exponent] * FloatNegPow10[factor];
        if (scaled <= int.MinValue + 512 || scaled >= int.MaxValue - 512)
            return false;

        int candidate = (int)(scaled + FloatMagic - FloatMagic);
        if (candidate * FloatPow10[factor] * FloatNegPow10[exponent] != value)
            return false;

        encoded = candidate;
        return true;
    }

    /// <summary>Takes up to <see cref="SamplesPerVector"/> evenly spaced values out of a vector.</summary>
    private static int Sample<T>(ReadOnlySpan<T> values, Span<T> destination)
    {
        if (values.Length <= destination.Length)
        {
            values.CopyTo(destination);
            return values.Length;
        }

        int stride = values.Length / destination.Length;
        for (int i = 0; i < destination.Length; i++)
            destination[i] = values[i * stride];

        return destination.Length;
    }

    /// <summary>
    /// Returns the <see cref="ShortlistSize"/> cheapest combinations, mapping each scoreboard slot
    /// back to the (exponent, factor) pair that filled it — the pairs are enumerated with f less
    /// than or equal to e, so slot c belongs to the exponent whose triangular number it falls under.
    /// </summary>
    private static (byte Exponent, byte Factor)[] TakeCheapest(Span<long> totals, int maxExponent)
    {
        var result = new (byte Exponent, byte Factor)[Math.Min(ShortlistSize, totals.Length)];

        for (int slot = 0; slot < result.Length; slot++)
        {
            int cheapest = 0;
            for (int c = 1; c < totals.Length; c++)
            {
                if (totals[c] < totals[cheapest])
                    cheapest = c;
            }

            int exponent = 0;
            while (exponent < maxExponent && (exponent + 1) * (exponent + 2) / 2 <= cheapest)
                exponent++;

            result[slot] = ((byte)exponent, (byte)(cheapest - (exponent * (exponent + 1) / 2)));
            totals[cheapest] = long.MaxValue;
        }

        return result;
    }

    /// <summary>FLOAT counterpart of <see cref="EncodeDoubleVectorWithParams"/>.</summary>
    private static void EncodeFloatVectorWithParams(
        ReadOnlySpan<float> values, int exponent, int factor,
        Span<int> destination,
        out int[] exceptionPositions, out float[] exceptionValues)
    {
        int fillValue = 0;
        bool hasFill = false;
        List<int>? exceptions = null;

        for (int i = 0; i < values.Length; i++)
        {
            if (!TryEncodeFloat(values[i], exponent, factor, out int encoded))
            {
                (exceptions ??= new List<int>()).Add(i);
                continue;
            }

            destination[i] = encoded;
            if (!hasFill)
            {
                fillValue = encoded;
                hasFill = true;
            }
        }

        if (exceptions is null)
        {
            exceptionPositions = [];
            exceptionValues = [];
            return;
        }

        exceptionPositions = exceptions.ToArray();
        exceptionValues = new float[exceptions.Count];
        for (int j = 0; j < exceptions.Count; j++)
        {
            int i = exceptions[j];
            exceptionValues[j] = values[i];
            destination[i] = fillValue;
        }
    }

    /// <summary>
    /// Bit-packs frame-of-reference deltas LSB-first, accumulating into a register and writing each
    /// output word once.
    /// </summary>
    /// <remarks>
    /// <para>The obvious shape — locate each value's byte, read the word there, OR the value in,
    /// write it back — makes consecutive values touch the same word, so every store feeds the next
    /// load. MEASURED, that ran at about 2 GB/s; accumulating and writing each word once runs at
    /// 9 to 17 GB/s depending on the width, and bit packing was roughly two fifths of encode.</para>
    /// <para>Note this is the opposite conclusion to the decoder's unpacker, where hoisting the
    /// per-value offsets into locals was the win. Unpacking is pure reads with no dependency
    /// between values; the same trick applied here measured no better than what it replaced,
    /// because it does not remove the read-modify-write.</para>
    /// </remarks>
    private static void PackBits(Span<byte> dest, ReadOnlySpan<long> values, long min, int bitWidth)
    {
        ulong accumulator = 0;
        int held = 0;
        int offset = 0;

        for (int i = 0; i < values.Length; i++)
        {
            ulong delta = unchecked((ulong)(values[i] - min));
            accumulator |= delta << held;
            held += bitWidth;

            if (held >= 64)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(offset, 8), accumulator);
                offset += 8;
                held -= 64;
                accumulator = held == 0 ? 0UL : delta >> (bitWidth - held);
            }
        }

        WriteTail(dest, offset, accumulator, held);
    }

    /// <summary>FLOAT counterpart of <see cref="PackBits(Span{byte}, ReadOnlySpan{long}, long, int)"/>.</summary>
    private static void PackBits(Span<byte> dest, ReadOnlySpan<int> values, long min, int bitWidth)
    {
        ulong accumulator = 0;
        int held = 0;
        int offset = 0;

        for (int i = 0; i < values.Length; i++)
        {
            ulong delta = unchecked((ulong)(values[i] - min));
            accumulator |= delta << held;
            held += bitWidth;

            if (held >= 64)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(offset, 8), accumulator);
                offset += 8;
                held -= 64;
                accumulator = held == 0 ? 0UL : delta >> (bitWidth - held);
            }
        }

        WriteTail(dest, offset, accumulator, held);
    }

    /// <summary>
    /// Writes the bits still in the accumulator, a byte at a time. The whole words above are
    /// written eight bytes at once and always fit, but the destination is sized to the exact
    /// packed length, so the last partial word cannot be.
    /// </summary>
    private static void WriteTail(Span<byte> dest, int offset, ulong accumulator, int held)
    {
        for (int k = 0; held > 0; k++, held -= 8)
            dest[offset + k] = (byte)(accumulator >> (k * 8));
    }

    private static int BitsRequired64(ulong v)
    {
        int n = 0;
        while (v != 0)
        {
            v >>= 1;
            n++;
        }
        return n;
    }

    private static int BitsRequired32(ulong v)
    {
        int n = BitsRequired64(v);
        return n > 32 ? 32 : n;
    }

    private static bool IsFiniteFloat(float v) => !float.IsNaN(v) && !float.IsInfinity(v);

    private static bool IsNegativeZero(float v) =>
        v == 0f && BitConverter.GetBytes(v)[3] != 0; // sign bit set

    private static bool IsNegativeZero(double v) =>
        v == 0.0 && BitConverter.DoubleToInt64Bits(v) != 0L;
}
