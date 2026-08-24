// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Globalization;
using Apache.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class AlpDecoderTests
{
    [Fact]
    public void DecodeDoubles_SingleVector_NoExceptions()
    {
        // Decimal-tenths with (e=1, f=0): values round-trip exactly because 1/10 of an
        // integer in IEEE-754 doubles can be inverted by *10 then /10 with no error.
        // encoded = [10, 25, 5, 30]; FOR min = 5; deltas = [5, 20, 0, 25]; bit_width = 5.
        var page = BuildSingleDoubleVectorPage(
            exponent: 1, factor: 0,
            frameOfReference: 5L,
            bitWidth: 5,
            deltas: [5, 20, 0, 25],
            exceptionPositions: [],
            exceptionValues: []);

        var output = new double[4];
        AlpDecoder.DecodeDoubles(page, output, 4);

        Assert.Equal(1.0, output[0]);
        Assert.Equal(2.5, output[1]);
        Assert.Equal(0.5, output[2]);
        Assert.Equal(3.0, output[3]);
    }

    [Fact]
    public void DecodeDoubles_SingleVector_AllIdentical_ZeroBitWidth()
    {
        // All values 5.0 with (e=0, f=0). Encoded = [5,5,5,5]; FOR min = 5; deltas = [0,0,0,0]; bit_width=0.
        var page = BuildSingleDoubleVectorPage(
            exponent: 0, factor: 0,
            frameOfReference: 5L,
            bitWidth: 0,
            deltas: [],
            exceptionPositions: [],
            exceptionValues: []);

        var output = new double[4];
        AlpDecoder.DecodeDoubles(page, output, 4);

        Assert.Equal(new[] { 5.0, 5.0, 5.0, 5.0 }, output);
    }

    [Fact]
    public void DecodeDoubles_SingleVector_WithExceptions()
    {
        // Values { 1.5, NaN, 2.5, 0.333... } with (e=1, f=0).
        // First non-exception encoded value = 15. Placeholders = [15, 15, 25, 15]; min=15; deltas=[0,0,10,0]; bit_width=4.
        double oneThird = 1.0 / 3.0;
        var page = BuildSingleDoubleVectorPage(
            exponent: 1, factor: 0,
            frameOfReference: 15L,
            bitWidth: 4,
            deltas: [0, 0, 10, 0],
            exceptionPositions: [1, 3],
            exceptionValues: [double.NaN, oneThird]);

        var output = new double[4];
        AlpDecoder.DecodeDoubles(page, output, 4);

        Assert.Equal(1.5, output[0]);
        Assert.True(double.IsNaN(output[1]));
        Assert.Equal(2.5, output[2]);
        Assert.Equal(oneThird, output[3]);
    }

    [Fact]
    public void DecodeDoubles_MultipleVectors_LastVectorShorter()
    {
        // Two vectors: vector_size = 8 (log_vector_size = 3), num_elements = 10.
        // Vector 0 has 8 values; vector 1 has 2 values.
        // All values: identity decoding (e=0, f=0).
        var v0 = SerializeDoubleVector(0, 0, 100L, 4, [0, 1, 2, 3, 4, 5, 6, 7], [], []);
        var v1 = SerializeDoubleVector(0, 0, 200L, 1, [0, 1], [], []);

        int numVectors = 2;
        var offsetArrayBytes = new byte[numVectors * 4];
        uint off0 = (uint)(numVectors * 4);
        uint off1 = off0 + (uint)v0.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(offsetArrayBytes.AsSpan(0, 4), off0);
        BinaryPrimitives.WriteUInt32LittleEndian(offsetArrayBytes.AsSpan(4, 4), off1);

        var page = ConcatPage(logVectorSize: 3, numElements: 10, offsetArrayBytes, v0, v1);

        var output = new double[10];
        AlpDecoder.DecodeDoubles(page, output, 10);

        Assert.Equal(new[] { 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 200.0, 201.0 }, output);
    }

    [Fact]
    public void DecodeDoubles_InverseTransform_MatchesClastAlpOnEveryCombination()
    {
        // The DOUBLE path writes the inverse transform out by hand instead of calling
        // Clast.Alp.AlpDecoder.DecodeValue per value, because the call does not inline. That is
        // only safe while the two agree bit for bit, so pin it: every (exponent, factor) the spec
        // allows, against encoded values spanning the int64 range including the magnitudes where
        // scaling overflows — which a legitimate page cannot reach but a corrupt one can.
        // SplitMix64 rather than Random: the same values are then checked on every target
        // framework, and Random.NextInt64 does not exist on net472 at all.
        ulong state = 20260823;

        for (int exponent = 0; exponent <= 18; exponent++)
        {
            for (int factor = 0; factor <= exponent; factor++)
            {
                for (int i = 0; i < 200; i++)
                {
                    long encoded = i switch
                    {
                        0 => 0L,
                        1 => 1L,
                        2 => -1L,
                        3 => long.MaxValue,
                        4 => long.MinValue,
                        5 => 1L << 53,
                        6 => -(1L << 53),
                        // Spread across magnitudes so small values, values around 2^53, and the
                        // overflow range are all covered.
                        _ => NextEncoded(ref state),
                    };

                    var page = BuildSingleDoubleVectorPage(
                        exponent: exponent, factor: factor,
                        frameOfReference: encoded,
                        bitWidth: 0,
                        deltas: [0],
                        exceptionPositions: [],
                        exceptionValues: []);

                    var output = new double[1];
                    AlpDecoder.DecodeDoubles(page, output, 1);

                    double expected = Clast.Alp.AlpDecoder.DecodeValue(encoded, exponent, factor);
                    Assert.Equal(
                        BitConverter.DoubleToInt64Bits(expected),
                        BitConverter.DoubleToInt64Bits(output[0]));
                }
            }
        }
    }

    [Fact]
    public void DecodeFloats_SingleVector_DecimalTenths()
    {
        // Same shape as the double test: (e=1, f=0) with halves and integers, which
        // round-trip exactly in single precision.
        var page = BuildSingleFloatVectorPage(
            exponent: 1, factor: 0,
            frameOfReference: 5,
            bitWidth: 5,
            deltas: [5, 20, 0, 25],
            exceptionPositions: [],
            exceptionValues: []);

        var output = new float[4];
        AlpDecoder.DecodeFloats(page, output, 4);

        Assert.Equal(1.0f, output[0]);
        Assert.Equal(2.5f, output[1]);
        Assert.Equal(0.5f, output[2]);
        Assert.Equal(3.0f, output[3]);
    }

    [Fact]
    public async Task ReadParquetTesting_AlpSpotify1_BitExact()
    {
        // ALP DOUBLE file from apache/parquet-testing PR #100.
        await VerifyParquetFileAgainstCsv("alp_spotify1.parquet", "alp_spotify1_expect.csv", isFloat: false);
    }

    [Fact]
    public async Task ReadParquetTesting_AlpArade_BitExact()
    {
        await VerifyParquetFileAgainstCsv("alp_arade.parquet", "alp_arade_expect.csv", isFloat: false);
    }

    [Fact]
    public async Task ReadParquetTesting_AlpJavaSpotify1_BitExact()
    {
        await VerifyParquetFileAgainstCsv("alp_java_spotify1.parquet", "alp_spotify1_expect.csv", isFloat: false);
    }

    [Fact]
    public async Task ReadParquetTesting_AlpJavaArade_BitExact()
    {
        await VerifyParquetFileAgainstCsv("alp_java_arade.parquet", "alp_arade_expect.csv", isFloat: false);
    }

    [Fact]
    public async Task ReadParquetTesting_AlpFloatSpotify1_BitExact()
    {
        await VerifyParquetFileAgainstCsv("alp_float_spotify1.parquet", "alp_float_spotify1_expect.csv", isFloat: true);
    }

    [Fact]
    public async Task ReadParquetTesting_AlpFloatArade_BitExact()
    {
        await VerifyParquetFileAgainstCsv("alp_float_arade.parquet", "alp_float_arade_expect.csv", isFloat: true);
    }

    [Fact]
    public async Task ReadParquetTesting_AlpJavaFloatSpotify1_BitExact()
    {
        await VerifyParquetFileAgainstCsv("alp_java_float_spotify1.parquet", "alp_float_spotify1_expect.csv", isFloat: true);
    }

    [Fact]
    public async Task ReadParquetTesting_AlpJavaFloatArade_BitExact()
    {
        await VerifyParquetFileAgainstCsv("alp_java_float_arade.parquet", "alp_float_arade_expect.csv", isFloat: true);
    }

    private static async Task VerifyParquetFileAgainstCsv(string parquetName, string csvName, bool isFloat)
    {
        var parquetPath = TestData.GetPath(parquetName);
        var csvPath = TestData.GetPath(csvName);
        // The ALP data files come from apache/parquet-testing PR #100, which has not merged — they are
        // not in the pinned submodule. No-op (rather than fail) when absent; to run these tests, fetch
        // the PR into the submodule and materialize the files as UNTRACKED (survives a re-checkout):
        //   git fetch origin pull/100/head:refs/remotes/origin/pr100
        //   git checkout origin/pr100 -- data/alp_*  &&  git restore --staged data/alp_*
        if (!File.Exists(parquetPath) || !File.Exists(csvPath))
        {
            return;
        }

        await using var file = new LocalRandomAccessFile(parquetPath);
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var expectedRows = ReadCsv(csvPath);
        int rowCount = expectedRows.Count;
        int columnCount = expectedRows[0].Length;

        int rowsRead = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            Assert.Equal(columnCount, batch.ColumnCount);
            for (int c = 0; c < columnCount; c++)
            {
                if (isFloat)
                {
                    var arr = (FloatArray)batch.Column(c);
                    for (int i = 0; i < arr.Length; i++)
                    {
                        float? actual = arr.IsNull(i) ? (float?)null : arr.GetValue(i);
                        AssertCellMatchesFloat(rowsRead + i, c, expectedRows[rowsRead + i][c], actual);
                    }
                }
                else
                {
                    var arr = (DoubleArray)batch.Column(c);
                    for (int i = 0; i < arr.Length; i++)
                    {
                        double? actual = arr.IsNull(i) ? (double?)null : arr.GetValue(i);
                        AssertCellMatchesDouble(rowsRead + i, c, expectedRows[rowsRead + i][c], actual);
                    }
                }
            }
            rowsRead += batch.Length;
        }

        Assert.Equal(rowCount, rowsRead);
    }

    private static void AssertCellMatchesDouble(int row, int col, string expectedText, double? actual)
    {
        if (string.IsNullOrEmpty(expectedText))
        {
            Assert.Null(actual);
            return;
        }
        Assert.NotNull(actual);
        double expected = double.Parse(expectedText, CultureInfo.InvariantCulture);
        if (double.IsNaN(expected))
            Assert.True(double.IsNaN(actual!.Value), $"row {row} col {col}: expected NaN");
        else
            Assert.True(BitConverter.DoubleToInt64Bits(expected) == BitConverter.DoubleToInt64Bits(actual!.Value),
                $"row {row} col {col}: expected {expected:R} got {actual.Value:R}");
    }

    private static void AssertCellMatchesFloat(int row, int col, string expectedText, float? actual)
    {
        if (string.IsNullOrEmpty(expectedText))
        {
            Assert.Null(actual);
            return;
        }
        Assert.NotNull(actual);
        float expected = float.Parse(expectedText, CultureInfo.InvariantCulture);
        if (float.IsNaN(expected))
            Assert.True(float.IsNaN(actual!.Value), $"row {row} col {col}: expected NaN");
        else
            Assert.True(SingleToInt32Bits(expected) == SingleToInt32Bits(actual!.Value),
                $"row {row} col {col}: expected {expected:R} got {actual.Value:R}");
    }

    private static int SingleToInt32Bits(float value)
    {
        var bytes = BitConverter.GetBytes(value);
        return BinaryPrimitives.ReadInt32LittleEndian(bytes);
    }

    private static List<string[]> ReadCsv(string path)
    {
        var rows = new List<string[]>();
        using var sr = new StreamReader(path);
        string? header = sr.ReadLine();
        Assert.NotNull(header);
        while (sr.ReadLine() is { } line)
            rows.Add(line.Split(','));
        return rows;
    }

    // ─── Page builders ───────────────────────────────────────────────────────

    /// <summary>A deterministic encoded value, spread across the whole int64 magnitude range.</summary>
    private static long NextEncoded(ref ulong state)
    {
        ulong bits = NextRandomBits(ref state);
        int shift = (int)(NextRandomBits(ref state) % 64);
        long value = unchecked((long)(bits >> shift));
        return (bits & 1) == 0 ? value : -value;
    }

    /// <summary>SplitMix64: a bit source that behaves identically on every target framework.</summary>
    private static ulong NextRandomBits(ref ulong state)
    {
        state = unchecked(state + 0x9E3779B97F4A7C15UL);
        ulong z = state;
        z = unchecked((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL);
        z = unchecked((z ^ (z >> 27)) * 0x94D049BB133111EBUL);
        return z ^ (z >> 31);
    }

    [Fact]
    public void DecodeDoubles_EveryBitWidthAndLength_RoundTrips()
    {
        // The bulk unpacker reads a whole 64-bit word per value, which means its behaviour turns on
        // the bit width (widths above 57 fall back per value) and on how close a group sits to the
        // end of the buffer. Sweep both. Exponent and factor are zero so the decoded value is the
        // encoded integer itself and any failure is the unpacker's, not the transform's.
        ulong state = 99;

        foreach (int n in new[] { 1, 7, 8, 9, 17, 63, 64, 1000, 1024, 1031, 2048 })
        {
            for (int bitWidth = 0; bitWidth <= 64; bitWidth++)
            {
                var deltas = new long[n];
                ulong mask = bitWidth == 64 ? ulong.MaxValue : (1UL << bitWidth) - 1UL;
                for (int i = 0; i < n; i++)
                    deltas[i] = unchecked((long)(NextRandomBits(ref state) & mask));

                const long Frame = -1_000_000;
                var page = BuildWideDoubleVectorPage(n, exponent: 0, factor: 0,
                    frameOfReference: Frame, bitWidth: bitWidth, deltas: deltas);

                var output = new double[n];
                AlpDecoder.DecodeDoubles(page, output, n);

                for (int i = 0; i < n; i++)
                {
                    double expected = (double)unchecked(deltas[i] + Frame);
                    Assert.True(
                        BitConverter.DoubleToInt64Bits(expected) == BitConverter.DoubleToInt64Bits(output[i]),
                        $"n={n} bitWidth={bitWidth} index={i}: expected {expected:R} got {output[i]:R}");
                }
            }
        }
    }

    [Fact]
    public void DecodeFloats_EveryBitWidthAndLength_RoundTrips()
    {
        // FLOAT counterpart. The spec caps FLOAT bit widths at 32, so every width takes the bulk
        // path and only the end-of-buffer cutoff varies.
        ulong state = 4242;

        foreach (int n in new[] { 1, 7, 8, 9, 17, 1000, 1024, 1031 })
        {
            for (int bitWidth = 0; bitWidth <= 32; bitWidth++)
            {
                var deltas = new long[n];
                ulong mask = bitWidth == 0 ? 0UL : (1UL << bitWidth) - 1UL;
                for (int i = 0; i < n; i++)
                    deltas[i] = unchecked((long)(NextRandomBits(ref state) & mask));

                const int Frame = -1000;
                var page = BuildWideFloatVectorPage(n, exponent: 0, factor: 0,
                    frameOfReference: Frame, bitWidth: bitWidth, deltas: deltas);

                var output = new float[n];
                AlpDecoder.DecodeFloats(page, output, n);

                for (int i = 0; i < n; i++)
                {
                    float expected = (float)unchecked(deltas[i] + Frame);
                    Assert.True(
                        SingleToInt32Bits(expected) == SingleToInt32Bits(output[i]),
                        $"n={n} bitWidth={bitWidth} index={i}: expected {expected:R} got {output[i]:R}");
                }
            }
        }
    }

    [Fact]
    public void DecodeDoubles_ExceptionsAcrossTileBoundaries_ArePatched()
    {
        // Deltas are unpacked a tile at a time while exceptions are patched per vector, so a vector
        // longer than one tile is where the two can disagree. Put an exception either side of every
        // boundary, and at both ends.
        const int N = 3000;
        const long Frame = 7;
        const int BitWidth = 13;

        ulong state = 31337;
        var deltas = new long[N];
        for (int i = 0; i < N; i++)
            deltas[i] = unchecked((long)(NextRandomBits(ref state) & ((1UL << BitWidth) - 1UL)));

        var positions = new List<int> { 0, 1023, 1024, 1025, 2047, 2048, 2049, N - 1 };
        var values = positions.Select(x => x * -0.5).ToArray();

        var page = BuildWideDoubleVectorPage(N, exponent: 0, factor: 0,
            frameOfReference: Frame, bitWidth: BitWidth, deltas: deltas,
            exceptionPositions: positions.ToArray(), exceptionValues: values);

        var output = new double[N];
        AlpDecoder.DecodeDoubles(page, output, N);

        for (int i = 0; i < N; i++)
        {
            int slot = positions.IndexOf(i);
            double expected = slot >= 0 ? values[slot] : (double)unchecked(deltas[i] + Frame);
            Assert.True(
                BitConverter.DoubleToInt64Bits(expected) == BitConverter.DoubleToInt64Bits(output[i]),
                $"index={i}: expected {expected:R} got {output[i]:R}");
        }
    }

    /// <summary>
    /// A one-vector page whose vector size is large enough to hold <paramref name="n"/> values, so
    /// lengths past the canonical 1024 exercise the decoder's tiling.
    /// </summary>
    private static byte[] BuildWideDoubleVectorPage(
        int n, int exponent, int factor, long frameOfReference, int bitWidth,
        ReadOnlySpan<long> deltas,
        int[]? exceptionPositions = null, double[]? exceptionValues = null)
    {
        int logVectorSize = 3;
        while ((1 << logVectorSize) < n)
            logVectorSize++;

        var vector = SerializeDoubleVector(exponent, factor, frameOfReference, bitWidth, deltas,
            exceptionPositions ?? [], exceptionValues ?? []);

        var offsetArrayBytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(offsetArrayBytes, 4u);
        return ConcatPage(logVectorSize, n, offsetArrayBytes, vector);
    }

    /// <summary>FLOAT counterpart of <see cref="BuildWideDoubleVectorPage"/>.</summary>
    private static byte[] BuildWideFloatVectorPage(
        int n, int exponent, int factor, int frameOfReference, int bitWidth,
        ReadOnlySpan<long> deltas)
    {
        int logVectorSize = 3;
        while ((1 << logVectorSize) < n)
            logVectorSize++;

        var vector = SerializeFloatVector(exponent, factor, frameOfReference, bitWidth, deltas, [], []);

        var offsetArrayBytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(offsetArrayBytes, 4u);
        return ConcatPage(logVectorSize, n, offsetArrayBytes, vector);
    }

    private static byte[] BuildSingleDoubleVectorPage(
        int exponent, int factor,
        long frameOfReference, int bitWidth,
        ReadOnlySpan<long> deltas,
        ReadOnlySpan<int> exceptionPositions,
        ReadOnlySpan<double> exceptionValues)
    {
        int n = Math.Max(deltas.Length,
            bitWidth == 0 ? exceptionPositions.Length : 0);
        if (bitWidth == 0 && deltas.Length == 0)
        {
            // Caller didn't supply deltas: assume all values are FOR (n derived from exceptions or default).
            n = exceptionPositions.Length == 0 ? 4 : Math.Max(exceptionPositions.Length, 4);
        }

        int logVectorSize = 3; // vector_size = 8 (must be at least 8 per spec, [3, 15])
        // The smallest legal log_vector_size is 3, but vectors can hold fewer than vector_size.
        // For unit tests we use small n with log_vector_size = 3.

        var vector = SerializeDoubleVector(exponent, factor, frameOfReference, bitWidth,
            deltas, exceptionPositions, exceptionValues);

        // 1 vector ⇒ offset array has 1 entry pointing past the array (= 4 bytes).
        var offsetArrayBytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(offsetArrayBytes, 4u);

        return ConcatPage(logVectorSize, n, offsetArrayBytes, vector);
    }

    private static byte[] BuildSingleFloatVectorPage(
        int exponent, int factor,
        int frameOfReference, int bitWidth,
        ReadOnlySpan<long> deltas,
        ReadOnlySpan<int> exceptionPositions,
        ReadOnlySpan<float> exceptionValues)
    {
        int n = Math.Max(deltas.Length, exceptionPositions.Length);
        if (n == 0) n = 4;

        int logVectorSize = 3;
        var vector = SerializeFloatVector(exponent, factor, frameOfReference, bitWidth,
            deltas, exceptionPositions, exceptionValues);

        var offsetArrayBytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(offsetArrayBytes, 4u);

        return ConcatPage(logVectorSize, n, offsetArrayBytes, vector);
    }

    private static byte[] SerializeDoubleVector(
        int exponent, int factor,
        long frameOfReference, int bitWidth,
        ReadOnlySpan<long> deltas,
        ReadOnlySpan<int> exceptionPositions,
        ReadOnlySpan<double> exceptionValues)
    {
        int packedSize = (deltas.Length * bitWidth + 7) / 8;
        int totalSize = 4 + 9 + packedSize + exceptionPositions.Length * 2 + exceptionValues.Length * 8;
        var buf = new byte[totalSize];

        buf[0] = (byte)exponent;
        buf[1] = (byte)factor;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2, 2), (ushort)exceptionPositions.Length);
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(4, 8), frameOfReference);
        buf[12] = (byte)bitWidth;

        if (bitWidth > 0)
            PackBitsLsbFirst64(buf.AsSpan(13, packedSize), deltas, bitWidth);

        int posOffset = 13 + packedSize;
        int valOffset = posOffset + exceptionPositions.Length * 2;
        for (int i = 0; i < exceptionPositions.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(posOffset + i * 2, 2), (ushort)exceptionPositions[i]);
        for (int i = 0; i < exceptionValues.Length; i++)
        {
            long bits = BitConverter.DoubleToInt64Bits(exceptionValues[i]);
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(valOffset + i * 8, 8), bits);
        }

        return buf;
    }

    private static byte[] SerializeFloatVector(
        int exponent, int factor,
        int frameOfReference, int bitWidth,
        ReadOnlySpan<long> deltas,
        ReadOnlySpan<int> exceptionPositions,
        ReadOnlySpan<float> exceptionValues)
    {
        int packedSize = (deltas.Length * bitWidth + 7) / 8;
        int totalSize = 4 + 5 + packedSize + exceptionPositions.Length * 2 + exceptionValues.Length * 4;
        var buf = new byte[totalSize];

        buf[0] = (byte)exponent;
        buf[1] = (byte)factor;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2, 2), (ushort)exceptionPositions.Length);
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(4, 4), frameOfReference);
        buf[8] = (byte)bitWidth;

        if (bitWidth > 0)
            PackBitsLsbFirst64(buf.AsSpan(9, packedSize), deltas, bitWidth);

        int posOffset = 9 + packedSize;
        int valOffset = posOffset + exceptionPositions.Length * 2;
        for (int i = 0; i < exceptionPositions.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(posOffset + i * 2, 2), (ushort)exceptionPositions[i]);
        for (int i = 0; i < exceptionValues.Length; i++)
        {
            byte[] fb = BitConverter.GetBytes(exceptionValues[i]);
            System.Array.Copy(fb, 0, buf, valOffset + i * 4, 4);
        }

        return buf;
    }

    private static void PackBitsLsbFirst64(Span<byte> dest, ReadOnlySpan<long> values, int bitWidth)
    {
        long bitOffset = 0;
        ulong mask = bitWidth == 64 ? ulong.MaxValue : ((1UL << bitWidth) - 1UL);
        for (int i = 0; i < values.Length; i++)
        {
            ulong v = unchecked((ulong)values[i]) & mask;
            int byteIdx = (int)(bitOffset >> 3);
            int bitIdx = (int)(bitOffset & 7);
            int spill = bitIdx + bitWidth - 64;

            ulong low = ReadLE(dest, byteIdx);
            low |= v << bitIdx;
            WriteLE(dest, byteIdx, low);
            if (spill > 0)
            {
                ulong high = ReadLE(dest, byteIdx + 8);
                high |= v >> (64 - bitIdx);
                WriteLE(dest, byteIdx + 8, high);
            }
            bitOffset += bitWidth;
        }
    }

    private static ulong ReadLE(Span<byte> dest, int idx)
    {
        if (idx >= dest.Length) return 0;
        int rem = dest.Length - idx;
        if (rem >= 8) return BinaryPrimitives.ReadUInt64LittleEndian(dest.Slice(idx, 8));
        ulong r = 0;
        for (int k = 0; k < rem; k++) r |= (ulong)dest[idx + k] << (k * 8);
        return r;
    }

    private static void WriteLE(Span<byte> dest, int idx, ulong v)
    {
        if (idx >= dest.Length) return;
        int rem = dest.Length - idx;
        if (rem >= 8)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(idx, 8), v);
            return;
        }
        for (int k = 0; k < rem; k++)
            dest[idx + k] = (byte)(v >> (k * 8));
    }

    private static byte[] ConcatPage(
        int logVectorSize, int numElements, byte[] offsetArrayBytes, params byte[][] vectors)
    {
        int total = 7 + offsetArrayBytes.Length;
        foreach (var v in vectors) total += v.Length;
        var buf = new byte[total];

        buf[0] = 0; // compression_mode = ALP
        buf[1] = 0; // integer_encoding = FOR + bit-packing
        buf[2] = (byte)logVectorSize;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(3, 4), numElements);
        System.Array.Copy(offsetArrayBytes, 0, buf, 7, offsetArrayBytes.Length);

        int pos = 7 + offsetArrayBytes.Length;
        foreach (var v in vectors)
        {
            System.Array.Copy(v, 0, buf, pos, v.Length);
            pos += v.Length;
        }
        return buf;
    }
}
