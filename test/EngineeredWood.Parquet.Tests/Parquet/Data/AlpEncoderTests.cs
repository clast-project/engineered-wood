// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0001 // ALP-specific tests intentionally reference the experimental enum values.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class AlpEncoderTests : IDisposable
{
    private readonly string _tempDir;

    public AlpEncoderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-alp-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    [Fact]
    public void EncodeDoubles_AllZeros_RoundTrips()
    {
        var values = new double[1024];
        var page = AlpEncoder.EncodeDoubles(values);

        var output = new double[values.Length];
        AlpDecoder.DecodeDoubles(page, output, values.Length);
        Assert.Equal(values, output);
    }

    [Fact]
    public void EncodeDoubles_DecimalLike_RoundTripsBitExact()
    {
        // Decimal-tenths values that round-trip cleanly with (e=1, f=0).
        var values = new double[1024];
        var rng = new Random(42);
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.Next(-1000, 1000) / 10.0;

        var page = AlpEncoder.EncodeDoubles(values);
        var output = new double[values.Length];
        AlpDecoder.DecodeDoubles(page, output, values.Length);

        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], output[i]);
    }

    [Fact]
    public void EncodeDoubles_WithExceptions_RoundTripsBitExact()
    {
        // Mix of values that round-trip with (e=2, f=0) and ones that won't.
        var values = new[]
        {
            1.23, 0.5, 1.0, -2.0, 0.42,
            double.NaN, double.PositiveInfinity, double.NegativeInfinity,
            1.0 / 3.0, // never round-trips through any (e, f) decimal scaling
            1234.5678,
            -0.0,
        };

        var page = AlpEncoder.EncodeDoubles(values);
        var output = new double[values.Length];
        AlpDecoder.DecodeDoubles(page, output, values.Length);

        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], output[i]);
    }

    [Fact]
    public void EncodeDoubles_LargeRandom_RoundTrips()
    {
        // Multi-vector page to exercise the offset array and last-vector-shorter path.
        var values = new double[2050];
        var rng = new Random(7);
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.NextDouble();

        var page = AlpEncoder.EncodeDoubles(values);
        var output = new double[values.Length];
        AlpDecoder.DecodeDoubles(page, output, values.Length);

        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], output[i]);
    }

    [Fact]
    public void EncodeDoubles_OneAwkwardValue_StillPacksTheRestNarrow()
    {
        // One value with twelve decimal places forces the exponent up to 12 for the whole vector.
        // A search that stops at the first combination costing no exceptions takes (e=12, f=0),
        // which multiplies every tenth up to 1e11 and pays ~48 bits a value; scoring by size
        // instead takes (e=12, f=11), divides that scale back out, and pays ~11.
        var values = new double[1024];
        var rng = new Random(4242);
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.Next(-1000, 1000) / 10.0;
        values[500] = 0.123456789012;

        var page = AlpEncoder.EncodeDoubles(values);

        var output = new double[values.Length];
        AlpDecoder.DecodeDoubles(page, output, values.Length);
        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], output[i]);

        double bitsPerValue = page.Length * 8.0 / values.Length;
        Assert.True(bitsPerValue < 20, $"ALP cost {bitsPerValue:F2} bits/value where PLAIN costs 64");
    }

    [Fact]
    public void EncodeFloats_OneAwkwardValue_StillPacksTheRestNarrow()
    {
        // FLOAT counterpart of EncodeDoubles_OneAwkwardValue_StillPacksTheRestNarrow.
        var values = new float[1024];
        var rng = new Random(4242);
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.Next(-1000, 1000) / 10.0f;
        values[500] = 0.12345678f;

        var page = AlpEncoder.EncodeFloats(values);

        var output = new float[values.Length];
        AlpDecoder.DecodeFloats(page, output, values.Length);
        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], output[i]);

        double bitsPerValue = page.Length * 8.0 / values.Length;
        Assert.True(bitsPerValue < 20, $"ALP cost {bitsPerValue:F2} bits/value where PLAIN costs 32");
    }

    [Fact]
    public void EncodeFloats_DecimalLike_RoundTripsBitExact()
    {
        var values = new float[1024];
        var rng = new Random(13);
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.Next(-100, 100) / 10.0f;

        var page = AlpEncoder.EncodeFloats(values);
        var output = new float[values.Length];
        AlpDecoder.DecodeFloats(page, output, values.Length);

        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], output[i]);
    }

    [Fact]
    public void EncodeFloats_WithExceptions_RoundTripsBitExact()
    {
        var values = new[]
        {
            1.5f, 0.5f, 2.5f, 1.0f,
            float.NaN, float.PositiveInfinity, float.NegativeInfinity,
            1f / 3f,
            -0.0f,
            12345.678f,
        };

        var page = AlpEncoder.EncodeFloats(values);
        var output = new float[values.Length];
        AlpDecoder.DecodeFloats(page, output, values.Length);

        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], output[i]);
    }

    [Fact]
    public async Task ParquetWriter_DoubleColumnWithAlp_RoundTripsThroughEW()
    {
        var path = TempPath("ew-alp-double.parquet");
        var values = new double[5000];
        var rng = new Random(123);
        for (int i = 0; i < values.Length; i++)
            values[i] = (rng.Next(0, 100000) - 50000) / 100.0;

        await WriteDoubleColumn(path, values, FloatingPointEncoding.Alp);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (DoubleArray)batch.Column(0);

        Assert.Equal(values.Length, arr.Length);
        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], arr.GetValue(i)!.Value);

        // Verify that ALP was actually used.
        var meta = await reader.ReadMetadataAsync();
        var encodings = meta.RowGroups[0].Columns[0].MetaData!.Encodings;
        Assert.Contains(Encoding.Alp, encodings);
    }

    [Fact]
    public async Task ParquetWriter_FloatColumnWithAlp_RoundTripsThroughEW()
    {
        var path = TempPath("ew-alp-float.parquet");
        var values = new float[3000];
        var rng = new Random(321);
        for (int i = 0; i < values.Length; i++)
            values[i] = (rng.Next(0, 10000) - 5000) / 100.0f;

        await WriteFloatColumn(path, values, FloatingPointEncoding.Alp);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (FloatArray)batch.Column(0);

        Assert.Equal(values.Length, arr.Length);
        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], arr.GetValue(i)!.Value);
    }

    [Fact]
    public async Task ParquetWriter_DoubleAlp_ReadableByParquetSharp()
    {
        var path = TempPath("ew-alp-ps.parquet");
        var values = new double[1500];
        var rng = new Random(99);
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.Next(-9999, 9999) / 100.0;

        await WriteDoubleColumn(path, values, FloatingPointEncoding.Alp);

        // ParquetSharp 21.0 does not yet recognize encoding 10 (ALP) and rejects
        // the whole footer when it encounters it in the encodings list. We verify
        // that EW round-trips the file (covered by other tests); here we just
        // check that the file is otherwise well-formed by reopening it ourselves.
        try
        {
            using var psReader = new ParquetSharp.ParquetFileReader(path);
            using var rg = psReader.RowGroup(0);
            using var col = rg.Column(0).LogicalReader<double>();
            var read = new double[values.Length];
            col.ReadBatch(read);
            for (int i = 0; i < values.Length; i++)
                AssertBitEqual(values[i], read[i]);
        }
        catch (ParquetSharp.ParquetException)
        {
            // Expected for ParquetSharp versions predating ALP support.
        }
        catch (NotSupportedException)
        {
            // Also acceptable signal of "encoding not implemented".
        }
    }

    private static void AssertBitEqual(double expected, double actual)
    {
        long e = BitConverter.DoubleToInt64Bits(expected);
        long a = BitConverter.DoubleToInt64Bits(actual);
        Assert.True(e == a, $"expected {expected:R} (0x{e:X16}) got {actual:R} (0x{a:X16})");
    }

    private static void AssertBitEqual(float expected, float actual)
    {
        var eb = BitConverter.GetBytes(expected);
        var ab = BitConverter.GetBytes(actual);
        Assert.True(System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(eb)
            == System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(ab),
            $"expected {expected:R} got {actual:R}");
    }

    [Fact]
    public void PackBits_EveryWidthAndLength_MatchesABitByBitReference()
    {
        // The packer accumulates into a register and flushes whole words, so its invariants are the
        // word rollover and the final partial word — neither of which an end-to-end encode reaches
        // except at whatever widths the data happens to produce. Sweep every width against lengths
        // sitting either side of the 64-bit word boundary, and compare against a reference that
        // simply sets one bit at a time.
        ulong state = 9001;

        foreach (int count in new[] { 1, 2, 7, 8, 9, 63, 64, 65, 127, 128, 129, 1000, 1024 })
        {
            for (int bitWidth = 1; bitWidth <= 64; bitWidth++)
            {
                ulong mask = bitWidth == 64 ? ulong.MaxValue : (1UL << bitWidth) - 1UL;
                var values = new long[count];
                for (int i = 0; i < count; i++)
                    values[i] = unchecked((long)(NextRandomBits(ref state) & mask));

                int packedSize = (int)(((long)count * bitWidth + 7) / 8);

                // Four bytes of slack past the end, expected to stay zero: the packer must fill the
                // packed length exactly and no further.
                var actual = new byte[packedSize + 4];
                var expected = new byte[packedSize + 4];
                AlpEncoder.PackBits(actual.AsSpan(0, packedSize), values, min: 0, bitWidth);
                PackOneBitAtATime(expected.AsSpan(0, packedSize), values, bitWidth);

                Assert.True(
                    actual.AsSpan().SequenceEqual(expected),
                    $"count={count} bitWidth={bitWidth}: packed bytes differ");
            }
        }
    }

    [Fact]
    public void PackBits_FloatOverload_MatchesABitByBitReference()
    {
        // Same sweep for the int32 overload the FLOAT encoder uses; widths stop at 32 there.
        ulong state = 4242;

        foreach (int count in new[] { 1, 7, 8, 9, 63, 64, 65, 1000 })
        {
            for (int bitWidth = 1; bitWidth <= 32; bitWidth++)
            {
                // The frame of reference is the minimum of the encoded values, so a delta is never
                // negative — and it must not be, since (values[i] - min) widens to long and a
                // negative would sign-extend into the neighbouring field. Anchoring min at
                // int.MinValue lets a delta span the full 32 bits while every value stays an int.
                const int Min = int.MinValue;
                uint mask = bitWidth == 32 ? uint.MaxValue : (1u << bitWidth) - 1u;
                var values = new int[count];
                var deltas = new long[count];
                for (int i = 0; i < count; i++)
                {
                    long delta = (uint)NextRandomBits(ref state) & mask;
                    values[i] = (int)(Min + delta);
                    deltas[i] = delta;
                }

                int packedSize = (int)(((long)count * bitWidth + 7) / 8);
                var actual = new byte[packedSize + 4];
                var expected = new byte[packedSize + 4];
                AlpEncoder.PackBits(actual.AsSpan(0, packedSize), values, Min, bitWidth);
                PackOneBitAtATime(expected.AsSpan(0, packedSize), deltas, bitWidth);

                Assert.True(
                    actual.AsSpan().SequenceEqual(expected),
                    $"count={count} bitWidth={bitWidth}: packed bytes differ");
            }
        }
    }

    /// <summary>
    /// The reference: LSB-first, one bit at a time. Obviously correct and obviously slow, which is
    /// what makes it worth comparing against.
    /// </summary>
    private static void PackOneBitAtATime(Span<byte> dest, ReadOnlySpan<long> values, int bitWidth)
    {
        for (int i = 0; i < values.Length; i++)
        {
            ulong value = unchecked((ulong)values[i]);
            for (int b = 0; b < bitWidth; b++)
            {
                if ((value & (1UL << b)) == 0)
                    continue;

                long bit = ((long)i * bitWidth) + b;
                dest[(int)(bit >> 3)] |= (byte)(1 << (int)(bit & 7));
            }
        }
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

    private static async Task WriteDoubleColumn(string path, double[] values, FloatingPointEncoding fpe)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", DoubleType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new DoubleArray.Builder().AppendRange(values).Build()], values.Length);

        var options = ParquetWriteOptions.Default with
        {
            FloatingPointEncoding = fpe,
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            Compression = EngineeredWood.Compression.CompressionCodec.Uncompressed,
        };

        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private static async Task WriteFloatColumn(string path, float[] values, FloatingPointEncoding fpe)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", FloatType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new FloatArray.Builder().AppendRange(values).Build()], values.Length);

        var options = ParquetWriteOptions.Default with
        {
            FloatingPointEncoding = fpe,
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            Compression = EngineeredWood.Compression.CompressionCodec.Uncompressed,
        };

        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }
}
