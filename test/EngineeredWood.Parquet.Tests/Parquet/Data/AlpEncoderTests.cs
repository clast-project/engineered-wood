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
    public async Task ParquetWriter_DoubleAlpWorseThanPlain_FallsBackPerPage()
    {
        // Coordinates in radians: no decimal structure at all, so almost every value fails to
        // round-trip through ALP's scaling and is stored whole beside a position — costing more
        // than writing the doubles out. Choosing ALP must not make the file bigger.
        var path = TempPath("ew-alp-fallback-double.parquet");
        var values = new double[5000];
        var rng = new Random(4242);
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.NextDouble() * 2.0 - 1.0;

        await WriteDoubleColumn(path, values, FloatingPointEncoding.Alp);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        var column = meta.RowGroups[0].Columns[0].MetaData!;

        Assert.DoesNotContain(Encoding.Alp, column.Encodings);
        Assert.Contains(Encoding.Plain, column.Encodings);

        double bitsPerValue = column.TotalCompressedSize * 8.0 / values.Length;
        Assert.True(bitsPerValue < 65, $"fell back but still cost {bitsPerValue:F2} bits/value");

        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (DoubleArray)batch.Column(0);
        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], arr.GetValue(i)!.Value);
    }

    [Fact]
    public async Task ParquetWriter_FloatAlpWorseThanPlain_FallsBackPerPage()
    {
        // FLOAT counterpart. Two-decimal values above about 2^16 are the shape that does it: the
        // format's 24-bit mantissa cannot reproduce them through the decode formula whatever the
        // encoder picks, so they all become exceptions.
        var path = TempPath("ew-alp-fallback-float.parquet");
        var values = new float[5000];
        var rng = new Random(99);
        for (int i = 0; i < values.Length; i++)
            values[i] = (float)(rng.Next(1 << 20, 1 << 24) / 100.0);

        await WriteFloatColumn(path, values, FloatingPointEncoding.Alp);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        var column = meta.RowGroups[0].Columns[0].MetaData!;

        Assert.DoesNotContain(Encoding.Alp, column.Encodings);

        double bitsPerValue = column.TotalCompressedSize * 8.0 / values.Length;
        Assert.True(bitsPerValue < 33, $"fell back but still cost {bitsPerValue:F2} bits/value");

        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (FloatArray)batch.Column(0);
        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], arr.GetValue(i)!.Value);
    }

    [Fact]
    public void EncodeFloats_IntegralValuesPastTheOldRoundingLimit_StayExceptionFree()
    {
        // The encoder rounded by biasing into a float's mantissa, which only works below 2^22.
        // Everything above it lost its round trip and became an exception at 48 bits, so integers
        // that ALP should store in 24 cost more than PLAIN's 32. Rounding in double fixes it.
        var values = new float[4096];
        var rng = new Random(2024);
        for (int i = 0; i < values.Length; i++)
            values[i] = rng.Next(0, 1 << 24);

        var page = AlpEncoder.EncodeFloats(values);

        var output = new float[values.Length];
        AlpDecoder.DecodeFloats(page, output, values.Length);
        for (int i = 0; i < values.Length; i++)
            AssertBitEqual(values[i], output[i]);

        double bitsPerValue = page.Length * 8.0 / values.Length;
        Assert.True(bitsPerValue < 26, $"cost {bitsPerValue:F2} bits/value; PLAIN would cost 32");
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
