// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="DecimalOutputKind"/> controls whether DECIMAL columns surface as the narrowest Arrow decimal
/// that fits the physical width (the default) or are always widened to the classic Decimal128/256 — the
/// latter for consumers that mishandle Decimal32/Decimal64 across the Arrow C data interface.
/// </summary>
public class DecimalOutputKindTests : IDisposable
{
    private readonly string _tempDir;

    public DecimalOutputKindTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"pq_dec_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly ParquetReadOptions Wide =
        new() { DecimalOutput = DecimalOutputKind.Decimal128 };

    [Fact]
    public async Task Default_KeepsNarrowDecimalTypes()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("int32_decimal.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        var decType = Assert.IsType<Decimal32Type>(batch.Schema.GetFieldByName("value").DataType);
        Assert.Equal(4, decType.Precision);
        Assert.Equal(2, decType.Scale);
    }

    [Theory]
    [InlineData("int32_decimal.parquet", 4, 2)]
    [InlineData("int64_decimal.parquet", 10, 2)]
    public async Task Decimal128Kind_WidensNarrowColumns(string fileName, int precision, int scale)
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath(fileName));
        using var reader = new ParquetFileReader(file, ownsFile: false, Wide);
        var batch = await reader.ReadRowGroupAsync(0);

        var decType = Assert.IsType<Decimal128Type>(batch.Schema.GetFieldByName("value").DataType);
        Assert.Equal(precision, decType.Precision);
        Assert.Equal(scale, decType.Scale);

        // Values survive the widening: 1.00 .. 24.00.
        var array = (Decimal128Array)batch.Column("value");
        Assert.Equal(24, array.Length);
        Assert.Equal(1.00m, array.GetValue(0));
        Assert.Equal(24.00m, array.GetValue(23));
    }

    [Fact]
    public async Task Decimal128Kind_LeavesWideColumnsAlone()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("fixed_length_decimal.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false, Wide);
        var batch = await reader.ReadRowGroupAsync(0);

        Assert.IsType<Decimal128Type>(batch.Schema.GetFieldByName("value").DataType);
    }

    // The corpus files are all positive, so round-trip our own INT32-backed decimal with negative values —
    // widening is only lossless if the decoder sign-extends the unscaled integer across the full target width.
    [Fact]
    public async Task Decimal128Kind_SignExtendsNegativeValues()
    {
        string path = Path.Combine(_tempDir, "neg32.parquet");
        var type = new Decimal32Type(9, 2);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", type, true))
            .Build();

        decimal[] expected = [-1.00m, -9999.99m, 0.00m, 12.34m, -0.01m];
        var builder = new Decimal32Array.Builder(type);
        foreach (var d in expected)
            builder.Append(d);

        await using (var outFile = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(outFile, ownsFile: false);
            await writer.WriteRowGroupAsync(
                new RecordBatch(schema, [builder.Build()], expected.Length));
        }

        // Narrow (default) round-trip — the baseline.
        await using (var inFile = new LocalRandomAccessFile(path))
        {
            using var reader = new ParquetFileReader(inFile, ownsFile: false);
            var batch = await reader.ReadRowGroupAsync(0);
            var array = (Decimal32Array)batch.Column("value");
            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], array.GetValue(i));
        }

        // Widened — same values, wider slots. A missing sign-extension would leave the high bytes zeroed
        // and turn every negative into a huge positive.
        await using (var inFile = new LocalRandomAccessFile(path))
        {
            using var reader = new ParquetFileReader(inFile, ownsFile: false, Wide);
            var batch = await reader.ReadRowGroupAsync(0);

            var decType = Assert.IsType<Decimal128Type>(batch.Schema.GetFieldByName("value").DataType);
            Assert.Equal(9, decType.Precision);
            Assert.Equal(2, decType.Scale);

            var array = (Decimal128Array)batch.Column("value");
            Assert.Equal(expected.Length, array.Length);
            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], array.GetValue(i));
        }
    }
}
