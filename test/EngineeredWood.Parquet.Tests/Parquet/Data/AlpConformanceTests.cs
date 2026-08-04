// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0001 // ALP-specific tests intentionally reference the experimental enum values.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public sealed class AlpConformanceTests : IDisposable
{
    private static readonly string[] FloatAlpColumns =
    [
        "float_alp_1024",
        "float_alp_4096",
        "float_alp_32",
    ];

    private static readonly string[] DoubleAlpColumns =
    [
        "double_alp_1024",
        "double_alp_4096",
        "double_alp_32",
    ];

    private readonly string _tempDir;

    public AlpConformanceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-alp-conformance-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task ReadAlpExtended_MatchesPlainReferencesBitExact()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", "alp_extended.zstd.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(9_000, metadata.NumRows);
        Assert.Equal([6_144L, 1_024L, 1_024L, 808L], metadata.RowGroups.Select(rowGroup => rowGroup.NumRows));

        for (int rowGroupIndex = 0; rowGroupIndex < metadata.RowGroups.Count; rowGroupIndex++)
        {
            var batch = await reader.ReadRowGroupAsync(rowGroupIndex);
            Assert.Equal(metadata.RowGroups[rowGroupIndex].NumRows, batch.Length);

            var floatPlain = Assert.IsType<FloatArray>(batch.Column(batch.Schema.GetFieldIndex("float_plain")));
            foreach (var columnName in FloatAlpColumns)
            {
                var actual = Assert.IsType<FloatArray>(batch.Column(batch.Schema.GetFieldIndex(columnName)));
                AssertFloatColumnsBitEqual(floatPlain, actual, columnName, rowGroupIndex);
            }

            var doublePlain = Assert.IsType<DoubleArray>(batch.Column(batch.Schema.GetFieldIndex("double_plain")));
            foreach (var columnName in DoubleAlpColumns)
            {
                var actual = Assert.IsType<DoubleArray>(batch.Column(batch.Schema.GetFieldIndex(columnName)));
                AssertDoubleColumnsBitEqual(doublePlain, actual, columnName, rowGroupIndex);
            }
        }

        var schema = await reader.GetSchemaAsync();
        foreach (var columnName in FloatAlpColumns.Concat(DoubleAlpColumns))
        {
            int columnIndex = -1;
            for (int i = 0; i < schema.Root.Children.Count; i++)
            {
                if (schema.Root.Children[i].Name == columnName)
                {
                    columnIndex = i;
                    break;
                }
            }
            Assert.True(columnIndex >= 0, $"Column '{columnName}' is missing.");
            Assert.All(metadata.RowGroups, rowGroup =>
                Assert.Contains(Encoding.Alp, rowGroup.Columns[columnIndex].MetaData!.Encodings));
        }
    }

    [Fact]
    public async Task AlpV2_WithNullsAndExceptions_RoundTripsBitExact()
    {
        const int rowCount = 3_000;
        var floatBuilder = new FloatArray.Builder();
        var doubleBuilder = new DoubleArray.Builder();

        for (int i = 0; i < rowCount; i++)
        {
            switch (i % 100)
            {
                case 0:
                    floatBuilder.AppendNull();
                    break;
                case 7:
                    floatBuilder.Append(float.NaN);
                    break;
                case 13:
                    floatBuilder.Append(-0.0f);
                    break;
                default:
                    floatBuilder.Append(i * 0.01f);
                    break;
            }

            switch (i % 100)
            {
                case 5:
                    doubleBuilder.AppendNull();
                    break;
                case 11:
                    doubleBuilder.Append(double.PositiveInfinity);
                    break;
                default:
                    doubleBuilder.Append(i * 0.001);
                    break;
            }
        }

        var expectedFloat = floatBuilder.Build();
        var expectedDouble = doubleBuilder.Build();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("f32", FloatType.Default, nullable: true))
            .Field(new Field("f64", DoubleType.Default, nullable: true))
            .Build();
        var batch = new RecordBatch(schema, [expectedFloat, expectedDouble], rowCount);
        var path = Path.Combine(_tempDir, "nullable-v2.parquet");

        var options = ParquetWriteOptions.Default with
        {
            Compression = CompressionCodec.Uncompressed,
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            FloatingPointEncoding = FloatingPointEncoding.Alp,
        };
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);
        var actual = await reader.ReadRowGroupAsync(0);

        AssertFloatColumnsBitEqual(expectedFloat, Assert.IsType<FloatArray>(actual.Column(0)), "f32", 0);
        AssertDoubleColumnsBitEqual(expectedDouble, Assert.IsType<DoubleArray>(actual.Column(1)), "f64", 0);

        var metadata = await reader.ReadMetadataAsync();
        Assert.All(metadata.RowGroups[0].Columns,
            column => Assert.Contains(Encoding.Alp, column.MetaData!.Encodings));
    }

    private static void AssertFloatColumnsBitEqual(
        FloatArray expected,
        FloatArray actual,
        string columnName,
        int rowGroupIndex)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int row = 0; row < expected.Length; row++)
        {
            Assert.True(
                expected.IsNull(row) == actual.IsNull(row),
                $"Null mismatch in row group {rowGroupIndex}, column '{columnName}', row {row}.");
            if (!expected.IsNull(row))
            {
                int expectedBits = SingleToInt32Bits(expected.GetValue(row)!.Value);
                int actualBits = SingleToInt32Bits(actual.GetValue(row)!.Value);
                Assert.True(
                    expectedBits == actualBits,
                    $"Bit mismatch in row group {rowGroupIndex}, column '{columnName}', row {row}: " +
                    $"expected 0x{expectedBits:X8}, actual 0x{actualBits:X8}.");
            }
        }
    }

    private static int SingleToInt32Bits(float value) =>
        BitConverter.ToInt32(BitConverter.GetBytes(value), 0);

    private static void AssertDoubleColumnsBitEqual(
        DoubleArray expected,
        DoubleArray actual,
        string columnName,
        int rowGroupIndex)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int row = 0; row < expected.Length; row++)
        {
            Assert.True(
                expected.IsNull(row) == actual.IsNull(row),
                $"Null mismatch in row group {rowGroupIndex}, column '{columnName}', row {row}.");
            if (!expected.IsNull(row))
            {
                long expectedBits = BitConverter.DoubleToInt64Bits(expected.GetValue(row)!.Value);
                long actualBits = BitConverter.DoubleToInt64Bits(actual.GetValue(row)!.Value);
                Assert.True(
                    expectedBits == actualBits,
                    $"Bit mismatch in row group {rowGroupIndex}, column '{columnName}', row {row}: " +
                    $"expected 0x{expectedBits:X16}, actual 0x{actualBits:X16}.");
            }
        }
    }
}
