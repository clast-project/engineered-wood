// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

public class BufferedParquetWriterTests : IDisposable
{
    private readonly string _tempDir;

    public BufferedParquetWriterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-buffered-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    [Fact]
    public async Task SingleBatch_RoundTrip()
    {
        string path = TempPath("single_batch.parquet");
        var batch = MakeMixedBatch(1000);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        await VerifyRoundTrip(path, batch.Length, batch.ColumnCount);
    }

    [Fact]
    public async Task MultipleBatches_ConsolidateIntoOneRowGroup()
    {
        string path = TempPath("multi_batch.parquet");
        int batchSize = 200;
        int batchCount = 5;

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            for (int i = 0; i < batchCount; i++)
                await writer.AppendAsync(MakeMixedBatch(batchSize, seed: 42 + i));
            await writer.CloseAsync();
        }

        // Should produce ONE row group with 1000 rows (not 5 row groups of 200)
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Equal(1000, metadata.NumRows);
        Assert.Single(metadata.RowGroups);
        Assert.Equal(1000, metadata.RowGroups[0].NumRows);
    }

    [Fact]
    public async Task AutoFlush_WhenRowGroupMaxReached()
    {
        string path = TempPath("auto_flush.parquet");
        int batchSize = 300;

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions
            {
                Compression = CompressionCodec.Uncompressed,
                RowGroupMaxRows = 500,
            }))
        {
            // Append 3 batches of 300 → 900 rows. With maxRows=500:
            // After batch 1: 300 buffered
            // After batch 2: 600 → auto-flush at 500, then 100 remaining + batch 3
            await writer.AppendAsync(MakeMixedBatch(batchSize, seed: 1));
            await writer.AppendAsync(MakeMixedBatch(batchSize, seed: 2));
            await writer.AppendAsync(MakeMixedBatch(batchSize, seed: 3));
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Equal(900, metadata.NumRows);
        // Should have produced 2 row groups (500 + 400)
        Assert.Equal(2, metadata.RowGroups.Count);
    }

    [Fact]
    public async Task MultipleBatches_AllDataPreserved()
    {
        string path = TempPath("data_preserved.parquet");
        var rng = new Random(42);
        string[] categories = ["alpha", "beta", "gamma"];

        int totalRows = 0;
        var allCategories = new List<string>();
        var allValues = new List<int>();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Snappy }))
        {
            for (int batch = 0; batch < 5; batch++)
            {
                int batchSize = 100 + batch * 50;
                var schema = new Apache.Arrow.Schema.Builder()
                    .Field(new Field("cat", StringType.Default, nullable: false))
                    .Field(new Field("val", Int32Type.Default, nullable: false))
                    .Build();

                var catBuilder = new StringArray.Builder();
                var valBuilder = new Int32Array.Builder();
                for (int i = 0; i < batchSize; i++)
                {
                    string cat = categories[rng.Next(categories.Length)];
                    int val = rng.Next(1000);
                    catBuilder.Append(cat);
                    valBuilder.Append(val);
                    allCategories.Add(cat);
                    allValues.Add(val);
                }

                var b = new RecordBatch(schema, [catBuilder.Build(), valBuilder.Build()], batchSize);
                await writer.AppendAsync(b);
                totalRows += batchSize;
            }
            await writer.CloseAsync();
        }

        // Read back and verify all values present
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(totalRows, metadata.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var readCats = (StringArray)readBatch.Column(0);
        var readVals = (Int32Array)readBatch.Column(1);

        var readCatList = new List<string>();
        var readValList = new List<int>();
        for (int i = 0; i < readBatch.Length; i++)
        {
            readCatList.Add(readCats.GetString(i)!);
            readValList.Add(readVals.GetValue(i)!.Value);
        }

        allCategories.Sort();
        readCatList.Sort();
        Assert.Equal(allCategories, readCatList);

        allValues.Sort();
        readValList.Sort();
        Assert.Equal(allValues, readValList);
    }

    [Fact]
    public async Task NullableColumns_HandledCorrectly()
    {
        string path = TempPath("nullable.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, nullable: true))
            .Field(new Field("score", Int32Type.Default, nullable: true))
            .Build();

        int expectedNulls = 0;

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            var rng = new Random(99);
            for (int b = 0; b < 3; b++)
            {
                var nameBuilder = new StringArray.Builder();
                var scoreBuilder = new Int32Array.Builder();
                for (int i = 0; i < 100; i++)
                {
                    if (rng.Next(5) == 0)
                    {
                        nameBuilder.AppendNull();
                        expectedNulls++;
                    }
                    else
                        nameBuilder.Append("item");

                    if (rng.Next(5) == 0)
                        scoreBuilder.AppendNull();
                    else
                        scoreBuilder.Append(rng.Next(100));
                }
                await writer.AppendAsync(new RecordBatch(schema,
                    [nameBuilder.Build(), scoreBuilder.Build()], 100));
            }
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(300, readBatch.Length);
        var names = (StringArray)readBatch.Column(0);
        int actualNulls = 0;
        for (int i = 0; i < names.Length; i++)
            if (names.IsNull(i)) actualNulls++;
        Assert.Equal(expectedNulls, actualNulls);
    }

    private static RecordBatch MakeMixedBatch(int rowCount, int seed = 42)
    {
        var rng = new Random(seed);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", StringType.Default, nullable: true))
            .Field(new Field("value", DoubleType.Default, nullable: false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        var valBuilder = new DoubleArray.Builder();
        string[] names = ["alpha", "beta", "gamma", "delta"];

        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(rng.Next(100));
            if (rng.Next(10) == 0) nameBuilder.AppendNull();
            else nameBuilder.Append(names[rng.Next(names.Length)]);
            valBuilder.Append(rng.NextDouble() * 100);
        }

        return new RecordBatch(schema, [idBuilder.Build(), nameBuilder.Build(), valBuilder.Build()], rowCount);
    }

    private static async Task VerifyRoundTrip(string path, int expectedRows, int expectedCols)
    {
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(expectedRows, metadata.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(expectedRows, readBatch.Length);
        Assert.Equal(expectedCols, readBatch.ColumnCount);
    }
}
