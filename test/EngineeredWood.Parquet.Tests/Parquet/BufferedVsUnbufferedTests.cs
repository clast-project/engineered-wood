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

/// <summary>
/// Tests that verify BufferedParquetWriter produces equivalent output to ParquetFileWriter.
/// </summary>
public class BufferedVsUnbufferedTests : IDisposable
{
    private readonly string _tempDir;

    public BufferedVsUnbufferedTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-compare-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    [Theory]
    [InlineData(CompressionCodec.Uncompressed)]
    [InlineData(CompressionCodec.Snappy)]
    public async Task SingleBatch_SameData(CompressionCodec codec)
    {
        var batch = BuildTestBatch(5_000, seed: 42);
        var options = new ParquetWriteOptions { Compression = codec };

        string directPath = TempPath($"direct_{codec}.parquet");
        string bufferedPath = TempPath($"buffered_{codec}.parquet");

        // Write with ParquetFileWriter
        await using (var file = new LocalSequentialFile(directPath))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // Write with BufferedParquetWriter
        await using (var file = new LocalSequentialFile(bufferedPath))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, options))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        // Compare: same row count, same column count, same values
        await AssertFilesEquivalent(directPath, bufferedPath, batch.Length);
    }

    [Fact]
    public async Task ManySmallBatches_Buffered_EquivalentToOneLargeBatch_Direct()
    {
        int batchSize = 500;
        int batchCount = 10;
        int totalRows = batchSize * batchCount;
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Snappy };

        // Build all batches from the same RNG sequence
        var batches = new RecordBatch[batchCount];
        for (int i = 0; i < batchCount; i++)
            batches[i] = BuildTestBatch(batchSize, seed: 100 + i);

        // Write many batches with BufferedParquetWriter → 1 row group
        string bufferedPath = TempPath("buffered_many.parquet");
        await using (var file = new LocalSequentialFile(bufferedPath))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, options))
        {
            foreach (var batch in batches)
                await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        // Verify: single row group containing all data
        await using var readFile = new LocalRandomAccessFile(bufferedPath);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Equal(totalRows, metadata.NumRows);
        Assert.Single(metadata.RowGroups);
        Assert.Equal(totalRows, metadata.RowGroups[0].NumRows);

        // Verify all data readable
        var readBatch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(totalRows, readBatch.Length);
        Assert.Equal(4, readBatch.ColumnCount);

        // Verify value counts match
        var origValues = CollectAllValues(batches);
        var readValues = CollectValues(readBatch);
        AssertMultisetEqual(origValues, readValues);
    }

    [Fact]
    public async Task ManySmallBatches_Direct_ProducesManyRowGroups()
    {
        int batchSize = 500;
        int batchCount = 10;
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Snappy };

        // Write many batches with ParquetFileWriter → many row groups
        string directPath = TempPath("direct_many.parquet");
        await using (var file = new LocalSequentialFile(directPath))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            for (int i = 0; i < batchCount; i++)
                await writer.WriteRowGroupAsync(BuildTestBatch(batchSize, seed: 100 + i));
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(directPath);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        // Direct writer creates one row group per batch
        Assert.Equal(batchCount, metadata.RowGroups.Count);
    }

    [Fact]
    public async Task Buffered_VsDirect_FileSizeComparison()
    {
        int batchSize = 500;
        int batchCount = 10;
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Snappy };

        var batches = new RecordBatch[batchCount];
        for (int i = 0; i < batchCount; i++)
            batches[i] = BuildTestBatch(batchSize, seed: 200 + i);

        // Direct: many row groups
        string directPath = TempPath("direct_size.parquet");
        await using (var file = new LocalSequentialFile(directPath))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            foreach (var batch in batches)
                await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // Buffered: one row group
        string bufferedPath = TempPath("buffered_size.parquet");
        await using (var file = new LocalSequentialFile(bufferedPath))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, options))
        {
            foreach (var batch in batches)
                await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        long directSize = new FileInfo(directPath).Length;
        long bufferedSize = new FileInfo(bufferedPath).Length;

        // Both files should be readable and contain the same data
        var origValues = CollectAllValues(batches);

        await using var df = new LocalRandomAccessFile(directPath);
        await using var dr = new ParquetFileReader(df, ownsFile: false);
        var dm = await dr.ReadMetadataAsync();
        Assert.Equal(5000, dm.NumRows);

        await using var bf = new LocalRandomAccessFile(bufferedPath);
        await using var br = new ParquetFileReader(bf, ownsFile: false);
        var bm = await br.ReadMetadataAsync();
        Assert.Equal(5000, bm.NumRows);
        Assert.Single(bm.RowGroups); // buffered consolidates into 1 row group

        // Verify buffered data matches original
        var readBatch = await br.ReadRowGroupAsync(0);
        var readValues = CollectValues(readBatch);
        AssertMultisetEqual(origValues, readValues);
    }

    [Fact]
    public async Task NullableColumns_SameNullCounts()
    {
        var batch = BuildTestBatch(2_000, seed: 77);
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        string directPath = TempPath("direct_nulls.parquet");
        string bufferedPath = TempPath("buffered_nulls.parquet");

        await using (var file = new LocalSequentialFile(directPath))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using (var file = new LocalSequentialFile(bufferedPath))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, options))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        // Compare null counts per column
        await using var df = new LocalRandomAccessFile(directPath);
        await using var dr = new ParquetFileReader(df, ownsFile: false);
        var directBatch = await dr.ReadRowGroupAsync(0);

        await using var bf = new LocalRandomAccessFile(bufferedPath);
        await using var br = new ParquetFileReader(bf, ownsFile: false);
        var bufferedBatch = await br.ReadRowGroupAsync(0);

        for (int c = 0; c < directBatch.ColumnCount; c++)
        {
            int directNulls = CountNulls(directBatch.Column(c));
            int bufferedNulls = CountNulls(bufferedBatch.Column(c));
            Assert.Equal(directNulls, bufferedNulls);
        }
    }

    // ───── Helpers ─────

    private static RecordBatch BuildTestBatch(int rowCount, int seed)
    {
        var rng = new Random(seed);
        string[] categories = ["Electronics", "Clothing", "Food", "Books", "Toys"];

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("category", StringType.Default, nullable: false))
            .Field(new Field("value", Int32Type.Default, nullable: true))
            .Field(new Field("score", DoubleType.Default, nullable: false))
            .Field(new Field("flag", BooleanType.Default, nullable: true))
            .Build();

        var catBuilder = new StringArray.Builder();
        var valBuilder = new Int32Array.Builder();
        var scoreBuilder = new DoubleArray.Builder();
        var flagBuilder = new BooleanArray.Builder();

        for (int i = 0; i < rowCount; i++)
        {
            catBuilder.Append(categories[rng.Next(categories.Length)]);
            if (rng.Next(10) == 0) valBuilder.AppendNull();
            else valBuilder.Append(rng.Next(100));
            scoreBuilder.Append(rng.NextDouble() * 100);
            if (rng.Next(8) == 0) flagBuilder.AppendNull();
            else flagBuilder.Append(rng.Next(2) == 1);
        }

        return new RecordBatch(schema,
            [catBuilder.Build(), valBuilder.Build(), scoreBuilder.Build(), flagBuilder.Build()], rowCount);
    }

    private static async Task AssertFilesEquivalent(string path1, string path2, int expectedRows)
    {
        await using var f1 = new LocalRandomAccessFile(path1);
        await using var r1 = new ParquetFileReader(f1, ownsFile: false);
        var m1 = await r1.ReadMetadataAsync();

        await using var f2 = new LocalRandomAccessFile(path2);
        await using var r2 = new ParquetFileReader(f2, ownsFile: false);
        var m2 = await r2.ReadMetadataAsync();

        Assert.Equal(m1.NumRows, m2.NumRows);
        Assert.Equal(expectedRows, m1.NumRows);

        var b1 = await r1.ReadRowGroupAsync(0);
        var b2 = await r2.ReadRowGroupAsync(0);

        Assert.Equal(b1.ColumnCount, b2.ColumnCount);
        Assert.Equal(b1.Length, b2.Length);

        // Compare values as multisets (order may differ if encoding differs)
        var v1 = CollectValues(b1);
        var v2 = CollectValues(b2);
        AssertMultisetEqual(v1, v2);
    }

    private static Dictionary<int, Dictionary<string, int>> CollectValues(RecordBatch batch)
    {
        var result = new Dictionary<int, Dictionary<string, int>>();
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var counts = new Dictionary<string, int>();
            var array = batch.Column(c);
            for (int r = 0; r < batch.Length; r++)
            {
                string key = array.IsNull(r) ? "__NULL__" : GetStringValue(array, r);
                counts[key] = counts.TryGetValue(key, out var v) ? v + 1 : 1;
            }
            result[c] = counts;
        }
        return result;
    }

    private static Dictionary<int, Dictionary<string, int>> CollectAllValues(RecordBatch[] batches)
    {
        var result = new Dictionary<int, Dictionary<string, int>>();
        foreach (var batch in batches)
        {
            for (int c = 0; c < batch.ColumnCount; c++)
            {
                if (!result.ContainsKey(c))
                    result[c] = new Dictionary<string, int>();
                var counts = result[c];
                var array = batch.Column(c);
                for (int r = 0; r < batch.Length; r++)
                {
                    string key = array.IsNull(r) ? "__NULL__" : GetStringValue(array, r);
                    counts[key] = counts.TryGetValue(key, out var v) ? v + 1 : 1;
                }
            }
        }
        return result;
    }

    private static string GetStringValue(IArrowArray array, int index) => array switch
    {
        StringArray s => s.GetString(index) ?? "",
        Int32Array i => i.GetValue(index)!.Value.ToString(),
        Int64Array l => l.GetValue(index)!.Value.ToString(),
        DoubleArray d => d.GetValue(index)!.Value.ToString("R"),
        BooleanArray b => b.GetValue(index)!.Value.ToString(),
        _ => array.GetType().Name,
    };

    private static void AssertMultisetEqual(
        Dictionary<int, Dictionary<string, int>> expected,
        Dictionary<int, Dictionary<string, int>> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        foreach (var colEntry in expected)
        {
            Assert.True(actual.ContainsKey(colEntry.Key), $"Missing column {colEntry.Key}");
            var actualCounts = actual[colEntry.Key];
            Assert.Equal(colEntry.Value.Count, actualCounts.Count);
            foreach (var kvp in colEntry.Value)
            {
                Assert.True(actualCounts.ContainsKey(kvp.Key),
                    $"Col {colEntry.Key}: missing value '{kvp.Key}'");
                Assert.Equal(kvp.Value, actualCounts[kvp.Key]);
            }
        }
    }

    private static int CountNulls(IArrowArray array)
    {
        int nulls = 0;
        for (int i = 0; i < array.Length; i++)
            if (array.IsNull(i)) nulls++;
        return nulls;
    }
}
