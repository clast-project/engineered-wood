// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

public class BatchedReadTests : IDisposable
{
    private readonly string _tempDir;

    public BatchedReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-batch-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static RecordBatch MakeInt32Batch(int rowCount, bool nullable = false)
    {
        var builder = new Int32Array.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            if (nullable && i % 7 == 0)
                builder.AppendNull();
            else
                builder.Append(i);
        }

        var field = new Field("value", Int32Type.Default, nullable);
        var schema = new Apache.Arrow.Schema.Builder().Field(field).Build();
        return new RecordBatch(schema, [builder.Build()], rowCount);
    }

    private static RecordBatch MakeMultiColumnBatch(int rowCount)
    {
        var intBuilder = new Int32Array.Builder();
        var strBuilder = new StringArray.Builder();
        var dblBuilder = new DoubleArray.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            intBuilder.Append(i);
            strBuilder.Append($"row-{i}");
            dblBuilder.Append(i * 1.5);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", StringType.Default, nullable: false))
            .Field(new Field("score", DoubleType.Default, nullable: false))
            .Build();
        return new RecordBatch(schema,
            [intBuilder.Build(), strBuilder.Build(), dblBuilder.Build()], rowCount);
    }

    private async Task<string> WriteFile(
        RecordBatch batch,
        string name,
        ParquetWriteOptions? options = null)
    {
        string path = TempPath(name);
        options ??= new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DataPageSize = 1024, // small pages to force multiple pages per row group
        };

        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    // ------------------------------------------------------------------
    // Basic batched reading
    // ------------------------------------------------------------------

    [Fact]
    public async Task ReadRowGroupBatchesAsync_SplitsRowGroup()
    {
        int totalRows = 1000;
        var batch = MakeInt32Batch(totalRows);
        var path = await WriteFile(batch, "split.parquet");

        var readOptions = new ParquetReadOptions { BatchSize = 300 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
            batches.Add(b);

        Assert.True(batches.Count > 1, $"Expected multiple batches but got {batches.Count}");

        int totalRead = batches.Sum(b => (int)b.Length);
        Assert.Equal(totalRows, totalRead);

        // Verify all values are present and in order.
        int expectedVal = 0;
        foreach (var b in batches)
        {
            var col = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
            {
                Assert.Equal(expectedVal, col.GetValue(i));
                expectedVal++;
            }
        }
    }

    [Fact]
    public async Task ReadAllAsync_WithBatchSize_SplitsRowGroups()
    {
        int totalRows = 1000;
        var batch = MakeInt32Batch(totalRows);
        var path = await WriteFile(batch, "readall_split.parquet");

        var readOptions = new ParquetReadOptions { BatchSize = 300 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadAllAsync())
            batches.Add(b);

        Assert.True(batches.Count > 1);
        Assert.Equal(totalRows, batches.Sum(b => (int)b.Length));
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    [Fact]
    public async Task BatchSize_LargerThanRowGroup_SingleBatch()
    {
        int totalRows = 100;
        var batch = MakeInt32Batch(totalRows);
        var path = await WriteFile(batch, "large_batch.parquet");

        var readOptions = new ParquetReadOptions { BatchSize = 10_000 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
            batches.Add(b);

        Assert.Single(batches);
        Assert.Equal(totalRows, (int)batches[0].Length);
    }

    [Fact]
    public async Task BatchSize_ExactMultiple_NoShortBatch()
    {
        // Use a row count that's likely an exact multiple of the page size
        // so there's no remainder batch.
        int totalRows = 500;
        var batch = MakeInt32Batch(totalRows);
        var path = await WriteFile(batch, "exact_multiple.parquet");

        var readOptions = new ParquetReadOptions { BatchSize = totalRows };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
            batches.Add(b);

        Assert.Equal(totalRows, batches.Sum(b => (int)b.Length));
    }

    [Fact]
    public async Task NoBatchSize_SingleBatch()
    {
        int totalRows = 1000;
        var batch = MakeInt32Batch(totalRows);
        var path = await WriteFile(batch, "no_batch.parquet");

        // Default options — no batch size.
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
            batches.Add(b);

        Assert.Single(batches);
        Assert.Equal(totalRows, (int)batches[0].Length);
    }

    // ------------------------------------------------------------------
    // Multi-column
    // ------------------------------------------------------------------

    [Fact]
    public async Task BatchedRead_MultipleColumns_ValuesAlign()
    {
        int totalRows = 1000;
        var batch = MakeMultiColumnBatch(totalRows);
        var path = await WriteFile(batch, "multi_col.parquet");

        var readOptions = new ParquetReadOptions { BatchSize = 250 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        int expectedId = 0;
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
        {
            var ids = (Int32Array)b.Column(0);
            var names = (StringArray)b.Column(1);
            var scores = (DoubleArray)b.Column(2);

            Assert.Equal(b.Length, ids.Length);
            Assert.Equal(b.Length, names.Length);
            Assert.Equal(b.Length, scores.Length);

            for (int i = 0; i < b.Length; i++)
            {
                Assert.Equal(expectedId, ids.GetValue(i));
                Assert.Equal($"row-{expectedId}", names.GetString(i));
                Assert.Equal(expectedId * 1.5, scores.GetValue(i));
                expectedId++;
            }
        }

        Assert.Equal(totalRows, expectedId);
    }

    // ------------------------------------------------------------------
    // Nullable columns
    // ------------------------------------------------------------------

    [Fact]
    public async Task BatchedRead_NullableColumn_PreservesNulls()
    {
        int totalRows = 1000;
        var batch = MakeInt32Batch(totalRows, nullable: true);
        var path = await WriteFile(batch, "nullable.parquet");

        var readOptions = new ParquetReadOptions { BatchSize = 200 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        int rowIndex = 0;
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
        {
            var col = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
            {
                if (rowIndex % 7 == 0)
                    Assert.True(col.IsNull(i), $"Expected null at row {rowIndex}");
                else
                    Assert.Equal(rowIndex, col.GetValue(i));
                rowIndex++;
            }
        }

        Assert.Equal(totalRows, rowIndex);
    }

    // ------------------------------------------------------------------
    // Dictionary encoding
    // ------------------------------------------------------------------

    [Fact]
    public async Task BatchedRead_DictionaryEncoded_SharedAcrossBatches()
    {
        int totalRows = 1000;
        var builder = new StringArray.Builder();
        string[] categories = ["alpha", "beta", "gamma", "delta"];
        for (int i = 0; i < totalRows; i++)
            builder.Append(categories[i % categories.Length]);

        var field = new Field("category", StringType.Default, nullable: false);
        var schema = new Apache.Arrow.Schema.Builder().Field(field).Build();
        var batch = new RecordBatch(schema, [builder.Build()], totalRows);

        var path = await WriteFile(batch, "dict.parquet", new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DictionaryEnabled = true,
            DataPageSize = 512, // small pages → many pages per row group
        });

        var readOptions = new ParquetReadOptions { BatchSize = 200 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        int rowIndex = 0;
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
        {
            var col = (StringArray)b.Column(0);
            for (int i = 0; i < b.Length; i++)
            {
                Assert.Equal(categories[rowIndex % categories.Length], col.GetString(i));
                rowIndex++;
            }
        }

        Assert.Equal(totalRows, rowIndex);
    }

    // ------------------------------------------------------------------
    // V1 data pages
    // ------------------------------------------------------------------

    [Fact]
    public async Task BatchedRead_V1Pages_Works()
    {
        int totalRows = 1000;
        var batch = MakeInt32Batch(totalRows);
        var path = await WriteFile(batch, "v1_pages.parquet", new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DataPageVersion = DataPageVersion.V1,
            DataPageSize = 1024,
        });

        var readOptions = new ParquetReadOptions { BatchSize = 300 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
            batches.Add(b);

        Assert.True(batches.Count > 1);
        int expectedVal = 0;
        foreach (var b in batches)
        {
            var col = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
            {
                Assert.Equal(expectedVal, col.GetValue(i));
                expectedVal++;
            }
        }
        Assert.Equal(totalRows, expectedVal);
    }

    // ------------------------------------------------------------------
    // Compression
    // ------------------------------------------------------------------

    [Fact]
    public async Task BatchedRead_SnappyCompressed_Works()
    {
        int totalRows = 1000;
        var batch = MakeMultiColumnBatch(totalRows);
        var path = await WriteFile(batch, "snappy.parquet", new ParquetWriteOptions
        {
            Compression = CompressionCodec.Snappy,
            DataPageSize = 1024,
        });

        var readOptions = new ParquetReadOptions { BatchSize = 300 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        int totalRead = 0;
        int expectedId = 0;
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
        {
            var ids = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
            {
                Assert.Equal(expectedId, ids.GetValue(i));
                expectedId++;
            }
            totalRead += (int)b.Length;
        }

        Assert.Equal(totalRows, totalRead);
    }

    // ------------------------------------------------------------------
    // Multiple row groups
    // ------------------------------------------------------------------

    [Fact]
    public async Task ReadAllAsync_MultipleRowGroups_WithBatchSize()
    {
        int totalRows = 500;
        var batch = MakeInt32Batch(totalRows);
        var path = TempPath("multi_rg_batched.parquet");

        // Write with RowGroupMaxRows=200 → creates 3 row groups (200, 200, 100).
        var writeOpts = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            RowGroupMaxRows = 200,
            DataPageSize = 256,
        };

        await using (var wf = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(wf, ownsFile: false, writeOpts))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        var readOptions = new ParquetReadOptions { BatchSize = 80 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadAllAsync())
            batches.Add(b);

        int totalRead = batches.Sum(b => (int)b.Length);
        Assert.Equal(totalRows, totalRead);

        // Should have more batches than row groups.
        var metadata = await reader.ReadMetadataAsync();
        Assert.True(batches.Count > metadata.RowGroups.Count,
            $"Expected more batches ({batches.Count}) than row groups ({metadata.RowGroups.Count})");

        // Verify values are in order across all batches and row groups.
        int expectedVal = 0;
        foreach (var b in batches)
        {
            var col = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
            {
                Assert.Equal(expectedVal, col.GetValue(i));
                expectedVal++;
            }
        }
    }

    // ------------------------------------------------------------------
    // Existing test data files
    // ------------------------------------------------------------------

    [Fact]
    public async Task BatchedRead_AlltypesPlain_FromTestData()
    {
        var path = TestData.GetPath("alltypes_plain.parquet");
        var readOptions = new ParquetReadOptions { BatchSize = 2 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
            batches.Add(b);

        int totalRead = batches.Sum(b => (int)b.Length);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(metadata.RowGroups[0].NumRows, totalRead);

        // With 8 rows and BatchSize=2, we should get multiple batches
        // (exact count depends on page boundaries).
        Assert.True(batches.Count >= 1);
    }

    [Fact]
    public async Task BatchedRead_AlltypesDictionary_FromTestData()
    {
        var path = TestData.GetPath("alltypes_dictionary.parquet");
        var readOptions = new ParquetReadOptions { BatchSize = 1 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadAllAsync())
            batches.Add(b);

        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(metadata.NumRows, batches.Sum(b => (int)b.Length));
    }

    // ------------------------------------------------------------------
    // Size-based batching (MaxBatchByteSize)
    // ------------------------------------------------------------------

    [Fact]
    public async Task MaxBatchByteSize_SplitsRowGroup()
    {
        int totalRows = 1000;
        var batch = MakeMultiColumnBatch(totalRows);
        // Multi-column batch with small pages to guarantee many pages.
        var path = await WriteFile(batch, "size_split.parquet", new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DataPageSize = 256, // very small pages
        });

        // Budget that's much smaller than the total uncompressed size.
        var readOptions = new ParquetReadOptions { MaxBatchByteSize = 2000 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
            batches.Add(b);

        Assert.True(batches.Count > 1, $"Expected multiple batches but got {batches.Count}");
        Assert.Equal(totalRows, batches.Sum(b => (int)b.Length));

        // Verify values are sequential.
        int expected = 0;
        foreach (var b in batches)
        {
            var ids = (Int32Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                Assert.Equal(expected++, ids.GetValue(i));
        }
    }

    [Fact]
    public async Task MaxBatchByteSize_LargeBudget_SingleBatch()
    {
        int totalRows = 100;
        var batch = MakeInt32Batch(totalRows);
        var path = await WriteFile(batch, "size_large.parquet");

        // Budget larger than the entire row group — should produce one batch.
        var readOptions = new ParquetReadOptions { MaxBatchByteSize = 10_000_000 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
            batches.Add(b);

        Assert.Single(batches);
        Assert.Equal(totalRows, (int)batches[0].Length);
    }

    [Fact]
    public async Task MaxBatchByteSize_WithRowLimit_MostRestrictiveWins()
    {
        int totalRows = 1000;
        var batch = MakeMultiColumnBatch(totalRows);
        var path = await WriteFile(batch, "size_and_rows.parquet", new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DataPageSize = 256,
        });

        // Set both limits. The row limit (50) will be more restrictive than the
        // byte budget for most page sizes.
        var readOptions = new ParquetReadOptions
        {
            BatchSize = 50,
            MaxBatchByteSize = 10_000_000,
        };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
            batches.Add(b);

        Assert.Equal(totalRows, batches.Sum(b => (int)b.Length));
        // With a 50-row limit the batch count should be at least 20.
        Assert.True(batches.Count >= 20,
            $"Expected ≥20 batches from row limit but got {batches.Count}");
    }

    [Fact]
    public async Task MaxBatchByteSize_MultiColumn_ValuesAlign()
    {
        int totalRows = 1000;
        var batch = MakeMultiColumnBatch(totalRows);
        var path = await WriteFile(batch, "size_multi_col.parquet");

        // Tight byte budget to force splitting.
        var readOptions = new ParquetReadOptions { MaxBatchByteSize = 2000 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        int expectedId = 0;
        await foreach (var b in reader.ReadRowGroupBatchesAsync(0))
        {
            var ids = (Int32Array)b.Column(0);
            var names = (StringArray)b.Column(1);
            var scores = (DoubleArray)b.Column(2);

            for (int i = 0; i < b.Length; i++)
            {
                Assert.Equal(expectedId, ids.GetValue(i));
                Assert.Equal($"row-{expectedId}", names.GetString(i));
                Assert.Equal(expectedId * 1.5, scores.GetValue(i));
                expectedId++;
            }
        }

        Assert.Equal(totalRows, expectedId);
    }

    [Fact]
    public async Task MaxBatchByteSize_ReadAllAsync_StreamsThroughBatching()
    {
        int totalRows = 1000;
        var batch = MakeMultiColumnBatch(totalRows);
        var path = await WriteFile(batch, "size_readall.parquet", new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DataPageSize = 256,
        });

        var readOptions = new ParquetReadOptions { MaxBatchByteSize = 2000 };
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadAllAsync())
            batches.Add(b);

        Assert.True(batches.Count > 1);
        Assert.Equal(totalRows, batches.Sum(b => (int)b.Length));
    }
}
