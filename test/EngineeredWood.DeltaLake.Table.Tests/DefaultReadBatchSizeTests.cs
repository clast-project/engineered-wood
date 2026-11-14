// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A Delta read with no batch limit set reads data files in batches of <see cref="DeltaTable.DefaultReadBatchSize"/>
/// rows, rather than one batch per row group (#652): a row group decoded whole held gigabytes.
/// </summary>
public class DefaultReadBatchSizeTests : IDisposable
{
    // One row group, larger than the default batch: the default must cut it.
    private const int Rows = DeltaTable.DefaultReadBatchSize + 100_000;
    private readonly string _tempDir;

    public DefaultReadBatchSizeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_batch_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private async Task CreateAsync()
    {
        var ids = new Int64Array.Builder().Reserve(Rows);
        for (int i = 0; i < Rows; i++)
            ids.Append(i);
        await using var table = await DeltaTable.CreateAsync(Fs, Schema, new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = Rows },
        });
        using var batch = new RecordBatch(Schema, [ids.Build()], Rows);
        await table.WriteAsync([batch]);
    }

    /// <summary>Scans the table with <paramref name="read"/> as its Parquet read options; returns each batch's
    /// rows, checking the ids come back in order.</summary>
    private async Task<List<int>> ScanAsync(ParquetReadOptions read, int maxConcurrentReads = 1)
    {
        await using var table = await DeltaTable.OpenAsync(Fs, new DeltaTableOptions
        {
            ParquetReadOptions = read,
            MaxConcurrentReads = maxConcurrentReads,
        });
        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        var sizes = new List<int>();
        long next = 0;
        await foreach (var batch in table.ReadAsync())
        {
            using (batch)
            {
                var ids = (Int64Array)batch.Column(0);
                for (int i = 0; i < batch.Length; i++)
                    Assert.Equal(next++, ids.GetValue(i));
                sizes.Add(batch.Length);
            }
        }

        Assert.Equal(Rows, next);
        return sizes;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task WithNoBatchLimit_ARowGroupIsReadInDefaultSizedBatches(int maxConcurrentReads)
    {
        await CreateAsync();
        var sizes = await ScanAsync(ParquetReadOptions.Default, maxConcurrentReads);
        Assert.Equal([DeltaTable.DefaultReadBatchSize, 100_000], sizes);
    }

    /// <summary>The change-file rewrite (a CDF row-id rebase) reads with the data files' batching, the default row
    /// limit included (#874 review round 2).</summary>
    [Theory]
    [InlineData(null, null, DeltaTable.DefaultReadBatchSize, null)]
    [InlineData(100_000, null, 100_000, null)]
    [InlineData(null, 1L << 20, null, 1L << 20)]
    public async Task TheChangeFileRewrite_ReadsWithTheDataFilesBatching(
        int? batchSize, long? maxBytes, int? expectedRows, long? expectedBytes)
    {
        await CreateAsync();
        await using var table = await DeltaTable.OpenAsync(Fs, new DeltaTableOptions
        {
            ParquetReadOptions = ParquetReadOptions.Default with { BatchSize = batchSize, MaxBatchByteSize = maxBytes },
        });
        var options = table.ChangeFileRewriteReadOptions;
        Assert.Equal(expectedRows, options.BatchSize);
        Assert.Equal(expectedBytes, options.MaxBatchByteSize);
    }

    [Fact]
    public async Task AnExplicitBatchSize_IsKept()
    {
        await CreateAsync();
        var sizes = await ScanAsync(ParquetReadOptions.Default with { BatchSize = 100_000 });
        Assert.All(sizes, rows => Assert.InRange(rows, 1, 100_000));
        Assert.True(sizes.Count >= Rows / 100_000);
    }

    [Fact]
    public async Task ABatchSizeOfIntMaxValue_ReadsEachRowGroupAsOneBatch()
    {
        await CreateAsync();
        var sizes = await ScanAsync(ParquetReadOptions.Default with { BatchSize = int.MaxValue });
        Assert.Equal([Rows], sizes);
    }

    [Fact]
    public async Task AMaxBatchByteSizeAlone_IsNotGivenTheDefaultRowLimit()
    {
        await CreateAsync();
        // Well over the row group's decoded size (8 bytes a row): with no row limit added, one batch.
        var sizes = await ScanAsync(ParquetReadOptions.Default with { MaxBatchByteSize = 1L << 30 });
        Assert.Equal([Rows], sizes);
    }
}
