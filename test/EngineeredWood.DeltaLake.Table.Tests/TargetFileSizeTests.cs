// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A write goes to as few data files as <see cref="DeltaTableOptions.TargetFileSize"/> allows, however the caller
/// batched its rows (#559). It used to write one file per input batch, each with a single row group the size of
/// that batch, and <c>TargetFileSize</c> was read by nothing.
/// </summary>
public class TargetFileSizeTests : IDisposable
{
    private readonly string _tempDir;

    public TargetFileSizeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_target_{Guid.NewGuid():N}");
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

    private static Apache.Arrow.Schema BuildSchema(bool idNullable = false) => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, idNullable))
        .Field(new Field("part", StringType.Default, true))
        .Build();

    /// <summary>Rows <paramref name="startId"/>.. of partition values cycling through
    /// <paramref name="parts"/>.</summary>
    private static RecordBatch Batch(long startId, int count, string[]? parts = null, bool idNullable = false)
    {
        var ids = new Int64Array.Builder();
        var part = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            part.Append(parts is null ? "p" : parts[i % parts.Length]);
        }
        return new RecordBatch(BuildSchema(idNullable), [ids.Build(), part.Build()], count);
    }

    private static List<RecordBatch> Batches(int batches, int rowsEach, string[]? parts = null)
    {
        var list = new List<RecordBatch>();
        for (int b = 0; b < batches; b++)
            list.Add(Batch(b * (long)rowsEach, rowsEach, parts));
        return list;
    }

    private string[] DataFilesOnDisk() =>
        [.. Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(p => !p.Contains("_delta_log", StringComparison.Ordinal))];

    private async Task<int> RowGroupCountAsync(string relativePath)
    {
        await using var file = await Fs.OpenReadAsync(DeltaPath.Decode(relativePath));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        return (await reader.ReadMetadataAsync()).RowGroups.Count;
    }

    private static async Task<List<long>> ReadIdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    private static List<long> Range(long start, long count) =>
        [.. Enumerable.Range(0, checked((int)count)).Select(i => start + i)];

    [Fact]
    public async Task ManySmallBatches_GoToOneFileWithOneRowGroup()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync(Batches(batches: 40, rowsEach: 250));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal(1, await RowGroupCountAsync(file.Path));
        Assert.Equal(Range(0, 10_000), await ReadIdsAsync(table));
    }

    /// <summary>The file's statistics cover every batch in it, not just one.</summary>
    [Fact]
    public async Task TheFilesStatistics_SpanEveryBatchInIt()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(500, 10), Batch(0, 10), Batch(1000, 10)]);

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        using var stats = JsonDocument.Parse(file.GetStatsJson()!);
        Assert.Equal(30, stats.RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal(0, stats.RootElement.GetProperty("minValues").GetProperty("id").GetInt64());
        Assert.Equal(1009, stats.RootElement.GetProperty("maxValues").GetProperty("id").GetInt64());
    }

    /// <summary>
    /// Batches join into row groups of at most <see cref="ParquetWriteOptions.RowGroupMaxRows"/> rows, and a file
    /// closes once it reaches the target. A target of one byte closes every file after its first row group.
    /// </summary>
    [Fact]
    public async Task AFileClosesAtTheTarget_AfterARowGroup()
    {
        var options = new DeltaTableOptions
        {
            TargetFileSize = 1,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);
        await table.WriteAsync(Batches(batches: 10, rowsEach: 250));

        var files = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(3, files.Count); // 1000 + 1000 + 500 rows
        foreach (var file in files)
            Assert.Equal(1, await RowGroupCountAsync(file.Path));
        Assert.Equal(new long[] { 500, 1000, 1000 },
            files.Select(f => JsonDocument.Parse(f.GetStatsJson()!).RootElement.GetProperty("numRecords").GetInt64())
                .OrderBy(n => n));
        Assert.Equal(Range(0, 2500), await ReadIdsAsync(table));
    }

    /// <summary>Under a large target, the same row groups share one file.</summary>
    [Fact]
    public async Task UnderTheTarget_RowGroupsShareAFile()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 1000 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);
        await table.WriteAsync(Batches(batches: 10, rowsEach: 250));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal(3, await RowGroupCountAsync(file.Path));
    }

    /// <summary>Each partition gets its own file, gathering its rows from every batch.</summary>
    [Fact]
    public async Task APartitionedWrite_GivesEachPartitionOneFile()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), partitionColumns: ["part"]);
        await table.WriteAsync(Batches(batches: 12, rowsEach: 100, parts: ["a", "b", "c"]));

        var files = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        Assert.Equal(3, files.Count);
        Assert.Equal(new[] { "a", "b", "c" }, files.Select(f => f.PartitionValues["part"]).OrderBy(p => p));
        Assert.Equal(Range(0, 1200), await ReadIdsAsync(table));
    }

    /// <summary>Row tracking assigns each file its own contiguous range: every row's id is distinct, and the
    /// high-water mark covers them all.</summary>
    [Fact]
    public async Task OnARowTrackingTable_EveryRowGetsADistinctId()
    {
        var options = new DeltaTableOptions
        {
            TargetFileSize = 1,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 300 },
        };
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), options, partitionColumns: ["part"], enableRowTracking: true);
        await table.WriteAsync(Batches(batches: 8, rowsEach: 100, parts: ["a", "b"]));

        Assert.True(table.CurrentSnapshot.FileCount > 2);
        var rowIds = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var col = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                rowIds.Add(col.GetValue(i)!.Value);
        }
        Assert.Equal(Range(0, 800), rowIds.OrderBy(id => id).ToList());
        Assert.Equal(800, table.CurrentSnapshot.RowIdHighWaterMark);
    }

    /// <summary>
    /// A batch of a different Arrow layout cannot always share the open file's Parquet schema, so it starts a new
    /// file rather than being refused. Here the first batch declares <c>id</c> non-nullable, so the file's column
    /// is required, and the next one holds a null in it, which a required column cannot store.
    /// </summary>
    [Fact]
    public async Task ABatchOfADifferentLayout_StartsANewFile()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(idNullable: true));
        var withNull = new RecordBatch(BuildSchema(idNullable: true),
            [new Int64Array.Builder().Append(10).AppendNull().Build(),
             new StringArray.Builder().Append("p").Append("p").Build()], 2);
        await table.WriteAsync([Batch(0, 10, idNullable: false), withNull, Batch(20, 10, idNullable: true)]);

        Assert.Equal(2, table.CurrentSnapshot.FileCount);
        long rows = 0, nulls = 0;
        await foreach (var batch in table.ReadAllAsync())
        {
            rows += batch.Length;
            nulls += batch.Column("id").NullCount;
        }
        Assert.Equal(22, rows);
        Assert.Equal(1, nulls);
    }

    /// <summary>In a transaction every rolled file is on the ledger, so an abort takes them all back.</summary>
    [Fact]
    public async Task InATransaction_AnAbortDeletesEveryRolledFile()
    {
        var options = new DeltaTableOptions
        {
            TargetFileSize = 1,
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 100 },
        };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);

        var txn = table.StartTransaction();
        await txn.WriteAsync(Batches(batches: 5, rowsEach: 100));
        Assert.Equal(5, DataFilesOnDisk().Length);
        await txn.AbortAsync();

        Assert.Empty(DataFilesOnDisk());
    }

    /// <summary>
    /// A host writer is handed each file's batches as they came, to cut its own row groups. Its output size is
    /// known only once it returns, so its files are cut by the batches' Arrow size.
    /// </summary>
    [Fact]
    public async Task AHostWriter_GetsEachFilesBatches_CutByTheirArrowSize()
    {
        var host = new RecordingWriter(Fs);
        long batchBytes = RollingDataFileWriter.EstimatedBytes(Batch(0, 100));
        var options = new DeltaTableOptions { DataFileWriter = host, TargetFileSize = 3 * batchBytes };
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), options);
        await table.WriteAsync(Batches(batches: 7, rowsEach: 100));

        Assert.Equal(new[] { 3, 3, 1 }, host.BatchCounts);
        Assert.Equal(3, table.CurrentSnapshot.FileCount);
        Assert.Equal(Range(0, 700), await ReadIdsAsync(table));
    }

    /// <summary>The host-staging write cuts files the same way, and its materialized row ids keep to their
    /// rows.</summary>
    [Fact]
    public async Task WriteDataFilesAsync_PutsTheBatchesInOneFile_KeepingMaterializedIds()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        var batches = new List<RecordBatch> { Batch(0, 3), Batch(3, 3) };

        var files = await table.WriteDataFilesAsync(
            batches, materializedRowIds: [100, 101, 102, null, 104, 105]);

        var file = Assert.Single(files);
        Assert.Equal(6, file.NumRecords);
        Assert.Equal(1, await RowGroupCountAsync(file.RelativePath));

        string matRowId = RowTrackingConfig
            .TryGetMaterializedColumnNames(table.CurrentSnapshot.Metadata.Configuration).RowIdColumnName!;
        await using var stream = await Fs.OpenReadAsync(file.RelativePath);
        using var reader = new ParquetFileReader(stream, ownsFile: false);
        var written = await reader.ReadRowGroupAsync(0);
        var ids = (Int64Array)written.Column("id");
        var rowIds = (Int64Array)written.Column(matRowId);
        var byId = Enumerable.Range(0, written.Length).ToDictionary(i => ids.GetValue(i)!.Value, i => rowIds.GetValue(i));
        Assert.Equal(new long?[] { 100, 101, 102, null, 104, 105 }, Range(0, 6).Select(id => byId[id]));
    }

    private sealed class RecordingWriter(ITableFileSystem fs) : IDataFileWriter
    {
        public List<int> BatchCounts { get; } = [];

        public async ValueTask<long> WriteAsync(
            IAsyncEnumerable<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            await using var file = await fs.CreateAsync(relativePath, cancellationToken: cancellationToken);
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            int count = 0;
            await foreach (var b in batches.WithCancellation(cancellationToken))
            {
                await writer.WriteRowGroupAsync(b, cancellationToken);
                count++;
            }
            await writer.CloseAsync(cancellationToken);
            BatchCounts.Add(count);
            return file.Position;
        }
    }
}
