// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// With <see cref="DeltaTableOptions.CollectStats"/> off, an add on a row-tracking table still carries its row count
/// (#742). An add that would otherwise have no stats gets <c>{"numRecords":N}</c> alone; one whose writer collects
/// column stats regardless (an UPDATE/DELETE/MERGE rewrite, IcebergCompat) keeps them. A rebase re-sizes each staged add's
/// row-id range from that count, and threw without it.
///
/// <para>MEASURED against pyspark 4.0.4 / delta-spark 4.0.0 and 4.1.2 / 4.3.1: with
/// <c>spark.databricks.delta.stats.collect=false</c> Spark refuses every INSERT and MERGE on a row-tracking table
/// (<c>DELTA_ROW_ID_ASSIGNMENT_WITHOUT_STATS</c>), so every add in its logs has a count. With
/// <c>delta.dataSkippingNumIndexedCols = 0</c> it writes the count alone, the shape written here. A table without row
/// tracking still gets no stats at all, as in Spark.</para>
/// </summary>
public class RowTrackingWithoutStatsTests : IDisposable
{
    private readonly string _tempDir;

    public RowTrackingWithoutStatsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rtnostats_{Guid.NewGuid():N}");
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

    private static readonly DeltaTableOptions NoStats = new() { CheckpointInterval = 0, CollectStats = false };

    private static readonly Apache.Arrow.Schema FlatSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch B(params long[] ids) => B("v", ids);

    private static RecordBatch B(string value, params long[] ids)
    {
        var id = new Int64Array.Builder();
        var v = new StringArray.Builder();
        foreach (long i in ids)
        {
            id.Append(i);
            v.Append(value);
        }
        return new RecordBatch(FlatSchema, [id.Build(), v.Build()], ids.Length);
    }

    private Task<DeltaTable> CreateAsync(
        bool rowTracking, DeltaTableOptions? options = null, bool deletionVectors = false) =>
        DeltaTable.CreateAsync(
            Fs, FlatSchema, options ?? NoStats, enableRowTracking: rowTracking,
            enableDeletionVectors: deletionVectors).AsTask();

    private async Task<List<AddFile>> AddsAsync(long version) =>
        (await new TransactionLog(Fs).ReadCommitAsync(version)).OfType<AddFile>().ToList();

    private static async Task<List<long>> RowIdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var rowId = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                ids.Add(rowId.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    // Every committed row has its own id, and the ids are 0..n-1: no range overlaps another and none was skipped.
    private static async Task AssertIdsDistinctAndDenseAsync(DeltaTable table, int rows)
    {
        Assert.Equal(Enumerable.Range(0, rows).Select(i => (long)i), await RowIdsAsync(table));
    }

    [Fact]
    public async Task AnAppend_RecordsTheRowCountAlone()
    {
        await using var table = await CreateAsync(rowTracking: true);
        long version = await table.WriteAsync([B(1, 2, 3)]);

        var add = Assert.Single(await AddsAsync(version));
        Assert.Equal("{\"numRecords\":3}", add.Stats);
        Assert.Equal(0, add.BaseRowId);
    }

    [Fact]
    public async Task WithoutRowTracking_NoStatsAreWritten()
    {
        await using var table = await CreateAsync(rowTracking: false);
        long version = await table.WriteAsync([B(1, 2, 3)]);

        Assert.Null(Assert.Single(await AddsAsync(version)).Stats);
    }

    [Fact]
    public async Task WithStatsOn_TheColumnStatsAreKept()
    {
        await using var table = await CreateAsync(rowTracking: true, new DeltaTableOptions { CheckpointInterval = 0 });
        long version = await table.WriteAsync([B(1, 2, 3)]);

        string stats = Assert.Single(await AddsAsync(version)).Stats!;
        Assert.Contains("\"numRecords\":3", stats, StringComparison.Ordinal);
        Assert.Contains("\"minValues\"", stats, StringComparison.Ordinal);
    }

    // The repro from #742: the staged append is rebased past a concurrent append.
    [Fact]
    public async Task ATransactionAppend_RebasesPastAConcurrentAppend()
    {
        await using var table = await CreateAsync(rowTracking: true);
        await table.WriteAsync([B(1, 2)]);

        await using var txn = table.StartTransaction();
        await txn.WriteAsync([B(10, 11)]);
        await using (var racer = await DeltaTable.OpenAsync(Fs, NoStats))
            await racer.WriteAsync([B(20)]);
        long version = await txn.CommitAsync();

        Assert.Equal(3, version);
        Assert.All(await AddsAsync(version), a => Assert.NotNull(a.GetNumRecords()));
        await AssertIdsDistinctAndDenseAsync(table, 5);
    }

    [Theory]
    [InlineData(RowDeleteMode.CopyOnWrite)]
    [InlineData(RowDeleteMode.DeletionVector)]
    public async Task ATransactionUpdate_RebasesPastAConcurrentAppend(RowDeleteMode mode)
    {
        await using var table = await CreateAsync(
            rowTracking: true, deletionVectors: mode == RowDeleteMode.DeletionVector);
        await table.WriteAsync([B(1, 2, 3)]);

        await using var txn = table.StartTransaction();
        await txn.UpdateAsync(
            b => new BooleanArray.Builder().AppendRange(
                Enumerable.Range(0, b.Length).Select(i => ((Int64Array)b.Column("id")).GetValue(i) == 2)).Build(),
            b => new RecordBatch(FlatSchema, [b.Column("id"), B("u", new long[b.Length]).Column("value")], b.Length),
            mode: mode);
        await txn.WriteAsync([B(10)]);
        await using (var racer = await DeltaTable.OpenAsync(Fs, NoStats))
            await racer.WriteAsync([B(20)]);
        long version = await txn.CommitAsync();

        Assert.All(await AddsAsync(version), a => Assert.NotNull(a.GetNumRecords()));
        Assert.Equal(5, (await RowIdsAsync(table)).Distinct().Count());
    }

    [Fact]
    public async Task ATransactionMerge_RebasesPastAConcurrentAppend()
    {
        await using var table = await CreateAsync(rowTracking: true);
        await table.WriteAsync([B(1, 2, 3)]);

        await using var txn = table.StartTransaction();
        await txn.MergeAsync([B("m", 2, 40, 41)], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.Update],
            WhenNotMatched = [MergeAction.Insert],
        });
        await using (var racer = await DeltaTable.OpenAsync(Fs, NoStats))
            await racer.WriteAsync([B(20)]);
        long version = await txn.CommitAsync();

        Assert.All(await AddsAsync(version), a => Assert.NotNull(a.GetNumRecords()));
        Assert.Equal(6, (await RowIdsAsync(table)).Distinct().Count());
    }

    // The compaction rebase re-derives the compacted adds' ranges from their counts too. Compaction already wrote
    // one with statistics off; this keeps it that way.
    [Fact]
    public async Task ACompaction_RecordsTheRowCount()
    {
        await using var table = await CreateAsync(rowTracking: true);
        await table.WriteAsync([B(1, 2)]);
        await table.WriteAsync([B(3)]);

        long? version = await table.CompactAsync();

        Assert.NotNull(version);
        Assert.Equal(3, Assert.Single(await AddsAsync(version.Value)).GetNumRecords());
    }
}
