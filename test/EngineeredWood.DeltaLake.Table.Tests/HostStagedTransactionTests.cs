// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The HOST-STAGED half of <see cref="DeltaTransaction"/>: an embedding engine that owns its data plane (its
/// own parquet codec, its own execution deciding which rows to delete) arrives with the work already done and
/// stages it, instead of handing engineered-wood batches and predicates. What matters here is not that each
/// staging call records the right action — it is that staged work goes through the SAME conflict-check /
/// rebase / retry loop as the computed operations, so the host does not reimplement it and get the invariants
/// subtly wrong. The pinned <see cref="DeltaTransaction.Snapshot"/> is what ties a host-planned file ordinal
/// to the transaction that validates it.
/// </summary>
public class HostStagedTransactionTests : IDisposable
{
    private readonly string _tempDir;

    public HostStagedTransactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_hoststage_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }


    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(BuildSchema(), [ids.Build(), values.Build()], count);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    private async Task<List<long>> ReadIdsFreshAsync()
    {
        await using var reader = await OpenAsync();
        var ids = new List<long>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    /// <summary>Maps id → (file ordinal, absolute position) by decoding the transient rowid, the way a host
    /// correlates its own scan with the coordinates the DML seam speaks.</summary>
    /// <summary>Every row's (user id -> its DML locator): the file's <c>add.path</c> plus the row's absolute
    /// in-file position. The packed address is unpacked into a path HERE, against the snapshot it was read
    /// from, which is where a stale address is caught rather than silently mis-addressing a file.</summary>
    private static async Task<Dictionary<long, (string Path, long Position)>> LocateRowsAsync(DeltaTable table)
    {
        var ordered = table.CurrentSnapshot.ActiveFiles.Values
            .Select(a => a.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();
        var located = new Dictionary<long, (string, long)>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
        {
            var ids = (Int64Array)batch.Column("id");
            var rids = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
            {
                long rid = rids.GetValue(i)!.Value;
                located[ids.GetValue(i)!.Value] =
                    (ordered[TransientRowAddress.FileOrdinal(rid)], TransientRowAddress.Position(rid));
            }
        }
        return located;
    }

    /// <summary>The rows at <paramref name="rows"/> as the path-keyed DML boundary key.</summary>
    private static RowSelection Sel(params (string Path, long Position)[] rows)
    {
        var byPath = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        foreach (var (path, position) in rows)
        {
            if (!byPath.TryGetValue(path, out var set))
                byPath[path] = set = new HashSet<long>();
            ((HashSet<long>)set).Add(position);
        }
        return RowSelection.ByPath(byPath);
    }

    // ── Stable row ids across SEVERAL staged operations ──
    //
    // Each staging call used to restart its row-id reservation at the base snapshot's high-water mark, so two
    // operations staged on one transaction handed the same ids to different rows. Nothing failed at commit —
    // the duplicate only surfaces when a spec reader resolves two rows to one identity.

    [Fact]
    public async Task TwoStagedAppends_ReserveDisjointRowIdRanges()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 3)]); // v1: 3 rows, high-water mark 3

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(10, 3)]);
        await txn.WriteAsync([Batch(20, 3)]);
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        var ranges = check.CurrentSnapshot.ActiveFiles.Values
            .Where(f => f.BaseRowId is not null)
            .Select(f => (Start: f.BaseRowId!.Value, Count: f.GetNumRecords() ?? 0))
            .OrderBy(r => r.Start).ToList();

        Assert.Equal(3, ranges.Count);
        // Contiguous and non-overlapping: 0..2 (v1), then the two staged appends.
        long expected = 0;
        foreach (var r in ranges)
        {
            Assert.Equal(expected, r.Start);
            expected += r.Count;
        }
        Assert.Equal(9, expected);
        Assert.Equal(9, check.CurrentSnapshot.RowIdHighWaterMark);
    }

    [Fact]
    public async Task StagedAppendThenUpdate_ReserveDisjointRowIdRanges()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 3)]);

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(10, 3)]);
        // The UPDATE rewrites v1's file copy-on-write, so its post-image add reserves ids too.
        await txn.UpdateAsync(Ex.Equal("id", 2L), b =>
        {
            var ids = (Int64Array)b.Column("id");
            var vals = new StringArray.Builder();
            for (int i = 0; i < b.Length; i++)
                vals.Append("updated");
            return new RecordBatch(BuildSchema(), [ids, vals.Build()], b.Length);
        });
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        var starts = check.CurrentSnapshot.ActiveFiles.Values
            .Where(f => f.BaseRowId is not null).Select(f => f.BaseRowId!.Value).ToList();
        Assert.Equal(starts.Count, starts.Distinct().Count());
    }

    [Fact]
    public async Task StagedWork_EmitsExactlyOneRowTrackingHighWaterMark()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 3)]);

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(10, 3)]);
        await txn.WriteAsync([Batch(20, 3)]);
        long version = await txn.CommitAsync();

        // Two domainMetadata entries for one domain in a version is malformed — the last written would win
        // regardless of which reserved the most ids.
        string commit = File.ReadAllText(Path.Combine(
            _tempDir, "_delta_log", $"{version:D20}.json"));
        int marks = commit.Split('\n')
            .Count(l => l.Contains("\"domainMetadata\"", StringComparison.Ordinal)
                && l.Contains("delta.rowTracking", StringComparison.Ordinal));
        Assert.Equal(1, marks);
    }

    // ── The host-staged surface ──

    [Fact]
    public async Task StageDataFiles_CommitsFilesTheHostWrote()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 3)]);

        // The host writes its files first (here through WriteDataFilesAsync; a real host uses its own codec).
        var files = await table.WriteDataFilesAsync([Batch(10, 2)]);

        var txn = table.StartTransaction();
        txn.StageDataFiles(files);
        await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 2, 3, 10, 11 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task StageRowDeletesAsync_DeletesTheRowsThePlanPointedAt()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 10)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        // A host plans against the TRANSACTION's snapshot, so what it addresses agrees with what the
        // commit validates.
        var planned = table.PlanFiles(Ex.LessThan("id", 5L), snapshot: txn.Snapshot);
        Assert.NotEmpty(planned);

        long rows = await txn.StageRowDeletesAsync(Sel(at[2], at[4]));
        Assert.Equal(2, rows);
        await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 3, 5, 6, 7, 8, 9, 10 }, await ReadIdsFreshAsync());
    }

    /// <summary>The payoff: a staged delete rebases onto a concurrent delete of DIFFERENT rows automatically.
    /// The host drove no rebase — this is the loop it would otherwise have reimplemented.</summary>
    [Fact]
    public async Task StagedRowDeletes_ComposeWithAConcurrentDisjointDelete()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 10)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[2]));

        // a racer deletes a different row of the same file while the transaction is open
        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 7L));

        await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 8, 9, 10 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task StagedRowDeletes_ConflictOnTheSameRow()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 10)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[2]));

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 2L)); // the same row

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());

        // deleted exactly once, by the racer
        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 7, 8, 9, 10 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task StageSchemaChange_LandsTheAlterInTheSameVersionAsTheData()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 2)]);

        var txn = table.StartTransaction();
        var change = table.ComputeAddColumn(new Field("extra", Int32Type.Default, true));
        txn.StageSchemaChange(change);

        var widened = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("extra", Int32Type.Default, true))
            .Build();
        var files = await table.WriteDataFilesAsync(
            [new RecordBatch(widened,
            [
                new Int64Array.Builder().Append(10L).Build(),
                new StringArray.Builder().Append("v10").Build(),
                new Int32Array.Builder().Append(100).Build(),
            ], 1)],
            schemaOverride: change.NewSchema);
        txn.StageDataFiles(files);

        long version = await txn.CommitAsync();

        await using var check = await OpenAsync();
        Assert.Equal(version, check.CurrentSnapshot.Version);
        Assert.Contains("extra", check.CurrentSnapshot.Schema.Fields.Select(f => f.Name));

        var extras = new Dictionary<long, int?>();
        await foreach (var batch in check.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var ex = (Int32Array)batch.Column("extra");
            for (int i = 0; i < batch.Length; i++)
                extras[ids.GetValue(i)!.Value] = ex.IsNull(i) ? null : ex.GetValue(i);
        }
        Assert.Null(extras[1]); // backfilled
        Assert.Equal(100, extras[10]);
    }

    [Fact]
    public async Task StageChangeDataAsync_FeedReadsTheStagedRows()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableDeletionVectors: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[3]));
        await txn.StageChangeDataAsync(Batch(3, 1), "delete");
        long version = await txn.CommitAsync();

        await using var check = await OpenAsync();
        var changes = new List<(long Id, string Type)>();
        await foreach (var batch in check.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)batch.Column("id");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((ids.GetValue(i)!.Value, types.GetString(i)));
        }

        // A commit carrying a cdc action reads cdc-only: exactly the staged delete, not an inferred whole-file
        // delete plus re-insert of the survivors.
        var change = Assert.Single(changes);
        Assert.Equal((3L, "delete"), change);
    }

    [Fact]
    public async Task StageChangeDataAsync_PartitionedTable_SplitsPerPartition()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            Fs, schema, partitionColumns: ["region"],
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });

        static RecordBatch Rows(Apache.Arrow.Schema s, params (long Id, string Region)[] rows)
        {
            var ids = new Int64Array.Builder();
            var regions = new StringArray.Builder();
            foreach (var (id, region) in rows)
            {
                ids.Append(id);
                regions.Append(region);
            }
            return new RecordBatch(s, [ids.Build(), regions.Build()], rows.Length);
        }

        await table.WriteAsync([Rows(schema, (1, "us"), (2, "eu"))]);

        var txn = table.StartTransaction();
        // Rows spanning TWO partitions in one call — the split, the partition-column strip, and the
        // partitionValues encoding are what a caller cannot do from outside the assembly.
        await txn.StageChangeDataAsync(Rows(schema, (1, "us"), (2, "eu")), "delete");
        long version = await txn.CommitAsync();

        var cdcPaths = Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(p => p.Contains("_change_data", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, cdcPaths.Count); // one per partition

        await using var check = await OpenAsync();
        var changes = new List<(long Id, string Region, string Type)>();
        await foreach (var batch in check.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)batch.Column("id");
            var regions = (StringArray)batch.Column("region");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((ids.GetValue(i)!.Value, regions.GetString(i), types.GetString(i)));
        }
        changes.Sort();

        // The partition column round-trips through partitionValues — it is not in the file bytes.
        Assert.Equal([(1L, "us", "delete"), (2L, "eu", "delete")], changes.OrderBy(c => c.Id).ToArray());
    }

    [Fact]
    public async Task StageActions_CommitsAnApplicationTransactionId()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(1, 2)]);
        txn.StageActions([new TransactionId
        {
            AppId = "producer-1",
            Version = 42,
            LastUpdated = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        }]);
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        Assert.True(check.CurrentSnapshot.AppTransactions.TryGetValue("producer-1", out var recorded));
        Assert.Equal(42, recorded!.Version);
    }

    /// <summary>The whole point, end to end: a host plans, writes its own files, deletes its own rows, alters
    /// the schema, and commits ONE version — driving no rebase itself even though a racer commits in between.
    /// </summary>
    [Fact]
    public async Task HostFlow_PlanStageCommit_IsOneAtomicVersionThroughAConcurrentWriter()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableDeletionVectors: true, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 10)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        long baseVersion = txn.ReadVersion;

        // 1. plan against the transaction's snapshot
        var planned = table.PlanFiles(Ex.LessThanOrEqual("id", 3L), snapshot: txn.Snapshot);
        Assert.NotEmpty(planned);

        // 2. the host's own DML + its own written files + an ALTER, all staged
        await txn.StageRowDeletesAsync(Sel(at[3]));
        var files = await table.WriteDataFilesAsync([Batch(20, 2)]);
        txn.StageDataFiles(files);
        txn.StageActions([new TransactionId
        {
            AppId = "host", Version = 7, LastUpdated = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        }]);

        // 3. a racer lands a disjoint delete first
        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 9L));

        long version = await txn.CommitAsync();

        await using var check = await OpenAsync();
        Assert.True(version > baseVersion + 1); // the racer's commit sits in between
        Assert.Equal(version, check.CurrentSnapshot.Version);
        Assert.Equal(new long[] { 1, 2, 4, 5, 6, 7, 8, 10, 20, 21 }, await ReadIdsFreshAsync());
        Assert.True(check.CurrentSnapshot.AppTransactions.ContainsKey("host"));

        // row ids stayed unique across the rebase
        var starts = check.CurrentSnapshot.ActiveFiles.Values
            .Where(f => f.BaseRowId is not null).Select(f => f.BaseRowId!.Value).ToList();
        Assert.Equal(starts.Count, starts.Distinct().Count());
    }

    [Fact]
    public async Task StageDataFiles_WhenTheTableNeedsWriteTimeProcessing_Throws()
    {
        // IcebergCompat materializes partition values into the data files, which an outside writer did not do.
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { ["delta.enableIcebergCompatV2"] = "true" });
        Assert.False(table.SupportsExternalDataFileCommit);

        var txn = table.StartTransaction();
        var ex = Assert.Throws<NotSupportedException>(() => txn.StageDataFiles(
        [
            new WrittenDataFile("x.parquet", 100, 1, null, null),
        ]));
        Assert.Contains("SupportsExternalDataFileCommit", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StagingAfterCommit_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(1, 1)]);
        await txn.CommitAsync();

        Assert.Throws<InvalidOperationException>(() => txn.StageActions([]));
        Assert.Throws<InvalidOperationException>(() => txn.StageDataFiles([]));
    }
}
