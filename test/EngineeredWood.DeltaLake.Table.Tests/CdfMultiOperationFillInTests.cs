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
/// A version that carries any change file is read from its change files alone (#530). One operation per
/// commit, as Spark and delta-rs write, never mixes a change file with work that relies on inference; a
/// <see cref="DeltaTransaction"/> can. So when one staged operation writes a change file, the commit writes
/// change rows for the appends and deletion-vector deletes beside it, which otherwise vanished from the feed.
/// </summary>
public class CdfMultiOperationFillInTests : IDisposable
{
    private readonly string _tempDir;

    public CdfMultiOperationFillInTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdffill_{Guid.NewGuid():N}");
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

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

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

    private Task<DeltaTable> CreateAsync(bool rowTracking = false) => DeltaTable.CreateAsync(
        Fs, BuildSchema(), enableDeletionVectors: true, enableRowTracking: rowTracking,
        configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }).AsTask();

    /// <summary>A table whose rows 1-3 and 11-13 sit in two files, so two deletes touch different files.</summary>
    private async Task<DeltaTable> CreateTwoFilesAsync(bool rowTracking = false)
    {
        var table = await CreateAsync(rowTracking);
        await table.WriteAsync([Batch(1, 3)]);
        await table.WriteAsync([Batch(11, 3)]);
        return table;
    }

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

    private async Task<List<(long Id, string Type)>> ReadFeedAsync(long version)
    {
        await using var check = await OpenAsync();
        var changes = new List<(long, string)>();
        await foreach (var batch in check.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)batch.Column("id");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((ids.GetValue(i)!.Value, types.GetString(i)));
        }
        return changes.OrderBy(c => c.Item2, StringComparer.Ordinal).ThenBy(c => c.Item1).ToList();
    }

    private async Task<bool> HasChangeFileAsync(long version) =>
        (await new TransactionLog(Fs).ReadCommitAsync(version)).Any(a => a is CdcFile);

    /// <summary>Change files on storage that no committed version names: what a fill-in must not leave.</summary>
    private async Task<List<string>> OrphanChangeFilesAsync()
    {
        string dir = Path.Combine(_tempDir, "_change_data");
        if (!Directory.Exists(dir))
            return [];
        await using var check = await OpenAsync();
        var log = new TransactionLog(Fs);
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        for (long v = 0; v <= check.CurrentSnapshot.Version; v++)
        {
            foreach (var cdc in (await log.ReadCommitAsync(v)).OfType<CdcFile>())
                referenced.Add(Path.GetFileName(DeltaPath.Decode(cdc.Path)));
        }
        return Directory.GetFiles(dir).Select(Path.GetFileName).Where(f => !referenced.Contains(f!)).ToList()!;
    }

    // ── The gaps the issue names ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AVectorDelete_BesideACopyOnWriteDelete_StaysInTheFeed()
    {
        await using var table = await CreateTwoFilesAsync();
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[2]), RowDeleteMode.CopyOnWrite); // writes a change file
        await txn.StageRowDeletesAsync(Sel(at[12]));                           // writes none of its own
        long version = await txn.CommitAsync();

        // Was: only 2 — the version is read cdc-only, and the vector delete had no change rows.
        Assert.Equal([(2L, "delete"), (12L, "delete")], await ReadFeedAsync(version));
    }

    [Fact]
    public async Task AStagedAppend_BesideHostStagedChangeRows_StaysInTheFeed()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Batch(1, 3)]);

        var txn = table.StartTransaction();
        txn.StageDataFiles(await table.WriteDataFilesAsync([Batch(20, 2)]));
        // Change rows for some other statement the host ran itself.
        await txn.StageChangeDataAsync(Batch(2, 1), "delete");
        long version = await txn.CommitAsync();

        Assert.Equal([(2L, "delete"), (20L, "insert"), (21L, "insert")], await ReadFeedAsync(version));
    }

    [Fact]
    public async Task AComputedAppend_BesideAComputedDelete_StaysInTheFeed()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Batch(1, 3)]);

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(30, 2)]);
        await txn.DeleteAsync(b => Matching(b, id => id == 3)); // a computed delete writes its change file
        long version = await txn.CommitAsync();

        Assert.Equal([(3L, "delete"), (30L, "insert"), (31L, "insert")], await ReadFeedAsync(version));
    }

    [Fact]
    public async Task ABornDeletedAppend_FillsInOnlyItsVisibleRows()
    {
        await using var table = await CreateTwoFilesAsync();
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        var files = await table.WriteDataFilesAsync([Batch(40, 3)]);
        var bornDeleted = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal)
        {
            [files[0].RelativePath] = new HashSet<long> { 1 }, // row 41 never appears in any version
        });
        await txn.StageDataFilesAsync(files, bornDeleted);
        await txn.StageRowDeletesAsync(Sel(at[1]), RowDeleteMode.CopyOnWrite);
        long version = await txn.CommitAsync();

        Assert.Equal([(1L, "delete"), (40L, "insert"), (42L, "insert")], await ReadFeedAsync(version));
    }

    // ── What does NOT change ─────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ATransactionWithNoChangeFile_WritesNone_AndStaysInferred()
    {
        await using var table = await CreateTwoFilesAsync();
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[12]));
        await txn.WriteAsync([Batch(50, 1)]);
        long version = await txn.CommitAsync();

        Assert.False(await HasChangeFileAsync(version));
        Assert.Equal([(12L, "delete"), (50L, "insert")], await ReadFeedAsync(version));
    }

    // ── changeDataStagedByCaller ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AVectorDeleteTheHostCovers_IsNotWrittenTwice()
    {
        await using var table = await CreateTwoFilesAsync();
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[12]), RowDeleteMode.DeletionVector, changeDataStagedByCaller: true);
        await txn.StageChangeDataAsync(Batch(12, 1), "delete");
        long version = await txn.CommitAsync();

        Assert.Equal([(12L, "delete")], await ReadFeedAsync(version));
    }

    [Fact]
    public async Task ACopyOnWriteDeleteTheHostCovers_WritesNoChangeFileOfItsOwn()
    {
        await using var table = await CreateTwoFilesAsync();
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[2]), RowDeleteMode.CopyOnWrite, changeDataStagedByCaller: true);
        await txn.StageChangeDataAsync(Batch(2, 1), "delete");
        long version = await txn.CommitAsync();

        Assert.Equal([(2L, "delete")], await ReadFeedAsync(version));
    }

    [Fact]
    public async Task AnAppendTheHostCovers_IsNotWrittenTwice()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Batch(1, 3)]);

        var txn = table.StartTransaction();
        txn.StageDataFiles(await table.WriteDataFilesAsync([Batch(20, 2)]), changeDataStagedByCaller: true);
        await txn.StageChangeDataAsync(Batch(20, 2), "insert");
        long version = await txn.CommitAsync();

        Assert.Equal([(20L, "insert"), (21L, "insert")], await ReadFeedAsync(version));
    }

    // ── Row tracking, rebase, abort ──────────────────────────────────────────────────────────────────────

    /// <summary>An inserted row's id is its add's <c>baseRowId</c> plus its position, and a rebase moves that
    /// <c>baseRowId</c>. The change file is the only place the id lives, so the fill-in must follow, and the
    /// files it wrote for the losing attempt must not be left behind.</summary>
    [Fact]
    public async Task OnARowTrackingTable_FilledInInserts_CarryTheIdsTheRebasedCommitAssigns()
    {
        await using var table = await CreateTwoFilesAsync(rowTracking: true);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[2]), RowDeleteMode.CopyOnWrite);
        await txn.WriteAsync([Batch(50, 2)]);

        // The racer takes the ids the transaction reserved for its append, so the commit must re-derive them.
        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(100, 2)]);

        long version = await txn.CommitAsync();

        await using var check = await OpenAsync();
        var committed = new Dictionary<long, long>();
        await foreach (var batch in check.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var id = (Int64Array)batch.Column("id");
            var rowId = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                committed[id.GetValue(i)!.Value] = rowId.GetValue(i)!.Value;
        }

        var inserts = new Dictionary<long, (long? RowId, long? RowVersion, long Commit)>();
        await foreach (var b in check.ReadChangesAsync(new DeltaChangeReadOptions
        {
            StartVersion = version, EndVersion = version, Metadata = DeltaRowMetadata.RowTracking,
        }))
        {
            var change = (StringArray)b.Column(b.Schema.GetFieldIndex("_change_type"));
            var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var rowId = (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            var rowVer = (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowCommitVersionColumnName));
            var commit = (Int64Array)b.Column(b.Schema.GetFieldIndex("_commit_version"));
            for (int i = 0; i < b.Length; i++)
            {
                if (change.GetString(i) == "insert")
                {
                    inserts[id.GetValue(i)!.Value] = (
                        rowId.IsNull(i) ? null : rowId.GetValue(i),
                        rowVer.IsNull(i) ? null : rowVer.GetValue(i),
                        commit.GetValue(i)!.Value);
                }
            }
        }

        Assert.Equal(new long[] { 50, 51 }, inserts.Keys.OrderBy(k => k));
        foreach (long id in new long[] { 50, 51 })
        {
            Assert.Equal(committed[id], inserts[id].RowId);
            Assert.Equal(version, inserts[id].RowVersion);
            Assert.Equal(version, inserts[id].Commit);
        }
        Assert.Empty(await OrphanChangeFilesAsync());
    }

    [Fact]
    public async Task AFillInWhoseCommitConflicts_IsTakenBackByTheAbort()
    {
        await using var table = await CreateTwoFilesAsync();
        var at = await LocateRowsAsync(table);

        await using var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[2]), RowDeleteMode.CopyOnWrite);
        await txn.StageRowDeletesAsync(Sel(at[12]));
        await txn.WriteAsync([Batch(50, 1)]);

        // A concurrent change to the file the copy-on-write delete rewrites aborts the commit.
        await using (var racer = await OpenAsync())
            await racer.DeleteRowsAsync(Sel(at[1]));

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
        await txn.AbortAsync();

        Assert.Empty(await OrphanChangeFilesAsync());
    }

    private static BooleanArray Matching(RecordBatch batch, Func<long, bool> match)
    {
        var ids = (Int64Array)batch.Column("id");
        var builder = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            builder.Append(match(ids.GetValue(i)!.Value));
        return builder.Build();
    }
}
