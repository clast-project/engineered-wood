// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTransaction.StageRowDeletesAsync(RowSelection, RowDeleteMode, CancellationToken, bool)"/> with
/// <see cref="RowDeleteMode.CopyOnWrite"/> (#525): the way a host that buffers DML stages a delete on a table
/// that must stay readable by engines without deletion vectors. Before it, such a table could only take a
/// delete as its own commit (<see cref="DeltaTable.DeleteRowsAsync"/>), never inside a transaction with other
/// work.
/// </summary>
public class StagedCopyOnWriteDeleteTests : IDisposable
{
    private readonly string _tempDir;

    public StagedCopyOnWriteDeleteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_stagedcow_{Guid.NewGuid():N}");
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

    private static Dictionary<string, string> Cdf() => new() { ["delta.enableChangeDataFeed"] = "true" };

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

    /// <summary>Every row's (user id -> its DML locator): the file's <c>add.path</c> plus the row's absolute
    /// in-file position.</summary>
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

    /// <summary>user id -> stable row id, as a row-tracking read reports it.</summary>
    private async Task<Dictionary<long, long>> ReadRowIdsAsync()
    {
        await using var reader = await OpenAsync();
        var byId = new Dictionary<long, long>();
        await foreach (var batch in reader.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var id = (Int64Array)batch.Column("id");
            var rowId = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                byId[id.GetValue(i)!.Value] = rowId.GetValue(i)!.Value;
        }
        return byId;
    }

    private async Task<List<(long Id, string Type)>> ReadFeedAsync(long version)
    {
        await using var check = await OpenAsync();
        var changes = new List<(long Id, string Type)>();
        await foreach (var batch in check.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)batch.Column("id");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((ids.GetValue(i)!.Value, types.GetString(i)));
        }
        return changes;
    }

    private string[] DataFilesOnDisk() =>
        [.. Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(p => !p.Contains("_delta_log", StringComparison.Ordinal))];

    [Fact]
    public async Task OnATableWithoutDeletionVectors_CommitsWithAStagedAppendInOneVersion()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        Assert.Equal(2, await txn.StageRowDeletesAsync(Sel(at[2], at[4]), RowDeleteMode.CopyOnWrite));
        txn.StageDataFiles(await table.WriteDataFilesAsync([Batch(6, 2)]));
        long version = await txn.CommitAsync();

        Assert.Equal(table.CurrentSnapshot.Version, version);
        Assert.Equal(2, version); // create, write, then ONE version for the delete and the append together
        Assert.Equal(new long[] { 1, 3, 5, 6, 7 }, await ReadIdsFreshAsync());

        await using var check = await OpenAsync();
        Assert.All(check.CurrentSnapshot.ActiveFiles.Values, a => Assert.Null(a.DeletionVector));
        Assert.False(check.CurrentSnapshot.Protocol.WriterFeatures?.Contains("deletionVectors") ?? false);
    }

    [Fact]
    public async Task DeletingEveryRowOfAFile_RemovesIt()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(10, 2)]);
        var at = await LocateRowsAsync(table);
        string doomed = at[1].Path;

        var txn = table.StartTransaction();
        Assert.Equal(2, await txn.StageRowDeletesAsync(Sel(at[1], at[2]), RowDeleteMode.CopyOnWrite));
        await txn.CommitAsync();

        Assert.Equal(new long[] { 10, 11 }, await ReadIdsFreshAsync());
        await using var check = await OpenAsync();
        var survivor = Assert.Single(check.CurrentSnapshot.ActiveFiles.Values);
        Assert.NotEqual(doomed, survivor.Path);
    }

    [Fact]
    public async Task Abort_DeletesTheRewrite()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), configuration: Cdf());
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);
        var before = DataFilesOnDisk();

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[3]), RowDeleteMode.CopyOnWrite);
        Assert.True(DataFilesOnDisk().Length > before.Length); // the rewrite and its change file
        await txn.AbortAsync();

        Assert.Equal(before.OrderBy(p => p), DataFilesOnDisk().OrderBy(p => p));
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task RebasesPastAConcurrentAppend()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[3]), RowDeleteMode.CopyOnWrite);

        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(100, 2)]);

        await txn.CommitAsync();
        Assert.Equal(new long[] { 1, 2, 4, 5, 100, 101 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task OnARowTrackingTable_SurvivorsKeepTheirIds_AcrossARebase()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]);
        var idsBefore = await ReadRowIdsAsync();
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[3]), RowDeleteMode.CopyOnWrite);
        // A staged append on the same transaction reserves ids after the rewrite's.
        await txn.WriteAsync([Batch(50, 2)]);

        // The racer's append takes ids the transaction's attempt had reserved, so the commit must re-derive.
        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(100, 2)]);

        await txn.CommitAsync();

        var idsAfter = await ReadRowIdsAsync();
        Assert.Equal(new long[] { 1, 2, 4, 5, 50, 51, 100, 101 }, idsAfter.Keys.OrderBy(k => k));
        foreach (long id in new long[] { 1, 2, 4, 5 })
            Assert.Equal(idsBefore[id], idsAfter[id]);
        Assert.Equal(idsAfter.Count, idsAfter.Values.Distinct().Count());
    }

    /// <summary>The rewrite reserves a row-id range like any fresh add, so work staged after it on the same
    /// transaction must continue from there. A rebase re-derives every range and would hide an overlap, so
    /// this commits without one.</summary>
    [Fact]
    public async Task OnARowTrackingTable_TheRewriteAndALaterStagedAppend_ReserveDisjointRanges()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]); // ids 0..4, high-water mark 5
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[3]), RowDeleteMode.CopyOnWrite); // a 4-row rewrite
        await txn.WriteAsync([Batch(50, 2)]);
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        var ranges = check.CurrentSnapshot.ActiveFiles.Values
            .Select(f => (Start: f.BaseRowId!.Value, Count: f.GetNumRecords() ?? 0))
            .OrderBy(r => r.Start).ToList();
        Assert.Equal(2, ranges.Count);
        Assert.True(ranges[0].Start + ranges[0].Count <= ranges[1].Start,
            $"overlapping row-id ranges: {string.Join(", ", ranges)}");
        Assert.Equal(5 + 4 + 2, check.CurrentSnapshot.RowIdHighWaterMark);
    }

    [Fact]
    public async Task ConflictsWithAConcurrentRewriteOfTheSameFile()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[2]), RowDeleteMode.CopyOnWrite);

        // A racer rewrites the same file to delete a DIFFERENT row: no vector to reconcile against.
        await using (var racer = await OpenAsync())
            await racer.DeleteRowsAsync(Sel(at[4]), RowDeleteMode.CopyOnWrite);

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
        Assert.Equal(new long[] { 1, 2, 3, 5 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task AFileTheTransactionAlreadyRewrote_IsRefused_AndNothingIsWritten()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.UpdateAsync(Ex.Equal("id", 1L), b => b);
        var staged = DataFilesOnDisk();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => txn.StageRowDeletesAsync(Sel(at[3]), RowDeleteMode.CopyOnWrite).AsTask());
        Assert.Contains(at[3].Path, ex.Message, StringComparison.Ordinal);
        Assert.Equal(staged.OrderBy(p => p), DataFilesOnDisk().OrderBy(p => p));
    }

    [Fact]
    public async Task TwiceOnOneFile_RefusesTheSecond()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[2]), RowDeleteMode.CopyOnWrite);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => txn.StageRowDeletesAsync(Sel(at[3]), RowDeleteMode.CopyOnWrite).AsTask());
        await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 3, 4, 5 }, await ReadIdsFreshAsync());
    }

    /// <summary>Without its own change file the feed would infer the remove/add pair as every survivor deleted
    /// and re-inserted — measured as <c>delete 1–5, insert 1,2,4,5</c> for a delete of row 3.</summary>
    [Fact]
    public async Task OnAChangeFeedTable_WritesTheDeletedRowsAsItsOwnChangeFile()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), configuration: Cdf());
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[3]), RowDeleteMode.CopyOnWrite);
        long version = await txn.CommitAsync();

        var change = Assert.Single(await ReadFeedAsync(version));
        Assert.Equal((3L, "delete"), change);
    }

    [Fact]
    public async Task EmptySelection_IsANoOp()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 3)]);
        var before = DataFilesOnDisk();

        var txn = table.StartTransaction();
        Assert.Equal(0, await txn.StageRowDeletesAsync(Sel(), RowDeleteMode.CopyOnWrite));
        Assert.Equal(before.Length, DataFilesOnDisk().Length);
    }

    [Fact]
    public async Task DeletionVectorMode_IsTheOriginalOverload()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        var txn = table.StartTransaction();
        await txn.StageRowDeletesAsync(Sel(at[3]), RowDeleteMode.DeletionVector);
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        var add = Assert.Single(check.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal(at[3].Path, add.Path);
        Assert.NotNull(add.DeletionVector);
        Assert.Equal(new long[] { 1, 2, 4, 5 }, await ReadIdsFreshAsync());
    }
}
