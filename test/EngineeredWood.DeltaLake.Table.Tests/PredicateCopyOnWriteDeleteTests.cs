// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A predicate DELETE on a table without deletion vectors rewrites the files it matches only in part
/// (copy-on-write), where it used to refuse (#562). That was every table <see cref="DeltaTable.CreateAsync"/>
/// makes by default, and every table delta-rs writes. A file the predicate matches entirely is still removed
/// without being read again or rewritten, and an explicit <see cref="RowDeleteMode"/> chooses either way.
/// </summary>
public class PredicateCopyOnWriteDeleteTests : IDisposable
{
    private readonly string _tempDir;

    public PredicateCopyOnWriteDeleteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_predcow_{Guid.NewGuid():N}");
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

    /// <summary>Rows 2 and 4 of a five-row file 1..5: a partial match.</summary>
    private static readonly Expressions.Predicate TwoAndFour = Ex.Or(Ex.Equal("id", 2L), Ex.Equal("id", 4L));

    private static BooleanArray TwoAndFourMask(RecordBatch batch)
    {
        var ids = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            mask.Append(ids.GetValue(i) is 2L or 4L);
        return mask.Build();
    }

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
        changes.Sort();
        return changes;
    }

    private string[] DataFilesOnDisk() =>
        [.. Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(p => !p.Contains("_delta_log", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal)];

    [Fact]
    public async Task OnATableWithoutDeletionVectors_APartialDeleteRewritesTheFile()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        string source = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values).Path;

        var (deleted, version) = await table.DeleteAsync(TwoAndFour);

        Assert.Equal(2, deleted);
        Assert.Equal(2, version);
        Assert.Equal(new long[] { 1, 3, 5 }, await ReadIdsFreshAsync());

        await using var check = await OpenAsync();
        var add = Assert.Single(check.CurrentSnapshot.ActiveFiles.Values);
        Assert.NotEqual(source, add.Path); // rewritten, not masked
        Assert.Null(add.DeletionVector);
        Assert.Equal(3, add.GetNumRecords());
        // The table was not upgraded to deletion vectors to make the delete possible.
        Assert.False(check.CurrentSnapshot.Protocol.WriterFeatures?.Contains("deletionVectors") ?? false);
        string? operation = null;
        await foreach (var entry in check.GetHistoryAsync())
        {
            if (entry.Version == version)
                operation = entry.Operation;
        }
        Assert.Equal("DELETE", operation);
    }

    [Fact]
    public async Task TheFunctionalOverload_RewritesToo()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);

        var (deleted, _) = await table.DeleteAsync(TwoAndFourMask);

        Assert.Equal(2, deleted);
        Assert.Equal(new long[] { 1, 3, 5 }, await ReadIdsFreshAsync());
        await using var check = await OpenAsync();
        Assert.Null(Assert.Single(check.CurrentSnapshot.ActiveFiles.Values).DeletionVector);
    }

    /// <summary>
    /// One delete, two files: one matched entirely and one in part. Only the second is rewritten; the first is
    /// removed with no new file written for it.
    /// </summary>
    [Fact]
    public async Task AFullyMatchedFile_IsRemovedWithoutARewrite_BesideAPartlyMatchedOne()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 2)]);   // ids 1, 2: every row matches
        await table.WriteAsync([Batch(10, 3)]);  // ids 10..12: only 11 matches
        var before = DataFilesOnDisk();

        var (deleted, _) = await table.DeleteAsync(Ex.Or(Ex.LessThanOrEqual("id", 2L), Ex.Equal("id", 11L)));

        Assert.Equal(3, deleted);
        Assert.Equal(new long[] { 10, 12 }, await ReadIdsFreshAsync());
        // Exactly one data file written: the rewrite of the partly-matched file.
        Assert.Single(DataFilesOnDisk().Except(before));
        await using var check = await OpenAsync();
        Assert.Equal(2, Assert.Single(check.CurrentSnapshot.ActiveFiles.Values).GetNumRecords());
    }

    /// <summary>On a table that has deletion vectors, copy-on-write is chosen explicitly. The rewrite drops the
    /// file's existing vector along with the rows it hid, which stay deleted.</summary>
    [Fact]
    public async Task ExplicitCopyOnWrite_OnADeletionVectorTable_RewritesAndDropsTheExistingVector()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 5)]);
        await table.DeleteAsync(Ex.Equal("id", 2L)); // a vector, the table's default
        Assert.NotNull(Assert.Single(table.CurrentSnapshot.ActiveFiles.Values).DeletionVector);

        var (deleted, _) = await table.DeleteAsync(Ex.Equal("id", 4L), RowDeleteMode.CopyOnWrite);

        Assert.Equal(1, deleted);
        Assert.Equal(new long[] { 1, 3, 5 }, await ReadIdsFreshAsync());
        await using var check = await OpenAsync();
        var add = Assert.Single(check.CurrentSnapshot.ActiveFiles.Values);
        Assert.Null(add.DeletionVector);
        Assert.Equal(3, add.GetNumRecords());
    }

    /// <summary>The default on a deletion-vector table is unchanged: a vector on the same file.</summary>
    [Fact]
    public async Task OnADeletionVectorTable_TheDefaultIsStillAVector()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 5)]);
        string source = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values).Path;

        await table.DeleteAsync(TwoAndFour);

        await using var check = await OpenAsync();
        var add = Assert.Single(check.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal(source, add.Path);
        Assert.NotNull(add.DeletionVector);
        Assert.Equal(new long[] { 1, 3, 5 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task DeletionVectorMode_WithoutVectors_IsRefused_AndWritesNothing()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        long version = table.CurrentSnapshot.Version;
        var before = DataFilesOnDisk();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => table.DeleteAsync(TwoAndFourMask, RowDeleteMode.DeletionVector).AsTask());

        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.Equal(before, DataFilesOnDisk());
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, await ReadIdsFreshAsync());
    }

    /// <summary>Explicit vectors still remove a file the predicate matches entirely: that needs no vector.</summary>
    [Fact]
    public async Task DeletionVectorMode_WithoutVectors_StillRemovesAWholeFile()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(10, 2)]);

        var (deleted, _) = await table.DeleteAsync(Ex.LessThanOrEqual("id", 2L), RowDeleteMode.DeletionVector);

        Assert.Equal(2, deleted);
        Assert.Equal(new long[] { 10, 11 }, await ReadIdsFreshAsync());
    }

    /// <summary>
    /// IcebergCompat tables cannot have deletion vectors, and the rewrite does not yet cover them, so a partial
    /// delete there is refused as unsupported, and leaves nothing behind. A whole-file delete needs no rewrite.
    /// </summary>
    [Fact]
    public async Task OnAnIcebergCompatTable_APartialDeleteIsRefused_AWholeFileDeleteIsNot()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(),
            new DeltaTableOptions { AllowWritesWithoutUniversalFormatConversion = true },
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV2Key] = "true" });
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(10, 3)]);
        long version = table.CurrentSnapshot.Version;
        var before = DataFilesOnDisk();

        var refusal = await Assert.ThrowsAsync<NotSupportedException>(
            () => table.DeleteAsync(Ex.Equal("id", 11L)).AsTask());
        Assert.Contains("IcebergCompat", refusal.Message);
        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.Equal(before, DataFilesOnDisk());

        var (deleted, _) = await table.DeleteAsync(Ex.LessThanOrEqual("id", 2L));
        Assert.Equal(2, deleted);
        Assert.Equal(new long[] { 10, 11, 12 }, await ReadIdsFreshAsync());
    }

    /// <summary>
    /// On an IcebergCompat change-feed table, a DELETE that removes one file whole and needs to rewrite another
    /// is refused, but only once the scan reaches the second file. By then it has written the first file's change
    /// file. In a transaction the refusal takes that file back, so committing the rest leaves nothing orphaned.
    /// </summary>
    [Fact]
    public async Task OnAnIcebergCompatChangeFeedTable_ARefusedDeleteInATransaction_LeavesNoChangeFile()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(),
            new DeltaTableOptions { AllowWritesWithoutUniversalFormatConversion = true },
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string>
            {
                [IcebergCompat.EnableV2Key] = "true",
                ["delta.enableChangeDataFeed"] = "true",
            });
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(10, 3)]);
        var before = DataFilesOnDisk();

        var txn = table.StartTransaction();
        await Assert.ThrowsAsync<NotSupportedException>(
            () => txn.DeleteAsync(Ex.Or(Ex.LessThanOrEqual("id", 2L), Ex.Equal("id", 11L))).AsTask());
        Assert.Equal(before, DataFilesOnDisk());

        await txn.WriteAsync([Batch(100, 1)]);
        await txn.CommitAsync();

        Assert.Equal(before.Length + 1, DataFilesOnDisk().Length);
        Assert.Equal(new long[] { 1, 2, 10, 11, 12, 100 }, await ReadIdsFreshAsync());
    }

    /// <summary>
    /// A positional <c>default</c> second argument still binds to the <see cref="CancellationToken"/> overloads,
    /// the ones that choose the mode automatically: C# prefers the candidate that needs no default arguments
    /// filled in. Bound to <see cref="RowDeleteMode"/> instead, <c>default</c> would be
    /// <see cref="RowDeleteMode.DeletionVector"/>, which this table without deletion vectors refuses.
    /// </summary>
    [Fact]
    public async Task APositionalDefaultSecondArgument_StillChoosesTheModeAutomatically()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 8)]);

        await table.DeleteAsync(Ex.Equal("id", 1L), default);
        await table.DeleteAsync(batch => MaskForId(batch, 2), default);
        var txn = table.StartTransaction();
        await txn.DeleteAsync(Ex.Equal("id", 3L), default);
        await txn.CommitAsync();
        txn = table.StartTransaction();
        await txn.DeleteAsync(batch => MaskForId(batch, 4), default);
        await txn.CommitAsync();

        Assert.Equal(new long[] { 5, 6, 7, 8 }, await ReadIdsFreshAsync());
    }

    private static BooleanArray MaskForId(RecordBatch batch, long target)
    {
        var ids = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            mask.Append(ids.GetValue(i) == target);
        return mask.Build();
    }

    [Fact]
    public async Task NoMatchingRow_CommitsNothing()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        long version = table.CurrentSnapshot.Version;
        var before = DataFilesOnDisk();

        var (deleted, committed) = await table.DeleteAsync(Ex.Equal("id", 99L));

        Assert.Equal(0, deleted);
        Assert.Equal(version, committed);
        Assert.Equal(before, DataFilesOnDisk());
    }

    [Fact]
    public async Task OnARowTrackingTable_SurvivorsKeepTheirIds()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]);
        var idsBefore = await ReadRowIdsAsync();

        await table.DeleteAsync(TwoAndFour);

        var idsAfter = await ReadRowIdsAsync();
        Assert.Equal(new long[] { 1, 3, 5 }, idsAfter.Keys.OrderBy(k => k));
        foreach (long id in new long[] { 1, 3, 5 })
            Assert.Equal(idsBefore[id], idsAfter[id]);
    }

    /// <summary>The rewrite reserves a row-id range like any fresh add, so work staged after it on the same
    /// transaction must start past it. A rebase re-derives every range and would hide an overlap, so this
    /// commits uncontended.</summary>
    [Fact]
    public async Task InATransaction_OnARowTrackingTable_TheRewriteAndALaterStagedAppend_ReserveDisjointRanges()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]); // ids 0..4, high-water mark 5

        var txn = table.StartTransaction();
        Assert.Equal(2, await txn.DeleteAsync(TwoAndFour)); // a 3-row rewrite
        await txn.WriteAsync([Batch(50, 2)]);
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        var ranges = check.CurrentSnapshot.ActiveFiles.Values
            .Select(f => (Start: f.BaseRowId!.Value, Count: f.GetNumRecords() ?? 0))
            .OrderBy(r => r.Start).ToList();
        Assert.Equal(2, ranges.Count);
        Assert.True(ranges[0].Start + ranges[0].Count <= ranges[1].Start,
            $"overlapping row-id ranges: {string.Join(", ", ranges)}");
        Assert.Equal(5 + 3 + 2, check.CurrentSnapshot.RowIdHighWaterMark);
    }

    /// <summary>Without its own change file the feed would infer the remove/add pair as every survivor deleted and
    /// re-inserted.</summary>
    [Fact]
    public async Task OnAChangeFeedTable_TheFeedHoldsExactlyTheDeletedRows()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), configuration: Cdf());
        await table.WriteAsync([Batch(1, 5)]);

        var (_, version) = await table.DeleteAsync(TwoAndFour);

        Assert.Equal([(2L, "delete"), (4L, "delete")], await ReadFeedAsync(version));
    }

    /// <summary>A change-feed table whose delete removes one file whole and rewrites another: each file's deleted
    /// rows reach the feed once, whichever way the file went.</summary>
    [Fact]
    public async Task OnAChangeFeedTable_AWholeFileAndARewrite_EachReachTheFeedOnce()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), configuration: Cdf());
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(10, 3)]);

        var (_, version) = await table.DeleteAsync(Ex.Or(Ex.LessThanOrEqual("id", 2L), Ex.Equal("id", 11L)));

        Assert.Equal([(1L, "delete"), (2L, "delete"), (11L, "delete")], await ReadFeedAsync(version));
    }

    /// <summary>
    /// On a table with both row tracking and the change feed, each deleted row reaches the feed with the stable id
    /// and commit version a scan reported for it before the delete. The change file is written from the delete's
    /// scan, not from the rewrite, so this is what pins the two to the same identity.
    /// </summary>
    [Fact]
    public async Task OnAChangeFeedRowTrackingTable_TheFeedCarriesEachDeletedRowsStableId()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableRowTracking: true, configuration: Cdf());
        await table.WriteAsync([Batch(1, 5)]);
        // A first rewrite, so the rows the second delete removes carry MATERIALIZED ids, not baseRowId + position.
        await table.DeleteAsync(Ex.Equal("id", 1L));
        await using (var before = await OpenAsync())
            Assert.NotNull(Assert.Single(before.CurrentSnapshot.ActiveFiles.Values).BaseRowId);
        var idsBefore = await ReadRowIdsAsync();

        var (_, version) = await table.DeleteAsync(TwoAndFour);

        var feed = new Dictionary<long, (long RowId, long CommitVersion)>();
        await using (var check = await OpenAsync())
        {
            await foreach (var batch in check.ReadChangesAsync(new DeltaChangeReadOptions
            {
                StartVersion = version, EndVersion = version, Metadata = DeltaRowMetadata.RowTracking,
            }))
            {
                var ids = (Int64Array)batch.Column("id");
                var types = (StringArray)batch.Column("_change_type");
                var rowIds = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
                var versions = (Int64Array)batch.Column(
                    batch.Schema.GetFieldIndex(RowTrackingConfig.RowCommitVersionColumnName));
                for (int i = 0; i < batch.Length; i++)
                {
                    Assert.Equal("delete", types.GetString(i));
                    feed[ids.GetValue(i)!.Value] = (rowIds.GetValue(i)!.Value, versions.GetValue(i)!.Value);
                }
            }
        }

        Assert.Equal(new long[] { 2, 4 }, feed.Keys.OrderBy(k => k));
        Assert.Equal(idsBefore[2], feed[2].RowId);
        Assert.Equal(idsBefore[4], feed[4].RowId);
        // Written at version 1 and carried through the first rewrite unchanged.
        Assert.Equal(1, feed[2].CommitVersion);
        Assert.Equal(1, feed[4].CommitVersion);
    }

    [Fact]
    public async Task InATransaction_AbortDeletesTheRewrite()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), configuration: Cdf());
        await table.WriteAsync([Batch(1, 5)]);
        var before = DataFilesOnDisk();

        var txn = table.StartTransaction();
        await txn.DeleteAsync(TwoAndFour);
        Assert.True(DataFilesOnDisk().Length > before.Length); // the rewrite and its change file
        await txn.AbortAsync();

        Assert.Equal(before, DataFilesOnDisk());
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task InATransaction_RebasesPastAConcurrentAppend_KeepingRowIdsDistinct()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]);
        var idsBefore = await ReadRowIdsAsync();

        var txn = table.StartTransaction();
        await txn.DeleteAsync(TwoAndFour);

        // The racer's append takes the ids the delete's rewrite reserved, so the commit must re-derive them.
        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(100, 2)]);

        await txn.CommitAsync();

        var idsAfter = await ReadRowIdsAsync();
        Assert.Equal(new long[] { 1, 3, 5, 100, 101 }, idsAfter.Keys.OrderBy(k => k));
        foreach (long id in new long[] { 1, 3, 5 })
            Assert.Equal(idsBefore[id], idsAfter[id]);
        Assert.Equal(idsAfter.Count, idsAfter.Values.Distinct().Count());
    }

    [Fact]
    public async Task InATransaction_AConcurrentRewriteOfTheSameFile_Conflicts()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);

        var txn = table.StartTransaction();
        await txn.DeleteAsync(Ex.Equal("id", 2L));

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 4L)); // rewrites the same file

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
        Assert.Equal(new long[] { 1, 2, 3, 5 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task InATransaction_TwiceOnOneFile_RefusesTheSecond()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);

        var txn = table.StartTransaction();
        await txn.DeleteAsync(Ex.Equal("id", 2L));
        await Assert.ThrowsAsync<InvalidOperationException>(() => txn.DeleteAsync(Ex.Equal("id", 4L)).AsTask());
        await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 3, 4, 5 }, await ReadIdsFreshAsync());
    }
}
