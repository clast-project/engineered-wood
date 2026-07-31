// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The one-shot row-level DML surface: a host reads rows, keeps their addresses, resolves them into a
/// <see cref="RowSelection"/>, and deletes/updates exactly those rows — <see cref="DeltaTable.DeleteRowsAsync"/>
/// in both <see cref="RowDeleteMode"/>s (deletion-vector soft delete, row-level-concurrency aware; and the
/// copy-on-write file rewrite, no DVs — maximally reader-compatible) plus
/// <see cref="DeltaTable.UpdateRowsAsync(RowSelection, Func{string, IReadOnlyList{RecordBatch}, IReadOnlyList{Int64Array}, IReadOnlyList{RecordBatch}}, CancellationToken)"/>.
/// </summary>
public class RowIdDmlTests : IDisposable
{
    private readonly string _tempDir;

    public RowIdDmlTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rowiddml_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            ids.Append(startId + i);
        return new RecordBatch(IdSchema, [ids.Build()], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>Transient rowids of the rows whose id is in <paramref name="ids"/>, in the current snapshot.</summary>
    private static async Task<List<long>> RowIdsOf(DeltaTable table, params long[] ids)
    {
        var wanted = new HashSet<long>(ids);
        var result = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
        {
            var id = (Int64Array)batch.Column("id");
            var rid = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
                if (wanted.Contains(id.GetValue(i)!.Value))
                    result.Add(rid.GetValue(i)!.Value);
        }
        return result;
    }

    /// <summary>The rows whose id is in <paramref name="ids"/>, as the path-keyed DML boundary key —
    /// resolved against the snapshot the addresses were read from, which is what the DML now speaks.</summary>
    private static async Task<RowSelection> SelectionOf(DeltaTable table, params long[] ids) =>
        RowSelection.FromRowAddresses(await RowIdsOf(table, ids), table.CurrentSnapshot);

    /// <summary>The (add.path, absolute position) locator pair for each row whose id is in
    /// <paramref name="ids"/>, read straight off the wire — the same pair the DML consumes.</summary>
    private static async Task<List<(string Path, long Position)>> LocatorsOf(
        DeltaTable table, params long[] ids)
    {
        var wanted = new HashSet<long>(ids);
        var result = new List<(string, long)>();
        await foreach (var batch in table.ReadAsync(
            new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
        {
            var id = (Int64Array)batch.Column("id");
            var path = (StringArray)batch.Column(
                DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.FilePathSuffix);
            var position = (Int64Array)batch.Column(
                DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIndexSuffix);
            for (int i = 0; i < batch.Length; i++)
                if (wanted.Contains(id.GetValue(i)!.Value))
                    result.Add((path.GetString(i), position.GetValue(i)!.Value));
        }
        return result;
    }

    private async Task<List<long>> ReadIdsFresh()
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

    [Fact]
    public async Task DeleteRowsViaVectors_DeletesExactlyThoseRows()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 6)]); // ids 1..6

        var (deleted, _) = await table.DeleteRowsAsync(await SelectionOf(table, 3, 4));
        Assert.Equal(2, deleted);

        Assert.Equal(new long[] { 1, 2, 5, 6 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task DeleteRowsViaVectors_NonDvTable_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);
        var selection = await SelectionOf(table, 2);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.DeleteRowsAsync(selection));
    }

    [Fact]
    public async Task DeleteRowsViaVectors_RowLevelRetry_ConcurrentDisjoint_BothLand()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(1, 6)]); // one file, ids 1..6
        }

        // two handles at the same base version, each deleting a DISJOINT row of the SAME file by rowid
        await using var tableA = await OpenAsync();
        await using var tableB = await OpenAsync();
        var selA = await SelectionOf(tableA, 2);
        var selB = await SelectionOf(tableB, 5);

        await tableA.DeleteRowsAsync(selA, rowLevelRetry: true);
        await tableB.DeleteRowsAsync(selB, rowLevelRetry: true); // stale → row-level rebase (DV union)

        Assert.Equal(new long[] { 1, 3, 4, 6 }, await ReadIdsFresh()); // both deletes composed
    }

    [Fact]
    public async Task DeleteRowsViaVectors_EmptySelection_NoOp()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 3)]);
        long before = table.CurrentSnapshot.Version;
        var (deleted, version) = await table.DeleteRowsAsync(RowSelection.ByPath(
            new Dictionary<string, IReadOnlyCollection<long>>()));
        Assert.Equal(0, deleted);
        Assert.Equal(before, version);
    }

    // ── copy-on-write DELETE by row id (no deletion vectors) ──

    [Fact]
    public async Task DeleteRows_CopyOnWrite_DeletesRows_OnPlainTable()
    {
        // No DVs enabled — the copy-on-write path rewrites the file without the rows.
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 6)]); // ids 1..6, one file

        var (deleted, _) = await table.DeleteRowsAsync(
            await SelectionOf(table, 3, 4), RowDeleteMode.CopyOnWrite);
        Assert.Equal(2, deleted);

        Assert.Equal(new long[] { 1, 2, 5, 6 }, await ReadIdsFresh());
        // No deletionVectors feature was declared (plain copy-on-write, maximally reader-compatible).
        await using var check = await OpenAsync();
        Assert.DoesNotContain(check.CurrentSnapshot.ActiveFiles.Values, a => a.DeletionVector is not null);
    }

    [Fact]
    public async Task DeleteRows_CopyOnWrite_WholeFile_DropsFile()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 2)]); // file A
        await table.WriteAsync([Batch(100, 3)]); // file B

        // delete every row of file A (whichever ordinal it sorts to)
        await table.DeleteRowsAsync(await SelectionOf(table, 1, 2), RowDeleteMode.CopyOnWrite);

        Assert.Equal(new long[] { 100, 101, 102 }, await ReadIdsFresh());
        await using var check = await OpenAsync();
        Assert.Single(check.CurrentSnapshot.ActiveFiles); // file A dropped outright, one file remains
    }

    [Fact]
    public async Task DeleteRows_CopyOnWrite_RowTrackingTable_RewritesAndReads()
    {
        // A row-tracking table: the copy-on-write rewrite materializes survivors' ids (M2 path).
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]); // ids 1..5, baseRowId 0

        var (deleted, _) = await table.DeleteRowsAsync(
            await SelectionOf(table, 2), RowDeleteMode.CopyOnWrite);
        Assert.Equal(1, deleted);
        Assert.Equal(new long[] { 1, 3, 4, 5 }, await ReadIdsFresh());
    }

    // ── copy-on-write UPDATE by row id ──

    // A rewriteFile callback that adds `delta` to the id of the rows whose id is in `targetIds`.
    private static Func<string, IReadOnlyList<RecordBatch>, IReadOnlyList<Int64Array>,
                        IReadOnlyList<RecordBatch>> AddToIds(long delta, params long[] targetIds)
    {
        var wanted = new HashSet<long>(targetIds);
        return (_, batches, _) =>
        {
            var outp = new List<RecordBatch>(batches.Count);
            foreach (var b in batches)
            {
                var id = (Int64Array)b.Column("id");
                var nb = new Int64Array.Builder();
                for (int i = 0; i < b.Length; i++)
                {
                    long v = id.GetValue(i)!.Value;
                    nb.Append(wanted.Contains(v) ? v + delta : v);
                }
                outp.Add(new RecordBatch(IdSchema, [nb.Build()], b.Length));
            }
            return outp;
        };
    }

    [Fact]
    public async Task UpdateRows_CopyOnWrite_ModifiesTargetedRows()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 5)]); // ids 1..5

        long version = await table.UpdateRowsAsync(await SelectionOf(table, 2, 4), AddToIds(1000, 2, 4));
        Assert.Equal(2, version);

        Assert.Equal(new long[] { 1, 3, 5, 1002, 1004 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task UpdateRows_CopyOnWrite_RowTrackingTable_RewritesAndReads()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 4)]); // ids 1..4

        await table.UpdateRowsAsync(await SelectionOf(table, 3), AddToIds(100, 3));

        Assert.Equal(new long[] { 1, 2, 4, 103 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task UpdateRows_EmptySelection_NoOp()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);
        long before = table.CurrentSnapshot.Version;
        long version = await table.UpdateRowsAsync(
            RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>()), AddToIds(1, 1));
        Assert.Equal(before, version);
    }

    // ── update-from-a-host-join: the locator-keyed overloads (no content-matching) ──

    /// <summary>A "join result" batch in the shape the convenience UpdateRowsAsync consumes: the locator pair
    /// plus one Int64 SET column.</summary>
    private static RecordBatch LocatorUpdates(
        IReadOnlyList<(string Path, long Position)> locators, string setColumn, params long[] values)
    {
        var paths = new StringArray.Builder();
        var positions = new Int64Array.Builder();
        var set = new Int64Array.Builder();
        for (int i = 0; i < locators.Count; i++)
        {
            paths.Append(locators[i].Path);
            positions.Append(locators[i].Position);
            set.Append(values[i]);
        }
        return new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field(
                    RowSelection.DefaultMetadataPrefix + RowSelection.FilePathColumnSuffix,
                    StringType.Default, false))
                .Field(new Field(
                    RowSelection.DefaultMetadataPrefix + RowSelection.RowIndexColumnSuffix,
                    Int64Type.Default, false))
                .Field(new Field(setColumn, Int64Type.Default, true))
                .Build(),
            [paths.Build(), positions.Build(), set.Build()], locators.Count);
    }

    private static Apache.Arrow.Schema IdScoreSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("score", Int64Type.Default, true))
        .Build();

    private static RecordBatch IdScoreBatch(params (long Id, long Score)[] rows)
    {
        var id = new Int64Array.Builder();
        var score = new Int64Array.Builder();
        foreach (var (i, s) in rows) { id.Append(i); score.Append(s); }
        return new RecordBatch(IdScoreSchema, [id.Build(), score.Build()], rows.Length);
    }

    private async Task<Dictionary<long, long>> ReadIdScoresFresh()
    {
        await using var reader = await OpenAsync();
        var map = new Dictionary<long, long>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var id = (Int64Array)batch.Column("id");
            var score = (Int64Array)batch.Column("score");
            for (int i = 0; i < batch.Length; i++)
                map[id.GetValue(i)!.Value] = score.GetValue(i)!.Value;
        }
        return map;
    }

    [Fact]
    public async Task UpdateRows_ValuesBatch_KeyedByLocator_AppliesJoinResult()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdScoreSchema);
        await table.WriteAsync([IdScoreBatch((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))]);

        // Simulate a DuckDB join result: the rows to change + their new SET values, as one batch.
        var locators = await LocatorsOf(table, 2, 4);
        var updates = LocatorUpdates(locators, "score", 999, 888);

        long version = await table.UpdateRowsAsync(updates);
        Assert.Equal(2, version);

        var scores = await ReadIdScoresFresh();
        Assert.Equal(999, scores[2]); // targeted rows take the new score
        Assert.Equal(888, scores[4]);
        Assert.Equal(10, scores[1]);  // everything else untouched (incl. the un-updated `id` column)
        Assert.Equal(30, scores[3]);
        Assert.Equal(50, scores[5]);
    }

    [Fact]
    public async Task UpdateRows_PositionExposedDelegate_KeysNewValuesByLocator()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdScoreSchema);
        await table.WriteAsync([IdScoreBatch((1, 10), (2, 20), (3, 30))]);

        var target = (await LocatorsOf(table, 2)).Single();
        var newScoreByLocator = new Dictionary<(string, long), long> { [target] = 777 };

        await table.UpdateRowsAsync(
            RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
            {
                [target.Path] = new[] { target.Position },
            }),
            (string path, IReadOnlyList<RecordBatch> batches, IReadOnlyList<Int64Array> positions) =>
            {
                var outp = new List<RecordBatch>(batches.Count);
                for (int b = 0; b < batches.Count; b++)
                {
                    var src = batches[b];
                    var score = (Int64Array)src.Column("score");
                    var nb = new Int64Array.Builder();
                    for (int i = 0; i < src.Length; i++)
                        nb.Append(
                            newScoreByLocator.TryGetValue((path, positions[b].GetValue(i)!.Value), out long v)
                                ? v : score.GetValue(i)!.Value);
                    outp.Add(new RecordBatch(src.Schema, [src.Column("id"), nb.Build()], src.Length));
                }
                return outp;
            });

        var scores = await ReadIdScoresFresh();
        Assert.Equal(777, scores[2]);
        Assert.Equal(10, scores[1]);
        Assert.Equal(30, scores[3]);
    }

    [Fact]
    public async Task UpdateRows_ValuesBatch_MissingLocatorColumns_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdScoreSchema);
        await table.WriteAsync([IdScoreBatch((1, 10))]);

        var bad = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("score", Int64Type.Default, true)).Build(),
            [new Int64Array.Builder().Append(1).Build()], 1);
        await Assert.ThrowsAsync<ArgumentException>(() => table.UpdateRowsAsync(bad).AsTask());
    }

    // ── Change Data Feed on the row-id DML paths ──
    //
    // A copy-on-write DELETE/UPDATE commits remove(old)+add(new), so an INFERRED feed would report every
    // surviving row of the rewritten file as deleted and re-inserted. Both paths therefore write change files
    // for exactly the rows they touched, which makes the reader use them instead of inferring.

    private static Apache.Arrow.Schema RegionSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, true))
        .Field(new Field("val", Int64Type.Default, true))
        .Build();

    private static RecordBatch RegionBatch(long startId, int count, string region)
    {
        var ids = new Int64Array.Builder();
        var regions = new StringArray.Builder();
        var vals = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            regions.Append(region);
            vals.Append(100 + startId + i);
        }
        return new RecordBatch(RegionSchema, [ids.Build(), regions.Build(), vals.Build()], count);
    }

    private Task<DeltaTable> CreateCdfTableAsync(
        Apache.Arrow.Schema schema,
        IReadOnlyList<string>? partitionColumns = null,
        bool enableDeletionVectors = false)
        => DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema,
            partitionColumns: partitionColumns,
            enableDeletionVectors: enableDeletionVectors,
            configuration: new Dictionary<string, string>
            {
                { EngineeredWood.DeltaLake.ChangeDataFeed.CdfConfig.EnableKey, "true" },
            }).AsTask();

    /// <summary>The feed over a version range as sorted "changeType|col=value|…" rows (metadata columns dropped,
    /// every data column of the feed's schema included in order — so a lost or duplicated column shows up).</summary>
    private async Task<List<string>> FeedRowsAsync(long from, long to)
    {
        await using var reader = await OpenAsync();
        var rows = new List<string>();
        await foreach (var b in reader.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = from, EndVersion = to }))
        {
            var changeType = (StringArray)b.Column(EngineeredWood.DeltaLake.ChangeDataFeed.CdfConfig.ChangeTypeColumn);
            for (int i = 0; i < b.Length; i++)
            {
                var parts = new List<string> { changeType.GetString(i) };
                for (int c = 0; c < b.ColumnCount; c++)
                {
                    string name = b.Schema.FieldsList[c].Name;
                    if (name == EngineeredWood.DeltaLake.ChangeDataFeed.CdfConfig.ChangeTypeColumn
                        || name == EngineeredWood.DeltaLake.ChangeDataFeed.CdfConfig.CommitVersionColumn
                        || name == EngineeredWood.DeltaLake.ChangeDataFeed.CdfConfig.CommitTimestampColumn)
                        continue;
                    parts.Add($"{name}={CellText(b.Column(c), i)}");
                }
                rows.Add(string.Join("|", parts));
            }
        }
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static string CellText(IArrowArray column, int i) => column switch
    {
        Int64Array a => a.IsNull(i) ? "null" : a.GetValue(i)!.Value.ToString(),
        StringArray s => s.IsNull(i) ? "null" : s.GetString(i),
        _ => column.GetType().Name,
    };

    [Fact]
    public async Task DeleteRows_CopyOnWrite_CdfTable_FeedReportsOnlyTheDeletedRows()
    {
        await using var table = await CreateCdfTableAsync(IdSchema);
        await table.WriteAsync([Batch(1, 4)]); // ids 1..4, one file

        var (deleted, version) = await table.DeleteRowsAsync(
            await SelectionOf(table, 2), RowDeleteMode.CopyOnWrite);
        Assert.Equal(1, deleted);

        // Exactly one delete — the survivors 1, 3 and 4 were rewritten, not changed.
        Assert.Equal(["delete|id=2"], await FeedRowsAsync(version, version));
        Assert.Equal(new long[] { 1, 3, 4 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task UpdateRows_CopyOnWrite_CdfTable_FeedReportsPreAndPostImages()
    {
        await using var table = await CreateCdfTableAsync(IdScoreSchema);
        await table.WriteAsync([IdScoreBatch((1, 10), (2, 20), (3, 30))]);

        var updates = LocatorUpdates(await LocatorsOf(table, 2), "score", 999);

        long version = await table.UpdateRowsAsync(updates);

        Assert.Equal(
            ["update_postimage|id=2|score=999", "update_preimage|id=2|score=20"],
            await FeedRowsAsync(version, version));
    }

    [Fact]
    public async Task DeleteRows_CopyOnWrite_PartitionedCdfTable_FeedKeepsEveryColumn()
    {
        // Partition columns live in the action's partitionValues, never in the change file's bytes — the reader
        // re-materializes them POSITIONALLY, so a partition column written into the file would come back twice
        // and push the columns after it out of the feed.
        await using var table = await CreateCdfTableAsync(RegionSchema, partitionColumns: ["region"]);
        await table.WriteAsync([RegionBatch(1, 3, "east")]);

        var (_, version) = await table.DeleteRowsAsync(
            await SelectionOf(table, 2), RowDeleteMode.CopyOnWrite);

        Assert.Equal(["delete|id=2|region=east|val=102"], await FeedRowsAsync(version, version));
    }

    [Fact]
    public async Task DeleteRows_CopyOnWrite_WholeFileCdfTable_FeedReportsEveryRowOnce()
    {
        // Deleting every row of a file drops it outright — there is no add to pair with the remove, and the
        // change files are the only record of which rows went away.
        await using var table = await CreateCdfTableAsync(IdSchema);
        await table.WriteAsync([Batch(1, 2)]); // file A
        await table.WriteAsync([Batch(100, 2)]); // file B

        var (deleted, version) = await table.DeleteRowsAsync(
            await SelectionOf(table, 1, 2), RowDeleteMode.CopyOnWrite);
        Assert.Equal(2, deleted);

        Assert.Equal(["delete|id=1", "delete|id=2"], await FeedRowsAsync(version, version));
        Assert.Equal(new long[] { 100, 101 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task DeleteRowsViaVectors_PartitionedCdfTable_FeedKeepsEveryColumn()
    {
        // Same shape through the deletion-vector path, whose CDC rows also come from the read path (which
        // materializes partition columns).
        await using var table = await CreateCdfTableAsync(
            RegionSchema, partitionColumns: ["region"], enableDeletionVectors: true);
        await table.WriteAsync([RegionBatch(1, 3, "east")]);

        var (_, version) = await table.DeleteRowsAsync(await SelectionOf(table, 3));

        Assert.Equal(["delete|id=3|region=east|val=103"], await FeedRowsAsync(version, version));
    }
}
