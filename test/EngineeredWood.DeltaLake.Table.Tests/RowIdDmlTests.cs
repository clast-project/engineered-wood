// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The one-shot DML-by-TRANSIENT-rowid surface: a host reads rows with
/// <see cref="DeltaTable.ReadAllWithRowIdsAsync"/>, keeps the ids, and deletes/updates exactly those rows —
/// <see cref="DeltaTable.DeleteByRowIdsViaVectorsAsync"/> (deletion-vector soft delete, row-level-concurrency
/// aware) and the copy-on-write <see cref="DeltaTable.DeleteByRowIdsAsync"/> /
/// <see cref="DeltaTable.UpdateByRowIdsAsync"/> (file rewrite, no DVs — maximally reader-compatible).
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
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var id = (Int64Array)batch.Column("id");
            var rid = (Int64Array)batch.Column("_metadata.row_id");
            for (int i = 0; i < batch.Length; i++)
                if (wanted.Contains(id.GetValue(i)!.Value))
                    result.Add(rid.GetValue(i)!.Value);
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
    public async Task DeleteByRowIdsViaVectors_DeletesExactlyThoseRows()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 6)]); // ids 1..6

        var ids = await RowIdsOf(table, 3, 4);
        var (deleted, _) = await table.DeleteByRowIdsViaVectorsAsync(ids);
        Assert.Equal(2, deleted);

        Assert.Equal(new long[] { 1, 2, 5, 6 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task DeleteByRowIdsViaVectors_NonDvTable_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);
        var ids = await RowIdsOf(table, 2);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.DeleteByRowIdsViaVectorsAsync(ids));
    }

    [Fact]
    public async Task DeleteByRowIdsViaVectors_RowLevelRetry_ConcurrentDisjoint_BothLand()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(1, 6)]); // one file, ids 1..6
        }

        // two handles at the same base version, each deleting a DISJOINT row of the SAME file by rowid
        await using var tableA = await OpenAsync();
        await using var tableB = await OpenAsync();
        var idsA = await RowIdsOf(tableA, 2);
        var idsB = await RowIdsOf(tableB, 5);

        await tableA.DeleteByRowIdsViaVectorsAsync(idsA, rowLevelRetry: true);
        await tableB.DeleteByRowIdsViaVectorsAsync(idsB, rowLevelRetry: true); // stale → row-level rebase (DV union)

        Assert.Equal(new long[] { 1, 3, 4, 6 }, await ReadIdsFresh()); // both deletes composed
    }

    [Fact]
    public async Task DeleteByRowIdsViaVectors_EmptyIds_NoOp()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 3)]);
        long before = table.CurrentSnapshot.Version;
        var (deleted, version) = await table.DeleteByRowIdsViaVectorsAsync([]);
        Assert.Equal(0, deleted);
        Assert.Equal(before, version);
    }

    // ── copy-on-write DELETE by row id (no deletion vectors) ──

    [Fact]
    public async Task DeleteByRowIds_CopyOnWrite_DeletesRows_OnPlainTable()
    {
        // No DVs enabled — the copy-on-write path rewrites the file without the rows.
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 6)]); // ids 1..6, one file

        var ids = await RowIdsOf(table, 3, 4);
        var (deleted, _) = await table.DeleteByRowIdsAsync(ids);
        Assert.Equal(2, deleted);

        Assert.Equal(new long[] { 1, 2, 5, 6 }, await ReadIdsFresh());
        // No deletionVectors feature was declared (plain copy-on-write, maximally reader-compatible).
        await using var check = await OpenAsync();
        Assert.DoesNotContain(check.CurrentSnapshot.ActiveFiles.Values, a => a.DeletionVector is not null);
    }

    [Fact]
    public async Task DeleteByRowIds_CopyOnWrite_WholeFile_DropsFile()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 2)]); // file A
        await table.WriteAsync([Batch(100, 3)]); // file B

        // delete every row of file A (whichever ordinal it sorts to)
        var idsA = await RowIdsOf(table, 1, 2);
        await table.DeleteByRowIdsAsync(idsA);

        Assert.Equal(new long[] { 100, 101, 102 }, await ReadIdsFresh());
        await using var check = await OpenAsync();
        Assert.Single(check.CurrentSnapshot.ActiveFiles); // file A dropped outright, one file remains
    }

    [Fact]
    public async Task DeleteByRowIds_CopyOnWrite_RowTrackingTable_RewritesAndReads()
    {
        // A row-tracking table: the copy-on-write rewrite materializes survivors' ids (M2 path).
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]); // ids 1..5, baseRowId 0

        var ids = await RowIdsOf(table, 2);
        var (deleted, _) = await table.DeleteByRowIdsAsync(ids);
        Assert.Equal(1, deleted);
        Assert.Equal(new long[] { 1, 3, 4, 5 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task DeleteByRowIds_CopyOnWrite_CdfTable_CapturesChangeFeed()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new EngineeredWood.DeltaLake.Log.TransactionLog(fs);
        await log.WriteCommitAsync(0, new List<EngineeredWood.DeltaLake.Actions.DeltaAction>
        {
            new EngineeredWood.DeltaLake.Actions.ProtocolAction
            {
                MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["changeDataFeed"],
            },
            new EngineeredWood.DeltaLake.Actions.MetadataAction
            {
                Id = "cdf", Format = EngineeredWood.DeltaLake.Actions.Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { EngineeredWood.DeltaLake.ChangeDataFeed.CdfConfig.EnableKey, "true" },
                },
            },
        });
        await using var table = await DeltaTable.OpenAsync(fs);
        await table.WriteAsync([Batch(1, 3)]);
        var ids = await RowIdsOf(table, 2);

        // The copy-on-write DELETE captures the deleted rows' content into a _change_data file fused into
        // the same commit (the rewrite has them in hand) — the feed for that version is EXACTLY one
        // 'delete' row carrying id=2 (no inferred survivor noise: a commit with a cdc action is read
        // cdc-only).
        var (deleted, version) = await table.DeleteByRowIdsAsync(ids);
        Assert.Equal(1, deleted);

        var changeRows = new List<(string ChangeType, long Id)>();
        await foreach (var b in table.ReadChangesAsync(version, version))
        {
            int typeIdx = b.Schema.GetFieldIndex("_change_type");
            int idIdx = b.Schema.GetFieldIndex("id");
            var types = (Apache.Arrow.StringArray)b.Column(typeIdx);
            var idsCol = (Apache.Arrow.Int64Array)b.Column(idIdx);
            for (int i = 0; i < b.Length; i++)
                changeRows.Add((types.GetString(i), idsCol.GetValue(i)!.Value));
        }
        var change = Assert.Single(changeRows);
        Assert.Equal("delete", change.ChangeType);
        Assert.Equal(2, change.Id);
    }

    // ── copy-on-write UPDATE by row id ──

    // A rewriteFile callback that adds `delta` to the id of the rows whose id is in `targetIds`.
    private static Func<long, IReadOnlyList<RecordBatch>, IReadOnlyList<RecordBatch>> AddToIds(
        long delta, params long[] targetIds)
    {
        var wanted = new HashSet<long>(targetIds);
        return (_, batches) =>
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
    public async Task UpdateByRowIds_CopyOnWrite_ModifiesTargetedRows()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 5)]); // ids 1..5

        var ids = await RowIdsOf(table, 2, 4);
        long version = await table.UpdateByRowIdsAsync(ids, AddToIds(1000, 2, 4));
        Assert.Equal(2, version);

        Assert.Equal(new long[] { 1, 3, 5, 1002, 1004 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task UpdateByRowIds_CopyOnWrite_RowTrackingTable_RewritesAndReads()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 4)]); // ids 1..4

        var ids = await RowIdsOf(table, 3);
        await table.UpdateByRowIdsAsync(ids, AddToIds(100, 3));

        Assert.Equal(new long[] { 1, 2, 4, 103 }, await ReadIdsFresh());
    }

    [Fact]
    public async Task UpdateByRowIds_EmptyIds_NoOp()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);
        long before = table.CurrentSnapshot.Version;
        long version = await table.UpdateByRowIdsAsync([], AddToIds(1, 1));
        Assert.Equal(before, version);
    }

    // ── update-from-a-host-join: the rowid-keyed overloads (no content-matching) ──

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
    public async Task UpdateByRowIds_ValuesBatch_KeyedByRowId_AppliesJoinResult()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdScoreSchema);
        await table.WriteAsync([IdScoreBatch((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))]);

        // Simulate a DuckDB join result: the rowids to change + their new SET values, as one batch.
        long rid2 = (await RowIdsOf(table, 2)).Single();
        long rid4 = (await RowIdsOf(table, 4)).Single();
        var updates = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("_metadata.row_id", Int64Type.Default, false))
                .Field(new Field("score", Int64Type.Default, true))
                .Build(),
            [
                new Int64Array.Builder().Append(rid2).Append(rid4).Build(),
                new Int64Array.Builder().Append(999).Append(888).Build(),
            ], 2);

        long version = await table.UpdateByRowIdsAsync(updates);
        Assert.Equal(2, version);

        var scores = await ReadIdScoresFresh();
        Assert.Equal(999, scores[2]); // targeted rows take the new score
        Assert.Equal(888, scores[4]);
        Assert.Equal(10, scores[1]);  // everything else untouched (incl. the un-updated `id` column)
        Assert.Equal(30, scores[3]);
        Assert.Equal(50, scores[5]);
    }

    [Fact]
    public async Task UpdateByRowIds_RowIdExposedDelegate_KeysNewValuesByRowId()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdScoreSchema);
        await table.WriteAsync([IdScoreBatch((1, 10), (2, 20), (3, 30))]);

        long rid2 = (await RowIdsOf(table, 2)).Single();
        var newScoreByRowId = new Dictionary<long, long> { [rid2] = 777 };

        await table.UpdateByRowIdsAsync(
            newScoreByRowId.Keys.ToArray(),
            (long ordinal, IReadOnlyList<RecordBatch> batches, IReadOnlyList<Int64Array> rids) =>
            {
                var outp = new List<RecordBatch>(batches.Count);
                for (int b = 0; b < batches.Count; b++)
                {
                    var src = batches[b];
                    var score = (Int64Array)src.Column("score");
                    var nb = new Int64Array.Builder();
                    for (int i = 0; i < src.Length; i++)
                        nb.Append(newScoreByRowId.TryGetValue(rids[b].GetValue(i)!.Value, out long v)
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
    public async Task UpdateByRowIds_ValuesBatch_MissingRowIdColumn_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdScoreSchema);
        await table.WriteAsync([IdScoreBatch((1, 10))]);

        var bad = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("score", Int64Type.Default, true)).Build(),
            [new Int64Array.Builder().Append(1).Build()], 1);
        await Assert.ThrowsAsync<ArgumentException>(() => table.UpdateByRowIdsAsync(bad).AsTask());
    }
}
