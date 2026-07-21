// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Row-level concurrency (the Databricks extension beyond OSS): two writers touching DISJOINT rows of the
/// SAME file both land, instead of the second aborting at file granularity. A DELETE marks rows with a
/// deletion vector and never moves a surviving row, so the losing delete's DV can be rebased onto the
/// winner's — the union of the two — rather than conflicting. Overlapping rows, or a file rewritten away
/// entirely, remain genuine conflicts.
///
/// <para>This covers Layer 3 sub-problem (A) — DELETE/DELETE deletion-vector union — which needs no
/// row tracking because DV positions are stable across a concurrent DV-delete. Sub-problem (B) (a delete
/// remapped through a concurrent compaction/UPDATE rewrite by stable row id) is separate and still
/// parked in <see cref="PendingCoverageTests"/>.</para>
/// </summary>
public class RowLevelConcurrencyTests : IDisposable
{
    private readonly string _tempDir;

    public RowLevelConcurrencyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rowlevel_{Guid.NewGuid():N}");
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

    private static RecordBatch Batch(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private static Func<RecordBatch, BooleanArray> IdEquals(long target) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    private async Task<List<long>> ReadIdsFresh()
    {
        await using var reader = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
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

    /// <summary>
    /// Two concurrent deletes touching DISJOINT rows of the SAME file both land. The point of row-level
    /// concurrency: a file-level checker would reject the second (it removes a file the first already
    /// removed), but the rows are disjoint, so the loser rebases its deletion vector onto the winner's —
    /// the union deletes both rows — and commits one version later.
    /// </summary>
    [Fact]
    public async Task ConcurrentDeletes_SameFile_DisjointRows_BothLand()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(5, 7)]); // both rows in ONE file
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        long baseVersion = tableA.CurrentSnapshot.Version;
        Assert.Equal(baseVersion, tableB.CurrentSnapshot.Version);

        // A deletes row 5 and commits; B (stale) deletes the DISJOINT row 7.
        var (rowsA, vA) = await tableA.DeleteAsync(IdEquals(5));
        var (rowsB, vB) = await tableB.DeleteAsync(IdEquals(7));

        Assert.Equal(1, rowsA);
        Assert.Equal(1, rowsB);
        Assert.Equal(baseVersion + 1, vA);
        Assert.Equal(baseVersion + 2, vB); // collided, DV-union rebased, landed

        Assert.Empty(await ReadIdsFresh()); // both rows gone
    }

    /// <summary>
    /// Two concurrent deletes of the SAME row conflict. The loser's rebase finds its row already deleted in
    /// the file's current deletion vector — a genuine row-level conflict, not a disjoint-row union — so it
    /// aborts rather than silently commit a no-op or double-delete.
    /// </summary>
    [Fact]
    public async Task ConcurrentDeletes_SameRow_RowLevelConflict()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(5, 7)]);
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        long baseVersion = tableA.CurrentSnapshot.Version;

        var (_, vA) = await tableA.DeleteAsync(IdEquals(5));
        Assert.Equal(baseVersion + 1, vA);

        // B (stale) also deletes row 5 — the same row A just removed.
        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await tableB.DeleteAsync(IdEquals(5)));
        Assert.Contains("row level", ex.Message, StringComparison.OrdinalIgnoreCase);

        Assert.Equal([7L], await ReadIdsFresh()); // only A's delete landed
    }

    /// <summary>
    /// The row-level retry is an addition, not a silencer: a genuine version conflict that it CANNOT
    /// reconcile still surfaces. Here a stale delete races a full OVERWRITE, which rewrites the file away
    /// entirely — the delete's target is gone, so no deletion-vector rebase is possible and the conflict
    /// aborts, exactly as it would without the row-level path.
    /// </summary>
    [Fact]
    public async Task WithoutRowLevelRetry_VersionConflictSurfaces()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(5, 7)]);
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        // A overwrites the whole table — the file B's delete targets no longer exists afterwards.
        await tableA.WriteAsync([Batch(9)], DeltaWriteMode.Overwrite);

        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await tableB.DeleteAsync(IdEquals(5)));

        Assert.Equal([9L], await ReadIdsFresh()); // only A's overwrite landed
    }

    /// <summary>
    /// The transactional path composes with row-level concurrency too: a transaction stages a delete, a
    /// concurrent delete of a disjoint row in the same file lands, and the transaction rebases its DV onto
    /// it at commit rather than aborting.
    /// </summary>
    [Fact]
    public async Task Transaction_DisjointRowDelete_RebasesPastConcurrentDelete()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(5, 7)]); // one file
        long baseVersion = table.CurrentSnapshot.Version;

        var tx = table.StartTransaction();
        await tx.DeleteAsync(IdEquals(5));

        // A concurrent delete of the disjoint row 7 lands through the same handle while tx is open.
        await table.DeleteAsync(IdEquals(7));

        long committed = await tx.CommitAsync(); // rebases its DV onto the concurrent one
        Assert.Equal(baseVersion + 2, committed);

        Assert.Empty(await ReadIdsFresh());
    }

    // ── Layer 3 sub-problem (B): remap a losing DELETE across a concurrent REWRITE ──
    //
    // When a concurrent compaction or copy-on-write UPDATE rewrites the file a stale delete targets, the file
    // is gone — the DV-union path of (A) cannot reconcile it. With row tracking enabled the deleted rows are
    // relocated by STABLE ROW ID onto the new file(s) (the rewrite preserved each row's original id + commit
    // version, Milestone 2), and the row's commit version is the concurrent-modification discriminator: an
    // untouched-but-relocated row both-lands; a concurrently updated/deleted row is a row-level conflict.

    private static Apache.Arrow.Schema RtSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("val", StringType.Default, true))
        .Build();

    private static RecordBatch RtBatch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var vals = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            vals.Append("v" + (startId + i));
        }
        return new RecordBatch(RtSchema, [ids.Build(), vals.Build()], count);
    }

    private static Func<RecordBatch, BooleanArray> RtIdEquals(long target) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    private async Task<List<(long Id, string? Val)>> ReadRowsFresh()
    {
        await using var reader = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var rows = new List<(long, string?)>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var id = (Int64Array)batch.Column("id");
            var val = (StringArray)batch.Column("val");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((id.GetValue(i)!.Value, val.GetString(i)));
        }

        rows.Sort((a, b) => a.Item1.CompareTo(b.Item1));
        return rows;
    }

    /// <summary>
    /// A concurrent UPDATE and DELETE of DISJOINT rows both land. The UPDATE copy-on-write-rewrites the file
    /// (moving every row to a new file, its ids materialized); the stale delete's target file is gone, so the
    /// deleted row is remapped by stable id onto the rewritten file — where it is untouched (same commit
    /// version) — and both changes survive.
    /// </summary>
    [Fact]
    public async Task ConcurrentUpdateAndDelete_DisjointRows_BothLand()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), RtSchema,
            enableDeletionVectors: true, enableRowTracking: true))
        {
            await setup.WriteAsync([RtBatch(1, 3)]); // id 1,2,3 in ONE file (stable ids 0,1,2)
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        long baseVersion = tableA.CurrentSnapshot.Version;

        // A updates id 2's value (rewrites the file); B (stale) deletes the disjoint id 1.
        var (updated, vA) = await tableA.UpdateAsync(
            RtIdEquals(2),
            batch =>
            {
                var id = (Int64Array)batch.Column("id");
                var vals = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    vals.Append(id.GetValue(i) == 2 ? "updated" : ((StringArray)batch.Column("val")).GetString(i));
                return new RecordBatch(RtSchema, [batch.Column(0), vals.Build()], batch.Length);
            });
        Assert.Equal(1, updated);
        Assert.Equal(baseVersion + 1, vA);

        var (deleted, vB) = await tableB.DeleteAsync(RtIdEquals(1));
        Assert.Equal(1, deleted);
        Assert.Equal(baseVersion + 2, vB); // collided with A's rewrite, remapped by stable id, landed

        Assert.Equal(
            new (long, string?)[] { (2, "updated"), (3, "v3") },
            await ReadRowsFresh());
    }

    /// <summary>
    /// A delete whose target file was concurrently COMPACTED is remapped onto the compacted file rather than
    /// failing. The compaction merges both files into one, carrying each surviving row's original id/version;
    /// the stale delete's row is located by that stable id and soft-deleted with a deletion vector on the new
    /// compacted file.
    /// </summary>
    [Fact]
    public async Task DeleteThroughConcurrentCompaction_Remapped()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), RtSchema,
            enableDeletionVectors: true, enableRowTracking: true))
        {
            await setup.WriteAsync([RtBatch(1, 3)]); // file 1: id 1,2,3
            await setup.WriteAsync([RtBatch(4, 2)]); // file 2: id 4,5 — something to compact with
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        // A compacts both files into one (materialized ids preserved).
        await tableA.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });

        // B (stale) deletes id 2 — its file is gone; the row is remapped onto the compacted file.
        var (deleted, _) = await tableB.DeleteAsync(RtIdEquals(2));
        Assert.Equal(1, deleted);

        var rows = await ReadRowsFresh();
        Assert.Equal(new long[] { 1, 3, 4, 5 }, rows.ConvertAll(r => r.Id).ToArray());
    }

    /// <summary>
    /// ...but if the same row was ALSO concurrently deleted, the remap finds its stable id gone from the
    /// post-rewrite state (the compacted file's deletion vector hides it) — a genuine row-level conflict that
    /// aborts rather than silently double-deleting.
    /// </summary>
    [Fact]
    public async Task DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), RtSchema,
            enableDeletionVectors: true, enableRowTracking: true))
        {
            await setup.WriteAsync([RtBatch(1, 3)]);
            await setup.WriteAsync([RtBatch(4, 2)]);
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        // A compacts, then deletes the same row B targets — its stable id is gone after the rewrite.
        await tableA.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        await tableA.DeleteAsync(RtIdEquals(2));

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await tableB.DeleteAsync(RtIdEquals(2)));
        Assert.Contains("row level", ex.Message, StringComparison.OrdinalIgnoreCase);

        var rows = await ReadRowsFresh();
        Assert.Equal(new long[] { 1, 3, 4, 5 }, rows.ConvertAll(r => r.Id).ToArray()); // deleted exactly once
    }
}
