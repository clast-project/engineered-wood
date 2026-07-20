// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// End-to-end optimistic-concurrency tests: two overlapping transactions, the second committing while
/// the first is still open. This is the shape OptimisticTransaction exists for — the first transaction
/// commits only if the concurrent change did not invalidate what it read, and aborts if it did, instead
/// of failing on every version collision the way the raw commit path does.
/// </summary>
public class DeltaTransactionTests : IDisposable
{
    private readonly string _tempDir;

    public DeltaTransactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_txn_{Guid.NewGuid():N}");
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

    /// <summary>"delete the row(s) with this id" as the functional predicate DeleteAsync takes.</summary>
    private static Func<RecordBatch, BooleanArray> IdEquals(long target) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    private static async Task<List<long>> ReadIds(DeltaTable table)
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

    /// <summary>
    /// Two transactions delete rows living in DIFFERENT files. Neither touches what the other read, so
    /// the second to commit rebases onto the first's version instead of failing — both land, at
    /// consecutive versions, and both rows are gone.
    /// </summary>
    [Fact]
    public async Task TwoTransactions_DisjointFiles_BothCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);

        // Two separate appends => two files: id 5 in one, id 7 in the other.
        await table.WriteAsync([Batch(5)]);
        await table.WriteAsync([Batch(7)]);
        long baseVersion = table.CurrentSnapshot.Version;

        var tx1 = table.StartTransaction();
        var tx2 = table.StartTransaction();
        Assert.Equal(baseVersion, tx1.ReadVersion);
        Assert.Equal(baseVersion, tx2.ReadVersion);

        await tx2.DeleteAsync(IdEquals(7));
        long v2 = await tx2.CommitAsync();

        await tx1.DeleteAsync(IdEquals(5));
        long v1 = await tx1.CommitAsync();

        // tx2 took baseVersion+1; tx1 could not (collision) but had no conflict, so it rebased to +2.
        Assert.Equal(baseVersion + 1, v2);
        Assert.Equal(baseVersion + 2, v1);

        Assert.Empty(await ReadIds(table));
    }

    /// <summary>
    /// Two transactions delete the SAME row of the same file. Under row-level concurrency a delete rebases
    /// its deletion vector onto a concurrent one when the rows are disjoint, but here they overlap — the
    /// row tx1 removes was already removed by tx2 — so tx1 aborts rather than double-delete. (Disjoint rows
    /// of the same file both landing is covered by <see cref="RowLevelConcurrencyTests"/>.)
    /// </summary>
    [Fact]
    public async Task TwoTransactions_SameRow_SecondAborts()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);

        // A single file holding both rows.
        await table.WriteAsync([Batch(5, 7)]);
        long baseVersion = table.CurrentSnapshot.Version;

        var tx1 = table.StartTransaction();
        var tx2 = table.StartTransaction();

        await tx2.DeleteAsync(IdEquals(7));
        long v2 = await tx2.CommitAsync();
        Assert.Equal(baseVersion + 1, v2);

        await tx1.DeleteAsync(IdEquals(7)); // the same row tx2 just removed
        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await tx1.CommitAsync());
        Assert.Contains("row level", ex.Message, StringComparison.OrdinalIgnoreCase);

        // Only tx2's delete took effect; the table is not corrupted by the aborted transaction.
        Assert.Equal([5L], await ReadIds(table));
    }

    /// <summary>
    /// With no concurrent writer, a transaction commits at read version + 1, exactly like the
    /// single-shot path. The OCC machinery must not add overhead or a version bump to the quiet case.
    /// </summary>
    [Fact]
    public async Task Transaction_NoConcurrency_CommitsAtNextVersion()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 2, 3)]);
        long baseVersion = table.CurrentSnapshot.Version;

        var tx = table.StartTransaction();
        await tx.DeleteAsync(IdEquals(2));
        long committed = await tx.CommitAsync();

        Assert.Equal(baseVersion + 1, committed);
        Assert.Equal([1L, 3L], await ReadIds(table));
    }

    /// <summary>A committed transaction is single-use.</summary>
    [Fact]
    public async Task Transaction_ReusedAfterCommit_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1)]);

        var tx = table.StartTransaction();
        await tx.DeleteAsync(IdEquals(1));
        await tx.CommitAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await tx.CommitAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await tx.DeleteAsync(IdEquals(1)));
    }

    // ── Staged appends ──

    /// <summary>
    /// Two transactions each stage a blind append. An append has no read dependency, so the second to
    /// commit rebases onto the first (no conflict) and both land — the "two concurrent INSERTs both
    /// land" case the transaction API exists to make safe.
    /// </summary>
    [Fact]
    public async Task TwoTransactions_ConcurrentAppends_BothCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1)]);
        long baseVersion = table.CurrentSnapshot.Version;

        var tx1 = table.StartTransaction();
        var tx2 = table.StartTransaction();

        Assert.Equal(1, await tx2.WriteAsync([Batch(2)]));
        long v2 = await tx2.CommitAsync();

        Assert.Equal(1, await tx1.WriteAsync([Batch(3)]));
        long v1 = await tx1.CommitAsync();

        Assert.Equal(baseVersion + 1, v2);
        Assert.Equal(baseVersion + 2, v1); // collided, no conflict, rebased
        Assert.Equal([1L, 2L, 3L], await ReadIds(table));
    }

    /// <summary>
    /// A staged append rebases over a concurrent DELETE. The append does not depend on which rows exist,
    /// so the delete is not a conflict for it — both land.
    /// </summary>
    [Fact]
    public async Task Transaction_Append_RebasesPastConcurrentDelete()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1)]); // file 1
        await table.WriteAsync([Batch(2)]); // file 2

        var txAppend = table.StartTransaction();
        var txDelete = table.StartTransaction();

        await txDelete.DeleteAsync(IdEquals(1));
        await txDelete.CommitAsync();

        await txAppend.WriteAsync([Batch(3)]);
        await txAppend.CommitAsync(); // rebases past the delete

        Assert.Equal([2L, 3L], await ReadIds(table));
    }

    /// <summary>
    /// One transaction stages a DELETE and an append together; both land in a SINGLE commit (one version
    /// bump), atomically. The read-set is the delete's file, so the fused commit still aborts only on a
    /// concurrent removal of that file.
    /// </summary>
    [Fact]
    public async Task Transaction_DeleteAndAppend_CommitAtomically()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 2)]);
        long baseVersion = table.CurrentSnapshot.Version;

        var tx = table.StartTransaction();
        await tx.DeleteAsync(IdEquals(1));
        await tx.WriteAsync([Batch(3)]);
        long committed = await tx.CommitAsync();

        Assert.Equal(baseVersion + 1, committed); // both changes, one version
        Assert.Equal([2L, 3L], await ReadIds(table));
    }

    // ── Staged updates ──

    /// <summary>
    /// Two transactions update rows living in DIFFERENT files. Neither touches what the other rewrote, so
    /// the second rebases onto the first and both updates land.
    /// </summary>
    [Fact]
    public async Task TwoTransactions_DisjointUpdates_BothCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdValueSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdValueBatch([5], [50])]);  // file 1
        await table.WriteAsync([IdValueBatch([7], [70])]);  // file 2

        var tx1 = table.StartTransaction();
        var tx2 = table.StartTransaction();

        await tx2.UpdateAsync(IdEquals(7), SetValue(700));
        long v2 = await tx2.CommitAsync();

        await tx1.UpdateAsync(IdEquals(5), SetValue(500));
        long v1 = await tx1.CommitAsync();

        Assert.True(v1 > v2);
        var values = await ReadIdValues(table);
        Assert.Equal(500, values[5]);
        Assert.Equal(700, values[7]);
    }

    /// <summary>
    /// Two transactions update rows in the SAME file. The second's read (that file) was rewritten away by
    /// the first — a conflict — so it aborts, leaving the table consistent with only the first update.
    /// </summary>
    [Fact]
    public async Task TwoTransactions_SameFileUpdate_SecondAborts()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdValueSchema, enableDeletionVectors: true);
        await table.WriteAsync([IdValueBatch([5, 7], [50, 70])]); // one file

        var tx1 = table.StartTransaction();
        var tx2 = table.StartTransaction();

        await tx2.UpdateAsync(IdEquals(7), SetValue(700));
        await tx2.CommitAsync();

        await tx1.UpdateAsync(IdEquals(5), SetValue(500));
        await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx1.CommitAsync());

        var values = await ReadIdValues(table);
        Assert.Equal(50, values[5]);   // tx1 aborted
        Assert.Equal(700, values[7]);  // tx2 landed
    }

    private static Apache.Arrow.Schema IdValueSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", Int64Type.Default, false))
        .Build();

    private static RecordBatch IdValueBatch(long[] ids, long[] values) =>
        new(IdValueSchema,
            [new Int64Array.Builder().AppendRange(ids).Build(),
             new Int64Array.Builder().AppendRange(values).Build()],
            ids.Length);

    /// <summary>An updater that rewrites the matched rows' <c>value</c> column to a constant.</summary>
    private static Func<RecordBatch, RecordBatch> SetValue(long newValue) => batch =>
    {
        var id = batch.Column("id");
        var vals = new Int64Array.Builder();
        for (int i = 0; i < batch.Length; i++)
            vals.Append(newValue);
        return new RecordBatch(IdValueSchema, [id, vals.Build()], batch.Length);
    };

    private static async Task<Dictionary<long, long>> ReadIdValues(DeltaTable table)
    {
        var map = new Dictionary<long, long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var id = (Int64Array)batch.Column("id");
            var val = (Int64Array)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                map[id.GetValue(i)!.Value] = val.GetValue(i)!.Value;
        }

        return map;
    }
}
