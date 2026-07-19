// Copyright (c) Curt Hagenlocher. All rights reserved.
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
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);

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
    /// Two transactions delete rows in the SAME file. The second commit's read (that file) was
    /// invalidated by the first — a delete/delete conflict — so it aborts. The user's canonical example.
    /// </summary>
    [Fact]
    public async Task TwoTransactions_SameFile_SecondAborts()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);

        // A single file holding both rows.
        await table.WriteAsync([Batch(5, 7)]);
        long baseVersion = table.CurrentSnapshot.Version;

        var tx1 = table.StartTransaction();
        var tx2 = table.StartTransaction();

        await tx2.DeleteAsync(IdEquals(7));
        long v2 = await tx2.CommitAsync();
        Assert.Equal(baseVersion + 1, v2);

        await tx1.DeleteAsync(IdEquals(5));
        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await tx1.CommitAsync());
        Assert.Contains("removed", ex.Message, StringComparison.OrdinalIgnoreCase);

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
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
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
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1)]);

        var tx = table.StartTransaction();
        await tx.DeleteAsync(IdEquals(1));
        await tx.CommitAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await tx.CommitAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await tx.DeleteAsync(IdEquals(1)));
    }
}
