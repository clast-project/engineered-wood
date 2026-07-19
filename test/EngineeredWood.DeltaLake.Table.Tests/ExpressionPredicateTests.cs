// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The analyzable-predicate <c>DeleteAsync</c>/<c>UpdateAsync</c> overloads (and their
/// <see cref="DeltaTransaction"/> equivalents). Two things they add over the functional overloads:
/// files that provably cannot match are skipped, and — because the predicate is recorded as the
/// operation's read-set — a concurrent commit that adds a file matching it is detected as a
/// concurrentAppend conflict, precise to the isolation level. The last is what closes the "predicates
/// are inert for DELETE" limitation.
/// </summary>
public class ExpressionPredicateTests : IDisposable
{
    private readonly string _tempDir;

    public ExpressionPredicateTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_pred_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdRegionSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, false))
        .Build();

    private static RecordBatch Batch(long[] ids, string[] regions)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var regionBuilder = new StringArray.Builder();
        foreach (string r in regions)
            regionBuilder.Append(r);
        return new RecordBatch(IdRegionSchema, [idArray, regionBuilder.Build()], ids.Length);
    }

    /// <summary>An updater rewriting matched rows' <c>region</c> to a constant.</summary>
    private static Func<RecordBatch, RecordBatch> SetRegion(string region) => batch =>
    {
        var id = batch.Column("id");
        var regions = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            regions.Append(region);
        return new RecordBatch(IdRegionSchema, [id, regions.Build()], batch.Length);
    };

    private static async Task<List<(long Id, string Region)>> ReadRows(DeltaTable table)
    {
        var rows = new List<(long, string)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var regions = (StringArray)batch.Column("region");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, regions.GetString(i)));
        }

        rows.Sort();
        return rows;
    }

    // ── Functional correctness ──

    /// <summary>The analyzable DELETE removes exactly the rows the predicate selects.</summary>
    [Fact]
    public async Task DeleteByPredicate_RemovesMatchingRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([Batch([1, 2, 3], ["us", "eu", "us"])]);

        var (deleted, _) = await table.DeleteAsync(Ex.Equal("region", "us"));

        Assert.Equal(2, deleted);
        Assert.Equal([(2L, "eu")], await ReadRows(table));
    }

    /// <summary>The analyzable UPDATE rewrites exactly the rows the predicate selects.</summary>
    [Fact]
    public async Task UpdateByPredicate_UpdatesMatchingRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([Batch([1, 2, 3], ["us", "eu", "us"])]);

        var (updated, _) = await table.UpdateAsync(Ex.Equal("region", "us"), SetRegion("xx"));

        Assert.Equal(2, updated);
        Assert.Equal([(1L, "xx"), (2L, "eu"), (3L, "xx")], await ReadRows(table));
    }

    // ── concurrentAppend precision — the point of the analyzable overload ──

    /// <summary>
    /// Under <see cref="IsolationLevel.Serializable"/> a concurrent blind append of a row that MATCHES
    /// the delete's predicate is a conflict: a strictly-serial order might have required the delete to see
    /// it. The transaction aborts.
    /// </summary>
    [Fact]
    public async Task Serializable_ConcurrentAppendMatchingPredicate_Aborts()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([Batch([1, 2], ["us", "eu"])]);

        var tx = table.StartTransaction(IsolationLevel.Serializable);
        await tx.DeleteAsync(Ex.Equal("region", "us"));

        // A concurrent blind append of another "us" row lands first.
        await table.WriteAsync([Batch([3], ["us"])]);

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx.CommitAsync());

        // The append landed; the aborted delete left the table otherwise unchanged.
        Assert.Equal([(1L, "us"), (2L, "eu"), (3L, "us")], await ReadRows(table));
    }

    /// <summary>
    /// Under the default <see cref="IsolationLevel.WriteSerializable"/> a concurrent BLIND append is
    /// exempt even when it matches the delete's predicate — blind appends are allowed to linearize after.
    /// So the delete rebases and lands; the appended row survives.
    /// </summary>
    [Fact]
    public async Task WriteSerializable_ConcurrentBlindAppendMatchingPredicate_Lands()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([Batch([1, 2], ["us", "eu"])]);

        var tx = table.StartTransaction(); // WriteSerializable (default)
        await tx.DeleteAsync(Ex.Equal("region", "us"));

        await table.WriteAsync([Batch([3], ["us"])]); // concurrent blind append

        await tx.CommitAsync(); // no conflict — rebases

        // id 1 (us) deleted by the transaction; id 3 (us), appended after, survives.
        Assert.Equal([(2L, "eu"), (3L, "us")], await ReadRows(table));
    }

    /// <summary>
    /// The precision the analyzable predicate buys: even under <see cref="IsolationLevel.Serializable"/> a
    /// concurrent append whose file provably CANNOT match the predicate (its stats put it in a different
    /// partition of the value space) is NOT a conflict, so the delete rebases and lands. Without the
    /// predicate this same append would either be ignored (functional DELETE) or, if the read-set were
    /// faked as "everything", wrongly abort.
    /// </summary>
    [Fact]
    public async Task Serializable_ConcurrentAppendDisjointPredicate_Lands()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([Batch([1, 2], ["us", "eu"])]);

        var tx = table.StartTransaction(IsolationLevel.Serializable);
        await tx.DeleteAsync(Ex.Equal("region", "us"));

        // A concurrent append of an "eu" row — its min/max stats prove it holds no "us" row, so it cannot
        // match the delete's predicate.
        await table.WriteAsync([Batch([3], ["eu"])]);

        await tx.CommitAsync(); // no conflict — the append is provably disjoint

        Assert.Equal([(2L, "eu"), (3L, "eu")], await ReadRows(table));
    }

    /// <summary>
    /// Two transactions delete disjoint predicates against the same file-less-overlap table; both land.
    /// Confirms the analyzable path still rebases cleanly when there is genuinely no conflict.
    /// </summary>
    [Fact]
    public async Task TwoTransactions_DisjointDeletePredicates_BothCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([Batch([1], ["us"])]); // file 1
        await table.WriteAsync([Batch([2], ["eu"])]); // file 2

        var tx1 = table.StartTransaction();
        var tx2 = table.StartTransaction();

        await tx2.DeleteAsync(Ex.Equal("region", "eu"));
        await tx2.CommitAsync();

        await tx1.DeleteAsync(Ex.Equal("region", "us"));
        await tx1.CommitAsync(); // rebases past tx2 — different file, no conflict

        Assert.Empty(await ReadRows(table));
    }
}
