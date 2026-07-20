// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
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
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
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
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
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
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
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
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
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
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
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
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableDeletionVectors: true);
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

    // ── Temporal + decimal column predicates, end to end through Delta ──

    private static Apache.Arrow.Schema IdAmountSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("amt", new Decimal128Type(12, 2), false))
        .Build();

    private static RecordBatch AmountBatch(long[] ids, decimal[] amounts)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        const int width = 16;
        var bytes = new byte[amounts.Length * width];
        for (int i = 0; i < amounts.Length; i++)
        {
            var unscaled = new BigInteger(amounts[i] * 100m); // scale 2
            var dest = bytes.AsSpan(i * width, width);
            dest.Fill(unscaled.Sign < 0 ? (byte)0xFF : (byte)0x00);
#if NET6_0_OR_GREATER
            unscaled.TryWriteBytes(dest, out _, isUnsigned: false, isBigEndian: false);
#else
            var bb = unscaled.ToByteArray();
            bb.AsSpan(0, Math.Min(bb.Length, width)).CopyTo(dest);
#endif
        }
        var data = new ArrayData(new Decimal128Type(12, 2), amounts.Length, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(bytes)]);
        return new RecordBatch(IdAmountSchema, [idArray, new Decimal128Array(data)], ids.Length);
    }

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

    /// <summary>A DELETE whose predicate is over a decimal column runs end to end: the predicate is
    /// evaluated per row against the Decimal128 column read back from the data file.</summary>
    [Fact]
    public async Task DeleteByDecimalPredicate_EndToEnd()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdAmountSchema, enableDeletionVectors: true);
        await table.WriteAsync([AmountBatch([1, 2, 3], [12.34m, 56.78m, 5.00m])]);

        var (deleted, _) = await table.DeleteAsync(Ex.GreaterThan("amt", 20m));

        Assert.Equal(1, deleted); // only 56.78 > 20
        Assert.Equal([1L, 3L], await ReadIds(table));
    }

    /// <summary>
    /// End-to-end precision for a DECIMAL predicate, which relies on decimal file statistics now being
    /// emitted as JSON numbers. A Serializable transaction deletes <c>amt &gt; 50</c>; a concurrent append
    /// of a small-amount row lands first. Its decimal stats prove it holds nothing above 50, so it does not
    /// match the delete's read predicate — no conflict, the delete rebases and lands. Without decimal stats
    /// the checker would treat the append as an unknown match and wrongly abort, so this guards the fix.
    /// </summary>
    [Fact]
    public async Task Serializable_ConcurrentDecimalAppendDisjoint_Lands()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdAmountSchema, enableDeletionVectors: true);
        await table.WriteAsync([AmountBatch([1], [100.00m])]);

        var tx = table.StartTransaction(IsolationLevel.Serializable);
        long matched = await tx.DeleteAsync(Ex.GreaterThan("amt", 50m));
        Assert.Equal(1, matched);

        // Concurrent append of a small amount — provably at or below 50 by its decimal stats.
        await table.WriteAsync([AmountBatch([2], [10.00m])]);

        await tx.CommitAsync(); // no conflict — the append cannot satisfy amt > 50

        Assert.Equal([2L], await ReadIds(table)); // id 1 (100.00) deleted, id 2 (10.00) survives
    }

    /// <summary>
    /// A high-scale decimal bound must not be rounded away when it is read back for pruning. The one row
    /// holds <c>1.000000000000000000000000000001</c> (decimal(38,30)); its stats max exceeds 1 only in the
    /// 30th fractional digit. If the decoder rounded that bound to System.Decimal precision it would become
    /// 1.0, and an <c>amt &gt; 1</c> filter would then prove the file empty and SKIP it — silently dropping
    /// the row. With exact decoding the file is kept and the row is returned.
    /// </summary>
    [Fact]
    public async Task HighScaleDecimal_ExactBoundNotRoundedAwayByPruning()
    {
        var type = new Decimal128Type(38, 30);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("amt", type, false))
            .Build();
        var unscaled = BigInteger.Pow(10, 30) + 1; // value = 1.000000000000000000000000000001

        var ids = new Int64Array.Builder().Append(1L).Build();
        var bytes = new byte[16];
        var dest = bytes.AsSpan();
        dest.Fill(0);
#if NET6_0_OR_GREATER
        unscaled.TryWriteBytes(dest, out _, isUnsigned: false, isBigEndian: false);
#else
        var bb = unscaled.ToByteArray();
        bb.AsSpan(0, Math.Min(bb.Length, 16)).CopyTo(dest);
#endif
        var data = new ArrayData(type, 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]);
        var batch = new RecordBatch(schema, [ids, new Decimal128Array(data)], 1);

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, schema, enableDeletionVectors: true);
        await table.WriteAsync([batch]);

        var seen = new List<long>();
        await foreach (var b in table.ReadAllAsync(columns: null, Ex.GreaterThan("amt", 1m)))
        {
            var col = (Int64Array)b.Column("id");
            for (int i = 0; i < b.Length; i++) seen.Add(col.GetValue(i)!.Value);
        }

        Assert.Equal([1L], seen); // file kept: the bound is exactly > 1 (would be pruned if rounded to 1.0)
    }

    private static Apache.Arrow.Schema IdDateSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("d", Date32Type.Default, false))
        .Build();

    private static RecordBatch DateBatch(long[] ids, DateTime[] dates)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var b = new Date32Array.Builder();
        foreach (var d in dates)
            b.Append(DateTime.SpecifyKind(d, DateTimeKind.Utc));
        return new RecordBatch(IdDateSchema, [idArray, b.Build()], ids.Length);
    }

    /// <summary>A DELETE whose predicate is over a date column, evaluated as UTC-midnight instants.</summary>
    [Fact]
    public async Task DeleteByDatePredicate_EndToEnd()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdDateSchema, enableDeletionVectors: true);
        await table.WriteAsync([DateBatch(
            [1, 2, 3],
            [new DateTime(2021, 1, 1), new DateTime(2021, 6, 1), new DateTime(2021, 12, 31)])]);

        var (deleted, _) = await table.DeleteAsync(
            Ex.GreaterThanOrEqual("d", new DateTimeOffset(2021, 6, 1, 0, 0, 0, TimeSpan.Zero)));

        Assert.Equal(2, deleted); // 2021-06-01 and 2021-12-31
        Assert.Equal([1L], await ReadIds(table));
    }

    /// <summary>
    /// End-to-end precision for a DATE predicate, which relies on date file statistics now being emitted as
    /// decodable "yyyy-MM-dd" strings. A Serializable transaction deletes <c>d &gt;= June</c>; a concurrent
    /// append of a January row lands first. Its date stats prove it holds nothing at or after June, so it
    /// does not match the delete's read predicate — no conflict, the delete rebases and lands. Were date
    /// stats still emitted as a raw (undecodable) number, the checker would treat the append as an unknown
    /// match and wrongly abort — so this also guards the stats fix.
    /// </summary>
    [Fact]
    public async Task Serializable_ConcurrentDateAppendDisjoint_Lands()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdDateSchema, enableDeletionVectors: true);
        await table.WriteAsync([DateBatch([1], [new DateTime(2021, 6, 15)])]);

        var tx = table.StartTransaction(IsolationLevel.Serializable);
        long matched = await tx.DeleteAsync(
            Ex.GreaterThanOrEqual("d", new DateTimeOffset(2021, 6, 1, 0, 0, 0, TimeSpan.Zero)));
        Assert.Equal(1, matched);

        // Concurrent append of a January row — provably before June by its date stats.
        await table.WriteAsync([DateBatch([2], [new DateTime(2021, 1, 10)])]);

        await tx.CommitAsync(); // no conflict — the append cannot satisfy d >= June

        Assert.Equal([2L], await ReadIds(table)); // id 1 (June) deleted, id 2 (January) survives
    }

    private static Apache.Arrow.Schema IdTsSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), false))
        .Build();

    private static RecordBatch TsBatch(long[] ids, DateTimeOffset[] timestamps)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var b = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        foreach (var t in timestamps)
            b.Append(t);
        return new RecordBatch(IdTsSchema, [idArray, b.Build()], ids.Length);
    }

    /// <summary>
    /// End-to-end precision for a temporal predicate: a Serializable transaction deletes
    /// <c>ts &gt;= June</c>; a concurrent append of a January row lands first. The appended file's
    /// timestamp statistics prove it holds nothing at or after June, so it does not match the delete's
    /// recorded read predicate — no conflict, the delete rebases and lands. This exercises the whole
    /// chain: timestamp row evaluation, timestamp stats decoding, and the checker's predicate match.
    /// </summary>
    [Fact]
    public async Task Serializable_ConcurrentTimestampAppendDisjoint_Lands()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdTsSchema, enableDeletionVectors: true);
        await table.WriteAsync([TsBatch([1], [new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero)])]);

        var tx = table.StartTransaction(IsolationLevel.Serializable);
        long matched = await tx.DeleteAsync(
            Ex.GreaterThanOrEqual("ts", new DateTimeOffset(2024, 6, 1, 0, 0, 0, TimeSpan.Zero)));
        Assert.Equal(1, matched);

        // Concurrent append of a January row — provably before June by its stats.
        await table.WriteAsync([TsBatch([2], [new DateTimeOffset(2024, 1, 10, 0, 0, 0, TimeSpan.Zero)])]);

        await tx.CommitAsync(); // no conflict — the append cannot satisfy ts >= June

        Assert.Equal([2L], await ReadIds(table)); // id 1 (June) deleted, id 2 (January) survives
    }
}
