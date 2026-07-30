// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The isolation level bounds row-level concurrency — but only the half of it that reconciles a concurrent
/// change to the data.
///
/// <para>Reconciling two deletes because their rows are disjoint means silencing a <c>dataChange=true</c>
/// remove of a file this transaction read, which conflicts at BOTH levels
/// (<see cref="IsolationLevel"/>). So the deletion-vector union is a
/// <see cref="IsolationLevel.WriteSerializable"/> behaviour: under
/// <see cref="IsolationLevel.Serializable"/> commit order IS the logical order and the second writer
/// conflicts at FILE granularity however disjoint the rows are.</para>
///
/// <para>Relocating rows across a concurrent COMPACTION is the other half, and it is not bounded the same
/// way: a compaction's removes and adds carry <c>dataChange=false</c> — it rearranges bytes without changing
/// which rows the table contains, and the conflict checker already exempts it from read conflicts at both
/// levels. So the remap survives under Serializable; a copy-on-write UPDATE's <c>dataChange=true</c> rewrite
/// does not.</para>
///
/// <para>The first test comes from cmettler's PR #5, which found the missing gate; the rest pin where the
/// gate belongs.</para>
/// </summary>
public class RowLevelIsolationTests : IDisposable
{
    private readonly string _tempDir;

    public RowLevelIsolationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rlisolation_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            ids.Append(startId + i);
        return new RecordBatch(BuildSchema(), [ids.Build()], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>One file of ids 0..9, deletion vectors and row tracking on — the shape row-level DML needs.</summary>
    private async Task<DeltaTable> CreateAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true, enableRowTracking: true);
        await table.WriteAsync([Batch(0, 10)]);
        return table;
    }

    /// <summary>Positions in the single file, keyed by its ordinal in the path-sorted active set.</summary>
    private static Dictionary<int, IReadOnlyCollection<long>> AtOrdinalZero(params long[] positions)
        => new() { [0] = positions };

    private async Task<int> RowCountAsync()
    {
        await using var table = await OpenAsync();
        int n = 0;
        await foreach (var b in table.ReadAllAsync())
            n += b.Length;
        return n;
    }

    // ── The deletion-vector union: a WriteSerializable behaviour ──

    /// <summary>
    /// Two deletes of DIFFERENT rows in the same file. Under WriteSerializable they reconcile and both land —
    /// the row-level behaviour. Under Serializable the second must conflict instead.
    /// </summary>
    [Fact]
    public async Task DisjointRowDeletes_ReconcileUnderWriteSerializable_ButConflictUnderSerializable()
    {
        // WriteSerializable: both deletes survive.
        await using (var created = await CreateAsync())
        {
            await using var table = await OpenAsync();
            var txn = table.StartTransaction(IsolationLevel.WriteSerializable);
            await txn.StageRowDeletesAsync(AtOrdinalZero(2));

            await using (var other = await OpenAsync())
            {
                await other.DeleteByRowIdsViaVectorsAsync(
                    new[] { TransientRowAddress.Pack(0, 7) });
            }

            await txn.CommitAsync();
            Assert.Equal(8, await RowCountAsync());
        }

        // Serializable, same shape on a fresh table: the second one conflicts and only the first delete landed.
        Directory.Delete(_tempDir, recursive: true);
        Directory.CreateDirectory(_tempDir);
        await using (var created = await CreateAsync())
        {
            await using var table = await OpenAsync();
            var txn = table.StartTransaction(IsolationLevel.Serializable);
            await txn.StageRowDeletesAsync(AtOrdinalZero(2));

            await using (var other = await OpenAsync())
            {
                await other.DeleteByRowIdsViaVectorsAsync(
                    new[] { TransientRowAddress.Pack(0, 7) });
            }

            await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
            Assert.Equal(9, await RowCountAsync());
        }
    }

    /// <summary>
    /// The gate must not turn into "no row-level machinery under Serializable": a concurrent commit that
    /// touched nothing this delete reads still lets it rebase and land. Narrowing the resolved set restores
    /// the ordinary checks for a path; it does not manufacture a conflict.
    /// </summary>
    [Fact]
    public async Task SerializableRowDelete_UnrelatedConcurrentAppend_Rebases()
    {
        await using var created = await CreateAsync();
        await using var table = await OpenAsync();
        var txn = table.StartTransaction(IsolationLevel.Serializable);
        await txn.StageRowDeletesAsync(AtOrdinalZero(2));

        await using (var other = await OpenAsync())
        {
            await other.WriteAsync([Batch(100, 2)]); // different file entirely
        }

        await txn.CommitAsync();
        Assert.Equal(11, await RowCountAsync()); // 10 - 1 deleted + 2 appended
    }

    // ── The remap across a rewrite: bounded by dataChange, not by the level ──

    /// <summary>
    /// A concurrent COMPACTION rewrites the file this delete targets. Its removes and adds carry
    /// <c>dataChange=false</c> — the table's contents are unchanged, only rearranged — so relocating the
    /// deleted rows onto the compacted file admits no interleaving Serializable forbids, and the delete lands
    /// at BOTH levels. Gating all row-level resolution on WriteSerializable would abort this one instead.
    /// </summary>
    [Theory]
    [InlineData(IsolationLevel.WriteSerializable)]
    [InlineData(IsolationLevel.Serializable)]
    public async Task RowDelete_ThroughConcurrentCompaction_Remaps_AtBothLevels(IsolationLevel level)
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true, enableRowTracking: true))
        {
            await setup.WriteAsync([Batch(1, 3)]);
            await setup.WriteAsync([Batch(4, 2)]); // something to compact with
        }

        await using var tableA = await OpenAsync();
        await using var tableB = await OpenAsync();

        var txn = tableB.StartTransaction(level);
        await txn.StageRowDeletesAsync(AtOrdinalZero(0));

        await tableA.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });

        await txn.CommitAsync(); // remapped by stable row id onto the compacted file

        Assert.Equal(4, await RowCountAsync());
    }

    /// <summary>
    /// A copy-on-write UPDATE is the other kind of rewrite: its remove carries <c>dataChange=true</c>, so it
    /// DID change the data this delete read. The remap still lands under WriteSerializable (writes do not
    /// conflict — the updated row is not one we delete), and conflicts under Serializable.
    /// </summary>
    [Theory]
    [InlineData(IsolationLevel.WriteSerializable, false)]
    [InlineData(IsolationLevel.Serializable, true)]
    public async Task RowDelete_ThroughConcurrentUpdateRewrite_ConflictsOnlyUnderSerializable(
        IsolationLevel level, bool expectConflict)
    {
        await using var created = await CreateAsync();
        await using var table = await OpenAsync();
        var txn = table.StartTransaction(level);
        await txn.StageRowDeletesAsync(AtOrdinalZero(2));

        await using (var other = await OpenAsync())
        {
            // Rewrites the file our delete targets, changing a DIFFERENT row.
            await other.UpdateAsync(
                Ex.Equal("id", 7L),
                batch => new RecordBatch(BuildSchema(), [Batch(70, batch.Length).Column(0)], batch.Length));
        }

        if (expectConflict)
        {
            await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
            Assert.Equal(10, await RowCountAsync()); // the update landed; the delete did not
        }
        else
        {
            await txn.CommitAsync();
            Assert.Equal(9, await RowCountAsync());
        }
    }

    // ── The read set stays the read set ──

    private static Apache.Arrow.Schema IdRegionSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, false))
        .Build();

    private static RecordBatch RegionBatch(long[] ids, string[] regions)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var regionBuilder = new StringArray.Builder();
        foreach (string r in regions)
            regionBuilder.Append(r);
        return new RecordBatch(IdRegionSchema, [idArray, regionBuilder.Build()], ids.Length);
    }

    /// <summary>
    /// Staging a row-level delete does not blind the transaction's OTHER read dependencies. Here a second
    /// statement — an analyzable DELETE of region='apac', matching nothing at the pinned version — records a
    /// read predicate, and a concurrent NON-blind-append commit adds a matching row. WriteSerializable exempts
    /// a concurrent BLIND append from that check and nothing else, so this conflicts: were it admitted, a
    /// transaction that says "delete every apac row" would commit ON TOP of an apac row it never saw.
    /// </summary>
    [Fact]
    public async Task StagedRowDelete_DoesNotExemptTheTransactionsReadPredicates()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdRegionSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([RegionBatch([1, 2, 3], ["us", "eu", "us"])]);
        }

        await using var table = await OpenAsync();
        var txn = table.StartTransaction(IsolationLevel.WriteSerializable);
        await txn.StageRowDeletesAsync(AtOrdinalZero(0));
        Assert.Equal(0, await txn.DeleteAsync(Ex.Equal("region", "apac")));

        await using (var other = await OpenAsync())
        {
            // Not a blind append: it removes a file (a row-level delete of its own) as well as adding one.
            var otherTxn = other.StartTransaction();
            await otherTxn.StageRowDeletesAsync(AtOrdinalZero(1));
            await otherTxn.WriteAsync([RegionBatch([9], ["apac"])]);
            await otherTxn.CommitAsync();
        }

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
    }
}
