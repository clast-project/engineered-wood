// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Pinning ONE version across all four steps — plan, read, address, commit. Before this, three of them named
/// a version and the fourth hoped:
///
/// <list type="bullet">
/// <item><b>Commit.</b> <c>StartTransaction()</c> always bases on <c>CurrentSnapshot</c>, which for a
/// transaction spanning several of a host's own statements makes the commit loop's validation VACUOUS — it
/// asks what landed since the latest version, and the answer is nothing.</item>
/// <item><b>Read.</b> A read inside a transaction silently followed <c>CurrentSnapshot</c>, so its rows, and
/// any address minted from them, could come from a version the transaction was not validating against.</item>
/// </list>
/// </summary>
public class PinnedVersionTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _otherDir;

    public PinnedVersionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_pinned_{Guid.NewGuid():N}");
        _otherDir = Path.Combine(Path.GetTempPath(), $"delta_pinned_other_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        Directory.CreateDirectory(_otherDir);
    }

    public void Dispose()
    {
        foreach (string dir in new[] { _tempDir, _otherDir })
        {
            if (Directory.Exists(dir))
            {
                try { Directory.Delete(dir, recursive: true); } catch { }
            }
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

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

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(Fs).AsTask();

    private static async Task<List<long>> IdsOf(IAsyncEnumerable<RecordBatch> batches)
    {
        var ids = new List<long>();
        await foreach (var batch in batches)
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    // ── basing a transaction on a version pinned earlier ──

    [Fact]
    public async Task StartTransactionAsync_BasesOnThePinnedVersion_NotTheCurrentOne()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);
        long pinned = table.CurrentSnapshot.Version;

        // The host's next statement is separated from the first by a concurrent commit.
        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(100, 2)]);

        await using var host = await OpenAsync();
        var txn = await host.StartTransactionAsync(pinned);

        Assert.Equal(pinned, txn.ReadVersion);
        Assert.Equal(pinned, txn.Snapshot.Version);
        Assert.True(host.CurrentSnapshot.Version > pinned); // the table really did move
    }

    /// <summary>
    /// The point of #8, stated as behaviour rather than as a version number: a transaction based on the
    /// CURRENT version cannot see a commit that landed before it started, so a delete/delete race against
    /// that commit goes unnoticed. Based on the pinned version, the same race is adjudicated.
    /// </summary>
    [Fact]
    public async Task PinnedBase_SeesAConcurrentCommitThatACurrentBasedTransactionWouldMiss()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 6)]);
        long pinned = table.CurrentSnapshot.Version;

        // The host planned here. Then a racer deletes row id 3 — the SAME row the host is about to delete.
        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 3L));

        await using var host = await OpenAsync();
        var pinnedSnapshot = await host.GetSnapshotAtVersionAsync(pinned);
        var selection = RowSelection.FromRowAddresses(
            [TransientRowAddress.Pack(0, 2)], pinnedSnapshot);   // position 2 == id 3

        var txn = await host.StartTransactionAsync(pinned);
        await txn.StageRowDeletesAsync(selection);

        // The racer's delete is IN the transaction's validation window, so the same-row conflict is seen.
        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());

        // The counterfactual, measured rather than asserted in the test's name: based on the CURRENT
        // version, the racer's commit is behind the transaction's start, so there is nothing to validate
        // against. The row is already hidden, so the same staging silently reports zero and commits
        // nothing — the host is never told another writer got to its row first.
        await using var currentBased = await OpenAsync();
        var vacuous = currentBased.StartTransaction();
        Assert.Equal(0, await vacuous.StageRowDeletesAsync(selection));
        await vacuous.CommitAsync();   // no conflict, no effect
    }

    [Fact]
    public async Task StartTransaction_TakesASnapshotDirectly()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);
        var pinned = table.CurrentSnapshot;
        await table.WriteAsync([Batch(100, 2)]);

        var txn = table.StartTransaction(pinned, IsolationLevel.Serializable);
        Assert.Equal(pinned.Version, txn.ReadVersion);
        Assert.Equal(IsolationLevel.Serializable, txn.IsolationLevel);
    }

    /// <summary>One transaction's snapshot handed to another — the case the snapshot overload exists for.</summary>
    [Fact]
    public async Task StartTransaction_AcceptsAnotherTransactionsSnapshot()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);

        var first = table.StartTransaction();
        var second = table.StartTransaction(first.Snapshot);
        Assert.Equal(first.ReadVersion, second.ReadVersion);
    }

    [Fact]
    public async Task StartTransactionAsync_VersionAheadOfTheTable_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);

        var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => table.StartTransactionAsync(table.CurrentSnapshot.Version + 5).AsTask());
        Assert.Contains("does not exist yet", ex.Message);
    }

    /// <summary>
    /// A snapshot from a DIFFERENT table has its own active set, so every path, ordinal and row-id range
    /// derived from it would address the wrong file — with nothing looking wrong. Refused by table id, which
    /// lives in every version's metaData and never changes.
    /// </summary>
    [Fact]
    public async Task StartTransaction_SnapshotOfADifferentTable_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);

        await using var other = await DeltaTable.CreateAsync(new LocalTableFileSystem(_otherDir), IdSchema);
        await other.WriteAsync([Batch(1, 2)]);

        var ex = Assert.Throws<ArgumentException>(() => table.StartTransaction(other.CurrentSnapshot));
        Assert.Contains("belongs to Delta table", ex.Message);
    }

    // ── the read half ──

    [Fact]
    public async Task ReadAsync_Snapshot_ReadsThePinnedVersion()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);
        var pinned = table.CurrentSnapshot;
        await table.WriteAsync([Batch(100, 2)]);

        Assert.Equal(
            new long[] { 1, 2 },
            await IdsOf(table.ReadAsync(new DeltaReadOptions { Snapshot = pinned })));
        // ...while an unpinned read of the same table sees both files.
        Assert.Equal(new long[] { 1, 2, 100, 101 }, await IdsOf(table.ReadAsync()));
    }

    /// <summary>
    /// The whole point of the read half: inside a transaction, the addresses a read mints must be resolvable
    /// against the version the transaction validates. Pinned, a concurrent append cannot renumber them.
    /// </summary>
    [Fact]
    public async Task ReadAsync_PinnedToATransaction_MintsAddressesThatStillResolve()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(10, 3)]);

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();

        // A racer appends while the transaction is open — a blind append, so it does not conflict, but it
        // CAN renumber path-sorted ordinals.
        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(0, 1)]);

        var addresses = new List<long>();
        await foreach (var batch in host.ReadAsync(new DeltaReadOptions
        {
            Snapshot = txn.Snapshot,
            Metadata = DeltaRowMetadata.RowAddress,
        }))
        {
            var id = (Int64Array)batch.Column("id");
            var address = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
                if (id.GetValue(i) == 11)
                    addresses.Add(address.GetValue(i)!.Value);
        }

        // The read saw only the pinned version's rows, and its address resolves against that same snapshot.
        var selection = RowSelection.FromRowAddresses(addresses, txn.Snapshot);
        await txn.StageRowDeletesAsync(selection);
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        Assert.Equal(new long[] { 0, 10, 12 }, await IdsOf(check.ReadAllAsync()));
    }

    [Fact]
    public async Task ReadAsync_SnapshotAndAtVersionTogether_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);
        var pinned = table.CurrentSnapshot;

        var options = new DeltaReadOptions { Snapshot = pinned, AtVersion = pinned.Version };

        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in table.ReadAsync(options)) { }
        });
        Assert.Contains("mutually exclusive", ex.Message);

        // GetReadSchema refuses on the same terms, so a host binding a scan finds out before it reads.
        Assert.Throws<ArgumentException>(() => table.GetReadSchema(options));
    }

    [Fact]
    public async Task ReadAsync_SnapshotOfADifferentTable_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);
        await using var other = await DeltaTable.CreateAsync(new LocalTableFileSystem(_otherDir), IdSchema);
        await other.WriteAsync([Batch(1, 2)]);

        var options = new DeltaReadOptions { Snapshot = other.CurrentSnapshot };
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in table.ReadAsync(options)) { }
        });
    }

    /// <summary>
    /// <c>GetReadSchema</c> honours a pinned snapshot because resolving one costs no I/O — so a host binding
    /// a scan against an older version gets THAT version's schema, not the current one's.
    /// </summary>
    [Fact]
    public async Task GetReadSchema_UsesThePinnedSnapshotsSchema()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);
        var pinned = table.CurrentSnapshot;

        await table.AddColumnAsync(new Field("added_later", Int32Type.Default, true));

        Assert.Equal(
            ["id"],
            table.GetReadSchema(new DeltaReadOptions { Snapshot = pinned })
                .FieldsList.Select(f => f.Name));
        Assert.Equal(
            ["id", "added_later"],
            table.GetReadSchema(new DeltaReadOptions()).FieldsList.Select(f => f.Name));
    }

    // ── the fourth step: reading back exactly the selected rows, at the pinned version ──

    /// <summary>
    /// The gap slice 2 left open. A concurrent rewrite replaces the file a selection names, so resolving the
    /// selection against <c>CurrentSnapshot</c> reports it as stale — correctly, but uselessly for a
    /// transaction that is still validating against the version where that path IS active.
    /// </summary>
    [Fact]
    public async Task ReadRowsAsync_ResolveAgainst_ReadsThroughAConcurrentRewrite()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 4)]);

        await using var host = await OpenAsync();
        var pinned = host.CurrentSnapshot;
        var selection = RowSelection.FromRowAddresses(
            [TransientRowAddress.Pack(0, 1), TransientRowAddress.Pack(0, 2)], pinned);

        // A racer rewrites that file away (copy-on-write delete of a different row).
        await using (var racer = await OpenAsync())
        {
            await racer.DeleteRowsAsync(
                RowSelection.FromRowAddresses(
                    [TransientRowAddress.Pack(0, 3)], racer.CurrentSnapshot),
                RowDeleteMode.CopyOnWrite);
        }

        // Unpinned, on a handle that HAS seen the rewrite, the path is gone and the read says so rather than
        // guessing. (CurrentSnapshot is cached per handle, so `host` would still be looking at the old one —
        // which is itself why a read that means to be pinned should say so rather than rely on staleness.)
        await using (var fresh = await OpenAsync())
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var _ in fresh.ReadRowsAsync(selection)) { }
            });
        }

        // Pinned to the version the selection came from, the same rows read back.
        Assert.Equal(
            new long[] { 2, 3 },
            await IdsOf(host.ReadRowsAsync(
                selection, options: new DeltaRowReadOptions { ResolveAgainst = pinned })));
    }

    [Fact]
    public async Task ReadRowsAsync_ResolveAgainstADifferentTable_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 2)]);
        await using var other = await DeltaTable.CreateAsync(new LocalTableFileSystem(_otherDir), IdSchema);
        await other.WriteAsync([Batch(1, 2)]);

        var selection = RowSelection.FromRowAddresses(
            [TransientRowAddress.Pack(0, 0)], table.CurrentSnapshot);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in table.ReadRowsAsync(
                selection,
                options: new DeltaRowReadOptions { ResolveAgainst = other.CurrentSnapshot }))
            {
            }
        });
    }

    // ── all four steps on one version ──

    /// <summary>
    /// The host guide's claim, exercised: plan, read, address and commit all name ONE version, and a
    /// concurrent blind append between the pin and the commit disturbs none of them.
    /// </summary>
    [Fact]
    public async Task PlanReadAddressCommit_AllNameOneVersion()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(10, 4)]);
        long pinnedVersion = table.CurrentSnapshot.Version;

        await using var host = await OpenAsync();
        var txn = await host.StartTransactionAsync(pinnedVersion);

        // 1. plan
        var planned = host.PlanFiles(Ex.GreaterThan("id", 10L), snapshot: txn.Snapshot);
        Assert.NotEmpty(planned);

        // a racer lands a blind append in the middle
        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(0, 2)]);

        // 2. read, pinned to the same version
        var doomed = new List<RecordBatch>();
        await foreach (var batch in host.ReadAsync(new DeltaReadOptions
        {
            Snapshot = txn.Snapshot,
            Metadata = DeltaRowMetadata.Locator,
        }))
        {
            var id = (Int64Array)batch.Column("id");
            var keep = new List<int>();
            for (int i = 0; i < batch.Length; i++)
                if (id.GetValue(i)!.Value >= 12)
                    keep.Add(i);
            if (keep.Count > 0)
                doomed.Add(EngineeredWood.Arrow.ArrowCompute.Take(batch, batch.Schema, keep));
        }

        // 3. address — straight off the locator columns, no coordinate arithmetic
        await txn.StageRowDeletesAsync(RowSelection.FromLocatorColumns(doomed));

        // 4. commit, validated against everything since the pinned version
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        Assert.Equal(new long[] { 0, 1, 10, 11 }, await IdsOf(check.ReadAllAsync()));
    }
}
