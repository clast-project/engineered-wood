// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTable.StartTransaction(Snapshot.Snapshot, IsolationLevel)"/> — a transaction pinned to a
/// snapshot the caller supplies rather than to <see cref="DeltaTable.CurrentSnapshot"/>.
///
/// <para>The motivating caller is a host whose transaction spans several of ITS OWN statements: it pins a
/// version at its first read (which is what its row identifiers and deletion-vector positions were captured
/// against) but cannot hold the table open in between, so by the time it stages anything the current version
/// has moved. What these tests pin is that the pinned base is what the commit loop VALIDATES AGAINST — not
/// merely that the property reports the right number, because a transaction based on the latest version
/// validates against nothing and would silently accept work that conflicts.</para>
/// </summary>
public class PinnedTransactionTests : IDisposable
{
    private readonly string _tempDir;

    public PinnedTransactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_pinnedtxn_{Guid.NewGuid():N}");
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

    /// <summary>One file of ids 0..9, on a table where deletion vectors and row tracking are on — the shape a
    /// row-level delete needs.</summary>
    private async Task<DeltaTable> CreateAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true, enableRowTracking: true);
        await table.WriteAsync([Batch(0, 10)]);
        return table;
    }

    /// <summary>Absolute positions in the single file of the fixture, keyed by its file ordinal (0).</summary>
    private static IReadOnlyDictionary<int, IReadOnlyCollection<long>> Select(params long[] positions) =>
        new Dictionary<int, IReadOnlyCollection<long>> { [0] = positions };

    /// <summary>The same positions as transient row addresses, for the autocommit DV delete.</summary>
    private static IReadOnlyCollection<long> Addresses(params long[] positions) =>
        positions.Select(p => TransientRowAddress.Pack(0, p)).ToList();

    private async Task<List<long>> ReadIdsAsync()
    {
        await using var table = await OpenAsync();
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

    /// <summary>The pinned version is what the transaction reads and reports, even after the table moves on.</summary>
    [Fact]
    public async Task PinnedSnapshot_IsTheBase_NotCurrentSnapshot()
    {
        await using var table = await CreateAsync();
        var pinned = table.CurrentSnapshot;

        // A concurrent writer advances the table through a SECOND handle, as a separate process would.
        await using (var other = await OpenAsync())
        {
            await other.WriteAsync([Batch(100, 3)]);
        }
        await using var reopened = await OpenAsync();
        Assert.True(reopened.CurrentSnapshot.Version > pinned.Version);

        var txn = reopened.StartTransaction(pinned);
        Assert.Equal(pinned.Version, txn.ReadVersion);
        Assert.Equal(pinned.Version, txn.Snapshot.Version);
        // The default overload pins the latest — the distinction the parameter exists for.
        Assert.Equal(reopened.CurrentSnapshot.Version, reopened.StartTransaction().ReadVersion);
    }

    /// <summary>
    /// THE POINT OF THE OVERLOAD: a delete staged against the pinned version REBASES onto the commits that
    /// landed since it. A concurrent delete of DIFFERENT rows in the same file re-unions, so both deletes
    /// survive — which is only reachable because the commit loop knows the transaction started at v1.
    /// </summary>
    [Fact]
    public async Task PinnedTransaction_RebasesOntoCommitsLandedSinceThePin()
    {
        await using var created = await CreateAsync();
        var pinned = created.CurrentSnapshot;

        // Concurrent writer deletes id 0 (position 0) through its own handle.
        await using (var other = await OpenAsync())
        {
            await other.DeleteByRowIdsViaVectorsAsync(Addresses(0));
        }

        // Our transaction was pinned BEFORE that delete and deletes a different row (position 5 = id 5).
        await using var table = await OpenAsync();
        var txn = table.StartTransaction(pinned);
        long deleted = await txn.StageRowDeletesAsync(Select(5));
        Assert.Equal(1, deleted);
        await txn.CommitAsync();

        var ids = await ReadIdsAsync();
        Assert.DoesNotContain(0L, ids);   // the concurrent delete survived the rebase
        Assert.DoesNotContain(5L, ids);   // ours landed on top of it
        Assert.Equal(8, ids.Count);
    }

    /// <summary>
    /// The other half: the SAME row deleted by both is a row-level conflict, raised because the transaction
    /// validates from its pinned version. Based on the latest snapshot instead there would be nothing to
    /// compare against — the position would already be covered and the delete would quietly report 0 rows.
    /// </summary>
    [Fact]
    public async Task PinnedTransaction_SameRowConcurrentlyDeleted_Conflicts()
    {
        await using var created = await CreateAsync();
        var pinned = created.CurrentSnapshot;

        await using (var other = await OpenAsync())
        {
            await other.DeleteByRowIdsViaVectorsAsync(Addresses(3));
        }

        await using var table = await OpenAsync();
        var txn = table.StartTransaction(pinned);
        await txn.StageRowDeletesAsync(Select(3));
        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());

        // The conflict left the table as the concurrent writer had it — id 3 gone, nothing else.
        var ids = await ReadIdsAsync();
        Assert.DoesNotContain(3L, ids);
        Assert.Equal(9, ids.Count);
    }

    [Fact]
    public async Task NullSnapshot_Throws()
    {
        await using var table = await CreateAsync();
        Assert.Throws<ArgumentNullException>(() => table.StartTransaction(null!));
    }
}
