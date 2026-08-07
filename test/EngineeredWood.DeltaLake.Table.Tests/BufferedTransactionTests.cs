// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The full BUFFERED (multi-statement) transaction flow: a host holds a whole transaction's changes — a schema
/// ALTER, an eagerly-written append, a deletion-vector DELETE — and commits them as ONE atomic Delta version
/// (the OptimisticTransaction shape Spark/delta-rs use). This fuses the seam halves landed across the milestones:
/// ComputeAddColumn (schema), WriteDataFilesAsync (write-no-commit), ComputeDeletionVectorActionsAsync (the
/// deferred DV DELETE, resolved against the PINNED snapshot), and CommitDataFilesAsync(extraActions:,
/// expectedVersion:) — everything in one commit, first-committer-wins. ReadRowsByRowIdsAsync(atVersion:) is the
/// exact-row read-back an UPDATE post-image is built from.
/// </summary>
public class BufferedTransactionTests : IDisposable
{
    private readonly string _tempDir;

    public BufferedTransactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_buftxn_{Guid.NewGuid():N}");
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
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch BuildBatch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(BuildSchema(), [ids.Build(), values.Build()], count);
    }

    private async Task<DeltaTable> CreateTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var table = await DeltaTable.CreateAsync(fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([BuildBatch(1, 5)]);
        return table;
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    [Fact]
    public async Task FusedCommit_AlterInsertDelete_IsOneAtomicVersion()
    {
        await using var table = await CreateTableAsync();
        var pinned = table.CurrentSnapshot; // v1: ids 1..5, one file

        // "ALTER TABLE ADD COLUMN extra INT" — computed, not committed
        var change = table.ComputeAddColumn(new Field("extra", Int32Type.Default, true));

        // "INSERT" under the pending schema — the file is written NOW, the add is deferred
        var widened = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("extra", Int32Type.Default, true))
            .Build();
        var insertBatch = new RecordBatch(widened,
        [
            new Int64Array.Builder().Append(6).Append(7).Build(),
            new StringArray.Builder().Append("v6").Append("v7").Build(),
            new Int32Array.Builder().Append(60).Append(70).Build(),
        ], 2);
        var files = await table.WriteDataFilesAsync([insertBatch], schemaOverride: change.NewSchema);

        // "DELETE WHERE id = 2" — position 1 of ordinal 0 in the PINNED snapshot
        var (dvActions, rowsDeleted) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = new long[] { 1 } },
            resolveAgainst: pinned);
        Assert.Equal(1, rowsDeleted);

        // COMMIT: everything in ONE version
        var extra = new List<DeltaAction>();
        extra.AddRange(change.Actions);
        extra.AddRange(dvActions);
        long committed = await table.CommitDataFilesAsync(files, DeltaWriteMode.Append,
            extraActions: extra, expectedVersion: pinned.Version, operation: "TRANSACTION");
        Assert.Equal(pinned.Version + 1, committed);

        // read back through a fresh handle: old rows NULL-backfilled, new rows carry values, id 2 gone
        await using var check = await OpenAsync();
        var seen = new Dictionary<long, int?>();
        await foreach (var batch in check.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column(0);
            var extras = (Int32Array)batch.Column(2);
            for (int i = 0; i < batch.Length; i++)
                seen[ids.GetValue(i)!.Value] = extras.IsNull(i) ? null : extras.GetValue(i);
        }
        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 7 }, seen.Keys.OrderBy(k => k).ToArray());
        Assert.Null(seen[1]);
        Assert.Equal(60, seen[6]);
        Assert.Equal(70, seen[7]);

        // the history shows ONE commit with the transaction's operation
        var history = new List<DeltaTable.DeltaHistoryEntry>();
        await foreach (var entry in check.GetHistoryAsync())
            history.Add(entry);
        Assert.Equal(committed, history[^1].Version);
        Assert.Equal("TRANSACTION", history[^1].Operation);
    }

    [Fact]
    public async Task ReadRows_AddressesResolvedAgainstPinnedSnapshot_ExactReadBack()
    {
        await using var table = await CreateTableAsync();
        long pinnedVersion = table.CurrentSnapshot.Version;

        // a concurrent append moves the table (and could shift path-sorted ordinals)
        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(100, 5)]);
        }

        // rowids (ordinal 0, positions 1 and 3 = ids 2 and 4) resolve against the PINNED snapshot: the
        // ordinal becomes a PATH there, and the path is what the read then looks up — so the concurrent
        // append cannot renumber the selection out from under it.
        await using var reader = await OpenAsync();
        var pinned = await reader.GetSnapshotAtVersionAsync(pinnedVersion);
        var selection = RowSelection.FromRowAddresses([1L, 3L], pinned);
        var ids = new List<long>();
        await foreach (var batch in reader.ReadRowsAsync(selection))
        {
            var col = (Int64Array)batch.Column(0);
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        Assert.Equal(new long[] { 2, 4 }, ids);
    }

    private static Func<RecordBatch, BooleanArray> IdEquals(long target) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    private async Task<List<long>> ReadIdsFreshAsync()
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

    /// <summary>The buffered consumer shape: DML positions are captured against a PINNED snapshot; a concurrent
    /// DV delete lands while the "transaction" is open; at COMMIT the actions rebase onto the latest snapshot
    /// (DV union), pass CheckLogicalRebaseAsync, and land in one commit — both deletes compose (disjoint rows).</summary>
    [Fact]
    public async Task BufferedFlow_ComputeThenRebaseThenCommit_ComposesWithConcurrentDelete()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([BuildBatch(1, 10)]); // v1: ids 1..10, one file
        var pinned = table.CurrentSnapshot;

        // positions of id=2 in the pinned snapshot (single file → ordinal 0, position = id - 1)
        var positions = new Dictionary<int, IReadOnlyCollection<long>> { [0] = new long[] { 1 } };
        var (actions, rowsDeleted) = await table.ComputeDeletionVectorActionsAsync(positions, resolveAgainst: pinned);
        Assert.Equal(1, rowsDeleted);

        // a concurrent writer DV-deletes id=7 (position 6) on the SAME file while the transaction is open
        await using (var racer = await OpenAsync())
        {
            await racer.DeleteAsync(IdEquals(7));
        }

        // rebase the pinned-resolved actions onto the latest snapshot, validate, commit
        await using var committer = await OpenAsync();
        var rebased = await committer.RebaseDvDmlActionsAsync(
            actions, positions, pinned, committer.CurrentSnapshot);
        await committer.CheckLogicalRebaseAsync(pinned, rebased, rowLevelDml: true);
        await committer.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), DeltaWriteMode.Append,
            extraActions: rebased, expectedVersion: committer.CurrentSnapshot.Version, operation: "DELETE");

        // both deletes composed: id 2 (this transaction) and id 7 (the racer) are gone
        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 8, 9, 10 }, await ReadIdsFreshAsync());
    }

    /// <summary>
    /// A snapshot-coupled commit whose pin went stale reports it as its OWN condition, not as a generic
    /// concurrent write. The distinction is load-bearing for a host: everything else that collides here
    /// leaves the staged actions valid at a later version, and this does not — the deletion-vector
    /// ordinals and row positions in <c>extraActions</c> resolve against one exact active-file set.
    /// </summary>
    [Fact]
    public async Task StaleExpectedVersion_ReportsStaleTransactionSnapshot_AndDemandsAReplan()
    {
        await using var table = await CreateTableAsync();
        long pinned = table.CurrentSnapshot.Version;

        // Someone else commits, moving the table off the version this commit is pinned to.
        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(100, 3)]);
        }

        await using var stale = await OpenAsync();
        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await stale.CommitDataFilesAsync(
                System.Array.Empty<WrittenDataFile>(), DeltaWriteMode.Append,
                extraActions: [new TransactionId { AppId = "p", Version = 1, LastUpdated = 0 }],
                expectedVersion: pinned, operation: "WRITE"));

        Assert.Equal(DeltaTableErrorCodes.StaleTransactionSnapshot, ex.ErrorCode);
        // NOT Replay: re-attempting these actions at a newer version is precisely what is unsafe.
        Assert.Equal(ConflictRecovery.Replan, ex.Recovery);
    }

    /// <summary>A Delta application transaction (the <c>txn</c> action): an idempotent producer commits its
    /// application-level version ATOMICALLY with the data via a fused commit; the snapshot exposes the
    /// high-water mark.</summary>
    [Fact]
    public async Task AppTransactionAction_RoundTrips()
    {
        await using var table = await CreateTableAsync();
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await table.CommitDataFilesAsync([], DeltaWriteMode.Append,
            extraActions: [new TransactionId { AppId = "producer-1", Version = 42, LastUpdated = nowMs }],
            expectedVersion: table.CurrentSnapshot.Version, operation: "WRITE");

        await using var check = await OpenAsync();
        Assert.True(check.CurrentSnapshot.AppTransactions.TryGetValue("producer-1", out var txn));
        Assert.Equal(42, txn!.Version);
    }

    // ── buffered DML through a concurrent REWRITE: Layer 3 (B) on the buffered surface ──
    //
    // A buffered transaction's DV pairs are computed against its PINNED snapshot. A concurrent compaction or
    // copy-on-write UPDATE makes a touched file VANISH from the latest active set, so there is nothing left to
    // re-union against. RebaseDvDmlActionsAsync now relocates those rows by STABLE ROW ID onto the new files
    // (RemapRowLevelDeletesAsync — the machinery the autocommit path already uses) instead of aborting. A row
    // the rewriter also changed stays a row-level conflict, and a table without row tracking keeps the clean
    // rewrite conflict because there are no stable ids to follow.


    private async Task<DeltaTable> CreateRowTrackedTableAsync(params (long Start, int Count)[] files)
    {
        var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true, enableRowTracking: true);
        foreach (var (start, count) in files)
            await table.WriteAsync([BuildBatch(start, count)]);
        return table;
    }

    /// <summary>
    /// Maps each id to its (file ordinal, absolute in-file position) by decoding the transient rowid, so a test
    /// never has to assume a file's ordinal or that position == id - 1. Data files are GUID-named, so the path
    /// sort that assigns ordinals is uncorrelated with write order.
    /// </summary>
    private static async Task<Dictionary<long, (int Ordinal, long Position)>> LocateRowsAsync(DeltaTable table)
    {
        var located = new Dictionary<long, (int, long)>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
        {
            var ids = (Int64Array)batch.Column("id");
            var rids = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
            {
                long rid = rids.GetValue(i)!.Value;
                located[ids.GetValue(i)!.Value] =
                    (TransientRowAddress.FileOrdinal(rid), TransientRowAddress.Position(rid));
            }
        }
        return located;
    }

    /// <summary>The buffered DELETE composes THROUGH a concurrent compaction: the pinned-resolved DV pair's file
    /// was rewritten away, the rebase remaps the deleted row by stable id onto the compacted file, and the fused
    /// commit lands — no abort, no retry.</summary>
    [Fact]
    public async Task BufferedFlow_DvDml_RemapsAcrossConcurrentCompaction()
    {
        await using var table = await CreateRowTrackedTableAsync((1, 10)); // v1: ids 1..10, one file
        var pinned = table.CurrentSnapshot;
        var at = await LocateRowsAsync(table);

        // "DELETE WHERE id = 2", resolved against the pinned snapshot
        var positions = new Dictionary<int, IReadOnlyCollection<long>>
        {
            [at[2].Ordinal] = new[] { at[2].Position },
        };
        var (dvActions, rowsDeleted) = await table.ComputeDeletionVectorActionsAsync(
            positions, resolveAgainst: pinned);
        Assert.Equal(1, rowsDeleted);

        // the racer appends and COMPACTS — the pinned file is rewritten away
        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(11, 2)]);
            await racer.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        }

        await using var committer = await OpenAsync();
        var rebased = await committer.RebaseDvDmlActionsAsync(
            dvActions, positions, pinned, committer.CurrentSnapshot);
        await committer.CheckLogicalRebaseAsync(pinned, rebased, rowLevelDml: true);
        await committer.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), DeltaWriteMode.Append,
            extraActions: rebased, expectedVersion: committer.CurrentSnapshot.Version, operation: "DELETE");

        // the delete landed on the COMPACTED file: id 2 gone, the racer's rows kept
        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, await ReadIdsFreshAsync());
    }

    /// <summary>The same remap across a copy-on-write UPDATE rather than a compaction: the rewrite carries
    /// <c>dataChange=true</c> (compaction-shaped files are the remap's preferred candidates), and the row this
    /// transaction deletes is a PASS-THROUGH of that rewrite — it keeps its original commit version, so it
    /// remaps rather than conflicting.</summary>
    [Fact]
    public async Task BufferedFlow_DvDml_RemapsAcrossConcurrentCopyOnWriteUpdate()
    {
        await using var table = await CreateRowTrackedTableAsync((1, 10));
        var pinned = table.CurrentSnapshot;
        var at = await LocateRowsAsync(table);

        var positions = new Dictionary<int, IReadOnlyCollection<long>>
        {
            [at[2].Ordinal] = new[] { at[2].Position },
        };
        var (dvActions, _) = await table.ComputeDeletionVectorActionsAsync(positions, resolveAgainst: pinned);

        // the racer UPDATEs a DIFFERENT row, rewriting the whole file copy-on-write
        await using (var racer = await OpenAsync())
        {
            await racer.UpdateAsync(IdEquals(9), batch =>
            {
                var ids = (Int64Array)batch.Column("id");
                var vals = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    vals.Append("updated" + ids.GetValue(i)!.Value);
                return new RecordBatch(BuildSchema(), [ids, vals.Build()], batch.Length);
            });
        }

        await using var committer = await OpenAsync();
        var rebased = await committer.RebaseDvDmlActionsAsync(
            dvActions, positions, pinned, committer.CurrentSnapshot);
        await committer.CheckLogicalRebaseAsync(pinned, rebased, rowLevelDml: true);
        await committer.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), DeltaWriteMode.Append,
            extraActions: rebased, expectedVersion: committer.CurrentSnapshot.Version, operation: "DELETE");

        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 7, 8, 9, 10 }, await ReadIdsFreshAsync());

        // the racer's update survived the remap intact
        await using var check = await OpenAsync();
        await foreach (var batch in check.ReadAllAsync(null, Ex.Equal("id", 9L)))
        {
            var ids = (Int64Array)batch.Column("id");
            var vals = (StringArray)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                if (ids.GetValue(i) == 9L)
                    Assert.Equal("updated9", vals.GetString(i));
        }
    }

    /// <summary>The two reconciliation mechanisms compose in ONE rebase: of the two files this transaction
    /// touches, the survivor re-unions its DV against the concurrent state while the rewritten-away one remaps
    /// by stable id. The split is per-file, not per-transaction.</summary>
    [Fact]
    public async Task BufferedFlow_MixedSurvivorAndRewrittenFile_UnionsOneAndRemapsTheOther()
    {
        await using var table = await CreateRowTrackedTableAsync((1, 5), (11, 5));
        var pinned = table.CurrentSnapshot;
        var at = await LocateRowsAsync(table);
        Assert.NotEqual(at[2].Ordinal, at[12].Ordinal); // genuinely two files

        // "DELETE WHERE id IN (2, 12)" — one row in each file
        var positions = new Dictionary<int, IReadOnlyCollection<long>>
        {
            [at[2].Ordinal] = new[] { at[2].Position },
            [at[12].Ordinal] = new[] { at[12].Position },
        };
        var (dvActions, rowsDeleted) = await table.ComputeDeletionVectorActionsAsync(
            positions, resolveAgainst: pinned);
        Assert.Equal(2, rowsDeleted);

        // the racer rewrites ONLY the file holding 11..15 (updating id 14, which we do not touch) and
        // DV-deletes id 4 from the OTHER file, which survives.
        await using (var racer = await OpenAsync())
        {
            await racer.UpdateAsync(IdEquals(14), batch =>
            {
                var ids = (Int64Array)batch.Column("id");
                var vals = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    vals.Append("updated");
                return new RecordBatch(BuildSchema(), [ids, vals.Build()], batch.Length);
            });
            await racer.DeleteAsync(IdEquals(4));
        }

        await using var committer = await OpenAsync();
        var rebased = await committer.RebaseDvDmlActionsAsync(
            dvActions, positions, pinned, committer.CurrentSnapshot);
        await committer.CheckLogicalRebaseAsync(pinned, rebased, rowLevelDml: true);
        await committer.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), DeltaWriteMode.Append,
            extraActions: rebased, expectedVersion: committer.CurrentSnapshot.Version, operation: "DELETE");

        // both of this transaction's deletes landed (2 via union, 12 via remap) and the racer's delete of 4 held
        Assert.Equal(new long[] { 1, 3, 5, 11, 13, 14, 15 }, await ReadIdsFreshAsync());
    }

    /// <summary>...but a row the concurrent writer ALSO removed stays a row-level conflict: the racer compacts
    /// AND deletes the same row, so the remap cannot find its stable id (a DV-deleted row is filtered from the
    /// scan) and aborts rather than silently dropping the intent.</summary>
    [Fact]
    public async Task BufferedFlow_RemapThroughRewrite_RowConcurrentlyDeleted_Conflicts()
    {
        await using var table = await CreateRowTrackedTableAsync((1, 10));
        var pinned = table.CurrentSnapshot;
        var at = await LocateRowsAsync(table);

        var positions = new Dictionary<int, IReadOnlyCollection<long>>
        {
            [at[2].Ordinal] = new[] { at[2].Position },
        };
        var (dvActions, _) = await table.ComputeDeletionVectorActionsAsync(positions, resolveAgainst: pinned);

        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(11, 2)]);
            await racer.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
            await racer.DeleteAsync(IdEquals(2)); // the same row this transaction deletes
        }

        await using var committer = await OpenAsync();
        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await committer.RebaseDvDmlActionsAsync(dvActions, positions, pinned, committer.CurrentSnapshot));
        Assert.Contains("row-level conflict", ex.Message, StringComparison.OrdinalIgnoreCase);

        // deleted exactly once (by the racer) — the aborted rebase left nothing behind
        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, await ReadIdsFreshAsync());
    }

    /// <summary>A row the concurrent rewrite UPDATED is a conflict too, discriminated by commit version rather
    /// than absence: the id is found in the new file but carries the rewrite's version, not its original.</summary>
    [Fact]
    public async Task BufferedFlow_RemapThroughRewrite_RowConcurrentlyUpdated_Conflicts()
    {
        await using var table = await CreateRowTrackedTableAsync((1, 10));
        var pinned = table.CurrentSnapshot;
        var at = await LocateRowsAsync(table);

        var positions = new Dictionary<int, IReadOnlyCollection<long>>
        {
            [at[2].Ordinal] = new[] { at[2].Position },
        };
        var (dvActions, _) = await table.ComputeDeletionVectorActionsAsync(positions, resolveAgainst: pinned);

        await using (var racer = await OpenAsync())
        {
            await racer.UpdateAsync(IdEquals(2), batch => // the very row this transaction deletes
            {
                var ids = (Int64Array)batch.Column("id");
                var vals = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    vals.Append("racer");
                return new RecordBatch(BuildSchema(), [ids, vals.Build()], batch.Length);
            });
        }

        await using var committer = await OpenAsync();
        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await committer.RebaseDvDmlActionsAsync(dvActions, positions, pinned, committer.CurrentSnapshot));
        Assert.Contains("row-level conflict", ex.Message, StringComparison.OrdinalIgnoreCase);

        Assert.Equal(new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, await ReadIdsFreshAsync());
    }

    /// <summary>Without ROW TRACKING there are no stable ids to follow across the rewrite, so a rewritten-away
    /// touched file keeps the clean pre-existing conflict — and the message says why, rather than looking like
    /// the row-level case.</summary>
    [Fact]
    public async Task BufferedFlow_RemapThroughRewrite_WithoutRowTracking_Conflicts()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), BuildSchema(),
            enableDeletionVectors: true); // no row tracking
        await table.WriteAsync([BuildBatch(1, 10)]);
        var pinned = table.CurrentSnapshot;
        var at = await LocateRowsAsync(table);

        var positions = new Dictionary<int, IReadOnlyCollection<long>>
        {
            [at[2].Ordinal] = new[] { at[2].Position },
        };
        var (dvActions, _) = await table.ComputeDeletionVectorActionsAsync(positions, resolveAgainst: pinned);

        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(11, 2)]);
            await racer.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        }

        await using var committer = await OpenAsync();
        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await committer.RebaseDvDmlActionsAsync(dvActions, positions, pinned, committer.CurrentSnapshot));
        Assert.Contains("row tracking is disabled", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
