// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

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
    public async Task ReadRowsByRowIds_AtVersion_ExactReadBack()
    {
        await using var table = await CreateTableAsync();
        long pinnedVersion = table.CurrentSnapshot.Version;

        // a concurrent append moves the table (and could shift path-sorted ordinals)
        await using (var racer = await OpenAsync())
        {
            await racer.WriteAsync([BuildBatch(100, 5)]);
        }

        // rowids (ordinal 0, positions 1 and 3 = ids 2 and 4) resolve against the PINNED snapshot
        await using var reader = await OpenAsync();
        var ids = new List<long>();
        await foreach (var batch in reader.ReadRowsByRowIdsAsync([1L, 3L], atVersion: pinnedVersion))
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
    /// (DV union), pass CheckLogicalRebaseAsync, and land in one commit — both deletes compose (disjoint rows).
    /// Un-parked from PendingCoverageTests.</summary>
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
}
