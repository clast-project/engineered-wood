// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTable.PlanFiles"/> — the scan-planning surface a host uses when it assembles its own
/// read. The verdict half is the same evaluator the read paths use (covered by
/// <see cref="PredicatePushdownTests"/>); what is pinned HERE is the ordinal contract, because that is what
/// makes a planned file composable with the row-level seam: ordinals are the path-sorted position in the
/// FULL active set, they agree with the transient-rowid encoding, they survive pruning ungapped-renumbered,
/// and they address <see cref="DeltaTable.ComputeDeletionVectorActionsAsync"/>. A renumbering bug here is
/// silent — a position fed back under the wrong ordinal deletes the wrong file's row.
/// </summary>
public class PlanFilesTests : IDisposable
{
    private readonly string _tempDir;

    public PlanFilesTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_plan_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private const int RowIdPositionBits = 40;

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

    private LocalTableFileSystem Fs => new(_tempDir);

    /// <summary>
    /// Three files with disjoint id ranges. Data files are named by GUID, so the PATH sort that assigns
    /// ordinals is uncorrelated with write order — a test must never assume "written third == ordinal 2".
    /// </summary>
    private static async Task<DeltaTable> ThreeFileTableAsync(
        LocalTableFileSystem fs, bool enableDeletionVectors = false)
    {
        var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: enableDeletionVectors);
        for (int i = 0; i < 3; i++)
            await table.WriteAsync([Batch(i * 100, 10)]); // 0..9, 100..109, 200..209
        return table;
    }

    /// <summary>
    /// The ids living in each file, keyed by the file's ordinal — read out of the transient rowid encoding
    /// rather than assumed, so the ordinal→content mapping under test comes from the read path itself.
    /// </summary>
    private static async Task<Dictionary<int, List<long>>> IdsByOrdinalAsync(DeltaTable table)
    {
        var byOrdinal = new Dictionary<int, List<long>>();
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var ids = (Int64Array)batch.Column("id");
            var rids = (Int64Array)batch.Column("_metadata.row_id");
            for (int i = 0; i < batch.Length; i++)
            {
                int ordinal = (int)(rids.GetValue(i)!.Value >> RowIdPositionBits);
                if (!byOrdinal.TryGetValue(ordinal, out var list))
                    byOrdinal[ordinal] = list = [];
                list.Add(ids.GetValue(i)!.Value);
            }
        }
        return byOrdinal;
    }

    [Fact]
    public async Task NoFilter_ReturnsEveryActiveFile_OrdinalsAscendingFromZero()
    {
        await using var table = await ThreeFileTableAsync(Fs);

        var planned = table.PlanFiles();

        Assert.Equal(3, planned.Count);
        Assert.Equal(new[] { 0, 1, 2 }, planned.Select(p => p.FileOrdinal).ToArray());
        // A null filter is how a caller enumerates the addressing domain — every active file must be there.
        Assert.Equal(
            table.CurrentSnapshot.ActiveFiles.Values.Select(f => f.Path).OrderBy(p => p, StringComparer.Ordinal),
            planned.Select(p => p.File.Path));
    }

    [Fact]
    public async Task Ordinals_ArePathSorted()
    {
        await using var table = await ThreeFileTableAsync(Fs);

        var paths = table.PlanFiles().Select(p => p.File.Path).ToList();

        // string.CompareOrdinal, matching OrderedActiveFiles — not culture-aware, not StringComparer default.
        var expected = paths.OrderBy(p => p, StringComparer.Ordinal).ToList();
        Assert.Equal(expected, paths);
    }

    [Fact]
    public async Task Ordinals_AgreeWithTheTransientRowIdEncoding()
    {
        await using var table = await ThreeFileTableAsync(Fs);

        var planned = table.PlanFiles();
        var idsByOrdinal = await IdsByOrdinalAsync(table);

        // The ordinal PlanFiles reports for a file must be the ordinal the read path packs into that file's
        // rowids — otherwise a host correlating planned files with rowids silently crosses files. Tie the
        // two together through the file's OWN statistics: the min id in the planned file's stats must be the
        // min id the rowid encoding attributes to that ordinal.
        Assert.Equal(3, idsByOrdinal.Count);
        foreach (var p in planned)
        {
            Assert.True(idsByOrdinal.ContainsKey(p.FileOrdinal),
                $"ordinal {p.FileOrdinal} has no rows in the rowid encoding");
            long statsMin = System.Text.Json.JsonDocument.Parse(p.File.Stats!)
                .RootElement.GetProperty("minValues").GetProperty("id").GetInt64();
            Assert.Equal(idsByOrdinal[p.FileOrdinal].Min(), statsMin);
        }

        // Each file holds exactly one of the three ranges, and each range appears once.
        var ranges = idsByOrdinal.Values.Select(v => v.Min()).OrderBy(v => v).ToArray();
        Assert.Equal([0L, 100L, 200L], ranges);
    }

    [Fact]
    public async Task PrunedFile_StillConsumesItsOrdinal()
    {
        await using var table = await ThreeFileTableAsync(Fs);

        // Target whichever file happens to sort LAST, so a renumbering bug is visible: the survivor's
        // ordinal must stay 2, not collapse to 0.
        var idsByOrdinal = await IdsByOrdinalAsync(table);
        long targetId = idsByOrdinal[2][0];

        var planned = table.PlanFiles(Ex.Equal("id", targetId));

        var survivor = Assert.Single(planned);
        Assert.Equal(2, survivor.FileOrdinal);
    }

    [Fact]
    public async Task PruningTheFirstFile_LeavesTheSequenceGapped()
    {
        await using var table = await ThreeFileTableAsync(Fs);
        var idsByOrdinal = await IdsByOrdinalAsync(table);

        // Everything except ordinal 0's range: the result must be [1, 2], never [0, 1].
        var keep = new[] { idsByOrdinal[1][0], idsByOrdinal[2][0] };
        var planned = table.PlanFiles(Ex.In("id", keep[0], keep[1]));

        Assert.Equal(new[] { 1, 2 }, planned.Select(p => p.FileOrdinal).ToArray());
    }

    [Fact]
    public async Task PlannedOrdinal_AddressesComputeDeletionVectorActions()
    {
        // The contract that matters end-to-end: plan → feed the ordinal into the DV seam → the row that
        // disappears is the one the plan pointed at.
        await using var table = await ThreeFileTableAsync(Fs, enableDeletionVectors: true);
        var idsByOrdinal = await IdsByOrdinalAsync(table);

        long targetId = idsByOrdinal[2][0];
        var planned = table.PlanFiles(Ex.Equal("id", targetId));
        var survivor = Assert.Single(planned);

        // Delete the target row by its absolute in-file position, keyed by the PLANNED ordinal.
        int position = idsByOrdinal[survivor.FileOrdinal].IndexOf(targetId);
        var (dvActions, rowsDeleted) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [survivor.FileOrdinal] = new long[] { position } });
        Assert.Equal(1, rowsDeleted);

        await table.CommitDataFilesAsync([], DeltaWriteMode.Append, extraActions: dvActions, operation: "DELETE");

        await using var check = await DeltaTable.OpenAsync(Fs);
        var remaining = new List<long>();
        await foreach (var batch in check.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                remaining.Add(ids.GetValue(i)!.Value);
        }

        Assert.Equal(29, remaining.Count);
        Assert.DoesNotContain(targetId, remaining);
    }

    [Fact]
    public async Task Snapshot_PlansAgainstThePinnedVersion_NotCurrent()
    {
        await using var table = await ThreeFileTableAsync(Fs);
        var pinned = table.CurrentSnapshot;

        await table.WriteAsync([Batch(300, 10)]); // a fourth file lands

        Assert.Equal(4, table.PlanFiles().Count);
        // Planning against the pinned snapshot must not see it — this is what lets a rewrite list and commit
        // against one version without a concurrent writer manufacturing a conflict.
        var atPinned = table.PlanFiles(filter: null, snapshot: pinned);
        Assert.Equal(3, atPinned.Count);
        Assert.Equal(new[] { 0, 1, 2 }, atPinned.Select(p => p.FileOrdinal).ToArray());
        Assert.DoesNotContain(atPinned, p => p.File.Path.Contains("300", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DeletionVector_IsReportedNotResolved()
    {
        await using var table = await ThreeFileTableAsync(Fs, enableDeletionVectors: true);
        var idsByOrdinal = await IdsByOrdinalAsync(table);

        long targetId = idsByOrdinal[0][0];
        await table.DeleteAsync(Ex.Equal("id", targetId));

        var planned = table.PlanFiles();
        var withDv = planned.Where(p => p.File.DeletionVector is not null).ToList();

        // PlanFiles hands the DV back untouched — it does not filter rows and does not drop the file.
        var dvFile = Assert.Single(withDv);
        Assert.NotNull(dvFile.File.DeletionVector);
        Assert.Equal(3, planned.Count);
    }

    [Fact]
    public async Task SchemaOverride_PendingRename_RestoresPruneQuality()
    {
        var fs = Fs;
        await using var table = await DeltaTable.CreateAsync(
            fs, IdSchema, columnMappingMode: ColumnMappingMode.Name);
        for (int i = 0; i < 3; i++)
            await table.WriteAsync([Batch(i * 100, 10)]);

        // "ALTER TABLE RENAME COLUMN id TO key" — computed, NOT committed.
        var pendingRename = table.ComputeRenameColumn("id", "key");

        // Against the snapshot's schema the new name resolves to nothing: Unknown keeps every file. Correct,
        // but useless — this is the prune quality the override exists to restore.
        Assert.Equal(3, table.PlanFiles(Ex.Equal("key", 105L)).Count);

        // With the pending schema the name resolves through the field's unchanged PHYSICAL name and prunes.
        var planned = table.PlanFiles(Ex.Equal("key", 105L), schemaOverride: pendingRename.NewSchema);
        var survivor = Assert.Single(planned);

        var idsByOrdinal = await IdsByOrdinalAsync(table);
        Assert.Contains(105L, idsByOrdinal[survivor.FileOrdinal]);
    }

    [Fact]
    public async Task UnknownColumn_KeepsEveryFile()
    {
        await using var table = await ThreeFileTableAsync(Fs);

        // An unresolvable reference evaluates Unknown — pruning must never guess, so nothing is dropped.
        Assert.Equal(3, table.PlanFiles(Ex.Equal("no_such_column", 1L)).Count);
    }

    [Fact]
    public async Task AfterDispose_Throws()
    {
        var table = await ThreeFileTableAsync(Fs);
        await table.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => table.PlanFiles());
    }
}
