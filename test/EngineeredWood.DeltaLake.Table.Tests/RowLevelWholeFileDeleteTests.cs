// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A deletion-vector delete that leaves a file no live row removes the file rather than re-adding it under a vector
/// that hides every row (#737), as Spark's DELETE and this library's predicate DELETE, UPDATE and MERGE do. The commit
/// carries the <c>remove</c> alone and writes no vector. Covered on every path that builds such a re-add: the
/// row-level delete (auto-committed and staged), the host's deferred actions, and each rebase, where a union with a
/// concurrent delete, or a remap onto a compacted file, can hide every row when neither delete alone did.
/// </summary>
public class RowLevelWholeFileDeleteTests : IDisposable
{
    private readonly string _tempDir;

    public RowLevelWholeFileDeleteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rowlevel_whole_{Guid.NewGuid():N}");
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

    private static RecordBatch Ids(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private static Func<RecordBatch, BooleanArray> IdEquals(long target) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    private LocalTableFileSystem Fs => new(_tempDir);

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(Fs).AsTask();

    /// <summary>A deletion-vector table whose every vector is written to a file of its own, so a test can count them.</summary>
    private async Task<DeltaTable> CreateAsync(bool cdf = false, bool rowTracking = false)
    {
        var table = await DeltaTable.CreateAsync(
            Fs, IdSchema, enableDeletionVectors: true, enableRowTracking: rowTracking,
            configuration: cdf ? new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" } : null);
        table.DeletionVectorInlineThreshold = 0;
        return table;
    }

    /// <summary>The one active file holding <paramref name="id"/>'s row, from a scan.</summary>
    private static async Task<string> FileOfAsync(DeltaTable table, long id)
    {
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
        {
            var ids = (Int64Array)batch.Column("id");
            var paths = (StringArray)batch.Column(RowSelection.DefaultMetadataPrefix + RowSelection.FilePathColumnSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                if (ids.GetValue(i) == id)
                    return paths.GetString(i);
            }
        }
        throw new InvalidOperationException($"No row {id}.");
    }

    private static RowSelection At(string path, params long[] positions) =>
        RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal)
        {
            [path] = positions,
        });

    private string[] VectorFiles() => Directory.GetFiles(_tempDir, "*.bin", SearchOption.AllDirectories);

    private async Task<List<long>> IdsAsync()
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

    private Task<IReadOnlyList<DeltaAction>> CommitAsync(long version) => new TransactionLog(Fs).ReadCommitAsync(version).AsTask();

    /// <summary>The commit's actions on <paramref name="path"/>: a <c>remove</c> and no <c>add</c>.</summary>
    private static RemoveFile AssertRemovedWhole(IReadOnlyList<DeltaAction> actions, string path)
    {
        Assert.DoesNotContain(actions, a => a is AddFile add && add.Path == path);
        return Assert.Single(actions.OfType<RemoveFile>(), r => r.Path == path);
    }

    private static async Task<IReadOnlyDictionary<string, string>> MetricsAsync(DeltaTable table, long version)
    {
        await foreach (var entry in table.GetHistoryAsync())
        {
            if (entry.Version == version)
                return entry.OperationMetrics!;
        }
        throw new InvalidOperationException($"No version {version}.");
    }

    // ── The row-level delete itself ──

    /// <summary>
    /// The #737 repro: one row by position, then the rest of the file. The second commit is the <c>remove</c> alone,
    /// carrying the first delete's vector, writes no vector of its own, and reports the counts Spark reports for the
    /// same commit (measured, delta-spark 4.0.0): one file removed, one vector removed, none added.
    /// </summary>
    [Fact]
    public async Task DeleteRows_TheLastLiveRows_RemovesTheFile()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Ids(1, 2, 3)]);
        string path = await FileOfAsync(table, 1);

        await table.DeleteRowsAsync(At(path, 1));
        Assert.Single(VectorFiles());

        var (deleted, version) = await table.DeleteRowsAsync(At(path, 0, 2));

        Assert.Equal(2, deleted);
        var remove = AssertRemovedWhole(await CommitAsync(version), path);
        Assert.NotNull(remove.DeletionVector); // the first delete's, which the file carried
        Assert.Single(VectorFiles()); // and no new one
        Assert.Empty(table.CurrentSnapshot.ActiveFiles);
        Assert.Empty(await IdsAsync());

        var metrics = await MetricsAsync(table, version);
        Assert.Equal("2", metrics["numDeletedRows"]);
        Assert.Equal("1", metrics["numRemovedFiles"]);
        Assert.Equal("0", metrics["numAddedFiles"]);
        Assert.Equal("1", metrics["numDeletionVectorsRemoved"]);
        Assert.Equal("0", metrics["numDeletionVectorsAdded"]);
        Assert.Equal("0", metrics["numDeletionVectorsUpdated"]);
    }

    /// <summary>Every row of a file with no vector yet: the plain <c>remove</c>, and no vector written at all.</summary>
    [Fact]
    public async Task DeleteRows_EveryRowAtOnce_RemovesTheFile()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Ids(1, 2, 3)]);
        await table.WriteAsync([Ids(4, 5)]);
        string path = await FileOfAsync(table, 1);

        var (deleted, version) = await table.DeleteRowsAsync(At(path, 0, 1, 2));

        Assert.Equal(3, deleted);
        Assert.Null(AssertRemovedWhole(await CommitAsync(version), path).DeletionVector);
        Assert.Empty(VectorFiles());
        Assert.Equal(new long[] { 4, 5 }, await IdsAsync());
        var metrics = await MetricsAsync(table, version);
        Assert.Equal("1", metrics["numRemovedFiles"]);
        Assert.Equal("0", metrics["numDeletionVectorsRemoved"]);
        Assert.Equal("0", metrics["numDeletionVectorsAdded"]);
    }

    /// <summary>A file the delete leaves rows in is still re-added under its vector: only a file with no live row goes.</summary>
    [Fact]
    public async Task DeleteRows_RowsLeft_StillReAddsUnderAVector()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Ids(1, 2, 3)]);
        string path = await FileOfAsync(table, 1);

        var (_, version) = await table.DeleteRowsAsync(At(path, 0, 2));

        var actions = await CommitAsync(version);
        Assert.Single(actions.OfType<RemoveFile>(), r => r.Path == path);
        Assert.NotNull(Assert.Single(actions.OfType<AddFile>(), a => a.Path == path).DeletionVector);
        Assert.Equal(new long[] { 2 }, await IdsAsync());
    }

    /// <summary>The staged row-level delete takes a file with no live row whole too.</summary>
    [Fact]
    public async Task StageRowDeletes_TheLastLiveRows_RemovesTheFile()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Ids(1, 2, 3)]);
        string path = await FileOfAsync(table, 1);
        await table.DeleteRowsAsync(At(path, 1));

        await using var transaction = table.StartTransaction();
        Assert.Equal(2, await transaction.StageRowDeletesAsync(At(path, 0, 2)));
        long version = await transaction.CommitAsync();

        AssertRemovedWhole(await CommitAsync(version), path);
        Assert.Single(VectorFiles());
        Assert.Empty(await IdsAsync());
    }

    /// <summary>The host's deferred actions for a file with no live row are the <c>remove</c> alone.</summary>
    [Fact]
    public async Task ComputeDeletionVectorActions_EveryRow_IsTheRemoveAlone()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Ids(1, 2, 3)]);

        var (actions, rows) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = [0L, 1L, 2L] });

        Assert.Equal(3, rows);
        Assert.IsType<RemoveFile>(Assert.Single(actions));
        Assert.Empty(VectorFiles());
    }

    // ── The change data feed ──

    /// <summary>
    /// The feed of a delete that takes a file whole is its deleted rows, from the change file the delete writes,
    /// in both steps of the repro, and not the rows the earlier vector already hid.
    /// </summary>
    [Fact]
    public async Task DeleteRows_TheLastLiveRows_FeedIsTheDeletedRows()
    {
        await using var table = await CreateAsync(cdf: true);
        await table.WriteAsync([Ids(1, 2, 3)]);
        string path = await FileOfAsync(table, 1);
        var (_, first) = await table.DeleteRowsAsync(At(path, 1));
        var (_, second) = await table.DeleteRowsAsync(At(path, 0, 2));

        Assert.Equal(
            new (long, string, long)[] { (first, "delete", 2), (second, "delete", 1), (second, "delete", 3) },
            await ChangesAsync(table, first, second));
    }

    /// <summary>
    /// A transaction that writes a change file for some other operation fills in the rows of its staged vector deletes
    /// (#530). A staged delete that took its file whole is filled in from the file it removed, as one that re-added it.
    /// </summary>
    [Fact]
    public async Task StagedWholeFileDelete_BesideAChangeFile_IsFilledIn()
    {
        await using var table = await CreateAsync(cdf: true);
        await table.WriteAsync([Ids(1, 2, 3)]);
        await table.WriteAsync([Ids(11, 12, 13)]);
        string whole = await FileOfAsync(table, 1);
        string rewritten = await FileOfAsync(table, 11);

        await using var transaction = table.StartTransaction();
        await transaction.StageRowDeletesAsync(At(rewritten, 1), RowDeleteMode.CopyOnWrite); // writes a change file
        await transaction.StageRowDeletesAsync(At(whole, 0, 1, 2));
        long version = await transaction.CommitAsync();

        AssertRemovedWhole(await CommitAsync(version), whole);
        Assert.Equal(
            new (long, string, long)[] { (version, "delete", 1), (version, "delete", 2), (version, "delete", 3), (version, "delete", 12) },
            await ChangesAsync(table, version, version));
    }

    private static async Task<List<(long Version, string Type, long Id)>> ChangesAsync(DeltaTable table, long from, long to)
    {
        var changes = new List<(long, string, long)>();
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = from, EndVersion = to }))
        {
            var ids = (Int64Array)batch.Column("id");
            var types = (StringArray)batch.Column("_change_type");
            var versions = (Int64Array)batch.Column("_commit_version");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((versions.GetValue(i)!.Value, types.GetString(i), ids.GetValue(i)!.Value));
        }
        return [.. changes.OrderBy(c => c.Item1).ThenBy(c => c.Item2, StringComparer.Ordinal).ThenBy(c => c.Item3)];
    }

    // ── Rebases ──

    /// <summary>
    /// Two row-level deletes of disjoint rows of one file that between them take every row: the loser rebases onto
    /// the winner's vector, and the union leaves no row, so it commits the <c>remove</c> alone. The vector of its
    /// losing attempt is collected, so only the winner's stays on disk.
    /// </summary>
    [Fact]
    public async Task RowLevelRebase_UnionHidesEveryRow_RemovesTheFile()
    {
        await using (var setup = await CreateAsync())
            await setup.WriteAsync([Ids(1, 2, 3)]);
        await using var winner = await OpenAsync();
        await using var loser = await OpenAsync();
        winner.DeletionVectorInlineThreshold = 0;
        loser.DeletionVectorInlineThreshold = 0;
        string path = await FileOfAsync(loser, 1);
        var lost = At(path, 0, 2);

        await winner.DeleteRowsAsync(At(path, 1), rowLevelRetry: true);
        var (deleted, version) = await loser.DeleteRowsAsync(lost, rowLevelRetry: true);

        Assert.Equal(2, deleted);
        var remove = AssertRemovedWhole(await CommitAsync(version), path);
        Assert.NotNull(remove.DeletionVector); // the winner's, which the file now carries
        Assert.Single(VectorFiles());
        Assert.Empty(await IdsAsync());
    }

    /// <summary>
    /// The predicate DELETE rebases the same way: a transaction deletes one of a file's two rows, a concurrent delete
    /// takes the other, and the transaction's union leaves no row, so it commits the <c>remove</c> alone.
    /// </summary>
    [Fact]
    public async Task PredicateDeleteRebase_UnionHidesEveryRow_RemovesTheFile()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Ids(5, 7)]);
        string path = await FileOfAsync(table, 5);

        await using var transaction = table.StartTransaction();
        await transaction.DeleteAsync(IdEquals(5));
        await table.DeleteAsync(IdEquals(7));
        long version = await transaction.CommitAsync();

        AssertRemovedWhole(await CommitAsync(version), path);
        Assert.Empty(await IdsAsync());
    }

    /// <summary>
    /// A row-level delete staged to take a file whole, rebased past a concurrent delete of DIFFERENT rows of
    /// another file: still the <c>remove</c> alone.
    /// </summary>
    [Fact]
    public async Task RowLevelRebase_WholeFileDelete_PastAnUnrelatedDelete_StaysWhole()
    {
        await using (var setup = await CreateAsync())
        {
            await setup.WriteAsync([Ids(1, 2, 3)]);
            await setup.WriteAsync([Ids(4, 5)]);
        }
        await using var winner = await OpenAsync();
        await using var loser = await OpenAsync();
        string path = await FileOfAsync(loser, 1);

        await winner.DeleteAsync(IdEquals(5));
        var (deleted, version) = await loser.DeleteRowsAsync(At(path, 0, 1, 2), rowLevelRetry: true);

        Assert.Equal(3, deleted);
        AssertRemovedWhole(await CommitAsync(version), path);
        Assert.Equal(new long[] { 4 }, await IdsAsync());
    }

    /// <summary>
    /// A file a row-level delete took whole that a concurrent delete ALSO deleted a row of is a row-level conflict, as
    /// it was when the delete re-added the file: the edit is kept for a whole-file remove so that the rebase checks it.
    /// </summary>
    [Fact]
    public async Task RowLevelRebase_WholeFileDelete_OverlappingADelete_Conflicts()
    {
        await using (var setup = await CreateAsync())
            await setup.WriteAsync([Ids(1, 2, 3)]);
        await using var winner = await OpenAsync();
        await using var loser = await OpenAsync();
        string path = await FileOfAsync(loser, 1);

        await winner.DeleteAsync(IdEquals(2));
        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await loser.DeleteRowsAsync(At(path, 0, 1, 2), rowLevelRetry: true));

        Assert.Equal(DeltaTableErrorCodes.RowLevelConflict, ex.ErrorCode);
        Assert.Equal(new long[] { 1, 3 }, await IdsAsync());
    }

    /// <summary>
    /// A row-level delete of every row of two files, rebased past a concurrent compaction of them into one: its rows
    /// are remapped by stable id onto the compacted file, every row of which they are, so that file goes whole.
    /// </summary>
    [Fact]
    public async Task RemapOntoACompactedFile_EveryRow_RemovesTheFile()
    {
        await using (var setup = await CreateAsync(rowTracking: true))
        {
            await setup.WriteAsync([Ids(1, 2, 3)]);
            await setup.WriteAsync([Ids(4, 5)]);
        }
        await using var compactor = await OpenAsync();
        await using var loser = await OpenAsync();
        var everything = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal)
        {
            [await FileOfAsync(loser, 1)] = [0L, 1L, 2L],
            [await FileOfAsync(loser, 4)] = [0L, 1L],
        });

        await compactor.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        string compacted = Assert.Single(compactor.CurrentSnapshot.ActiveFiles.Values).Path;
        var (deleted, version) = await loser.DeleteRowsAsync(everything, rowLevelRetry: true);

        Assert.Equal(5, deleted);
        AssertRemovedWhole(await CommitAsync(version), compacted);
        Assert.Empty(await IdsAsync());
    }

    // ── The host's rebase (RebaseDvDmlActionsAsync) ──

    /// <summary>
    /// The host's buffered delete took a file whole; a concurrent delete of another file lands; the rebase keeps the
    /// <c>remove</c> alone, passes the logical check, and commits.
    /// </summary>
    [Fact]
    public async Task HostRebase_WholeFileRemove_PastAnUnrelatedDelete_StaysWhole()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Ids(1, 2, 3)]);
        await table.WriteAsync([Ids(4, 5)]);
        var pinned = table.CurrentSnapshot;
        string path = await FileOfAsync(table, 1);
        int ordinal = OrdinalOf(pinned, path);
        var positions = new Dictionary<int, IReadOnlyCollection<long>> { [ordinal] = [0L, 1L, 2L] };
        var (actions, _) = await table.ComputeDeletionVectorActionsAsync(positions, resolveAgainst: pinned);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(IdEquals(5));

        await using var committer = await OpenAsync();
        var rebased = await committer.RebaseDvDmlActionsAsync(actions, positions, pinned, committer.CurrentSnapshot);
        AssertRemovedWhole(rebased, path);
        await committer.CheckLogicalRebaseAsync(pinned, rebased, rowLevelDml: true);
        await committer.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), DeltaWriteMode.Append,
            extraActions: rebased, expectedVersion: committer.CurrentSnapshot.Version, operation: "DELETE");

        Assert.Equal(new long[] { 4 }, await IdsAsync());
    }

    /// <summary>
    /// The host's buffered delete took a file whole, and a concurrent delete took one of the SAME rows. The staged
    /// actions hold no <c>add</c> for the rebase to check, so the <c>remove</c> is checked itself: a row-level
    /// conflict, not a silent second delete of the row.
    /// </summary>
    [Fact]
    public async Task HostRebase_WholeFileRemove_OverlappingADelete_Conflicts()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Ids(1, 2, 3)]);
        var pinned = table.CurrentSnapshot;
        var positions = new Dictionary<int, IReadOnlyCollection<long>> { [0] = [0L, 1L, 2L] };
        var (actions, _) = await table.ComputeDeletionVectorActionsAsync(positions, resolveAgainst: pinned);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(IdEquals(2));

        await using var committer = await OpenAsync();
        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await committer.RebaseDvDmlActionsAsync(actions, positions, pinned, committer.CurrentSnapshot));
        Assert.Equal(DeltaTableErrorCodes.RowLevelConflict, ex.ErrorCode);
    }

    /// <summary>
    /// The host's buffered delete left a row; a concurrent delete took it; the rebase's union leaves no row, so the
    /// staged <c>add</c> is dropped and the file goes whole.
    /// </summary>
    [Fact]
    public async Task HostRebase_UnionHidesEveryRow_DropsTheReAdd()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Ids(1, 2, 3)]);
        var pinned = table.CurrentSnapshot;
        string path = await FileOfAsync(table, 1);
        var positions = new Dictionary<int, IReadOnlyCollection<long>> { [0] = [0L, 2L] };
        var (actions, _) = await table.ComputeDeletionVectorActionsAsync(positions, resolveAgainst: pinned);
        Assert.Contains(actions, a => a is AddFile);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(IdEquals(2));

        await using var committer = await OpenAsync();
        var rebased = await committer.RebaseDvDmlActionsAsync(actions, positions, pinned, committer.CurrentSnapshot);
        AssertRemovedWhole(rebased, path);
        await committer.CheckLogicalRebaseAsync(pinned, rebased, rowLevelDml: true);
        await committer.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), DeltaWriteMode.Append,
            extraActions: rebased, expectedVersion: committer.CurrentSnapshot.Version, operation: "DELETE");

        Assert.Empty(await IdsAsync());
    }

    /// <summary>The path-sorted ordinal the host's positions are keyed by.</summary>
    private static int OrdinalOf(Snapshot.Snapshot snapshot, string path) =>
        snapshot.ActiveFiles.Values.Select(f => f.Path).OrderBy(p => p, StringComparer.Ordinal).ToList().IndexOf(path);
}
