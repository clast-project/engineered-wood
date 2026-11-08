// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #697: a handle opened before log cleanup removed the commits after its version. Readers move forward from the
/// checkpoint; a commit on the overtaken base is refused before anything is written, where it used to land below the
/// checkpoint and be lost; and VACUUM works from the latest version, where it used to delete files later commits
/// added.
/// </summary>
public class StaleHandleAcrossLogCleanupTests : IDisposable
{
    // Short, so a snapshot counts as fresh for only this long and a test can outwait it (the guard's window is
    // the smaller of this and five minutes).
    private const string ShortRetention = "interval 1 seconds";

    private readonly string _tempDir = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), $"delta_stale_cleanup_{Guid.NewGuid():N}")).FullName;

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private string LogDir => Path.Combine(_tempDir, "_delta_log");

    private static DeltaTableOptions Options => new() { CheckpointInterval = 0 };

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Build();

    private static RecordBatch Rows(params long[] ids)
    {
        var id = new Int64Array.Builder();
        foreach (long i in ids)
            id.Append(i);
        return new RecordBatch(TableSchema, [id.Build()], ids.Length);
    }

    private static Func<RecordBatch, BooleanArray> IdIs(long value) => batch =>
    {
        var column = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            mask.Append(column.GetValue(i) == value);
        return mask.Build();
    };

    private static async Task<List<long>> IdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync())
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    private async Task<List<long>> FreshIdsAsync()
    {
        await using var fresh = await DeltaTable.OpenAsync(Fs);
        return await IdsAsync(fresh);
    }

    /// <param name="cleanUpLog">False sets <c>delta.enableExpiredLogCleanup = false</c>, so a checkpoint removes no
    /// commit on its own: under the one-second retention which commits it would remove depends on how fast the
    /// machine wrote them.</param>
    private async Task CreateAsync(string retention, bool cleanUpLog = true)
    {
        var configuration = new Dictionary<string, string> { ["delta.logRetentionDuration"] = retention };
        if (!cleanUpLog)
            configuration["delta.enableExpiredLogCleanup"] = "false";
        await using var writer = await DeltaTable.CreateAsync(Fs, TableSchema, Options, configuration: configuration);
        for (long i = 1; i <= 9; i++)
            await writer.WriteAsync([Rows(i)]);                                             // v1..v9
    }

    /// <summary>
    /// v0 create and v1..v9; the handle opened at v9; v10..v19 by another writer, a checkpoint at v19, and every
    /// commit before v19 removed, as log cleanup leaves them once they expire. The handle is then older than the
    /// table's (short) retention, so its commits get checked.
    /// </summary>
    private async Task<DeltaTable> OvertakenHandleAsync(ITableFileSystem? fs = null)
    {
        await CreateAsync(ShortRetention);
        var stale = await DeltaTable.OpenAsync(fs ?? Fs, Options);
        Assert.Equal(19, await OvertakeAsync());
        Assert.Equal(9, stale.CurrentSnapshot.Version);
        return stale;
    }

    /// <summary>
    /// Another writer appends ids 10..19 in ten commits and checkpoints the last; every commit before the checkpoint
    /// is then removed, as log cleanup leaves them once they expire, and the table's (short) retention is outwaited.
    /// Returns the checkpoint version.
    /// </summary>
    private async Task<long> OvertakeAsync()
    {
        long checkpoint;
        await using (var writer = await DeltaTable.OpenAsync(Fs, Options))
        {
            for (long i = 10; i <= 19; i++)
                await writer.WriteAsync([Rows(i)]);
            checkpoint = await writer.CheckpointAsync();
        }
        for (long v = 0; v < checkpoint; v++)
        {
            File.Delete(Path.Combine(LogDir, $"{v:D20}.json"));
            File.Delete(Path.Combine(LogDir, $"{v:D20}.crc"));
        }
        await Task.Delay(1200);
        return checkpoint;
    }

    private static readonly List<long> AllNineteen = Enumerable.Range(1, 19).Select(i => (long)i).ToList();

    private void AssertNothingWrittenBelowTheCheckpoint(long checkpoint = 19)
    {
        for (long v = 0; v < checkpoint; v++)
            Assert.False(File.Exists(Path.Combine(LogDir, $"{v:D20}.json")), $"a commit was written at v{v}");
    }

    private static async Task AssertRefusedAsync(Func<Task> commit)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(commit);
        Assert.Equal(DeltaErrorCodes.TruncatedTransactionLog, ex.ErrorCode);
        Assert.Contains("Nothing was written", ex.Message);
    }

    /// <summary>An append used to create v10 below the v19 checkpoint, where no reader looks: the write was lost
    /// after being reported durable. Now it is refused and nothing is written.</summary>
    [Fact]
    public async Task Append_OnAnOvertakenBase_IsRefusedAndWritesNothing()
    {
        await using var stale = await OvertakenHandleAsync();

        await AssertRefusedAsync(async () => await stale.WriteAsync([Rows(100)]));

        AssertNothingWrittenBelowTheCheckpoint();
        Assert.Equal(AllNineteen, await FreshIdsAsync());
    }

    /// <summary>The overwrite family commits once, directly, outside the optimistic-concurrency loop.</summary>
    [Fact]
    public async Task Overwrite_OnAnOvertakenBase_IsRefusedAndWritesNothing()
    {
        await using var stale = await OvertakenHandleAsync();

        await AssertRefusedAsync(async () => await stale.WriteAsync([Rows(100)], DeltaWriteMode.Overwrite));

        AssertNothingWrittenBelowTheCheckpoint();
        Assert.Equal(AllNineteen, await FreshIdsAsync());
    }

    [Fact]
    public async Task Delete_OnAnOvertakenBase_IsRefusedAndWritesNothing()
    {
        await using var stale = await OvertakenHandleAsync();

        await AssertRefusedAsync(async () => await stale.DeleteAsync(IdIs(3)));

        AssertNothingWrittenBelowTheCheckpoint();
        Assert.Equal(AllNineteen, await FreshIdsAsync());
    }

    /// <summary>The remedy the refusal names: a refresh moves the handle to the checkpoint, where it used to throw,
    /// and the retried write lands after it.</summary>
    [Fact]
    public async Task Refresh_MovesAnOvertakenHandleToTheCheckpoint_AndTheRetryCommits()
    {
        await using var stale = await OvertakenHandleAsync();
        await AssertRefusedAsync(async () => await stale.WriteAsync([Rows(100)]));

        await stale.RefreshAsync();
        Assert.Equal(19, stale.CurrentSnapshot.Version);
        Assert.Equal(20, await stale.WriteAsync([Rows(100)]));

        Assert.Equal([.. AllNineteen, 100], await FreshIdsAsync());
    }

    /// <summary>Restore starts by moving to the latest version; it used to throw on the gap.</summary>
    [Fact]
    public async Task Restore_FromAnOvertakenHandle_StartsFromTheCheckpoint()
    {
        await using var stale = await OvertakenHandleAsync();

        var result = await stale.RestoreAsync(19);

        Assert.Equal(19, result.RestoredVersion);
        Assert.Equal(AllNineteen, await IdsAsync(stale));
    }

    /// <summary>
    /// VACUUM deletes what its snapshot does not name, so from a stale snapshot it deleted the files later commits
    /// added once they were older than the retention, leaving the table unreadable. It now works from the latest
    /// version. No log cleanup is needed for this one; only another writer's commit.
    /// </summary>
    [Fact]
    public async Task Vacuum_FromAStaleHandle_KeepsFilesLaterCommitsAdded()
    {
        await using (var writer = await DeltaTable.CreateAsync(Fs, TableSchema, Options))
            await writer.WriteAsync([Rows(1)]);
        await using var stale = await DeltaTable.OpenAsync(Fs, Options);
        await using (var writer = await DeltaTable.OpenAsync(Fs, Options))
            await writer.WriteAsync([Rows(2)]);
        foreach (string file in Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories))
            File.SetLastWriteTimeUtc(file, DateTime.UtcNow.AddDays(-30));

        var result = await stale.VacuumAsync(TimeSpan.FromDays(7));

        Assert.Equal(0, result.FilesDeleted);
        Assert.Equal([1L, 2L], await FreshIdsAsync());
    }

    /// <summary>
    /// A base whose own commit is gone but which nothing follows (a handle opened on a checkpoint whose commit was
    /// cleaned up) is not overtaken: the commit proceeds.
    /// </summary>
    [Fact]
    public async Task Commit_OnACheckpointOnlyLatestVersion_Proceeds()
    {
        await CreateAsync(ShortRetention);
        await using (var writer = await DeltaTable.OpenAsync(Fs, Options))
            Assert.Equal(9, await writer.CheckpointAsync());
        for (int v = 0; v <= 9; v++)
        {
            File.Delete(Path.Combine(LogDir, $"{v:D20}.json"));
            File.Delete(Path.Combine(LogDir, $"{v:D20}.crc"));
        }
        await using var table = await DeltaTable.OpenAsync(Fs, Options);
        await Task.Delay(1200);

        Assert.Equal(10, await table.WriteAsync([Rows(100)]));
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 100L], await FreshIdsAsync());
    }

    /// <summary>
    /// What the guard costs since #750: one single-file listing of the base's commit per commit, whether the snapshot
    /// is fresh or idle, and no existence check. Freshness no longer makes it free, because a drop and re-create can
    /// happen moments after the base was read; the listing answers both questions (replaced, and overtaken).
    /// </summary>
    [Fact]
    public async Task Guard_CostsOneBaseCheckPerCommit_FreshOrIdle()
    {
        await CreateAsync(ShortRetention);
        var counting = new CommitExistsCountingFileSystem(Fs);
        await using var table = await DeltaTable.OpenAsync(counting, Options);

        await table.WriteAsync([Rows(100)]);
        await table.WriteAsync([Rows(101)]);
        Assert.Equal(2, counting.CommitListings);

        await Task.Delay(1200);
        await table.WriteAsync([Rows(102)]);
        Assert.Equal(3, counting.CommitListings);
        Assert.Equal(0, counting.CommitExistsChecks);
        Assert.Equal(12, table.CurrentSnapshot.Version);
    }

    /// <summary>Under the default retention too: one check per commit, the window notwithstanding.</summary>
    [Fact]
    public async Task Guard_CostsOneBaseCheckPerCommit_UnderTheDefaultRetention()
    {
        await using (var writer = await DeltaTable.CreateAsync(Fs, TableSchema, Options))
            await writer.WriteAsync([Rows(1)]);
        var counting = new CommitExistsCountingFileSystem(Fs);
        await using var table = await DeltaTable.OpenAsync(counting, Options);

        await table.WriteAsync([Rows(2)]);
        await table.DeleteAsync(IdIs(1));

        Assert.Equal(2, counting.CommitListings);
        Assert.Equal(0, counting.CommitExistsChecks);
        Assert.Equal(CleanedBaseGuard.MaxFreshWindow, TimeSpan.FromMinutes(5));
    }

    /// <summary>
    /// #698 review: a restore that actually commits, from a handle cleanup overtook. It commits on the base it
    /// brought up from the checkpoint, and the refresh after the commit used to start from the handle's own stale
    /// snapshot instead, replay across the gap, and throw after the commit was durable.
    /// </summary>
    [Fact]
    public async Task Restore_ThatCommits_FromAnOvertakenHandle_RefreshesFromItsOwnBase()
    {
        await using var stale = await OvertakenHandleAsync();
        await using (var writer = await DeltaTable.OpenAsync(Fs, Options))
            Assert.Equal(20, await writer.WriteAsync([Rows(20)]));

        var result = await stale.RestoreAsync(19);

        Assert.Equal(19, result.RestoredVersion);
        Assert.Equal(21, result.Version);
        Assert.Equal(21, stale.CurrentSnapshot.Version);
        Assert.Equal(AllNineteen, await FreshIdsAsync());
    }

    /// <summary>
    /// #698 review: cleanup landing mid-refresh. The listing names v10..v19, and the commits before v19 are deleted
    /// before the replay reads v10, so the read fails with the file system's own not-found error rather than the
    /// truncated-log one. The refresh re-lists, sees the gap, and builds from the checkpoint.
    /// </summary>
    [Fact]
    public async Task Refresh_WhenCleanupDeletesAListedCommitMidReplay_BuildsFromTheCheckpoint()
    {
        // The race deletes the commits; the writer's checkpoint must not, or on a slow machine v10 has expired and
        // is gone before the refresh lists, and the refresh never reads it (seen on CI, net472).
        await CreateAsync(ShortRetention, cleanUpLog: false);
        var racing = new CleanupMidReplayFileSystem(Fs, firstRead: 10, deleteBelow: 19, LogDir);
        await using var stale = await DeltaTable.OpenAsync(racing, Options);
        await using (var writer = await DeltaTable.OpenAsync(Fs, Options))
        {
            for (long i = 10; i <= 19; i++)
                await writer.WriteAsync([Rows(i)]);                                         // v10..v19
            Assert.Equal(19, await writer.CheckpointAsync());
        }

        await stale.RefreshAsync();

        Assert.True(racing.Raced);
        Assert.Equal(19, stale.CurrentSnapshot.Version);
        Assert.Equal(AllNineteen, await IdsAsync(stale));
    }

    /// <summary>
    /// #698 review: create-or-replace commits at the replaced version + 1 directly, after staging its data. Another
    /// writer overtakes it and cleanup runs while it stages; it used to create v10 below the v19 checkpoint. It is
    /// refused, and the data it staged is taken back.
    /// </summary>
    [Fact]
    public async Task CreateOrReplace_OvertakenWhileStaging_IsRefusedAndWritesNothing()
    {
        await CreateAsync(ShortRetention);
        var racing = new RaceOnDataFileFileSystem(Fs, OvertakeAsync, onDelete: false);

        await AssertRefusedAsync(async () => await DeltaTable.CreateOrReplaceAsync(
            racing, TableSchema, [Rows(100)], Options,
            configuration: new Dictionary<string, string> { ["delta.logRetentionDuration"] = ShortRetention }));

        Assert.True(racing.Raced);
        AssertNothingWrittenBelowTheCheckpoint();
        Assert.Equal(AllNineteen, await FreshIdsAsync());
        Assert.Equal(19, DataFiles().Length);
    }

    /// <summary>
    /// #698 review: VACUUM END follows the deletes, which can outlast the retention. Another writer overtakes START
    /// and cleanup runs while VACUUM deletes; END used to be created at START + 1, below the checkpoint. It carries
    /// only a commitInfo, so it lands after the checkpoint instead, and the handle moves there.
    /// </summary>
    [Fact]
    public async Task Vacuum_OvertakenWhileDeleting_CommitsEndAfterTheCheckpoint()
    {
        await CreateAsync(ShortRetention);                                                  // v0..v9
        string orphan = Path.Combine(_tempDir, "orphan.parquet");
        File.WriteAllBytes(orphan, [1, 2, 3]);
        File.SetLastWriteTimeUtc(orphan, DateTime.UtcNow.AddDays(-30));
        var racing = new RaceOnDataFileFileSystem(Fs, OvertakeAsync, onDelete: true);
        await using var table = await DeltaTable.OpenAsync(racing, Options);

        var result = await table.VacuumAsync(TimeSpan.FromDays(7));                         // START at v10

        Assert.True(racing.Raced);                                                          // v11..v20, checkpoint v20
        Assert.Equal(1, result.FilesDeleted);
        Assert.False(File.Exists(orphan));
        Assert.Equal(21, result.LastCommittedVersion);
        Assert.Equal(21, table.CurrentSnapshot.Version);
        AssertNothingWrittenBelowTheCheckpoint(20);
        Assert.Equal(AllNineteen, await FreshIdsAsync());
    }

    private string[] DataFiles() => Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
        .Where(f => !f.StartsWith(LogDir, StringComparison.Ordinal))
        .ToArray();

    /// <summary>
    /// Runs <c>race</c> once, the first time a file outside the log is created (or, with <c>onDelete</c>, deleted):
    /// the moment a replace is staging its data, or VACUUM is deleting.
    /// </summary>
    private sealed class RaceOnDataFileFileSystem(ITableFileSystem inner, Func<Task<long>> race, bool onDelete)
        : ITableFileSystem
    {
        private int _raced;

        public bool Raced => Volatile.Read(ref _raced) == 1;

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        private async ValueTask RaceOnceAsync(string path, bool deleting)
        {
            if (deleting == onDelete && !path.Replace('\\', '/').Contains("_delta_log")
                && Interlocked.Exchange(ref _raced, 1) == 0)
            {
                await race();
            }
        }

        public async ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            await RaceOnceAsync(path, deleting: false);
            return await inner.CreateAsync(path, overwrite, cancellationToken);
        }

        public async ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            await RaceOnceAsync(path, deleting: false);
            return await inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        }

        public async ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            await RaceOnceAsync(path, deleting: false);
            await inner.WriteAllBytesAsync(path, data, cancellationToken);
        }

        public async ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
        {
            await RaceOnceAsync(path, deleting: true);
            await inner.DeleteAsync(path, cancellationToken);
        }

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);
    }

    /// <summary>
    /// The first time commit <c>firstRead</c> is read, deletes every commit below <c>deleteBelow</c>, as a cleanup
    /// running between a listing and the read would; the read then fails as the inner file system fails it.
    /// </summary>
    private sealed class CleanupMidReplayFileSystem(
        ITableFileSystem inner, long firstRead, long deleteBelow, string logDir) : ITableFileSystem
    {
        private int _raced;

        public bool Raced => Volatile.Read(ref _raced) == 1;

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
        {
            if (DeltaVersion.TryParseCommitVersion(Path.GetFileName(path), out long v) && v == firstRead
                && Interlocked.Exchange(ref _raced, 1) == 0)
            {
                for (long d = 0; d < deleteBelow; d++)
                {
                    File.Delete(Path.Combine(logDir, $"{d:D20}.json"));
                    File.Delete(Path.Combine(logDir, $"{d:D20}.crc"));
                }
            }
            return inner.ReadAllBytesAsync(path, cancellationToken);
        }

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);

        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }

    /// <summary>Counts existence checks of commit files; everything else passes through.</summary>
    private sealed class CommitExistsCountingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        private int _commitExistsChecks;

        public int CommitExistsChecks => Volatile.Read(ref _commitExistsChecks);

        private int _commitListings;

        /// <summary>Listings whose prefix is one commit file: the guard's check of a base.</summary>
        public int CommitListings => Volatile.Read(ref _commitListings);

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
        {
            if (DeltaVersion.TryParseCommitVersion(Path.GetFileName(path), out _))
                Interlocked.Increment(ref _commitExistsChecks);
            return inner.ExistsAsync(path, cancellationToken);
        }

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
        {
            if (DeltaVersion.TryParseCommitVersion(Path.GetFileName(prefix), out _))
                Interlocked.Increment(ref _commitListings);
            return inner.ListAsync(prefix, cancellationToken);
        }

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);

        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }
}
