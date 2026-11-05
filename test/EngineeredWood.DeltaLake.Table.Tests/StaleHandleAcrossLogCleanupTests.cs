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

    private async Task CreateAsync(string retention)
    {
        await using var writer = await DeltaTable.CreateAsync(
            Fs, TableSchema, Options,
            configuration: new Dictionary<string, string> { ["delta.logRetentionDuration"] = retention });
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
        await using (var writer = await DeltaTable.OpenAsync(Fs, Options))
        {
            for (long i = 10; i <= 19; i++)
                await writer.WriteAsync([Rows(i)]);                                         // v10..v19
            Assert.Equal(19, await writer.CheckpointAsync());
        }
        for (int v = 0; v < 19; v++)
        {
            File.Delete(Path.Combine(LogDir, $"{v:D20}.json"));
            File.Delete(Path.Combine(LogDir, $"{v:D20}.crc"));
        }
        await Task.Delay(1200);
        Assert.Equal(9, stale.CurrentSnapshot.Version);
        return stale;
    }

    private static readonly List<long> AllNineteen = Enumerable.Range(1, 19).Select(i => (long)i).ToList();

    private void AssertNothingWrittenBelowTheCheckpoint()
    {
        for (int v = 0; v < 19; v++)
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
    /// What the guard costs. A snapshot seen as the latest within the window pays nothing, and the handle's own commit
    /// refreshes it, so a writer that keeps writing never pays. One older than the window pays one existence check of
    /// its base commit, which is there, and commits.
    /// </summary>
    [Fact]
    public async Task Guard_IsFreeForAFreshSnapshot_AndOneExistenceCheckForAnIdleOne()
    {
        await CreateAsync(ShortRetention);
        var counting = new CommitExistsCountingFileSystem(Fs);
        await using var table = await DeltaTable.OpenAsync(counting, Options);

        await table.WriteAsync([Rows(100)]);
        await table.WriteAsync([Rows(101)]);
        Assert.Equal(0, counting.CommitExistsChecks);

        await Task.Delay(1200);
        await table.WriteAsync([Rows(102)]);
        Assert.Equal(1, counting.CommitExistsChecks);
        Assert.Equal(12, table.CurrentSnapshot.Version);
    }

    /// <summary>The default retention caps the window at five minutes, so a just-opened handle pays nothing.</summary>
    [Fact]
    public async Task Guard_IsFreeUnderTheDefaultRetention()
    {
        await using (var writer = await DeltaTable.CreateAsync(Fs, TableSchema, Options))
            await writer.WriteAsync([Rows(1)]);
        var counting = new CommitExistsCountingFileSystem(Fs);
        await using var table = await DeltaTable.OpenAsync(counting, Options);

        await table.WriteAsync([Rows(2)]);
        await table.DeleteAsync(IdIs(1));

        Assert.Equal(0, counting.CommitExistsChecks);
        Assert.Equal(CleanedBaseGuard.MaxFreshWindow, TimeSpan.FromMinutes(5));
    }

    /// <summary>Counts existence checks of commit files; everything else passes through.</summary>
    private sealed class CommitExistsCountingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        private int _commitExistsChecks;

        public int CommitExistsChecks => Volatile.Read(ref _commitExistsChecks);

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
}
