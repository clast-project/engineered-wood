// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Which commits the change feed reads, and what a missing or unreadable one does (#726), measured against
/// delta-spark 4.0.0 (pyspark 4.0.4) on the same layouts: ten single-row appends, versions 1..10 after the
/// CREATE, checkpointed at 10 so the table opens whatever commit files are gone.
/// <list type="bullet">
/// <item>A version with no commit file inside the range is skipped (Spark: <c>table_changes(6, 10)</c> with 7
/// deleted returns 6, 8, 9, 10).</item>
/// <item>A range starting before the earliest commit file left is refused (Spark: an error when 0..4 are gone
/// and the range starts at 2; fine from 5).</item>
/// <item>A range whose end has no commit file is refused (Spark: "Could not find any delta files for version
/// 10").</item>
/// <item>A commit that exists but cannot be read fails the feed (Spark: an error). EW used to skip it as if it
/// were missing, so the feed silently lost that version's changes.</item>
/// </list>
/// The listed commits are also read ahead, several at once (#724).
/// </summary>
public class CdfCommitReadTests : IDisposable
{
    private const int Appends = 10;

    private readonly string _tempDir;

    public CdfCommitReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdf_commits_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private async Task CreateTableAsync()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), Schema,
            configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" });
        for (int i = 0; i < Appends; i++)
        {
            var id = new Int64Array.Builder().Append(i).Build();
            await table.WriteAsync([new RecordBatch(table.ArrowSchema, [id], 1)]);
        }
        await table.CheckpointAsync();
    }

    private string CommitPath(long version) =>
        Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json");

    private static async Task<List<long>> VersionsAsync(ITableFileSystem fs, long start, long end, int window = 4)
    {
        await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions { MaxConcurrentReads = window });
        var versions = new List<long>();
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions
                       {
                           StartVersion = start,
                           EndVersion = end,
                       }))
        {
            var column = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(CdfConfig.CommitVersionColumn));
            for (int i = 0; i < batch.Length; i++)
                versions.Add(column.GetValue(i)!.Value);
        }
        return versions;
    }

    private static long[] Range(long from, long to) =>
        Enumerable.Range((int)from, (int)(to - from + 1)).Select(v => (long)v).ToArray();

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AnIntactLog_ReadsEveryVersion(int window)
    {
        await CreateTableAsync();
        Assert.Equal(Range(2, 10), await VersionsAsync(new LocalTableFileSystem(_tempDir), 2, 10, window));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AVersionMissingInsideTheRange_IsSkipped(int window)
    {
        await CreateTableAsync();
        File.Delete(CommitPath(7));

        var fs = new LocalTableFileSystem(_tempDir);
        Assert.Equal([5, 6, 8, 9, 10], await VersionsAsync(fs, 5, 10, window));
        Assert.Equal([6, 8, 9, 10], await VersionsAsync(fs, 6, 10, window));
    }

    [Fact]
    public async Task AMissingStartVersion_IsSkipped_WhileEarlierCommitsRemain()
    {
        await CreateTableAsync();
        File.Delete(CommitPath(5));

        Assert.Equal(Range(6, 10), await VersionsAsync(new LocalTableFileSystem(_tempDir), 5, 10));
    }

    [Fact]
    public async Task ARangeStartingBeforeTheEarliestCommit_IsRefused()
    {
        await CreateTableAsync();
        for (int v = 0; v < 5; v++)
            File.Delete(CommitPath(v));
        var fs = new LocalTableFileSystem(_tempDir);

        var refused = await Assert.ThrowsAsync<DeltaFormatException>(() => VersionsAsync(fs, 2, 10));
        Assert.Equal(DeltaErrorCodes.TruncatedTransactionLog, refused.ErrorCode);
        Assert.Contains("earliest commit left in the log is version 5", refused.Message);

        Assert.Equal(Range(5, 10), await VersionsAsync(fs, 5, 10));
    }

    [Fact]
    public async Task ARangeWhoseEndHasNoCommit_IsRefused()
    {
        await CreateTableAsync();
        File.Delete(CommitPath(10)); // the checkpoint at 10 still opens the table

        var refused = await Assert.ThrowsAsync<DeltaFormatException>(
            () => VersionsAsync(new LocalTableFileSystem(_tempDir), 6, 10));
        Assert.Equal(DeltaErrorCodes.TruncatedTransactionLog, refused.ErrorCode);
    }

    /// <summary>The end's own commit, below the latest version (Spark, with 7 deleted and the table at 10: 6..7
    /// and 7..7 both fail). Checking only that SOME commit at or after the end exists let 6..7 return version 6
    /// alone.</summary>
    [Theory]
    [InlineData(6)]
    [InlineData(7)]
    public async Task ARangeWhoseEndHasNoCommit_BelowTheLatest_IsRefused(long start)
    {
        await CreateTableAsync();
        File.Delete(CommitPath(7));
        var fs = new LocalTableFileSystem(_tempDir);

        var refused = await Assert.ThrowsAsync<DeltaFormatException>(() => VersionsAsync(fs, start, 7));
        Assert.Equal(DeltaErrorCodes.TruncatedTransactionLog, refused.ErrorCode);
        Assert.Contains("version 7", refused.Message);

        // An end past the hole still reads, skipping it.
        Assert.Equal([6, 8], await VersionsAsync(fs, 6, 8));
    }

    /// <summary>
    /// The feed reads at most the handle's snapshot version, as every other read on the handle does. A commit
    /// another writer made after it is not read: its schema (here a column a schema-merging append added) is not
    /// the one the feed reconciles to, which is the snapshot's. <see cref="DeltaTable.RefreshAsync"/> moves the
    /// handle, and the feed with it.
    /// </summary>
    [Fact]
    public async Task ACommitAfterTheHandlesSnapshot_IsNotRead_UntilTheHandleRefreshes()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var created = await DeltaTable.CreateAsync(
                         fs, Schema, configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" }))
        {
            await created.WriteAsync([new RecordBatch(created.ArrowSchema, [new Int64Array.Builder().Append(1).Build()], 1)]);
        }

        await using var stale = await DeltaTable.OpenAsync(fs);
        Assert.Equal(1, stale.CurrentSnapshot.Version);

        await using (var other = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
        {
            var wider = new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int64Type.Default, false))
                .Field(new Field("extra", StringType.Default, true))
                .Build();
            await other.WriteAsync(
                [new RecordBatch(wider, [new Int64Array.Builder().Append(2).Build(), new StringArray.Builder().Append("x").Build()], 1)],
                mergeSchema: true);
        }

        var seen = new List<(long Version, bool HasExtra)>();
        await foreach (var batch in stale.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 0, EndVersion = long.MaxValue }))
        {
            var version = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(CdfConfig.CommitVersionColumn));
            for (int i = 0; i < batch.Length; i++)
                seen.Add((version.GetValue(i)!.Value, batch.Schema.GetFieldIndex("extra") >= 0));
        }
        Assert.Equal([(1L, false)], seen);

        await stale.RefreshAsync();
        var after = new List<(long Version, bool HasExtra)>();
        await foreach (var batch in stale.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 0, EndVersion = long.MaxValue }))
        {
            var version = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(CdfConfig.CommitVersionColumn));
            for (int i = 0; i < batch.Length; i++)
                after.Add((version.GetValue(i)!.Value, batch.Schema.GetFieldIndex("extra") >= 0));
        }
        Assert.Equal([(1L, true), (2L, true)], after);
    }

    [Fact]
    public async Task AnEndPastTheLatestVersion_ReadsToTheLatest()
    {
        await CreateTableAsync();
        var fs = new LocalTableFileSystem(_tempDir);

        Assert.Equal(Range(6, 10), await VersionsAsync(fs, 6, 15));
        Assert.Empty(await VersionsAsync(fs, 12, 15));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AnUnreadableCommit_FailsTheFeed(int window)
    {
        await CreateTableAsync();
        File.WriteAllText(CommitPath(7), "this is not json\n");

        await Assert.ThrowsAnyAsync<Exception>(() => VersionsAsync(new LocalTableFileSystem(_tempDir), 6, 10, window));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task ACommitReadThatFails_FailsTheFeed_RatherThanSkippingTheVersion(int window)
    {
        await CreateTableAsync();
        var fs = new CommitReadFileSystem(new LocalTableFileSystem(_tempDir)) { FailVersion = 7 };

        var failed = await Assert.ThrowsAsync<IOException>(() => VersionsAsync(fs, 6, 10, window));
        Assert.Contains("injected", failed.Message);
    }

    [Fact]
    public async Task TheCommits_AreReadAhead_SeveralAtOnce()
    {
        await CreateTableAsync();
        var fs = new CommitReadFileSystem(new LocalTableFileSystem(_tempDir)) { DelayMs = 30 };

        Assert.Equal(Range(1, 10), await VersionsAsync(fs, 1, 10, window: 4));
        Assert.InRange(fs.MaxInFlight, 2, 4);

        fs.Reset();
        Assert.Equal(Range(1, 10), await VersionsAsync(fs, 1, 10, window: 1));
        Assert.Equal(1, fs.MaxInFlight);
    }

    /// <summary>Commit reads (<c>_delta_log/*.json</c>): optionally delayed, with the most in flight at once
    /// recorded, or failing for one version with an <see cref="IOException"/> that is not a missing file.</summary>
    private sealed class CommitReadFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        private int _inFlight, _maxInFlight;

        public int DelayMs { get; init; }
        public long? FailVersion { get; init; }
        public int MaxInFlight => Volatile.Read(ref _maxInFlight);

        public void Reset() => Interlocked.Exchange(ref _maxInFlight, 0);

        public async ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
        {
            string name = Path.GetFileName(path);
            bool isCommit = path.Contains("_delta_log") && name.EndsWith(".json", StringComparison.Ordinal)
                && long.TryParse(Path.GetFileNameWithoutExtension(name), out _);
            if (!isCommit)
                return await inner.ReadAllBytesAsync(path, cancellationToken);
            if (FailVersion is { } fail && name == $"{fail:D20}.json")
                throw new IOException($"injected read failure for {name}");

            int now = Interlocked.Increment(ref _inFlight);
            int seen;
            while (now > (seen = Volatile.Read(ref _maxInFlight)))
                Interlocked.CompareExchange(ref _maxInFlight, now, seen);
            try
            {
                if (DelayMs > 0)
                    await Task.Delay(DelayMs, cancellationToken);
                return await inner.ReadAllBytesAsync(path, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        public PathNameConstraints PathConstraints => inner.PathConstraints;
        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);
        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }
}
