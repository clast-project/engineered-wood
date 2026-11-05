// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Time travel and restore by timestamp binary-search the in-commit-timestamp range (#662): the answers are the
/// linear walk's, on tables with in-commit timestamps from creation, from mid-life, never, and turned off after a
/// handle was opened, while a search reads O(log N) commits.
/// </summary>
public class TimeTravelByTimestampSearchTests : IDisposable
{
    private const long Base = 1_700_000_000_000;
    private const string Schema =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""";

    private readonly string _tempDir;

    public TimeTravelByTimestampSearchTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ict_search_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static long TimestampOf(long version) => Base + (1000 * version);

    /// <summary>
    /// Writes versions 0..<paramref name="count"/>-1, version v at <see cref="TimestampOf"/>(v). In-commit timestamps
    /// are on from <paramref name="ictFrom"/> (null: never), recorded as a mid-life enablement when it is not 0 and
    /// <paramref name="recordEnablement"/> holds.
    /// </summary>
    private async Task WriteTableAsync(int count, int? ictFrom, bool recordEnablement = true)
    {
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        for (int v = 0; v < count; v++)
            await log.WriteCommitAsync(v, CommitActions(v, ictFrom, recordEnablement));
    }

    private static List<DeltaAction> CommitActions(int v, int? ictFrom, bool recordEnablement = true)
    {
        bool stamped = ictFrom is { } from && v >= from;
        var actions = new List<DeltaAction>
        {
            InCommitTimestamp.CreateCommitInfo(
                TimestampOf(v), v == 0 ? "CREATE TABLE" : "WRITE", includeInCommitTimestamp: stamped),
        };
        if (v == 0)
        {
            actions.Add(new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["inCommitTimestamp"],
            });
        }
        if (v == 0 || v == ictFrom)
        {
            var configuration = new Dictionary<string, string>();
            if (stamped)
            {
                configuration[InCommitTimestamp.EnableKey] = "true";
                if (v > 0 && recordEnablement)
                {
                    configuration[InCommitTimestamp.EnablementVersionKey] = v.ToString();
                    configuration[InCommitTimestamp.EnablementTimestampKey] = TimestampOf(v).ToString();
                }
            }
            actions.Add(Metadata(configuration));
        }
        return actions;
    }

    private static MetadataAction Metadata(Dictionary<string, string> configuration) => new()
    {
        Id = "ict-search",
        Format = Format.Parquet,
        SchemaString = Schema,
        PartitionColumns = [],
        Configuration = configuration,
    };

    /// <summary>
    /// The latest version at or before <paramref name="target"/>, worked out from the timestamps written rather than
    /// read: each raised to one past the one before where it is not later, as the walk does.
    /// </summary>
    private static long? Expected(long target, IReadOnlyList<long> written)
    {
        long? best = null, previous = null;
        for (int v = 0; v < written.Count; v++)
        {
            long timestamp = previous is { } p && written[v] <= p ? p + 1 : written[v];
            previous = timestamp;
            if (timestamp > target)
                break;
            best = v;
        }
        return best;
    }

    private static long[] Written(int count) => Enumerable.Range(0, count).Select(v => TimestampOf(v)).ToArray();

    private static IEnumerable<long> Targets(int count)
    {
        yield return Base - 1;
        for (int v = 0; v < count; v++)
        {
            yield return TimestampOf(v) - 1;
            yield return TimestampOf(v);
            yield return TimestampOf(v) + 1;
        }
        yield return TimestampOf(count) + 5000;
    }

    private static async Task AssertResolvesLikeTheWalkAsync(DeltaTable table, int count) =>
        await AssertResolvesLikeTheWalkAsync(table, Written(count));

    private static async Task AssertResolvesLikeTheWalkAsync(DeltaTable table, IReadOnlyList<long> written)
    {
        foreach (long target in Targets(written.Count))
        {
            long? expected = Expected(target, written);
            var at = DateTimeOffset.FromUnixTimeMilliseconds(target);
            if (expected is null)
            {
                var ex = await Assert.ThrowsAsync<DeltaFormatException>(
                    async () => await table.GetSnapshotAtTimestampAsync(at));
                Assert.Equal(DeltaTableErrorCodes.NoCommitAtTimestamp, ex.ErrorCode);
            }
            else
            {
                var snapshot = await table.GetSnapshotAtTimestampAsync(at);
                Assert.True(expected == snapshot.Version, $"target {target - Base}: expected v{expected}, got v{snapshot.Version}");
            }
        }
    }

    [Theory]
    [InlineData(0)]    // on from creation
    [InlineData(7)]    // turned on mid-life, recorded
    [InlineData(null)] // never
    public async Task ResolvesAsTheWalkDoes(int? ictFrom)
    {
        const int count = 20;
        await WriteTableAsync(count, ictFrom);
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await AssertResolvesLikeTheWalkAsync(table, count);
    }

    /// <summary>
    /// A writer that turned them on mid-life without recording where: the commits before carry none, so a probe
    /// landing there sends the search back to the walk, and an answer the search does give is still exact.
    /// </summary>
    [Fact]
    public async Task UnrecordedEnablement_StillResolvesAsTheWalkDoes()
    {
        const int count = 20;
        await WriteTableAsync(count, ictFrom: 7, recordEnablement: false);
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await AssertResolvesLikeTheWalkAsync(table, count);
    }

    /// <summary>
    /// #690 review: an unrecorded mid-life enablement whose unstamped commit before it carries a LATER informational
    /// timestamp than the stamped ones after it (a skewed clock). The walk raises the stamped commits past it; a
    /// search over the stamped suffix alone would not, and need never probe the unstamped prefix to notice (a target
    /// just after v5 searches v9, v4, v6, v5 and answers v5, where the walk answers v0). Reading the first commit
    /// first finds the prefix and walks instead.
    /// </summary>
    [Fact]
    public async Task UnrecordedEnablement_WithASkewedUnstampedPrefix_ResolvesAsTheWalkDoes()
    {
        const int count = 20, ictFrom = 2, skewed = 1;
        var written = Written(count);
        written[skewed] = TimestampOf(10) + 500;
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        for (int v = 0; v < count; v++)
        {
            var actions = CommitActions(v, ictFrom, recordEnablement: false);
            if (v == skewed)
                actions[0] = InCommitTimestamp.CreateCommitInfo(written[v], "WRITE", includeInCommitTimestamp: false);
            await log.WriteCommitAsync(v, actions);
        }
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        Assert.Equal(0, (await table.GetSnapshotAtTimestampAsync(
            DateTimeOffset.FromUnixTimeMilliseconds(TimestampOf(5) + 1))).Version);
        await AssertResolvesLikeTheWalkAsync(table, written);
    }

    /// <summary>
    /// A handle opened while in-commit timestamps were on still sees them on after another writer turns them off: its
    /// probes into the unstamped commits make it walk instead, so the stale configuration costs only speed. One of
    /// those commits is dated before its predecessor (a skewed clock), so a search that read the informational
    /// <c>commitInfo.timestamp</c> instead of giving up would answer differently from the walk.
    /// </summary>
    [Fact]
    public async Task TurnedOffAfterTheHandleOpened_StillResolvesAsTheWalkDoes()
    {
        const int stamped = 12, count = 20, skewed = 16;
        await WriteTableAsync(stamped, ictFrom: 0);
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        var written = Written(count);
        written[skewed] = TimestampOf(stamped) + 1;
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        for (int v = stamped; v < count; v++)
        {
            var actions = new List<DeltaAction>
            {
                InCommitTimestamp.CreateCommitInfo(written[v], "WRITE", includeInCommitTimestamp: false),
            };
            if (v == stamped)
                actions.Add(Metadata(new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "false" }));
            await log.WriteCommitAsync(v, actions);
        }

        Assert.True(InCommitTimestamp.IsEnabled(table.CurrentSnapshot.Metadata.Configuration));
        await AssertResolvesLikeTheWalkAsync(table, written);
    }

    /// <summary>
    /// #690 review: a handle opened while in-commit timestamps were on from creation, after which another writer
    /// turned them off and then on again under a clock set back. The stale range would span both stamped runs, which
    /// do not increase across the gap, and the search could skip the gap and answer from the earlier run. Resolving
    /// from the latest configuration searches only the re-enabled run, so the stale handle answers as a fresh one,
    /// and a restore after the latest commit's time is refused rather than landing on the earlier run.
    /// </summary>
    [Fact]
    public async Task StaleHandle_AcrossADisableAndARolledBackReEnable_AnswersAsAFreshOne()
    {
        const int firstRun = 10, reEnabled = 12, count = 20;
        await WriteTableAsync(firstRun, ictFrom: 0);
        await using var stale = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        static long RolledBack(int v) => Base + 500 + (10 * (v - reEnabled));
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        for (int v = firstRun; v < count; v++)
        {
            bool stamped = v >= reEnabled;
            long timestamp = stamped ? RolledBack(v) : TimestampOf(v);
            var actions = new List<DeltaAction>
            {
                InCommitTimestamp.CreateCommitInfo(timestamp, "WRITE", includeInCommitTimestamp: stamped),
            };
            if (v == firstRun)
                actions.Add(Metadata(new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "false" }));
            if (v == reEnabled)
            {
                actions.Add(Metadata(new Dictionary<string, string>
                {
                    [InCommitTimestamp.EnableKey] = "true",
                    [InCommitTimestamp.EnablementVersionKey] = reEnabled.ToString(),
                    [InCommitTimestamp.EnablementTimestampKey] = RolledBack(reEnabled).ToString(),
                }));
            }
            await log.WriteCommitAsync(v, actions);
        }
        await using var fresh = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal(firstRun - 1, stale.CurrentSnapshot.Version);

        // From the re-enablement on, the re-enabled run's own timestamps decide: v5's time is after all of them.
        var atV5 = DateTimeOffset.FromUnixTimeMilliseconds(TimestampOf(5));
        Assert.Equal(count - 1, (await stale.GetSnapshotAtTimestampAsync(atV5)).Version);

        foreach (long target in Targets(count).Concat(Enumerable.Range(reEnabled, count - reEnabled)
                     .SelectMany(v => new[] { RolledBack(v) - 1, RolledBack(v), RolledBack(v) + 1 })))
        {
            var at = DateTimeOffset.FromUnixTimeMilliseconds(target);
            long? expected = null, actual = null;
            try { expected = (await fresh.GetSnapshotAtTimestampAsync(at)).Version; }
            catch (DeltaFormatException) { }
            try { actual = (await stale.GetSnapshotAtTimestampAsync(at)).Version; }
            catch (DeltaFormatException) { }
            Assert.True(expected == actual, $"target {target - Base}: fresh v{expected}, stale v{actual}");
        }
        Assert.Equal(firstRun - 1, stale.CurrentSnapshot.Version); // time travel does not move the handle

        var greater = await Assert.ThrowsAsync<DeltaFormatException>(async () => await stale.RestoreAsync(atV5));
        Assert.Equal(DeltaTableErrorCodes.CannotRestoreTimestampGreater, greater.ErrorCode);
    }

    /// <summary>
    /// #690 review: a handle opened before log cleanup removed the commits after its version, which a checkpoint now
    /// covers. The table is still readable, so time travel through that handle must still resolve, taking the latest
    /// configuration from the checkpoint where an incremental update would refuse the gap.
    /// </summary>
    [Fact]
    public async Task StaleHandle_AfterLogCleanupPastItsVersion_StillResolves()
    {
        const int opened = 10, count = 20;
        await WriteTableAsync(opened, ictFrom: 0);
        await using var stale = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        for (int v = opened; v < count; v++)
            await log.WriteCommitAsync(v, CommitActions(v, ictFrom: 0));
        await using (var writer = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
            Assert.Equal(count - 1, await writer.CheckpointAsync());
        for (int v = opened; v < count - 1; v++)
            File.Delete(Path.Combine(_tempDir, "_delta_log", $"{v:D20}.json"));

        Assert.Equal(opened - 1, stale.CurrentSnapshot.Version);
        var early = await stale.GetSnapshotAtTimestampAsync(DateTimeOffset.FromUnixTimeMilliseconds(TimestampOf(5) + 1));
        Assert.Equal(5, early.Version);
        var latest = await stale.GetSnapshotAtTimestampAsync(
            DateTimeOffset.FromUnixTimeMilliseconds(TimestampOf(count - 1) + 1));
        Assert.Equal(count - 1, latest.Version);
        Assert.Equal(opened - 1, stale.CurrentSnapshot.Version);
    }

    /// <summary>
    /// The point of #662: on an in-commit-timestamp table, resolving a timestamp reads O(log N) commits beyond what
    /// building the snapshot at the answer reads, where the walk reads every commit up to the answer.
    /// </summary>
    [Theory]
    [InlineData(0, 150, 10)]   // from creation: ceil(log2 300) + 1
    [InlineData(100, 250, 10)] // mid-life, after the enablement: the search covers 200
    [InlineData(100, 50, 52)]  // mid-life, before it: the walk, but only of the commits up to just past the answer
    [InlineData(null, 150, 152)]
    public async Task ResolutionReadsLogarithmicallyManyCommits(int? ictFrom, int answer, int maxReads)
    {
        const int count = 300;
        await WriteTableAsync(count, ictFrom);
        var counting = new CommitReadCountingFileSystem(new LocalTableFileSystem(_tempDir));
        await using var table = await DeltaTable.OpenAsync(counting);

        counting.Reset();
        await table.GetSnapshotAtVersionAsync(answer);
        int building = counting.CommitReads;

        counting.Reset();
        var snapshot = await table.GetSnapshotAtTimestampAsync(
            DateTimeOffset.FromUnixTimeMilliseconds(TimestampOf(answer) + 1));
        int resolving = counting.CommitReads - building;

        Assert.Equal(answer, snapshot.Version);
        Assert.True(resolving <= maxReads, $"{resolving} commit reads to resolve the timestamp");
        if (ictFrom is null)
            Assert.True(resolving > answer, $"the walk read only {resolving}"); // the harness measures something
    }

    [Theory]
    [InlineData(0)]
    [InlineData(7)]
    [InlineData(null)]
    public async Task Restore_ResolvesAndRefusesAsBefore(int? ictFrom)
    {
        const int count = 20;
        await WriteTableAsync(count, ictFrom);
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        var earlier = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.RestoreAsync(DateTimeOffset.FromUnixTimeMilliseconds(Base - 1)));
        Assert.Equal(DeltaTableErrorCodes.CannotRestoreTimestampEarlier, earlier.ErrorCode);
        var greater = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.RestoreAsync(DateTimeOffset.FromUnixTimeMilliseconds(TimestampOf(count - 1) + 1)));
        Assert.Equal(DeltaTableErrorCodes.CannotRestoreTimestampGreater, greater.ErrorCode);

        // Each restore commits a version stamped with the clock, years after these, so earlier answers stand.
        foreach (int v in new[] { 0, 3, 7, 8, 15, count - 1 })
        {
            var exact = await table.RestoreAsync(DateTimeOffset.FromUnixTimeMilliseconds(TimestampOf(v)));
            Assert.Equal(v, exact.RestoredVersion);
            var between = await table.RestoreAsync(DateTimeOffset.FromUnixTimeMilliseconds(TimestampOf(v) + 999));
            Assert.Equal(v, between.RestoredVersion);
        }
    }

    /// <summary>Counts whole reads of commit files; everything else passes through.</summary>
    private sealed class CommitReadCountingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        private int _commitReads;

        public int CommitReads => Volatile.Read(ref _commitReads);

        public void Reset() => Interlocked.Exchange(ref _commitReads, 0);

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
        {
            if (DeltaVersion.TryParseCommitVersion(Path.GetFileName(path), out _))
                Interlocked.Increment(ref _commitReads);
            return inner.ReadAllBytesAsync(path, cancellationToken);
        }

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
