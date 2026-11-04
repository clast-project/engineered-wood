// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// #115: log cleanup on a table whose listing cannot date its files, aged by the in-commit timestamps the commits
/// carry. A listing that CAN date everything keeps delta-spark's rule (modification times) exactly.
/// </summary>
public class LogCleanupInCommitTimestampTests : IDisposable
{
    private static readonly DateTimeOffset Now = new(2026, 8, 8, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTime Epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TransactionLog _log;

    public LogCleanupInCommitTimestampTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_logclean_ict_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
        _log = new TransactionLog(_fs);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Dictionary<string, string> Config(long? enablementVersion = null, long enablementTimestamp = 0)
    {
        var config = new Dictionary<string, string>
        {
            [InCommitTimestamp.EnableKey] = "true",
            ["delta.logRetentionDuration"] = "interval 1 days",
        };
        if (enablementVersion is { } v)
        {
            config[InCommitTimestamp.EnablementVersionKey] = v.ToString();
            config[InCommitTimestamp.EnablementTimestampKey] = enablementTimestamp.ToString();
        }
        return config;
    }

    private static long DaysAgo(double days) => (Now - TimeSpan.FromDays(days)).ToUnixTimeMilliseconds();

    private static MetadataAction Metadata(IReadOnlyDictionary<string, string> configuration) => new()
    {
        Id = "cleanup-ict",
        Format = Format.Parquet,
        SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
        PartitionColumns = [],
        CreatedTime = 1700000000000,
        Configuration = configuration,
    };

    private static AddFile Add(long v) => new()
    {
        Path = $"f{v}.parquet",
        PartitionValues = new Dictionary<string, string>(),
        Size = 100,
        ModificationTime = 1700000001000,
        DataChange = true,
    };

    /// <summary>
    /// Writes versions 0..<paramref name="stamps"/>.Length - 1. A null stamp writes a commit with no in-commit
    /// timestamp (only the informational one). The metadata (in v0, or at <paramref name="enableAt"/>) carries <paramref name="configuration"/>.
    /// </summary>
    private async ValueTask WriteAsync(long?[] stamps, Dictionary<string, string> configuration, long enableAt = 0)
    {
        for (long v = 0; v < stamps.Length; v++)
        {
            var actions = new List<DeltaAction>();
            // A commit without an in-commit timestamp still has commitInfo's informational `timestamp`, and an old
            // one: cleanup must not take it for an in-commit timestamp.
            actions.Add(stamps[v] is { } stamp
                ? InCommitTimestamp.CreateCommitInfo(stamp)
                : InCommitTimestamp.CreateCommitInfo(DaysAgo(30), includeInCommitTimestamp: false));
            if (v == 0)
            {
                actions.Add(new ProtocolAction
                {
                    MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["inCommitTimestamp"],
                });
                actions.Add(Metadata(enableAt == 0 ? configuration : new Dictionary<string, string>()));
            }
            else if (v == enableAt)
            {
                actions.Add(Metadata(configuration));
            }
            actions.Add(Add(v));
            await _log.WriteCommitAsync(v, actions);
        }
    }

    private async ValueTask CheckpointAtAsync(long version)
    {
        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs), atVersion: version);
        await new CheckpointWriter(_fs).WriteCheckpointAsync(snapshot);
    }

    private void UndateEverything()
    {
        foreach (var file in Directory.GetFiles(Path.Combine(_tempDir, "_delta_log")))
            File.SetLastWriteTimeUtc(file, Epoch);
    }

    private bool LogFileExists(string name) => File.Exists(Path.Combine(_tempDir, "_delta_log", name));

    private static string Commit(long v) => $"{v:D20}.json";

    [Fact]
    public async Task UndatedListing_DeletesTheCommitsWhoseInCommitTimestampHasExpired()
    {
        var config = Config();
        await WriteAsync([DaysAgo(10), DaysAgo(9), DaysAgo(8), DaysAgo(2), DaysAgo(0.5), DaysAgo(0.1)], config);
        await CheckpointAtAsync(5);
        File.WriteAllText(Path.Combine(_tempDir, "_delta_log", $"{1:D20}.crc"), "{}");
        File.WriteAllText(Path.Combine(_tempDir, "_delta_log", $"{4:D20}.crc"), "{}");
        UndateEverything();

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        // v0..v3 expired (v3 is two days old against a one-day horizon); v4 is twelve hours old and stays. The
        // checksum for v1 goes with its commit, by version, since the listing cannot age it; v4's stays.
        Assert.Equal(5, deleted);
        for (long v = 0; v <= 3; v++)
            Assert.False(LogFileExists(Commit(v)), $"v{v} should be gone");
        Assert.True(LogFileExists(Commit(4)));
        Assert.True(LogFileExists(Commit(5)));
        Assert.False(LogFileExists($"{1:D20}.crc"));
        Assert.True(LogFileExists($"{4:D20}.crc"));

        // And the table still reads: cleanup, not corruption.
        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs), atVersion: null);
        Assert.Equal(5, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>
    /// #660 review: in-commit timestamps date commits, not sidecars. With a listing that could not date a commit, a
    /// sidecar's own modification time is no age either, and an unreferenced one may be about to be named by a
    /// concurrent checkpoint, so the sidecar sweep does not run.
    /// </summary>
    [Fact]
    public async Task UndatedListing_LeavesUnreferencedSidecars()
    {
        var config = Config();
        await WriteAsync([DaysAgo(10), DaysAgo(9), DaysAgo(8), DaysAgo(2), DaysAgo(0.5), DaysAgo(0.1)], config);
        await CheckpointAtAsync(5);
        UndateEverything();
        string sidecars = Path.Combine(_tempDir, "_delta_log", "_sidecars");
        Directory.CreateDirectory(sidecars);
        string orphan = Path.Combine(sidecars, "unreferenced.parquet");
        File.WriteAllText(orphan, "");
        File.SetLastWriteTimeUtc(orphan, Now.UtcDateTime.AddDays(-30));

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        Assert.Equal(4, deleted);
        Assert.True(File.Exists(orphan));
    }

    [Fact]
    public async Task ACommitAtTheHorizon_Stays()
    {
        var config = Config();
        await WriteAsync([DaysAgo(5), DaysAgo(4), DaysAgo(1), DaysAgo(0.5)], config);
        await CheckpointAtAsync(3);
        UndateEverything();

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 3, Now);

        // v2 is EXACTLY one day old, not older than the horizon.
        Assert.Equal(2, deleted);
        Assert.True(LogFileExists(Commit(2)));
    }

    [Fact]
    public async Task AnOlderCheckpoint_GoesWithItsVersionsCommit()
    {
        var config = Config();
        await WriteAsync([DaysAgo(10), DaysAgo(9), DaysAgo(8), DaysAgo(7), DaysAgo(0.5), DaysAgo(0.1)], config);
        await CheckpointAtAsync(2);
        await CheckpointAtAsync(5);
        UndateEverything();

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        // Commits 0..3 and the checkpoint at 2, dated by commit 2.
        Assert.Equal(5, deleted);
        Assert.False(LogFileExists($"{2:D20}.checkpoint.parquet"));
        Assert.True(LogFileExists($"{5:D20}.checkpoint.parquet"));
    }

    [Fact]
    public async Task AMidLifeEnablement_AgesTheCommitsBeforeItByTheEnablementCommit()
    {
        var config = Config(enablementVersion: 3, enablementTimestamp: DaysAgo(6));
        await WriteAsync([null, null, null, DaysAgo(6), DaysAgo(5), DaysAgo(0.1)], config, enableAt: 3);
        await CheckpointAtAsync(5);
        UndateEverything();

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        // v0..v2 have no timestamp, but they were written before v3, which is six days old.
        Assert.Equal(5, deleted);
        Assert.False(LogFileExists(Commit(0)));
        Assert.True(LogFileExists(Commit(5)));
    }

    [Fact]
    public async Task AMidLifeEnablement_StillWithinTheHorizon_KeepsEverything()
    {
        var config = Config(enablementVersion: 3, enablementTimestamp: DaysAgo(0.5));
        await WriteAsync([null, null, null, DaysAgo(0.5), DaysAgo(0.4), DaysAgo(0.1)], config, enableAt: 3);
        await CheckpointAtAsync(5);
        UndateEverything();

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        Assert.Equal(0, deleted);
        Assert.True(LogFileExists(Commit(0)));
    }

    [Fact]
    public async Task ACommitWithoutAnInCommitTimestamp_DeclinesThePass()
    {
        // Enabled, but no enablement point recorded and the commits carry no timestamp: the shape an enablement
        // left before #598. Nothing can be aged, so nothing goes.
        var config = Config();
        await WriteAsync([null, null, null, null, null, null], config);
        await CheckpointAtAsync(5);
        UndateEverything();

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        Assert.Equal(0, deleted);
        Assert.True(LogFileExists(Commit(0)));
    }

    [Fact]
    public async Task WithoutInCommitTimestamps_AnUndatedListingStillDeclines()
    {
        var config = new Dictionary<string, string> { ["delta.logRetentionDuration"] = "interval 1 days" };
        await WriteAsync([DaysAgo(10), DaysAgo(9), DaysAgo(8), DaysAgo(7), DaysAgo(6), DaysAgo(5)], config);
        await CheckpointAtAsync(5);
        UndateEverything();

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        Assert.Equal(0, deleted);
    }

    [Fact]
    public async Task ADatedListing_IgnoresTheInCommitTimestamps()
    {
        // delta-spark ages by modification time even on a table with in-commit timestamps, so the dated path does
        // too: ancient in-commit timestamps under fresh file times delete nothing.
        var config = Config();
        await WriteAsync([DaysAgo(10), DaysAgo(9), DaysAgo(8), DaysAgo(7), DaysAgo(6), DaysAgo(5)], config);
        await CheckpointAtAsync(5);
        var files = Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"));
        Array.Sort(files, StringComparer.Ordinal);
        for (int i = 0; i < files.Length; i++)
            File.SetLastWriteTimeUtc(files[i], Now.UtcDateTime.AddMinutes(-60 + i));

        int deleted = await LogCleanup.RunAsync(_log, config, latestCheckpointVersion: 5, Now);

        Assert.Equal(0, deleted);
    }
}
