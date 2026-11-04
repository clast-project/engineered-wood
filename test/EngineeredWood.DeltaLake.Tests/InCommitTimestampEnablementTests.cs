// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// #598: a commit that turns in-commit timestamps on for an existing table is timestamped, and its metadata records
/// where they begin (<c>delta.inCommitTimestampEnablementVersion</c> / <c>…Timestamp</c>), as delta-spark's
/// <c>InCommitTimestampUtils.getUpdatedMetadataWithICTEnablementInfo</c> does. Without the pair, Spark assumes every
/// commit carries a timestamp and its history and time travel fail with <c>DELTA_MISSING_COMMIT_TIMESTAMP</c>.
/// </summary>
public class InCommitTimestampEnablementTests : IDisposable
{
    private const long Now = 1_800_000_000_000;

    private static readonly Dictionary<string, string> Enabled = new() { [InCommitTimestamp.EnableKey] = "true" };

    private readonly string _tempDir;

    public InCommitTimestampEnablementTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ict_enable_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static MetadataAction Metadata(IReadOnlyDictionary<string, string>? configuration) => new()
    {
        Id = "ict-enable",
        Format = Format.Parquet,
        SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""",
        PartitionColumns = [],
        Configuration = configuration,
    };

    private static AddFile Add(string path) => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string>(),
        Size = 100,
        ModificationTime = 1,
        DataChange = true,
    };

    private static CommitInfo CommitInfoOf(IReadOnlyList<DeltaAction> actions) =>
        Assert.IsType<CommitInfo>(actions[0]);

    private static long? InCommitTimestampOf(IReadOnlyList<DeltaAction> actions) =>
        CommitInfoOf(actions).GetValue("inCommitTimestamp") is { ValueKind: JsonValueKind.Number } value
            ? value.GetInt64()
            : null;

    private static IReadOnlyDictionary<string, string> ConfigurationOf(IReadOnlyList<DeltaAction> actions) =>
        actions.OfType<MetadataAction>().Single().Configuration!;

    // ── the rule, on EnsureCommitInfo ───────────────────────────────────────────────────────────────────

    [Fact]
    public void EnablingMidLife_TimestampsTheCommit_AndStampsTheEnablementPoint()
    {
        var result = InCommitTimestamp.EnsureCommitInfo(
            [Metadata(Enabled)], configuration: null, version: 7, nowMs: Now, previousCommitTimestamp: Now - 50);

        // A base of null with version 7 is unusual, but the rule only asks whether the BASE had the feature.
        Assert.Equal(Now, InCommitTimestampOf(result));
        var configuration = ConfigurationOf(result);
        Assert.Equal("7", configuration[InCommitTimestamp.EnablementVersionKey]);
        Assert.Equal(Now.ToString(), configuration[InCommitTimestamp.EnablementTimestampKey]);
        Assert.Equal((7L, Now), InCommitTimestamp.GetEnablement(configuration));
    }

    [Fact]
    public void TheBasesConfiguration_DoesNotDecide_TheInstalledMetadataDoes()
    {
        // Before #598 the base's configuration decided, so an enabling commit carried no inCommitTimestamp at all.
        var enabling = InCommitTimestamp.EnsureCommitInfo(
            [Metadata(Enabled)], new Dictionary<string, string>(), version: 3, nowMs: Now);
        Assert.Equal(Now, InCommitTimestampOf(enabling));

        // And a commit whose metadata turns them OFF carries none, whatever the base said.
        var disabling = InCommitTimestamp.EnsureCommitInfo(
            [Metadata(new Dictionary<string, string>())], Enabled, version: 3, nowMs: Now);
        Assert.Null(InCommitTimestampOf(disabling));
        Assert.DoesNotContain(InCommitTimestamp.EnablementVersionKey, ConfigurationOf(disabling).Keys);
    }

    [Fact]
    public void Creation_RecordsNoEnablementPoint()
    {
        // Version 0: every commit of the table carries a timestamp, so there is nothing to mark (Spark skips it too).
        var result = InCommitTimestamp.EnsureCommitInfo(
            [Metadata(Enabled)], configuration: null, version: 0, nowMs: Now);

        Assert.Equal(Now, InCommitTimestampOf(result));
        Assert.Null(InCommitTimestamp.GetEnablement(ConfigurationOf(result)));
    }

    [Fact]
    public void AlreadyEnabled_StampsNothingNew()
    {
        var result = InCommitTimestamp.EnsureCommitInfo(
            [Metadata(Enabled)], Enabled, version: 4, nowMs: Now);

        Assert.Equal(Now, InCommitTimestampOf(result));
        Assert.Null(InCommitTimestamp.GetEnablement(ConfigurationOf(result)));
    }

    [Fact]
    public void ReEnabling_OverwritesAStalePair()
    {
        // RESTORE brings back an older version's metadata, pair and all; the commit that re-enables names itself.
        var stale = new Dictionary<string, string>(Enabled)
        {
            [InCommitTimestamp.EnablementVersionKey] = "2",
            [InCommitTimestamp.EnablementTimestampKey] = "1000",
        };

        var result = InCommitTimestamp.EnsureCommitInfo(
            [Metadata(stale)], new Dictionary<string, string>(), version: 9, nowMs: Now);

        Assert.Equal((9L, Now), InCommitTimestamp.GetEnablement(ConfigurationOf(result)));
    }

    [Fact]
    public void ReplacingTheMetadata_KeepsTheBasesEnablementPoint()
    {
        // The commits before the enablement still have no timestamp; new metadata that leaves the pair out does not
        // give them one.
        var baseConfiguration = new Dictionary<string, string>(Enabled)
        {
            [InCommitTimestamp.EnablementVersionKey] = "5",
            [InCommitTimestamp.EnablementTimestampKey] = "1234",
        };

        var result = InCommitTimestamp.EnsureCommitInfo(
            [Metadata(Enabled)], baseConfiguration, version: 11, nowMs: Now);

        Assert.Equal((5L, 1234L), InCommitTimestamp.GetEnablement(ConfigurationOf(result)));
    }

    [Fact]
    public void TheTimestamp_StaysAfterThePreviousCommits()
    {
        // A previous commit stamped by a writer whose clock runs ahead: this one must still come after it.
        var result = InCommitTimestamp.EnsureCommitInfo(
            [Add("a.parquet")], Enabled, version: 4, nowMs: Now, previousCommitTimestamp: Now + 10_000);

        Assert.Equal(Now + 10_001, InCommitTimestampOf(result));
        Assert.Equal(Now + 10_001, CommitInfoOf(result).GetValue("timestamp")!.Value.GetInt64());
    }

    [Fact]
    public void AnEnablementPoint_UsesTheRaisedTimestamp()
    {
        var result = InCommitTimestamp.EnsureCommitInfo(
            [Metadata(Enabled)], configuration: new Dictionary<string, string>(), version: 4, nowMs: Now,
            previousCommitTimestamp: Now + 10_000);

        Assert.Equal((4L, Now + 10_001), InCommitTimestamp.GetEnablement(ConfigurationOf(result)));
    }

    [Fact]
    public void APreBuiltCommitInfo_GetsTheTimestampAndThePair()
    {
        // A host committing through LogCommitter with its own commitInfo is covered too.
        var own = new CommitInfo
        {
            Values = new Dictionary<string, JsonElement>
            {
                ["operation"] = JsonDocument.Parse("\"SET TBLPROPERTIES\"").RootElement.Clone(),
            },
        };

        var result = InCommitTimestamp.EnsureCommitInfo(
            [own, Metadata(Enabled)], new Dictionary<string, string>(), version: 6, nowMs: Now);

        Assert.Equal(Now, InCommitTimestampOf(result));
        Assert.Equal("SET TBLPROPERTIES", CommitInfoOf(result).GetValue("operation")!.Value.GetString());
        Assert.Equal((6L, Now), InCommitTimestamp.GetEnablement(ConfigurationOf(result)));
    }

    [Fact]
    public void APreBuiltTimestamp_IsKept_UnlessItGoesBackwards()
    {
        IReadOnlyList<DeltaAction> own = [InCommitTimestamp.CreateCommitInfo(Now - 5), Add("a.parquet")];

        var kept = InCommitTimestamp.EnsureCommitInfo(own, Enabled, version: 2, nowMs: Now);
        Assert.Same(own, kept);

        var raised = InCommitTimestamp.EnsureCommitInfo(
            own, Enabled, version: 2, nowMs: Now, previousCommitTimestamp: Now);
        Assert.Equal(Now + 1, InCommitTimestampOf(raised));
    }

    [Fact]
    public void ACommitInfoAfterOtherActions_IsMovedFirst_WhenTimestamped()
    {
        // #660 review: a host's [metaData, commitInfo] got its timestamp but kept the commitInfo second, where a
        // reader looking at the first action finds none.
        var own = InCommitTimestamp.CreateCommitInfo(Now, "SET TBLPROPERTIES");
        var result = InCommitTimestamp.EnsureCommitInfo(
            [Metadata(Enabled), own], new Dictionary<string, string>(), version: 6, nowMs: Now);

        Assert.Equal(Now, InCommitTimestampOf(result));
        Assert.IsType<MetadataAction>(result[1]);

        // Even when nothing else about it changes.
        IReadOnlyList<DeltaAction> late = [Add("a.parquet"), own];
        var moved = InCommitTimestamp.EnsureCommitInfo(late, Enabled, version: 6, nowMs: Now);
        Assert.Same(own, moved[0]);
        Assert.IsType<AddFile>(moved[1]);

        // Without in-commit timestamps there is no such rule, and the actions are left alone.
        Assert.Same(late, InCommitTimestamp.EnsureCommitInfo(late, configuration: null, version: 6, nowMs: Now));
    }

    // ── through LogCommitter ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The pair names the version the commit LANDS at, not the one first attempted: a concurrent append takes the
    /// attempted version, the enabling commit rebases past it, and is stamped again for the version it gets.
    /// </summary>
    [Fact]
    public async Task ARebasedEnablement_NamesTheVersionItLandsAt()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["inCommitTimestamp"] },
            Metadata(new Dictionary<string, string>()),
        ]);
        var baseSnapshot = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs), atVersion: null);

        // The concurrent append, stamped far in the future, so the rebased timestamp must move past it.
        long concurrentStamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3_600_000;
        await log.WriteCommitAsync(1, [InCommitTimestamp.CreateCommitInfo(concurrentStamp), Add("other.parquet")]);

        var result = await new LogCommitter(log).CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = baseSnapshot,
            Actions = [Metadata(Enabled)],
            Operation = "SET TBLPROPERTIES",
        });

        Assert.Equal(2, result.Version);
        var committed = await log.ReadCommitAsync(2);
        long stamp = InCommitTimestamp.GetTimestampFromActions(committed)!.Value;
        Assert.Equal(concurrentStamp + 1, stamp);
        Assert.Equal(
            (2L, stamp),
            InCommitTimestamp.GetEnablement(committed.OfType<MetadataAction>().Single().Configuration));
    }

    /// <summary>
    /// #660 review: a table opened exactly at a checkpoint replays nothing, and a checkpoint carries no commitInfo,
    /// so the snapshot had no timestamp and the next commit lost its floor. A previous writer whose clock ran ahead
    /// then let this commit's timestamp go backwards.
    /// </summary>
    [Fact]
    public async Task OpenedAtACheckpoint_TheNextCommitStillFollowsThePreviousTimestamp()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        long ahead = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3_600_000;
        await log.WriteCommitAsync(0,
        [
            InCommitTimestamp.CreateCommitInfo(ahead - 10),
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["inCommitTimestamp"] },
            Metadata(Enabled),
        ]);
        await log.WriteCommitAsync(1, [InCommitTimestamp.CreateCommitInfo(ahead), Add("a.parquet")]);
        var atOne = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs), atVersion: null);
        await new CheckpointWriter(fs).WriteCheckpointAsync(atOne);

        var opened = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs), atVersion: null);
        Assert.Equal(ahead, opened.InCommitTimestamp);

        await new LogCommitter(log).CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = opened,
            Actions = [Add("b.parquet")],
        });
        Assert.Equal(ahead + 1, InCommitTimestamp.GetTimestampFromActions(await log.ReadCommitAsync(2)));
    }

    /// <summary>
    /// #660 review: the same, for a table whose checkpoint comes BEFORE in-commit timestamps are turned on. The
    /// snapshot builder reads a timestamp only for a table that already has them, so the enabling commit resolves
    /// its floor itself, from the commit before it, as Spark floors it by <c>snapshot.timestamp</c>.
    /// </summary>
    [Fact]
    public async Task EnablingAfterACheckpoint_StillFollowsThePreviousCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        long ahead = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3_600_000;
        await log.WriteCommitAsync(0,
        [
            InCommitTimestamp.CreateCommitInfo(ahead - 10, includeInCommitTimestamp: false),
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["inCommitTimestamp"] },
            Metadata(new Dictionary<string, string>()),
        ]);
        await log.WriteCommitAsync(1,
            [InCommitTimestamp.CreateCommitInfo(ahead, includeInCommitTimestamp: false), Add("a.parquet")]);
        var atOne = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs), atVersion: null);
        await new CheckpointWriter(fs).WriteCheckpointAsync(atOne);

        var opened = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs), atVersion: null);
        Assert.Null(opened.InCommitTimestamp);

        await new LogCommitter(log).CommitAsync(new LogCommitRequest
        {
            BaseSnapshot = opened,
            Actions = [Metadata(Enabled)],
        });
        var committed = await log.ReadCommitAsync(2);
        Assert.Equal(ahead + 1, InCommitTimestamp.GetTimestampFromActions(committed));
        Assert.Equal(
            (2L, ahead + 1),
            InCommitTimestamp.GetEnablement(committed.OfType<MetadataAction>().Single().Configuration));
    }
}
