// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <c>_last_checkpoint</c> only summarizes what <c>_delta_log</c> already says, so when it is absent,
/// unusable or stale the reader has to find the newest checkpoint by listing. Skipping that fallback is
/// not merely slower: once log cleanup has removed the commits a checkpoint covers, replaying from the
/// surviving commits alone cannot rebuild the table at all.
/// </summary>
public class CheckpointListingFallbackTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TransactionLog _log;

    public CheckpointListingFallbackTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ckptlist_{Guid.NewGuid():N}");
        Directory.CreateDirectory(Path.Combine(_tempDir, "_delta_log"));
        _fs = new LocalTableFileSystem(_tempDir);
        _log = new TransactionLog(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    #region Fixtures

    private const string SchemaJson =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""";

    /// <summary>Commit 0: protocol + metadata. Commits 1..n: one add each, named part-N.parquet.</summary>
    private async Task WriteTableAsync(int addCommits, bool v2Checkpoints = false)
    {
        await _log.WriteCommitAsync(0, [
            v2Checkpoints
                ? new ProtocolAction
                {
                    MinReaderVersion = 3,
                    MinWriterVersion = 7,
                    ReaderFeatures = ["v2Checkpoint"],
                    WriterFeatures = ["v2Checkpoint"],
                }
                : new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "listing-fallback",
                Format = Format.Parquet,
                SchemaString = SchemaJson,
                PartitionColumns = [],
            },
        ]);

        for (int i = 1; i <= addCommits; i++)
        {
            await _log.WriteCommitAsync(i, [
                new AddFile
                {
                    Path = $"part-{i}.parquet",
                    PartitionValues = new Dictionary<string, string>(),
                    Size = 100 * i,
                    ModificationTime = 1700000000000 + i,
                    DataChange = true,
                },
            ]);
        }
    }

    private async Task CheckpointAsync(long version) =>
        await new CheckpointWriter(_fs).WriteCheckpointAsync(
            await SnapshotBuilder.BuildAsync(_log, atVersion: version));

    private async Task V2CheckpointAsync(long version) =>
        await new V2CheckpointWriter(_fs).WriteCheckpointAsync(
            await SnapshotBuilder.BuildAsync(_log, atVersion: version));

    /// <summary>What a write that died part-way leaves: the first half of the file, at its final name.</summary>
    private static void Truncate(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        File.WriteAllBytes(path, bytes.AsSpan(0, bytes.Length / 2).ToArray());
    }

    private void TruncateClassicCheckpoint(long version) =>
        Truncate(Path.Combine(_tempDir, DeltaVersion.CheckpointPath(version)));

    /// <summary>The UUID-named top-level file of the V2 checkpoint at <paramref name="version"/>.</summary>
    private string V2CheckpointFile(long version) =>
        Assert.Single(Directory.GetFiles(
            Path.Combine(_tempDir, "_delta_log"), $"{DeltaVersion.Format(version)}.checkpoint.*-*.*"));

    private string LogPath(string fileName) => Path.Combine(_tempDir, "_delta_log", fileName);

    private void DeleteHint() => File.Delete(LogPath("_last_checkpoint"));

    /// <summary>
    /// What Delta's metadata cleanup does: drop the commits a checkpoint has subsumed. Stopping below the
    /// checkpoint keeps these tests aimed at the checkpoint LOOKUP; cleaning through it as well is covered
    /// by <see cref="CleanedLogVersionTests"/>.
    /// </summary>
    private void DeleteCommitsBefore(long version)
    {
        for (long v = 0; v < version; v++)
            File.Delete(Path.Combine(_tempDir, DeltaVersion.CommitPath(v)));
    }

    private void WriteHint(string content) =>
        File.WriteAllBytes(LogPath("_last_checkpoint"), Encoding.UTF8.GetBytes(content));

    /// <summary>An empty file at a checkpoint name — enough for the finder, which only lists.</summary>
    private void TouchCheckpoint(string fileName) => File.WriteAllBytes(LogPath(fileName), []);

    #endregion

    #region Through SnapshotBuilder

    /// <summary>
    /// The case that makes this a correctness fix rather than an optimization. After cleanup the log no
    /// longer starts at 0, so a reader that ignores the checkpoint has nothing to replay: it cannot see
    /// the protocol or metadata, and every file added before the checkpoint is gone.
    /// </summary>
    [Fact]
    public async Task CleanedLog_WithNoHint_RebuildsFromTheListedCheckpoint()
    {
        await WriteTableAsync(addCommits: 4);
        await CheckpointAsync(3);
        DeleteHint();
        DeleteCommitsBefore(3);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(4L, snapshot.Version);
        Assert.Equal(4, snapshot.FileCount);
    }

    /// <summary>A stale hint — the checkpoint it names has been removed — must fall through the same way.</summary>
    [Fact]
    public async Task StaleHint_NamingAMissingCheckpoint_FallsBackToTheListedOne()
    {
        await WriteTableAsync(addCommits: 4);
        await CheckpointAsync(3);
        DeleteCommitsBefore(3);
        WriteHint("""{"version":99,"size":5}""");

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(4L, snapshot.Version);
        Assert.Equal(4, snapshot.FileCount);
    }

    /// <summary>
    /// A hint above the requested version is no help for time travel, but an older checkpoint is. Commits
    /// 0-1 are removed so the read can only succeed through the checkpoint at version 2.
    /// </summary>
    [Fact]
    public async Task TimeTravelBelowTheHint_UsesTheOlderCheckpoint()
    {
        await WriteTableAsync(addCommits: 4);
        await CheckpointAsync(2);
        await CheckpointAsync(4); // rewrites _last_checkpoint to name version 4
        DeleteCommitsBefore(2);

        var snapshot = await SnapshotBuilder.BuildAsync(
            _log, new CheckpointReader(_fs), atVersion: 3);

        Assert.Equal(3L, snapshot.Version);
        Assert.Equal(3, snapshot.FileCount); // part-1..3, part-4 excluded
    }

    /// <summary>An intact hint is still the fast path: no listing, and the same answer.</summary>
    [Fact]
    public async Task IntactHint_IsStillUsed()
    {
        await WriteTableAsync(addCommits: 4);
        await CheckpointAsync(3);
        DeleteCommitsBefore(3);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(4L, snapshot.Version);
        Assert.Equal(4, snapshot.FileCount);
    }

    /// <summary>No checkpoint anywhere: unchanged full replay, not an error.</summary>
    [Fact]
    public async Task NoCheckpoint_ReplaysTheWholeLog()
    {
        await WriteTableAsync(addCommits: 3);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(3L, snapshot.Version);
        Assert.Equal(3, snapshot.FileCount);
    }

    #endregion

    #region An unreadable checkpoint (#500)

    /// <summary>
    /// The newest checkpoint is truncated and the commits below the older one are gone, so the table opens
    /// only through the older checkpoint and the commits after it. Trying the newest and then replaying
    /// from 0 failed here, although nothing the table needs was missing.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TruncatedNewestCheckpoint_FallsBackToAnOlderOne(bool keepHint)
    {
        await WriteTableAsync(addCommits: 6);
        await CheckpointAsync(2);
        await CheckpointAsync(5);
        DeleteCommitsBefore(2);
        TruncateClassicCheckpoint(5);
        if (!keepHint)
            DeleteHint();

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(6L, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>Time travel takes the same path: the newest checkpoint at or below the target is unreadable.</summary>
    [Fact]
    public async Task TimeTravel_ToAnUnreadableCheckpointsVersion_UsesAnOlderOne()
    {
        await WriteTableAsync(addCommits: 6);
        await CheckpointAsync(2);
        await CheckpointAsync(5);
        await CheckpointAsync(6);
        DeleteCommitsBefore(2);
        TruncateClassicCheckpoint(5);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs), atVersion: 5);

        Assert.Equal(5L, snapshot.Version);
        Assert.Equal(5, snapshot.FileCount);
    }

    /// <summary>
    /// The way EW itself can still leave one: a process that dies while streaming a V2 checkpoint on local
    /// disk never reaches the cleanup that deletes the partial file, so it stays at its final UUID name.
    /// </summary>
    [Fact]
    public async Task TruncatedV2Checkpoint_FallsBackToAnOlderClassicOne()
    {
        await WriteTableAsync(addCommits: 6, v2Checkpoints: true);
        await CheckpointAsync(2);
        await V2CheckpointAsync(5);
        DeleteCommitsBefore(2);
        Truncate(V2CheckpointFile(5));

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(6L, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>
    /// Two forms of one version cover the same state, so when the preferred one is unreadable the other is
    /// as good — even with every commit below that version gone, and no older checkpoint at all.
    /// </summary>
    [Fact]
    public async Task UnreadableClassicCheckpoint_FallsBackToAV2OneAtTheSameVersion()
    {
        await WriteTableAsync(addCommits: 6, v2Checkpoints: true);
        await V2CheckpointAsync(5);
        await CheckpointAsync(5);
        DeleteCommitsBefore(5);
        TruncateClassicCheckpoint(5);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(6L, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>
    /// A replay may cross a compacted range in place of its commits, so a checkpoint below that range is
    /// still a starting point.
    /// </summary>
    [Fact]
    public async Task OlderCheckpoint_IsUsable_AcrossACompactedRange()
    {
        await WriteTableAsync(addCommits: 6);
        await CheckpointAsync(2);
        await CheckpointAsync(5);
        await new LogCompaction(_fs, _log).CompactRangeAsync(3, 4);
        DeleteCommitsBefore(2);
        File.Delete(Path.Combine(_tempDir, DeltaVersion.CommitPath(3)));
        File.Delete(Path.Combine(_tempDir, DeltaVersion.CommitPath(4)));
        TruncateClassicCheckpoint(5);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(6L, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>
    /// A hint naming a checkpoint whose replay would cross a hole is passed over for a listed one that does
    /// not. It used to be read, and the replay after it failed, although the newer checkpoint was there.
    /// </summary>
    [Fact]
    public async Task HintBelowAHole_IsPassedOverForANewerListedCheckpoint()
    {
        await WriteTableAsync(addCommits: 6);
        await CheckpointAsync(2);
        string staleHint = File.ReadAllText(LogPath("_last_checkpoint"));
        await CheckpointAsync(5);
        WriteHint(staleHint);
        DeleteCommitsBefore(5);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(6L, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>
    /// Replay uses a compaction file only when it starts at or after the replay's first version. From the
    /// older checkpoint here it does not, so that checkpoint is no starting point — but a full replay can
    /// use it, and opens the table. Taking the readable older checkpoint shut that replay out.
    /// </summary>
    [Fact]
    public async Task CompactionStartingBelowTheReplay_LeavesTheFullReplay()
    {
        await WriteTableAsync(addCommits: 6);
        await CheckpointAsync(2);
        await CheckpointAsync(5);
        await new LogCompaction(_fs, _log).CompactRangeAsync(1, 5);
        for (long v = 1; v <= 5; v++)
            File.Delete(Path.Combine(_tempDir, DeltaVersion.CommitPath(v)));
        TruncateClassicCheckpoint(5);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(6L, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>
    /// Nor does replay use a compaction file that ends past the target, so one cannot bridge a hole for a
    /// time-travel read. The stale hint below the hole is passed over for the checkpoint at the target.
    /// </summary>
    [Fact]
    public async Task CompactionEndingPastTheTarget_DoesNotRescueAStaleHint()
    {
        await WriteTableAsync(addCommits: 6);
        await CheckpointAsync(2);
        string staleHint = File.ReadAllText(LogPath("_last_checkpoint"));
        await CheckpointAsync(5);
        WriteHint(staleHint);
        await new LogCompaction(_fs, _log).CompactRangeAsync(3, 6);
        File.Delete(Path.Combine(_tempDir, DeltaVersion.CommitPath(4)));

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs), atVersion: 5);

        Assert.Equal(5L, snapshot.Version);
        Assert.Equal(5, snapshot.FileCount);
    }

    /// <summary>
    /// Two complete multi-part checkpoints at one version (a re-checkpoint that split differently) cover
    /// the same state, so the second is tried when the first does not read. The parts here are copies of
    /// one classic checkpoint; the two-part one carries every action twice, which replay reconciles.
    /// </summary>
    [Fact]
    public async Task UnreadableMultiPartCheckpoint_FallsBackToAnotherAtTheSameVersion()
    {
        await WriteTableAsync(addCommits: 6);
        await CheckpointAsync(5);
        string classic = Path.Combine(_tempDir, DeltaVersion.CheckpointPath(5));
        string onePart = Path.Combine(_tempDir, DeltaVersion.CheckpointPartPath(5, 1, 1));
        File.Copy(classic, onePart);
        File.Copy(classic, Path.Combine(_tempDir, DeltaVersion.CheckpointPartPath(5, 1, 2)));
        File.Copy(classic, Path.Combine(_tempDir, DeltaVersion.CheckpointPartPath(5, 2, 2)));
        File.Delete(classic);
        Truncate(onePart);
        DeleteHint();
        DeleteCommitsBefore(5);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(6L, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>
    /// Two writers racing to checkpoint one version leave two UUID-named V2 files. The listing prefers one
    /// of them; when that one does not read, the other covers the same state.
    /// </summary>
    [Fact]
    public async Task UnreadableV2Checkpoint_FallsBackToAnotherV2AtTheSameVersion()
    {
        await WriteTableAsync(addCommits: 6, v2Checkpoints: true);
        await V2CheckpointAsync(5);
        await V2CheckpointAsync(5);
        var both = Directory.GetFiles(
            Path.Combine(_tempDir, "_delta_log"), $"{DeltaVersion.Format(5)}.checkpoint.*-*.json");
        Assert.Equal(2, both.Length);
        Array.Sort(both, StringComparer.Ordinal);
        Truncate(both[0]); // the ordinally smaller JSON body is the preferred one
        DeleteHint();
        DeleteCommitsBefore(5);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(6L, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>
    /// A hint naming a multi-part checkpoint of zero parts read as zero files and counted as a starting
    /// point. With a later commit that carries its own protocol and metadata, the table then opened
    /// holding only that commit's file, the five before it silently gone. The listed checkpoint is used.
    /// </summary>
    [Fact]
    public async Task HintOfZeroParts_DoesNotStartTheTableEmpty()
    {
        await WriteTableAsync(addCommits: 5);
        await CheckpointAsync(5);
        await _log.WriteCommitAsync(6, [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "listing-fallback",
                Format = Format.Parquet,
                SchemaString = SchemaJson,
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "part-6.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 600,
                ModificationTime = 1700000000006,
                DataChange = true,
            },
        ]);
        DeleteCommitsBefore(5);
        WriteHint("""{"version":5,"size":7,"parts":0}""");

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(6L, snapshot.Version);
        Assert.Equal(6, snapshot.FileCount);
    }

    /// <summary>
    /// The read itself refuses a checkpoint with no protocol or metadata, however it came to read
    /// nothing, so a bootstrap moves on to the next candidate rather than starting from it.
    /// </summary>
    [Fact]
    public async Task CheckpointReadYieldingNoProtocolOrMetadata_IsRefused()
    {
        await WriteTableAsync(addCommits: 1);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await new CheckpointReader(_fs).ReadCheckpointAsync(
                new LastCheckpointInfo { Version = 1, Size = 0, Parts = 0 }));

        Assert.Equal(DeltaErrorCodes.StateRecoverError, ex.ErrorCode);
    }

    /// <summary>
    /// A hole above the only checkpoint cannot be replayed around. The error names that hole, as it did
    /// before checkpoints were judged by their replay, rather than claiming version 0 is missing.
    /// </summary>
    [Fact]
    public async Task HoleAboveTheCheckpoint_IsTheOneReported()
    {
        await WriteTableAsync(addCommits: 7);
        await CheckpointAsync(5);
        DeleteCommitsBefore(5);
        File.Delete(Path.Combine(_tempDir, DeltaVersion.CommitPath(6)));

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs)));

        Assert.Equal(DeltaErrorCodes.TruncatedTransactionLog, ex.ErrorCode);
        Assert.Contains("version 6 is missing", ex.Message);
    }

    /// <summary>
    /// With no readable checkpoint left, the error names the one that would have covered the hole and
    /// carries why it could not be read. It used to say no checkpoint covered the hole at all.
    /// </summary>
    [Fact]
    public async Task NoReadableCheckpoint_NamesTheUnreadableOne()
    {
        await WriteTableAsync(addCommits: 6);
        await CheckpointAsync(5);
        DeleteCommitsBefore(5);
        TruncateClassicCheckpoint(5);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs)));

        Assert.Equal(DeltaErrorCodes.TruncatedTransactionLog, ex.ErrorCode);
        Assert.Contains(DeltaVersion.CheckpointPath(5), ex.Message);
        Assert.NotNull(ex.InnerException);
    }

    /// <summary>An unreadable checkpoint the replay never needed is not blamed for a hole above it.</summary>
    [Fact]
    public async Task UnreadableCheckpoint_BelowTheHole_IsNotBlamed()
    {
        await WriteTableAsync(addCommits: 6);
        await CheckpointAsync(2);
        TruncateClassicCheckpoint(2);
        File.Delete(Path.Combine(_tempDir, DeltaVersion.CommitPath(4)));

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs)));

        Assert.Equal(DeltaErrorCodes.TruncatedTransactionLog, ex.ErrorCode);
        Assert.DoesNotContain(DeltaVersion.CheckpointPath(2), ex.Message);
    }

    #endregion

    #region FindLatestCheckpointAsync

    [Fact]
    public async Task Finder_ReturnsNull_WhenThereIsNoCheckpoint()
    {
        await WriteTableAsync(addCommits: 1);

        Assert.Null(await new CheckpointReader(_fs).FindLatestCheckpointAsync(long.MaxValue));
    }

    [Fact]
    public async Task Finder_PicksTheNewestAtOrBelowMaxVersion()
    {
        TouchCheckpoint($"{DeltaVersion.Format(2)}.checkpoint.parquet");
        TouchCheckpoint($"{DeltaVersion.Format(7)}.checkpoint.parquet");

        var reader = new CheckpointReader(_fs);

        Assert.Equal(7L, (await reader.FindLatestCheckpointAsync(long.MaxValue))!.Version);
        Assert.Equal(2L, (await reader.FindLatestCheckpointAsync(6))!.Version);
        Assert.Null(await reader.FindLatestCheckpointAsync(1));
    }

    /// <summary>
    /// A multi-part checkpoint missing a part is a writer caught mid-write. Bootstrapping from the parts
    /// that did land would silently drop every file in the ones that did not, so it must be passed over
    /// for the newest COMPLETE checkpoint instead.
    /// </summary>
    [Fact]
    public async Task Finder_SkipsAnIncompleteMultiPartCheckpoint()
    {
        TouchCheckpoint($"{DeltaVersion.Format(2)}.checkpoint.parquet");
        TouchCheckpoint($"{DeltaVersion.Format(9)}.checkpoint.0000000001.0000000003.parquet");
        TouchCheckpoint($"{DeltaVersion.Format(9)}.checkpoint.0000000002.0000000003.parquet");

        var info = await new CheckpointReader(_fs).FindLatestCheckpointAsync(long.MaxValue);

        Assert.Equal(2L, info!.Version);
        Assert.Null(info.Parts);
    }

    [Fact]
    public async Task Finder_TakesACompleteMultiPartCheckpoint()
    {
        for (int part = 1; part <= 3; part++)
            TouchCheckpoint($"{DeltaVersion.Format(9)}.checkpoint.{part:D10}.0000000003.parquet");

        var info = await new CheckpointReader(_fs).FindLatestCheckpointAsync(long.MaxValue);

        Assert.Equal(9L, info!.Version);
        Assert.Equal(3, info.Parts);
    }

    [Fact]
    public async Task Finder_TakesAV2Checkpoint()
    {
        string name = $"{DeltaVersion.Format(4)}.checkpoint.0f3e2d1c-0000-0000-0000-000000000000.json";
        TouchCheckpoint(name);

        var info = await new CheckpointReader(_fs).FindLatestCheckpointAsync(long.MaxValue);

        Assert.True(info!.IsV2);
        Assert.Equal(4L, info.Version);
        Assert.Equal("_delta_log/" + name, info.V2CheckpointPath);
    }

    /// <summary>
    /// Both forms at one version: prefer the classic file, which every read path here handles directly.
    /// </summary>
    [Fact]
    public async Task Finder_PrefersClassicOverV2AtTheSameVersion()
    {
        TouchCheckpoint($"{DeltaVersion.Format(4)}.checkpoint.parquet");
        TouchCheckpoint($"{DeltaVersion.Format(4)}.checkpoint.0f3e2d1c-0000-0000-0000-000000000000.json");

        var info = await new CheckpointReader(_fs).FindLatestCheckpointAsync(long.MaxValue);

        Assert.False(info!.IsV2);
        Assert.Equal(4L, info.Version);
    }

    /// <summary>Commit files, compaction files and the hint itself are not checkpoints.</summary>
    [Fact]
    public async Task Finder_IgnoresNonCheckpointLogFiles()
    {
        await WriteTableAsync(addCommits: 2);
        TouchCheckpoint($"{DeltaVersion.Format(1)}.{DeltaVersion.Format(2)}.compacted.json");
        WriteHint("""{"version":2,"size":5}""");

        Assert.Null(await new CheckpointReader(_fs).FindLatestCheckpointAsync(long.MaxValue));
    }

    #endregion
}
