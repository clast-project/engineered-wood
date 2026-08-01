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
    private async Task WriteTableAsync(int addCommits)
    {
        await _log.WriteCommitAsync(0, [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
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
