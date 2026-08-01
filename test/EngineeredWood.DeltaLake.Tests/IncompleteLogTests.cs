// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// Replay has to cover every version up to the one being built. A version it cannot apply is not a
/// slightly smaller snapshot — it is a snapshot of no version of the table, missing whatever those
/// commits added or removed, with nothing in the result to say so. These used to be skipped in silence;
/// they now name the first uncovered version.
/// </summary>
public class IncompleteLogTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TransactionLog _log;

    public IncompleteLogTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_gap_{Guid.NewGuid():N}");
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

    private async Task WriteTableAsync(int addCommits)
    {
        await _log.WriteCommitAsync(0, [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "incomplete-log",
                Format = Format.Parquet,
                SchemaString =
                    """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
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
                    Size = 100,
                    ModificationTime = 1700000000000 + i,
                    DataChange = true,
                },
            ]);
        }
    }

    private void DeleteCommit(long version) =>
        File.Delete(Path.Combine(_tempDir, DeltaVersion.CommitPath(version)));

    /// <summary>
    /// A hole in the middle. Before, version 2's add simply never appeared and the snapshot claimed to be
    /// version 4 regardless.
    /// </summary>
    [Fact]
    public async Task GapInTheMiddle_Throws_NamingTheMissingVersion()
    {
        await WriteTableAsync(addCommits: 4);
        DeleteCommit(2);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs)));

        Assert.Contains("version 2", ex.Message);
        Assert.Contains("[0..4]", ex.Message);
    }

    /// <summary>
    /// A cleaned log with no checkpoint to recover it. This is what #35's fallback fails to find when the
    /// checkpoint itself is gone too, and the old message for it — "Table has no metadata action" —
    /// described a symptom three steps downstream of the cause.
    /// </summary>
    [Fact]
    public async Task MissingPrefix_WithNoCheckpoint_Throws_NamingTheFirstVersion()
    {
        await WriteTableAsync(addCommits: 4);
        DeleteCommit(0);
        DeleteCommit(1);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs)));

        Assert.Contains("version 0", ex.Message);
    }

    /// <summary>The gap is only a gap below the target: time travel under it still succeeds.</summary>
    [Fact]
    public async Task GapAboveTheRequestedVersion_IsNotAGap()
    {
        await WriteTableAsync(addCommits: 4);
        DeleteCommit(3);

        var snapshot = await SnapshotBuilder.BuildAsync(
            _log, new CheckpointReader(_fs), atVersion: 2);

        Assert.Equal(2L, snapshot.Version);
        Assert.Equal(2, snapshot.FileCount);
    }

    /// <summary>A version past the end of the log is now an error rather than a quietly older snapshot.</summary>
    [Fact]
    public async Task TimeTravelPastTheEndOfTheLog_Throws()
    {
        await WriteTableAsync(addCommits: 2);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(
                _log, new CheckpointReader(_fs), atVersion: 9));

        Assert.Contains("version 3", ex.Message);
    }

    /// <summary>
    /// The check must not fire on the case #35 exists for: cleanup removed the commits, and the
    /// checkpoint covers exactly those versions.
    /// </summary>
    [Fact]
    public async Task CleanedLogCoveredByACheckpoint_DoesNotThrow()
    {
        await WriteTableAsync(addCommits: 4);
        await new CheckpointWriter(_fs).WriteCheckpointAsync(
            await SnapshotBuilder.BuildAsync(_log, atVersion: 3));
        DeleteCommit(0);
        DeleteCommit(1);
        DeleteCommit(2);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(4L, snapshot.Version);
        Assert.Equal(4, snapshot.FileCount);
    }

    /// <summary>An intact log is unaffected — the guard has to stay silent on every healthy table.</summary>
    [Fact]
    public async Task IntactLog_DoesNotThrow()
    {
        await WriteTableAsync(addCommits: 4);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(4L, snapshot.Version);
        Assert.Equal(4, snapshot.FileCount);
    }

    /// <summary>
    /// UpdateAsync has no checkpoint to fall back on — it only moves forward from a snapshot it was
    /// given — so a hole there can only be reported.
    /// </summary>
    [Fact]
    public async Task IncrementalUpdate_OverAGap_Throws()
    {
        await WriteTableAsync(addCommits: 4);
        var atV1 = await SnapshotBuilder.BuildAsync(_log, atVersion: 1);
        DeleteCommit(3);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.UpdateAsync(atV1, _log));

        Assert.Contains("version 3", ex.Message);
    }

    [Fact]
    public async Task IncrementalUpdate_OverAnIntactLog_Succeeds()
    {
        await WriteTableAsync(addCommits: 4);
        var atV1 = await SnapshotBuilder.BuildAsync(_log, atVersion: 1);

        var updated = await SnapshotBuilder.UpdateAsync(atV1, _log);

        Assert.Equal(4L, updated.Version);
        Assert.Equal(4, updated.FileCount);
    }
}
