// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// A checkpoint names a version just as a commit does. Metadata cleanup deletes commit files and keeps
/// the checkpoint that subsumes them, so on a table left idle longer than the log retention there may be
/// no commit file left at all — and a reader that only counts commit files then reports a table with no
/// versions, for a table it could have read straight from the checkpoint.
/// </summary>
public class CleanedLogVersionTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TransactionLog _log;

    public CleanedLogVersionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cleaned_{Guid.NewGuid():N}");
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
                Id = "cleaned-log",
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

    /// <summary>Cleanup as Spark performs it once a checkpoint covers the commits and retention has passed.</summary>
    private void DeleteCommitsThrough(long version)
    {
        for (long v = 0; v <= version; v++)
            File.Delete(Path.Combine(_tempDir, DeltaVersion.CommitPath(v)));
    }

    private async Task CheckpointAsync(long version) =>
        await new CheckpointWriter(_fs).WriteCheckpointAsync(
            await SnapshotBuilder.BuildAsync(_log, atVersion: version));

    [Fact]
    public async Task LatestVersion_ComesFromTheCheckpoint_WhenEveryCommitIsCleaned()
    {
        await WriteTableAsync(addCommits: 3);
        await CheckpointAsync(3);
        DeleteCommitsThrough(3);

        Assert.Equal(3L, await _log.GetLatestVersionAsync());
    }

    /// <summary>The end-to-end shape: an idle table cleaned to nothing but its checkpoint still reads.</summary>
    [Fact]
    public async Task Snapshot_OfAFullyCleanedTable_StillBuilds()
    {
        await WriteTableAsync(addCommits: 3);
        await CheckpointAsync(3);
        DeleteCommitsThrough(3);

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(3L, snapshot.Version);
        Assert.Equal(3, snapshot.FileCount);
    }

    /// <summary>Same, with the hint gone too — the listing has to supply both halves.</summary>
    [Fact]
    public async Task Snapshot_OfAFullyCleanedTable_WithNoHint_StillBuilds()
    {
        await WriteTableAsync(addCommits: 3);
        await CheckpointAsync(3);
        DeleteCommitsThrough(3);
        File.Delete(Path.Combine(_tempDir, DeltaVersion.LastCheckpointPath));

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));

        Assert.Equal(3L, snapshot.Version);
        Assert.Equal(3, snapshot.FileCount);
    }

    /// <summary>A commit newer than the checkpoint still wins — the checkpoint is a floor, not the answer.</summary>
    [Fact]
    public async Task LatestVersion_PrefersACommitAboveTheCheckpoint()
    {
        await WriteTableAsync(addCommits: 4);
        await CheckpointAsync(3);
        DeleteCommitsThrough(3);

        Assert.Equal(4L, await _log.GetLatestVersionAsync());

        var snapshot = await SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs));
        Assert.Equal(4L, snapshot.Version);
        Assert.Equal(4, snapshot.FileCount);
    }

    /// <summary>
    /// A multi-part checkpoint names its version in every part file, and a V2 one in a uuid-suffixed
    /// file. Neither is a commit, and both still say the table reached that version.
    /// </summary>
    [Theory]
    [InlineData("checkpoint.parquet")]
    [InlineData("checkpoint.0000000001.0000000002.parquet")]
    [InlineData("checkpoint.0f3e2d1c-0000-0000-0000-000000000000.json")]
    public async Task LatestVersion_ReadsEveryCheckpointNaming(string suffix)
    {
        File.WriteAllBytes(
            Path.Combine(_tempDir, "_delta_log", $"{DeltaVersion.Format(6)}.{suffix}"), []);

        Assert.Equal(6L, await _log.GetLatestVersionAsync());
    }

    /// <summary>Neither a compaction file nor the hint itself names a version.</summary>
    [Fact]
    public async Task LatestVersion_IgnoresCompactionFilesAndTheHint()
    {
        await WriteTableAsync(addCommits: 2);
        File.WriteAllBytes(
            Path.Combine(_tempDir, DeltaVersion.CompactedPath(1, 9)), []);
        File.WriteAllBytes(Path.Combine(_tempDir, DeltaVersion.LastCheckpointPath), []);

        Assert.Equal(2L, await _log.GetLatestVersionAsync());
    }

    [Fact]
    public async Task LatestVersion_IsMinusOne_ForAnEmptyLog()
    {
        Assert.Equal(-1L, await _log.GetLatestVersionAsync());
    }
}
