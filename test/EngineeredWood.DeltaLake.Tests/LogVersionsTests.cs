// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <see cref="TransactionLog.ReadVersionsAsync"/> — what the log holds, versus what can be read.
/// </summary>
public class LogVersionsTests : IDisposable
{
    private readonly string _tempDir;

    public LogVersionsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_logver_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private const string SchemaString =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""";

    private static List<DeltaAction> CreateCommit(string id) =>
    [
        new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
        new MetadataAction
        {
            Id = id,
            Format = Format.Parquet,
            SchemaString = SchemaString,
            PartitionColumns = [],
        },
    ];

    private static AddFile Add(string path) => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string>(),
        Size = 100,
        ModificationTime = 1000,
        DataChange = true,
    };

    private string LogPath(string fileName) => Path.Combine(_tempDir, "_delta_log", fileName);

    private void DeleteCommit(long version) =>
        File.Delete(LogPath($"{version:D20}.json"));

    [Fact]
    public async Task EmptyTable_ReportsNothing()
    {
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        Directory.CreateDirectory(Path.Combine(_tempDir, "_delta_log"));

        var versions = await log.ReadVersionsAsync();

        Assert.Equal(-1, versions.LatestVersion);
        Assert.Empty(versions.CommitVersions);
        Assert.Empty(versions.CheckpointVersions);
        Assert.Empty(versions.ReadableVersions);
    }

    [Fact]
    public async Task CommitsOnly_EveryVersionIsReadable()
    {
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        await log.WriteCommitAsync(0, CreateCommit("commits-only"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);
        await log.WriteCommitAsync(2, [Add("b.parquet")]);

        var versions = await log.ReadVersionsAsync();

        Assert.Equal(2, versions.LatestVersion);
        Assert.Equal([0L, 1L, 2L], versions.CommitVersions);
        Assert.Empty(versions.CheckpointVersions);
        Assert.Equal([0L, 1L, 2L], versions.ReadableVersions);
    }

    /// <summary>
    /// The case the old API got wrong. Cleanup deletes the commits a checkpoint subsumes, so the
    /// checkpoint version has no commit file — listing commit files alone loses it entirely.
    /// </summary>
    [Fact]
    public async Task CleanedLog_CheckpointOnlyVersionIsStillReadable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("cleaned"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);
        await log.WriteCommitAsync(2, [Add("b.parquet")]);

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        // Metadata cleanup: the checkpoint at 2 subsumes 0..2, so those commit files go.
        DeleteCommit(0);
        DeleteCommit(1);
        DeleteCommit(2);

        var versions = await log.ReadVersionsAsync();

        Assert.Equal(2, versions.LatestVersion);
        Assert.Empty(versions.CommitVersions);
        Assert.Equal([2L], versions.CheckpointVersions);
        Assert.Equal([2L], versions.ReadableVersions);

        // And it really is readable — the claim is not just bookkeeping.
        var rebuilt = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));
        Assert.Equal(2, rebuilt.Version);
        Assert.Equal(2, rebuilt.FileCount);
    }

    /// <summary>
    /// The mirror error: a commit file whose predecessors are gone, with no checkpoint covering them,
    /// names a version nothing can reconstruct. It must be listed as a commit and NOT as readable.
    /// </summary>
    [Fact]
    public async Task OrphanedCommits_AreListedButNotReadable()
    {
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));

        await log.WriteCommitAsync(0, CreateCommit("orphaned"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);
        await log.WriteCommitAsync(2, [Add("b.parquet")]);

        // No checkpoint was ever written, and the base of the log is deleted anyway.
        DeleteCommit(0);
        DeleteCommit(1);

        var versions = await log.ReadVersionsAsync();

        Assert.Equal(2, versions.LatestVersion);
        Assert.Equal([2L], versions.CommitVersions);
        Assert.Empty(versions.CheckpointVersions);
        Assert.Empty(versions.ReadableVersions);

        // Confirming the emptiness is the truth: building it really does fail.
        await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(log));
    }

    /// <summary>
    /// Readable versions are not guaranteed contiguous: a hole in the middle of the log leaves a
    /// readable range either side of it.
    /// </summary>
    [Fact]
    public async Task HoleInTheMiddle_LeavesTwoReadableRanges()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("hole"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);
        await log.WriteCommitAsync(2, [Add("b.parquet")]);

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        await log.WriteCommitAsync(3, [Add("c.parquet")]);
        await log.WriteCommitAsync(4, [Add("d.parquet")]);

        // A second checkpoint above the hole, written while the log is still whole.
        var atFour = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(atFour);

        // Punch a hole between them: 3 is gone.
        DeleteCommit(3);

        var versions = await log.ReadVersionsAsync();

        Assert.Equal(4, versions.LatestVersion);
        Assert.Equal([0L, 1L, 2L, 4L], versions.CommitVersions);
        Assert.Equal([2L, 4L], versions.CheckpointVersions);

        // Two disjoint ranges: 0..2 below the hole, 4 above it on its own checkpoint. Version 3 is
        // gone and nothing reaches it — this is the case that makes ReadableVersions non-contiguous.
        Assert.Equal([0L, 1L, 2L, 4L], versions.ReadableVersions);
        Assert.DoesNotContain(3L, versions.ReadableVersions);

        // Both ends really are buildable, and 3 really is not.
        Assert.Equal(4, (await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs))).Version);
        Assert.Equal(2, (await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs), atVersion: 2)).Version);
        await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs), atVersion: 3));
    }

    /// <summary>
    /// A checkpoint carries replay forward across the commits that follow it unbroken, even when its
    /// own commit file and everything before it is gone.
    /// </summary>
    [Fact]
    public async Task CheckpointAnchorsTheCommitsAfterIt()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("anchor"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        await log.WriteCommitAsync(2, [Add("b.parquet")]);
        await log.WriteCommitAsync(3, [Add("c.parquet")]);

        DeleteCommit(0);
        DeleteCommit(1);

        var versions = await log.ReadVersionsAsync();

        Assert.Equal([2L, 3L], versions.CommitVersions);
        Assert.Equal([1L], versions.CheckpointVersions);
        Assert.Equal([1L, 2L, 3L], versions.ReadableVersions);

        var rebuilt = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));
        Assert.Equal(3, rebuilt.Version);
    }

    /// <summary>
    /// A multi-part checkpoint missing a part is NOT a checkpoint: a writer that died midway leaves a
    /// prefix, and bootstrapping from it would silently drop the files in the parts that never landed.
    /// The completeness rule used to live only at the selection site, so a new listing API could
    /// easily have inherited the wrong answer.
    /// </summary>
    [Fact]
    public async Task TornMultiPartCheckpoint_IsNotCountedAsACheckpoint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("torn"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        string logDir = Path.Combine(_tempDir, "_delta_log");

        // Declare three parts and provide two. Contents do not matter — the listing decides on names.
        // Written synchronously: net472 has no File.WriteAllTextAsync, and these are one byte each.
        File.WriteAllText(
            Path.Combine(logDir, $"{1:D20}.checkpoint.0000000001.0000000003.parquet"), "x");
        File.WriteAllText(
            Path.Combine(logDir, $"{1:D20}.checkpoint.0000000002.0000000003.parquet"), "x");

        var torn = await log.ReadVersionsAsync();
        Assert.Empty(torn.CheckpointVersions);

        // Still readable, but on the strength of its commit files rather than the torn checkpoint.
        Assert.Equal([0L, 1L], torn.ReadableVersions);

        // The final part lands: now it counts.
        File.WriteAllText(
            Path.Combine(logDir, $"{1:D20}.checkpoint.0000000003.0000000003.parquet"), "x");

        var complete = await log.ReadVersionsAsync();
        Assert.Equal([1L], complete.CheckpointVersions);
    }

    /// <summary>
    /// The contradiction that motivated this: on a cleaned table the commit-file enumerator reports a
    /// maximum below what <see cref="TransactionLog.GetLatestVersionAsync"/> reports, or nothing at
    /// all. Both are still true of what they measure — the new API is the one that reconciles them.
    /// </summary>
    [Fact]
    public async Task ListVersionsAsync_AndGetLatestVersion_StillDisagree_ButReadVersionsReconciles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, CreateCommit("disagree"));
        await log.WriteCommitAsync(1, [Add("a.parquet")]);

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        DeleteCommit(0);
        DeleteCommit(1);

        var listed = new List<long>();
        await foreach (long v in log.ListVersionsAsync())
            listed.Add(v);

        Assert.Empty(listed);                                        // no commit files survive
        Assert.Equal(1, await log.GetLatestVersionAsync());          // the checkpoint names version 1

        var versions = await log.ReadVersionsAsync();
        Assert.Equal(1, versions.LatestVersion);
        Assert.Equal([1L], versions.ReadableVersions);
    }
}
