// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests;

public class ExpireSnapshotsTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
    ]);

    public ExpireSnapshotsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"iceberg-expire-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
        _ops = new TableOperations(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<TableMetadata> CreateTableWithSnapshotsAsync(int count)
    {
        var metadata = TableMetadata.Create(_schema, location: Path.Combine(_tempDir, "table"));

        for (int i = 0; i < count; i++)
        {
            metadata = await _ops.AppendFilesAsync(metadata, [
                new DataFile
                {
                    FilePath = $"data/file{i}.parquet",
                    RecordCount = (i + 1) * 100,
                    FileSizeInBytes = (i + 1) * 5000,
                }
            ]);
        }

        return metadata;
    }

    [Fact]
    public async Task ExpireOlderThan_RemovesOldSnapshots()
    {
        var metadata = await CreateTableWithSnapshotsAsync(3);
        var cutoff = metadata.Snapshots[1].TimestampMs + 1;

        var result = await new ExpireSnapshots()
            .ExpireOlderThan(cutoff)
            .ApplyAsync(metadata, _fs);

        // Should keep snap 2 (latest/current) and snap 1 (at cutoff boundary)
        // snap 0 should be expired (its timestamp < cutoff)
        Assert.True(result.Metadata.Snapshots.Count < metadata.Snapshots.Count);
        Assert.Equal(metadata.CurrentSnapshotId, result.Metadata.CurrentSnapshotId);
    }

    [Fact]
    public async Task ExpireSnapshotId_RemovesSpecificSnapshot()
    {
        var metadata = await CreateTableWithSnapshotsAsync(3);
        var firstSnapshotId = metadata.Snapshots[0].SnapshotId;

        var result = await new ExpireSnapshots()
            .ExpireSnapshotId(firstSnapshotId)
            .ApplyAsync(metadata, _fs);

        Assert.Equal(2, result.Metadata.Snapshots.Count);
        Assert.DoesNotContain(result.Metadata.Snapshots,
            s => s.SnapshotId == firstSnapshotId);
    }

    [Fact]
    public async Task NeverExpireCurrentSnapshot()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);

        var result = await new ExpireSnapshots()
            .ExpireSnapshotId(metadata.CurrentSnapshotId!.Value)
            .ApplyAsync(metadata, _fs);

        // Current snapshot should not be expired
        Assert.Single(result.Metadata.Snapshots);
        Assert.Equal(metadata.CurrentSnapshotId, result.Metadata.CurrentSnapshotId);
    }

    [Fact]
    public async Task RetainLast_KeepsNNewestSnapshots()
    {
        var metadata = await CreateTableWithSnapshotsAsync(5);

        var result = await new ExpireSnapshots()
            .ExpireOlderThan(long.MaxValue) // Expire everything...
            .RetainLast(2) // ...but keep the last 2
            .ApplyAsync(metadata, _fs);

        Assert.Equal(2, result.Metadata.Snapshots.Count);
        Assert.Equal(metadata.CurrentSnapshotId, result.Metadata.CurrentSnapshotId);
    }

    [Fact]
    public async Task ExpireSnapshots_DeletesManifestFiles()
    {
        var metadata = await CreateTableWithSnapshotsAsync(3);
        var expiredSnap = metadata.Snapshots[0];

        // Verify manifest files exist before expiry
        Assert.True(await _fs.ExistsAsync(expiredSnap.ManifestList));
        var manifestList = await ManifestIO.ReadManifestListAsync(_fs, expiredSnap.ManifestList);
        Assert.True(await _fs.ExistsAsync(manifestList[0].ManifestPath));

        await new ExpireSnapshots()
            .ExpireSnapshotId(expiredSnap.SnapshotId)
            .ApplyAsync(metadata, _fs);

        // Manifest list for expired snapshot should be deleted
        Assert.False(await _fs.ExistsAsync(expiredSnap.ManifestList));
    }

    [Fact]
    public async Task ExpireSnapshots_PreservesSharedManifests()
    {
        var metadata = await CreateTableWithSnapshotsAsync(3);

        // The current snapshot's manifest list references manifests from earlier snapshots
        var currentSnap = metadata.Snapshots.Last();
        var currentManifests = await ManifestIO.ReadManifestListAsync(_fs, currentSnap.ManifestList);

        // Expire the first snapshot
        var result = await new ExpireSnapshots()
            .ExpireSnapshotId(metadata.Snapshots[0].SnapshotId)
            .ApplyAsync(metadata, _fs);

        // Manifests referenced by current snapshot should still exist
        foreach (var m in currentManifests)
            Assert.True(await _fs.ExistsAsync(m.ManifestPath));
    }

    [Fact]
    public async Task ExpireSnapshots_FixesDanglingParentReferences()
    {
        var metadata = await CreateTableWithSnapshotsAsync(3);
        var firstId = metadata.Snapshots[0].SnapshotId;
        var secondSnap = metadata.Snapshots[1];

        Assert.Equal(firstId, secondSnap.ParentSnapshotId);

        var result = await new ExpireSnapshots()
            .ExpireSnapshotId(firstId)
            .ApplyAsync(metadata, _fs);

        // Second snapshot's parent should be cleared
        var updatedSecond = result.Metadata.Snapshots
            .First(s => s.SnapshotId == secondSnap.SnapshotId);
        Assert.Null(updatedSecond.ParentSnapshotId);
    }

    [Fact]
    public async Task ExpireSnapshots_UpdatesSnapshotLog()
    {
        var metadata = await CreateTableWithSnapshotsAsync(3);
        var firstId = metadata.Snapshots[0].SnapshotId;

        var result = await new ExpireSnapshots()
            .ExpireSnapshotId(firstId)
            .ApplyAsync(metadata, _fs);

        Assert.DoesNotContain(result.Metadata.SnapshotLog,
            e => e.SnapshotId == firstId);
    }

    [Fact]
    public async Task ExpireSnapshots_NoOp_ReturnsUnchanged()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);

        var result = await new ExpireSnapshots()
            .ExpireOlderThan(0) // Nothing older than epoch 0
            .ApplyAsync(metadata, _fs);

        Assert.Single(result.Metadata.Snapshots);
        Assert.Empty(result.DeletedFiles);
    }

    [Fact]
    public async Task ExpireSnapshots_WithoutFileIO_SkipsDeletion()
    {
        var metadata = await CreateTableWithSnapshotsAsync(3);
        var expiredSnap = metadata.Snapshots[0];

        var result = await new ExpireSnapshots()
            .ExpireSnapshotId(expiredSnap.SnapshotId)
            .ApplyAsync(metadata, fs: null);

        // Metadata is updated
        Assert.Equal(2, result.Metadata.Snapshots.Count);

        // But files still exist
        Assert.True(await _fs.ExistsAsync(expiredSnap.ManifestList));
    }
}
