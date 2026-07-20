// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests;

public class SnapshotRefsTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
    ]);

    public SnapshotRefsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"iceberg-refs-{Guid.NewGuid():N}");
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
        var metadata = TableMetadata.Create(_schema, location: "table");
        for (int i = 0; i < count; i++)
        {
            metadata = await _ops.AppendFilesAsync(metadata, [
                new DataFile { FilePath = $"data/f{i}.parquet", RecordCount = 100, FileSizeInBytes = 5000 }
            ]);
        }
        return metadata;
    }

    // --- Branches ---

    [Fact]
    public async Task SetBranch_CreatesRef()
    {
        var metadata = await CreateTableWithSnapshotsAsync(2);
        var snapId = metadata.Snapshots[0].SnapshotId;

        var updated = SnapshotRefs.SetBranch(metadata, "staging", snapId);

        Assert.True(updated.Refs.ContainsKey("staging"));
        Assert.Equal(snapId, updated.Refs["staging"].SnapshotId);
        Assert.Equal(SnapshotRefType.Branch, updated.Refs["staging"].Type);
    }

    [Fact]
    public async Task SetBranch_WithRetention()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        var snapId = metadata.Snapshots[0].SnapshotId;

        var updated = SnapshotRefs.SetBranch(metadata, "audit",
            snapId, minSnapshotsToKeep: 5, maxSnapshotAgeMs: 86400000);

        var r = updated.Refs["audit"];
        Assert.Equal(5, r.MinSnapshotsToKeep);
        Assert.Equal(86400000, r.MaxSnapshotAgeMs);
    }

    [Fact]
    public async Task SetBranch_Main_UpdatesCurrentSnapshotId()
    {
        var metadata = await CreateTableWithSnapshotsAsync(2);
        var snap1 = metadata.Snapshots[0].SnapshotId;

        var updated = SnapshotRefs.SetBranch(metadata, "main", snap1);

        Assert.Equal(snap1, updated.CurrentSnapshotId);
        Assert.Equal(snap1, updated.Refs["main"].SnapshotId);
    }

    [Fact]
    public async Task SetBranch_InvalidSnapshot_Throws()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);

        Assert.Throws<ArgumentException>(() =>
            SnapshotRefs.SetBranch(metadata, "bad", 99999));
    }

    [Fact]
    public async Task FastForwardBranch_MovesRef()
    {
        var metadata = await CreateTableWithSnapshotsAsync(2);
        var snap1 = metadata.Snapshots[0].SnapshotId;
        var snap2 = metadata.Snapshots[1].SnapshotId;

        metadata = SnapshotRefs.SetBranch(metadata, "dev", snap1);
        var updated = SnapshotRefs.FastForwardBranch(metadata, "dev", snap2);

        Assert.Equal(snap2, updated.Refs["dev"].SnapshotId);
    }

    [Fact]
    public async Task FastForwardBranch_PreservesRetention()
    {
        var metadata = await CreateTableWithSnapshotsAsync(2);
        var snap1 = metadata.Snapshots[0].SnapshotId;
        var snap2 = metadata.Snapshots[1].SnapshotId;

        metadata = SnapshotRefs.SetBranch(metadata, "dev", snap1,
            minSnapshotsToKeep: 10);
        var updated = SnapshotRefs.FastForwardBranch(metadata, "dev", snap2);

        Assert.Equal(10, updated.Refs["dev"].MinSnapshotsToKeep);
    }

    [Fact]
    public async Task FastForwardBranch_NotABranch_Throws()
    {
        var metadata = await CreateTableWithSnapshotsAsync(2);
        var snap1 = metadata.Snapshots[0].SnapshotId;
        var snap2 = metadata.Snapshots[1].SnapshotId;

        metadata = SnapshotRefs.SetTag(metadata, "v1", snap1);

        Assert.Throws<ArgumentException>(() =>
            SnapshotRefs.FastForwardBranch(metadata, "v1", snap2));
    }

    // --- Tags ---

    [Fact]
    public async Task SetTag_CreatesImmutableRef()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        var snapId = metadata.Snapshots[0].SnapshotId;

        var updated = SnapshotRefs.SetTag(metadata, "v1.0", snapId);

        Assert.Equal(SnapshotRefType.Tag, updated.Refs["v1.0"].Type);
        Assert.Equal(snapId, updated.Refs["v1.0"].SnapshotId);
    }

    [Fact]
    public async Task SetTag_WithMaxAge()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        var snapId = metadata.Snapshots[0].SnapshotId;

        var updated = SnapshotRefs.SetTag(metadata, "v1.0", snapId,
            maxRefAgeMs: 7 * 86400000L);

        Assert.Equal(7 * 86400000L, updated.Refs["v1.0"].MaxRefAgeMs);
    }

    [Fact]
    public async Task SetTag_Duplicate_Throws()
    {
        var metadata = await CreateTableWithSnapshotsAsync(2);
        var snap1 = metadata.Snapshots[0].SnapshotId;
        var snap2 = metadata.Snapshots[1].SnapshotId;

        metadata = SnapshotRefs.SetTag(metadata, "v1.0", snap1);

        Assert.Throws<ArgumentException>(() =>
            SnapshotRefs.SetTag(metadata, "v1.0", snap2));
    }

    // --- Remove ---

    [Fact]
    public async Task RemoveRef_DropsRef()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        var snapId = metadata.Snapshots[0].SnapshotId;

        metadata = SnapshotRefs.SetTag(metadata, "v1.0", snapId);
        var updated = SnapshotRefs.RemoveRef(metadata, "v1.0");

        Assert.False(updated.Refs.ContainsKey("v1.0"));
    }

    [Fact]
    public async Task RemoveRef_Main_Throws()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        metadata = SnapshotRefs.SyncMainBranch(metadata);

        Assert.Throws<ArgumentException>(() =>
            SnapshotRefs.RemoveRef(metadata, "main"));
    }

    [Fact]
    public async Task RemoveRef_NotFound_Throws()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);

        Assert.Throws<ArgumentException>(() =>
            SnapshotRefs.RemoveRef(metadata, "nope"));
    }

    // --- Resolve ---

    [Fact]
    public async Task ResolveRef_ReturnsSnapshotId()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        var snapId = metadata.Snapshots[0].SnapshotId;

        metadata = SnapshotRefs.SetTag(metadata, "v1", snapId);

        Assert.Equal(snapId, SnapshotRefs.ResolveRef(metadata, "v1"));
    }

    [Fact]
    public async Task ResolveRef_NotFound_ReturnsNull()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        Assert.Null(SnapshotRefs.ResolveRef(metadata, "nope"));
    }

    // --- SyncMainBranch ---

    [Fact]
    public async Task SyncMainBranch_CreatesMainRef()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        Assert.False(metadata.Refs.ContainsKey("main"));

        var synced = SnapshotRefs.SyncMainBranch(metadata);

        Assert.True(synced.Refs.ContainsKey("main"));
        Assert.Equal(metadata.CurrentSnapshotId, synced.Refs["main"].SnapshotId);
    }

    [Fact]
    public async Task SyncMainBranch_UpdatesExistingMain()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        metadata = SnapshotRefs.SyncMainBranch(metadata);
        var snap1 = metadata.CurrentSnapshotId!.Value;

        // Append creates snapshot 2, current-snapshot-id changes
        metadata = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/new.parquet", RecordCount = 50, FileSizeInBytes = 2500 }
        ]);

        // main ref still points to snap1
        Assert.Equal(snap1, metadata.Refs["main"].SnapshotId);

        // Sync fixes it
        var synced = SnapshotRefs.SyncMainBranch(metadata);
        Assert.Equal(metadata.CurrentSnapshotId, synced.Refs["main"].SnapshotId);
    }

    [Fact]
    public void SyncMainBranch_NoSnapshot_NoOp()
    {
        var metadata = TableMetadata.Create(_schema, location: _tempDir);
        var synced = SnapshotRefs.SyncMainBranch(metadata);
        Assert.Empty(synced.Refs);
    }

    // --- PublishBranch (WAP) ---

    [Fact]
    public async Task PublishBranch_SetsMainToSourceSnapshot()
    {
        var metadata = await CreateTableWithSnapshotsAsync(2);
        var snap1 = metadata.Snapshots[0].SnapshotId;

        // Create a staging branch at snap1
        metadata = SnapshotRefs.SetBranch(metadata, "staging", snap1);
        metadata = SnapshotRefs.SyncMainBranch(metadata);

        // "Publish" staging → main
        var published = SnapshotRefs.PublishBranch(metadata, "staging");

        Assert.Equal(snap1, published.CurrentSnapshotId);
        Assert.Equal(snap1, published.Refs["main"].SnapshotId);
    }

    [Fact]
    public async Task PublishBranch_NotABranch_Throws()
    {
        var metadata = await CreateTableWithSnapshotsAsync(1);
        var snapId = metadata.Snapshots[0].SnapshotId;

        metadata = SnapshotRefs.SetTag(metadata, "v1", snapId);

        Assert.Throws<ArgumentException>(() =>
            SnapshotRefs.PublishBranch(metadata, "v1"));
    }

    // --- JSON round-trip ---

    [Fact]
    public async Task Refs_JsonRoundTrip()
    {
        var metadata = await CreateTableWithSnapshotsAsync(2);
        var snap1 = metadata.Snapshots[0].SnapshotId;
        var snap2 = metadata.Snapshots[1].SnapshotId;

        metadata = SnapshotRefs.SyncMainBranch(metadata);
        metadata = SnapshotRefs.SetBranch(metadata, "staging", snap1,
            minSnapshotsToKeep: 5, maxSnapshotAgeMs: 86400000);
        metadata = SnapshotRefs.SetTag(metadata, "v1.0", snap1, maxRefAgeMs: 604800000);

        var json = EngineeredWood.Iceberg.Serialization.IcebergJsonSerializer.Serialize(metadata);
        var deserialized = EngineeredWood.Iceberg.Serialization.IcebergJsonSerializer.Deserialize<TableMetadata>(json);

        Assert.Equal(3, deserialized.Refs.Count);

        Assert.Equal(SnapshotRefType.Branch, deserialized.Refs["main"].Type);
        Assert.Equal(snap2, deserialized.Refs["main"].SnapshotId);

        Assert.Equal(SnapshotRefType.Branch, deserialized.Refs["staging"].Type);
        Assert.Equal(snap1, deserialized.Refs["staging"].SnapshotId);
        Assert.Equal(5, deserialized.Refs["staging"].MinSnapshotsToKeep);
        Assert.Equal(86400000, deserialized.Refs["staging"].MaxSnapshotAgeMs);

        Assert.Equal(SnapshotRefType.Tag, deserialized.Refs["v1.0"].Type);
        Assert.Equal(snap1, deserialized.Refs["v1.0"].SnapshotId);
        Assert.Equal(604800000, deserialized.Refs["v1.0"].MaxRefAgeMs);
    }

    // --- Integration with TimeTravel ---

    [Fact]
    public async Task TimeTravel_UsingRef()
    {
        var metadata = await CreateTableWithSnapshotsAsync(3);
        var snap1 = metadata.Snapshots[0].SnapshotId;

        metadata = SnapshotRefs.SetTag(metadata, "baseline", snap1);

        // Use the ref to time travel
        var refId = SnapshotRefs.ResolveRef(metadata, "baseline");
        Assert.NotNull(refId);

        var atBaseline = TimeTravel.AtSnapshot(metadata, refId.Value);
        var files = await _ops.ListDataFilesAsync(atBaseline);
        Assert.Single(files);
        Assert.Equal("data/f0.parquet", files[0].FilePath);
    }
}
