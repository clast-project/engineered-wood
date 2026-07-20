// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests;

public class TimeTravelTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
    ]);

    public TimeTravelTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"iceberg-tt-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
        _ops = new TableOperations(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<TableMetadata> CreateTableWithSnapshotsAsync(
        Action<long>? onSnap1 = null, Action<long>? onSnap2 = null, Action<long>? onSnap3 = null)
    {
        var metadata = TableMetadata.Create(_schema, location: "table");

        var m1 = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "f1.parquet", RecordCount = 100, FileSizeInBytes = 5000 }
        ]);
        onSnap1?.Invoke(m1.CurrentSnapshotId!.Value);

        var m2 = await _ops.AppendFilesAsync(m1, [
            new DataFile { FilePath = "f2.parquet", RecordCount = 200, FileSizeInBytes = 10000 }
        ]);
        onSnap2?.Invoke(m2.CurrentSnapshotId!.Value);

        var m3 = await _ops.AppendFilesAsync(m2, [
            new DataFile { FilePath = "f3.parquet", RecordCount = 300, FileSizeInBytes = 15000 }
        ]);
        onSnap3?.Invoke(m3.CurrentSnapshotId!.Value);

        return m3;
    }

    [Fact]
    public async Task AtSnapshot_ReturnsMetadataAtGivenSnapshot()
    {
        long snap1 = 0;
        var metadata = await CreateTableWithSnapshotsAsync(onSnap1: id => snap1 = id);

        var atSnap1 = TimeTravel.AtSnapshot(metadata, snap1);

        Assert.Equal(snap1, atSnap1.CurrentSnapshotId);
        var files = await _ops.ListDataFilesAsync(atSnap1);
        Assert.Single(files);
        Assert.Equal("f1.parquet", files[0].FilePath);
    }

    [Fact]
    public async Task AtSnapshot_MiddleSnapshot_ShowsAccumulatedFiles()
    {
        long snap2 = 0;
        var metadata = await CreateTableWithSnapshotsAsync(onSnap2: id => snap2 = id);

        var atSnap2 = TimeTravel.AtSnapshot(metadata, snap2);
        var files = await _ops.ListDataFilesAsync(atSnap2);
        Assert.Equal(2, files.Count);
    }

    [Fact]
    public async Task AtSnapshot_InvalidId_Throws()
    {
        var metadata = await CreateTableWithSnapshotsAsync();
        Assert.Throws<ArgumentException>(() => TimeTravel.AtSnapshot(metadata, 999));
    }

    [Fact]
    public async Task AsOfTimestamp_ReturnsLatestSnapshotBeforeTime()
    {
        long snap2 = 0;
        var metadata = await CreateTableWithSnapshotsAsync(onSnap2: id => snap2 = id);

        var snap2Ts = metadata.Snapshots.First(s => s.SnapshotId == snap2).TimestampMs;
        var atTime = TimeTravel.AsOfTimestamp(metadata, snap2Ts);

        Assert.Equal(snap2, atTime.CurrentSnapshotId);
    }

    [Fact]
    public async Task AsOfTimestamp_BeforeAllSnapshots_Throws()
    {
        var metadata = await CreateTableWithSnapshotsAsync();
        Assert.Throws<ArgumentException>(() => TimeTravel.AsOfTimestamp(metadata, 0));
    }

    [Fact]
    public async Task SnapshotHistory_ReturnsOrderedChain()
    {
        long snap1 = 0, snap2 = 0, snap3 = 0;
        var metadata = await CreateTableWithSnapshotsAsync(
            onSnap1: id => snap1 = id,
            onSnap2: id => snap2 = id,
            onSnap3: id => snap3 = id);

        var history = TimeTravel.SnapshotHistory(metadata);

        Assert.Equal(3, history.Count);
        Assert.Equal(snap3, history[0].SnapshotId);
        Assert.Equal(snap2, history[1].SnapshotId);
        Assert.Equal(snap1, history[2].SnapshotId);
    }

    [Fact]
    public void SnapshotHistory_EmptyTable_ReturnsEmpty()
    {
        var metadata = TableMetadata.Create(_schema, location: _tempDir);
        Assert.Empty(TimeTravel.SnapshotHistory(metadata));
    }

    [Fact]
    public async Task Rollback_SetsCurrentSnapshotToPrevious()
    {
        long snap1 = 0;
        var metadata = await CreateTableWithSnapshotsAsync(onSnap1: id => snap1 = id);

        var rolledBack = TimeTravel.Rollback(metadata, snap1);

        Assert.Equal(snap1, rolledBack.CurrentSnapshotId);
        Assert.Equal(3, rolledBack.Snapshots.Count); // all snapshots preserved
        var files = await _ops.ListDataFilesAsync(rolledBack);
        Assert.Single(files);
    }

    [Fact]
    public async Task Rollback_InvalidSnapshot_Throws()
    {
        var metadata = await CreateTableWithSnapshotsAsync();
        Assert.Throws<ArgumentException>(() => TimeTravel.Rollback(metadata, 999));
    }

    [Fact]
    public async Task CherryPick_CreatesNewSnapshotFromExisting()
    {
        long snap1 = 0, snap3 = 0;
        var metadata = await CreateTableWithSnapshotsAsync(
            onSnap1: id => snap1 = id,
            onSnap3: id => snap3 = id);

        // Roll back to snap1, then cherry-pick snap3's manifest
        var rolledBack = TimeTravel.Rollback(metadata, snap1);
        var cherryPicked = TimeTravel.CherryPick(rolledBack, snap3);

        Assert.Equal(4, cherryPicked.Snapshots.Count);
        Assert.NotEqual(snap3, cherryPicked.CurrentSnapshotId);

        var latest = cherryPicked.Snapshots.First(
            s => s.SnapshotId == cherryPicked.CurrentSnapshotId);
        Assert.Equal("replace", latest.Summary["operation"]);
        Assert.Equal(snap3.ToString(), latest.Summary["cherry-pick-snapshot-id"]);
    }
}
