// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests.Manifest;

public class TableOperationsTests : IDisposable
{
    private readonly string _warehouse;
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "data", IcebergType.String, false),
    ]);

    public TableOperationsTests()
    {
        _warehouse = Path.Combine(Path.GetTempPath(), $"iceberg-ops-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_warehouse);
        _fs = new LocalTableFileSystem(_warehouse);
        _ops = new TableOperations(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_warehouse))
            Directory.Delete(_warehouse, recursive: true);
    }

    private TableMetadata CreateTable()
    {
        var location = Path.Combine(_warehouse, "test_table");
        return TableMetadata.Create(_schema, location: location);
    }

    [Fact]
    public async Task AppendFiles_CreatesSnapshot()
    {
        var metadata = CreateTable();
        Assert.Null(metadata.CurrentSnapshotId);

        var file = new DataFile
        {
            FilePath = "data/file1.parquet",
            RecordCount = 1000,
            FileSizeInBytes = 50000,
        };

        var updated = await _ops.AppendFilesAsync(metadata, [file]);

        Assert.NotNull(updated.CurrentSnapshotId);
        Assert.Single(updated.Snapshots);
        Assert.Equal(1, updated.LastSequenceNumber);
        Assert.Equal("append", updated.Snapshots[0].Summary["operation"]);
        Assert.Equal("1", updated.Snapshots[0].Summary["added-data-files"]);
        Assert.Equal("1000", updated.Snapshots[0].Summary["total-records"]);
    }

    [Fact]
    public async Task AppendFiles_WritesManifestFiles()
    {
        var metadata = CreateTable();
        var file = new DataFile
        {
            FilePath = "data/file1.parquet",
            RecordCount = 500,
            FileSizeInBytes = 25000,
        };

        var updated = await _ops.AppendFilesAsync(metadata, [file]);

        // Verify manifest list exists
        var snapshot = updated.Snapshots[0];
        Assert.True(await _fs.ExistsAsync(snapshot.ManifestList));

        // Verify manifest list contents
        var manifestList = await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList);
        Assert.Single(manifestList);
        Assert.Equal(1, manifestList[0].AddedDataFilesCount);

        // Verify manifest file contents
        var manifestEntries = await ManifestIO.ReadManifestAsync(_fs, manifestList[0].ManifestPath);
        Assert.Single(manifestEntries);
        Assert.Equal("data/file1.parquet", manifestEntries[0].DataFile.FilePath);
        Assert.Equal(ManifestEntryStatus.Added, manifestEntries[0].Status);
    }

    [Fact]
    public async Task AppendFiles_MultipleAppends_AccumulatesSnapshots()
    {
        var metadata = CreateTable();

        var file1 = new DataFile
        {
            FilePath = "data/file1.parquet",
            RecordCount = 1000,
            FileSizeInBytes = 50000,
        };

        var file2 = new DataFile
        {
            FilePath = "data/file2.parquet",
            RecordCount = 2000,
            FileSizeInBytes = 100000,
        };

        var after1 = await _ops.AppendFilesAsync(metadata, [file1]);
        var after2 = await _ops.AppendFilesAsync(after1, [file2]);

        Assert.Equal(2, after2.Snapshots.Count);
        Assert.Equal(2, after2.LastSequenceNumber);
        Assert.Equal("3000", after2.Snapshots[1].Summary["total-records"]);
        Assert.Equal(after2.Snapshots[0].SnapshotId, after2.Snapshots[1].ParentSnapshotId);
    }

    [Fact]
    public async Task AppendFiles_MultipleFiles_InSingleAppend()
    {
        var metadata = CreateTable();

        var files = new[]
        {
            new DataFile { FilePath = "data/a.parquet", RecordCount = 100, FileSizeInBytes = 5000 },
            new DataFile { FilePath = "data/b.parquet", RecordCount = 200, FileSizeInBytes = 10000 },
            new DataFile { FilePath = "data/c.parquet", RecordCount = 300, FileSizeInBytes = 15000 },
        };

        var updated = await _ops.AppendFilesAsync(metadata, files);

        Assert.Equal("3", updated.Snapshots[0].Summary["added-data-files"]);
        Assert.Equal("600", updated.Snapshots[0].Summary["total-records"]);
    }

    [Fact]
    public async Task ListDataFiles_EmptyTable_ReturnsEmpty()
    {
        var metadata = CreateTable();
        var files = await _ops.ListDataFilesAsync(metadata);
        Assert.Empty(files);
    }

    [Fact]
    public async Task ListDataFiles_ReturnsAllFiles()
    {
        var metadata = CreateTable();

        var file1 = new DataFile { FilePath = "data/a.parquet", RecordCount = 100, FileSizeInBytes = 5000 };
        var file2 = new DataFile { FilePath = "data/b.parquet", RecordCount = 200, FileSizeInBytes = 10000 };

        var after1 = await _ops.AppendFilesAsync(metadata, [file1]);
        var after2 = await _ops.AppendFilesAsync(after1, [file2]);

        var dataFiles = await _ops.ListDataFilesAsync(after2);
        Assert.Equal(2, dataFiles.Count);
        Assert.Contains(dataFiles, f => f.FilePath == "data/a.parquet");
        Assert.Contains(dataFiles, f => f.FilePath == "data/b.parquet");
    }

    [Fact]
    public async Task AppendFiles_Empty_Throws()
    {
        var metadata = CreateTable();
        await Assert.ThrowsAsync<ArgumentException>(() => _ops.AppendFilesAsync(metadata, []).AsTask());
    }

    [Fact]
    public async Task EndToEnd_WithFileSystemCatalog()
    {
        var catalog = new FileSystemCatalog(new LocalTableFileSystem(_warehouse));
        var ns = Namespace.Of("db");
        await catalog.CreateNamespaceAsync(ns);

        var id = new TableIdentifier(ns, "events");
        var table = await catalog.CreateTableAsync(id, _schema);

        // Append files
        var file = new DataFile
        {
            FilePath = "data/events-001.parquet",
            RecordCount = 5000,
            FileSizeInBytes = 250000,
        };

        var updated = await _ops.AppendFilesAsync(table.Metadata, [file]);
        await catalog.UpdateTableAsync(id, updated);

        // Reload and verify
        var reloaded = await catalog.LoadTableAsync(id);
        Assert.NotNull(reloaded.Metadata.CurrentSnapshotId);
        Assert.Single(reloaded.Metadata.Snapshots);

        var dataFiles = await _ops.ListDataFilesAsync(reloaded.Metadata);
        Assert.Single(dataFiles);
        Assert.Equal("data/events-001.parquet", dataFiles[0].FilePath);
    }
}
