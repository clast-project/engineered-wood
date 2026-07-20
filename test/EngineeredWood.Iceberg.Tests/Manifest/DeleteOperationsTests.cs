// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests.Manifest;

public class DeleteOperationsTests : IDisposable
{
    private readonly string _warehouse;
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "data", IcebergType.String, false),
    ]);

    public DeleteOperationsTests()
    {
        _warehouse = Path.Combine(Path.GetTempPath(), $"iceberg-del-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_warehouse);
        _fs = new LocalTableFileSystem(_warehouse);
        _ops = new TableOperations(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_warehouse))
            Directory.Delete(_warehouse, recursive: true);
    }

    private TableMetadata CreateTable() =>
        TableMetadata.Create(_schema, location: Path.Combine(_warehouse, "table"));

    [Fact]
    public async Task AppendDeleteFiles_CreatesSnapshot()
    {
        var metadata = CreateTable();
        metadata = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/f1.parquet", RecordCount = 1000, FileSizeInBytes = 50000 },
        ]);

        var deleteFile = new DataFile
        {
            Content = FileContent.PositionDeletes,
            FilePath = "data/f1-deletes.parquet",
            RecordCount = 10,
            FileSizeInBytes = 500,
        };

        var updated = await _ops.AppendDeleteFilesAsync(metadata, [deleteFile]);

        Assert.Equal(2, updated.Snapshots.Count);
        Assert.Equal("delete", updated.Snapshots[1].Summary["operation"]);
        Assert.Equal("1", updated.Snapshots[1].Summary["added-delete-files"]);
    }

    [Fact]
    public async Task AppendDeleteFiles_RejectsDataContent()
    {
        var metadata = CreateTable();
        var dataFile = new DataFile
        {
            Content = FileContent.Data,
            FilePath = "data/f.parquet",
            RecordCount = 100,
            FileSizeInBytes = 5000,
        };

        await Assert.ThrowsAsync<ArgumentException>(() => _ops.AppendDeleteFilesAsync(metadata, [dataFile]).AsTask());
    }

    [Fact]
    public async Task ListDeleteFiles_ReturnsDeleteFiles()
    {
        var metadata = CreateTable();
        metadata = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/f1.parquet", RecordCount = 1000, FileSizeInBytes = 50000 },
        ]);

        var posDelete = new DataFile
        {
            Content = FileContent.PositionDeletes,
            FilePath = "data/pos-del.parquet",
            RecordCount = 5,
            FileSizeInBytes = 200,
        };

        var eqDelete = new DataFile
        {
            Content = FileContent.EqualityDeletes,
            FilePath = "data/eq-del.parquet",
            RecordCount = 3,
            FileSizeInBytes = 150,
        };

        metadata = await _ops.AppendDeleteFilesAsync(metadata, [posDelete, eqDelete]);

        var deleteFiles = await _ops.ListDeleteFilesAsync(metadata);
        Assert.Equal(2, deleteFiles.Count);
        Assert.Contains(deleteFiles, f => f.Content == FileContent.PositionDeletes);
        Assert.Contains(deleteFiles, f => f.Content == FileContent.EqualityDeletes);
    }

    [Fact]
    public async Task ScanFiles_ReturnsBothDataAndDeleteFiles()
    {
        var metadata = CreateTable();
        metadata = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/f1.parquet", RecordCount = 1000, FileSizeInBytes = 50000 },
        ]);

        metadata = await _ops.AppendDeleteFilesAsync(metadata, [
            new DataFile
            {
                Content = FileContent.PositionDeletes,
                FilePath = "data/del.parquet",
                RecordCount = 5,
                FileSizeInBytes = 200,
            },
        ]);

        var (dataFiles, deleteFiles) = await _ops.ScanFilesAsync(metadata);
        Assert.Single(dataFiles);
        Assert.Single(deleteFiles);
        Assert.Equal("data/f1.parquet", dataFiles[0].FilePath);
        Assert.Equal("data/del.parquet", deleteFiles[0].FilePath);
    }

    [Fact]
    public async Task OverwriteFiles_ReplacesDataFiles()
    {
        var metadata = CreateTable();
        var originalFile = new DataFile
        {
            FilePath = "data/original.parquet",
            RecordCount = 1000,
            FileSizeInBytes = 50000,
        };
        metadata = await _ops.AppendFilesAsync(metadata, [originalFile]);

        var newFile = new DataFile
        {
            FilePath = "data/rewritten.parquet",
            RecordCount = 900,
            FileSizeInBytes = 45000,
        };

        var updated = await _ops.OverwriteFilesAsync(metadata, [originalFile], [newFile]);

        Assert.Equal("overwrite", updated.Snapshots.Last().Summary["operation"]);

        var dataFiles = await _ops.ListDataFilesAsync(updated);
        Assert.Single(dataFiles);
        Assert.Equal("data/rewritten.parquet", dataFiles[0].FilePath);
    }

    [Fact]
    public async Task OverwriteFiles_UpdatesRecordCounts()
    {
        var metadata = CreateTable();
        metadata = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/a.parquet", RecordCount = 500, FileSizeInBytes = 25000 },
            new DataFile { FilePath = "data/b.parquet", RecordCount = 500, FileSizeInBytes = 25000 },
        ]);

        var updated = await _ops.OverwriteFilesAsync(
            metadata,
            filesToDelete: [new DataFile { FilePath = "data/a.parquet", RecordCount = 500, FileSizeInBytes = 25000 }],
            filesToAdd: [new DataFile { FilePath = "data/a-v2.parquet", RecordCount = 400, FileSizeInBytes = 20000 }]);

        var summary = updated.Snapshots.Last().Summary;
        Assert.Equal("1", summary["deleted-data-files"]);
        Assert.Equal("1", summary["added-data-files"]);
        Assert.Equal("900", summary["total-records"]);
    }

    [Fact]
    public async Task OverwriteFiles_DeleteOnly()
    {
        var metadata = CreateTable();
        metadata = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/a.parquet", RecordCount = 500, FileSizeInBytes = 25000 },
            new DataFile { FilePath = "data/b.parquet", RecordCount = 500, FileSizeInBytes = 25000 },
        ]);

        var updated = await _ops.OverwriteFilesAsync(
            metadata,
            filesToDelete: [new DataFile { FilePath = "data/a.parquet", RecordCount = 500, FileSizeInBytes = 25000 }],
            filesToAdd: []);

        var dataFiles = await _ops.ListDataFilesAsync(updated);
        Assert.Single(dataFiles);
        Assert.Equal("data/b.parquet", dataFiles[0].FilePath);
    }

    [Fact]
    public async Task AvroManifests_UsedByTableOperations()
    {
        var metadata = CreateTable();
        metadata = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/f1.parquet", RecordCount = 100, FileSizeInBytes = 5000 },
        ]);

        // Verify .avro files were created
        var snapshot = metadata.Snapshots[0];
        Assert.EndsWith(".avro", snapshot.ManifestList);

        var manifestList = await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList);
        Assert.EndsWith(".avro", manifestList[0].ManifestPath);

        // Verify data is readable
        var dataFiles = await _ops.ListDataFilesAsync(metadata);
        Assert.Single(dataFiles);
        Assert.Equal("data/f1.parquet", dataFiles[0].FilePath);
    }
}
