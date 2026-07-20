// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests.Manifest;

public class ManifestAvroTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;

    public ManifestAvroTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"iceberg-avro-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task ManifestEntries_Avro_RoundTrip()
    {
        var path = Path.Combine(_tempDir, "test.avro");
        var entries = new List<ManifestEntry>
        {
            new()
            {
                Status = ManifestEntryStatus.Added,
                SnapshotId = 100,
                SequenceNumber = 1,
                FileSequenceNumber = 1,
                DataFile = new DataFile
                {
                    FilePath = "data/file1.parquet",
                    FileFormat = FileFormat.Parquet,
                    RecordCount = 1000,
                    FileSizeInBytes = 50000,
                    ColumnSizes = new Dictionary<int, long> { [1] = 25000, [2] = 25000 },
                    NullValueCounts = new Dictionary<int, long> { [2] = 50 },
                    SplitOffsets = [0, 25000],
                    SortOrderId = 0,
                }
            },
            new()
            {
                Status = ManifestEntryStatus.Added,
                SnapshotId = 100,
                SequenceNumber = 1,
                FileSequenceNumber = 1,
                DataFile = new DataFile
                {
                    FilePath = "data/file2.parquet",
                    FileFormat = FileFormat.Parquet,
                    RecordCount = 2000,
                    FileSizeInBytes = 100000,
                }
            }
        };

        await ManifestIO.WriteManifestAsync(_fs, path, entries);
        var loaded = await ManifestIO.ReadManifestAsync(_fs, path);

        Assert.Equal(2, loaded.Count);

        Assert.Equal("data/file1.parquet", loaded[0].DataFile.FilePath);
        Assert.Equal(1000, loaded[0].DataFile.RecordCount);
        Assert.Equal(FileFormat.Parquet, loaded[0].DataFile.FileFormat);
        Assert.Equal(ManifestEntryStatus.Added, loaded[0].Status);
        Assert.Equal(100, loaded[0].SnapshotId);
        Assert.NotNull(loaded[0].DataFile.ColumnSizes);
        Assert.Equal(25000, loaded[0].DataFile.ColumnSizes![1]);
        Assert.NotNull(loaded[0].DataFile.SplitOffsets);
        Assert.Equal(2, loaded[0].DataFile.SplitOffsets!.Count);
        Assert.Equal(0, loaded[0].DataFile.SortOrderId);

        Assert.Equal("data/file2.parquet", loaded[1].DataFile.FilePath);
        Assert.Equal(2000, loaded[1].DataFile.RecordCount);
        Assert.Null(loaded[1].DataFile.ColumnSizes);
        Assert.Null(loaded[1].DataFile.SplitOffsets);
    }

    [Fact]
    public async Task ManifestListEntries_Avro_RoundTrip()
    {
        var path = Path.Combine(_tempDir, "snap-100.avro");
        var entries = new List<ManifestListEntry>
        {
            new()
            {
                ManifestPath = "metadata/abc.avro",
                ManifestLength = 1234,
                PartitionSpecId = 0,
                Content = ManifestContent.Data,
                SequenceNumber = 1,
                MinSequenceNumber = 1,
                AddedSnapshotId = 100,
                AddedDataFilesCount = 2,
                ExistingDataFilesCount = 0,
                DeletedDataFilesCount = 0,
                AddedRowsCount = 3000,
                ExistingRowsCount = 0,
                DeletedRowsCount = 0,
            },
            new()
            {
                ManifestPath = "metadata/def.avro",
                ManifestLength = 567,
                PartitionSpecId = 0,
                Content = ManifestContent.Deletes,
                SequenceNumber = 2,
                MinSequenceNumber = 2,
                AddedSnapshotId = 200,
                AddedDataFilesCount = 0,
                DeletedDataFilesCount = 1,
                AddedRowsCount = 0,
                DeletedRowsCount = 100,
            }
        };

        await ManifestIO.WriteManifestListAsync(_fs, path, entries);
        var loaded = await ManifestIO.ReadManifestListAsync(_fs, path);

        Assert.Equal(2, loaded.Count);

        Assert.Equal("metadata/abc.avro", loaded[0].ManifestPath);
        Assert.Equal(1234, loaded[0].ManifestLength);
        Assert.Equal(ManifestContent.Data, loaded[0].Content);
        Assert.Equal(100, loaded[0].AddedSnapshotId);
        Assert.Equal(2, loaded[0].AddedDataFilesCount);
        Assert.Equal(3000, loaded[0].AddedRowsCount);

        Assert.Equal(ManifestContent.Deletes, loaded[1].Content);
        Assert.Equal(200, loaded[1].AddedSnapshotId);
    }

    [Fact]
    public async Task DispatchByExtension_Avro()
    {
        var path = Path.Combine(_tempDir, "test.avro");
        var entries = new List<ManifestEntry>
        {
            new()
            {
                Status = ManifestEntryStatus.Added,
                SnapshotId = 1,
                DataFile = new DataFile
                {
                    FilePath = "data/f.parquet",
                    RecordCount = 10,
                    FileSizeInBytes = 500,
                }
            }
        };

        // Uses Avro because of .avro extension
        await ManifestIO.WriteManifestAsync(_fs, path, entries);
        var loaded = await ManifestIO.ReadManifestAsync(_fs, path);

        Assert.Single(loaded);
        Assert.Equal("data/f.parquet", loaded[0].DataFile.FilePath);
    }

    [Fact]
    public async Task DispatchByExtension_Json()
    {
        var path = Path.Combine(_tempDir, "test.manifest.json");
        var entries = new List<ManifestEntry>
        {
            new()
            {
                Status = ManifestEntryStatus.Added,
                SnapshotId = 1,
                DataFile = new DataFile
                {
                    FilePath = "data/f.parquet",
                    RecordCount = 10,
                    FileSizeInBytes = 500,
                }
            }
        };

        // Uses JSON because of .json extension
        await ManifestIO.WriteManifestAsync(_fs, path, entries);
        var loaded = await ManifestIO.ReadManifestAsync(_fs, path);

        Assert.Single(loaded);
    }
}
