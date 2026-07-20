// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests.Manifest;

public class ManifestIOTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;

    public ManifestIOTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"iceberg-manifest-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task ManifestEntries_RoundTrip()
    {
        var path = Path.Combine(_tempDir, "test.manifest.json");
        var entries = new List<ManifestEntry>
        {
            new()
            {
                Status = ManifestEntryStatus.Added,
                SnapshotId = 100,
                SequenceNumber = 1,
                DataFile = new DataFile
                {
                    FilePath = "data/file1.parquet",
                    FileFormat = FileFormat.Parquet,
                    RecordCount = 1000,
                    FileSizeInBytes = 50000,
                }
            },
            new()
            {
                Status = ManifestEntryStatus.Added,
                SnapshotId = 100,
                SequenceNumber = 1,
                DataFile = new DataFile
                {
                    FilePath = "data/file2.parquet",
                    FileFormat = FileFormat.Parquet,
                    RecordCount = 2000,
                    FileSizeInBytes = 100000,
                    Content = FileContent.Data,
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
        Assert.Equal(2000, loaded[1].DataFile.RecordCount);
    }

    [Fact]
    public async Task ManifestListEntries_RoundTrip()
    {
        var path = Path.Combine(_tempDir, "snap-100.manifest-list.json");
        var entries = new List<ManifestListEntry>
        {
            new()
            {
                ManifestPath = "metadata/abc.manifest.json",
                ManifestLength = 1234,
                AddedSnapshotId = 100,
                Content = ManifestContent.Data,
                SequenceNumber = 1,
                AddedDataFilesCount = 2,
                AddedRowsCount = 3000,
            }
        };

        await ManifestIO.WriteManifestListAsync(_fs, path, entries);
        var loaded = await ManifestIO.ReadManifestListAsync(_fs, path);

        Assert.Single(loaded);
        Assert.Equal("metadata/abc.manifest.json", loaded[0].ManifestPath);
        Assert.Equal(1234, loaded[0].ManifestLength);
        Assert.Equal(100, loaded[0].AddedSnapshotId);
        Assert.Equal(2, loaded[0].AddedDataFilesCount);
        Assert.Equal(3000, loaded[0].AddedRowsCount);
    }
}
