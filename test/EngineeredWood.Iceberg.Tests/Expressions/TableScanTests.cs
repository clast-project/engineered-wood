// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Manifest;
using Ex = EngineeredWood.Iceberg.Expressions.Expressions;

namespace EngineeredWood.Iceberg.Tests.Expressions;

public class TableScanTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TableOperations _ops;
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "region", IcebergType.String, true),
        new NestedField(3, "score", IcebergType.Double, false),
    ]);

    public TableScanTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"iceberg-scan-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
        _ops = new TableOperations(_fs);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // Test data files with known bounds
    private readonly DataFile[] _testFiles =
    [
        // File 1: ids 1-100, region a-m, scores 0-50
        new()
        {
            FilePath = "data/file1.parquet", RecordCount = 100, FileSizeInBytes = 50000,
            NullValueCounts = new Dictionary<int, long> { [1] = 0, [2] = 0, [3] = 0 },
            ColumnLowerBounds = new Dictionary<int, LiteralValue>
                { [1] = LiteralValue.Of(1L), [2] = LiteralValue.Of("a"), [3] = LiteralValue.Of(0.0) },
            ColumnUpperBounds = new Dictionary<int, LiteralValue>
                { [1] = LiteralValue.Of(100L), [2] = LiteralValue.Of("m"), [3] = LiteralValue.Of(50.0) },
        },
        // File 2: ids 101-200, region n-z, scores 50-100
        new()
        {
            FilePath = "data/file2.parquet", RecordCount = 100, FileSizeInBytes = 50000,
            NullValueCounts = new Dictionary<int, long> { [1] = 0, [2] = 0, [3] = 0 },
            ColumnLowerBounds = new Dictionary<int, LiteralValue>
                { [1] = LiteralValue.Of(101L), [2] = LiteralValue.Of("n"), [3] = LiteralValue.Of(50.0) },
            ColumnUpperBounds = new Dictionary<int, LiteralValue>
                { [1] = LiteralValue.Of(200L), [2] = LiteralValue.Of("z"), [3] = LiteralValue.Of(100.0) },
        },
        // File 3: ids 201-300, region a-z, all nulls for score
        new()
        {
            FilePath = "data/file3.parquet", RecordCount = 100, FileSizeInBytes = 50000,
            NullValueCounts = new Dictionary<int, long> { [1] = 0, [2] = 0, [3] = 100 },
            ColumnLowerBounds = new Dictionary<int, LiteralValue>
                { [1] = LiteralValue.Of(201L), [2] = LiteralValue.Of("a") },
            ColumnUpperBounds = new Dictionary<int, LiteralValue>
                { [1] = LiteralValue.Of(300L), [2] = LiteralValue.Of("z") },
        },
    ];

    private TableMetadata CreateMetadata() =>
        TableMetadata.Create(_schema, location: Path.Combine(_tempDir, "table"));

    [Fact]
    public async Task NoFilter_ReturnsAllFiles()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .PlanFiles(_testFiles);
        Assert.Equal(3, result.FilesMatched);
        Assert.Equal(0, result.FilesSkipped);
    }

    [Fact]
    public async Task Filter_Eq_PrunesFiles()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.Equal("id", 50L))
            .PlanFiles(_testFiles);

        Assert.Equal(1, result.FilesMatched);
        Assert.Equal(2, result.FilesSkipped);
        Assert.Equal("data/file1.parquet", result.DataFiles[0].FilePath);
    }

    [Fact]
    public async Task Filter_Gt_PrunesFiles()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.GreaterThan("id", 200L))
            .PlanFiles(_testFiles);

        Assert.Equal(1, result.FilesMatched);
        Assert.Equal(2, result.FilesSkipped);
        Assert.Equal("data/file3.parquet", result.DataFiles[0].FilePath);
    }

    [Fact]
    public async Task Filter_Lt_PrunesFiles()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.LessThan("id", 50L))
            .PlanFiles(_testFiles);

        Assert.Equal(1, result.FilesMatched);
        Assert.Equal(2, result.FilesSkipped);
    }

    [Fact]
    public async Task Filter_String_PrunesFiles()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.Equal("region", "xyz"))
            .PlanFiles(_testFiles);

        // "xyz" between "n"-"z" (file2), between "a"-"z" (file3). Not in file1 "a"-"m".
        Assert.Equal(2, result.FilesMatched);
        Assert.Equal(1, result.FilesSkipped);
    }

    [Fact]
    public async Task Filter_And_NarrowsResults()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.GreaterThanOrEqual("id", 101L))
            .Filter(Ex.LessThanOrEqual("id", 200L))
            .PlanFiles(_testFiles);

        Assert.Equal(1, result.FilesMatched);
        Assert.Equal(2, result.FilesSkipped);
        Assert.Equal("data/file2.parquet", result.DataFiles[0].FilePath);
    }

    [Fact]
    public async Task Filter_IsNull_PrunesFiles()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.IsNull("score"))
            .PlanFiles(_testFiles);

        Assert.Equal(1, result.FilesMatched);
        Assert.Equal(2, result.FilesSkipped);
        Assert.Equal("data/file3.parquet", result.DataFiles[0].FilePath);
    }

    [Fact]
    public async Task Filter_NotNull_PrunesAllNullFiles()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.NotNull("score"))
            .PlanFiles(_testFiles);

        Assert.Equal(2, result.FilesMatched);
        Assert.Equal(1, result.FilesSkipped);
    }

    [Fact]
    public async Task Filter_In_PrunesFiles()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.In("id", 50L, 150L))
            .PlanFiles(_testFiles);

        Assert.Equal(2, result.FilesMatched);
        Assert.Equal(1, result.FilesSkipped);
    }

    [Fact]
    public async Task Filter_UnknownColumn_ReturnsAll()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.Equal("nonexistent", 1))
            .PlanFiles(_testFiles);

        Assert.Equal(3, result.FilesMatched);
    }

    [Fact]
    public async Task EmptyFiles_ReturnsEmpty()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.Equal("id", 1L))
            .PlanFiles([]);

        Assert.Empty(result.DataFiles);
    }

    [Fact]
    public async Task PlanFiles_FromManifests_NoFilter()
    {
        var metadata = CreateMetadata();
        var after = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/v1.parquet", RecordCount = 100, FileSizeInBytes = 5000 },
            new DataFile { FilePath = "data/v2.parquet", RecordCount = 100, FileSizeInBytes = 5000 },
        ]);

        var result = await new TableScan(after, _fs).PlanFilesAsync();
        Assert.Equal(2, result.FilesMatched);
    }

    [Fact]
    public async Task UseSnapshot_ScansSpecificSnapshot()
    {
        var metadata = CreateMetadata();

        var after1 = await _ops.AppendFilesAsync(metadata, [
            new DataFile { FilePath = "data/v1.parquet", RecordCount = 100, FileSizeInBytes = 5000 },
        ]);
        var snap1 = after1.CurrentSnapshotId!.Value;

        var after2 = await _ops.AppendFilesAsync(after1, [
            new DataFile { FilePath = "data/v2.parquet", RecordCount = 100, FileSizeInBytes = 5000 },
        ]);

        var result = await new TableScan(after2, _fs)
            .UseSnapshot(snap1)
            .PlanFilesAsync();

        Assert.Single(result.DataFiles);
        Assert.Equal("data/v1.parquet", result.DataFiles[0].FilePath);
    }

    [Fact]
    public async Task ScanResult_ReportsTotalAndSkipped()
    {
        var result = new TableScan(CreateMetadata(), _fs)
            .Filter(Ex.Equal("id", 50L))
            .PlanFiles(_testFiles);

        Assert.Equal(3, result.TotalFilesScanned);
        Assert.Equal(2, result.FilesSkipped);
        Assert.Equal(1, result.FilesMatched);
    }
}
