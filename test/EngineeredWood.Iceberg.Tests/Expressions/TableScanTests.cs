// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
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

    // #776: a top-level column literally named "s.x" beside a struct "s" with
    // field "x". The backticked name must prune by the top-level column's
    // bounds; the struct path must never borrow them. Nested fields aren't
    // pruned today, so the struct predicate keeps every file (Unknown). If
    // nested pruning lands, the struct case must use field 12's own bounds,
    // which here also admit 500, so this test keeps passing.
    private static readonly Schema DottedSchema = new(0, [
        new NestedField(1, "s.x", IcebergType.Long, false),
        new NestedField(10, "s", new StructType([
            new NestedField(12, "x", IcebergType.Long, false),
        ]), false),
    ]);

    private static readonly DataFile[] DottedFiles =
    [
        // Top-level s.x in 1-100 (rules out 500); struct s.x in 0-1000.
        new()
        {
            FilePath = "data/d1.parquet", RecordCount = 10, FileSizeInBytes = 1000,
            NullValueCounts = new Dictionary<int, long> { [1] = 0, [12] = 0 },
            ColumnLowerBounds = new Dictionary<int, LiteralValue>
                { [1] = LiteralValue.Of(1L), [12] = LiteralValue.Of(0L) },
            ColumnUpperBounds = new Dictionary<int, LiteralValue>
                { [1] = LiteralValue.Of(100L), [12] = LiteralValue.Of(1000L) },
        },
        // Top-level s.x in 101-200 (rules out 500); no stats for the struct field.
        new()
        {
            FilePath = "data/d2.parquet", RecordCount = 10, FileSizeInBytes = 1000,
            NullValueCounts = new Dictionary<int, long> { [1] = 0 },
            ColumnLowerBounds = new Dictionary<int, LiteralValue> { [1] = LiteralValue.Of(101L) },
            ColumnUpperBounds = new Dictionary<int, LiteralValue> { [1] = LiteralValue.Of(200L) },
        },
        // Top-level s.x in 400-600 (admits 500).
        new()
        {
            FilePath = "data/d3.parquet", RecordCount = 10, FileSizeInBytes = 1000,
            NullValueCounts = new Dictionary<int, long> { [1] = 0 },
            ColumnLowerBounds = new Dictionary<int, LiteralValue> { [1] = LiteralValue.Of(400L) },
            ColumnUpperBounds = new Dictionary<int, LiteralValue> { [1] = LiteralValue.Of(600L) },
        },
    ];

    private ScanResult PlanDotted(string sql) =>
        new TableScan(TableMetadata.Create(DottedSchema, location: Path.Combine(_tempDir, "dotted")), _fs)
            .Filter(SparkSqlParser.ParsePredicate(sql))
            .PlanFiles(DottedFiles);

    [Fact]
    public void Filter_BacktickedDottedTopLevelColumn_PrunesByItsOwnBounds()
    {
        var result = PlanDotted("`s.x` = 500");

        Assert.Equal(1, result.FilesMatched);
        Assert.Equal(2, result.FilesSkipped);
        Assert.Equal("data/d3.parquet", result.DataFiles[0].FilePath);
    }

    [Fact]
    public void Filter_StructFieldPath_NeverUsesDottedTopLevelColumnBounds()
    {
        var result = PlanDotted("s.x = 500");

        Assert.Equal(3, result.FilesMatched);
        Assert.Equal(0, result.FilesSkipped);
    }
}
