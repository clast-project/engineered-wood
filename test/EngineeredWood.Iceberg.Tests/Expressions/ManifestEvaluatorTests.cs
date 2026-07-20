// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Iceberg.Expressions.Expressions;

namespace EngineeredWood.Iceberg.Tests.Expressions;

/// <summary>
/// Iceberg's manifest-level filtering goes through the shared
/// <see cref="StatisticsEvaluator"/> using an Iceberg-specific
/// <see cref="IStatisticsAccessor{T}"/>. These tests exercise that
/// integration end-to-end via <see cref="Iceberg.Expressions.TableScan.PlanFiles"/>.
///
/// Per-predicate evaluation logic lives in
/// <c>EngineeredWood.Expressions.Tests.StatisticsEvaluatorTests</c>; tests
/// here cover Iceberg-specific concerns (field ID translation, schema-based
/// binding, multi-file pruning).
/// </summary>
public class ManifestEvaluatorTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "name", IcebergType.String, true),
        new NestedField(3, "score", IcebergType.Double, false),
    ]);

    public ManifestEvaluatorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"iceberg-eval-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private TableMetadata Meta() =>
        TableMetadata.Create(_schema, location: Path.Combine(_tempDir, "table"));

    private static DataFile File(string path, long recordCount,
        Dictionary<int, LiteralValue>? lower = null,
        Dictionary<int, LiteralValue>? upper = null,
        Dictionary<int, long>? nullCounts = null) =>
        new()
        {
            FilePath = path, RecordCount = recordCount, FileSizeInBytes = 1000,
            ColumnLowerBounds = lower,
            ColumnUpperBounds = upper,
            NullValueCounts = nullCounts,
        };

    [Fact]
    public void Eq_ValueOutsideRange_FilePruned()
    {
        var f = File("a.parquet", 100,
            lower: new() { [1] = LiteralValue.Of(10L) },
            upper: new() { [1] = LiteralValue.Of(100L) },
            nullCounts: new() { [1] = 0 });

        var result = new Iceberg.Expressions.TableScan(Meta(), _fs)
            .Filter(Ex.Equal("id", 200L))
            .PlanFiles([f]);

        Assert.Equal(0, result.FilesMatched);
        Assert.Equal(1, result.FilesSkipped);
    }

    [Fact]
    public void Eq_ValueInRange_FileKept()
    {
        var f = File("a.parquet", 100,
            lower: new() { [1] = LiteralValue.Of(10L) },
            upper: new() { [1] = LiteralValue.Of(100L) });

        var result = new Iceberg.Expressions.TableScan(Meta(), _fs)
            .Filter(Ex.Equal("id", 50L))
            .PlanFiles([f]);

        Assert.Equal(1, result.FilesMatched);
    }

    [Fact]
    public void IsNull_NoNulls_FilePruned()
    {
        var f = File("a.parquet", 100, nullCounts: new() { [3] = 0 });

        var result = new Iceberg.Expressions.TableScan(Meta(), _fs)
            .Filter(Ex.IsNull("score"))
            .PlanFiles([f]);

        Assert.Equal(0, result.FilesMatched);
        Assert.Equal(1, result.FilesSkipped);
    }

    [Fact]
    public void NotNull_AllNulls_FilePruned()
    {
        var f = File("a.parquet", 100, nullCounts: new() { [3] = 100 });

        var result = new Iceberg.Expressions.TableScan(Meta(), _fs)
            .Filter(Ex.NotNull("score"))
            .PlanFiles([f]);

        Assert.Equal(0, result.FilesMatched);
        Assert.Equal(1, result.FilesSkipped);
    }

    [Fact]
    public void StringEq_OutOfRange_FilePruned()
    {
        var f = File("a.parquet", 100,
            lower: new() { [2] = LiteralValue.Of("alice") },
            upper: new() { [2] = LiteralValue.Of("dave") },
            nullCounts: new() { [2] = 0 });

        var result = new Iceberg.Expressions.TableScan(Meta(), _fs)
            .Filter(Ex.Equal("name", "zoe"))
            .PlanFiles([f]);

        Assert.Equal(0, result.FilesMatched);
    }

    [Fact]
    public void In_NoValuesInRange_FilePruned()
    {
        var f = File("a.parquet", 100,
            lower: new() { [1] = LiteralValue.Of(10L) },
            upper: new() { [1] = LiteralValue.Of(20L) });

        var result = new Iceberg.Expressions.TableScan(Meta(), _fs)
            .Filter(Ex.In("id", 1L, 25L))
            .PlanFiles([f]);

        Assert.Equal(0, result.FilesMatched);
    }

    [Fact]
    public void In_SomeValuesInRange_FileKept()
    {
        var f = File("a.parquet", 100,
            lower: new() { [1] = LiteralValue.Of(10L) },
            upper: new() { [1] = LiteralValue.Of(20L) });

        var result = new Iceberg.Expressions.TableScan(Meta(), _fs)
            .Filter(Ex.In("id", 1L, 15L, 25L))
            .PlanFiles([f]);

        Assert.Equal(1, result.FilesMatched);
    }

    [Fact]
    public void NoStats_FileKept()
    {
        // Conservative: with no stats, can't prove file is empty.
        var f = File("a.parquet", 100);

        var result = new Iceberg.Expressions.TableScan(Meta(), _fs)
            .Filter(Ex.Equal("id", 42L))
            .PlanFiles([f]);

        Assert.Equal(1, result.FilesMatched);
    }

    [Fact]
    public void And_OneSubpredicateFalsifies_FilePruned()
    {
        var f = File("a.parquet", 100,
            lower: new() { [1] = LiteralValue.Of(10L) },
            upper: new() { [1] = LiteralValue.Of(100L) });

        var result = new Iceberg.Expressions.TableScan(Meta(), _fs)
            .Filter(Ex.And(
                Ex.GreaterThan("id", 5L),
                Ex.LessThan("id", 5L)))
            .PlanFiles([f]);

        Assert.Equal(0, result.FilesMatched);
    }
}
