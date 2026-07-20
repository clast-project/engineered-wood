// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.DeltaLake.Table.Stats;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class MultiBatchStatsTests
{
    [Fact]
    public void CollectAll_AggregatesNumRecords()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build()], 2);
        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(3).Append(4).Append(5).Build()], 3);

        string? stats = StatsCollector.Collect([batch1, batch2]);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats!);
        Assert.Equal(5, doc.RootElement.GetProperty("numRecords").GetInt64());
    }

    [Fact]
    public void CollectAll_MergesMinMax()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", Int64Type.Default, false))
            .Build();

        // Batch 1: min=10, max=50
        var batch1 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(10).Append(50).Build()], 2);
        // Batch 2: min=5, max=30
        var batch2 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(5).Append(30).Build()], 2);
        // Batch 3: min=20, max=100
        var batch3 = new RecordBatch(schema,
            [new Int64Array.Builder().Append(20).Append(100).Build()], 2);

        string? stats = StatsCollector.Collect([batch1, batch2, batch3]);
        var doc = JsonDocument.Parse(stats!);

        // Overall min should be 5, max should be 100
        Assert.Equal(5, doc.RootElement.GetProperty("minValues").GetProperty("value").GetInt64());
        Assert.Equal(100, doc.RootElement.GetProperty("maxValues").GetProperty("value").GetInt64());
    }

    [Fact]
    public void CollectAll_SumsNullCounts()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, true))
            .Build();

        var batch1 = new RecordBatch(schema,
            [new StringArray.Builder().Append("a").AppendNull().Build()], 2);
        var batch2 = new RecordBatch(schema,
            [new StringArray.Builder().AppendNull().AppendNull().Append("b").Build()], 3);

        string? stats = StatsCollector.Collect([batch1, batch2]);
        var doc = JsonDocument.Parse(stats!);

        // 1 null in batch1 + 2 nulls in batch2 = 3 total
        Assert.Equal(3, doc.RootElement.GetProperty("nullCount").GetProperty("name").GetInt64());
    }

    [Fact]
    public void CollectAll_MergesStringMinMax()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, false))
            .Build();

        var batch1 = new RecordBatch(schema,
            [new StringArray.Builder().Append("delta").Append("beta").Build()], 2);
        var batch2 = new RecordBatch(schema,
            [new StringArray.Builder().Append("alpha").Append("gamma").Build()], 2);

        string? stats = StatsCollector.Collect([batch1, batch2]);
        var doc = JsonDocument.Parse(stats!);

        Assert.Equal("alpha", doc.RootElement.GetProperty("minValues").GetProperty("name").GetString());
        Assert.Equal("gamma", doc.RootElement.GetProperty("maxValues").GetProperty("name").GetString());
    }

    [Fact]
    public void CollectAll_MultipleColumns()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("score", DoubleType.Default, true))
            .Build();

        var batch1 = new RecordBatch(schema,
            [new Int32Array.Builder().Append(5).Append(10).Build(),
             new DoubleArray.Builder().Append(1.5).AppendNull().Build()], 2);
        var batch2 = new RecordBatch(schema,
            [new Int32Array.Builder().Append(1).Append(3).Build(),
             new DoubleArray.Builder().Append(9.9).Append(0.1).Build()], 2);

        string? stats = StatsCollector.Collect([batch1, batch2]);
        var doc = JsonDocument.Parse(stats!);

        Assert.Equal(4, doc.RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal(1, doc.RootElement.GetProperty("minValues").GetProperty("id").GetInt32());
        Assert.Equal(10, doc.RootElement.GetProperty("maxValues").GetProperty("id").GetInt32());
        Assert.Equal(0.1, doc.RootElement.GetProperty("minValues").GetProperty("score").GetDouble(), 5);
        Assert.Equal(9.9, doc.RootElement.GetProperty("maxValues").GetProperty("score").GetDouble(), 5);
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("score").GetInt64());
    }

    [Fact]
    public void CollectAll_EmptyBatches_ReturnsNull()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var empty1 = new RecordBatch(schema,
            [new Int64Array.Builder().Build()], 0);
        var empty2 = new RecordBatch(schema,
            [new Int64Array.Builder().Build()], 0);

        Assert.Null(StatsCollector.Collect([empty1, empty2]));
    }
}

public class CompactionStatsIntegrationTests : IDisposable
{
    private readonly string _tempDir;

    public CompactionStatsIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cstats_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Compaction_ProducesCorrectAggregateStats()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema,
            new DeltaTableOptions { CheckpointInterval = 0 });

        // Write 3 small files with different value ranges
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(10).Append(20).Build(),
             new StringArray.Builder().Append("beta").AppendNull().Build()], 2)]);

        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(5).Build(),
             new StringArray.Builder().Append("alpha").Build()], 1)]);

        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(100).Build(),
             new StringArray.Builder().Append("gamma").Build()], 1)]);

        Assert.Equal(3, table.CurrentSnapshot.FileCount);

        // Compact
        await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });

        // Verify compacted file has correct aggregated stats
        var compactedFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        Assert.NotNull(compactedFile.Stats);

        var stats = JsonDocument.Parse(compactedFile.Stats!);

        // Total rows: 2 + 1 + 1 = 4
        Assert.Equal(4, stats.RootElement.GetProperty("numRecords").GetInt64());

        // Min id: 5, Max id: 100
        Assert.Equal(5, stats.RootElement.GetProperty("minValues").GetProperty("id").GetInt64());
        Assert.Equal(100, stats.RootElement.GetProperty("maxValues").GetProperty("id").GetInt64());

        // Min value: "alpha", Max value: "gamma"
        Assert.Equal("alpha",
            stats.RootElement.GetProperty("minValues").GetProperty("value").GetString());
        Assert.Equal("gamma",
            stats.RootElement.GetProperty("maxValues").GetProperty("value").GetString());

        // Null count for value: 1 (from first batch)
        Assert.Equal(1,
            stats.RootElement.GetProperty("nullCount").GetProperty("value").GetInt64());
    }
}
