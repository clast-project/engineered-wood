// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class PredicatePushdownTests : IDisposable
{
    private readonly string _tempDir;

    public PredicatePushdownTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_pd_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static async Task<int> CountRows(DeltaTable table, EngineeredWood.Expressions.Predicate? filter)
    {
        int total = 0;
        await foreach (var b in table.ReadAllAsync(columns: null, filter))
            total += b.Length;
        return total;
    }

    private static async Task<List<RecordBatch>> Collect(DeltaTable table, EngineeredWood.Expressions.Predicate? filter)
    {
        var list = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync(columns: null, filter))
            list.Add(b);
        return list;
    }

    // ── Stats-based pruning (non-partitioned table) ──

    [Fact]
    public async Task NoFilter_ReadsAllFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Three writes → three files with disjoint ID ranges.
        for (int i = 0; i < 3; i++)
        {
            var b = new Int64Array.Builder();
            for (int j = 0; j < 10; j++) b.Append(i * 100 + j);
            await table.WriteAsync([new RecordBatch(schema, [b.Build()], 10)]);
        }

        Assert.Equal(30, await CountRows(table, filter: null));
    }

    [Fact]
    public async Task EqualityFilter_OutsideAllStats_PrunesAllFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        for (int i = 0; i < 3; i++)
        {
            var b = new Int64Array.Builder();
            for (int j = 0; j < 10; j++) b.Append(i * 100 + j);
            await table.WriteAsync([new RecordBatch(schema, [b.Build()], 10)]);
        }

        // No file's stats can contain id=999.
        var batches = await Collect(table, Ex.Equal("id", 999L));
        Assert.Empty(batches);
    }

    [Fact]
    public async Task RangeFilter_PrunesNonOverlappingFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        for (int i = 0; i < 3; i++)
        {
            var b = new Int64Array.Builder();
            for (int j = 0; j < 10; j++) b.Append(i * 100 + j); // file i has 100i..100i+9
            await table.WriteAsync([new RecordBatch(schema, [b.Build()], 10)]);
        }

        // id >= 100 AND id < 200: only the middle file qualifies.
        var batches = await Collect(table, Ex.And(
            Ex.GreaterThanOrEqual("id", 100L),
            Ex.LessThan("id", 200L)));

        Assert.Single(batches);
        Assert.Equal(10, batches[0].Length);
    }

    [Fact]
    public async Task EqualityFilter_InsideOneFile_KeepsThatFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        for (int i = 0; i < 3; i++)
        {
            var b = new Int64Array.Builder();
            for (int j = 0; j < 10; j++) b.Append(i * 100 + j);
            await table.WriteAsync([new RecordBatch(schema, [b.Build()], 10)]);
        }

        var batches = await Collect(table, Ex.Equal("id", 105L)); // in file 1
        Assert.Single(batches);
    }

    // ── Partition pruning ──

    private async Task<DeltaTable> CreatePartitioned()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Build();

        var table = await DeltaTable.CreateAsync(fs, schema, partitionColumns: ["region"]);

        // Populate three partitions, each with three rows.
        var ids = new Int64Array.Builder()
            .Append(1).Append(2).Append(3).Append(4).Append(5).Append(6)
            .Append(7).Append(8).Append(9).Build();
        var regions = new StringArray.Builder()
            .Append("us").Append("us").Append("us")
            .Append("eu").Append("eu").Append("eu")
            .Append("ap").Append("ap").Append("ap").Build();

        await table.WriteAsync([new RecordBatch(schema, [ids, regions], 9)]);
        return table;
    }

    [Fact]
    public async Task PartitionEquality_KeepsOnlyMatchingPartition()
    {
        await using var table = await CreatePartitioned();

        Assert.Equal(3, table.CurrentSnapshot.FileCount);

        var batches = await Collect(table, Ex.Equal("region", "us"));
        Assert.Equal(3, batches.Sum(b => b.Length));
        // All matching rows have region == "us"
        foreach (var b in batches)
        {
            var regionCol = (StringArray)b.Column(b.Schema.GetFieldIndex("region"));
            for (int i = 0; i < b.Length; i++)
                Assert.Equal("us", regionCol.GetString(i));
        }
    }

    [Fact]
    public async Task PartitionInequality_PrunesNonMatching()
    {
        await using var table = await CreatePartitioned();

        var batches = await Collect(table, Ex.NotEqual("region", "ap"));
        Assert.Equal(6, batches.Sum(b => b.Length));
    }

    [Fact]
    public async Task PartitionIn_KeepsListedPartitions()
    {
        await using var table = await CreatePartitioned();

        var batches = await Collect(table, Ex.In("region", "us", "eu"));
        Assert.Equal(6, batches.Sum(b => b.Length));
    }

    [Fact]
    public async Task PartitionEquality_NoMatchingPartition_PrunesAll()
    {
        await using var table = await CreatePartitioned();

        var batches = await Collect(table, Ex.Equal("region", "antarctica"));
        Assert.Empty(batches);
    }

    [Fact]
    public async Task PartitionAndDataFilter_BothPrune()
    {
        await using var table = await CreatePartitioned();

        // Combined filter that prunes by both partition AND stats:
        //   region == "ap"  → keeps only ap file (ids 7,8,9)
        //   AND id < 5      → ap file's min=7 fails the stats check → AlwaysFalse → file pruned
        // The reader does file-level pruning, so all 3 files should be dropped.
        var batches = await Collect(table, Ex.And(
            Ex.Equal("region", "ap"),
            Ex.LessThan("id", 5L)));

        Assert.Empty(batches);
    }

    // ── ReadAtVersionAsync overload ──

    [Fact]
    public async Task ReadAtVersion_WithFilter_PrunesFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        for (int i = 0; i < 3; i++)
        {
            var b = new Int64Array.Builder();
            for (int j = 0; j < 10; j++) b.Append(i * 100 + j);
            await table.WriteAsync([new RecordBatch(schema, [b.Build()], 10)]);
        }

        long latest = table.CurrentSnapshot.Version;

        int total = 0;
        await foreach (var b in table.ReadAtVersionAsync(latest, columns: null,
            filter: Ex.Equal("id", 999L)))
            total += b.Length;

        Assert.Equal(0, total);
    }
}
