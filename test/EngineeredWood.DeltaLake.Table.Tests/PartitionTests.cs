// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class PartitionTests : IDisposable
{
    private readonly string _tempDir;

    public PartitionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_part_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task WritePartitioned_CreatesSubdirectories()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["region"]);

        // Write data with two different regions
        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var regions = new StringArray.Builder().Append("us").Append("eu").Append("us").Build();
        var batch = new RecordBatch(schema, [ids, regions], 3);

        await table.WriteAsync([batch]);

        // Should have 2 files (one per partition value)
        Assert.Equal(2, table.CurrentSnapshot.FileCount);

        // Verify partition values are stored in the actions
        foreach (var addFile in table.CurrentSnapshot.ActiveFiles.Values)
        {
            Assert.True(addFile.PartitionValues.ContainsKey("region"));
            string region = addFile.PartitionValues["region"];
            Assert.True(region == "us" || region == "eu",
                $"Unexpected region: {region}");

            // Verify file is in a subdirectory
            Assert.Contains("region=", addFile.Path);
        }
    }

    [Fact]
    public async Task WritePartitioned_SpecialCharValue_EscapesPathAndRoundTrips()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["region"]);

        // ':' is escaped by Spark's escapePathName to %3A in the on-disk directory; add.path then
        // URL-encodes that ('%' -> '%25'). Both layers must round-trip.
        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var regions = new StringArray.Builder().Append("a:b").Append("a:b").Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, regions], 2)]);

        var addFile = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        // The partition VALUE in the log stays the raw logical value.
        Assert.Equal("a:b", addFile.PartitionValues["region"]);
        // add.path is URL-encoded: the Hive-escaped '%3A' appears as '%253A'.
        Assert.Contains("region=a%253Ab/", addFile.Path);
        // The physical directory uses Hive escaping ('%3A'), not URL-encoding.
        Assert.True(Directory.Exists(Path.Combine(_tempDir, "region=a%3Ab")),
            "expected a Hive-escaped partition directory on disk");

        // Read back through the (Decode-wrapped) read path — the ':' value reconstructs.
        var readRegions = new List<string>();
        await foreach (var b in table.ReadAllAsync())
        {
            var col = (StringArray)b.Column(1);
            for (int i = 0; i < b.Length; i++)
                readRegions.Add(col.GetString(i));
        }
        Assert.Equal(2, readRegions.Count);
        Assert.All(readRegions, r => Assert.Equal("a:b", r));
    }

    [Fact]
    public async Task ReadPartitioned_RestoresPartitionColumns()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["region"]);

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var regions = new StringArray.Builder().Append("us").Append("eu").Append("us").Build();
        var batch = new RecordBatch(schema, [ids, regions], 3);

        await table.WriteAsync([batch]);

        // Read back — should get partition columns restored
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        int totalRows = readBatches.Sum(b => b.Length);
        Assert.Equal(3, totalRows);

        // Verify schema has both columns
        foreach (var b in readBatches)
        {
            Assert.Equal(2, b.ColumnCount);
            Assert.Equal("id", b.Schema.FieldsList[0].Name);
            Assert.Equal("region", b.Schema.FieldsList[1].Name);

            // Verify region values are correct
            var regionArray = (StringArray)b.Column(1);
            for (int i = 0; i < b.Length; i++)
            {
                string region = regionArray.GetString(i);
                Assert.True(region == "us" || region == "eu");
            }
        }
    }

    [Fact]
    public async Task ReadPartitioned_WithColumnProjection()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("region", StringType.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["region"]);

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var values = new StringArray.Builder().Append("a").Append("b").Build();
        var regions = new StringArray.Builder().Append("us").Append("eu").Build();
        var batch = new RecordBatch(schema, [ids, values, regions], 2);

        await table.WriteAsync([batch]);

        // Read only id and region (region is a partition column)
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync(columns: ["id", "region"]))
            readBatches.Add(b);

        int totalRows = readBatches.Sum(b => b.Length);
        Assert.Equal(2, totalRows);

        foreach (var b in readBatches)
        {
            Assert.Equal(2, b.ColumnCount);
            // Should have id and region, not value
            var fieldNames = b.Schema.FieldsList.Select(f => f.Name).ToList();
            Assert.Contains("id", fieldNames);
            Assert.Contains("region", fieldNames);
        }
    }

    [Fact]
    public async Task WritePartitioned_MultiplePartitionColumns()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("year", Int32Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["year", "region"]);

        var ids = new Int64Array.Builder()
            .Append(1).Append(2).Append(3).Append(4).Build();
        var years = new Int32Array.Builder()
            .Append(2024).Append(2024).Append(2025).Append(2025).Build();
        var regions = new StringArray.Builder()
            .Append("us").Append("eu").Append("us").Append("us").Build();
        var batch = new RecordBatch(schema, [ids, years, regions], 4);

        await table.WriteAsync([batch]);

        // Should have 3 files: (2024,us), (2024,eu), (2025,us)
        Assert.Equal(3, table.CurrentSnapshot.FileCount);

        // Verify partition paths
        foreach (var addFile in table.CurrentSnapshot.ActiveFiles.Values)
        {
            Assert.True(addFile.PartitionValues.ContainsKey("year"));
            Assert.True(addFile.PartitionValues.ContainsKey("region"));
            Assert.Contains("year=", addFile.Path);
            Assert.Contains("region=", addFile.Path);
        }
    }

    [Fact]
    public async Task WritePartitioned_ReadBackDataIntegrity()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", DoubleType.Default, true))
            .Field(new Field("category", StringType.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["category"]);

        var ids = new Int64Array.Builder()
            .Append(1).Append(2).Append(3).Append(4).Append(5).Build();
        var values = new DoubleArray.Builder()
            .Append(1.1).Append(2.2).Append(3.3).Append(4.4).Append(5.5).Build();
        var categories = new StringArray.Builder()
            .Append("A").Append("B").Append("A").Append("B").Append("A").Build();
        var batch = new RecordBatch(schema, [ids, values, categories], 5);

        await table.WriteAsync([batch]);

        // Read back and verify all data
        var allIds = new List<long>();
        var allValues = new List<double>();
        var allCategories = new List<string>();

        await foreach (var b in table.ReadAllAsync())
        {
            var idCol = (Int64Array)b.Column(0);
            var valCol = (DoubleArray)b.Column(1);
            var catCol = (StringArray)b.Column(2);

            for (int i = 0; i < b.Length; i++)
            {
                allIds.Add(idCol.GetValue(i)!.Value);
                allValues.Add(valCol.GetValue(i)!.Value);
                allCategories.Add(catCol.GetString(i));
            }
        }

        Assert.Equal(5, allIds.Count);
        // All original IDs should be present (order may differ)
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, allIds.OrderBy(x => x).ToArray());
        // Values should match (sorted by id)
        var indices = Enumerable.Range(0, allIds.Count).OrderBy(i => allIds[i]).ToList();
        Assert.Equal(1.1, allValues[indices[0]], 5);
        Assert.Equal("A", allCategories[indices[0]]);
        Assert.Equal(2.2, allValues[indices[1]], 5);
        Assert.Equal("B", allCategories[indices[1]]);
    }

    [Fact]
    public async Task NonPartitioned_StillWorksUnchanged()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        // No partition columns
        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var batch = new RecordBatch(schema, [ids], 2);
        await table.WriteAsync([batch]);

        Assert.Equal(1, table.CurrentSnapshot.FileCount);

        // Verify file is in root, not in a subdirectory
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        Assert.DoesNotContain("/", addFile.Path);
        Assert.Empty(addFile.PartitionValues);

        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;
        Assert.Equal(2, totalRows);
    }
}
