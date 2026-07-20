// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class ColumnMappingIntegrationTests : IDisposable
{
    private readonly string _tempDir;

    public ColumnMappingIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_colmap_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task CreateTable_NameMode_AssignsMapping()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);

        // Protocol should be upgraded
        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinReaderVersion);
        Assert.Equal(5, table.CurrentSnapshot.Protocol.MinWriterVersion);

        // Configuration should have column mapping mode
        Assert.NotNull(table.CurrentSnapshot.Metadata.Configuration);
        Assert.Equal("name",
            table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.ModeKey]);

        // Schema should have column mapping metadata
        var deltaSchema = table.CurrentSnapshot.Schema;
        foreach (var field in deltaSchema.Fields)
        {
            Assert.NotNull(field.Metadata);
            Assert.True(field.Metadata!.ContainsKey(ColumnMapping.FieldIdKey));
            Assert.True(field.Metadata!.ContainsKey(ColumnMapping.PhysicalNameKey));
        }
    }

    [Fact]
    public async Task WriteAndRead_NameMode_RoundTrips()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);

        // Write data using logical names
        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var values = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var batch = new RecordBatch(schema, [ids, values], 3);

        await table.WriteAsync([batch]);

        // Read back — should get logical names
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(3, readBatches[0].Length);
        Assert.Equal("id", readBatches[0].Schema.FieldsList[0].Name);
        Assert.Equal("value", readBatches[0].Schema.FieldsList[1].Name);

        // Verify data
        var readIds = (Int64Array)readBatches[0].Column(0);
        var readValues = (StringArray)readBatches[0].Column(1);
        Assert.Equal(1L, readIds.GetValue(0));
        Assert.Equal("a", readValues.GetString(0));
    }

    [Fact]
    public async Task WriteAndRead_NameMode_WithProjection()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Field(new Field("score", DoubleType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var names = new StringArray.Builder().Append("alice").Append("bob").Build();
        var scores = new DoubleArray.Builder().Append(95.5).Append(87.3).Build();
        var batch = new RecordBatch(schema, [ids, names, scores], 2);

        await table.WriteAsync([batch]);

        // Read only "id" and "score" columns
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync(columns: ["id", "score"]))
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(2, readBatches[0].ColumnCount);
        Assert.Equal("id", readBatches[0].Schema.FieldsList[0].Name);
        Assert.Equal("score", readBatches[0].Schema.FieldsList[1].Name);
    }

    [Fact]
    public async Task WriteAndRead_NameMode_Reopen()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        // Create and write
        await using (var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name))
        {
            var ids = new Int64Array.Builder().Append(10).Append(20).Build();
            var values = new StringArray.Builder().Append("x").Append("y").Build();
            var batch = new RecordBatch(schema, [ids, values], 2);
            await table.WriteAsync([batch]);
        }

        // Re-open and read
        await using var reopened = await DeltaTable.OpenAsync(fs);

        Assert.Equal(2, reopened.CurrentSnapshot.Protocol.MinReaderVersion);
        Assert.Equal(ColumnMappingMode.Name,
            ColumnMapping.GetMode(reopened.CurrentSnapshot.Metadata.Configuration));

        var readBatches = new List<RecordBatch>();
        await foreach (var b in reopened.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(2, readBatches[0].Length);
        Assert.Equal("id", readBatches[0].Schema.FieldsList[0].Name);
        Assert.Equal("value", readBatches[0].Schema.FieldsList[1].Name);

        var readIds = (Int64Array)readBatches[0].Column(0);
        Assert.Equal(10L, readIds.GetValue(0));
    }

    [Fact]
    public async Task WriteAndRead_NameMode_WithPartitions()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, partitionColumns: ["region"],
            columnMappingMode: ColumnMappingMode.Name);

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var regions = new StringArray.Builder().Append("us").Append("eu").Append("us").Build();
        var batch = new RecordBatch(schema, [ids, regions], 3);

        await table.WriteAsync([batch]);

        // Read back
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        int totalRows = readBatches.Sum(b => b.Length);
        Assert.Equal(3, totalRows);

        foreach (var b in readBatches)
        {
            Assert.Equal(2, b.ColumnCount);
            Assert.Equal("id", b.Schema.FieldsList[0].Name);
            Assert.Equal("region", b.Schema.FieldsList[1].Name);
        }
    }

    [Fact]
    public async Task CreateTable_NoneMode_NoMapping()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        // Default (None) should not upgrade protocol
        await using var table = await DeltaTable.CreateAsync(fs, schema);

        Assert.Equal(1, table.CurrentSnapshot.Protocol.MinReaderVersion);
        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }
}
