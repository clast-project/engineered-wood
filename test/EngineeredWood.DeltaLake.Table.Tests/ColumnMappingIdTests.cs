// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class ColumnMappingIdTests : IDisposable
{
    private readonly string _tempDir;

    public ColumnMappingIdTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cmid_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task CreateTable_IdMode_AssignsMapping()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Id);

        // Protocol should be upgraded
        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinReaderVersion);
        Assert.Equal(5, table.CurrentSnapshot.Protocol.MinWriterVersion);

        // Configuration should have id mode
        Assert.Equal("id",
            table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.ModeKey]);

        // Schema should have field IDs
        var deltaSchema = table.CurrentSnapshot.Schema;
        Assert.NotNull(deltaSchema.Fields[0].Metadata);
        Assert.True(deltaSchema.Fields[0].Metadata!.ContainsKey(ColumnMapping.FieldIdKey));
        Assert.Equal("1", deltaSchema.Fields[0].Metadata![ColumnMapping.FieldIdKey]);
        Assert.Equal("2", deltaSchema.Fields[1].Metadata![ColumnMapping.FieldIdKey]);
    }

    [Fact]
    public async Task WriteAndRead_IdMode_RoundTrips()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Id);

        // Write data
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
    public async Task WriteAndRead_IdMode_Reopen()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        await using (var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Id))
        {
            var ids = new Int64Array.Builder().Append(10).Append(20).Build();
            var values = new StringArray.Builder().Append("x").Append("y").Build();
            var batch = new RecordBatch(schema, [ids, values], 2);
            await table.WriteAsync([batch]);
        }

        // Re-open and verify
        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal(ColumnMappingMode.Id,
            ColumnMapping.GetMode(reopened.CurrentSnapshot.Metadata.Configuration));

        var readBatches = new List<RecordBatch>();
        await foreach (var b in reopened.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(2, readBatches[0].Length);
        Assert.Equal("id", readBatches[0].Schema.FieldsList[0].Name);
        Assert.Equal(10L, ((Int64Array)readBatches[0].Column(0)).GetValue(0));
    }

    [Fact]
    public async Task WriteAndRead_IdMode_WithProjection()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Field(new Field("score", DoubleType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Id);

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var names = new StringArray.Builder().Append("alice").Append("bob").Build();
        var scores = new DoubleArray.Builder().Append(95.5).Append(87.3).Build();
        var batch = new RecordBatch(schema, [ids, names, scores], 2);

        await table.WriteAsync([batch]);

        // Read only "id" and "score"
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync(columns: ["id", "score"]))
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(2, readBatches[0].ColumnCount);
        Assert.Equal("id", readBatches[0].Schema.FieldsList[0].Name);
        Assert.Equal("score", readBatches[0].Schema.FieldsList[1].Name);
    }

    [Fact]
    public async Task ParquetFile_HasFieldIds()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Id);

        var ids = new Int64Array.Builder().Append(1).Build();
        var values = new StringArray.Builder().Append("a").Build();
        var batch = new RecordBatch(schema, [ids, values], 1);
        await table.WriteAsync([batch]);

        // Read the Parquet file directly and verify field_ids are set
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        await using var file = await fs.OpenReadAsync(addFile.Path);
        using var reader = new EngineeredWood.Parquet.ParquetFileReader(file, ownsFile: false);
        var parquetSchema = await reader.GetSchemaAsync();

        // Top-level children should have field_ids
        bool foundFieldId = false;
        foreach (var child in parquetSchema.Root.Children)
        {
            if (child.Element.FieldId.HasValue)
                foundFieldId = true;
        }
        Assert.True(foundFieldId, "Parquet file should have field_id set on schema elements");
    }
}
