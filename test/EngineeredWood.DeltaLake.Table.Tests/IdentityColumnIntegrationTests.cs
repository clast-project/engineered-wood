// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class IdentityColumnIntegrationTests : IDisposable
{
    private readonly string _tempDir;

    public IdentityColumnIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ic_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateTableWithIdentityColumn(
        bool allowExplicit = false, long start = 1, long step = 1)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        var idMeta = IdentityColumn.CreateMetadata(start, step, allowExplicit);
        string idMetaJson = System.Text.Json.JsonSerializer.Serialize(idMeta);

        string schemaString = $@"{{""type"":""struct"",""fields"":[{{""name"":""id"",""type"":""long"",""nullable"":false,""metadata"":{idMetaJson}}},{{""name"":""value"",""type"":""string"",""nullable"":true,""metadata"":{{}}}}]}}";

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["identityColumns"],
            },
            new MetadataAction
            {
                Id = "ic-table",
                Format = Format.Parquet,
                SchemaString = schemaString,
                PartitionColumns = [],
            },
        });

        return await DeltaTable.OpenAsync(fs);
    }

    [Fact]
    public async Task Write_AutoGeneratesIdentityValues()
    {
        await using var table = await CreateTableWithIdentityColumn();

        // Write without providing the id column
        var valueSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", StringType.Default, true))
            .Build();
        var values = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var batch = new RecordBatch(valueSchema, [values], 3);

        await table.WriteAsync([batch]);

        // Read back — id column should have auto-generated values
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(3, readBatches[0].Length);

        // Find the id column
        int idIdx = readBatches[0].Schema.GetFieldIndex("id");
        Assert.True(idIdx >= 0, "Should have an id column");
        var ids = (Int64Array)readBatches[0].Column(idIdx);

        Assert.Equal(1L, ids.GetValue(0)); // start=1, step=1
        Assert.Equal(2L, ids.GetValue(1));
        Assert.Equal(3L, ids.GetValue(2));
    }

    [Fact]
    public async Task Write_MultipleCommits_ContinuesSequence()
    {
        await using var table = await CreateTableWithIdentityColumn();

        var valueSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", StringType.Default, true))
            .Build();

        // First write: 2 rows
        var batch1 = new RecordBatch(valueSchema,
            [new StringArray.Builder().Append("a").Append("b").Build()], 2);
        await table.WriteAsync([batch1]);

        // Second write: 2 more rows
        var batch2 = new RecordBatch(valueSchema,
            [new StringArray.Builder().Append("c").Append("d").Build()], 2);
        await table.WriteAsync([batch2]);

        // Read all
        var allIds = new List<long>();
        await foreach (var b in table.ReadAllAsync())
        {
            int idIdx = b.Schema.GetFieldIndex("id");
            var ids = (Int64Array)b.Column(idIdx);
            for (int i = 0; i < b.Length; i++)
                allIds.Add(ids.GetValue(i)!.Value);
        }

        Assert.Equal(4, allIds.Count);
        // Should be 1, 2, 3, 4 (continuous sequence)
        Assert.Equal(new long[] { 1, 2, 3, 4 }, allIds.OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task Write_CustomStartAndStep()
    {
        await using var table = await CreateTableWithIdentityColumn(
            start: 100, step: 10);

        var valueSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", StringType.Default, true))
            .Build();

        var batch = new RecordBatch(valueSchema,
            [new StringArray.Builder().Append("x").Append("y").Append("z").Build()], 3);
        await table.WriteAsync([batch]);

        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        int idIdx = readBatches[0].Schema.GetFieldIndex("id");
        var ids = (Int64Array)readBatches[0].Column(idIdx);

        Assert.Equal(100L, ids.GetValue(0));
        Assert.Equal(110L, ids.GetValue(1));
        Assert.Equal(120L, ids.GetValue(2));
    }

    [Fact]
    public async Task Write_ExplicitInsert_UsesProvidedValues()
    {
        await using var table = await CreateTableWithIdentityColumn(allowExplicit: true);

        // Provide explicit id values
        var schema = table.ArrowSchema;
        var ids = new Int64Array.Builder().Append(10).Append(20).Append(30).Build();
        var values = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var batch = new RecordBatch(schema, [ids, values], 3);

        await table.WriteAsync([batch]);

        // Read back — should have the provided values
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        int idIdx = readBatches[0].Schema.GetFieldIndex("id");
        var readIds = (Int64Array)readBatches[0].Column(idIdx);

        Assert.Equal(10L, readIds.GetValue(0));
        Assert.Equal(20L, readIds.GetValue(1));
        Assert.Equal(30L, readIds.GetValue(2));
    }

    [Fact]
    public async Task Write_ExplicitInsert_NextAutoUsesContinuation()
    {
        await using var table = await CreateTableWithIdentityColumn(allowExplicit: true);

        // First write with explicit values
        var schema = table.ArrowSchema;
        var ids = new Int64Array.Builder().Append(50).Build();
        var values = new StringArray.Builder().Append("a").Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, values], 1)]);

        // Second write without explicit values — should continue from 51
        var valueSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", StringType.Default, true))
            .Build();
        var batch2 = new RecordBatch(valueSchema,
            [new StringArray.Builder().Append("b").Build()], 1);
        await table.WriteAsync([batch2]);

        var allIds = new List<long>();
        await foreach (var b in table.ReadAllAsync())
        {
            int idIdx = b.Schema.GetFieldIndex("id");
            var readIds = (Int64Array)b.Column(idIdx);
            for (int i = 0; i < b.Length; i++)
                allIds.Add(readIds.GetValue(i)!.Value);
        }

        Assert.Contains(50L, allIds);
        Assert.Contains(51L, allIds);
    }

    [Fact]
    public async Task HighWaterMark_UpdatedInSchema()
    {
        await using var table = await CreateTableWithIdentityColumn();

        var valueSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", StringType.Default, true))
            .Build();

        var batch = new RecordBatch(valueSchema,
            [new StringArray.Builder().Append("a").Append("b").Build()], 2);
        await table.WriteAsync([batch]);

        // Check the schema metadata has the updated high water mark
        var idField = table.CurrentSnapshot.Schema.Fields
            .First(f => f.Name == "id");
        var config = IdentityColumn.GetConfig(idField);

        Assert.NotNull(config);
        Assert.Equal(2L, config!.HighWaterMark); // Two rows generated: 1, 2
    }

    [Fact]
    public async Task ProtocolFeature_IdentityColumns_Accepted()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["identityColumns"],
            },
            new MetadataAction
            {
                Id = "ic-feat",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(7, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }
}
