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

public class IcebergCompatTests : IDisposable
{
    private readonly string _tempDir;

    public IcebergCompatTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_iceberg_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    #region Helpers

    /// <summary>
    /// Creates a table with IcebergCompatV1 enabled and column mapping in name mode.
    /// Uses a simple schema with id (long) and value (string) columns.
    /// </summary>
    private async Task<DeltaTable> CreateIcebergV1Table(
        string? schemaJson = null,
        IReadOnlyList<string>? partitionColumns = null)
    {
        var deltaSchema = DeltaSchemaSerializer.Parse(schemaJson ??
            """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""");

        var (mappedSchema, maxId) = ColumnMapping.AssignColumnMapping(deltaSchema);
        string mappedSchemaJson = DeltaSchemaSerializer.Serialize(mappedSchema);

        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 2,
                MinWriterVersion = 7,
                WriterFeatures = ["columnMapping", "icebergCompatV1"],
            },
            new MetadataAction
            {
                Id = "iceberg-v1-table",
                Format = Format.Parquet,
                SchemaString = mappedSchemaJson,
                PartitionColumns = partitionColumns ?? [],
                Configuration = new Dictionary<string, string>
                {
                    { ColumnMapping.ModeKey, "name" },
                    { ColumnMapping.MaxColumnIdKey, maxId.ToString() },
                    { IcebergCompat.EnableV1Key, "true" },
                },
            },
        });

        return await DeltaTable.OpenAsync(fs);
    }

    /// <summary>
    /// Creates a table with IcebergCompatV2 enabled and column mapping in name mode.
    /// </summary>
    private async Task<DeltaTable> CreateIcebergV2Table(string? schemaJson = null)
    {
        var deltaSchema = DeltaSchemaSerializer.Parse(schemaJson ??
            """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""");

        var (mappedSchema, maxId) = ColumnMapping.AssignColumnMapping(deltaSchema);
        string mappedSchemaJson = DeltaSchemaSerializer.Serialize(mappedSchema);

        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 2,
                MinWriterVersion = 7,
                WriterFeatures = ["columnMapping", "icebergCompatV2"],
            },
            new MetadataAction
            {
                Id = "iceberg-v2-table",
                Format = Format.Parquet,
                SchemaString = mappedSchemaJson,
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { ColumnMapping.ModeKey, "name" },
                    { ColumnMapping.MaxColumnIdKey, maxId.ToString() },
                    { IcebergCompat.EnableV2Key, "true" },
                },
            },
        });

        return await DeltaTable.OpenAsync(fs);
    }

    #endregion

    #region IcebergCompatVersion detection

    [Fact]
    public void GetVersion_None_WhenNoConfig()
    {
        Assert.Equal(IcebergCompatVersion.None, IcebergCompat.GetVersion(null));
    }

    [Fact]
    public void GetVersion_V1_WhenEnabled()
    {
        var config = new Dictionary<string, string> { { IcebergCompat.EnableV1Key, "true" } };
        Assert.Equal(IcebergCompatVersion.V1, IcebergCompat.GetVersion(config));
    }

    [Fact]
    public void GetVersion_V2_WhenEnabled()
    {
        var config = new Dictionary<string, string> { { IcebergCompat.EnableV2Key, "true" } };
        Assert.Equal(IcebergCompatVersion.V2, IcebergCompat.GetVersion(config));
    }

    [Fact]
    public void GetVersion_V2_TakesPrecedence()
    {
        var config = new Dictionary<string, string>
        {
            { IcebergCompat.EnableV1Key, "true" },
            { IcebergCompat.EnableV2Key, "true" },
        };
        Assert.Equal(IcebergCompatVersion.V2, IcebergCompat.GetVersion(config));
    }

    #endregion

    #region V1 validation — column mapping required

    [Fact]
    public void Validate_V1_ThrowsWithoutColumnMapping()
    {
        var metadata = new MetadataAction
        {
            Id = "test",
            Format = Format.Parquet,
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
            PartitionColumns = [],
            Configuration = new Dictionary<string, string>
            {
                { IcebergCompat.EnableV1Key, "true" },
                // No column mapping mode
            },
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 2,
            MinWriterVersion = 7,
            WriterFeatures = ["icebergCompatV1"],
        };

        var ex = Assert.Throws<DeltaFormatException>(
            () => IcebergCompat.Validate(IcebergCompatVersion.V1, metadata, protocol));
        Assert.Contains("column mapping", ex.Message);
    }

    #endregion

    #region V1 validation — no deletion vectors

    [Fact]
    public void Validate_V1_ThrowsWithDeletionVectors()
    {
        var metadata = new MetadataAction
        {
            Id = "test",
            Format = Format.Parquet,
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
            PartitionColumns = [],
            Configuration = new Dictionary<string, string>
            {
                { ColumnMapping.ModeKey, "name" },
                { IcebergCompat.EnableV1Key, "true" },
            },
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 2,
            MinWriterVersion = 7,
            WriterFeatures = ["columnMapping", "icebergCompatV1", "deletionVectors"],
        };

        var ex = Assert.Throws<DeltaFormatException>(
            () => IcebergCompat.Validate(IcebergCompatVersion.V1, metadata, protocol));
        Assert.Contains("deletion vectors", ex.Message);
    }

    #endregion

    #region V1 validation — prohibited types

    [Fact]
    public void Validate_V1_ThrowsForArrayType()
    {
        var metadata = new MetadataAction
        {
            Id = "test",
            Format = Format.Parquet,
            SchemaString = """{"type":"struct","fields":[{"name":"tags","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}}]}""",
            PartitionColumns = [],
            Configuration = new Dictionary<string, string>
            {
                { ColumnMapping.ModeKey, "name" },
                { IcebergCompat.EnableV1Key, "true" },
            },
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 2,
            MinWriterVersion = 7,
            WriterFeatures = ["columnMapping", "icebergCompatV1"],
        };

        var ex = Assert.Throws<DeltaFormatException>(
            () => IcebergCompat.Validate(IcebergCompatVersion.V1, metadata, protocol));
        Assert.Contains("array", ex.Message);
    }

    [Fact]
    public void Validate_V1_ThrowsForMapType()
    {
        var metadata = new MetadataAction
        {
            Id = "test",
            Format = Format.Parquet,
            SchemaString = """{"type":"struct","fields":[{"name":"props","type":{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true},"nullable":true,"metadata":{}}]}""",
            PartitionColumns = [],
            Configuration = new Dictionary<string, string>
            {
                { ColumnMapping.ModeKey, "name" },
                { IcebergCompat.EnableV1Key, "true" },
            },
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 2,
            MinWriterVersion = 7,
            WriterFeatures = ["columnMapping", "icebergCompatV1"],
        };

        var ex = Assert.Throws<DeltaFormatException>(
            () => IcebergCompat.Validate(IcebergCompatVersion.V1, metadata, protocol));
        Assert.Contains("map", ex.Message);
    }

    #endregion

    #region V2 validation — allows Array and Map

    [Fact]
    public void Validate_V2_AllowsArrayType()
    {
        var metadata = new MetadataAction
        {
            Id = "test",
            Format = Format.Parquet,
            SchemaString = """{"type":"struct","fields":[{"name":"tags","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}}]}""",
            PartitionColumns = [],
            Configuration = new Dictionary<string, string>
            {
                { ColumnMapping.ModeKey, "name" },
                { IcebergCompat.EnableV2Key, "true" },
            },
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 2,
            MinWriterVersion = 7,
            WriterFeatures = ["columnMapping", "icebergCompatV2"],
        };

        // Should not throw
        IcebergCompat.Validate(IcebergCompatVersion.V2, metadata, protocol);
    }

    [Fact]
    public void Validate_V2_AllowsMapType()
    {
        var metadata = new MetadataAction
        {
            Id = "test",
            Format = Format.Parquet,
            SchemaString = """{"type":"struct","fields":[{"name":"props","type":{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true},"nullable":true,"metadata":{}}]}""",
            PartitionColumns = [],
            Configuration = new Dictionary<string, string>
            {
                { ColumnMapping.ModeKey, "name" },
                { IcebergCompat.EnableV2Key, "true" },
            },
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 2,
            MinWriterVersion = 7,
            WriterFeatures = ["columnMapping", "icebergCompatV2"],
        };

        // Should not throw
        IcebergCompat.Validate(IcebergCompatVersion.V2, metadata, protocol);
    }

    [Fact]
    public void Validate_V2_AllowsDeletionVectors()
    {
        var metadata = new MetadataAction
        {
            Id = "test",
            Format = Format.Parquet,
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
            PartitionColumns = [],
            Configuration = new Dictionary<string, string>
            {
                { ColumnMapping.ModeKey, "name" },
                { IcebergCompat.EnableV2Key, "true" },
            },
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 2,
            MinWriterVersion = 7,
            WriterFeatures = ["columnMapping", "icebergCompatV2", "deletionVectors"],
        };

        // V2 does not prohibit deletion vectors
        IcebergCompat.Validate(IcebergCompatVersion.V2, metadata, protocol);
    }

    #endregion

    #region Write path — basic write with IcebergCompatV1

    [Fact]
    public async Task WriteAsync_V1_WritesSuccessfully()
    {
        await using var table = await CreateIcebergV1Table();
        var schema = table.ArrowSchema;

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var values = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var batch = new RecordBatch(schema, [ids, values], 3);

        long version = await table.WriteAsync([batch]);
        Assert.Equal(1, version);

        // Verify data round-trips
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(3, readBatches[0].Length);
    }

    #endregion

    #region Write path — stats always collected

    [Fact]
    public async Task WriteAsync_V1_CollectsStatsEvenWhenDisabled()
    {
        // Create table with CollectStats = false but IcebergCompat enabled
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        var deltaSchema = DeltaSchemaSerializer.Parse(
            """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""");
        var (mappedSchema, maxId) = ColumnMapping.AssignColumnMapping(deltaSchema);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 2,
                MinWriterVersion = 7,
                WriterFeatures = ["columnMapping", "icebergCompatV1"],
            },
            new MetadataAction
            {
                Id = "stats-test",
                Format = Format.Parquet,
                SchemaString = DeltaSchemaSerializer.Serialize(mappedSchema),
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { ColumnMapping.ModeKey, "name" },
                    { ColumnMapping.MaxColumnIdKey, maxId.ToString() },
                    { IcebergCompat.EnableV1Key, "true" },
                },
            },
        });

        var options = new DeltaTableOptions { CollectStats = false };
        await using var table = await DeltaTable.OpenAsync(fs, options);
        var schema = table.ArrowSchema;

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var values = new StringArray.Builder().Append("x").Append("y").Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, values], 2)]);

        // Stats should still be present because IcebergCompat requires them
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        Assert.NotNull(addFile.Stats);
        Assert.Contains("numRecords", addFile.Stats);
    }

    #endregion

    #region Write path — partition materialization

    [Fact]
    public async Task WriteAsync_V1_MaterializesPartitionColumns()
    {
        string schemaJson =
            """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"region","type":"string","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""";

        await using var table = await CreateIcebergV1Table(
            schemaJson: schemaJson,
            partitionColumns: ["region"]);

        var schema = table.ArrowSchema;

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var regions = new StringArray.Builder().Append("us").Append("us").Build();
        var values = new StringArray.Builder().Append("a").Append("b").Build();
        var batch = new RecordBatch(schema, [ids, regions, values], 2);

        await table.WriteAsync([batch]);

        // Read back — should get all columns including partition column
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(2, readBatches[0].Length);

        // The data should contain id, region, and value
        // (region was materialized into the Parquet file)
        Assert.Equal(3, readBatches[0].ColumnCount);
    }

    #endregion

    #region Write path — V2 write

    [Fact]
    public async Task WriteAsync_V2_WritesSuccessfully()
    {
        await using var table = await CreateIcebergV2Table();
        var schema = table.ArrowSchema;

        var ids = new Int64Array.Builder().Append(10).Append(20).Build();
        var values = new StringArray.Builder().Append("hello").Append("world").Build();
        var batch = new RecordBatch(schema, [ids, values], 2);

        long version = await table.WriteAsync([batch]);
        Assert.Equal(1, version);

        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(2, readBatches[0].Length);
    }

    #endregion

    #region Protocol feature registration

    [Fact]
    public async Task OpenTable_V1Feature_Accepted()
    {
        // Table with icebergCompatV1 should be readable (feature is recognized)
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 2,
                MinWriterVersion = 7,
                WriterFeatures = ["columnMapping", "icebergCompatV1"],
            },
            new MetadataAction
            {
                Id = "feature-test",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { ColumnMapping.ModeKey, "name" },
                },
            },
        });

        // Should not throw — the feature is recognized
        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.NotNull(table.CurrentSnapshot);
    }

    [Fact]
    public async Task OpenTable_V2Feature_Accepted()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 2,
                MinWriterVersion = 7,
                WriterFeatures = ["columnMapping", "icebergCompatV2"],
            },
            new MetadataAction
            {
                Id = "feature-test",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { ColumnMapping.ModeKey, "name" },
                },
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.NotNull(table.CurrentSnapshot);
    }

    #endregion
}
