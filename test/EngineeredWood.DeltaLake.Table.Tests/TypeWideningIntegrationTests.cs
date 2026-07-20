// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class TypeWideningIntegrationTests : IDisposable
{
    private readonly string _tempDir;

    public TypeWideningIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_tw_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task ReadWidenedColumn_Int32ToInt64()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Step 1: Write a Parquet file with Int32 column
        string dataFile = "data_int32.parquet";
        var int32Schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Build();

        await using (var file = await fs.CreateAsync(dataFile))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            var ids = new Int32Array.Builder().Append(10).Append(20).Append(30).Build();
            await writer.WriteRowGroupAsync(new RecordBatch(int32Schema, [ids], 3));
        }

        // Step 2: Create table with Int64 schema + type change metadata
        string schemaString = """
        {
            "type":"struct",
            "fields":[{
                "name":"id",
                "type":"long",
                "nullable":false,
                "metadata":{
                    "delta.typeChanges":"[{\"fromType\":\"integer\",\"toType\":\"long\"}]"
                }
            }]
        }
        """;

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["typeWidening"],
                WriterFeatures = ["typeWidening"],
            },
            new MetadataAction
            {
                Id = "tw-table",
                Format = Format.Parquet,
                SchemaString = schemaString,
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { Schema.TypeWidening.EnableKey, "true" },
                },
            },
            new AddFile
            {
                Path = dataFile,
                PartitionValues = new Dictionary<string, string>(),
                Size = 100,
                ModificationTime = 1000,
                DataChange = true,
            },
        });

        // Step 3: Open and read — should widen Int32 → Int64
        await using var table = await DeltaTable.OpenAsync(fs);

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);

        Assert.Single(batches);
        Assert.Equal(3, batches[0].Length);

        // The column should be Int64 now
        Assert.IsType<Int64Array>(batches[0].Column(0));
        var ids64 = (Int64Array)batches[0].Column(0);
        Assert.Equal(10L, ids64.GetValue(0));
        Assert.Equal(20L, ids64.GetValue(1));
        Assert.Equal(30L, ids64.GetValue(2));
    }

    [Fact]
    public async Task ReadWidenedColumn_FloatToDouble()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Write Parquet file with Float column
        string dataFile = "data_float.parquet";
        var floatSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", FloatType.Default, true))
            .Build();

        await using (var file = await fs.CreateAsync(dataFile))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            var values = new FloatArray.Builder()
                .Append(1.5f).AppendNull().Append(3.7f).Build();
            await writer.WriteRowGroupAsync(new RecordBatch(floatSchema, [values], 3));
        }

        // Create table with Double schema
        string schemaString = """
        {"type":"struct","fields":[{"name":"value","type":"double","nullable":true,"metadata":{"delta.typeChanges":"[{\"fromType\":\"float\",\"toType\":\"double\"}]"}}]}
        """;

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["typeWidening"],
                WriterFeatures = ["typeWidening"],
            },
            new MetadataAction
            {
                Id = "tw-float",
                Format = Format.Parquet,
                SchemaString = schemaString,
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { Schema.TypeWidening.EnableKey, "true" },
                },
            },
            new AddFile
            {
                Path = dataFile,
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);

        Assert.Single(batches);
        Assert.IsType<DoubleArray>(batches[0].Column(0));

        var doubles = (DoubleArray)batches[0].Column(0);
        Assert.Equal(1.5, doubles.GetValue(0)!.Value, 5);
        Assert.True(doubles.IsNull(1));
        Assert.Equal(3.7, doubles.GetValue(2)!.Value, 1);
    }

    [Fact]
    public async Task CompactionWidensValues()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Write two small Parquet files with Int32 column
        for (int f = 0; f < 2; f++)
        {
            string dataFile = $"data_{f}.parquet";
            var int32Schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int32Type.Default, false))
                .Build();

            await using (var file = await fs.CreateAsync(dataFile))
            {
                await using var writer = new ParquetFileWriter(file, ownsFile: false);
                var ids = new Int32Array.Builder().Append(f * 10 + 1).Build();
                await writer.WriteRowGroupAsync(new RecordBatch(int32Schema, [ids], 1));
            }
        }

        // Create table with Int64 schema (widened)
        string schemaString = """
        {"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{"delta.typeChanges":"[{\"fromType\":\"integer\",\"toType\":\"long\"}]"}}]}
        """;

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3, MinWriterVersion = 7,
                ReaderFeatures = ["typeWidening"],
                WriterFeatures = ["typeWidening"],
            },
            new MetadataAction
            {
                Id = "tw-compact",
                Format = Format.Parquet,
                SchemaString = schemaString,
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { Schema.TypeWidening.EnableKey, "true" },
                },
            },
            new AddFile
            {
                Path = "data_0.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1, ModificationTime = 1000, DataChange = true,
            },
            new AddFile
            {
                Path = "data_1.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1, ModificationTime = 1000, DataChange = true,
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(2, table.CurrentSnapshot.FileCount);

        // Compact — should widen Int32 → Int64 during rewrite
        var result = await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue,
        });

        Assert.NotNull(result);

        // Read back — should be Int64
        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);

        int totalRows = batches.Sum(b => b.Length);
        Assert.Equal(2, totalRows);

        foreach (var b in batches)
            Assert.IsType<Int64Array>(b.Column(0));
    }

    [Fact]
    public async Task ProtocolFeature_TypeWidening_Accepted()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["typeWidening"],
                WriterFeatures = ["typeWidening"],
            },
            new MetadataAction
            {
                Id = "tw-feat",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(3, table.CurrentSnapshot.Protocol.MinReaderVersion);
    }
}
