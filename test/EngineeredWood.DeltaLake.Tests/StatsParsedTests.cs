// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Tests;

public class StatsParsedTests : IDisposable
{
    private readonly string _tempDir;

    public StatsParsedTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_sp_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Checkpoint_ContainsStatsParsed()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "sp-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000, ModificationTime = 1000, DataChange = true,
                Stats = """{"numRecords":100,"minValues":{"id":1,"value":"a"},"maxValues":{"id":100,"value":"z"},"nullCount":{"id":0,"value":5}}""",
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        // Read the checkpoint Parquet file directly
        string ckptPath = DeltaVersion.CheckpointPath(0);
        await using var file = await fs.OpenReadAsync(ckptPath);
        using var reader = new ParquetFileReader(file, ownsFile: false);

        await foreach (var batch in reader.ReadAllAsync())
        {
            // Find stats_parsed column
            int spIdx = batch.Schema.GetFieldIndex("stats_parsed");
            Assert.True(spIdx >= 0, "Checkpoint should have a stats_parsed column");

            var spCol = batch.Column(spIdx);
            Assert.IsType<StructArray>(spCol);

            var spStruct = (StructArray)spCol;

            // The struct should have numRecords, minValues, maxValues, nullCount
            var spType = (Apache.Arrow.Types.StructType)spStruct.Data.DataType;
            var fieldNames = spType.Fields.Select(f => f.Name).ToList();
            Assert.Contains("numRecords", fieldNames);
            Assert.Contains("minValues", fieldNames);
            Assert.Contains("maxValues", fieldNames);
            Assert.Contains("nullCount", fieldNames);

            // Find the row with stats (the add action row)
            int numRecordsIdx = spType.Fields.ToList().FindIndex(f => f.Name == "numRecords");
            var numRecordsArray = (Int64Array)spStruct.Fields[numRecordsIdx];

            // One row should have numRecords=100
            bool foundStats = false;
            for (int row = 0; row < batch.Length; row++)
            {
                if (!numRecordsArray.IsNull(row) && numRecordsArray.GetValue(row) == 100)
                {
                    foundStats = true;
                    break;
                }
            }
            Assert.True(foundStats, "Should find numRecords=100 in stats_parsed");
        }
    }

    [Fact]
    public async Task Checkpoint_StatsParsed_MinMaxValues()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "sp-minmax",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"score","type":"double","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000, ModificationTime = 1000, DataChange = true,
                Stats = """{"numRecords":50,"minValues":{"id":1,"score":1.5},"maxValues":{"id":50,"score":99.9},"nullCount":{"id":0,"score":3}}""",
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        // Read and verify minValues/maxValues/nullCount structs
        string ckptPath = DeltaVersion.CheckpointPath(0);
        await using var file = await fs.OpenReadAsync(ckptPath);
        using var reader = new ParquetFileReader(file, ownsFile: false);

        await foreach (var batch in reader.ReadAllAsync())
        {
            int spIdx = batch.Schema.GetFieldIndex("stats_parsed");
            var spStruct = (StructArray)batch.Column(spIdx);
            var spType = (Apache.Arrow.Types.StructType)spStruct.Data.DataType;

            // Find minValues struct
            int minIdx = spType.Fields.ToList().FindIndex(f => f.Name == "minValues");
            var minStruct = (StructArray)spStruct.Fields[minIdx];
            var minType = (Apache.Arrow.Types.StructType)minStruct.Data.DataType;

            // minValues should have "id" and "score" fields
            Assert.Contains(minType.Fields, f => f.Name == "id");
            Assert.Contains(minType.Fields, f => f.Name == "score");

            // Find the add action row (check numRecords to identify it)
            int nrIdx = spType.Fields.ToList().FindIndex(f => f.Name == "numRecords");
            var nrArray = (Int64Array)spStruct.Fields[nrIdx];

            for (int row = 0; row < batch.Length; row++)
            {
                if (!nrArray.IsNull(row) && nrArray.GetValue(row) == 50)
                {
                    // Verify min id value
                    int idFieldIdx = minType.Fields.ToList().FindIndex(f => f.Name == "id");
                    var idMinArray = (Int64Array)minStruct.Fields[idFieldIdx];
                    Assert.Equal(1L, idMinArray.GetValue(row));

                    // Verify nullCount
                    int ncIdx = spType.Fields.ToList().FindIndex(f => f.Name == "nullCount");
                    var ncStruct = (StructArray)spStruct.Fields[ncIdx];
                    var ncType = (Apache.Arrow.Types.StructType)ncStruct.Data.DataType;
                    int scoreNcIdx = ncType.Fields.ToList().FindIndex(f => f.Name == "score");
                    var scoreNcArray = (Int64Array)ncStruct.Fields[scoreNcIdx];
                    Assert.Equal(3L, scoreNcArray.GetValue(row));
                }
            }
        }
    }

    [Fact]
    public async Task Checkpoint_WithStatsParsed_StillReadable()
    {
        // Verify that checkpoints with stats_parsed can still be read
        // by the standard checkpoint reader (which ignores unknown columns)
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "sp-readable",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000, ModificationTime = 1000, DataChange = true,
                Stats = """{"numRecords":10}""",
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        // Read via CheckpointReader (standard path)
        var reader = new CheckpointReader(fs);
        var lastCkpt = await reader.ReadLastCheckpointAsync();
        var actions = await reader.ReadCheckpointAsync(lastCkpt!);

        var builder = new SnapshotBuilder();
        builder.ApplyCommit(lastCkpt!.Version, actions);
        var restored = builder.Build();

        Assert.Equal("sp-readable", restored.Metadata.Id);
        Assert.Equal(1, restored.FileCount);
    }
}
