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

    /// <summary>
    /// The typed statistics live INSIDE the add struct, as <c>add.stats_parsed</c> — that is where
    /// delta-spark writes them and the only place its readers look.
    /// </summary>
    private static StructArray StatsParsed(RecordBatch batch)
    {
        int addIdx = batch.Schema.GetFieldIndex("add");
        Assert.True(addIdx >= 0, "Checkpoint should have an add column");
        var add = (StructArray)batch.Column(addIdx);

        var addType = (Apache.Arrow.Types.StructType)add.Data.DataType;
        int spIdx = addType.Fields.ToList().FindIndex(f => f.Name == "stats_parsed");
        Assert.True(spIdx >= 0, "add should carry a stats_parsed field");
        return (StructArray)add.Fields[spIdx];
    }

    /// <summary>Writes a one-commit table and returns its checkpoint's single RecordBatch.</summary>
    private async Task<RecordBatch> WriteAndReadCheckpoint(
        string schemaString,
        string? stats,
        IReadOnlyDictionary<string, string>? configuration = null)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "typed-stats",
                Format = Format.Parquet,
                SchemaString = schemaString,
                PartitionColumns = [],
                Configuration = configuration,
            },
            new AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000, ModificationTime = 1000, DataChange = true,
                Stats = stats,
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        await using var file = await fs.OpenReadAsync(DeltaVersion.CheckpointPath(0));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        await foreach (var batch in reader.ReadAllAsync())
            return batch;

        throw new InvalidOperationException("checkpoint had no row groups");
    }

    private static IArrowArray Child(StructArray parent, string name)
    {
        var type = (Apache.Arrow.Types.StructType)parent.Data.DataType;
        int index = type.Fields.ToList().FindIndex(f => f.Name == name);
        Assert.True(index >= 0, $"expected a '{name}' field");
        return parent.Fields[index];
    }

    /// <summary>
    /// Bounds carry each column's OWN type, which is what makes them usable: a <c>decimal(9,2)</c>
    /// column's min/max are <c>decimal(9,2)</c>, not an approximating double, and a timestamp's are a
    /// timestamp rather than a bare micros integer. Booleans are ordered by nothing, so delta-spark
    /// omits them from min/max while still counting their nulls — matched here.
    /// </summary>
    [Fact]
    public async Task Checkpoint_StatsParsed_BoundsCarryTheColumnsOwnType()
    {
        const string schemaString = """
            {"type":"struct","fields":[
              {"name":"id","type":"long","nullable":false,"metadata":{}},
              {"name":"amount","type":"decimal(9,2)","nullable":true,"metadata":{}},
              {"name":"d","type":"date","nullable":true,"metadata":{}},
              {"name":"ts","type":"timestamp","nullable":true,"metadata":{}},
              {"name":"s","type":"string","nullable":true,"metadata":{}},
              {"name":"b","type":"boolean","nullable":true,"metadata":{}}]}
            """;

        var batch = await WriteAndReadCheckpoint(schemaString, """
            {"numRecords":10,"minValues":{"id":1,"amount":1.25,"d":"2021-06-20",
             "ts":"2021-06-20T10:00:00.000000Z","s":"a"},
             "maxValues":{"id":9,"amount":9.75,"d":"2022-06-20",
             "ts":"2022-06-20T10:00:00.000000Z","s":"z"},
             "nullCount":{"id":0,"amount":1,"d":2,"ts":3,"s":4,"b":5}}
            """);

        var stats = StatsParsed(batch);
        var minValues = (StructArray)Child(stats, "minValues");
        var minType = (Apache.Arrow.Types.StructType)minValues.Data.DataType;

        Assert.Equal(
            ["id", "amount", "d", "ts", "s"],
            minType.Fields.Select(f => f.Name));
        Assert.IsType<Int64Type>(minType.Fields[0].DataType);
        // The reader narrows a decimal to the smallest width its precision fits, so assert on
        // precision/scale rather than on which Decimal{32,64,128,256}Type came back.
        Assert.Equal((9, 2), DecimalPrecisionScale(minType.Fields[1].DataType));
        Assert.IsType<Date32Type>(minType.Fields[2].DataType);
        var tsType = Assert.IsType<TimestampType>(minType.Fields[3].DataType);
        Assert.Equal(TimeUnit.Microsecond, tsType.Unit);
        Assert.IsType<StringType>(minType.Fields[4].DataType);

        // nullCount counts every column, boolean included, always as a long.
        var nullCount = (StructArray)Child(stats, "nullCount");
        var nullCountType = (Apache.Arrow.Types.StructType)nullCount.Data.DataType;
        Assert.Equal(
            ["id", "amount", "d", "ts", "s", "b"],
            nullCountType.Fields.Select(f => f.Name));
        Assert.All(nullCountType.Fields, f => Assert.IsType<Int64Type>(f.DataType));
    }

    /// <summary>
    /// The values, not just the types. A decimal bound is decoded from the JSON digits exactly — the
    /// value here has more significant digits than <c>System.Decimal</c> can hold, so a builder that
    /// went through <c>decimal</c> would silently round the bound and could skip a matching file.
    /// </summary>
    [Fact]
    public async Task Checkpoint_StatsParsed_DecimalBoundsAreExact()
    {
        const string schemaString = """
            {"type":"struct","fields":[
              {"name":"amount","type":"decimal(38,10)","nullable":true,"metadata":{}}]}
            """;

        var batch = await WriteAndReadCheckpoint(schemaString, """
            {"numRecords":2,
             "minValues":{"amount":1234567890123456789012345678.0000000001},
             "maxValues":{"amount":1234567890123456789012345678.0000000009},
             "nullCount":{"amount":0}}
            """);

        var stats = StatsParsed(batch);
        var minAmount = (Decimal128Array)Child((StructArray)Child(stats, "minValues"), "amount");
        var maxAmount = (Decimal128Array)Child((StructArray)Child(stats, "maxValues"), "amount");

        int row = FindStatsRow(stats);
        Assert.Equal("1234567890123456789012345678.0000000001", minAmount.GetString(row));
        Assert.Equal("1234567890123456789012345678.0000000009", maxAmount.GetString(row));
    }

    /// <summary>Nested struct columns recurse, so a bound lives at <c>minValues.payload.score</c>.</summary>
    [Fact]
    public async Task Checkpoint_StatsParsed_RecursesIntoNestedStructs()
    {
        const string schemaString = """
            {"type":"struct","fields":[
              {"name":"payload","type":{"type":"struct","fields":[
                {"name":"score","type":"long","nullable":true,"metadata":{}}]},
               "nullable":true,"metadata":{}}]}
            """;

        var batch = await WriteAndReadCheckpoint(schemaString, """
            {"numRecords":5,"minValues":{"payload":{"score":10}},
             "maxValues":{"payload":{"score":99}},"nullCount":{"payload":{"score":1}}}
            """);

        var stats = StatsParsed(batch);
        int row = FindStatsRow(stats);

        var minPayload = (StructArray)Child((StructArray)Child(stats, "minValues"), "payload");
        Assert.Equal(10L, ((Int64Array)Child(minPayload, "score")).GetValue(row));

        var maxPayload = (StructArray)Child((StructArray)Child(stats, "maxValues"), "payload");
        Assert.Equal(99L, ((Int64Array)Child(maxPayload, "score")).GetValue(row));

        var ncPayload = (StructArray)Child((StructArray)Child(stats, "nullCount"), "payload");
        Assert.Equal(1L, ((Int64Array)Child(ncPayload, "score")).GetValue(row));
    }

    /// <summary>
    /// <c>delta.checkpoint.writeStatsAsJson=false</c> drops the JSON string and leaves the typed
    /// struct as the only copy — the shape delta-spark 4.1 reads by re-encoding it back to JSON.
    /// </summary>
    [Fact]
    public async Task Checkpoint_WriteStatsAsJsonFalse_LeavesOnlyTheTypedStats()
    {
        const string schemaString = """
            {"type":"struct","fields":[
              {"name":"id","type":"long","nullable":false,"metadata":{}}]}
            """;

        var batch = await WriteAndReadCheckpoint(
            schemaString,
            """{"numRecords":10,"minValues":{"id":1},"maxValues":{"id":9},"nullCount":{"id":0}}""",
            new Dictionary<string, string> { ["delta.checkpoint.writeStatsAsJson"] = "false" });

        var add = (StructArray)batch.Column(batch.Schema.GetFieldIndex("add"));
        var addType = (Apache.Arrow.Types.StructType)add.Data.DataType;
        Assert.DoesNotContain(addType.Fields, f => f.Name == "stats");
        Assert.Contains(addType.Fields, f => f.Name == "stats_parsed");

        var stats = StatsParsed(batch);
        int row = FindStatsRow(stats);
        Assert.Equal(1L, ((Int64Array)Child((StructArray)Child(stats, "minValues"), "id")).GetValue(row));
    }

    /// <summary>
    /// <c>delta.checkpoint.writeStatsAsStruct=false</c> drops the typed struct and keeps the JSON.
    /// </summary>
    [Fact]
    public async Task Checkpoint_WriteStatsAsStructFalse_LeavesOnlyTheJsonStats()
    {
        const string schemaString = """
            {"type":"struct","fields":[
              {"name":"id","type":"long","nullable":false,"metadata":{}}]}
            """;

        var batch = await WriteAndReadCheckpoint(
            schemaString,
            """{"numRecords":10,"minValues":{"id":1},"maxValues":{"id":9},"nullCount":{"id":0}}""",
            new Dictionary<string, string> { ["delta.checkpoint.writeStatsAsStruct"] = "false" });

        var add = (StructArray)batch.Column(batch.Schema.GetFieldIndex("add"));
        var addType = (Apache.Arrow.Types.StructType)add.Data.DataType;
        Assert.Contains(addType.Fields, f => f.Name == "stats");
        Assert.DoesNotContain(addType.Fields, f => f.Name == "stats_parsed");
    }

    private static (int Precision, int Scale) DecimalPrecisionScale(IArrowType type) => type switch
    {
        Decimal32Type d => (d.Precision, d.Scale),
        Decimal64Type d => (d.Precision, d.Scale),
        Decimal128Type d => (d.Precision, d.Scale),
        Decimal256Type d => (d.Precision, d.Scale),
        _ => throw new InvalidOperationException($"expected a decimal type, got {type.Name}"),
    };

    /// <summary>The one row whose statistics are present — the add action.</summary>
    private static int FindStatsRow(StructArray stats)
    {
        var numRecords = (Int64Array)Child(stats, "numRecords");
        for (int row = 0; row < stats.Length; row++)
        {
            if (!numRecords.IsNull(row))
                return row;
        }

        Assert.Fail("no row carried statistics");
        return -1;
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
            var spStruct = StatsParsed(batch);

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
            var spStruct = StatsParsed(batch);
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
    public async Task Checkpoint_StatsParsed_OutOfRangeBound_BecomesNull()
    {
        // A bound that doesn't fit the column's type (here a min beyond long.MaxValue) must land as a
        // null in that one column, leaving every other stats column aligned on the row.
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "sp-overflow",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000, ModificationTime = 1000, DataChange = true,
                Stats = """{"numRecords":100,"minValues":{"id":99999999999999999999999,"value":"a"},"maxValues":{"id":100,"value":"z"},"nullCount":{"id":0,"value":5}}""",
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        var writer = new CheckpointWriter(fs);
        await writer.WriteCheckpointAsync(snapshot);

        string ckptPath = DeltaVersion.CheckpointPath(0);
        await using var file = await fs.OpenReadAsync(ckptPath);
        using var reader = new ParquetFileReader(file, ownsFile: false);

        bool foundStats = false;
        await foreach (var batch in reader.ReadAllAsync())
        {
            var spStruct = StatsParsed(batch);
            var spType = (Apache.Arrow.Types.StructType)spStruct.Data.DataType;
            var fieldNames = spType.Fields.Select(f => f.Name).ToList();

            var nrArray = (Int64Array)spStruct.Fields[fieldNames.IndexOf("numRecords")];
            var minStruct = (StructArray)spStruct.Fields[fieldNames.IndexOf("minValues")];
            var maxStruct = (StructArray)spStruct.Fields[fieldNames.IndexOf("maxValues")];
            var minNames = ((Apache.Arrow.Types.StructType)minStruct.Data.DataType)
                .Fields.Select(f => f.Name).ToList();

            for (int row = 0; row < batch.Length; row++)
            {
                if (nrArray.IsNull(row) || nrArray.GetValue(row) != 100)
                    continue;

                foundStats = true;

                // The unrepresentable min is null...
                var minId = (Int64Array)minStruct.Fields[minNames.IndexOf("id")];
                Assert.True(minId.IsNull(row));

                // ...while the rest of the row's stats are still on the same row.
                var minValue = (StringArray)minStruct.Fields[minNames.IndexOf("value")];
                Assert.Equal("a", minValue.GetString(row));
                var maxId = (Int64Array)maxStruct.Fields[minNames.IndexOf("id")];
                Assert.Equal(100L, maxId.GetValue(row));
            }
        }

        Assert.True(foundStats, "Should find numRecords=100 in stats_parsed");
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
