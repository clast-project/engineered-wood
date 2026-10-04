// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Array = Apache.Arrow.Array;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowStructType = Apache.Arrow.Types.StructType;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #453: statistics that made the pruner skip files holding matching rows. Item 1 (rewrites keyed their
/// statistics by logical name) was fixed by #459; these are items 2-5.
/// </summary>
public class StatsCorrectnessTests : IDisposable
{
    private readonly string _tempDir;

    public StatsCorrectnessTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_statsok_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    // ReadAllAsync skips files but does not filter rows, so a count below the file's row count means a file
    // holding matching rows was pruned.
    private static async Task<int> CountAsync(DeltaTable table, Predicate filter)
    {
        int n = 0;
        await foreach (var b in table.ReadAllAsync(columns: null, filter))
            n += b.Length;
        return n;
    }

    private static AddFile MakeAdd(string stats) => new()
    {
        Path = "part-0.parquet",
        PartitionValues = new Dictionary<string, string>(),
        Size = 1,
        ModificationTime = 0,
        DataChange = true,
        Stats = stats,
    };

    private static StructField Field(string name, string type, Dictionary<string, string>? metadata = null) => new()
    {
        Name = name,
        Type = new PrimitiveType { TypeName = type },
        Nullable = true,
        Metadata = metadata,
    };

    private static long NullCountAt(string stats, params string[] path)
    {
        using var doc = JsonDocument.Parse(stats);
        var e = doc.RootElement.GetProperty("nullCount");
        foreach (var p in path)
            e = e.GetProperty(p);
        return e.GetInt64();
    }

    // ── Item 2: a nested leaf's null count includes every null ancestor ──────────────────────────────

    private static Apache.Arrow.Schema DeepSchema()
    {
        var t = new ArrowStructType([new Apache.Arrow.Field("x", Int64Type.Default, true)]);
        var s = new ArrowStructType([new Apache.Arrow.Field("t", t, true)]);
        return new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("id", Int64Type.Default, false))
            .Field(new Apache.Arrow.Field("s", s, true))
            .Build();
    }

    private static ArrowBuffer Validity(params bool[] valid)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        foreach (var v in valid)
            b.Append(v);
        return b.Build();
    }

    // Rows: s = null (its t slot valid, x = 5), s = {t: {x: 7}}, s = {t: null}, s = {t: {x: null}}.
    private static RecordBatch DeepBatch()
    {
        var schema = DeepSchema();
        var sType = (ArrowStructType)schema.FieldsList[1].DataType;
        var tType = (ArrowStructType)sType.Fields[0].DataType;
        var x = new Int64Array.Builder().Append(5).Append(7).Append(9).AppendNull().Build();
        var t = new StructArray(tType, 4, [x], Validity(true, true, false, true), 1);
        var s = new StructArray(sType, 4, [t], Validity(false, true, true, true), 1);
        return new RecordBatch(schema, [new Int64Array.Builder().AppendRange([1, 2, 3, 4]).Build(), s], 4);
    }

    [Fact]
    public void NestedNullCount_CountsANullGrandparent()
    {
        string stats = Stats.StatsCollector.Collect(DeepBatch())!;
        // Row 1 (s null), row 3 (t null) and row 4 (x null) all read s.t.x as null.
        Assert.Equal(3, NullCountAt(stats, "s", "t", "x"));
    }

    // A sliced parent: only the slice's rows count, and the grandparent's validity is read at the slice's rows.
    [Fact]
    public void NestedNullCount_OfASlicedBatch_CountsOnlyItsRows()
    {
        var full = DeepBatch();
        var sliced = new RecordBatch(full.Schema,
            [((Array)full.Column(0)).Slice(1, 3), ((Array)full.Column(1)).Slice(1, 3)], 3); // rows 2-4

        // Row 3 (t null) and row 4 (x null); row 1's null s is outside the slice.
        string stats = Stats.StatsCollector.Collect(sliced)!;
        Assert.Equal(2, NullCountAt(stats, "s", "t", "x"));
    }

    // Offsets at EVERY level: the leaf, the middle struct and the batch are each a slice. Arrow applies a struct's
    // offset on top of its child's own (struct slot j is the child's logical element struct.offset + j, and the
    // child's accessors add its own offset), so both are honoured and neither is subtracted.
    //
    // Batch row r is s[1 + r] (s is sliced at 1), which is t's logical element 1 + r, which is tFull[2 + r] (t is
    // sliced at 1), whose x is x's logical element 2 + r (tFull has no offset), which is xFull[3 + r] (x is sliced at
    // 1). Subtracting each child's own offset instead would read tFull[1 + r] and xFull[1 + r]; each case is built
    // so that gives a different count.
    [Theory]
    [InlineData(false, 2)] // nulls at the leaf: xFull[3], xFull[4] are null; xFull[1], xFull[2] are not
    [InlineData(true, 2)]  // nulls at the middle struct: tFull[2], tFull[3] are null; tFull[1] is not
    public void NestedNullCount_WithAnOffsetAtEveryLevel_ReadsTheRightRows(bool nullsInTheStruct, long expected)
    {
        var xb = new Int64Array.Builder().Append(1).Append(2).Append(3);
        var xFull = nullsInTheStruct ? xb.Append(4).Append(5).Build() : xb.AppendNull().AppendNull().Build();
        var x = (Int64Array)xFull.Slice(1, 4);
        var tType = new ArrowStructType([new Apache.Arrow.Field("x", Int64Type.Default, true)]);
        var tFull = nullsInTheStruct
            ? new StructArray(tType, 4, [x], Validity(true, true, false, false), 2)
            : new StructArray(tType, 4, [x], ArrowBuffer.Empty);
        var t = (StructArray)tFull.Slice(1, 3);
        var sType = new ArrowStructType([new Apache.Arrow.Field("t", tType, true)]);
        var s = (StructArray)new StructArray(sType, 3, [t], ArrowBuffer.Empty).Slice(1, 2);
        var batch = new RecordBatch(new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("s", sType, true)).Build(), [s], 2);

        string stats = Stats.StatsCollector.Collect(batch)!;
        Assert.Equal(expected, NullCountAt(stats, "s", "t", "x"));
    }

    [Fact]
    public async Task IsNull_OnADeepLeafUnderANullGrandparent_KeepsTheFile()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), DeepSchema());
        // Rows: s = null over a valid t slot (x = 5), s = {t: {x: 7}}. s.t.x is null in the first row only.
        // Built unsliced: a slice used to hide this, since the nested count ran over the children's full length.
        var schema = DeepSchema();
        var sType = (ArrowStructType)schema.FieldsList[1].DataType;
        var tType = (ArrowStructType)sType.Fields[0].DataType;
        var t = new StructArray(tType, 2, [new Int64Array.Builder().Append(5).Append(7).Build()], ArrowBuffer.Empty);
        var s = new StructArray(sType, 2, [t], Validity(false, true), 1);
        await table.WriteAsync([new RecordBatch(schema, [new Int64Array.Builder().AppendRange([1, 2]).Build(), s], 2)]);

        Assert.Equal(2, await CountAsync(table, Ex.IsNull("s.t.x")));
    }

    // ── Item 3: string bounds are computed in code-point order ───────────────────────────────────────

    private const string Emoji = "\U0001F600";        // supplementary: a surrogate pair in UTF-16
    private const string HalfwidthStop = "｡";    // above the surrogate block as a code unit

    [Fact]
    public void StringBounds_AreInCodePointOrder()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("s", StringType.Default, true)).Build();
        var batch = new RecordBatch(schema,
            [new StringArray.Builder().Append("a").Append(HalfwidthStop).Append(Emoji).Build()], 3);

        string stats = Stats.StatsCollector.Collect(batch)!;
        using var doc = JsonDocument.Parse(stats);
        Assert.Equal("a", doc.RootElement.GetProperty("minValues").GetProperty("s").GetString());
        Assert.Equal(Emoji, doc.RootElement.GetProperty("maxValues").GetProperty("s").GetString());
    }

    // The merge across batches orders the same way.
    [Fact]
    public void StringBounds_MergedAcrossBatches_AreInCodePointOrder()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("s", StringType.Default, true)).Build();
        RecordBatch One(string v) => new(schema, [new StringArray.Builder().Append(v).Build()], 1);

        string stats = Stats.StatsCollector.Collect([One(Emoji), One(HalfwidthStop)])!;
        using var doc = JsonDocument.Parse(stats);
        Assert.Equal(HalfwidthStop, doc.RootElement.GetProperty("minValues").GetProperty("s").GetString());
        Assert.Equal(Emoji, doc.RootElement.GetProperty("maxValues").GetProperty("s").GetString());
    }

    [Fact]
    public async Task Equality_OnASupplementaryString_KeepsTheFile()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("s", StringType.Default, true)).Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);
        await table.WriteAsync([new RecordBatch(schema,
            [new StringArray.Builder().Append("a").Append(HalfwidthStop).Append(Emoji).Build()], 3)]);

        Assert.Equal(3, await CountAsync(table, Ex.Equal("s", LiteralValue.Of(Emoji))));
    }

    // ── Item 4: Spark's timestamp max is truncated to the millisecond ────────────────────────────────

    private const string SparkStats =
        """{"numRecords":1,"minValues":{"ts":"2024-01-01T00:00:00.123Z"},"maxValues":{"ts":"2024-01-01T00:00:00.123Z"},"nullCount":{"ts":0}}""";

    // The file holds 00:00:00.123456; Spark records both bounds as .123.
    private static readonly DateTimeOffset InFile =
        new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).AddTicks(1_234_560);

    [Theory]
    [InlineData("timestamp")]
    [InlineData("timestamp_ntz")]
    public void SparkTruncatedTimestampMax_KeepsASubMillisecondValue(string type)
    {
        var pruner = new DeltaFilePruner(new DeltaStructType { Fields = [Field("ts", type)] }, []);
        var add = MakeAdd(SparkStats);

        Assert.True(pruner.ShouldInclude(add, Ex.Equal("ts", LiteralValue.Of(InFile))));
        Assert.True(pruner.ShouldInclude(add, Ex.GreaterThanOrEqual("ts", LiteralValue.Of(InFile.AddTicks(-1000)))));
        // A full millisecond past the truncated max is still pruned.
        Assert.False(pruner.ShouldInclude(add, Ex.GreaterThan("ts", LiteralValue.Of(InFile.AddMilliseconds(1)))));
    }

    [Fact]
    public async Task SparkTruncatedTimestampMax_InTheCheckpoint_KeepsASubMillisecondValue()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        string schemaJson = """{"type":"struct","fields":[{"name":"ts","type":"timestamp","nullable":true,"metadata":{}}]}""";
        await log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "ts", Format = Format.Parquet, SchemaString = schemaJson, PartitionColumns = [],
                Configuration = new Dictionary<string, string> { ["delta.checkpoint.writeStatsAsStruct"] = "true" },
            },
            MakeAdd(SparkStats),
        ]);
        await new CheckpointWriter(fs).WriteCheckpointAsync(await SnapshotBuilder.BuildAsync(log));
        var reader = new CheckpointReader(fs);
        var add = (await reader.ReadCheckpointAsync((await reader.ReadLastCheckpointAsync())!))
            .OfType<AddFile>().Single();
        Assert.NotNull(add.TypedStats);

        var pruner = new DeltaFilePruner(DeltaSchemaSerializer.Parse(schemaJson), []);
        Assert.True(pruner.ShouldInclude(add with { Stats = null }, Ex.Equal("ts", LiteralValue.Of(InFile))));
    }

    [Fact]
    public async Task SparkTruncatedTimestampMax_EndToEnd_KeepsTheFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var tsType = new TimestampType(TimeUnit.Microsecond, "UTC");
        var fileSchema = new Apache.Arrow.Schema.Builder().Field(new Apache.Arrow.Field("ts", tsType, true)).Build();
        await using (var file = await fs.CreateAsync("ts.parquet"))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            await writer.WriteRowGroupAsync(new RecordBatch(fileSchema,
                [new TimestampArray.Builder(tsType).Append(InFile).Build()], 1));
        }
        await new TransactionLog(fs).WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "ts", Format = Format.Parquet, PartitionColumns = [],
                SchemaString = """{"type":"struct","fields":[{"name":"ts","type":"timestamp","nullable":true,"metadata":{}}]}""",
            },
            MakeAdd(SparkStats) with
            {
                Path = "ts.parquet",
                Size = new FileInfo(Path.Combine(_tempDir, "ts.parquet")).Length,
            },
        ]);

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(1, await CountAsync(table, Ex.GreaterThanOrEqual("ts", LiteralValue.Of(InFile.AddTicks(-10)))));
    }

    // ── Item 5: float -> double widening ─────────────────────────────────────────────────────────────

    private static Dictionary<string, string> TypeChanged(string from, string to) => new()
    {
        [Schema.TypeWidening.TypeChangesKey] = $$"""[{"fromType":"{{from}}","toType":"{{to}}"}]""",
    };

    // An old file's bound is the float's shortest text, "0.1", which decodes to the double 0.1; the widened row
    // value is (double)0.1f = 0.10000000149..., outside [0.1, 0.1].
    [Fact]
    public void FloatWidenedToDouble_OldFloatBounds_DoNotPrune()
    {
        var schema = new DeltaStructType { Fields = [Field("v", "double", TypeChanged("float", "double"))] };
        var add = MakeAdd("""{"numRecords":1,"minValues":{"v":0.1},"maxValues":{"v":0.1},"nullCount":{"v":0}}""");
        var pruner = new DeltaFilePruner(schema, []);

        Assert.True(pruner.ShouldInclude(add, Ex.Equal("v", LiteralValue.Of((double)0.1f))));
        // The null count is exact whatever the type was, so IS NULL still prunes.
        Assert.False(pruner.ShouldInclude(add, Ex.IsNull("v")));
    }

    [Fact]
    public void FloatWidenedToDouble_InsideAStruct_OldFloatBounds_DoNotPrune()
    {
        var schema = new DeltaStructType
        {
            Fields =
            [
                new StructField
                {
                    Name = "s", Nullable = true,
                    Type = new DeltaStructType { Fields = [Field("v", "double", TypeChanged("float", "double"))] },
                },
            ],
        };
        var add = MakeAdd("""{"numRecords":1,"minValues":{"s":{"v":0.1}},"maxValues":{"s":{"v":0.1}},"nullCount":{"s":{"v":0}}}""");

        Assert.True(new DeltaFilePruner(schema, []).ShouldInclude(add, Ex.Equal("s.v", LiteralValue.Of((double)0.1f))));
    }

    // Integer bounds decode exactly at the wider type, so those columns keep pruning.
    [Fact]
    public void IntWidenedToLong_OldBounds_StillPrune()
    {
        var schema = new DeltaStructType { Fields = [Field("v", "long", TypeChanged("integer", "long"))] };
        var add = MakeAdd("""{"numRecords":1,"minValues":{"v":5},"maxValues":{"v":5},"nullCount":{"v":0}}""");

        Assert.False(new DeltaFilePruner(schema, []).ShouldInclude(add, Ex.Equal("v", LiteralValue.Of(6L))));
    }

    [Fact]
    public async Task FloatWidenedToDouble_EndToEnd_KeepsTheFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Apache.Arrow.Field("v", FloatType.Default, true)).Build();
        await using (var file = await fs.CreateAsync("old.parquet"))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [new FloatArray.Builder().Append(0.1f).Build()], 1));
        }
        await new TransactionLog(fs).WriteCommitAsync(0,
        [
            new ProtocolAction
            {
                MinReaderVersion = 3, MinWriterVersion = 7,
                ReaderFeatures = ["typeWidening"], WriterFeatures = ["typeWidening"],
            },
            new MetadataAction
            {
                Id = "fd", Format = Format.Parquet, PartitionColumns = [],
                SchemaString = """
                    {"type":"struct","fields":[{"name":"v","type":"double","nullable":true,
                      "metadata":{"delta.typeChanges":"[{\"fromType\":\"float\",\"toType\":\"double\"}]"}}]}
                    """,
                Configuration = new Dictionary<string, string> { [Schema.TypeWidening.EnableKey] = "true" },
            },
            MakeAdd("""{"numRecords":1,"minValues":{"v":0.1},"maxValues":{"v":0.1},"nullCount":{"v":0}}""") with
            {
                Path = "old.parquet",
                Size = new FileInfo(Path.Combine(_tempDir, "old.parquet")).Length,
            },
        ]);

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(1, await CountAsync(table, Ex.Equal("v", LiteralValue.Of((double)0.1f))));
    }
}
