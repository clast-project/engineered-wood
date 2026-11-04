// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ParquetLogicalType = EngineeredWood.Parquet.Metadata.LogicalType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Variant <c>minValues</c> / <c>maxValues</c> (#675): a shredded file's per-path bounds, derived from its footer
/// as delta-spark derives them, and only on a table that supports variant shredding.
/// </summary>
public class VariantStatisticsTests : IDisposable
{
    private readonly string _tempDir;

    public VariantStatisticsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_stats_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema VariantSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

    private static Dictionary<string, string> Enabled() => new() { ["delta.enableVariantShredding"] = "true" };

    private static DeltaTableOptions Shredding(ShredSchema? schema = null, int rowGroupRows = 0) =>
        DeltaTableOptions.Default with
        {
            ParquetWriteOptions = (ParquetWriteOptions.Default with
            {
                ShredVariants = schema is null ? ShredOptions.Default : null,
                VariantShredSchemas = schema is null ? null : new Dictionary<string, ShredSchema> { ["v"] = schema },
            }) is var o && rowGroupRows > 0 ? o with { RowGroupMaxRows = rowGroupRows } : o,
        };

    private static VariantValue Obj(params (string Key, VariantValue Value)[] fields) =>
        VariantValue.FromObject(fields.ToDictionary(f => f.Key, f => f.Value));

    private static RecordBatch Batch(params VariantValue?[] values)
    {
        var ids = new Int64Array.Builder();
        var vb = new VariantArray.Builder();
        for (int i = 0; i < values.Length; i++)
        {
            ids.Append(i);
            if (values[i] is { } v)
                vb.Append(v);
            else
                vb.AppendNull();
        }
        return new RecordBatch(VariantSchema(), [ids.Build(), vb.Build(allocator: null)], values.Length);
    }

    /// <summary>Writes the rows as one commit and returns that commit's add-file stats.</summary>
    private async Task<JsonElement> WriteAndReadStatsAsync(
        DeltaTableOptions options, Dictionary<string, string>? configuration, params VariantValue?[] values)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, VariantSchema(), configuration: configuration, options: options))
            await table.WriteAsync([Batch(values)]);
        var add = (await new TransactionLog(fs).ReadCommitAsync(1)).OfType<AddFile>().Single();
        return JsonDocument.Parse(add.Stats!).RootElement.Clone();
    }

    /// <summary>A Z85 variant bound as path → the JSON of its value.</summary>
    private static Dictionary<string, string> Decode(JsonElement bound)
    {
        Assert.Equal(JsonValueKind.String, bound.ValueKind);
        Assert.True(VariantStatsEncoding.TryDecode(bound.GetString()!, out byte[] metadata, out byte[] value),
            $"not a z85 variant: {bound}");
        using var doc = JsonDocument.Parse(VariantJsonWriter.ToJson(metadata, value, indented: false));
        return doc.RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetRawText());
    }

    private static (Dictionary<string, string> Min, Dictionary<string, string> Max) Bounds(JsonElement stats) =>
        (Decode(stats.GetProperty("minValues").GetProperty("v")), Decode(stats.GetProperty("maxValues").GetProperty("v")));

    [Fact]
    public async Task ShreddedWrite_RecordsEachUniformPathsBounds()
    {
        var stats = await WriteAndReadStatsAsync(Shredding(), Enabled(),
            Obj(("a", VariantValue.FromInt32(5)), ("b", VariantValue.FromString("x")),
                ("c", Obj(("d", VariantValue.FromDouble(1.5))))),
            Obj(("a", VariantValue.FromInt32(-2)), ("b", VariantValue.FromString("y")),
                ("c", Obj(("d", VariantValue.FromDouble(0.25))))),
            null);

        var (min, max) = Bounds(stats);
        Assert.Equal(new Dictionary<string, string>
        {
            ["$['a']"] = "-2", ["$['b']"] = "\"x\"", ["$['c']['d']"] = "0.25",
        }, min);
        Assert.Equal(new Dictionary<string, string>
        {
            ["$['a']"] = "5", ["$['b']"] = "\"y\"", ["$['c']['d']"] = "1.5",
        }, max);
        Assert.Equal(1, stats.GetProperty("nullCount").GetProperty("v").GetInt64());
    }

    [Fact]
    public async Task APathOfMixedTypes_IsLeftOut_AndItsSiblingsKeepTheirs()
    {
        // `m` is an int in one row and a string in the other, so one of them is in the residual; `a` is uniform.
        var schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
        {
            ["a"] = ShredSchema.Primitive(ShredType.Int64),
            ["m"] = ShredSchema.Primitive(ShredType.Int64),
        });
        var stats = await WriteAndReadStatsAsync(Shredding(schema), Enabled(),
            Obj(("a", VariantValue.FromInt64(1)), ("m", VariantValue.FromInt64(7))),
            Obj(("a", VariantValue.FromInt64(3)), ("m", VariantValue.FromString("seven"))));

        var (min, max) = Bounds(stats);
        Assert.Equal(["$['a']"], min.Keys);
        Assert.Equal("1", min["$['a']"]);
        Assert.Equal("3", max["$['a']"]);
    }

    [Fact]
    public async Task APathMissingFromSomeRows_IsStillBounded()
    {
        var schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
        {
            ["a"] = ShredSchema.Primitive(ShredType.Int64),
        });
        var stats = await WriteAndReadStatsAsync(Shredding(schema), Enabled(),
            Obj(("a", VariantValue.FromInt64(4))),
            Obj(("z", VariantValue.FromString("no a here"))),
            VariantValue.FromString("not even an object"));

        var (min, max) = Bounds(stats);
        Assert.Equal("4", min["$['a']"]);
        Assert.Equal("4", max["$['a']"]);
    }

    [Fact]
    public async Task PathsInsideAnArray_AreNotBounded()
    {
        var stats = await WriteAndReadStatsAsync(Shredding(), Enabled(),
            Obj(("a", VariantValue.FromInt32(1)), ("arr", VariantValue.FromArray([VariantValue.FromInt32(1), VariantValue.FromInt32(2)]))),
            Obj(("a", VariantValue.FromInt32(2)), ("arr", VariantValue.FromArray([VariantValue.FromInt32(3)]))));

        var (min, _) = Bounds(stats);
        Assert.Equal(["$['a']"], min.Keys);
    }

    [Fact]
    public async Task LongStrings_AreCutToThePrefixLength_TheMaxWithTheLargestCodePoint()
    {
        var config = Enabled();
        config["delta.dataSkippingStringPrefixLength"] = "4";
        var stats = await WriteAndReadStatsAsync(Shredding(), config,
            Obj(("s", VariantValue.FromString("abcdefgh"))),
            Obj(("s", VariantValue.FromString("abzzzzzz"))));

        var (min, max) = Bounds(stats);
        Assert.Equal("\"abcd\"", min["$['s']"]);
        Assert.Equal(JsonSerializer.Serialize("abzz\U0010FFFF"), max["$['s']"]);
    }

    [Fact]
    public async Task SeveralRowGroups_MergeIntoOneBoundPerPath()
    {
        var schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
        {
            ["a"] = ShredSchema.Primitive(ShredType.Int64),
        });
        var rows = Enumerable.Range(0, 10).Select(i => (VariantValue?)Obj(("a", VariantValue.FromInt64(100 - i * 7)))).ToArray();
        var stats = await WriteAndReadStatsAsync(Shredding(schema, rowGroupRows: 3), Enabled(), rows);

        var (min, max) = Bounds(stats);
        Assert.Equal("37", min["$['a']"]);
        Assert.Equal("100", max["$['a']"]);

        var file = Directory.GetFiles(_tempDir, "*.parquet").Single();
        await using var raf = new LocalRandomAccessFile(file);
        await using var reader = new ParquetFileReader(raf, ownsFile: false);
        Assert.True((await reader.ReadMetadataAsync()).RowGroups.Count > 1);
    }

    [Fact]
    public async Task OneRowGroupWithAStrayType_DropsThePathForTheWholeFile()
    {
        var schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
        {
            ["a"] = ShredSchema.Primitive(ShredType.Int64),
        });
        var rows = Enumerable.Range(0, 9)
            .Select(i => (VariantValue?)Obj(("a", i == 7 ? VariantValue.FromString("odd") : VariantValue.FromInt64(i))))
            .ToArray();
        var stats = await WriteAndReadStatsAsync(Shredding(schema, rowGroupRows: 3), Enabled(), rows);

        // The other row groups are uniform, but the bound describes the file.
        Assert.False(stats.GetProperty("minValues").TryGetProperty("v", out _), stats.GetRawText());
    }

    [Fact]
    public async Task TypedScalars_KeepTheirVariantTypes()
    {
        var day = new DateTime(2026, 11, 4);
        var schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
        {
            ["date"] = ShredSchema.Primitive(ShredType.Date),
            ["ts"] = ShredSchema.Primitive(ShredType.Timestamp),
            ["dec"] = ShredSchema.FromArrowType(new Decimal32Type(9, 2)),
            ["big"] = ShredSchema.FromArrowType(new Decimal64Type(18, 3)),
        });
        var stats = await WriteAndReadStatsAsync(Shredding(schema), Enabled(),
            Obj(("date", VariantValue.FromDate(day)), ("ts", VariantValue.FromTimestamp(new DateTimeOffset(day, TimeSpan.Zero))),
                ("dec", VariantValue.FromDecimal4(12.30m)), ("big", VariantValue.FromDecimal8(123456789012345.500m))),
            Obj(("date", VariantValue.FromDate(day.AddDays(1))), ("ts", VariantValue.FromTimestamp(new DateTimeOffset(day.AddHours(1), TimeSpan.Zero))),
                ("dec", VariantValue.FromDecimal4(-1.05m)), ("big", VariantValue.FromDecimal8(-1.000m))));

        var (min, max) = Bounds(stats);
        Assert.Equal("\"2026-11-04\"", min["$['date']"]);
        Assert.Equal("\"2026-11-05\"", max["$['date']"]);
        Assert.Equal("-1.05", min["$['dec']"]);
        Assert.Equal("12.30", max["$['dec']"]);
        Assert.Equal("-1.000", min["$['big']"]);
        Assert.Equal("123456789012345.500", max["$['big']"]);
        Assert.StartsWith("\"2026-11-04T00:00:00", min["$['ts']"]);
        Assert.StartsWith("\"2026-11-04T01:00:00", max["$['ts']"]);
    }

    [Fact]
    public async Task UnshreddedWrite_GetsNullCountOnly()
    {
        var stats = await WriteAndReadStatsAsync(DeltaTableOptions.Default, Enabled(),
            Obj(("a", VariantValue.FromInt32(1))), null);

        Assert.False(stats.GetProperty("minValues").TryGetProperty("v", out _));
        Assert.False(stats.GetProperty("maxValues").TryGetProperty("v", out _));
        Assert.Equal(1, stats.GetProperty("nullCount").GetProperty("v").GetInt64());
    }

    [Fact]
    public async Task AColumnOutsideTheStatsColumns_GetsNoBounds()
    {
        var config = Enabled();
        config["delta.dataSkippingStatsColumns"] = "id";
        var stats = await WriteAndReadStatsAsync(Shredding(), config, Obj(("a", VariantValue.FromInt32(1))));

        Assert.False(stats.GetProperty("minValues").TryGetProperty("v", out _));
    }

    // ── Checkpoints ──────────────────────────────────────────────────────────────────────────────────

    // delta-spark 4.3.1's own bounds for {"a":10|20, "b":"p"|"q", "c":{"d":8.5|9.5}, "m":2|3} (measured, with
    // collectVariantDataSkippingStats, parseFooterForStats and stats.limitPerVariant=100).
    private const string SparkMin =
        "0r-xb5F(Dktpi(ct{JS4v<+L^cUaPgbWhT8cTw04cV7kp0T5M80rS-g0U(:WA3LD:000000000o3ig5a000007Y<Wq0000000000";
    private const string SparkMax =
        "0r-xb5F(Dktpi(ct{JS4v<+L^cUaPgbWhT8cTw04cV7kp0T5M80rS-g0U(:WAu>M(000000000o6Awak000007Y%:r0000000000";

    [Fact]
    public void SparksZ85Bound_DecodesToItsPaths()
    {
        var min = Decode(JsonDocument.Parse(JsonSerializer.Serialize(SparkMin)).RootElement);
        Assert.Equal(new Dictionary<string, string>
        {
            ["$['a']"] = "10", ["$['b']"] = "\"p\"", ["$['c']['d']"] = "8.5", ["$['m']"] = "2",
        }, min);

        // Spark's empty bound (stats.limitPerVariant = 0, its default) is the empty object.
        Assert.Empty(Decode(JsonDocument.Parse("\"0rr9300000\"").RootElement));
        Assert.False(VariantStatsEncoding.TryDecode("0rr93", out _, out _));
        Assert.False(VariantStatsEncoding.TryDecode("not z85!!!", out _, out _));
    }

    /// <summary>A one-file table, committed by hand with the given stats, then checkpointed.</summary>
    private async Task<AddFile> CheckpointedAddAsync(string stats, bool shredding, bool writeStatsAsJson = true)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        string[] features = shredding ? ["variantShredding", "variantType"] : ["variantType"];
        await log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = features, WriterFeatures = features },
            new MetadataAction
            {
                Id = "variantstats", Format = Format.Parquet, PartitionColumns = [],
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"v","type":"variant","nullable":true,"metadata":{}}]}""",
                Configuration = new Dictionary<string, string>
                {
                    ["delta.checkpoint.writeStatsAsStruct"] = "true",
                    ["delta.checkpoint.writeStatsAsJson"] = writeStatsAsJson ? "true" : "false",
                },
            },
            new AddFile
            {
                Path = "part-0.parquet", PartitionValues = new Dictionary<string, string>(), Size = 1,
                ModificationTime = 1, DataChange = true, Stats = stats,
            },
        ]);
        await new CheckpointWriter(fs).WriteCheckpointAsync(await Snapshot.SnapshotBuilder.BuildAsync(log));
        var reader = new CheckpointReader(fs);
        return (await reader.ReadCheckpointAsync((await reader.ReadLastCheckpointAsync())!)).OfType<AddFile>().Single();
    }

    private static string SparkStats =>
        "{\"numRecords\":2,\"minValues\":{\"id\":4,\"v\":\"" + SparkMin + "\"},\"maxValues\":{\"id\":5,\"v\":\""
        + SparkMax + "\"},\"nullCount\":{\"id\":0,\"v\":0}}";

    [Fact]
    public async Task Checkpoint_CarriesTheVariantBoundsInStatsParsed_AsAVariantGroup()
    {
        var add = await CheckpointedAddAsync(SparkStats, shredding: true);

        // The typed copy alone rebuilds the original strings.
        using var rebuilt = JsonDocument.Parse(add.TypedStats!.View.BuildStatsJson(add.TypedStats.Row)!);
        Assert.Equal(SparkMin, rebuilt.RootElement.GetProperty("minValues").GetProperty("v").GetString());
        Assert.Equal(SparkMax, rebuilt.RootElement.GetProperty("maxValues").GetProperty("v").GetString());
        Assert.Equal(4, rebuilt.RootElement.GetProperty("minValues").GetProperty("id").GetInt64());

        // Laid out as delta-spark lays it out: a Variant-annotated group under stats_parsed.minValues.
        var checkpoint = Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.checkpoint.parquet").Single();
        await using var raf = new LocalRandomAccessFile(checkpoint);
        await using var reader = new ParquetFileReader(raf, ownsFile: false);
        var schema = (await reader.ReadMetadataAsync()).Schema;
        int minValues = schema.ToList().FindIndex(e => e.Name == "minValues");
        Assert.True(minValues >= 0);
        Assert.Contains(schema.Skip(minValues), e => e.Name == "v" && e.LogicalType is ParquetLogicalType.VariantType);
    }

    [Fact]
    public async Task Checkpoint_WithoutJsonStats_KeepsTheVariantBounds()
    {
        var add = await CheckpointedAddAsync(SparkStats, shredding: true, writeStatsAsJson: false);

        Assert.Null(add.Stats);
        using var stats = JsonDocument.Parse(add.GetStatsJson()!);
        Assert.Equal(SparkMin, stats.RootElement.GetProperty("minValues").GetProperty("v").GetString());
        Assert.Equal(SparkMax, stats.RootElement.GetProperty("maxValues").GetProperty("v").GetString());
    }

    [Fact]
    public async Task Checkpoint_OfATableWithoutShredding_HasNoVariantBoundColumn()
    {
        var add = await CheckpointedAddAsync(SparkStats, shredding: false);

        Assert.False(add.TypedStats!.View.HasBound("v"));
        Assert.True(add.TypedStats.View.HasNullCount("v"));
        Assert.True(add.TypedStats.View.HasBound("id"));
    }

    [Fact]
    public async Task Checkpoint_ABoundThatIsNotZ85_IsNullInStatsParsed_AndKeptInTheJson()
    {
        const string stats =
            "{\"numRecords\":1,\"minValues\":{\"id\":1,\"v\":\"garbage\"},\"maxValues\":{\"id\":1,\"v\":{\"x\":1}},\"nullCount\":{\"id\":0,\"v\":0}}";
        var add = await CheckpointedAddAsync(stats, shredding: true);

        Assert.Equal(stats, add.Stats);
        using var rebuilt = JsonDocument.Parse(add.TypedStats!.View.BuildStatsJson(add.TypedStats.Row)!);
        Assert.False(rebuilt.RootElement.GetProperty("minValues").TryGetProperty("v", out _));
        Assert.Equal(1, rebuilt.RootElement.GetProperty("minValues").GetProperty("id").GetInt64());
    }

    [Fact]
    public async Task EwWrittenBounds_SurviveACheckpoint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema(), configuration: Enabled(), options: Shredding());
        await table.WriteAsync([Batch(Obj(("a", VariantValue.FromInt32(3))), Obj(("a", VariantValue.FromInt32(9))))]);
        string written = (await new TransactionLog(fs).ReadCommitAsync(1)).OfType<AddFile>().Single().Stats!;
        await table.CheckpointAsync();

        var reader = new CheckpointReader(fs);
        var add = (await reader.ReadCheckpointAsync((await reader.ReadLastCheckpointAsync())!)).OfType<AddFile>().Single();
        using var rebuilt = JsonDocument.Parse(add.TypedStats!.View.BuildStatsJson(add.TypedStats.Row)!);
        using var original = JsonDocument.Parse(written);
        Assert.Equal(
            original.RootElement.GetProperty("minValues").GetProperty("v").GetString(),
            rebuilt.RootElement.GetProperty("minValues").GetProperty("v").GetString());
        Assert.Equal("9", Bounds(rebuilt.RootElement).Max["$['a']"]);
    }

    // ── Bounds that would not bound (#676 review) ────────────────────────────────────────────────────

    private static ShredSchema Fields(params (string Name, ShredType Type)[] fields) =>
        ShredSchema.ForObject(fields.ToDictionary(f => f.Name, f => ShredSchema.Primitive(f.Type)));

    [Fact]
    public async Task AFloatingPointPathHoldingANaN_IsLeftOut()
    {
        // The footer's min/max skip the NaN (it is counted apart), but NaN sorts above every number.
        var stats = await WriteAndReadStatsAsync(
            Shredding(Fields(("x", ShredType.Double), ("a", ShredType.Int64))), Enabled(),
            Obj(("x", VariantValue.FromDouble(1.0)), ("a", VariantValue.FromInt64(1))),
            Obj(("x", VariantValue.FromDouble(double.NaN)), ("a", VariantValue.FromInt64(2))));

        var (min, _) = Bounds(stats);
        Assert.Equal(["$['a']"], min.Keys);
    }

    [Fact]
    public async Task AStringBoundCutInsideACharacter_IsLeftOut()
    {
        // 22 three-byte characters: the Parquet writer cuts its statistics at 64 bytes, inside the 22nd.
        string euros = new('€', 22);
        var config = Enabled();
        config["delta.dataSkippingStringPrefixLength"] = "100";
        var stats = await WriteAndReadStatsAsync(
            Shredding(Fields(("s", ShredType.String), ("a", ShredType.Int64))), config,
            Obj(("s", VariantValue.FromString(euros)), ("a", VariantValue.FromInt64(1))));

        var (min, _) = Bounds(stats);
        Assert.Equal(["$['a']"], min.Keys);
    }

    [Fact]
    public async Task ACutMaximumThatContinuesWithTheLargestCodePoint_IsLeftOut()
    {
        // Cut to "abcd" + U+10FFFF, the maximum "abcd" + U+10FFFF + "x" would sit ABOVE its own bound.
        var config = Enabled();
        config["delta.dataSkippingStringPrefixLength"] = "4";
        var stats = await WriteAndReadStatsAsync(
            Shredding(Fields(("s", ShredType.String), ("a", ShredType.Int64))), config,
            Obj(("s", VariantValue.FromString("abcd\U0010FFFFx")), ("a", VariantValue.FromInt64(1))));

        var (_, max) = Bounds(stats);
        Assert.Equal(["$['a']"], max.Keys);
    }

    [Fact]
    public async Task AZeroStringPrefix_BoundsStringsWithTheEmptyPrefix()
    {
        var config = Enabled();
        config["delta.dataSkippingStringPrefixLength"] = "0";
        var stats = await WriteAndReadStatsAsync(
            Shredding(Fields(("s", ShredType.String))), config,
            Obj(("s", VariantValue.FromString("abc"))), Obj(("s", VariantValue.FromString("xyz"))));

        var (min, max) = Bounds(stats);
        Assert.Equal("\"\"", min["$['s']"]);
        Assert.Equal(JsonSerializer.Serialize("\U0010FFFF"), max["$['s']"]);
    }

    [Fact]
    public async Task AFloatingPointPathWhoseNaNCountIsUnknown_IsLeftOut()
    {
        // EW's writer always records the count; another writer may not, and then a NaN may hide behind the max.
        await WriteAndReadStatsAsync(
            Shredding(Fields(("x", ShredType.Double), ("a", ShredType.Int64))), Enabled(),
            Obj(("x", VariantValue.FromDouble(1.0)), ("a", VariantValue.FromInt64(1))));
        var file = Directory.GetFiles(_tempDir, "*.parquet").Single();
        Parquet.Metadata.FileMetaData footer;
        await using (var raf = new LocalRandomAccessFile(file))
        await using (var reader = new ParquetFileReader(raf, ownsFile: false))
            footer = await reader.ReadMetadataAsync();

        var known = Stats.VariantFooterStats.Compute(footer, ["v"], stringPrefixLength: 32);
        Assert.Equal(["$['a']", "$['x']"], Decode(JsonSerializer.SerializeToElement(known["v"].Min)).Keys.OrderBy(k => k, StringComparer.Ordinal));

        foreach (var chunk in footer.RowGroups.SelectMany(g => g.Columns))
        {
            if (chunk.MetaData is { Statistics: { NanCount: not null } s } meta)
            {
                meta.Statistics = new Parquet.Metadata.Statistics
                {
                    Min = s.Min, Max = s.Max, MinValue = s.MinValue, MaxValue = s.MaxValue, NullCount = s.NullCount,
                    IsMinValueExact = s.IsMinValueExact, IsMaxValueExact = s.IsMaxValueExact, NanCount = null,
                };
            }
        }
        var unknown = Stats.VariantFooterStats.Compute(footer, ["v"], stringPrefixLength: 32);
        Assert.Equal(["$['a']"], Decode(JsonSerializer.SerializeToElement(unknown["v"].Min)).Keys);
    }
}
