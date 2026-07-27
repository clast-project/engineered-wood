// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// File pruning driven by a checkpoint's typed <c>add.stats_parsed</c> columns instead of its JSON
/// <c>stats</c> string. The typed path exists to avoid parsing a whole statistics blob to answer a
/// predicate that names one column — so what these tests are really protecting is that the shortcut
/// reaches the SAME verdict as the long way round. A faster pruner that disagrees drops files.
/// </summary>
public class TypedCheckpointStatsTests : IDisposable
{
    private readonly string _tempDir;

    public TypedCheckpointStatsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_typed_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private const string MixedSchemaJson = """
        {"type":"struct","fields":[
          {"name":"id","type":"long","nullable":false,"metadata":{}},
          {"name":"region","type":"string","nullable":true,"metadata":{}},
          {"name":"amount","type":"decimal(12,2)","nullable":true,"metadata":{}},
          {"name":"d","type":"date","nullable":true,"metadata":{}},
          {"name":"ts","type":"timestamp","nullable":true,"metadata":{}},
          {"name":"flag","type":"boolean","nullable":true,"metadata":{}},
          {"name":"payload","type":{"type":"struct","fields":[
            {"name":"score","type":"long","nullable":true,"metadata":{}}]},
           "nullable":true,"metadata":{}}]}
        """;

    private static DeltaStructType MixedSchema() =>
        EngineeredWood.DeltaLake.Schema.SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int64Type.Default, false))
                .Field(new Field("region", StringType.Default, true))
                .Field(new Field("amount", new Decimal128Type(12, 2), true))
                .Field(new Field("d", Date32Type.Default, true))
                .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true))
                .Field(new Field("flag", BooleanType.Default, true))
                .Field(new Field("payload", new Apache.Arrow.Types.StructType(
                    [new Field("score", Int64Type.Default, true)]), true))
                .Build());

    /// <summary>
    /// Writes a checkpoint holding one add per stats blob and reads the adds back out of it, so each
    /// carries the typed reference the pruner reads.
    /// </summary>
    private async Task<List<AddFile>> CheckpointedAdds(
        IReadOnlyList<string?> statsBlobs,
        IReadOnlyDictionary<string, string>? configuration = null,
        string schemaJson = MixedSchemaJson)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        var actions = new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "typed-stats",
                Format = Format.Parquet,
                SchemaString = schemaJson,
                PartitionColumns = [],
                Configuration = configuration,
            },
        };

        for (int i = 0; i < statsBlobs.Count; i++)
        {
            actions.Add(new AddFile
            {
                Path = $"part-{i:D3}.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1, ModificationTime = 1, DataChange = true,
                Stats = statsBlobs[i],
            });
        }

        await log.WriteCommitAsync(0, actions);
        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        var reader = new CheckpointReader(fs);
        var adds = (await reader.ReadCheckpointAsync((await reader.ReadLastCheckpointAsync())!))
            .OfType<AddFile>().ToList();
        Assert.Equal(statsBlobs.Count, adds.Count);
        return adds;
    }

    private static readonly string?[] VariedBlobs =
    [
        // Full bounds on every column.
        """{"numRecords":10,"minValues":{"id":0,"region":"aa","amount":1.25,"d":"2020-01-01","ts":"2020-01-01T00:00:00.000000Z","flag":false,"payload":{"score":5}},"maxValues":{"id":9,"region":"mm","amount":5.75,"d":"2020-12-31","ts":"2020-12-31T23:59:59.999999Z","flag":false,"payload":{"score":50}},"nullCount":{"id":0,"region":1,"amount":2,"d":0,"ts":0,"flag":0,"payload":{"score":1}}}""",
        // A disjoint range, negative decimal, true flag.
        """{"numRecords":20,"minValues":{"id":100,"region":"nn","amount":-9.99,"d":"2021-06-15","ts":"2021-06-15T12:00:00.000000Z","flag":true,"payload":{"score":500}},"maxValues":{"id":199,"region":"zz","amount":1234.56,"d":"2021-06-16","ts":"2021-06-16T12:00:00.000000Z","flag":true,"payload":{"score":900}},"nullCount":{"id":0,"region":0,"amount":0,"d":0,"ts":0,"flag":0,"payload":{"score":0}}}""",
        // Only some columns have bounds.
        """{"numRecords":5,"minValues":{"id":1000},"maxValues":{"id":1005},"nullCount":{"id":0}}""",
        // numRecords only.
        """{"numRecords":7}""",
        // Explicit JSON nulls, and an all-null column.
        """{"numRecords":3,"minValues":{"id":null,"region":null},"maxValues":{"id":null},"nullCount":{"id":3}}""",
        // No statistics at all.
        null,
    ];

    private static List<Predicate> DifferentialFilters()
    {
        var filters = new List<Predicate>();
        foreach (long v in new long[] { -1, 0, 5, 9, 10, 100, 150, 199, 1000, 1005, 5000 })
        {
            filters.Add(Ex.GreaterThanOrEqual("id", LiteralValue.Of(v)));
            filters.Add(Ex.LessThan("id", LiteralValue.Of(v)));
            filters.Add(Ex.Equal("id", LiteralValue.Of(v)));
            filters.Add(Ex.GreaterThan("payload.score", LiteralValue.Of(v)));
        }
        foreach (string v in new[] { "a", "aa", "mm", "nn", "zz", "zzzz" })
        {
            filters.Add(Ex.GreaterThanOrEqual("region", LiteralValue.Of(v)));
            filters.Add(Ex.LessThanOrEqual("region", LiteralValue.Of(v)));
            filters.Add(Ex.StartsWith("region", v));
        }
        foreach (decimal v in new[] { -10m, -9.99m, 0m, 1.25m, 5.75m, 1234.56m, 9999m })
        {
            filters.Add(Ex.GreaterThan("amount", LiteralValue.Of(v)));
            filters.Add(Ex.LessThanOrEqual("amount", LiteralValue.Of(v)));
        }
        foreach (var v in new[]
        {
            new DateTimeOffset(2019, 12, 31, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2020, 6, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2021, 6, 15, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2022, 1, 1, 0, 0, 0, TimeSpan.Zero),
        })
        {
            filters.Add(Ex.GreaterThanOrEqual("d", LiteralValue.Of(v)));
            filters.Add(Ex.LessThan("d", LiteralValue.Of(v)));
            filters.Add(Ex.GreaterThanOrEqual("ts", LiteralValue.Of(v)));
            filters.Add(Ex.LessThan("ts", LiteralValue.Of(v)));
        }
        filters.Add(Ex.Equal("flag", LiteralValue.Of(true)));
        filters.Add(Ex.Equal("flag", LiteralValue.Of(false)));
        foreach (string column in new[] { "id", "region", "amount", "flag", "payload.score" })
        {
            filters.Add(Ex.IsNull(column));
            filters.Add(Ex.IsNotNull(column));
        }
        return filters;
    }

    /// <summary>
    /// The load-bearing test: for every predicate and every file, reading the typed columns must reach
    /// the same include/exclude decision as parsing the JSON. Covers each stat type, files with only
    /// some columns bounded, explicit JSON nulls and a file with no statistics at all.
    /// </summary>
    [Fact]
    public async Task TypedAndJsonPathsAgreeOnEveryFileAndPredicate()
    {
        var adds = await CheckpointedAdds(VariedBlobs);
        Assert.All(adds, a => Assert.NotNull(a.TypedStats));

        var pruner = new DeltaFilePruner(MixedSchema(), [], preferTypedStats: true);
        var disagreements = new List<string>();

        foreach (var filter in DifferentialFilters())
        {
            for (int i = 0; i < adds.Count; i++)
            {
                bool typed = pruner.ShouldInclude(adds[i], filter);
                bool json = pruner.ShouldInclude(adds[i] with { TypedStats = null }, filter);
                if (typed != json)
                    disagreements.Add($"file {i}, filter {filter}: typed={typed} json={json}");
            }
        }

        Assert.Empty(disagreements);
    }

    /// <summary>
    /// A column the typed struct does not bound falls back to the JSON copy rather than reporting "no
    /// bound". <c>stats_parsed</c> follows delta-spark and omits booleans from min/max, while EW's
    /// JSON statistics do carry them — a typed-only lookup would quietly stop pruning on them.
    /// </summary>
    [Fact]
    public async Task BooleanBoundsFallBackToTheJsonCopy()
    {
        // Every row false, so `flag = true` cannot match this file.
        var adds = await CheckpointedAdds([
            """{"numRecords":10,"minValues":{"id":0,"flag":false},"maxValues":{"id":9,"flag":false},"nullCount":{"id":0,"flag":0}}"""
        ]);

        var pruner = new DeltaFilePruner(MixedSchema(), []);
        Assert.False(pruner.ShouldInclude(adds[0], Ex.Equal("flag", LiteralValue.Of(true))));
        Assert.True(pruner.ShouldInclude(adds[0], Ex.Equal("flag", LiteralValue.Of(false))));
    }

    /// <summary>Nested struct bounds resolve through the typed path under their dotted path.</summary>
    [Fact]
    public async Task NestedBoundsPruneThroughTheTypedPath()
    {
        var adds = await CheckpointedAdds([
            """{"numRecords":10,"minValues":{"id":0,"payload":{"score":10}},"maxValues":{"id":9,"payload":{"score":20}},"nullCount":{"id":0,"payload":{"score":0}}}"""
        ]);
        Assert.NotNull(adds[0].TypedStats);

        var pruner = new DeltaFilePruner(MixedSchema(), []);
        Assert.False(pruner.ShouldInclude(
            adds[0], Ex.GreaterThan("payload.score", LiteralValue.Of(100L))));
        Assert.True(pruner.ShouldInclude(
            adds[0], Ex.GreaterThan("payload.score", LiteralValue.Of(15L))));
    }

    /// <summary>
    /// Decimal bounds keep their exact digits through the typed path. This value has more significant
    /// digits than <c>System.Decimal</c> holds, so a bound that went through it would shift and could
    /// prune a file that matches.
    /// </summary>
    [Fact]
    public async Task WideDecimalBoundsArePreciseThroughTheTypedPath()
    {
        const string schemaJson = """
            {"type":"struct","fields":[
              {"name":"amount","type":"decimal(38,10)","nullable":true,"metadata":{}}]}
            """;

        var adds = await CheckpointedAdds(
            ["""{"numRecords":2,"minValues":{"amount":1234567890123456789012345678.0000000001},"maxValues":{"amount":1234567890123456789012345678.0000000009},"nullCount":{"amount":0}}"""],
            schemaJson: schemaJson);

        var schema = EngineeredWood.DeltaLake.Schema.SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("amount", new Decimal128Type(38, 10), true))
                .Build());
        var pruner = new DeltaFilePruner(schema, []);

        // Just below the min: the file must survive. A bound rounded to System.Decimal precision
        // would collapse both bounds onto the same value and could answer this wrongly.
        var justBelowMin = LiteralValue.HighPrecisionDecimalOf(
            System.Numerics.BigInteger.Parse("12345678901234567890123456780000000000"), 10);
        Assert.True(pruner.ShouldInclude(adds[0], Ex.GreaterThanOrEqual("amount", justBelowMin)));

        // Above the max: prunable.
        var aboveMax = LiteralValue.HighPrecisionDecimalOf(
            System.Numerics.BigInteger.Parse("12345678901234567890123456790000000000"), 10);
        Assert.False(pruner.ShouldInclude(adds[0], Ex.GreaterThan("amount", aboveMax)));
    }

    /// <summary>
    /// With <c>writeStatsAsJson=false</c> the checkpoint carries no statistics string at all, so the
    /// typed columns are the only source — the shape EW could previously write but not read.
    /// </summary>
    [Fact]
    public async Task StructOnlyCheckpointStillPrunesAndCountsRows()
    {
        var adds = await CheckpointedAdds(
            [
                """{"numRecords":10,"minValues":{"id":0},"maxValues":{"id":9},"nullCount":{"id":0}}""",
                """{"numRecords":25,"minValues":{"id":100},"maxValues":{"id":199},"nullCount":{"id":0}}""",
            ],
            new Dictionary<string, string> { ["delta.checkpoint.writeStatsAsJson"] = "false" });

        Assert.All(adds, a => Assert.Null(a.Stats));
        Assert.All(adds, a => Assert.NotNull(a.TypedStats));

        var pruner = new DeltaFilePruner(MixedSchema(), []);
        Assert.False(pruner.ShouldInclude(adds[0], Ex.GreaterThanOrEqual("id", LiteralValue.Of(50L))));
        Assert.True(pruner.ShouldInclude(adds[1], Ex.GreaterThanOrEqual("id", LiteralValue.Of(50L))));

        // Row counts must survive too: they assign row ids and size compaction groups, and reading
        // them as zero from an absent JSON string would corrupt both.
        Assert.Equal(10L, adds[0].GetNumRecords());
        Assert.Equal(25L, adds[1].GetNumRecords());
    }

    /// <summary>
    /// Turning the preference off forces the JSON path where both copies exist — and must not disable
    /// pruning for a checkpoint that only has the typed copy, which would otherwise lose every skip.
    /// </summary>
    [Fact]
    public async Task PreferenceOffUsesJsonButStructOnlyStillWorks()
    {
        const string blob =
            """{"numRecords":10,"minValues":{"id":0},"maxValues":{"id":9},"nullCount":{"id":0}}""";

        var bothCopies = await CheckpointedAdds([blob]);
        var jsonPruner = new DeltaFilePruner(MixedSchema(), [], preferTypedStats: false);
        Assert.False(jsonPruner.ShouldInclude(
            bothCopies[0], Ex.GreaterThanOrEqual("id", LiteralValue.Of(50L))));
        Assert.True(jsonPruner.ShouldInclude(
            bothCopies[0], Ex.GreaterThanOrEqual("id", LiteralValue.Of(5L))));

        using var second = new TypedCheckpointStatsTests();
        var structOnly = await second.CheckpointedAdds(
            [blob], new Dictionary<string, string> { ["delta.checkpoint.writeStatsAsJson"] = "false" });
        Assert.Null(structOnly[0].Stats);
        Assert.False(jsonPruner.ShouldInclude(
            structOnly[0], Ex.GreaterThanOrEqual("id", LiteralValue.Of(50L))));
    }

    /// <summary>
    /// Statistics read from a typed-only checkpoint can be written back out as JSON, which is the only
    /// form the rest of the log speaks. Every value must survive the round trip: a bound that changed
    /// on the way out would be worse than one that vanished.
    /// </summary>
    [Fact]
    public async Task TypedStatsReencodeToEquivalentJson()
    {
        const string original =
            """{"numRecords":10,"minValues":{"id":0,"region":"aa","amount":1.25,"d":"2020-01-01","ts":"2020-01-01T00:00:00.000000Z","payload":{"score":5}},"maxValues":{"id":9,"region":"mm","amount":5.75,"d":"2020-12-31","ts":"2020-12-31T23:59:59.999999Z","payload":{"score":50}},"nullCount":{"id":0,"region":1,"amount":2,"d":0,"ts":0,"flag":4,"payload":{"score":1}}}""";

        var adds = await CheckpointedAdds(
            [original],
            new Dictionary<string, string> { ["delta.checkpoint.writeStatsAsJson"] = "false" });

        Assert.Null(adds[0].Stats);
        string? rebuilt = adds[0].GetStatsJson();
        Assert.NotNull(rebuilt);

        using var expected = System.Text.Json.JsonDocument.Parse(original);
        using var actual = System.Text.Json.JsonDocument.Parse(rebuilt!);

        Assert.Equal(10, actual.RootElement.GetProperty("numRecords").GetInt64());

        foreach (string group in new[] { "minValues", "maxValues" })
        {
            var want = expected.RootElement.GetProperty(group);
            var got = actual.RootElement.GetProperty(group);
            Assert.Equal(want.GetProperty("id").GetInt64(), got.GetProperty("id").GetInt64());
            Assert.Equal(want.GetProperty("region").GetString(), got.GetProperty("region").GetString());
            Assert.Equal(want.GetProperty("amount").GetRawText(), got.GetProperty("amount").GetRawText());
            Assert.Equal(want.GetProperty("d").GetString(), got.GetProperty("d").GetString());
            Assert.Equal(want.GetProperty("ts").GetString(), got.GetProperty("ts").GetString());
            Assert.Equal(
                want.GetProperty("payload").GetProperty("score").GetInt64(),
                got.GetProperty("payload").GetProperty("score").GetInt64());
        }

        // nullCount covers boolean columns too, which carry no bounds.
        var wantNulls = expected.RootElement.GetProperty("nullCount");
        var gotNulls = actual.RootElement.GetProperty("nullCount");
        foreach (string column in new[] { "id", "region", "amount", "d", "ts", "flag" })
            Assert.Equal(wantNulls.GetProperty(column).GetInt64(), gotNulls.GetProperty(column).GetInt64());
        Assert.Equal(
            wantNulls.GetProperty("payload").GetProperty("score").GetInt64(),
            gotNulls.GetProperty("payload").GetProperty("score").GetInt64());
    }

    /// <summary>
    /// A wide decimal survives the re-encode with every digit intact — the case where going through
    /// <c>System.Decimal</c> would round the bound.
    /// </summary>
    [Fact]
    public async Task WideDecimalSurvivesTheReencode()
    {
        const string schemaJson = """
            {"type":"struct","fields":[
              {"name":"amount","type":"decimal(38,10)","nullable":true,"metadata":{}}]}
            """;

        var adds = await CheckpointedAdds(
            ["""{"numRecords":2,"minValues":{"amount":1234567890123456789012345678.0000000001},"maxValues":{"amount":1234567890123456789012345678.0000000009},"nullCount":{"amount":0}}"""],
            new Dictionary<string, string> { ["delta.checkpoint.writeStatsAsJson"] = "false" },
            schemaJson);

        using var rebuilt = System.Text.Json.JsonDocument.Parse(adds[0].GetStatsJson()!);
        Assert.Equal("1234567890123456789012345678.0000000001",
            rebuilt.RootElement.GetProperty("minValues").GetProperty("amount").GetRawText());
        Assert.Equal("1234567890123456789012345678.0000000009",
            rebuilt.RootElement.GetProperty("maxValues").GetProperty("amount").GetRawText());
    }

    /// <summary>
    /// <para>The gap this closes, end to end. A DELETE that writes a deletion vector keeps the file and
    /// re-commits it with its bounds WIDENED — <c>StatsWithLooseBounds</c> rewrites the statistics as
    /// text. On a table whose checkpoint carries only typed statistics there is no text to rewrite, so
    /// the surviving add came out with no statistics at all and the table scanned every file from then
    /// on.</para>
    ///
    /// <para>The same commit exercises the serializer, which likewise has to synthesise the string for
    /// an add it did not read from JSON.</para>
    /// </summary>
    [Fact]
    public async Task DeletionVectorRewriteKeepsStatsOnAStructOnlyTable()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(
            fs, arrowSchema, new DeltaTableOptions { CheckpointInterval = 1 },
            enableDeletionVectors: true,
            configuration: new Dictionary<string, string>
            {
                ["delta.checkpoint.writeStatsAsJson"] = "false",
            }))
        {
            var ids = new Int64Array.Builder()
                .AppendRange(Enumerable.Range(0, 20).Select(v => (long)v)).Build();
            await table.WriteAsync([new RecordBatch(arrowSchema, [ids], 20)]);
        }

        // Reopen so the file's statistics come from the checkpoint, not the commit that wrote it.
        await using (var reopened = await DeltaTable.OpenAsync(fs))
        {
            var add = Assert.Single(reopened.CurrentSnapshot.ActiveFiles.Values);
            Assert.Null(add.Stats);
            Assert.NotNull(add.TypedStats);

            await reopened.DeleteAsync(Ex.LessThan("id", LiteralValue.Of(5L)));
        }

        // A second delete rewrites a file that ALREADY carries a deletion vector — the path that
        // widens the bounds as text, and the one with no text to widen before this fix.
        await using (var again = await DeltaTable.OpenAsync(fs))
            await again.DeleteAsync(Ex.GreaterThan("id", LiteralValue.Of(17L)));

        await using var after = await DeltaTable.OpenAsync(fs);
        var survivor = Assert.Single(after.CurrentSnapshot.ActiveFiles.Values);

        Assert.NotNull(survivor.DeletionVector);
        Assert.NotNull(survivor.Stats);

        // Bounds survived the widening, and still prune.
        using var stats = System.Text.Json.JsonDocument.Parse(survivor.Stats!);
        Assert.Equal(0, stats.RootElement.GetProperty("minValues").GetProperty("id").GetInt64());
        Assert.Equal(19, stats.RootElement.GetProperty("maxValues").GetProperty("id").GetInt64());

        var pruner = new DeltaFilePruner(after.CurrentSnapshot.Schema, []);
        Assert.False(pruner.ShouldInclude(survivor, Ex.GreaterThan("id", LiteralValue.Of(100L))));
        Assert.True(pruner.ShouldInclude(survivor, Ex.GreaterThan("id", LiteralValue.Of(10L))));
    }

    /// <summary>
    /// A table read end to end through its checkpoint returns the same rows with the preference on or
    /// off — the paths are interchangeable from outside.
    /// </summary>
    [Fact]
    public async Task TableReadsMatchWithPreferenceOnAndOff()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(
            fs, arrowSchema, new DeltaTableOptions { CheckpointInterval = 1 }))
        {
            for (long batchStart = 0; batchStart < 30; batchStart += 10)
            {
                var ids = new Int64Array.Builder()
                    .AppendRange(Enumerable.Range((int)batchStart, 10).Select(v => (long)v)).Build();
                await table.WriteAsync([new RecordBatch(arrowSchema, [ids], 10)]);
            }
        }

        var filter = Ex.GreaterThanOrEqual("id", LiteralValue.Of(15L));

        async Task<List<long>> Read(bool preferTyped)
        {
            await using var table = await DeltaTable.OpenAsync(
                fs, new DeltaTableOptions { PreferTypedCheckpointStats = preferTyped });
            var values = new List<long>();
            await foreach (var batch in table.ReadAllAsync(columns: null, filter: filter))
            {
                var ids = (Int64Array)batch.Column("id");
                for (int i = 0; i < batch.Length; i++)
                    values.Add(ids.GetValue(i)!.Value);
            }
            values.Sort();
            return values;
        }

        var typed = await Read(preferTyped: true);
        var json = await Read(preferTyped: false);

        Assert.Equal(json, typed);
        // Pruning is per FILE: the 0-9 file cannot match and is skipped, while the 10-19 file is read
        // whole because part of it can. Both paths must land on exactly that set.
        Assert.Equal(Enumerable.Range(10, 20).Select(v => (long)v), typed);
    }
}
