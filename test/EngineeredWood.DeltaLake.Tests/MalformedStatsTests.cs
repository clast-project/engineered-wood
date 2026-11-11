// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// #820: statistics a reader can't use count as absent, never as an error. <see cref="ColumnStats.Parse"/> is
/// documented to return null for stats that don't parse, but a root that isn't a JSON object threw
/// <see cref="InvalidOperationException"/> and a null count that isn't a 64-bit integer threw
/// <see cref="FormatException"/>, failing both the file's row count and a pruned scan over it.
/// </summary>
public class MalformedStatsTests
{
    private static AddFile File(string? stats) => new()
    {
        Path = "part-0.parquet",
        PartitionValues = new Dictionary<string, string>(),
        Size = 1,
        ModificationTime = 0,
        DataChange = true,
        Stats = stats,
    };

    private static readonly EngineeredWood.DeltaLake.Schema.StructType Schema = new()
    {
        Fields =
        [
            new StructField { Name = "id", Type = new PrimitiveType { TypeName = "long" }, Nullable = true },
            new StructField { Name = "v", Type = new PrimitiveType { TypeName = "long" }, Nullable = true },
        ],
    };

    [Theory]
    [InlineData("[]")]
    [InlineData("[1, 2]")]
    [InlineData("0")]
    [InlineData("\"stats\"")]
    [InlineData("null")]
    [InlineData("true")]
    public void ARootThatIsNotAnObjectIsNoStatistics(string stats)
    {
        Assert.Null(ColumnStats.Parse(stats));
        Assert.Null(File(stats).GetNumRecords());
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("-9223372036854775808")]
    public void ANegativeRowCountIsUnknown(string count)
    {
        string stats = "{\"numRecords\":" + count + "}";
        Assert.Null(ColumnStats.Parse(stats)!.NumRecords);
        Assert.Null(File(stats).GetNumRecords());
    }

    [Theory]
    [InlineData("1.5")]
    [InlineData("1e30")]
    [InlineData("-1")]
    [InlineData("9223372036854775808")] // one past long.MaxValue
    public void ANullCountThatIsNotANonNegativeInt64IsUnknown(string count)
    {
        var parsed = ColumnStats.Parse("{\"numRecords\":3,\"nullCount\":{\"id\":" + count + ",\"v\":2,\"s\":{\"a\":" + count + ",\"b\":1}}}");

        Assert.NotNull(parsed);
        Assert.Equal(3, parsed!.NumRecords);
        Assert.False(parsed.NullCount!.ContainsKey("id"));
        Assert.False(parsed.NullCount.ContainsKey("s.a"));
        Assert.Equal(2, parsed.NullCount["v"]);
        Assert.Equal(1, parsed.NullCount["s.b"]);
    }

    /// <summary>
    /// A key whose count is unusable still marks the key ambiguous (#825 review): a usable copy of it, under the same
    /// name or as a literal dotted name beside a nested path, is dropped with it, whichever comes first. Keeping that
    /// copy alone let <c>IS NULL</c> prune a file on a count of 0 its statistics also contradict.
    /// </summary>
    [Theory]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"id\":0,\"id\":-1}}", "id")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"id\":-1,\"id\":0}}", "id")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"id\":0,\"id\":1.5}}", "id")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"id\":0,\"id\":\"3\"}}", "id")]     // not a number at all (round 2)
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"id\":\"3\",\"id\":0}}", "id")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"id\":0,\"id\":null}}", "id")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"id\":0,\"id\":true}}", "id")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"id\":0,\"id\":[0]}}", "id")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"s.a\":\"0\",\"s\":{\"a\":0}}}", "s.a")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"s\":{\"a\":\"0\"},\"s.a\":0}}", "s.a")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"s.a\":-1,\"s\":{\"a\":0}}}", "s.a")]
    [InlineData("{\"numRecords\":3,\"nullCount\":{\"s\":{\"a\":0},\"s.a\":-1}}", "s.a")]
    public void AnUnusableCountMakesItsKeyAmbiguous(string stats, string key)
    {
        var counts = ColumnStats.Parse(stats)!.NullCount!;
        Assert.False(counts.ContainsKey(key));
    }

    [Theory]
    [InlineData("{\"numRecords\":3,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":9},\"nullCount\":{\"id\":0,\"id\":-1}}")]
    [InlineData("{\"numRecords\":3,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":9},\"nullCount\":{\"id\":-1,\"id\":0}}")]
    [InlineData("{\"numRecords\":3,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":9},\"nullCount\":{\"id\":0,\"id\":\"3\"}}")]
    public void AnAmbiguousCountDoesNotPruneIsNull(string stats) =>
        Assert.True(new DeltaFilePruner(Schema, []).ShouldInclude(File(stats), Ex.IsNull("id")));

    /// <summary>A pruned scan over a file whose statistics are no object at all keeps it, rather than failing.</summary>
    [Fact]
    public void APrunedScanKeepsAFileWhoseStatisticsAreNoObject()
    {
        var pruner = new DeltaFilePruner(Schema, []);
        foreach (var filter in new Predicate[] { Ex.GreaterThan("id", 100L), Ex.IsNull("id"), Ex.IsNotNull("id") })
            Assert.True(pruner.ShouldInclude(File("[]"), filter));
    }

    /// <summary>
    /// An unusable count is unknown and nothing more: the decisions that need it keep the file (<c>IS NULL</c>, and
    /// <c>IS NOT NULL</c>, which needs the row count to tell an all-NULL column), while the bounds, which are fine,
    /// still prune.
    /// </summary>
    [Theory]
    [InlineData("{\"numRecords\":3,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":9},\"nullCount\":{\"id\":1.5}}")]
    [InlineData("{\"numRecords\":3,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":9},\"nullCount\":{\"id\":-1}}")]
    [InlineData("{\"numRecords\":-1,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":9},\"nullCount\":{\"id\":-1}}")]
    public void AnUnusableCountKeepsOnlyTheDecisionsThatNeedIt(string stats)
    {
        var pruner = new DeltaFilePruner(Schema, []);
        Assert.True(pruner.ShouldInclude(File(stats), Ex.IsNull("id")));
        Assert.True(pruner.ShouldInclude(File(stats), Ex.IsNotNull("id")));
        Assert.False(pruner.ShouldInclude(File(stats), Ex.GreaterThan("id", 100L)));
    }

    /// <summary>
    /// The typed copy a checkpoint writes (<c>stats_parsed</c>) reads counts as counts too (#825 review): a negative
    /// <c>numRecords</c> or null count is written null, as the JSON path reads it, while a BIGINT column's negative
    /// bounds are kept.
    /// </summary>
    [Fact]
    public void ACheckpointsTypedStatisticsWriteANegativeCountAsUnknown()
    {
        var add = File("{\"numRecords\":-1,\"minValues\":{\"id\":-5},\"maxValues\":{\"id\":-1},\"nullCount\":{\"id\":-1,\"v\":2}}");
        using var stats = StatsParsedBuilder.BuildStatsColumn([add], 1, Schema)!;

        var type = (Apache.Arrow.Types.StructType)stats.Data.DataType;
        StructArray Child(string name) => (StructArray)stats.Fields[type.GetFieldIndex(name)];
        Int64Array Leaf(StructArray parent, string name) =>
            (Int64Array)parent.Fields[((Apache.Arrow.Types.StructType)parent.Data.DataType).GetFieldIndex(name)];

        Assert.True(((Int64Array)stats.Fields[type.GetFieldIndex("numRecords")]).IsNull(0));
        Assert.True(Leaf(Child("nullCount"), "id").IsNull(0));
        Assert.Equal(2, Leaf(Child("nullCount"), "v").GetValue(0));
        Assert.Equal(-5, Leaf(Child("minValues"), "id").GetValue(0));
        Assert.Equal(-1, Leaf(Child("maxValues"), "id").GetValue(0));
    }

    /// <summary>
    /// A typed copy another writer put in a checkpoint with negative counts reads them as unknown (#825 review): a
    /// file's row count comes from it before the JSON, and fed row ids.
    /// </summary>
    [Fact]
    public void ACheckpointsTypedStatisticsReadANegativeCountAsUnknown()
    {
        var nullCountType = new Apache.Arrow.Types.StructType([new Field("id", Int64Type.Default, true), new Field("v", Int64Type.Default, true)]);
        var statsType = new Apache.Arrow.Types.StructType([
            new Field("numRecords", Int64Type.Default, true), new Field("nullCount", nullCountType, true)]);
        var nullCount = new StructArray(nullCountType, 1,
            [new Int64Array.Builder().Append(-2).Build(), new Int64Array.Builder().Append(3).Build()], ArrowBuffer.Empty, 0);
        var stats = new StructArray(statsType, 1, [new Int64Array.Builder().Append(-1).Build(), nullCount], ArrowBuffer.Empty, 0);
        var addType = new Apache.Arrow.Types.StructType([new Field("stats_parsed", statsType, true)]);
        var addStruct = new StructArray(addType, 1, [stats], ArrowBuffer.Empty, 0);

        var view = CheckpointStatsView.TryCreate(addStruct)!;

        Assert.Null(view.GetNumRecords(0));
        Assert.Null(view.GetNullCount("id", 0));
        Assert.Equal(3, view.GetNullCount("v", 0));
        Assert.Null((File(null) with { TypedStats = new ParsedStatsRef(view, 0) }).GetNumRecords());

        // Turned back into JSON (a stats_parsed-only checkpoint's file, re-emitted), the negative counts are omitted,
        // not passed on: the JSON says what the getters do (#825 review).
        string json = (File(null) with { TypedStats = new ParsedStatsRef(view, 0) }).GetStatsJson()!;
        Assert.DoesNotContain("-1", json, StringComparison.Ordinal);
        Assert.DoesNotContain("-2", json, StringComparison.Ordinal);
        var reparsed = ColumnStats.Parse(json)!;
        Assert.Null(reparsed.NumRecords);
        Assert.False(reparsed.NullCount!.ContainsKey("id"));
        Assert.Equal(3, reparsed.NullCount["v"]);
    }

    /// <summary>Usable statistics still prune: the fix declines only what it can't read.</summary>
    [Fact]
    public void UsableStatisticsStillPrune()
    {
        var pruner = new DeltaFilePruner(Schema, []);
        var stats = "{\"numRecords\":3,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":9},\"nullCount\":{\"id\":0}}";
        Assert.False(pruner.ShouldInclude(File(stats), Ex.GreaterThan("id", 100L)));
        Assert.False(pruner.ShouldInclude(File(stats), Ex.IsNull("id")));
    }
}
