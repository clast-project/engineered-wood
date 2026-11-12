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

    /// <summary>
    /// A leaf named like an object's path is ambiguous in the JSON too (#841 review), whichever comes first: the typed
    /// copy drops such a path and falls back to this one, which used to keep the leaf because only leaves were
    /// tracked.
    /// </summary>
    [Theory]
    [InlineData("{\"nullCount\":{\"id\":0,\"id\":{\"a\":3}}}", "id")]
    [InlineData("{\"nullCount\":{\"id\":{\"a\":3},\"id\":0}}", "id")]
    [InlineData("{\"nullCount\":{\"s\":{\"t\":0,\"t\":{\"x\":1}}}}", "s.t")]
    [InlineData("{\"nullCount\":{\"s.t\":0,\"s\":{\"t\":{\"x\":1}}}}", "s.t")]
    public void AJsonCountNamedLikeAStructIsAmbiguous(string stats, string key) =>
        Assert.False(ColumnStats.Parse(stats)!.NullCount!.ContainsKey(key));

    [Theory]
    [InlineData("{\"minValues\":{\"s\":{\"t\":5,\"t\":{\"x\":1}}}}", "s.t")]
    [InlineData("{\"minValues\":{\"s\":{\"t\":{\"x\":1},\"t\":5}}}", "s.t")]
    [InlineData("{\"minValues\":{\"s.t\":5,\"s\":{\"t\":{\"x\":1}}}}", "s.t")]
    [InlineData("{\"minValues\":{\"s\":{\"t\":{\"x\":1}},\"s.t\":5}}", "s.t")]
    public void AJsonBoundNamedLikeAStructIsAmbiguous(string stats, string key)
    {
        var mins = ColumnStats.Parse(stats)!.MinValues!;
        Assert.False(mins.ContainsKey(key));
        Assert.True(mins.ContainsKey("s.t.x"));
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

    private static Int64Array Longs(long value) => new Int64Array.Builder().Append(value).Build();

    private static (Field Field, IArrowArray Array) Leaf(string name, IArrowArray array) =>
        (new Field(name, array.Data.DataType, true), array);

    private static (Field Field, IArrowArray Array) Group(string name, params (Field Field, IArrowArray Array)[] children)
    {
        var type = new Apache.Arrow.Types.StructType(children.Select(c => c.Field).ToList());
        return (new Field(name, type, true), new StructArray(type, 1, children.Select(c => c.Array), ArrowBuffer.Empty, 0));
    }

    /// <summary>A one-file checkpoint view over a hand-built <c>stats_parsed</c> with the given groups.</summary>
    private static CheckpointStatsView TypedView(params (Field Field, IArrowArray Array)[] groups)
    {
        var stats = Group("stats_parsed", [Leaf("numRecords", Longs(3)), .. groups]);
        var addType = new Apache.Arrow.Types.StructType([stats.Field]);
        return CheckpointStatsView.TryCreate(new StructArray(addType, 1, [stats.Array], ArrowBuffer.Empty, 0))!;
    }

    /// <summary>
    /// A typed path reached twice is ambiguous, as the JSON's is (#826): the same field name twice, or a literal dotted
    /// field beside a struct child of that path, in either order, drops the path rather than keeping the last copy.
    /// </summary>
    [Fact]
    public void ACollidingTypedNullCountMakesItsPathAmbiguous()
    {
        var cases = new[]
        {
            Group("nullCount", Leaf("id", Longs(0)), Leaf("id", Longs(3)), Leaf("v", Longs(2))),
            Group("nullCount", Leaf("id", Longs(3)), Leaf("id", Longs(0)), Leaf("v", Longs(2))),
            // A copy of another type is unusable, and still makes the path ambiguous (as the JSON's does since #825).
            Group("nullCount", Leaf("id", Longs(0)), Leaf("id", new StringArray.Builder().Append("3").Build()), Leaf("v", Longs(2))),
            Group("nullCount", Leaf("id", new Int32Array.Builder().Append(3).Build()), Leaf("id", Longs(0)), Leaf("v", Longs(2))),
            // A leaf and a struct under one name.
            Group("nullCount", Leaf("id", Longs(0)), Group("id", Leaf("a", Longs(0))), Leaf("v", Longs(2))),
        };
        foreach (var nullCount in cases)
        {
            var view = TypedView(nullCount);
            Assert.False(view.HasNullCount("id"));
            Assert.Null(view.GetNullCount("id", 0));
            Assert.Equal(2, view.GetNullCount("v", 0));
        }

        foreach (var nullCount in new[]
        {
            Group("nullCount", Leaf("s.a", Longs(0)), Group("s", Leaf("a", Longs(3)), Leaf("b", Longs(1)))),
            Group("nullCount", Group("s", Leaf("a", Longs(3)), Leaf("b", Longs(1))), Leaf("s.a", Longs(0))),
        })
        {
            var view = TypedView(nullCount);
            Assert.False(view.HasNullCount("s.a"));
            Assert.Equal(1, view.GetNullCount("s.b", 0));
        }
    }

    [Fact]
    public void ACollidingTypedBoundMakesItsPathAmbiguous()
    {
        var view = TypedView(
            Group("minValues", Leaf("id", Longs(0)), Leaf("id", Longs(50)), Leaf("v", Longs(1))),
            Group("maxValues", Leaf("id", Longs(9)), Leaf("v", Longs(2))));

        Assert.Null(view.GetBound("id", 0, isMin: true, "long"));
        Assert.NotNull(view.GetBound("id", 0, isMin: false, "long"));
        Assert.NotNull(view.GetBound("v", 0, isMin: true, "long"));

        var dotted = TypedView(
            Group("minValues", Leaf("s.a", Longs(100)), Group("s", Leaf("a", Longs(0)))),
            Group("maxValues", Group("s", Leaf("a", Longs(9))), Leaf("s.a", Longs(200))));
        Assert.False(dotted.HasBound("s.a", isMin: true));
        Assert.False(dotted.HasBound("s.a", isMin: false));
    }

    /// <summary>
    /// End to end: a file whose only statistics are a typed copy with a colliding null count, or colliding bounds, is
    /// kept. Before #826 the last copy won, so <c>IS NULL</c> pruned on a count of 0 and <c>id &gt; 100</c> on a max
    /// of 9 that a second max of 500 contradicts.
    /// </summary>
    [Fact]
    public void ACollidingTypedCopyDoesNotPrune()
    {
        var view = TypedView(
            Group("minValues", Leaf("id", Longs(0)), Leaf("v", Longs(0))),
            Group("maxValues", Leaf("id", Longs(500)), Leaf("id", Longs(9)), Leaf("v", Longs(9))),
            Group("nullCount", Leaf("id", Longs(3)), Leaf("id", Longs(0)), Leaf("v", Longs(0))));
        var add = File(null) with { TypedStats = new ParsedStatsRef(view, 0) };
        var pruner = new DeltaFilePruner(Schema, []);

        Assert.True(pruner.ShouldInclude(add, Ex.IsNull("id")));
        Assert.True(pruner.ShouldInclude(add, Ex.GreaterThan("id", 100L)));
        // A column beside them that doesn't collide still prunes.
        Assert.False(pruner.ShouldInclude(add, Ex.GreaterThan("v", 100L)));
        Assert.False(pruner.ShouldInclude(add, Ex.IsNull("v")));
    }

    /// <summary>A path the typed copy drops as ambiguous falls back to the file's JSON copy, which may be fine.</summary>
    [Fact]
    public void ACollidingTypedPathFallsBackToTheJson()
    {
        var view = TypedView(Group("nullCount", Leaf("id", Longs(3)), Leaf("id", Longs(0))));
        var add = File("{\"numRecords\":3,\"nullCount\":{\"id\":0}}") with { TypedStats = new ParsedStatsRef(view, 0) };

        Assert.False(new DeltaFilePruner(Schema, []).ShouldInclude(add, Ex.IsNull("id")));
    }

    /// <summary>
    /// A null-count struct shaped like a variant bound (two binaries) is malformed, not opaque (#841 review): its
    /// children's paths still meet the collision check, so a literal dotted count beside one is ambiguous.
    /// </summary>
    [Fact]
    public void AVariantShapedNullCountStructStillCollides()
    {
        IArrowArray Bytes() => new BinaryArray.Builder().Append(new byte[] { 1 }.AsSpan()).Build();
        foreach (var nullCount in new[]
        {
            Group("nullCount", Group("s", Leaf("metadata", Bytes()), Leaf("value", Bytes())), Leaf("s.metadata", Longs(0))),
            Group("nullCount", Leaf("s.metadata", Longs(0)), Group("s", Leaf("metadata", Bytes()), Leaf("value", Bytes()))),
        })
        {
            Assert.False(TypedView(nullCount).HasNullCount("s.metadata"));
        }
    }

    /// <summary>
    /// The JSON synthesised from a typed copy says what the getters do (#841 review): a colliding path is left out,
    /// even when the writer would skip one copy anyway (a negative or null count, a null bound), which used to leave
    /// its twin alone and unambiguous in the string.
    /// </summary>
    [Fact]
    public void SynthesisedJsonLeavesOutACollidingPath()
    {
        Int64Array Null() => new Int64Array.Builder().AppendNull().Build();
        var view = TypedView(
            Group("minValues", Leaf("id", Null()), Leaf("id", Longs(50)), Leaf("v", Longs(1))),
            Group("maxValues", Leaf("id", Longs(90)), Leaf("v", Longs(2))),
            Group("nullCount",
                Leaf("id", Longs(0)), Leaf("id", Longs(-1)),
                Leaf("v", Longs(0)), Leaf("v", Null()),
                Group("s", Leaf("a", Longs(4))), Leaf("s", Longs(0)),
                Leaf("w", Longs(2))));

        var reparsed = ColumnStats.Parse(view.BuildStatsJson(0))!;

        Assert.False(reparsed.MinValues!.ContainsKey("id"));
        Assert.Equal(1, reparsed.MinValues["v"].GetInt64());
        Assert.Equal(90, reparsed.MaxValues!["id"].GetInt64());
        Assert.False(reparsed.NullCount!.ContainsKey("id"));
        Assert.False(reparsed.NullCount.ContainsKey("v"));
        Assert.False(reparsed.NullCount.ContainsKey("s"));
        // A struct at a collided path is still written: its child has a path of its own.
        Assert.Equal(4, reparsed.NullCount["s.a"]);
        Assert.Equal(2, reparsed.NullCount["w"]);
    }

    /// <summary>
    /// The typed copy is asked per side (#841 review): a min that collides falls back to the JSON's min while the
    /// typed max is still used, so a comparison needing both bounds still prunes.
    /// </summary>
    [Fact]
    public void ACollidingTypedMinFallsBackToTheJsonMinAlone()
    {
        var view = TypedView(
            Group("minValues", Leaf("id", Longs(-50)), Leaf("id", Longs(0))),
            Group("maxValues", Leaf("id", Longs(9))));
        Assert.False(view.HasBound("id", isMin: true));
        Assert.True(view.HasBound("id", isMin: false));

        var add = File("{\"numRecords\":3,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":500}}")
            with { TypedStats = new ParsedStatsRef(view, 0) };
        var pruner = new DeltaFilePruner(Schema, []);

        Assert.False(pruner.ShouldInclude(add, Ex.LessThan("id", 0L)));       // the JSON min, 0
        Assert.False(pruner.ShouldInclude(add, Ex.GreaterThan("id", 100L)));  // the typed max, 9, not the JSON's 500
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
