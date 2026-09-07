// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Float bounds and the NaN they may or may not mention (#214).
/// </summary>
/// <remarks>
/// Two Delta writers disagree about what a float bound MEANS, which is what makes a reader's job
/// here awkward. Measured against Spark 4.0 and delta-rs 1.6.2, both given the same double column
/// holding <c>[3.0, NaN]</c>:
/// <list type="bullet">
///   <item>Spark commits <c>"minValues":{"g":3.0},"maxValues":{"g":"NaN"}</c> -- the NaN recorded
///     as the maximum, quoted because JSON has no NaN.</item>
///   <item>delta-rs commits <c>"minValues":{"g":3.0},"maxValues":{"g":3.0}</c> -- the NaN dropped,
///     leaving bounds that look entirely ordinary.</item>
/// </list>
/// Nothing in the file says which wrote it, so a finite maximum cannot rule a NaN out.
/// </remarks>
public class FloatStatsPruningTests : IDisposable
{
    private readonly string _tempDir;

    public FloatStatsPruningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_fstats_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly EngineeredWood.DeltaLake.Schema.StructType DoubleSchema = new()
    {
        Fields =
        [
            new StructField
            {
                Name = "g", Type = new PrimitiveType { TypeName = "double" }, Nullable = true,
            },
        ],
    };

    private static AddFile Add(string stats) => new()
    {
        Path = "part-0.parquet",
        PartitionValues = new Dictionary<string, string>(),
        Size = 1,
        ModificationTime = 0,
        DataChange = true,
        Stats = stats,
    };

    private static bool Keeps(string stats, Predicate filter) =>
        new DeltaFilePruner(DoubleSchema, []).ShouldInclude(Add(stats), filter);

    private static LiteralValue D(double v) => LiteralValue.Of(v);

    [Fact]
    public void FiniteBounds_CannotRuleOutANaNAboveThem()
    {
        // delta-rs' shape, and the data-loss case: the file holds a NaN, the bounds say [3.0, 3.0],
        // and `g > 5.0` is TRUE of the NaN row. Pruning on the maximum drops a matching row and
        // reports success.
        const string DeltaRsShape =
            """{"numRecords":2,"minValues":{"g":3.0},"maxValues":{"g":3.0},"nullCount":{"g":0}}""";

        Assert.True(Keeps(DeltaRsShape, Ex.GreaterThan("g", D(5.0))));
        Assert.True(Keeps(DeltaRsShape, Ex.GreaterThanOrEqual("g", D(5.0))));

        // The other direction is untouched -- a NaN is below nothing, so a minimum above the
        // predicate still proves no row matches. Giving this up too would have been over-correction.
        Assert.False(Keeps(DeltaRsShape, Ex.LessThan("g", D(2.0))));
        Assert.False(Keeps(DeltaRsShape, Ex.LessThanOrEqual("g", D(2.0))));

        // So is equality, which is the predicate that matters most: a NaN equals nothing else.
        Assert.False(Keeps(DeltaRsShape, Ex.Equal("g", D(5.0))));
        Assert.False(Keeps(DeltaRsShape, Ex.In("g", D(5.0), D(6.0))));
    }

    [Fact]
    public void SparkNaNMaximum_IsReadAsAMaximum()
    {
        // A quoted "NaN" used to decode to nothing, and a missing bound is Unknown -- safe, but it
        // gave up the pruning this file CAN support. An all-NaN column proves `g < 5.0` matches
        // nothing at all.
        const string AllNaN =
            """{"numRecords":2,"minValues":{"g":"NaN"},"maxValues":{"g":"NaN"},"nullCount":{"g":0}}""";

        Assert.False(Keeps(AllNaN, Ex.LessThan("g", D(5.0))));
        Assert.False(Keeps(AllNaN, Ex.Equal("g", D(5.0))));

        // ...and every one of its rows satisfies `g > 5.0`, so the file is kept.
        Assert.True(Keeps(AllNaN, Ex.GreaterThan("g", D(5.0))));

        const string SparkShape =
            """{"numRecords":2,"minValues":{"g":3.0},"maxValues":{"g":"NaN"},"nullCount":{"g":0}}""";
        Assert.True(Keeps(SparkShape, Ex.GreaterThan("g", D(5.0))));
        Assert.False(Keeps(SparkShape, Ex.LessThan("g", D(1.0))));
    }

    [Fact]
    public void QuotedInfinities_AreReadAsBounds()
    {
        // Same gap, without a NaN in sight: an infinite bound is quoted for the same reason, and
        // leaving it undecodable cost the file all of its pruning.
        const string ReachesInfinity =
            """{"numRecords":2,"minValues":{"g":5.0},"maxValues":{"g":"Infinity"},"nullCount":{"g":0}}""";

        Assert.False(Keeps(ReachesInfinity, Ex.LessThan("g", D(1.0))));
        Assert.True(Keeps(ReachesInfinity, Ex.GreaterThan("g", D(1e300))));

        // A "-Infinity" MINIMUM prunes nothing by itself -- nothing sorts below it. What it buys is
        // that the bound is no longer MISSING: one undecodable end made the evaluator give up on
        // the pair, so the finite maximum could not prune either.
        const string ReachesNegativeInfinity =
            """{"numRecords":2,"minValues":{"g":"-Infinity"},"maxValues":{"g":-5.0},"nullCount":{"g":0}}""";
        Assert.False(Keeps(ReachesNegativeInfinity, Ex.Equal("g", D(0.0))));
    }

    [Fact]
    public async Task WriteThenReadWithAFilter_KeepsTheNaNRow()
    {
        // End to end through the table: two files, one finite and one holding a NaN. `g > 5.0`
        // matches only the NaN, so a read under that filter has to return it.
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("g", DoubleType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([new RecordBatch(schema,
            [new DoubleArray.Builder().Append(1.0).Append(2.0).Build()], 2)]);
        await table.WriteAsync([new RecordBatch(schema,
            [new DoubleArray.Builder().Append(3.0).Append(double.NaN).Build()], 2)]);

        var matched = new List<double>();
        await foreach (var batch in table.ReadAllAsync(columns: null!, Ex.GreaterThan("g", D(5.0))))
        {
            var col = (DoubleArray)batch.Column(0);
            for (int i = 0; i < col.Length; i++)
                matched.Add(col.GetValue(i)!.Value);
        }

        Assert.Contains(matched, double.IsNaN);

        // And the write itself is the other half of the bug: Utf8JsonWriter throws on a NaN, so
        // this WriteAsync used to fail outright rather than record a wrong bound.
        string stats = table.CurrentSnapshot.ActiveFiles.Values
            .Select(f => f.Stats!)
            .Single(s => s.Contains("NaN", StringComparison.Ordinal));
        Assert.Contains("\"maxValues\":{\"g\":\"NaN\"}", stats, StringComparison.Ordinal);
    }
}
