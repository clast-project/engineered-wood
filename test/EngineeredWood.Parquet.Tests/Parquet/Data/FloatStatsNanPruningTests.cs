// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Relational pruning on a FLOAT/DOUBLE column, where the bounds do not tell the whole story.
/// </summary>
/// <remarks>
/// Parquet's spec says NaN must not be written to min/max, so a row group holding
/// <c>[1.0, 2.0, NaN]</c> records an unremarkable <c>min = 1.0, max = 2.0</c>. NaN is ABOVE every
/// value in SQL's order (#204), so <c>g &gt; 5.0</c> is TRUE of that row while the maximum says the
/// row group cannot match -- the row group is skipped and the row lost. No comparator fix reaches
/// this: the bound is accurate about every non-NaN value and simply does not describe the NaN.
/// <para>
/// What resolves it is <c>nan_count</c> (PARQUET-2249), which this writer records for every
/// FLOAT/DOUBLE column, mandatory even at zero. These pin both halves: the count present and
/// positive keeps the row group, the count present and zero still prunes it.
/// </para>
/// </remarks>
public sealed class FloatStatsNanPruningTests : IDisposable
{
    private readonly string _tempDir;

    public FloatStatsNanPruningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-nan-prune-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private async Task<(FilterResult Result, long? NanCount)> EvaluateAsync(
        Predicate predicate, params double[] values)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        var builder = new DoubleArray.Builder();
        foreach (double v in values)
        {
            builder.Append(v);
        }

        var schema = new Apache.Arrow.Schema(
            [new Field("g", DoubleType.Default, nullable: false)], null);

        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions());
            await writer.WriteRowGroupAsync(
                new RecordBatch(schema, [builder.Build()], values.Length));
        }

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var accessor = new ParquetStatisticsAccessor(await reader.GetSchemaAsync());
        var rowGroup = metadata.RowGroups[0];

        return (StatisticsEvaluator.Evaluate(predicate, rowGroup, accessor),
                accessor.GetNanCount(rowGroup, "g"));
    }

    /// <summary>
    /// Evaluates against a row group whose statistics have been REPLACED, so shapes this writer
    /// never produces can still be pinned. The file underneath is a real one -- only the stats blob
    /// is swapped, which is all the accessor reads.
    /// </summary>
    private async Task<FilterResult> EvaluateWithStatsAsync(Predicate predicate, Statistics stats)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        var schema = new Apache.Arrow.Schema(
            [new Field("g", DoubleType.Default, nullable: false)], null);

        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions());
            await writer.WriteRowGroupAsync(new RecordBatch(schema,
                [new DoubleArray.Builder().Append(1.0).Append(2.0).Build()], 2));
        }

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var rowGroup = metadata.RowGroups[0];
        rowGroup.Columns[0].MetaData!.Statistics = stats;

        return StatisticsEvaluator.Evaluate(
            predicate, rowGroup, new ParquetStatisticsAccessor(await reader.GetSchemaAsync()));
    }

    private static Statistics DoubleStats(double? min, double? max, long nullCount = 0) => new()
    {
        NullCount = nullCount,
        MinValue = min is { } lo ? BitConverter.GetBytes(lo) : null,
        MaxValue = max is { } hi ? BitConverter.GetBytes(hi) : null,
        IsMinValueExact = min is null ? null : true,
        IsMaxValueExact = max is null ? null : true,
    };

    private static LiteralValue D(double v) => LiteralValue.Of(v);

    [Fact]
    public async Task ANaNMaximumIsReadAsAMaximum()
    {
        // parquet-mr 1.15.2 -- Spark's writer -- writes this shape despite the spec, and records no
        // nan_count: measured, a DOUBLE column holding [3.0, NaN] comes out as min_value = 3.0,
        // max_value = NaN. Dropping the NaN end used to cost the FINITE end too, because the
        // evaluator gives up on a pair with a missing bound, so the row group lost all pruning.
        var stats = DoubleStats(3.0, double.NaN);

        Assert.Equal(FilterResult.AlwaysFalse,
            await EvaluateWithStatsAsync(Ex.LessThan("g", D(1.0)), stats));
        Assert.Equal(FilterResult.AlwaysFalse,
            await EvaluateWithStatsAsync(Ex.Equal("g", D(1.0)), stats));

        // ...and the NaN it names is still above the predicate, so `>` keeps the row group.
        Assert.NotEqual(FilterResult.AlwaysFalse,
            await EvaluateWithStatsAsync(Ex.GreaterThan("g", D(5.0)), stats));
    }

    [Fact]
    public async Task ANaNMinimumBesideAFiniteMaximumIsRefused()
    {
        // The one NaN bound that cannot be believed. A writer using .NET's order -- where NaN sorts
        // BELOW everything -- records [1.0, NaN] as min = NaN, max = 1.0, so min > max. Reading that
        // NaN as a real lower bound would make `g < 5.0` compare 5.0 below it and fold to
        // AlwaysFalse, pruning away a row group whose values genuinely are under 5.0.
        Assert.NotEqual(FilterResult.AlwaysFalse,
            await EvaluateWithStatsAsync(
                Ex.LessThan("g", D(5.0)), DoubleStats(double.NaN, 1.0)));

        // A NaN at BOTH ends is unambiguous -- it means every value is NaN under either order --
        // and it does prune, which is what this writer emits for an all-NaN chunk under
        // FloatingPointColumnOrder.Ieee754TotalOrder.
        Assert.Equal(FilterResult.AlwaysFalse,
            await EvaluateWithStatsAsync(
                Ex.LessThan("g", D(5.0)), DoubleStats(double.NaN, double.NaN)));
    }

    [Fact]
    public async Task NaNAboveAFiniteMaximum_KeepsTheRowGroup()
    {
        var (result, nanCount) = await EvaluateAsync(
            Ex.GreaterThan("g", D(5.0)), 1.0, 2.0, double.NaN);

        // The bound really is finite -- this is not a case of the NaN leaking into min/max.
        Assert.Equal(1L, nanCount);
        Assert.NotEqual(FilterResult.AlwaysFalse, result);
    }

    [Fact]
    public async Task NoNaN_StillPrunes()
    {
        // The other half, and the reason this is a count rather than a blanket refusal to prune
        // float columns: nan_count = 0 is a proof, and the row group goes.
        var (result, nanCount) = await EvaluateAsync(
            Ex.GreaterThan("g", D(5.0)), 1.0, 2.0);

        Assert.Equal(0L, nanCount);
        Assert.Equal(FilterResult.AlwaysFalse, result);
    }

    [Fact]
    public async Task ADirectionANaNCannotReach_PrunesEvenWithOneThere()
    {
        // `NaN < 5.0` is FALSE, so a minimum above the predicate still proves nothing matches.
        var (result, _) = await EvaluateAsync(
            Ex.LessThan("g", D(5.0)), 6.0, 7.0, double.NaN);

        Assert.Equal(FilterResult.AlwaysFalse, result);
    }

    [Fact]
    public async Task EqualityPrunesEvenWithANaNThere()
    {
        // The commonest predicate keeps working: a NaN equals nothing but another NaN.
        var (equal, _) = await EvaluateAsync(
            Ex.Equal("g", D(5.0)), 1.0, 2.0, double.NaN);
        Assert.Equal(FilterResult.AlwaysFalse, equal);

        var (inList, _) = await EvaluateAsync(
            Ex.In("g", D(5.0), D(6.0)), 1.0, 2.0, double.NaN);
        Assert.Equal(FilterResult.AlwaysFalse, inList);

        // ...but a NaN named in the list is a value the hidden row can match.
        var (withNaN, _) = await EvaluateAsync(
            Ex.In("g", D(5.0), D(double.NaN)), 1.0, 2.0, double.NaN);
        Assert.Equal(FilterResult.Unknown, withNaN);
    }
}
