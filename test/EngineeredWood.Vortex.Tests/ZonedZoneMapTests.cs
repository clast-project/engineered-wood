// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Layouts;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Zones-table shapes the fixtures can't produce, because upstream's writer never emits them.
/// </summary>
public class ZonedZoneMapTests
{
    /// <summary>
    /// A <c>vortex.bounded_max</c> partial is <c>{ bound, unknown }</c>. Upstream writes a null bound
    /// whenever <c>unknown</c> is set, and its reader ignores the bound whenever <c>unknown</c> is set,
    /// so a bound under <c>unknown = true</c> is not an upper bound even if it's present.
    /// </summary>
    [Fact]
    public void BoundedMaxIgnoresBoundMarkedUnknown()
    {
        // Zone 0: a real bound. Zone 1: a bound flagged unknown. Zone 2: empty (null partial).
        var bound = new StringArray.Builder().Append("b0").Append("b1").Append("b2").Build();
        var unknown = new BooleanArray.Builder().Append(false).Append(true).Append(false).Build();
        var partial = new StructArray(
            BoundedMaxPartialType(), 3, new IArrowArray[] { bound, unknown },
            Bitmap(true, true, false), nullCount: 1);

        var stats = FromTable(partial);

        var max = Assert.IsType<StringArray>(stats.Max);
        Assert.Equal("b0", max.GetString(0));
        Assert.False(max.IsValid(1));
        Assert.False(max.IsValid(2));
        Assert.Equal(2, max.NullCount);
        Assert.All(Enumerable.Range(0, 3), z => Assert.True(stats.MaxIsTruncated!.GetValue(z)));
    }

    [Fact]
    public void BoundedMaxWithNoNullsOrUnknownsKeepsEveryBound()
    {
        var bound = new StringArray.Builder().Append("b0").Append("b1").Build();
        var unknown = new BooleanArray.Builder().Append(false).Append(false).Build();
        var partial = new StructArray(
            BoundedMaxPartialType(), 2, new IArrowArray[] { bound, unknown },
            ArrowBuffer.Empty, nullCount: 0);

        var stats = FromTable(partial);

        var max = Assert.IsType<StringArray>(stats.Max);
        Assert.Equal(new[] { "b0", "b1" }, new[] { max.GetString(0), max.GetString(1) });
        Assert.Equal(0, max.NullCount);
    }

    private static ZoneStats FromTable(StructArray boundedMaxPartial)
    {
        var aggregate = new ZoneAggregate(ZonedZoneMap.BoundedMax, BitConverter.GetBytes(64UL));
        var aggregates = new[] { aggregate };
        var tableType = ZonedZoneMap.BuildStructType(StringType.Default, aggregates)!;
        var table = new StructArray(
            tableType, boundedMaxPartial.Length, new IArrowArray[] { boundedMaxPartial },
            ArrowBuffer.Empty, nullCount: 0);
        var zoneInfo = new ZoneInfo(8192, aggregates, zonesSegmentRef: 0, zoneCount: table.Length);
        return ZonedZoneMap.FromStruct(table, StringType.Default, zoneInfo);
    }

    private static StructType BoundedMaxPartialType() => new(new[]
    {
        new Field("bound", StringType.Default, nullable: true),
        new Field("unknown", BooleanType.Default, nullable: false),
    });

    private static ArrowBuffer Bitmap(params bool[] valid)
    {
        var builder = new ArrowBuffer.BitmapBuilder(valid.Length);
        foreach (var v in valid) builder.Append(v);
        return builder.Build();
    }
}
