// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// <see cref="LiteralValue.Kind.TimestampNanos"/>: a timestamp that keeps every nanosecond, compared exactly
/// against the tick-resolution kinds.
/// </summary>
public class TimestampNanosLiteralTests
{
    private static readonly DateTimeOffset Epoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // 2017-11-16T22:31:08.123456789Z
    private const long Nanos = 1_510_871_468_123_456_789L;

    [Fact]
    public void HoldsEveryNanosecond()
    {
        var v = LiteralValue.TimestampNanosOf(Nanos);
        Assert.Equal(LiteralValue.Kind.TimestampNanos, v.Type);
        Assert.Equal(Nanos, v.AsTimestampNanos);
        Assert.Equal(new NanosecondTimestamp(Nanos), v.ToObject());
        Assert.Equal("2017-11-16T22:31:08.123456789Z", v.ToString());
        Assert.Throws<InvalidOperationException>(() => v.AsInt64);
        Assert.Throws<InvalidOperationException>(() => LiteralValue.Of(1L).AsTimestampNanos);
    }

    [Theory]
    [InlineData(0L, "1970-01-01T00:00:00.000000000Z")]
    [InlineData(-1L, "1969-12-31T23:59:59.999999999Z")]
    [InlineData(long.MaxValue, "2262-04-11T23:47:16.854775807Z")]
    [InlineData(long.MinValue, "1677-09-21T00:12:43.145224192Z")]
    public void ToStringIsIsoUtcWithNineDigits(long nanos, string expected) =>
        Assert.Equal(expected, new NanosecondTimestamp(nanos).ToString());

    [Fact]
    public void ToDateTimeOffsetFloorsToATick()
    {
        var t = new NanosecondTimestamp(-1);
        Assert.Equal(Epoch.AddTicks(-1), t.ToDateTimeOffset());
        Assert.Equal(99, t.NanosBeyondTick);
        Assert.Equal(Epoch.AddTicks(15_108_714_681_234_567L), new NanosecondTimestamp(Nanos).ToDateTimeOffset());
        Assert.Equal(89, new NanosecondTimestamp(Nanos).NanosBeyondTick);
    }

    [Fact]
    public void ComparesByTheCount()
    {
        var a = LiteralValue.TimestampNanosOf(Nanos);
        var b = LiteralValue.TimestampNanosOf(Nanos + 1);
        Assert.True(a < b);
        Assert.Equal(0, a.CompareTo(LiteralValue.TimestampNanosOf(Nanos)));
        Assert.True(a == LiteralValue.TimestampNanosOf(Nanos));
        Assert.NotEqual(a, b);
        Assert.Equal(a.GetHashCode(), LiteralValue.TimestampNanosOf(Nanos).GetHashCode());
    }

    /// <summary>
    /// Against a tick, the nanoseconds a tick cannot hold still decide the order: the instant 89 ns past a
    /// tick is after that tick and before the next, and is never equal to either.
    /// </summary>
    [Fact]
    public void ComparesExactlyAgainstADateTimeOffset()
    {
        var ns = LiteralValue.TimestampNanosOf(Nanos);
        var floor = LiteralValue.Of(Epoch.AddTicks(15_108_714_681_234_567L));
        var ceiling = LiteralValue.Of(Epoch.AddTicks(15_108_714_681_234_568L));

        Assert.True(ns.CompareTo(floor, out bool exact) > 0);
        Assert.True(exact);
        Assert.True(ns < ceiling);
        Assert.True(floor < ns);
        Assert.True(ceiling > ns);

        var whole = LiteralValue.TimestampNanosOf(1_510_871_468_123_456_700L);
        Assert.Equal(0, whole.CompareTo(floor));
        Assert.Equal(0, floor.CompareTo(whole));

        // Equality stays representational: the kinds differ.
        Assert.NotEqual(whole, floor);
    }

    /// <summary>Instants a nanosecond count cannot reach still compare, through the wider type.</summary>
    [Fact]
    public void ComparesAgainstDateTimeOffsetsBeyondItsRange()
    {
        var max = LiteralValue.TimestampNanosOf(long.MaxValue);
        var min = LiteralValue.TimestampNanosOf(long.MinValue);
        Assert.True(max < LiteralValue.Of(DateTimeOffset.MaxValue));
        Assert.True(min > LiteralValue.Of(DateTimeOffset.MinValue));
        Assert.True(LiteralValue.Of(new DateTimeOffset(2262, 4, 12, 0, 0, 0, TimeSpan.Zero)) > max);
    }

    [Fact]
    public void ComparesAgainstAnOffsetDateTimeOffsetAsAnInstant()
    {
        var ns = LiteralValue.TimestampNanosOf(3_600_000_000_000L); // 01:00Z
        var sameInstant = LiteralValue.Of(new DateTimeOffset(1970, 1, 1, 2, 0, 0, TimeSpan.FromHours(1)));
        Assert.Equal(0, ns.CompareTo(sameInstant));
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void ComparesAgainstADateAsItsUtcMidnight()
    {
        var midnight = LiteralValue.TimestampNanosOf(17_486L * 86_400_000_000_000L);
        var date = LiteralValue.Of(new DateOnly(2017, 11, 16));
        Assert.Equal(0, midnight.CompareTo(date));
        Assert.True(LiteralValue.TimestampNanosOf((17_486L * 86_400_000_000_000L) - 1) < date);
        Assert.True(date < LiteralValue.TimestampNanosOf((17_486L * 86_400_000_000_000L) + 1));
    }
#endif

    [Fact]
    public void DoesNotCompareWithAnInteger()
    {
        // Nanoseconds are not a number to compare by: an Int64 literal is not an instant.
        Assert.Throws<InvalidOperationException>(() => LiteralValue.TimestampNanosOf(5).CompareTo(LiteralValue.Of(5L)));
    }

    /// <summary>A file whose bounds sit one nanosecond either side of a literal is pruned exactly.</summary>
    [Theory]
    [InlineData(ComparisonOperator.Equal, Nanos + 1, Nanos + 5, FilterResult.AlwaysFalse)]
    [InlineData(ComparisonOperator.Equal, Nanos - 5, Nanos - 1, FilterResult.AlwaysFalse)]
    [InlineData(ComparisonOperator.Equal, Nanos, Nanos, FilterResult.AlwaysTrue)]
    [InlineData(ComparisonOperator.LessThan, Nanos, Nanos + 5, FilterResult.AlwaysFalse)]
    [InlineData(ComparisonOperator.LessThan, Nanos - 1, Nanos + 5, FilterResult.Unknown)]
    [InlineData(ComparisonOperator.GreaterThan, Nanos + 1, Nanos + 5, FilterResult.AlwaysTrue)]
    [InlineData(ComparisonOperator.GreaterThanOrEqual, Nanos - 5, Nanos - 1, FilterResult.AlwaysFalse)]
    public void StatisticsEvaluatorPrunesOnNanosecondBounds(ComparisonOperator op, long min, long max, FilterResult expected)
    {
        var stats = new Bounds(LiteralValue.TimestampNanosOf(min), LiteralValue.TimestampNanosOf(max));
        var predicate = new ComparisonPredicate(
            new UnboundReference("ts"), op, new LiteralExpression(LiteralValue.TimestampNanosOf(Nanos)));
        Assert.Equal(expected, StatisticsEvaluator.Evaluate(predicate, stats, new BoundsAccessor()));
    }

    /// <summary>A microsecond literal against nanosecond bounds: the sub-microsecond part of a bound counts.</summary>
    [Fact]
    public void StatisticsEvaluatorComparesAMicrosecondLiteralWithNanosecondBounds()
    {
        // Every value is in (12:00:00.000000000, 12:00:00.000000999]: strictly after the microsecond literal.
        long noon = 43_200_000_000_000L;
        var stats = new Bounds(LiteralValue.TimestampNanosOf(noon + 1), LiteralValue.TimestampNanosOf(noon + 999));
        var predicate = new ComparisonPredicate(
            new UnboundReference("ts"), ComparisonOperator.LessThanOrEqual,
            new LiteralExpression(LiteralValue.Of(Epoch.AddTicks(noon / 100))));
        Assert.Equal(FilterResult.AlwaysFalse, StatisticsEvaluator.Evaluate(predicate, stats, new BoundsAccessor()));
    }

    private sealed record Bounds(LiteralValue Min, LiteralValue Max);

    private sealed class BoundsAccessor : IStatisticsAccessor<Bounds>
    {
        public LiteralValue? GetMinValue(Bounds stats, IReadOnlyList<string> column) => stats.Min;
        public LiteralValue? GetMaxValue(Bounds stats, IReadOnlyList<string> column) => stats.Max;
        public long? GetNullCount(Bounds stats, IReadOnlyList<string> column) => 0;
        public long? GetValueCount(Bounds stats, IReadOnlyList<string> column) => 10;
        public bool IsMinExact(Bounds stats, IReadOnlyList<string> column) => true;
        public bool IsMaxExact(Bounds stats, IReadOnlyList<string> column) => true;
    }
}
