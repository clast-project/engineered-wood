// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Tests;

public class StatisticsEvaluatorTests
{
    /// <summary>
    /// Test accessor backed by a dictionary of column → stats. Allows
    /// independently setting min, max, null count, value count, and the exact
    /// flags for each column.
    /// </summary>
    private sealed class TestStats
    {
        public Dictionary<string, ColumnStats> Columns { get; } = new();

        public TestStats With(
            string col,
            LiteralValue? min = null, LiteralValue? max = null,
            long? nullCount = null, long? valueCount = null,
            bool minExact = true, bool maxExact = true,
            long? nanCount = null)
        {
            Columns[col] = new ColumnStats(min, max, nullCount, valueCount, minExact, maxExact, nanCount);
            return this;
        }
    }

    private sealed record ColumnStats(
        LiteralValue? Min, LiteralValue? Max,
        long? NullCount, long? ValueCount,
        bool MinExact, bool MaxExact,
        long? NanCount);

    private sealed class TestAccessor
        : IStatisticsAccessor<TestStats>, INanCountAccessor<TestStats>
    {
        public LiteralValue? GetMinValue(TestStats s, string c) =>
            s.Columns.TryGetValue(c, out var v) ? v.Min : null;
        public LiteralValue? GetMaxValue(TestStats s, string c) =>
            s.Columns.TryGetValue(c, out var v) ? v.Max : null;
        public long? GetNullCount(TestStats s, string c) =>
            s.Columns.TryGetValue(c, out var v) ? v.NullCount : null;
        public long? GetValueCount(TestStats s, string c) =>
            s.Columns.TryGetValue(c, out var v) ? v.ValueCount : null;
        public bool IsMinExact(TestStats s, string c) =>
            !s.Columns.TryGetValue(c, out var v) || v.MinExact;
        public bool IsMaxExact(TestStats s, string c) =>
            !s.Columns.TryGetValue(c, out var v) || v.MaxExact;
        public long? GetNanCount(TestStats s, string c) =>
            s.Columns.TryGetValue(c, out var v) ? v.NanCount : null;
    }

    private static readonly TestAccessor Accessor = new();

    private static FilterResult Eval(Predicate p, TestStats s) =>
        StatisticsEvaluator.Evaluate(p, s, Accessor);

    // ── Constants ──

    [Fact]
    public void True_AlwaysTrue()
    {
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.True, new TestStats()));
    }

    [Fact]
    public void False_AlwaysFalse()
    {
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.False, new TestStats()));
    }

    // ── Equal ──

    [Fact]
    public void Equal_ValueOutsideRange_AlwaysFalse()
    {
        var stats = new TestStats().With("x", min: 10, max: 20);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.Equal("x", 5), stats));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.Equal("x", 25), stats));
    }

    [Fact]
    public void Equal_ValueInRange_Unknown()
    {
        var stats = new TestStats().With("x", min: 10, max: 20);
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.Equal("x", 15), stats));
    }

    [Fact]
    public void Equal_MinEqMaxEqValue_NoNulls_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 7, max: 7, nullCount: 0);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.Equal("x", 7), stats));
    }

    [Fact]
    public void Equal_MinEqMaxEqValue_WithNulls_Unknown()
    {
        var stats = new TestStats().With("x", min: 7, max: 7, nullCount: 1);
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.Equal("x", 7), stats));
    }

    [Fact]
    public void Equal_MinEqMaxEqValue_TruncatedMin_Unknown()
    {
        var stats = new TestStats().With("x", min: 7, max: 7, nullCount: 0, minExact: false);
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.Equal("x", 7), stats));
    }

    [Fact]
    public void Equal_NullValue_Unknown()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        Assert.Equal(FilterResult.Unknown, Eval(
            new ComparisonPredicate(new UnboundReference("x"), ComparisonOperator.Equal,
                new LiteralExpression(LiteralValue.Null)),
            stats));
    }

    // ── NotEqual ──

    [Fact]
    public void NotEqual_AllValuesEqualV_AlwaysFalse()
    {
        var stats = new TestStats().With("x", min: 5, max: 5, nullCount: 0);
        var p = new ComparisonPredicate(
            new UnboundReference("x"), ComparisonOperator.NotEqual,
            new LiteralExpression(5));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(p, stats));
    }

    [Fact]
    public void NotEqual_VOutsideRange_NoNulls_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        var p = new ComparisonPredicate(
            new UnboundReference("x"), ComparisonOperator.NotEqual,
            new LiteralExpression(20));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(p, stats));
    }

    [Fact]
    public void NotEqual_VOutsideRange_WithNulls_Unknown()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 5);
        var p = new ComparisonPredicate(
            new UnboundReference("x"), ComparisonOperator.NotEqual,
            new LiteralExpression(20));
        Assert.Equal(FilterResult.Unknown, Eval(p, stats));
    }

    // ── LessThan / LessThanOrEqual ──

    [Fact]
    public void LessThan_MinGreaterThanOrEqualV_AlwaysFalse()
    {
        var stats = new TestStats().With("x", min: 10, max: 20);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.LessThan("x", 10), stats));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.LessThan("x", 5), stats));
    }

    [Fact]
    public void LessThan_MaxLessThanV_NoNulls_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.LessThan("x", 100), stats));
    }

    [Fact]
    public void LessThan_VInRange_Unknown()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.LessThan("x", 5), stats));
    }

    [Fact]
    public void LessThanOrEqual_MaxEqV_NoNulls_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.LessThanOrEqual("x", 10), stats));
    }

    // ── GreaterThan / GreaterThanOrEqual ──

    [Fact]
    public void GreaterThan_MaxLessThanOrEqualV_AlwaysFalse()
    {
        var stats = new TestStats().With("x", min: 1, max: 10);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.GreaterThan("x", 10), stats));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.GreaterThan("x", 100), stats));
    }

    [Fact]
    public void GreaterThan_MinGreaterThanV_NoNulls_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 10, max: 20, nullCount: 0);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.GreaterThan("x", 5), stats));
    }

    [Fact]
    public void GreaterThanOrEqual_MinEqV_NoNulls_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 10, max: 20, nullCount: 0);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.GreaterThanOrEqual("x", 10), stats));
    }

    // ── IS NULL / IS NOT NULL ──

    [Fact]
    public void IsNull_NullCountZero_AlwaysFalse()
    {
        var stats = new TestStats().With("x", nullCount: 0, valueCount: 100);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.IsNull("x"), stats));
    }

    [Fact]
    public void IsNull_AllNulls_AlwaysTrue()
    {
        var stats = new TestStats().With("x", nullCount: 50, valueCount: 50);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.IsNull("x"), stats));
    }

    [Fact]
    public void IsNull_SomeNulls_Unknown()
    {
        var stats = new TestStats().With("x", nullCount: 10, valueCount: 100);
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.IsNull("x"), stats));
    }

    [Fact]
    public void IsNotNull_NullCountZero_AlwaysTrue()
    {
        var stats = new TestStats().With("x", nullCount: 0, valueCount: 100);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.IsNotNull("x"), stats));
    }

    [Fact]
    public void IsNotNull_AllNulls_AlwaysFalse()
    {
        var stats = new TestStats().With("x", nullCount: 50, valueCount: 50);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.IsNotNull("x"), stats));
    }

    // ── IS NaN / IS NOT NaN ──

    private static UnaryPredicate IsNaN(string col) =>
        Expressions.IsNaN(new UnboundReference(col));

    private static UnaryPredicate IsNotNaN(string col) =>
        Expressions.IsNotNaN(new UnboundReference(col));

    [Fact]
    public void IsNaN_NanCountZero_AlwaysFalse()
    {
        var stats = new TestStats().With("x", nanCount: 0);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(IsNaN("x"), stats));
    }

    [Fact]
    public void IsNaN_AllValuesNaN_NoNulls_AlwaysTrue()
    {
        var stats = new TestStats().With("x", nullCount: 0, valueCount: 10, nanCount: 10);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(IsNaN("x"), stats));
    }

    [Fact]
    public void IsNaN_SomeNaN_Unknown()
    {
        var stats = new TestStats().With("x", nullCount: 0, valueCount: 10, nanCount: 3);
        Assert.Equal(FilterResult.Unknown, Eval(IsNaN("x"), stats));
    }

    [Fact]
    public void IsNaN_AllNaNButHasNulls_Unknown()
    {
        // NaNs cover every non-null value, but the nulls make IsNaN not provably all-true.
        var stats = new TestStats().With("x", nullCount: 2, valueCount: 12, nanCount: 10);
        Assert.Equal(FilterResult.Unknown, Eval(IsNaN("x"), stats));
    }

    [Fact]
    public void IsNaN_UnknownNanCount_Unknown()
    {
        // No nan_count recorded ⇒ NaNs may be present.
        var stats = new TestStats().With("x", nullCount: 0, valueCount: 10);
        Assert.Equal(FilterResult.Unknown, Eval(IsNaN("x"), stats));
    }

    [Fact]
    public void IsNotNaN_NanCountZero_NoNulls_AlwaysTrue()
    {
        var stats = new TestStats().With("x", nullCount: 0, valueCount: 10, nanCount: 0);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(IsNotNaN("x"), stats));
    }

    [Fact]
    public void IsNotNaN_AllValuesNaN_NoNulls_AlwaysFalse()
    {
        var stats = new TestStats().With("x", nullCount: 0, valueCount: 10, nanCount: 10);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(IsNotNaN("x"), stats));
    }

    [Fact]
    public void IsNotNaN_NanCountZeroButHasNulls_Unknown()
    {
        var stats = new TestStats().With("x", nullCount: 2, valueCount: 12, nanCount: 0);
        Assert.Equal(FilterResult.Unknown, Eval(IsNotNaN("x"), stats));
    }

    // ── In / NotIn ──

    [Fact]
    public void In_AllValuesOutsideRange_AlwaysFalse()
    {
        var stats = new TestStats().With("x", min: 10, max: 20);
        var p = Expressions.In("x", 1, 5, 30, 100);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(p, stats));
    }

    [Fact]
    public void In_SomeValuesInRange_Unknown()
    {
        var stats = new TestStats().With("x", min: 10, max: 20);
        var p = Expressions.In("x", 1, 15, 100);
        Assert.Equal(FilterResult.Unknown, Eval(p, stats));
    }

    [Fact]
    public void In_ListHoldingAnExpression_Unknown()
    {
        // `x IN (y, 5)` is partly a claim about ANOTHER column, and statistics do not pair rows
        // of x with rows of y -- min/max says nothing about whether any single row matches. The
        // list carries expressions now (#261), so this is the shape pruning has to decline.
        var stats = new TestStats().With("x", min: 10, max: 20);
        var p = Expressions.In(
            Expressions.Ref("x"),
            new Expression[] { Expressions.Ref("y"), new LiteralExpression(5) });

        Assert.Equal(FilterResult.Unknown, Eval(p, stats));

        // A list of literals still prunes, which is what most of them are.
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.In("x", 1, 5), stats));
    }

    [Fact]
    public void In_EmptyValues_AlwaysFalse()
    {
        var stats = new TestStats().With("x", min: 10, max: 20);
        var p = new SetPredicate(new UnboundReference("x"),
            Array.Empty<LiteralValue>(), SetOperator.In);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(p, stats));
    }

    [Fact]
    public void NotIn_EmptyValues_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 10, max: 20);
        var p = new SetPredicate(new UnboundReference("x"),
            Array.Empty<LiteralValue>(), SetOperator.NotIn);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(p, stats));
    }

    // ── And / Or / Not ──

    [Fact]
    public void And_AnyAlwaysFalse_AlwaysFalse()
    {
        var stats = new TestStats().With("x", min: 1, max: 10);
        var p = Expressions.And(
            Expressions.Equal("x", 100),  // AlwaysFalse
            Expressions.LessThan("x", 50)); // Unknown
        Assert.Equal(FilterResult.AlwaysFalse, Eval(p, stats));
    }

    [Fact]
    public void And_AllAlwaysTrue_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        var p = Expressions.And(
            Expressions.LessThan("x", 100),
            Expressions.GreaterThan("x", 0));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(p, stats));
    }

    [Fact]
    public void And_SomeUnknown_Unknown()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        var p = Expressions.And(
            Expressions.Equal("x", 5),     // Unknown
            Expressions.LessThan("x", 100)); // AlwaysTrue
        Assert.Equal(FilterResult.Unknown, Eval(p, stats));
    }

    [Fact]
    public void Or_AnyAlwaysTrue_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        var p = Expressions.Or(
            Expressions.Equal("x", 100),   // AlwaysFalse
            Expressions.LessThan("x", 100)); // AlwaysTrue
        Assert.Equal(FilterResult.AlwaysTrue, Eval(p, stats));
    }

    [Fact]
    public void Or_AllAlwaysFalse_AlwaysFalse()
    {
        var stats = new TestStats().With("x", min: 1, max: 10);
        var p = Expressions.Or(
            Expressions.Equal("x", 100),
            Expressions.Equal("x", 200));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(p, stats));
    }

    [Fact]
    public void Not_AlwaysFalse_AlwaysTrue()
    {
        var stats = new TestStats().With("x", min: 1, max: 10);
        var p = Expressions.Not(Expressions.Equal("x", 100));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(p, stats));
    }

    [Fact]
    public void Not_AlwaysTrue_AlwaysFalse()
    {
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        var p = Expressions.Not(Expressions.LessThan("x", 100));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(p, stats));
    }

    // ── Missing stats ──

    [Fact]
    public void MissingColumn_Unknown()
    {
        var stats = new TestStats();
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.Equal("nonexistent", 5), stats));
    }

    [Fact]
    public void MissingMinMax_Unknown()
    {
        var stats = new TestStats().With("x", nullCount: 0, valueCount: 100);
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.Equal("x", 5), stats));
    }

    // ── Operator flipping (literal on left) ──

    [Fact]
    public void Comparison_LiteralOnLeft_FlipsOperator()
    {
        // 100 > x  ≡  x < 100
        var stats = new TestStats().With("x", min: 1, max: 10, nullCount: 0);
        var p = new ComparisonPredicate(
            new LiteralExpression(100),
            ComparisonOperator.GreaterThan,
            new UnboundReference("x"));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(p, stats));
    }

    // ── Two literals (constant folding) ──

    [Fact]
    public void Comparison_TwoLiterals_FoldsToConstant()
    {
        var stats = new TestStats();
        var pTrue = new ComparisonPredicate(
            new LiteralExpression(5), ComparisonOperator.LessThan,
            new LiteralExpression(10));
        var pFalse = new ComparisonPredicate(
            new LiteralExpression(5), ComparisonOperator.GreaterThan,
            new LiteralExpression(10));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(pTrue, stats));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(pFalse, stats));
    }

    // ── String comparisons ──

    [Fact]
    public void StringComparison_OutsideRange_AlwaysFalse()
    {
        var stats = new TestStats().With("name", min: "alice", max: "frank");
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.Equal("name", "zoe"), stats));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.Equal("name", "alex"), stats));
    }

    // ── StartsWith ──

    [Fact]
    public void StartsWith_BothBoundsHavePrefix_AlwaysTrue()
    {
        var stats = new TestStats().With("name", min: "alpha", max: "alpine");
        Assert.Equal(FilterResult.AlwaysTrue,
            Eval(Expressions.StartsWith("name", "alp"), stats));
    }

    [Fact]
    public void StartsWith_MaxBeforePrefix_AlwaysFalse()
    {
        var stats = new TestStats().With("name", min: "alice", max: "bob");
        Assert.Equal(FilterResult.AlwaysFalse,
            Eval(Expressions.StartsWith("name", "zebra"), stats));
    }

    // ── NullSafeEqual ──

    [Fact]
    public void NullSafeEqual_NullValue_AllNullColumn_AlwaysTrue()
    {
        var stats = new TestStats().With("x", nullCount: 10, valueCount: 10);
        var p = new ComparisonPredicate(
            new UnboundReference("x"), ComparisonOperator.NullSafeEqual,
            new LiteralExpression(LiteralValue.Null));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(p, stats));
    }

    [Fact]
    public void NullSafeEqual_NonNullValue_AllNullColumn_AlwaysFalse()
    {
        var stats = new TestStats().With("x", nullCount: 10, valueCount: 10);
        var p = new ComparisonPredicate(
            new UnboundReference("x"), ComparisonOperator.NullSafeEqual,
            new LiteralExpression(5));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(p, stats));
    }

    /// <summary>
    /// Folding two literals under <c>&lt;=&gt;</c> asks the SQL comparison, so it keeps matching
    /// across numeric kinds after #206 made <c>Equals</c> representation-based.
    /// </summary>
    /// <remarks>
    /// This path read <c>a.Equals(b)</c>, which was SQL's relation until #206 and is not any more.
    /// Left alone it would have answered <c>AlwaysFalse</c> for <c>1 &lt;=&gt; 1.0d</c> -- a wrong
    /// prune, and a divergence from the row evaluator, which reaches <c>CompareTo</c>.
    /// </remarks>
    [Theory]
    [InlineData(1, 1.0d, true)]
    [InlineData(1, 2.0d, false)]
    public void NullSafeEqual_TwoLiterals_MatchesAcrossNumericKinds(int left, double right, bool expected)
    {
        Assert.Equal(
            expected ? FilterResult.AlwaysTrue : FilterResult.AlwaysFalse,
            EvalLiterals(LiteralValue.Of(left), LiteralValue.Of(right)));

        Assert.Equal(
            expected ? FilterResult.AlwaysTrue : FilterResult.AlwaysFalse,
            EvalLiterals(LiteralValue.Of(left), LiteralValue.Of((long)right)));
    }

    /// <summary>
    /// <c>&lt;=&gt;</c> over two literals answers for every pair instead of throwing out of a
    /// pruning entry point.
    /// </summary>
    /// <remarks>
    /// Measured before #206, all three of these raised <c>InvalidOperationException</c> from
    /// <c>Equals</c> and escaped <c>StatisticsEvaluator.Evaluate</c> -- the null handling lives in
    /// <c>CompareTo</c>, which this path did not reach, and <c>ConstantCompare</c>'s try/catch was
    /// bypassed. A caller pruning row groups got an exception for <c>x &lt;=&gt; NULL</c>.
    /// </remarks>
    [Fact]
    public void NullSafeEqual_TwoLiterals_DoesNotThrow_ForNullOrIncomparableKinds()
    {
        Assert.Equal(FilterResult.AlwaysTrue, EvalLiterals(LiteralValue.Null, LiteralValue.Null));
        Assert.Equal(FilterResult.AlwaysFalse, EvalLiterals(LiteralValue.Of(1), LiteralValue.Null));
        Assert.Equal(FilterResult.AlwaysFalse, EvalLiterals(LiteralValue.Null, LiteralValue.Of(1)));
        Assert.Equal(FilterResult.AlwaysFalse, EvalLiterals(LiteralValue.Of("x"), LiteralValue.Of(1)));
    }

    private static FilterResult EvalLiterals(LiteralValue a, LiteralValue b) =>
        Eval(
            new ComparisonPredicate(
                new LiteralExpression(a), ComparisonOperator.NullSafeEqual, new LiteralExpression(b)),
            new TestStats());

    // ── Function calls (not supported) ──

    [Fact]
    public void FunctionCallInComparison_Unknown()
    {
        var stats = new TestStats().With("ts", min: 1000, max: 2000);
        var p = new ComparisonPredicate(
            new FunctionCall("YEAR", new[] { (Expression)new UnboundReference("ts") }),
            ComparisonOperator.Equal,
            new LiteralExpression(2024));
        Assert.Equal(FilterResult.Unknown, Eval(p, stats));
    }

    // ── Lossy comparisons must not skip data (#208) ──

    private static Predicate Compare(string column, ComparisonOperator op, LiteralValue value) =>
        new ComparisonPredicate(new UnboundReference(column), op, new LiteralExpression(value));

    [Fact]
    public void BigintAgainstDouble_PastTwoToTheFiftyThree_IsUnknownRatherThanSkipping()
    {
        // The row group holds exactly 9007199254740993, which is 2^53+1 and has no double form.
        // Widening both sides to compare makes the column value look like 9007199254740992, and
        // the answer to `> 9007199254740992.0` flips from true to false. Acting on that skips the
        // row group and loses a row that matches.
        var v = LiteralValue.Of(9007199254740993L);
        var stats = new TestStats().With("b", min: v, max: v);
        var literal = LiteralValue.Of(9007199254740992d);

        Assert.Equal(FilterResult.Unknown, Eval(Compare("b", ComparisonOperator.GreaterThan, literal), stats));

        // And the other direction: the rounded values look equal, so equality claimed certainty
        // about a row that does not match.
        Assert.Equal(FilterResult.Unknown, Eval(Compare("b", ComparisonOperator.Equal, literal), stats));
    }

    [Fact]
    public void DecimalAgainstDouble_IsUnknownRatherThanSkipping()
    {
        // Same shape carried by a wide decimal, which is how #171 introduced it here: that
        // comparison used to throw, and throwing was safe because it produced Unknown.
        var v = LiteralValue.HighPrecisionDecimalOf(
            System.Numerics.BigInteger.Parse("9007199254740993"), 0);
        var stats = new TestStats().With("d", min: v, max: v);
        var literal = LiteralValue.Of(9007199254740992d);

        Assert.Equal(FilterResult.Unknown, Eval(Compare("d", ComparisonOperator.GreaterThan, literal), stats));
        Assert.Equal(FilterResult.Unknown, Eval(Compare("d", ComparisonOperator.Equal, literal), stats));
    }

    [Fact]
    public void OrdinaryIntegerAgainstDouble_StillPrunes()
    {
        // The cost of the rule has to stay proportional. Values inside 2^53 convert exactly, so
        // the ordinary predicate keeps its pruning -- only the values that genuinely cannot
        // round-trip give it up.
        var stats = new TestStats().With(
            "x", min: LiteralValue.Of(10L), max: LiteralValue.Of(20L), nullCount: 0);

        Assert.Equal(FilterResult.AlwaysFalse,
            Eval(Compare("x", ComparisonOperator.GreaterThan, LiteralValue.Of(25.5d)), stats));
        Assert.Equal(FilterResult.AlwaysTrue,
            Eval(Compare("x", ComparisonOperator.LessThan, LiteralValue.Of(25.5d)), stats));
        Assert.Equal(FilterResult.Unknown,
            Eval(Compare("x", ComparisonOperator.GreaterThan, LiteralValue.Of(15.5d)), stats));
    }

    [Fact]
    public void FloatAgainstDouble_StillPrunes()
    {
        // A float widens to double exactly, so nothing is given up here either. The nan_count says
        // no NaN hides above the maximum, which is what leaves the widening as the only question
        // this test is asking (#214).
        var stats = new TestStats().With(
            "f", min: LiteralValue.Of(1.5f), max: LiteralValue.Of(2.5f), nanCount: 0);

        Assert.Equal(FilterResult.AlwaysFalse,
            Eval(Compare("f", ComparisonOperator.GreaterThan, LiteralValue.Of(10d)), stats));
    }

    [Fact]
    public void ConstantComparison_KeepsTheDialectsAnswer_EvenWhenLossy()
    {
        // Folding two constants is NOT pruning: nothing is being skipped on the strength of a
        // column's range, so the answer has to be whatever the row-level evaluator produces for
        // the same two constants -- which widens them to double and calls them equal.
        //
        // Refusing the lossy answer here is not conservative, it is wrong in the dangerous
        // direction: a constant folds straight to AlwaysTrue or AlwaysFalse, so the "safe" sentinel
        // becomes AlwaysFalse and skips EVERYTHING. Review feedback on #209 caught this.
        var big = LiteralValue.Of(9007199254740993L);      // 2^53+1
        var dbl = LiteralValue.Of(9007199254740992d);

        Assert.Equal(FilterResult.AlwaysTrue, Eval(
            new ComparisonPredicate(
                new LiteralExpression(big), ComparisonOperator.Equal, new LiteralExpression(dbl)),
            new TestStats()));

        Assert.Equal(FilterResult.AlwaysFalse, Eval(
            new ComparisonPredicate(
                new LiteralExpression(big), ComparisonOperator.NotEqual, new LiteralExpression(dbl)),
            new TestStats()));

        Assert.Equal(FilterResult.AlwaysFalse, Eval(
            new ComparisonPredicate(
                new LiteralExpression(big), ComparisonOperator.GreaterThan, new LiteralExpression(dbl)),
            new TestStats()));
    }

    [Fact]
    public void NaNInTheMaximum_DoesNotSkipARowGroupThatMayMatch()
    {
        // Why the ordering matters beyond a scalar answer. A column whose maximum is NaN sits at
        // the TOP of Spark's order, so `col > 5.0` may well match. Ordering NaN at the bottom made
        // the maximum look smaller than the predicate and folded to AlwaysFalse -- the row group
        // skipped, and any NaN row lost.
        var stats = new TestStats().With(
            "g", min: LiteralValue.Of(1.0d), max: LiteralValue.Of(double.NaN), nullCount: 0);

        Assert.NotEqual(FilterResult.AlwaysFalse,
            Eval(new ComparisonPredicate(
                new UnboundReference("g"), ComparisonOperator.GreaterThan,
                new LiteralExpression(LiteralValue.Of(5.0d))), stats));

        // And a finite maximum still prunes, so the rule has not been blunted -- as long as the NaN
        // count says there is no NaN hiding behind it (#214; the nanCount: 0 is load-bearing).
        var finite = new TestStats().With(
            "h", min: LiteralValue.Of(1.0d), max: LiteralValue.Of(4.0d), nullCount: 0, nanCount: 0);
        Assert.Equal(FilterResult.AlwaysFalse,
            Eval(new ComparisonPredicate(
                new UnboundReference("h"), ComparisonOperator.GreaterThan,
                new LiteralExpression(LiteralValue.Of(5.0d))), finite));
    }

    // â”€â”€ A NaN the bounds do not mention (#214) â”€â”€
    //
    // Formats that follow Parquet's spec never write a NaN to min/max, so a file holding
    // [1.0, NaN] presents as an ordinary min = max = 1.0. Every conclusion below is checked
    // against the row those bounds do not describe.

    private static readonly LiteralValue Five = LiteralValue.Of(5.0d);
    private static readonly LiteralValue NaN = LiteralValue.Of(double.NaN);

    /// <summary>A float column whose bounds are finite and whose NaN count is not recorded.</summary>
    private static TestStats HiddenNaN(double min = 1.0, double max = 4.0, long? nullCount = 0) =>
        new TestStats().With("g",
            min: LiteralValue.Of(min), max: LiteralValue.Of(max), nullCount: nullCount);

    /// <summary>The same bounds, with the count that proves no NaN is there.</summary>
    private static TestStats NoNaN(double min = 1.0, double max = 4.0, long? nullCount = 0) =>
        new TestStats().With("g",
            min: LiteralValue.Of(min), max: LiteralValue.Of(max), nullCount: nullCount, nanCount: 0);

    [Fact]
    public void GreaterThan_HiddenNaNAboveTheMaximum_CannotPrune()
    {
        // The headline case. `NaN > 5.0` is TRUE, so a file whose maximum is 4.0 may still hold a
        // matching row. Pruning it on the strength of the maximum loses that row silently.
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.GreaterThan("g", Five), HiddenNaN()));
        Assert.Equal(FilterResult.Unknown,
            Eval(Expressions.GreaterThanOrEqual("g", Five), HiddenNaN()));

        // A recorded zero is what turns the pruning back on.
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.GreaterThan("g", Five), NoNaN()));
        Assert.Equal(FilterResult.AlwaysFalse,
            Eval(Expressions.GreaterThanOrEqual("g", Five), NoNaN()));
    }

    [Fact]
    public void LessThan_HiddenNaNIsBelowNothing_StillPrunes()
    {
        // The other direction survives untouched, which is the point of answering per operator
        // rather than refusing to prune float columns at all: `NaN < 5.0` is FALSE, so a minimum
        // above the predicate still proves the file cannot match.
        var above = HiddenNaN(min: 6.0, max: 8.0);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.LessThan("g", Five), above));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.LessThanOrEqual("g", Five), above));

        // And AlwaysTrue is the half that goes: every value being under 5.0 says nothing about a NaN.
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.LessThan("g", Five), HiddenNaN()));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.LessThan("g", Five), NoNaN()));
    }

    [Fact]
    public void GreaterThan_AlwaysTrueSurvivesAHiddenNaN()
    {
        // Mirror image: a NaN row satisfies `> 5.0` too, so an AlwaysTrue drawn from a minimum
        // above the predicate needs no NaN count.
        Assert.Equal(FilterResult.AlwaysTrue,
            Eval(Expressions.GreaterThan("g", Five), HiddenNaN(min: 6.0, max: 8.0)));
    }

    [Fact]
    public void Equal_HiddenNaNMatchesNothing_StillPrunes()
    {
        // Equality keeps its pruning, which matters -- it is the commonest predicate there is.
        // A NaN equals nothing but another NaN, so a value outside finite bounds is still absent.
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.Equal("g", Five), HiddenNaN()));

        // `g = NaN` is the mirror: the bounds cannot rule the NaN row in either, so it stays open.
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.Equal("g", NaN), HiddenNaN()));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.Equal("g", NaN), NoNaN()));

        // And `g <> NaN` is TRUE of every value the bounds describe and FALSE of the one they do
        // not, which is exactly when a definite answer has to be given up.
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.NotEqual("g", NaN), HiddenNaN()));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.NotEqual("g", NaN), NoNaN()));
    }

    [Fact]
    public void Equal_AlwaysTrueNeedsTheNaNRuledOut()
    {
        // The one equality conclusion a hidden NaN breaks: min == max == 5.0 says every value the
        // bounds cover is 5.0, and a NaN row is not covered.
        var pinned = new TestStats().With(
            "g", min: Five, max: Five, nullCount: 0);
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.Equal("g", Five), pinned));
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.NotEqual("g", Five), pinned));

        var counted = new TestStats().With(
            "g", min: Five, max: Five, nullCount: 0, nanCount: 0);
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.Equal("g", Five), counted));
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.NotEqual("g", Five), counted));
    }

    [Fact]
    public void In_HiddenNaN_PrunesUnlessTheListNamesANaN()
    {
        Assert.Equal(FilterResult.AlwaysFalse,
            Eval(Expressions.In("g", Five, LiteralValue.Of(6.0d)), HiddenNaN()));

        // ...but a NaN in the list is a value the hidden row could match.
        Assert.Equal(FilterResult.Unknown,
            Eval(Expressions.In("g", Five, NaN), HiddenNaN()));
        Assert.Equal(FilterResult.AlwaysFalse,
            Eval(Expressions.In("g", Five, NaN), NoNaN()));
    }

    [Fact]
    public void HiddenNaN_AppliesToEveryFloatWidthAndNoOthers()
    {
        // Float32 gets the same treatment as Float64...
        var f32 = new TestStats().With(
            "g", min: LiteralValue.Of(1.0f), max: LiteralValue.Of(4.0f), nullCount: 0);
        Assert.Equal(FilterResult.Unknown, Eval(Expressions.GreaterThan("g", Five), f32));

        // ...and an integer column is untouched, because its order has no NaN in it. Without this
        // the guard would cost every ordinary column its pruning.
        var ints = new TestStats().With("x", min: 10, max: 20, nullCount: 0);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.GreaterThan("x", 25), ints));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.GreaterThan("x", 5), ints));
    }

    [Fact]
    public void NaNInTheMaximum_NeedsNoNaNCount()
    {
        // A format that DOES record the NaN -- Spark's Delta writer puts it in maxValues -- already
        // says everything the guard would add, so nothing is lost by not knowing a count. An all-NaN
        // column still proves `g < 5.0` cannot match.
        var allNaN = new TestStats().With("g", min: NaN, max: NaN, nullCount: 0);
        Assert.Equal(FilterResult.AlwaysFalse, Eval(Expressions.LessThan("g", Five), allNaN));
        Assert.Equal(FilterResult.AlwaysTrue, Eval(Expressions.GreaterThan("g", Five), allNaN));
    }
}
