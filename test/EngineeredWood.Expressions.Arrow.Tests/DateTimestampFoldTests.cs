// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// A DATE folded with a TIMESTAMP, which Spark unifies to TIMESTAMP.
/// </summary>
/// <remarks>
/// <para>
/// #311, and it was a FALSE REJECTION — the inverse of #286. Every site that folds branch types
/// threw <c>no common type for timestamp and date32</c> where Spark resolves a timestamp, so a
/// CHECK constraint Spark accepts made the table unwritable through us. That direction is the
/// one a user hits without doing anything unusual, and it is worth keeping apart from #286's:
/// they are not the same bug with a sign flipped.
/// </para>
/// <para>
/// <b>Stated here as well as pinned in the corpus's <c>date-timestamp-fold</c> group</b>, for
/// the two things a corpus row cannot say on its own. The refused SITES are the measurement —
/// the rule is the fold, not any one function, so this file asks all seven rather than trusting
/// that <c>coalesce</c> speaks for <c>greatest</c> — and the DIRECTION of the promotion is
/// visible only in which instant comes back, which is why every case below asserts a value and
/// not merely a type.
/// </para>
/// <para>
/// <b><c>nullif</c> is here as a control.</b> It answered this pair before the fix and still
/// does, because it types from <c>args[0]</c> rather than folding — it SIDESTEPS the rule rather
/// than exercising it. Asking it alone is exactly the false reassurance that kept #311 invisible.
/// </para>
/// </remarks>
public class DateTimestampFoldTests
{
    private static readonly SparkFunctionRegistry Ansi = new();

    private static readonly SparkFunctionRegistry Legacy =
        new(new SparkDialectOptions { Ansi = false });

    /// <summary>2026-08-11 12:30:00Z, the corpus's <c>ts</c>.</summary>
    private static readonly DateTimeOffset Noon =
        new(2026, 8, 11, 12, 30, 0, TimeSpan.Zero);

    /// <summary>Midnight of the SAME day, which is where the corpus's <c>dt</c> promotes to.</summary>
    private static readonly DateTimeOffset Midnight =
        new(2026, 8, 11, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// The corpus's <c>ntz</c>: a WALL CLOCK on the same day, at neither of the other two.
    /// </summary>
    /// <remarks>
    /// 08:00 rather than 12:30 deliberately, in the corpus and here alike: a naive timestamp
    /// holding the same number as <c>ts</c> would let a fold that picked the wrong operand agree
    /// by coincidence, and the direction the fold moved is exactly what these tests read off the
    /// value. #349.
    /// </remarks>
    private static readonly DateTimeOffset Wall =
        new(2026, 8, 11, 8, 0, 0, TimeSpan.Zero);

    /// <summary>One row carrying a timestamp, a date on the same day, and an int to branch on.</summary>
    private static RecordBatch Row()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, true))
            .Field(new Field("ts", SparkArrays.Timestamp, true))
            .Field(new Field("dt", Date32Type.Default, true))
            .Field(new Field("ntz", SparkArrays.NaiveTimestamp, true))
            // NON-CANONICAL sources: a millisecond zoned timestamp and a millisecond naive one,
            // which is what a Parquet reader hands over and what no corpus row can supply.
            .Field(new Field("tsms", MilliZoned, true))
            .Field(new Field("ntzms", MilliNaive, true))
            .Build();

        var timestamps = new TimestampArray.Builder(SparkArrays.Timestamp);
        timestamps.Append(Noon);

        var dates = new Date32Array.Builder();
        dates.Append(Midnight);

        // The micros of a wall clock, in an array that does NOT claim a zone -- which is how
        // Delta's converter hands `timestamp_ntz` to this evaluator.
        var naive = new TimestampArray.Builder(SparkArrays.NaiveTimestamp);
        naive.Append(Wall);

        return new RecordBatch(schema, new IArrowArray[]
        {
            new Int32Array.Builder().Append(1).Build(),
            timestamps.Build(),
            dates.Build(),
            naive.Build(),
            Milli(MilliZoned, Noon),
            Milli(MilliNaive, Wall),
        }, 1);
    }

    private static readonly TimestampType MilliZoned = new(TimeUnit.Millisecond, "UTC");

    private static readonly TimestampType MilliNaive = new(TimeUnit.Millisecond, (string?)null);

    private static IArrowArray Milli(TimestampType type, DateTimeOffset value)
    {
        var builder = new TimestampArray.Builder(type);
        builder.Append(value);
        return builder.Build();
    }

    private static IArrowArray Evaluate(SparkFunctionRegistry registry, string expression) =>
        new ArrowRowEvaluator(registry)
            .EvaluateExpression(SparkSqlParser.ParseExpression(expression), Row());

    /// <summary>
    /// Every site that folds branch types resolves a TIMESTAMP, and picks the right instant.
    /// </summary>
    /// <remarks>
    /// The instant is what says the DATE moved UP rather than the timestamp being truncated
    /// down. Both orders of <c>coalesce</c> are asked, and <c>greatest</c> against
    /// <c>least</c>, because those are the pairs where the two candidate answers differ:
    /// 12:30:00Z is the timestamp and 00:00:00Z is the date at midnight, and a fold that
    /// truncated instead would answer midnight for all four.
    /// </remarks>
    [Theory]
    [InlineData("coalesce(ts, dt)", 12, 30)]
    [InlineData("coalesce(dt, ts)", 0, 0)]
    [InlineData("nvl(ts, dt)", 12, 30)]
    [InlineData("ifnull(dt, ts)", 0, 0)]
    [InlineData("if(a > 0, ts, dt)", 12, 30)]
    [InlineData("if(a < 0, ts, dt)", 0, 0)]
    [InlineData("nvl2(a, ts, dt)", 12, 30)]
    [InlineData("CASE WHEN a > 0 THEN ts ELSE dt END", 12, 30)]
    [InlineData("CASE WHEN a < 0 THEN ts ELSE dt END", 0, 0)]
    [InlineData("greatest(ts, dt)", 12, 30)]
    [InlineData("least(ts, dt)", 0, 0)]
    [InlineData("greatest(dt, ts)", 12, 30)]
    [InlineData("least(dt, ts)", 0, 0)]
    public void EverySiteThatFoldsBranchTypesUnifiesToTimestamp(string sql, int hour, int minute)
    {
        foreach (var registry in new[] { Ansi, Legacy })
        {
            var result = Evaluate(registry, sql);
            var timestamps = Assert.IsType<TimestampArray>(result);

            Assert.Equal(TimeUnit.Microsecond, ((TimestampType)timestamps.Data.DataType).Unit);
            Assert.Equal(
                new DateTimeOffset(2026, 8, 11, hour, minute, 0, TimeSpan.Zero),
                timestamps.GetTimestamp(0));
        }
    }

    /// <summary>
    /// <c>nullif</c> answered before the fix and is asked only so its silence stays explained.
    /// </summary>
    [Fact]
    public void NullifSidestepsTheFoldAndTypesFromItsFirstArgument()
    {
        var result = Assert.IsType<TimestampArray>(Evaluate(Ansi, "nullif(ts, dt)"));
        Assert.Equal(Noon, result.GetTimestamp(0));
    }

    /// <summary>The rule at the level it lives at, over types alone.</summary>
    /// <remarks>
    /// Two DATES and two TIMESTAMPS are here as well as the mixed pair, because the temporal
    /// branch is taken BEFORE <c>CommonType</c>'s identity check and so now owns them too. A
    /// timestamp in another unit or zone — which Parquet writes happily — resolves to the one
    /// this evaluator BUILDS rather than echoing the left operand's type back, and the last case
    /// is the one that says so.
    /// </remarks>
    [Fact]
    public void TheCommonTypeOfADateAndATimestampIsATimestamp()
    {
        var date = Date32Type.Default;
        var timestamp = SparkArrays.Timestamp;

        Assert.Same(timestamp, SparkNumericTypes.CommonType(date, timestamp));
        Assert.Same(timestamp, SparkNumericTypes.CommonType(timestamp, date));
        Assert.Same(timestamp, SparkNumericTypes.CommonType(timestamp, timestamp));
        Assert.IsType<Date32Type>(SparkNumericTypes.CommonType(date, date));

        Assert.Same(
            timestamp,
            SparkNumericTypes.CommonType(new TimestampType(TimeUnit.Millisecond, "UTC"), date));
    }

    /// <summary>
    /// A TIMESTAMP_NTZ folds too since #349, and the rule is ASYMMETRIC.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This test used to pin the opposite.</b> #311 kept a naive timestamp out of the fold on
    /// purpose — the harvest schema had no NTZ column, so folding one would have been inventing a
    /// rule — and this method recorded that decision by asserting the refusal. #349 added the
    /// column, the <c>timestamp-ntz</c> corpus group measured the rule, and the assertions below
    /// are the measurement rather than the guess the old ones were written to avoid.
    /// </para>
    /// <para>
    /// <b>A ZONED operand wins and a DATE does not force a zone</b>, which is the half a guess
    /// would have got wrong: measured on 4.0.3, <c>coalesce(ntz, dt)</c> is <c>timestamp_ntz</c>
    /// while <c>coalesce(ntz, ts)</c> is a zoned <c>timestamp</c>. Answering <c>Timestamp</c> for
    /// the first is exactly the reinterpretation Delta's own widening refuses — it permits
    /// <c>date -&gt; timestamp_ntz</c> and rejects <c>date -&gt;</c> zoned
    /// (<c>TypeWideningPolicyTests.Date32ToZonedTimestamp_IsNotWidened</c>).
    /// </para>
    /// </remarks>
    [Fact]
    public void ANaiveTimestampFoldsAndAZonedOperandWins()
    {
        var naive = SparkArrays.NaiveTimestamp;
        var zoned = SparkArrays.Timestamp;
        var date = Date32Type.Default;

        // No zoned operand: the naive type survives, which is what `coalesce(ntz, dt)` and
        // `coalesce(ntz, ntz)` resolve.
        Assert.Same(naive, SparkNumericTypes.CommonType(naive, naive));
        Assert.Same(naive, SparkNumericTypes.CommonType(naive, date));
        Assert.Same(naive, SparkNumericTypes.CommonType(date, naive));

        // One zoned operand anywhere, and the pair is zoned.
        Assert.Same(zoned, SparkNumericTypes.CommonType(naive, zoned));
        Assert.Same(zoned, SparkNumericTypes.CommonType(zoned, naive));

        // A naive timestamp in another UNIT normalises like a zoned one does, for the reason
        // #311 gave: the type resolved has to be the type the array is built at.
        Assert.Same(
            naive,
            SparkNumericTypes.CommonType(new TimestampType(TimeUnit.Millisecond, (string?)null), date));

        // An EMPTY zone string reads as naive, not as zoned. Nothing here produces one, and the
        // conservative direction is the one that cannot relabel a wall clock as an instant.
        Assert.Same(
            naive,
            SparkNumericTypes.CommonType(new TimestampType(TimeUnit.Microsecond, string.Empty), date));
    }

    /// <summary>
    /// The array a naive fold hands back is naive too, which is #349's gap 1.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>A LABELLING defect, and it survived #311 untouched.</b> <c>SparkFunctions.Unify</c>
    /// routed every temporal branch through <c>BuildTimestamp</c>, which stamps UTC on whatever
    /// it read — so a fold that resolved <c>timestamp_ntz</c> handed back a zoned array. The
    /// micros were right either way, because under the pinned UTC session zone a wall clock and
    /// an instant are the same number; what was wrong was the NAME, and a Delta generated column
    /// writes its schema from the name.
    /// </para>
    /// <para>
    /// Asserted on the ARRAY rather than on <c>CommonType</c>, because the two agreeing is the
    /// whole content of the fix — the resolution was already right.
    /// </para>
    /// </remarks>
    [Fact]
    public void ANaiveFoldBuildsANaiveArray()
    {
        foreach (var registry in new[] { Ansi, Legacy })
        {
            var naive = Assert.IsType<TimestampArray>(Evaluate(registry, "coalesce(ntz, ntz)"));
            Assert.True(string.IsNullOrEmpty(((TimestampType)naive.Data.DataType).Timezone));
            Assert.Equal(Wall, naive.GetTimestamp(0));

            // ...and the DATE pair, which is the one #311 refused outright.
            var withDate = Assert.IsType<TimestampArray>(Evaluate(registry, "coalesce(ntz, dt)"));
            Assert.True(string.IsNullOrEmpty(((TimestampType)withDate.Data.DataType).Timezone));
            Assert.Equal(Wall, withDate.GetTimestamp(0));

            // A zoned operand still produces a zoned array, and the same instant it always did.
            var zoned = Assert.IsType<TimestampArray>(Evaluate(registry, "coalesce(ntz, ts)"));
            Assert.Equal("UTC", ((TimestampType)zoned.Data.DataType).Timezone);
            Assert.Equal(Wall, zoned.GetTimestamp(0));

            // ...and the fold PICKS between them by instant, so `least` takes the date's midnight
            // where `coalesce` took the first operand.
            var least = Assert.IsType<TimestampArray>(Evaluate(registry, "least(ntz, dt)"));
            Assert.True(string.IsNullOrEmpty(((TimestampType)least.Data.DataType).Timezone));
            Assert.Equal(Midnight, least.GetTimestamp(0));
        }
    }

    /// <summary>
    /// A fold builds at a CANONICAL timestamp, never at a source column's own type.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The corpus cannot see this and never will</b>: the harvest schema has one timestamp
    /// column and it is microseconds, so no row of it carries a unit the evaluator would have to
    /// normalise. That is exactly how #349 nearly reintroduced the promise #311 removed — passing
    /// the RESOLVED type to <c>BuildTimestamp</c> is right for the zone and wrong for everything
    /// else, because two callers hand <c>Unify</c> a source column's raw type: <c>nullif</c>
    /// passes <c>args[0].Data.DataType</c> straight through, and <c>ConditionalType</c> keeps a
    /// sole surviving branch's own type.
    /// </para>
    /// <para>
    /// Measured on the branch before the fix, over a <c>timestamp(ms, UTC)</c> column:
    /// <c>coalesce(tsms)</c>, <c>coalesce(tsms, NULL)</c> and <c>nullif(tsms, dt)</c> all came
    /// back MILLISECOND, where every route that actually unified two types came back microsecond.
    /// Parquet writes millisecond timestamps happily, so this is an ordinary column rather than a
    /// contrived one. Caught on review of #380.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("coalesce(tsms)")]
    [InlineData("coalesce(tsms, NULL)")]
    // Against `dt`, not `ts`: the two timestamps hold the same instant, so `nullif` would
    // answer NULL and the value assertion would have nothing to check.
    [InlineData("nullif(tsms, dt)")]
    [InlineData("if(1 > 0, tsms, tsms)")]
    [InlineData("coalesce(tsms, ts)")]
    public void AFoldOverANonCanonicalSourceStillBuildsMicroseconds(string sql)
    {
        foreach (var registry in new[] { Ansi, Legacy })
        {
            var result = Assert.IsType<TimestampArray>(Evaluate(registry, sql));
            var type = (TimestampType)result.Data.DataType;

            Assert.Equal(TimeUnit.Microsecond, type.Unit);
            Assert.Equal("UTC", type.Timezone);
            Assert.Equal(Noon, result.GetTimestamp(0));
        }
    }

    /// <summary>...and the naive arm keeps its own canonical instance, not the source's.</summary>
    [Fact]
    public void ANaiveFoldOverANonCanonicalSourceIsCanonicalToo()
    {
        var result = Assert.IsType<TimestampArray>(Evaluate(Ansi, "coalesce(ntzms)"));
        var type = (TimestampType)result.Data.DataType;

        Assert.Equal(TimeUnit.Microsecond, type.Unit);
        Assert.True(string.IsNullOrEmpty(type.Timezone));
        Assert.Equal(Wall, result.GetTimestamp(0));
    }

    /// <summary>
    /// A temporal against a type Spark will not fold it with is still refused.
    /// </summary>
    /// <remarks>
    /// The fix adds a rule for one PAIR, not a general "temporal folds with anything". Measured,
    /// <c>coalesce(a, dt)</c> is <c>DATATYPE_MISMATCH.DATA_DIFF_TYPES</c> in Spark, so the
    /// refusal is the right answer and widening it would turn #311 into #286.
    /// </remarks>
    [Theory]
    [InlineData("coalesce(a, dt)")]
    [InlineData("greatest(a, ts)")]
    public void ANumberIsStillNotFoldedWithATemporal(string sql) =>
        Assert.Throws<NotSupportedException>(() => Evaluate(Ansi, sql));
}
