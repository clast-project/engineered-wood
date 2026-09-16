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

    /// <summary>One row carrying a timestamp, a date on the same day, and an int to branch on.</summary>
    private static RecordBatch Row()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, true))
            .Field(new Field("ts", SparkArrays.Timestamp, true))
            .Field(new Field("dt", Date32Type.Default, true))
            .Build();

        var timestamps = new TimestampArray.Builder(SparkArrays.Timestamp);
        timestamps.Append(Noon);

        var dates = new Date32Array.Builder();
        dates.Append(Midnight);

        return new RecordBatch(schema, new IArrowArray[]
        {
            new Int32Array.Builder().Append(1).Build(),
            timestamps.Build(),
            dates.Build(),
        }, 1);
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
    /// A TIMESTAMP_NTZ is left exactly where it was, because it was never measured.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Delta maps <c>timestamp_ntz</c> to an Arrow <c>TimestampType</c> with a NULL ZONE, so it
    /// arrives here looking just like a timestamp — and the corpus has no NTZ column, so nothing
    /// in #311's group says what Spark does with one. Folding it would have been inventing a
    /// rule, and the invented answer is wrong twice: Spark resolves <c>coalesce(ntz, dt)</c> to
    /// <c>timestamp_ntz</c>, and Delta's widening permits <c>date -&gt; timestamp_ntz</c> while
    /// refusing <c>date -&gt;</c> a ZONED timestamp, because that reads a naive calendar date as
    /// an absolute instant.
    /// </para>
    /// <para>
    /// <b>These are main's answers, pinned as such.</b> Both were verified against the branch
    /// point rather than asserted from the code: two naive timestamps resolved the naive type and
    /// an NTZ against a DATE threw, and both still do. #349 carries what it would take to answer
    /// them properly, which starts with an NTZ column in the harvest schema.
    /// </para>
    /// </remarks>
    [Fact]
    public void ANaiveTimestampIsNotThisRulesPairAndKeepsItsOldAnswer()
    {
        var naive = new TimestampType(TimeUnit.Microsecond, (string?)null);

        // Two naive timestamps take the identity arm, exactly as before #311.
        var both = Assert.IsType<TimestampType>(SparkNumericTypes.CommonType(naive, naive));
        Assert.Null(both.Timezone);

        // ...and a naive timestamp against a DATE is still refused, which is the gap #349 names.
        Assert.Throws<NotSupportedException>(
            () => SparkNumericTypes.CommonType(naive, Date32Type.Default));
        Assert.Throws<NotSupportedException>(
            () => SparkNumericTypes.CommonType(Date32Type.Default, naive));

        // An empty zone string is treated as naive too. Nothing in this repository produces one,
        // and the conservative direction is the one that cannot relabel a wall clock as an
        // instant.
        Assert.Throws<NotSupportedException>(
            () => SparkNumericTypes.CommonType(
                new TimestampType(TimeUnit.Microsecond, string.Empty), Date32Type.Default));
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
