// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Arithmetic and CAST, checked against values measured from Spark.
/// </summary>
/// <remarks>
/// Every expectation here came from asking Spark, not from reading its documentation. Four of
/// them contradicted what the implementation first assumed, and each is called out where it
/// appears.
/// </remarks>
public sealed class SparkFunctionRegistryTests
{
    private static readonly SparkFunctionRegistry Ansi = new();
    private static readonly SparkFunctionRegistry Legacy = new(new SparkDialectOptions { Ansi = false });

    private static RecordBatch Batch(params (string Name, IArrowArray Array)[] columns)
    {
        var schema = new Schema.Builder();
        foreach (var (name, array) in columns)
            schema.Field(new Field(name, array.Data.DataType, true));

        return new RecordBatch(schema.Build(), columns.Select(c => c.Array), columns[0].Array.Length);
    }

    private static IArrowArray Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (var v in values) { if (v is { } x) b.Append(x); else b.AppendNull(); }
        return b.Build();
    }

    private static IArrowArray Shorts(params short[] values)
    {
        var b = new Int16Array.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    private static IArrowArray Longs(params long[] values)
    {
        var b = new Int64Array.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    private static IArrowArray Doubles(params double[] values)
    {
        var b = new DoubleArray.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    private static IArrowArray Strings(params string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(v); }
        return b.Build();
    }

    private static IArrowArray Decimals(int precision, int scale, params decimal[] values)
    {
        var b = new Decimal128Array.Builder(new Decimal128Type(precision, scale));
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    /// <summary>Parses and evaluates, which is the path a real constraint takes.</summary>
    private static IArrowArray Eval(SparkFunctionRegistry registry, string sql, RecordBatch batch) =>
        new ArrowRowEvaluator(registry).EvaluateExpression(SparkSqlParser.ParseExpression(sql), batch);

    // ── Arithmetic values ──────────────────────────────────────────────────────────────────

    [Fact]
    public void IntegerArithmeticKeepsItsWidthAndItsValues()
    {
        var batch = Batch(("a", Ints(7, 3, null)), ("b", Ints(2, 4, 5)));

        var sum = Assert.IsType<Int32Array>(Eval(Ansi, "a + b", batch));
        Assert.Equal(9, sum.GetValue(0));
        Assert.Equal(7, sum.GetValue(1));
        Assert.Null(sum.GetValue(2));

        Assert.Equal(1, Assert.IsType<Int32Array>(Eval(Ansi, "a % b", batch)).GetValue(0));
    }

    [Fact]
    public void DivisionOfTwoIntegersProducesADoubleRatherThanTruncating()
    {
        var batch = Batch(("a", Ints(7)), ("b", Ints(2)));

        var quotient = Assert.IsType<DoubleArray>(Eval(Ansi, "a / b", batch));
        Assert.Equal(3.5, quotient.GetValue(0));
    }

    [Fact]
    public void DecimalArithmeticProducesSparksResultTypeAndValue()
    {
        var batch = Batch(
            ("d1", Decimals(10, 2, 12.34m)),
            ("d2", Decimals(6, 4, 1.2345m)));

        var sum = Assert.IsType<Decimal128Array>(Eval(Ansi, "d1 + d2", batch));
        var type = Assert.IsType<Decimal128Type>(sum.Data.DataType);
        Assert.Equal(13, type.Precision);
        Assert.Equal(4, type.Scale);
        Assert.Equal(13.5745m, sum.GetValue(0));

        var product = Assert.IsType<Decimal128Array>(Eval(Ansi, "d1 * d2", batch));
        var productType = Assert.IsType<Decimal128Type>(product.Data.DataType);
        Assert.Equal(17, productType.Precision);
        Assert.Equal(6, productType.Scale);
    }

    [Fact]
    public void UnaryMinusOfTheMostNegativeIntegerOverflowsRatherThanReturningItself()
    {
        var batch = Batch(("a", Ints(int.MinValue)));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "-a", batch));
        Assert.Equal("ARITHMETIC_OVERFLOW", ex.ErrorClass);
    }

    // ── ANSI, which is where SparkDialectOptions first shows ───────────────────────────────

    [Fact]
    public void SmallintOverflowUsesSparksOtherOverflowClass()
    {
        // Measured: `smallint * smallint` reports BINARY_ARITHMETIC_OVERFLOW, while int and
        // bigint report ARITHMETIC_OVERFLOW. 200 * 200 exceeds smallint even though it is
        // nowhere near a 64-bit limit — integral arithmetic does not widen.
        var batch = Batch(("sh", Shorts(200)));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "sh * sh", batch));
        Assert.Equal("BINARY_ARITHMETIC_OVERFLOW", ex.ErrorClass);
    }

    [Fact]
    public void BigintOverflowUsesTheArithmeticClass()
    {
        var batch = Batch(("b", Longs(4_000_000_000L)));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "b * b", batch));
        Assert.Equal("ARITHMETIC_OVERFLOW", ex.ErrorClass);
    }

    [Theory]
    [InlineData("a / 0")]
    [InlineData("a % 0")]
    public void AZeroDivisorRaisesUnderAnsi(string sql)
    {
        var batch = Batch(("a", Ints(1)));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, sql, batch));
        Assert.Equal("DIVIDE_BY_ZERO", ex.ErrorClass);
    }

    [Fact]
    public void AZeroDivisorRaisesForFloatingPointToo()
    {
        // Not what IEEE 754 alone would suggest, and the implementation originally got this
        // wrong: measured, `g / g2` with a column holding 0.0 reports DIVIDE_BY_ZERO rather
        // than yielding infinity.
        var batch = Batch(("g", Doubles(1.5)), ("g2", Doubles(0.0)));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "g / g2", batch));
        Assert.Equal("DIVIDE_BY_ZERO", ex.ErrorClass);
    }

    [Fact]
    public void TheLegacyDialectProducesNullWhereAnsiRaises()
    {
        var batch = Batch(("a", Ints(1)));

        Assert.Null(Assert.IsType<DoubleArray>(Eval(Legacy, "a / 0", batch)).GetValue(0));
        Assert.Null(Assert.IsType<Int32Array>(Eval(Legacy, "a % 0", batch)).GetValue(0));
    }

    [Fact]
    public void TheLegacyDialectWrapsIntegerOverflowRatherThanRaising()
    {
        var batch = Batch(("a", Ints(int.MaxValue)), ("b", Ints(1)));

        Assert.Equal(int.MinValue,
            Assert.IsType<Int32Array>(Eval(Legacy, "a + b", batch)).GetValue(0));
    }

    // ── CAST ───────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void CastingANumberToAnIntegerTruncatesTowardZero()
    {
        var batch = Batch(("g", Doubles(1.7, -1.7)));

        var result = Assert.IsType<Int32Array>(Eval(Ansi, "CAST(g AS INT)", batch));
        Assert.Equal(1, result.GetValue(0));
        Assert.Equal(-1, result.GetValue(1));
    }

    [Fact]
    public void CastingAStringToAnIntegerRequiresAnInteger()
    {
        // The rule that differs from the numeric case, and the one the implementation first got
        // wrong: `CAST('12.5' AS INT)` is refused rather than truncated to 12.
        var batch = Batch(("s", Strings("12.5")));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(s AS INT)", batch));
        Assert.Equal("CAST_INVALID_INPUT", ex.ErrorClass);
    }

    [Fact]
    public void CastingAStringToAnIntegerTrimsWhitespace()
    {
        var batch = Batch(("s", Strings("  7 ")));

        Assert.Equal(7, Assert.IsType<Int32Array>(Eval(Ansi, "CAST(s AS INT)", batch)).GetValue(0));
    }

    [Fact]
    public void AnUnparseableStringIsInvalidInputAndAnOversizedNumberIsOverflow()
    {
        // Spark separates the two, so a caller can tell a malformed value from one that simply
        // does not fit.
        var text = Batch(("s", Strings("abc")));
        Assert.Equal("CAST_INVALID_INPUT",
            Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(s AS INT)", text)).ErrorClass);

        var oversized = Batch(("g", Doubles(1e30)));
        Assert.Equal("CAST_OVERFLOW",
            Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(g AS INT)", oversized)).ErrorClass);
    }

    [Fact]
    public void TryCastNeverRaisesEvenUnderAnsi()
    {
        var batch = Batch(("s", Strings("abc")));

        Assert.Null(Assert.IsType<Int32Array>(Eval(Ansi, "TRY_CAST(s AS INT)", batch)).GetValue(0));
    }

    [Fact]
    public void CastingToDecimalRoundsHalfAwayFromZero()
    {
        var batch = Batch(("g", Doubles(2.5, 1.45)));

        Assert.Equal(3m, Assert.IsType<Decimal128Array>(
            Eval(Ansi, "CAST(g AS DECIMAL(3,0))", batch)).GetValue(0));
        Assert.Equal(1.5m, Assert.IsType<Decimal128Array>(
            Eval(Ansi, "CAST(g AS DECIMAL(3,1))", batch)).GetValue(1));
    }

    [Fact]
    public void CastingToStringRendersTheValue()
    {
        var batch = Batch(("g", Doubles(1.5)));

        Assert.Equal("1.5", Assert.IsType<StringArray>(
            Eval(Ansi, "CAST(g AS STRING)", batch)).GetString(0));
    }

    [Fact]
    public void AnUnsupportedCastTargetIsRefusedByNameRatherThanSilentlyWrong()
    {
        // TIMESTAMP_NTZ is a real Spark type with no offset at all. It is deliberately not
        // aliased onto TIMESTAMP, because doing so would reinterpret values under the fixed
        // timezone rather than admit it is unsupported. A table carrying such a column must fail
        // closed with something a caller can act on.
        var batch = Batch(("g", Doubles(1.5)));

        var ex = Assert.Throws<NotSupportedException>(
            () => Eval(Ansi, "CAST(g AS TIMESTAMP_NTZ)", batch));
        Assert.Contains("TIMESTAMP_NTZ", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AnUnimplementedFunctionIsNotClaimedAsRegistered()
    {
        // ArrowRowEvaluator's own error is what a caller should see, rather than this registry
        // accepting the call and failing somewhere less legible.
        Assert.False(Ansi.IsRegistered("substring"));
        Assert.False(Ansi.IsRegistered("date_format"));
        Assert.True(Ansi.IsRegistered("+"));
        Assert.True(Ansi.IsRegistered("cast"));
    }

    // ── Limits, which must refuse rather than crash ────────────────────────────────────────

    /// <summary>A decimal(38,0) cell holding 10^30 — past System.Decimal's ~7.9e28 ceiling.</summary>
    private static RecordBatch WideDecimalBatch()
    {
        var type = new Decimal128Type(38, 0);
        var bytes = new byte[16];
        System.Numerics.BigInteger.Pow(10, 30).ToByteArray().CopyTo(bytes, 0);

        var array = new Decimal128Array(new ArrayData(
            type, 1, 0, 0, new[] { ArrowBuffer.Empty, new ArrowBuffer(bytes) }));

        var schema = new Schema.Builder().Field(new Field("big", type, true)).Build();
        return new RecordBatch(schema, new IArrowArray[] { array }, 1);
    }

    [Fact]
    public void ADecimalTooWideForExactArithmeticIsRefusedRatherThanCrashing()
    {
        // Spark decimals reach precision 38 where System.Decimal stops near 7.9e28, so
        // Decimal128Array.GetValue raises on a legitimate column value. It used to escape as a
        // bare OverflowException, which is a crash on table data rather than a refusal.
        var ex = Assert.Throws<NotSupportedException>(
            () => Eval(Ansi, "big + big", WideDecimalBatch()));

        Assert.Contains("too wide", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AWideDecimalStillParticipatesWhereTheResultIsADouble()
    {
        // Converting to double is lossy either way, so the wide value costs nothing the target
        // type was going to keep. Only exact arithmetic has to refuse.
        var result = Assert.IsType<DoubleArray>(
            Eval(Ansi, "CAST(big AS DOUBLE)", WideDecimalBatch()));

        Assert.Equal(1e30, result.GetValue(0)!.Value, 1e15);
    }

    [Theory]
    [InlineData("DECIMAL(10")]        // no closing paren
    [InlineData("DECIMAL(x,2)")]      // unparseable precision
    [InlineData("DECIMAL(10,2,3)")]   // too many parameters
    [InlineData("DECIMAL(0,0)")]      // precision below 1
    [InlineData("DECIMAL(50,2)")]     // precision beyond Spark's maximum
    [InlineData("DECIMAL(4,9)")]      // scale larger than precision
    public void AMalformedCastTargetIsNamedRatherThanLeakingAnInternalFailure(string target)
    {
        // Reachable from the public IFunctionRegistry surface, not only from the parser, so it
        // has to fail with something a caller can act on.
        var call = new FunctionCall("cast", new Expression[]
        {
            new UnboundReference("a"),
            new LiteralExpression(LiteralValue.Of(target)),
        });

        var ex = Assert.Throws<NotSupportedException>(() =>
            new ArrowRowEvaluator(Ansi).EvaluateExpression(call, Batch(("a", Ints(1)))));

        Assert.Contains(target, ex.Message, StringComparison.Ordinal);
    }

    // ── Temporal casts, where the timezone policy is load-bearing ──────────────────────────

    /// <summary>2026-08-11T03:00Z — 2026-08-11 in UTC, but 2026-08-10 in America/Los_Angeles.</summary>
    private static readonly DateTimeOffset Straddling =
        new(2026, 8, 11, 3, 0, 0, TimeSpan.Zero);

    private static IArrowArray Timestamps(params DateTimeOffset[] values)
    {
        var b = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    private static IArrowArray Dates(params DateTimeOffset[] values)
    {
        var b = new Date32Array.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    [Fact]
    public void CastingATimestampToADateResolvesInUtc()
    {
        // The measurement that settled the policy: this instant is 2026-08-11 in UTC and
        // 2026-08-10 in America/Los_Angeles, so the answer is a choice rather than a fact. A
        // generated column CAST(ts AS DATE) stores whichever the resolving zone says.
        var batch = Batch(("ts", Timestamps(Straddling)));

        var result = Assert.IsType<Date32Array>(Eval(Ansi, "CAST(ts AS DATE)", batch));
        Assert.Equal(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero),
            result.GetDateTimeOffset(0)!.Value);
    }

    [Fact]
    public void CastingADateToATimestampGivesUtcMidnight()
    {
        var batch = Batch(("dt", Dates(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero))));

        var result = Assert.IsType<TimestampArray>(Eval(Ansi, "CAST(dt AS TIMESTAMP)", batch));
        Assert.Equal(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero),
            result.GetTimestamp(0)!.Value);
    }

    [Fact]
    public void ATimestampRendersWithItsTimeAndADateWithoutOne()
    {
        Assert.Equal("2026-08-11 03:00:00", Assert.IsType<StringArray>(
            Eval(Ansi, "CAST(ts AS STRING)", Batch(("ts", Timestamps(Straddling))))).GetString(0));

        Assert.Equal("2026-08-11", Assert.IsType<StringArray>(
            Eval(Ansi, "CAST(dt AS STRING)",
                Batch(("dt", Dates(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero))))))
            .GetString(0));
    }

    [Fact]
    public void ATimestampCastsToEpochSecondsAndBack()
    {
        var batch = Batch(("ts", Timestamps(Straddling)));

        var seconds = Assert.IsType<Int64Array>(Eval(Ansi, "CAST(ts AS BIGINT)", batch));
        Assert.Equal(Straddling.ToUnixTimeSeconds(), seconds.GetValue(0));

        var back = Assert.IsType<TimestampArray>(
            Eval(Ansi, "CAST(CAST(ts AS BIGINT) AS TIMESTAMP)", batch));
        Assert.Equal(Straddling, back.GetTimestamp(0)!.Value);
    }

    [Fact]
    public void ADateHasNoIntegerFormBecauseSparkRefusesOne()
    {
        // Measured: CAST(DATE'…' AS LONG) is an error, unlike the timestamp case.
        var batch = Batch(("dt", Dates(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero))));

        Assert.Throws<NotSupportedException>(() => Eval(Ansi, "CAST(dt AS BIGINT)", batch));
    }

    [Fact]
    public void AStringParsesToADateOrATimestamp()
    {
        var batch = Batch(("s", Strings("2026-08-11 03:00:00")));

        Assert.Equal(Straddling, Assert.IsType<TimestampArray>(
            Eval(Ansi, "CAST(s AS TIMESTAMP)", batch)).GetTimestamp(0)!.Value);

        var dates = Batch(("s", Strings("2026-08-11")));
        Assert.Equal(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero),
            Assert.IsType<Date32Array>(Eval(Ansi, "CAST(s AS DATE)", dates)).GetDateTimeOffset(0)!.Value);
    }

    [Fact]
    public void AnUnparseableStringIsRefusedByBothTemporalCasts()
    {
        var batch = Batch(("s", Strings("abc")));

        Assert.Equal("CAST_INVALID_INPUT",
            Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(s AS DATE)", batch)).ErrorClass);
        Assert.Equal("CAST_INVALID_INPUT",
            Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(s AS TIMESTAMP)", batch)).ErrorClass);

        Assert.Null(Assert.IsType<Date32Array>(
            Eval(Ansi, "TRY_CAST(s AS DATE)", batch)).GetDateTimeOffset(0));
    }

    [Fact]
    public void TheTimezonePolicyIsUtcAndTheLiteralPathAgreesWithIt()
    {
        // The parser resolves a zone-less TIMESTAMP'…' literal as UTC, in a different assembly
        // that cannot see these options. This asserts the two agree, which is the coupling that
        // makes the policy fixed rather than settable.
        Assert.Equal(TimeZoneInfo.Utc, SparkDialectOptions.TimeZone);

        var batch = Batch(("ts", Timestamps(Straddling)));
        var result = Assert.IsType<BooleanArray>(
            new ArrowRowEvaluator(Ansi).EvaluatePredicate(
                SparkSqlParser.ParsePredicate("ts = TIMESTAMP'2026-08-11 03:00:00'"), batch));

        Assert.True(result.GetValue(0));
    }
}
