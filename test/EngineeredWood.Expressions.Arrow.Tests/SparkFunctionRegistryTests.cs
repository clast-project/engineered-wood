// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
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


    // ── Integral casts under the legacy dialect: four source families, four rules (#243) ────

    /// <summary>
    /// An exact source WRAPS to the target's width.
    /// </summary>
    /// <remarks>
    /// The rule #243 was filed on, and the only one of the four that matches it. Read from the
    /// unscaled integer rather than from the <see cref="decimal"/> the evaluator holds, because
    /// 10^30 has no decimal form at all — which is why it used to answer null instead.
    /// </remarks>
    [Fact]
    public void TheLegacyDialectWrapsAnExactSourceToTheTargetWidth()
    {
        var wide = WideDecimalBatch(("big", System.Numerics.BigInteger.Pow(10, 30)));

        // 10^30 mod 2^32, and 10^30 mod 2^64. Both measured.
        Assert.Equal(1073741824,
            Assert.IsType<Int32Array>(Eval(Legacy, "CAST(big AS INT)", wide)).GetValue(0));
        Assert.Equal(5076944270305263616L,
            Assert.IsType<Int64Array>(Eval(Legacy, "CAST(big AS BIGINT)", wide)).GetValue(0));

        // 10^30 carries a factor of 2^30, so its low 16 and 8 bits are zero.
        Assert.Equal((short)0,
            Assert.IsType<Int16Array>(Eval(Legacy, "CAST(big AS SMALLINT)", wide)).GetValue(0));

        var negative = WideDecimalBatch(("big", -System.Numerics.BigInteger.Pow(10, 30)));
        Assert.Equal(-1073741824,
            Assert.IsType<Int32Array>(Eval(Legacy, "CAST(big AS INT)", negative)).GetValue(0));

        // An integral source narrowing takes the same rule, and so does a decimal inside
        // System.Decimal's range.
        var narrow = Batch(("b", Longs(300, -300, 4294967298L)));
        var bytes = Assert.IsType<Int8Array>(Eval(Legacy, "CAST(b AS TINYINT)", narrow));
        Assert.Equal((sbyte)44, bytes.GetValue(0));
        Assert.Equal((sbyte)-44, bytes.GetValue(1));
        Assert.Equal(2,
            Assert.IsType<Int32Array>(Eval(Legacy, "CAST(b AS INT)", narrow)).GetValue(2));

        // The fraction goes before the width does: 4294967298.5 truncates to 4294967298, whose
        // low 32 bits are 2.
        var fractional = Batch(("d", Decimals(20, 1, 4294967298.5m)));
        Assert.Equal(2,
            Assert.IsType<Int32Array>(Eval(Legacy, "CAST(d AS INT)", fractional)).GetValue(0));
    }

    /// <summary>
    /// A floating-point source SATURATES, and the clamp is at int even for a narrower target.
    /// </summary>
    /// <remarks>
    /// Scala's <c>toInt</c> saturates where <c>BigDecimal.longValue</c> wraps, so the same value
    /// answers differently depending on which type held it: 1e30 as a double is int.MaxValue and
    /// as a decimal is 1073741824. Generalising the wrap across families would have got every
    /// line here wrong.
    /// </remarks>
    [Fact]
    public void TheLegacyDialectSaturatesAFloatingSourceAtIntAndThenWraps()
    {
        var batch = Batch(("g", Doubles(1e30, -1e30, 4294967298.5, 300.0)));

        var ints = Assert.IsType<Int32Array>(Eval(Legacy, "CAST(g AS INT)", batch));
        Assert.Equal(int.MaxValue, ints.GetValue(0));
        Assert.Equal(int.MinValue, ints.GetValue(1));
        Assert.Equal(int.MaxValue, ints.GetValue(2));

        Assert.Equal(long.MaxValue,
            Assert.IsType<Int64Array>(Eval(Legacy, "CAST(g AS BIGINT)", batch)).GetValue(0));

        // The two rows that separate "clamp at the target" from "clamp at int, then wrap".
        // Clamping at the target would answer 127 and 127 instead of -1 and 44.
        var bytes = Assert.IsType<Int8Array>(Eval(Legacy, "CAST(g AS TINYINT)", batch));
        Assert.Equal((sbyte)-1, bytes.GetValue(2));
        Assert.Equal((sbyte)44, bytes.GetValue(3));

        // NaN is zero and an infinity clamps, neither of which has an integer to truncate.
        var special = Batch(("g", Doubles(double.NaN, double.PositiveInfinity)));
        var edge = Assert.IsType<Int32Array>(Eval(Legacy, "CAST(g AS INT)", special));
        Assert.Equal(0, edge.GetValue(0));
        Assert.Equal(int.MaxValue, edge.GetValue(1));
    }

    /// <summary>
    /// A string source yields NULL when it does not fit, and truncates a fraction.
    /// </summary>
    /// <remarks>
    /// Out of range is a failed PARSE for a string rather than an overflow, which is also why
    /// every string failure is CAST_INVALID_INPUT under ANSI and never CAST_OVERFLOW. The
    /// fraction is the one place the two dialects read the same text differently.
    /// </remarks>
    [Fact]
    public void AStringSourceNullsWhenItDoesNotFitAndTruncatesAFraction()
    {
        var batch = Batch(("s", Strings("4294967298", "12.5", "-12.9", "300.5")));

        var ints = Assert.IsType<Int32Array>(Eval(Legacy, "CAST(s AS INT)", batch));
        Assert.True(ints.IsNull(0));
        Assert.Equal(12, ints.GetValue(1));
        Assert.Equal(-12, ints.GetValue(2));

        // Truncated first, then out of range for the target — so null for the range and not for
        // the fraction.
        Assert.True(Assert.IsType<Int8Array>(Eval(Legacy, "CAST(s AS TINYINT)", batch)).IsNull(3));

        // ANSI refuses the same text, and names the parse rather than the range.
        foreach (var sql in new[] { "CAST(s AS INT)", "CAST(s AS TINYINT)" })
        {
            Assert.Equal(
                "CAST_INVALID_INPUT",
                Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, sql, batch)).ErrorClass);
        }
    }

    /// <summary>
    /// try_cast is not the legacy dialect, though neither of them raises.
    /// </summary>
    /// <remarks>
    /// One flag covered both for as long as every non-raising answer was null. It stops covering
    /// them the moment the legacy dialect answers a VALUE, and the corpus caught exactly that:
    /// try_cast yields null under EITHER dialect, including for the fraction the legacy cast
    /// truncates.
    /// </remarks>
    [Fact]
    public void TryCastNullsWhereTheLegacyDialectAnswers()
    {
        var numbers = Batch(("b", Longs(300)));
        Assert.Equal((sbyte)44,
            Assert.IsType<Int8Array>(Eval(Legacy, "CAST(b AS TINYINT)", numbers)).GetValue(0));

        foreach (var registry in new[] { Ansi, Legacy })
        {
            Assert.True(Assert.IsType<Int8Array>(
                Eval(registry, "TRY_CAST(b AS TINYINT)", numbers)).IsNull(0));
        }

        var text = Batch(("s", Strings("12.5")));
        Assert.Equal(12, Assert.IsType<Int32Array>(Eval(Legacy, "CAST(s AS INT)", text)).GetValue(0));

        foreach (var registry in new[] { Ansi, Legacy })
        {
            Assert.True(Assert.IsType<Int32Array>(
                Eval(registry, "TRY_CAST(s AS INT)", text)).IsNull(0));
        }
    }

    /// <summary>
    /// A temporal source yields null rather than wrapping, which no other exact-valued source does.
    /// </summary>
    /// <remarks>
    /// Spark checks that the epoch second round-trips through the target rather than truncating
    /// it, so this is the family that would have been wrong had #243's wrap been generalised.
    /// Unchanged behaviour, asserted because nothing else pins it.
    /// </remarks>
    [Fact]
    public void ATemporalSourceNullsRatherThanWrapping()
    {
        // 9999-12-31T23:59:59Z, whose epoch second fits a BIGINT and no narrower type.
        var batch = Batch(("ts", Timestamps(
            DateTimeOffset.FromUnixTimeSeconds(253402300799L))));

        Assert.Equal(253402300799L,
            Assert.IsType<Int64Array>(Eval(Legacy, "CAST(ts AS BIGINT)", batch)).GetValue(0));

        Assert.True(Assert.IsType<Int32Array>(Eval(Legacy, "CAST(ts AS INT)", batch)).IsNull(0));
        Assert.True(Assert.IsType<Int16Array>(Eval(Legacy, "CAST(ts AS SMALLINT)", batch)).IsNull(0));
    }


    // ── Double to decimal, through Spark's rendering of the value (#244) ────────────────────

    private static IArrowArray Floats(params float[] values)
    {
        var b = new FloatArray.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    /// <summary>
    /// A double past System.Decimal's ceiling reaches a decimal, where it used to be refused.
    /// </summary>
    [Fact]
    public void ADoublePastDecimalsCeilingCastsRatherThanBeingRefused()
    {
        var batch = Batch(("g", Doubles(1e30, -1e30, 1e37)));

        Assert.Equal("1000000000000000000000000000000", Rendered(Ansi, "CAST(g AS DECIMAL(38,0))", batch, 0));
        Assert.Equal("-1000000000000000000000000000000", Rendered(Ansi, "CAST(g AS DECIMAL(38,0))", batch, 1));
        Assert.Equal("10000000000000000000000000000000000000", Rendered(Ansi, "CAST(g AS DECIMAL(38,0))", batch, 2));

        // The scale is applied to the rendering, not to a truncated form of it. Its own batch,
        // because a cast runs over the whole column and 1e37 needs 39 digits at scale 2.
        Assert.Equal("1000000000000000000000000000000.00",
            Rendered(Ansi, "CAST(g AS DECIMAL(38,2))", Batch(("g", Doubles(1e30))), 0));
    }

    /// <summary>
    /// Spark converts the RENDERING of a double, which is what makes a float source surprising.
    /// </summary>
    /// <remarks>
    /// 1e30f widens to the double 1.0000000150474662E30, and Spark's answer is those digits — not
    /// the digits of 1e30, and not the exact binary value of the float. Reading the float's own
    /// shortest form instead would answer 1000000000000000000000000000000.
    /// </remarks>
    [Fact]
    public void AFloatSourceRendersTheWidenedDouble()
    {
        Assert.Equal("1000000015047466200000000000000",
            Rendered(Ansi, "CAST(f AS DECIMAL(38,0))", Batch(("f", Floats(1e30f))), 0));

        // A separate batch, because a cast runs over the whole column and 1e30 does not fit a
        // scale of 20.
        Assert.Equal("0.10000000149011612000",
            Rendered(Ansi, "CAST(f AS DECIMAL(38,20))", Batch(("f", Floats(0.1f))), 0));
    }

    /// <summary>
    /// The digits that <c>(decimal)double</c> used to round away are kept.
    /// </summary>
    /// <remarks>
    /// That conversion rounds to 15 significant digits where Spark keeps up to 17, so it was
    /// losing digits on values well inside <see cref="decimal"/>'s range — measured over ~1e6
    /// doubles, it disagreed with Spark's rendering on 93% of the ones a decimal could hold.
    /// </remarks>
    [Fact]
    public void SeventeenSignificantDigitsSurviveWhereFifteenUsedTo()
    {
        var batch = Batch(("g", Doubles(0.1, 0.3333333333333333)));

        Assert.Equal("0.100000000000000000000000000000",
            Rendered(Ansi, "CAST(g AS DECIMAL(38,30))", batch, 0));

        // Sixteen digits, and the value that made this depend on the target framework: net472's
        // ToString("R") renders it with seventeen. SparkFloatText.ShortestRoundTrip is what keeps
        // every build answering the same thing.
        Assert.Equal("0.33333333333333330000",
            Rendered(Ansi, "CAST(g AS DECIMAL(38,20))", batch, 1));
    }

    [Fact]
    public void TheShortestRenderingIsTheSameOnEveryTargetFramework()
    {
        // Asserted directly as well as through the cast, because the difference it guards against
        // shows up on one target framework only and would otherwise be invisible here.
        Assert.Equal("0.3333333333333333", SparkFloatText.ShortestRoundTrip(0.3333333333333333));
        Assert.Equal("0.1", SparkFloatText.ShortestRoundTrip(0.1));
        Assert.Equal("2.5", SparkFloatText.ShortestRoundTrip(2.5));

        // Every rendering must read back as the value it came from, whichever rung produced it.
        foreach (var value in new[] { 0.1, 2.5, 1e30, -1e30, 1.0000000150474662E30, 5e-324, double.MaxValue })
        {
            Assert.Equal(value, double.Parse(
                SparkFloatText.ShortestRoundTrip(value), NumberStyles.Float, CultureInfo.InvariantCulture));
        }
    }

    /// <summary>
    /// NaN and the infinities yield null rather than raising, even under ANSI.
    /// </summary>
    /// <remarks>
    /// The one refusal on this path that is not an error. Measured — every other failure of a
    /// cast to a decimal raises NUMERIC_VALUE_OUT_OF_RANGE under ANSI.
    /// </remarks>
    [Fact]
    public void NaNAndInfinityBecomeNullRatherThanRaising()
    {
        var batch = Batch(("g", Doubles(double.NaN, double.PositiveInfinity, double.NegativeInfinity)));

        var result = Assert.IsType<Decimal128Array>(Eval(Ansi, "CAST(g AS DECIMAL(10,2))", batch));

        Assert.True(result.IsNull(0));
        Assert.True(result.IsNull(1));
        Assert.True(result.IsNull(2));
    }

    /// <summary>A value the target cannot hold is the same refusal every other source gets.</summary>
    [Fact]
    public void ADoubleTooWideForItsTargetIsNumericValueOutOfRange()
    {
        var batch = Batch(("g", Doubles(1e39)));

        // NUMERIC_VALUE_OUT_OF_RANGE and not NUMERIC_OUT_OF_SUPPORTED_RANGE, which is what a
        // STRING of the same width reports: only the string route meets Spark's digit-count
        // fast-fail, and a double reaches the decimal by a different one. Measured.
        Assert.Equal(
            "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION",
            Assert.Throws<SparkEvaluationException>(
                () => Eval(Ansi, "CAST(g AS DECIMAL(38,0))", batch)).ErrorClass);

        Assert.True(Assert.IsType<Decimal128Array>(
            Eval(Legacy, "CAST(g AS DECIMAL(38,0))", batch)).IsNull(0));

        // Narrow enough to overflow without being past the ceiling at all.
        var small = Batch(("g", Doubles(12345)));
        Assert.Equal(
            "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION",
            Assert.Throws<SparkEvaluationException>(
                () => Eval(Ansi, "CAST(g AS DECIMAL(3,0))", small)).ErrorClass);
    }

    /// <summary>Renders one cell of a decimal result the way Spark prints it.</summary>
    private static string Rendered(SparkFunctionRegistry registry, string sql, RecordBatch batch, int row)
    {
        var result = Assert.IsType<Decimal128Array>(Eval(registry, sql, batch));
        var type = (Decimal128Type)result.Data.DataType;
        return SparkWideDecimals.Render(
            new SparkWideDecimals.Operand(SparkWideDecimals.Read(result, row)!.Value.Unscaled, 38, type.Scale));
    }


    // ── Printing a float or a double, which is Java's spelling and not .NET's (#248) ────────

    /// <summary>
    /// Java switches to scientific notation outside [1e-3, 1e7), and .NET switches elsewhere.
    /// </summary>
    /// <remarks>
    /// Every expectation is what <c>Double.toString</c> prints, which is what the corpus measured
    /// Spark printing. The .NET rendering each replaces is in the comment beside it — this is the
    /// half of #248 that has nothing to do with digit counts.
    /// </remarks>
    [Theory]
    [InlineData(1.0, "1.0")]                    // "R" gives 1
    [InlineData(2.5, "2.5")]
    [InlineData(1234567.0, "1234567.0")]        // the last magnitude that prints plainly
    [InlineData(12345678.0, "1.2345678E7")]     // "R" gives 12345678
    [InlineData(1e7, "1.0E7")]                  // "R" gives 10000000
    [InlineData(9999999.0, "9999999.0")]
    [InlineData(0.001, "0.001")]                // the smallest that prints plainly
    [InlineData(0.0001, "1.0E-4")]              // "R" gives 0.0001
    [InlineData(1e-7, "1.0E-7")]                // "R" gives 1E-07
    [InlineData(1e30, "1.0E30")]                // "R" gives 1E+30
    [InlineData(-1e30, "-1.0E30")]
    [InlineData(1e-30, "1.0E-30")]
    [InlineData(0.0, "0.0")]                    // "R" gives 0
    [InlineData(0.3333333333333333, "0.3333333333333333")]
    [InlineData(double.NaN, "NaN")]
    [InlineData(double.PositiveInfinity, "Infinity")]
    [InlineData(double.NegativeInfinity, "-Infinity")]
    public void ADoublePrintsTheWayJavaPrintsIt(double value, string expected) =>
        Assert.Equal(expected, SparkFloatText.Render(value));

    /// <summary>
    /// A float prints as a FLOAT, which is the opposite of what the cast to a decimal does.
    /// </summary>
    /// <remarks>
    /// <c>0.3333333f</c> prints as <c>0.3333333</c> here and converts to a decimal as the widened
    /// double's 0.3333333134651184 — measured on both paths. One ladder could not serve both.
    /// </remarks>
    [Theory]
    [InlineData(1e30f, "1.0E30")]
    [InlineData(0.1f, "0.1")]
    [InlineData(1.5f, "1.5")]
    [InlineData(0.3333333f, "0.3333333")]
    [InlineData(float.NaN, "NaN")]
    [InlineData(float.PositiveInfinity, "Infinity")]
    public void AFloatPrintsAsAFloatAndNotAsTheWidenedDouble(float value, string expected) =>
        Assert.Equal(expected, SparkFloatText.Render(value));

    [Fact]
    public void NegativeZeroKeepsItsSignWhenItSurvivesToTheRenderer()
    {
        // Java prints -0.0, and the sign bit is the only way to see it: `value < 0` is false.
        Assert.Equal("-0.0", SparkFloatText.Render(-0.0));
        Assert.Equal("0.0", SparkFloatText.Render(0.0));

        // It does NOT survive the SQL literal, which is why the corpus records 0.0 for
        // CAST(CAST(-0.0 AS DOUBLE) AS STRING): a fractional literal is a DECIMAL in Spark, and a
        // decimal has no negative zero to carry through the negation.
        var batch = Batch(("g", Doubles(1.0)));
        Assert.Equal("0.0", Assert.IsType<StringArray>(
            Eval(Ansi, "CAST(CAST(-0.0 AS DOUBLE) AS STRING)", batch)).GetString(0));
    }

    [Fact]
    public void CastingAColumnToStringUsesTheSameSpelling()
    {
        var batch = Batch(("g", Doubles(2.5, 1e30)), ("f", Floats(1.5f, 0.3333333f)));

        var doubles = Assert.IsType<StringArray>(Eval(Ansi, "CAST(g AS STRING)", batch));
        Assert.Equal("2.5", doubles.GetString(0));
        Assert.Equal("1.0E30", doubles.GetString(1));

        var floats = Assert.IsType<StringArray>(Eval(Ansi, "CAST(f AS STRING)", batch));
        Assert.Equal("1.5", floats.GetString(0));
        Assert.Equal("0.3333333", floats.GetString(1));
    }

    [Fact]
    public void TheFloatLadderIsShortestAndPortable()
    {
        // Nine significant digits always round-trip a float; the ladder stops earlier when it can.
        foreach (var value in new[] { 0.1f, 1.5f, 0.3333333f, 1e30f, float.Epsilon, float.MaxValue })
        {
            Assert.Equal(value, float.Parse(
                SparkFloatText.ShortestRoundTrip(value), NumberStyles.Float, CultureInfo.InvariantCulture));
        }

        Assert.Equal("0.1", SparkFloatText.ShortestRoundTrip(0.1f));
        Assert.Equal("0.3333333", SparkFloatText.ShortestRoundTrip(0.3333333f));
    }

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
        // accepting the call and failing somewhere less legible. current_timestamp is excluded
        // deliberately: it is non-deterministic, which Delta forbids in a constraint or a
        // generated column in the first place.
        Assert.False(Ansi.IsRegistered("current_timestamp"));
        Assert.False(Ansi.IsRegistered("to_date"));
        Assert.True(Ansi.IsRegistered("+"));
        Assert.True(Ansi.IsRegistered("cast"));
    }

    // ── Limits, which must refuse rather than crash ────────────────────────────────────────

    private static RecordBatch WideDecimalBatch(
        params (string Name, System.Numerics.BigInteger Unscaled)[] columns) =>
        WideDecimalBatch(0, columns);

    /// <summary>A decimal(38,s) column built from unscaled integers, past System.Decimal's reach.</summary>
    private static RecordBatch WideDecimalBatch(
        int scale, params (string Name, System.Numerics.BigInteger Unscaled)[] columns)
    {
        var type = new Decimal128Type(38, scale);
        var schema = new Schema.Builder();
        var arrays = new List<IArrowArray>();

        foreach (var (name, unscaled) in columns)
        {
            // Sign-extended by hand: BigInteger.ToByteArray gives the shortest two's complement
            // form, and Arrow wants all sixteen bytes.
            var bytes = new byte[16];
            if (unscaled.Sign < 0) bytes.AsSpan().Fill(0xFF);
            unscaled.ToByteArray().CopyTo(bytes, 0);

            schema.Field(new Field(name, type, true));
            arrays.Add(new Decimal128Array(new ArrayData(
                type, 1, 0, 0, new[] { ArrowBuffer.Empty, new ArrowBuffer(bytes) })));
        }

        return new RecordBatch(schema.Build(), arrays, 1);
    }

    /// <summary>The unscaled integer behind the single cell of a decimal result.</summary>
    private static System.Numerics.BigInteger Unscaled(Decimal128Array array)
    {
        // The byte[] overload rather than the span one: it reads signed little-endian two's
        // complement, which is Arrow's decimal layout, and net472 has only this one.
        return new System.Numerics.BigInteger(array.ValueBuffer.Span.Slice(0, 16).ToArray());
    }

    [Fact]
    public void ADecimalPastSystemDecimalsRangeIsEvaluatedRatherThanRefused()
    {
        // Spark decimals reach precision 38 where System.Decimal stops near 7.9e28. Arithmetic is
        // computed on the unscaled integer, so the top of the range is ordinary arithmetic rather
        // than the NotSupportedException it used to raise.
        var batch = WideDecimalBatch(
            ("big", System.Numerics.BigInteger.Pow(10, 30)),
            ("one", System.Numerics.BigInteger.One));

        var sum = Assert.IsType<Decimal128Array>(Eval(Ansi, "big + one", batch));

        Assert.Equal(System.Numerics.BigInteger.Pow(10, 30) + 1, Unscaled(sum));
    }

    [Theory]
    // Measured from Spark 4.0 via the expr_oracle driver on 2026-08-20, under the corpus's pinned
    // configuration (ansi on, UTC, ANSI store assignment). decimal(38,0) first:
    [InlineData(0, "1000000000000000000000000000000", "1000000000000000000000000000000")]
    [InlineData(0, "-1000000000000000000000000000000", "-1000000000000000000000000000000")]
    [InlineData(0, "99999999999999999999999999999999999999", "99999999999999999999999999999999999999")]
    // decimal(38,38), where the scale is the whole width. Trailing zeros are KEPT to the declared
    // scale, and the smallest magnitude prints all 37 leading zeros rather than in exponent form.
    [InlineData(38, "12345678901234567890123456789012345678", "0.12345678901234567890123456789012345678")]
    [InlineData(38, "-1", "-0.00000000000000000000000000000000000001")]
    [InlineData(38, "10000000000000000000000000000000000000", "0.10000000000000000000000000000000000000")]
    public void AWideDecimalRendersEveryDigitRatherThanAPlaceholder(
        int scale, string unscaled, string expected)
    {
        // Was #175: past System.Decimal's ceiling the exact form is null and Render emitted the
        // literal text "<out of range>" AS THE VALUE, and inside the ceiling a value carrying more
        // than 28 significant digits was silently rounded to 28. Rendering now works from the
        // unscaled integer and scale, which is exact across all of precision 38.
        var batch = WideDecimalBatch(scale, ("d",
            System.Numerics.BigInteger.Parse(unscaled, System.Globalization.CultureInfo.InvariantCulture)));

        var rendered = Assert.IsType<StringArray>(Eval(Ansi, "CAST(d AS STRING)", batch));
        Assert.Equal(expected, rendered.GetString(0));

        // It composed into ordinary string data through concat, which is what made it a
        // corruption rather than a cosmetic wart. Measured the same way.
        var joined = Assert.IsType<StringArray>(Eval(Ansi, "concat('v=', CAST(d AS STRING))", batch));
        Assert.Equal("v=" + expected, joined.GetString(0));
    }

    [Fact]
    public void ANarrowDecimalStillRendersExactlyAsItDidBefore()
    {
        // The other half of #175: rendering moved off System.Decimal for EVERY decimal, not just
        // wide ones, so the narrow cases have to be unchanged. Spark's answers for decimal(10,2),
        // measured in the same run — trailing zeros kept, sign attached, zero not special-cased.
        var batch = Batch(("a", Decimals(10, 2, 1.00m)), ("b", Decimals(10, 2, -1.50m)),
                          ("c", Decimals(10, 2, 0.00m)));

        Assert.Equal("1.00", Assert.IsType<StringArray>(Eval(Ansi, "CAST(a AS STRING)", batch)).GetString(0));
        Assert.Equal("-1.50", Assert.IsType<StringArray>(Eval(Ansi, "CAST(b AS STRING)", batch)).GetString(0));
        Assert.Equal("0.00", Assert.IsType<StringArray>(Eval(Ansi, "CAST(c AS STRING)", batch)).GetString(0));
    }

    [Fact]
    public void ACastNeedingExactnessRefusesPastTheCeilingRatherThanRaisingFromTheRead()
    {
        // Pins the pair that ReadForCast depends on: its +/-7.9e28 bound is stricter than
        // System.Decimal's ~7.9228e28 ceiling, so a value that passes the bound can never make the
        // exact read raise, and a value that fails it gets no exact form and is REFUSED by
        // whichever cast needs one. What must not happen either side of that line is a
        // NotSupportedException escaping the evaluator.
        //
        // Excess significant digits are not this line -- Decimal128Array rounds those to 28 and
        // reports success. That silent loss is #175 and deliberately not asserted here.
        var past = WideDecimalBatch(("big", System.Numerics.BigInteger.Pow(10, 30)));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(big AS INT)", past));
        Assert.Equal("CAST_OVERFLOW", ex.ErrorClass);

        // Under the bound the exact form is produced rather than refused, and nothing raises.
        var under = WideDecimalBatch(("small", new System.Numerics.BigInteger(42)));
        Assert.Equal(42, Assert.IsType<Int32Array>(Eval(Ansi, "CAST(small AS INT)", under)).GetValue(0));
    }

    [Fact]
    public void ComparingAWideDecimalAgainstADoubleDegradesToADoubleComparison()
    {
        // The one place the System.Decimal ceiling is still reachable, and the reason the refusal
        // in ExactDecimal has to stay. Equality is exact only where BOTH sides have an exact form;
        // a double never does, so the pair falls back to comparing as doubles — and getting there
        // runs the wide decimal through the exact-decimal read, which refuses. The refusal is the
        // mechanism that selects the fallback, not a failure.
        //
        // nullif rather than `=`: nullif is what reaches SparkFunctions.AreEqual, and it nulls
        // when the two are equal.
        //
        // 2^100 rather than a power of ten, and it is about 1.27e30 so it clears decimal's ceiling
        // near 7.9e28 either way. A power of two survives the unscaled BigInteger to double
        // conversion exactly, so this asserts on the fallback and not on a rounding difference.
        var value = System.Numerics.BigInteger.Pow(2, 100);
        var wide = WideDecimalBatch(("big", value));
        var batch = new RecordBatch(
            new Schema.Builder()
                .Field(new Field("big", wide.Schema.GetFieldByName("big").DataType, true))
                .Field(new Field("same", DoubleType.Default, true))
                .Field(new Field("other", DoubleType.Default, true))
                .Build(),
            new[] { wide.Column(0), Doubles(Math.Pow(2, 100)), Doubles(Math.Pow(2, 101)) },
            1);

        Assert.True(Eval(Ansi, "nullif(big, same)", batch).IsNull(0));

        var kept = Assert.IsType<Decimal128Array>(Eval(Ansi, "nullif(big, other)", batch));
        Assert.False(kept.IsNull(0));
        Assert.Equal(value, Unscaled(kept));
    }

    [Fact]
    public void AWideNegativeDecimalKeepsItsSignThroughTheUnscaledForm()
    {
        // Two's complement sign extension is the part of reading sixteen raw bytes that a positive
        // value cannot exercise.
        var batch = WideDecimalBatch(
            ("neg", -System.Numerics.BigInteger.Pow(10, 30)),
            ("one", System.Numerics.BigInteger.One));

        var sum = Assert.IsType<Decimal128Array>(Eval(Ansi, "neg + one", batch));

        Assert.Equal(-System.Numerics.BigInteger.Pow(10, 30) + 1, Unscaled(sum));
    }

    [Fact]
    public void ADiscardedHalfRoundsAwayFromZeroRatherThanToEven()
    {
        // 246913 / 2000000 is exactly 0.1234565, and decimal(38,0) / decimal(38,0) lands on
        // decimal(38,6), so the discarded digit is exactly half a unit with an even digit before
        // it — the one case where half-up and half-even disagree. Spark rounds half away from
        // zero, measured as CAST(2.5 AS DECIMAL(3,0)) = 3.
        var batch = WideDecimalBatch(("a", 246913), ("b", 2000000));

        var quotient = Assert.IsType<Decimal128Array>(Eval(Ansi, "a / b", batch));
        var type = Assert.IsType<Decimal128Type>(quotient.Data.DataType);

        Assert.Equal(6, type.Scale);
        Assert.Equal(123457, Unscaled(quotient));   // half to even would give 123456
    }

    [Fact]
    public void ANegativeQuotientRoundsAwayFromZeroToo()
    {
        // Away from zero, not down: the sign of the quotient decides the direction, and where the
        // quotient is zero the signs of the operands do. -1/2000000 is exactly -0.0000005, which
        // rounds to -0.000001 while its integer part never leaves zero.
        var batch = WideDecimalBatch(("a", -246913), ("b", 2000000), ("tiny", -1));

        Assert.Equal(-123457, Unscaled(Assert.IsType<Decimal128Array>(Eval(Ansi, "a / b", batch))));
        Assert.Equal(-1, Unscaled(Assert.IsType<Decimal128Array>(Eval(Ansi, "tiny / b", batch))));
    }

    [Fact]
    public void AResultBeyondTheDeclaredPrecisionOverflowsEvenWhereItFitsTheWidth()
    {
        // 6e37 + 6e37 is 1.2e38, which no decimal(38,0) can hold — but Int128 runs to about
        // 1.7e38, so the width alone does not catch it. Spark bounds a result by the precision it
        // declared, not by the machine word behind it.
        //
        // NUMERIC_VALUE_OUT_OF_RANGE, not ARITHMETIC_OVERFLOW: harvested, and not what this test
        // asserted when it was written from Spark's integer behaviour instead of measured. A
        // decimal result that will not fit names a different condition from an int one that will
        // not. See the wide-decimal group of the corpus.
        var big = System.Numerics.BigInteger.Parse(
            "60000000000000000000000000000000000000", System.Globalization.CultureInfo.InvariantCulture);
        var batch = WideDecimalBatch(("a", big), ("b", big));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "a + b", batch));
        Assert.Equal("NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION", ex.ErrorClass);

        // The legacy dialect nulls instead, which Spark's own message says it will: "set
        // spark.sql.ansi.enabled to false to bypass this error, and return NULL instead".
        var tolerated = Assert.IsType<Decimal128Array>(Eval(Legacy, "a + b", batch));
        Assert.True(tolerated.IsNull(0));
    }

    [Fact]
    public void IntegerOverflowKeepsItsOwnErrorClass()
    {
        // The other half of the split above: a decimal result that does not fit is
        // NUMERIC_VALUE_OUT_OF_RANGE, while the same condition on an int stays
        // ARITHMETIC_OVERFLOW. Both harvested.
        var batch = Batch(("a", Ints(int.MaxValue)), ("b", Ints(1)));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "a + b", batch));
        Assert.Equal("ARITHMETIC_OVERFLOW", ex.ErrorClass);
    }

    [Fact]
    public void AnAllScaleDecimalDividesAndAddsAtTheTopOfTheRange()
    {
        // decimal(38,38) is the hardest shape for division: the dividend pre-scales by 10^44,
        // which no 128-bit mantissa holds even where the quotient is exactly 1.
        //
        // Addition is the interesting half. decimal(38,38) + decimal(38,38) wants precision 39,
        // and clamping that back to 38 comes out of the SCALE — so the result is decimal(38,37),
        // narrower than either operand, and the sum has to be rounded down a digit rather than
        // simply carried. Both answers harvested into the corpus's wide-decimal group.
        var tenth = System.Numerics.BigInteger.Pow(10, 37);   // 0.1 at scale 38
        var batch = WideDecimalBatch(38, ("a", tenth), ("b", tenth));

        var sum = Assert.IsType<Decimal128Array>(Eval(Ansi, "a + b", batch));
        Assert.Equal(37, Assert.IsType<Decimal128Type>(sum.Data.DataType).Scale);
        Assert.Equal(2 * System.Numerics.BigInteger.Pow(10, 36), Unscaled(sum));   // 0.2

        var quotient = Assert.IsType<Decimal128Array>(Eval(Ansi, "a / b", batch));
        Assert.Equal(6, Assert.IsType<Decimal128Type>(quotient.Data.DataType).Scale);
        Assert.Equal(1000000, Unscaled(quotient));                                 // 1.000000
    }

    [Fact]
    public void CastingAWideDecimalRescalesItRatherThanRefusingIt()
    {
        var batch = WideDecimalBatch(("big", System.Numerics.BigInteger.Pow(10, 30)));

        var widened = Assert.IsType<Decimal128Array>(
            Eval(Ansi, "CAST(big AS DECIMAL(38,2))", batch));

        Assert.Equal(System.Numerics.BigInteger.Pow(10, 32), Unscaled(widened));
    }

    [Fact]
    public void CastingAWideDecimalRoundsAwayFromZeroAndRefusesWhatDoesNotFit()
    {
        // 2.5 and -2.5 at scale 1, cast to scale 0. Measured: CAST(2.5 AS DECIMAL(3,0)) is 3, so
        // the discarded half goes away from zero rather than to the even neighbour.
        var halves = WideDecimalBatch(1, ("up", 25), ("down", -25));

        Assert.Equal(3, Unscaled(Assert.IsType<Decimal128Array>(
            Eval(Ansi, "CAST(up AS DECIMAL(38,0))", halves))));
        Assert.Equal(-3, Unscaled(Assert.IsType<Decimal128Array>(
            Eval(Ansi, "CAST(down AS DECIMAL(38,0))", halves))));

        // A value that no longer fits the narrower target is Spark's CAST_OVERFLOW, and null in
        // the legacy dialect — the same split arithmetic overflow takes.
        var wide = WideDecimalBatch(("big", System.Numerics.BigInteger.Pow(10, 30)));

        // NUMERIC_VALUE_OUT_OF_RANGE rather than CAST_OVERFLOW. Harvested, and it is the target
        // type that decides: CAST(big AS INT) on the same value reports CAST_OVERFLOW.
        var ex = Assert.Throws<SparkEvaluationException>(
            () => Eval(Ansi, "CAST(big AS DECIMAL(10,0))", wide));
        Assert.Equal("NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION", ex.ErrorClass);

        Assert.Equal("CAST_OVERFLOW", Assert.Throws<SparkEvaluationException>(
            () => Eval(Ansi, "CAST(big AS INT)", wide)).ErrorClass);

        Assert.True(Assert.IsType<Decimal128Array>(
            Eval(Legacy, "CAST(big AS DECIMAL(10,0))", wide)).IsNull(0));
    }

    [Fact]
    public void AWideDecimalStillParticipatesWhereTheResultIsADouble()
    {
        // Converting to double is lossy either way, so the wide value costs nothing the target
        // type was going to keep.
        var batch = WideDecimalBatch(("big", System.Numerics.BigInteger.Pow(10, 30)));

        var result = Assert.IsType<DoubleArray>(Eval(Ansi, "CAST(big AS DOUBLE)", batch));

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

    [Fact]
    public void AnEpochSecondBeyondTheRepresentableRangeIsRefusedRatherThanCrashing()
    {
        var batch = Batch(("b", Longs(999_999_999_999_999L)));

        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(b AS TIMESTAMP)", batch));
        Assert.Equal("CAST_OVERFLOW", ex.ErrorClass);

        Assert.Null(Assert.IsType<TimestampArray>(
            Eval(Ansi, "TRY_CAST(b AS TIMESTAMP)", batch)).GetTimestamp(0));
    }

    [Fact]
    public void ALargeEpochSecondKeepsItsExactValueRatherThanGoingThroughDouble()
    {
        // Past 2^53 a double can no longer hold every integer, so routing epoch seconds through
        // one would shift the instant. Within DateTimeOffset's range the value must be exact.
        const long seconds = 253_402_300_799L; // 9999-12-31T23:59:59Z, the last representable second
        var batch = Batch(("b", Longs(seconds)));

        var result = Assert.IsType<TimestampArray>(Eval(Ansi, "CAST(b AS TIMESTAMP)", batch));
        Assert.Equal(seconds, result.GetTimestamp(0)!.Value.ToUnixTimeSeconds());
    }
}
