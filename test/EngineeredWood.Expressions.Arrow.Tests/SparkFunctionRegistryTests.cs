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


    /// <summary>
    /// A refused row names its own value, which is what deferring the rendering puts at risk.
    /// </summary>
    /// <remarks>
    /// <c>CastInput.Text</c> is rendered on demand for a numeric source (#251), from an array and
    /// an index carried in the struct rather than from a string built when the row was read. If
    /// either were wrong the message would name a different row's value — or no row's — and every
    /// existing test would still pass, because they assert error CLASSES and not messages.
    /// </remarks>
    [Fact]
    public void ADeferredRenderingNamesTheRowThatWasRefused()
    {
        // Row 2 is the one that overflows, and it is neither the first nor the last.
        var batch = Batch(("g", Doubles(1.0, 2.0, 1e30, 4.0)));

        var thrown = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(g AS INT)", batch));
        Assert.Contains("1.0E30", thrown.Message, StringComparison.Ordinal);

        // ...and in Java's spelling rather than .NET's, so the deferral goes through the same
        // renderer the eager path used.
        Assert.DoesNotContain("1E+30", thrown.Message, StringComparison.Ordinal);

        // The same for a decimal source, whose rendering comes from the unscaled buffer — and
        // through a cast to an INTEGRAL type, because that is one that reads Text. A cast from a
        // decimal to a DECIMAL does not: it takes CastExactToDecimal, which never builds a
        // CastInput and renders eagerly, so asserting on it would have proved nothing about the
        // deferral. Row 1 is in range for decimal(18,2) and out of range for INT.
        var decimals = Batch(("d", Decimals(18, 2, 1.00m, 99999999999.99m)));
        Assert.Contains(
            "99999999999.99",
            Assert.Throws<SparkEvaluationException>(
                () => Eval(Ansi, "CAST(d AS INT)", decimals)).Message,
            StringComparison.Ordinal);
    }


    // ── Wide decimal literals reach the evaluator (#173) ────────────────────────────────────

    /// <summary>
    /// A literal too wide for <see cref="decimal"/> materialises and computes, which is the seam
    /// #173 names: the same value worked as a COLUMN and not as a literal.
    /// </summary>
    [Fact]
    public void AWideLiteralComputesAgainstAWideColumn()
    {
        var batch = WideDecimalBatch(("d4", System.Numerics.BigInteger.Pow(10, 30)));

        // The issue's own example.
        Assert.Equal(
            "1000000000000000000000000000001",
            Rendered(Ansi, "d4 + 1", batch, 0));

        Assert.Equal(
            "2000000000000000000000000000000",
            Rendered(Ansi, "d4 + 1000000000000000000000000000000", batch, 0));

        Assert.True(Assert.IsType<BooleanArray>(
            Eval(Ansi, "d4 = 1000000000000000000000000000000", batch)).GetValue(0));
    }

    /// <summary>
    /// A wide literal's type comes from its own digits, and the digit count must be exact.
    /// </summary>
    /// <remarks>
    /// Counting with <c>BigInteger.Log10</c> got both boundaries wrong: 10^30 came back as thirty
    /// digits, because its logarithm is 29.999999999999996 in a double, and 10^38-1 as
    /// thirty-nine, because that one rounds up. The first is not cosmetic — a value needing
    /// thirty-one digits was handed a decimal(30,0) to live in, so NEGATING it overflowed and
    /// produced null. The second built a decimal(39,0), wider than any Spark decimal.
    /// </remarks>
    [Fact]
    public void AWideLiteralsPrecisionIsCountedExactly()
    {
        var batch = Batch(("a", Ints(1)));

        // 10^30 needs 31 digits. Under the old count this was null.
        var negated = Assert.IsType<Decimal128Array>(
            Eval(Ansi, "-1000000000000000000000000000000", batch));

        Assert.Equal(31, ((Decimal128Type)negated.Data.DataType).Precision);
        Assert.Equal(
            "-1000000000000000000000000000000",
            SparkWideDecimals.Render(SparkWideDecimals.Read(negated, 0)!.Value));

        // 38 nines needs 38, not 39 — a decimal(39,0) is wider than Spark has.
        var nines = Assert.IsType<Decimal128Array>(
            Eval(Ansi, "99999999999999999999999999999999999999", batch));

        Assert.Equal(38, ((Decimal128Type)nines.Data.DataType).Precision);
    }

    [Fact]
    public void AWideLiteralsScaleSurvivesMaterialisation()
    {
        var batch = Batch(("a", Ints(1)));

        // All scale, which is the shape that has no integral digits to fall back on.
        var value = Assert.IsType<Decimal128Array>(
            Eval(Ansi, "0.12345678901234567890123456789012345678", batch));

        var type = (Decimal128Type)value.Data.DataType;
        Assert.Equal(38, type.Precision);
        Assert.Equal(38, type.Scale);
        Assert.Equal(
            "0.12345678901234567890123456789012345678",
            SparkWideDecimals.Render(SparkWideDecimals.Read(value, 0)!.Value));
    }


    // ── round, greatest and least (#182) ────────────────────────────────────────────────────

    /// <summary>
    /// <c>round</c> is half AWAY FROM ZERO, which is what separates it from <c>bround</c>.
    /// </summary>
    [Theory]
    [InlineData("round(2.5)", "3")]
    [InlineData("round(-2.5)", "-3")]
    [InlineData("round(3.5)", "4")]
    [InlineData("round(0.5)", "1")]
    [InlineData("round(1.45, 1)", "1.5")]
    [InlineData("round(-1.45, 1)", "-1.5")]
    public void RoundGoesHalfAwayFromZero(string sql, string expected) =>
        Assert.Equal(expected, Rendered(Ansi, sql, Batch(("a", Ints(1))), 0));

    /// <summary>
    /// A decimal's result type moves with the scale, and a WIDER scale still widens the precision.
    /// </summary>
    /// <remarks>
    /// decimal(p,s) rounded to d becomes decimal(p - s + s' + 1, s') with s' = min(s, d). The
    /// third row is the one that is not obvious: rounding a decimal(10,2) to five places leaves
    /// the scale at 2 and still adds a digit of precision.
    /// </remarks>
    [Theory]
    [InlineData("round(d, 1)", 10, 1)]
    [InlineData("round(d, 0)", 9, 0)]
    [InlineData("round(d, 5)", 11, 2)]
    [InlineData("round(d, 2)", 11, 2)]
    public void RoundingADecimalMovesItsType(string sql, int precision, int scale)
    {
        var batch = Batch(("d", Decimals(10, 2, 12.34m)));
        var type = (Decimal128Type)Eval(Ansi, sql, batch).Data.DataType;

        Assert.Equal(precision, type.Precision);
        Assert.Equal(scale, type.Scale);
    }

    [Fact]
    public void RoundingAnIntegralTypeKeepsItAndTakesANegativeScale()
    {
        var batch = Batch(("b", Longs(12345, 10)));
        var rounded = Assert.IsType<Int64Array>(Eval(Ansi, "round(b, -2)", batch));

        Assert.Equal(12300L, rounded.GetValue(0));
        Assert.Equal(0L, rounded.GetValue(1));

        // A non-negative scale has nothing to round on an integer.
        Assert.Equal(12345L, Assert.IsType<Int64Array>(Eval(Ansi, "round(b, 2)", batch)).GetValue(0));
    }

    [Fact]
    public void RoundingPastAnIntegralTypesRangeIsArithmeticOverflow()
    {
        // Measured: `round(a, -1)` over INT_MIN reports ARITHMETIC_OVERFLOW rather than
        // CAST_OVERFLOW — rounding is arithmetic, not a conversion.
        var batch = Batch(("a", Ints(int.MinValue)));

        Assert.Equal(
            "ARITHMETIC_OVERFLOW",
            Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "round(a, -1)", batch)).ErrorClass);

        // THE LEGACY HALF WAS ASSUMED, NOT MEASURED, and it was wrong: this asserted null,
        // because "legacy yields null where ANSI raises" is true of so much else here. Measured
        // for #285, the legacy dialect WRAPS — the same thing an overflowing integral cast does
        // (#243) — so INT_MIN rounded to -2147483650 comes back as 2147483646.
        Assert.Equal(
            2147483646, Assert.IsType<Int32Array>(Eval(Legacy, "round(a, -1)", batch)).GetValue(0));
    }

    [Fact]
    public void RoundLeavesAloneWhatHasNoFractionToLose()
    {
        var batch = Batch(("g", Doubles(double.NaN, double.PositiveInfinity, 2.5)));
        var rounded = Assert.IsType<DoubleArray>(Eval(Ansi, "round(g, 2)", batch));

        Assert.True(double.IsNaN(rounded.GetValue(0)!.Value));
        Assert.True(double.IsPositiveInfinity(rounded.GetValue(1)!.Value));

        // A scale past what a double can hold is a no-op rather than an overflow.
        Assert.Equal(2.5, Assert.IsType<DoubleArray>(Eval(Ansi, "round(g, 20)", batch)).GetValue(2));
    }

    /// <summary>
    /// <c>greatest</c> and <c>least</c> SKIP nulls, which is the opposite of nearly everything else.
    /// </summary>
    [Fact]
    public void GreatestAndLeastSkipNullsRatherThanPropagatingThem()
    {
        var batch = Batch(("a", Ints(1, null)), ("b", Longs(10, 10)));

        var greatest = Assert.IsType<Int64Array>(Eval(Ansi, "greatest(a, b)", batch));
        Assert.Equal(10L, greatest.GetValue(0));
        Assert.Equal(10L, greatest.GetValue(1));      // a is null and is skipped, not propagated

        Assert.Equal(1L, Assert.IsType<Int64Array>(Eval(Ansi, "least(a, b)", batch)).GetValue(0));

        // A bare NULL is `void` and constrains neither the type nor the answer.
        var withNull = Assert.IsType<Int32Array>(Eval(Ansi, "greatest(a, NULL)", batch));
        Assert.Equal(1, withNull.GetValue(0));
        Assert.True(withNull.IsNull(1));

        // ...and every argument being null is the one case that answers null.
        Assert.True(SparkFunctions.IsNull(Eval(Ansi, "greatest(NULL, NULL)", batch), 0));
    }

    [Fact]
    public void GreatestAndLeastUnifyTheirArgumentTypes()
    {
        var batch = Batch(("a", Ints(1)), ("g", Doubles(2.5)), ("s", Strings("abc")), ("t", Strings("xyz")));

        Assert.Equal(2.5, Assert.IsType<DoubleArray>(Eval(Ansi, "greatest(a, g)", batch)).GetValue(0));
        Assert.Equal(1.0, Assert.IsType<DoubleArray>(Eval(Ansi, "least(a, g)", batch)).GetValue(0));
        Assert.Equal("xyz", Assert.IsType<StringArray>(Eval(Ansi, "greatest(s, t)", batch)).GetString(0));
    }

    [Fact]
    public void RoundRefusesAScaleThatVariesByRow()
    {
        // Spark requires a foldable scale and reports NON_FOLDABLE_INPUT for a column, so a scale
        // that differs between rows cannot come from an expression it accepted. This registry
        // sees columns rather than the tree, so it refuses the case it can actually see rather
        // than silently rounding every row to the first row's scale.
        var varying = Batch(("g", Doubles(2.5, 2.5)), ("n", Ints(1, 2)));
        Assert.Throws<NotSupportedException>(() => Eval(Ansi, "round(g, n)", varying));

        // A constant column is what a literal scale materialises as, and it still works.
        var constant = Batch(("g", Doubles(2.55, 2.55)), ("n", Ints(1, 1)));
        Assert.Equal(2.6, Assert.IsType<DoubleArray>(Eval(Ansi, "round(g, n)", constant)).GetValue(0));
    }

    [Fact]
    public void GreatestNeedsTwoArguments()
    {
        // Measured: Spark reports WRONG_NUM_ARGS for one argument as well as for none, rather
        // than treating a single argument as the identity.
        var batch = Batch(("a", Ints(1)));

        Assert.Throws<ArgumentException>(() => Eval(Ansi, "greatest(a)", batch));
    }


    // ── DATE and TIMESTAMP literals reach the evaluator (#254) ──────────────────────────────

    /// <summary>
    /// A DATE literal evaluates as a DATE, and a TIMESTAMP literal as a timestamp.
    /// </summary>
    /// <remarks>
    /// Both parsed and resolved before this and then threw at materialisation, which is the last
    /// step. The two must not collapse into one type: measured, <c>CAST(DATE'…' AS STRING)</c> is
    /// <c>2026-08-11</c>, where the same instant as a timestamp prints the time as well.
    /// </remarks>
    [Fact]
    public void ADateLiteralEvaluatesAsADate()
    {
        var batch = Batch(("a", Ints(1)));

        var date = Assert.IsType<Date32Array>(Eval(Ansi, "DATE'2026-08-11'", batch));
        Assert.Equal(
            new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero), date.GetDateTimeOffset(0));

        Assert.Equal(2026, Assert.IsType<Int32Array>(Eval(Ansi, "year(DATE'2026-08-11')", batch)).GetValue(0));

        Assert.Equal(
            "2026-08-11",
            Assert.IsType<StringArray>(
                Eval(Ansi, "CAST(DATE'2026-08-11' AS STRING)", batch)).GetString(0));
    }

    [Fact]
    public void ATimestampLiteralEvaluatesAsATimestamp()
    {
        var batch = Batch(("a", Ints(1)));

        var instant = Assert.IsType<TimestampArray>(
            Eval(Ansi, "TIMESTAMP'2026-08-11 12:30:00'", batch));

        Assert.Equal(
            new DateTimeOffset(2026, 8, 11, 12, 30, 0, TimeSpan.Zero), instant.GetTimestamp(0));

        // The distinction the lowering exists for: the same calendar day printed as a timestamp
        // keeps its time, where the DATE literal above does not have one.
        Assert.Equal(
            "2026-08-11 00:00:00",
            Assert.IsType<StringArray>(
                Eval(Ansi, "CAST(TIMESTAMP'2026-08-11 00:00:00' AS STRING)", batch)).GetString(0));
    }

    [Fact]
    public void ALiteralComparesAgainstAColumnOfItsOwnKind()
    {
        var batch = Batch(
            ("dt", Dates(new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero))),
            ("ts", Timestamps(new DateTimeOffset(2026, 8, 11, 12, 30, 0, TimeSpan.Zero))));

        Assert.True(Assert.IsType<BooleanArray>(
            Eval(Ansi, "DATE'2026-08-11' = dt", batch)).GetValue(0));

        Assert.True(Assert.IsType<BooleanArray>(
            Eval(Ansi, "TIMESTAMP'2026-08-11 12:30:00' = ts", batch)).GetValue(0));
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

    [Theory]
    [InlineData("1e3")]     // an exponent is a floating form, never an integral one
    [InlineData("1E3")]
    [InlineData("1e+3")]
    [InlineData("1.5e2")]
    [InlineData("1_0")]
    [InlineData("12abc")]
    public void AnIntegralCastRefusesTextItsParseDoesNotAccept(string text)
    {
        // Spark's integral parse takes a sign, digits and an optional point -- and no exponent,
        // in EITHER dialect. .NET's reads 1e3 as 1000, which is what we answered. #258.
        var batch = Batch(("s", Strings(text)));

        var thrown = Assert.Throws<SparkEvaluationException>(
            () => Eval(Ansi, "CAST(s AS BIGINT)", batch));
        Assert.Equal("CAST_INVALID_INPUT", thrown.ErrorClass);

        // The legacy dialect does not truncate it either -- the parse is what failed.
        Assert.True(Assert.IsType<Int64Array>(Eval(Legacy, "CAST(s AS BIGINT)", batch)).IsNull(0));
    }

    [Theory]
    [InlineData("1.0", 1L)]     // ANSI objects to the POINT, not to a non-zero fraction
    [InlineData("0.0", 0L)]
    [InlineData("10.", 10L)]
    [InlineData("1.", 1L)]
    [InlineData("-1.", -1L)]
    [InlineData(".0", 0L)]
    [InlineData("1.5", 1L)]
    public void ADecimalPointIsRefusedUnderAnsiAndTruncatedWithoutIt(string text, long truncated)
    {
        // Measured: every one of these is CAST_INVALID_INPUT under ANSI. Asking whether the
        // VALUE survives truncation accepted the first four, which is what #258 was.
        var batch = Batch(("s", Strings(text)));

        Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(s AS BIGINT)", batch));
        Assert.Equal(
            truncated, Assert.IsType<Int64Array>(Eval(Legacy, "CAST(s AS BIGINT)", batch)).GetValue(0));
    }

    [Theory]
    [InlineData("1d", 1d)]
    [InlineData("1D", 1d)]
    [InlineData("1.5f", 1.5d)]
    [InlineData("1e3d", 1000d)]
    public void AFloatingCastTakesJavasTypeSuffix(string text, double expected)
    {
        // Java's floating literal carries a trailing d/D/f/F and Spark's parse is Java's, so
        // CAST('1d' AS DOUBLE) is 1.0 where .NET reads nothing. Fail-CLOSED before #258: we
        // refused a value Spark answers.
        var batch = Batch(("s", Strings(text)));

        Assert.Equal(
            expected, Assert.IsType<DoubleArray>(Eval(Ansi, "CAST(s AS DOUBLE)", batch)).GetValue(0));
    }

    [Theory]
    [InlineData("NaNd")]        // the suffix attaches to a numeric form, not to a named one
    [InlineData("Infinityf")]
    [InlineData("1l")]          // and only to d/D/f/F
    [InlineData("1 d")]
    public void AFloatingCastTakesNoSuffixOnANamedOrSpacedForm(string text)
    {
        var batch = Batch(("s", Strings(text)));
        Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "CAST(s AS DOUBLE)", batch));
    }

    [Fact]
    public void OnlyAFloatingTargetTakesTheTypeSuffix()
    {
        // Measured: CAST('1d' AS DECIMAL(20,4)) is an error while CAST('1e3' AS DECIMAL(20,4))
        // is 1000 -- the decimal target takes the exponent the integral one refuses, and not the
        // suffix the floating one accepts. Three targets, three acceptances.
        var batch = Batch(("s", Strings("1d")));
        Assert.Throws<SparkEvaluationException>(
            () => Eval(Ansi, "CAST(s AS DECIMAL(20,4))", batch));
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
        // type was going to keep. EXACTLY 1e30, though: this assertion used to allow 1e15 of
        // slack and passed on 9.999999999999999E+29, which is #202.
        var batch = WideDecimalBatch(("big", System.Numerics.BigInteger.Pow(10, 30)));

        var result = Assert.IsType<DoubleArray>(Eval(Ansi, "CAST(big AS DOUBLE)", batch));

        Assert.Equal(1e30, result.GetValue(0)!.Value);
    }

    [Fact]
    public void ANarrowDecimalConvertsExactlyToo()
    {
        // The half of #202 the issue scoped OUT. This value fits System.Decimal, so the old code
        // took (double)GetValue(index) and never reached the wide fallback -- and answered
        // 5814944.017002577, an ulp above the nearest double. Measured, that cast is wrong on
        // 17.4% of the decimals it accepts, so the narrow path was not the safe one.
        var batch = WideDecimalBatch(
            11, ("d", System.Numerics.BigInteger.Parse("581494401700257601")));

        var result = Assert.IsType<DoubleArray>(Eval(Ansi, "CAST(d AS DOUBLE)", batch));

        // Written as a bit pattern rather than a literal so that reading the expectation does not
        // depend on the platform's parser, which on net472 is itself an ulp out.
        Assert.Equal(BitConverter.Int64BitsToDouble(0x41562EA8011691F9L), result.GetValue(0)!.Value);
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

    // ── nullif over operands with no exact System.Decimal form (#290) ───────────────────────

    /// <summary>
    /// A value <see cref="decimal"/> cannot hold no longer escapes as a bare BCL exception.
    /// </summary>
    /// <remarks>
    /// <c>SparkFunctions.AreEqual</c> compares exactly where both sides have an exact form and
    /// degrades to a double otherwise, and the degrade is signalled by an exception. The
    /// Decimal128 route raised <c>NotSupportedException</c> and was caught; a wide FLOAT or DOUBLE
    /// reached a checked conversion and raised <see cref="OverflowException"/>, which was not —
    /// so it left the evaluator as a raw BCL exception a caller cannot tell from a defect.
    /// <para>
    /// <b>The boundary is <see cref="decimal"/>'s, not Spark's</b>, which is what makes the pair
    /// below look arbitrary from Spark's side: 7.9e28 answered and 1e29 crashed, and Spark has no
    /// boundary anywhere near either. <c>nullif</c> is the only function that reaches this — the
    /// comparison operators answer their own way, and the corpus carries them as controls.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("nullif(g, 1e29)")]
    [InlineData("nullif(1e29, g)")]
    [InlineData("nullif(g, 1e308)")]
    [InlineData("nullif(g, CAST('NaN' AS DOUBLE))")]
    [InlineData("nullif(g, CAST('Infinity' AS DOUBLE))")]
    public void NullIfDoesNotLeakAnOverflowExceptionForAValueDecimalCannotHold(string sql)
    {
        var batch = Batch(("g", Doubles(1d)));

        foreach (var registry in new[] { Ansi, Legacy })
        {
            var result = Eval(registry, sql, batch);

            // Nothing here is equal, so the answer is the first operand rather than null. What is
            // being asserted is that there IS an answer.
            Assert.False(result.IsNull(0));
        }
    }

    /// <summary>
    /// The pair either way round, which used to disagree about whether to crash.
    /// </summary>
    /// <remarks>
    /// Two spellings of one comparison: <c>1e29</c> has no exact <see cref="decimal"/> form and
    /// neither does a <c>decimal(38,0)</c> holding it, so both sides degrade — but the two sides
    /// raised DIFFERENT exceptions and only the decimal one was caught, so writing the double
    /// first crashed and writing it second answered. Spark answers NULL to both.
    /// </remarks>
    [Fact]
    public void TheSamePairAnswersTheSameWhicheverSideIsWrittenFirst()
    {
        // The literal 1e29 is a DOUBLE and the column is a decimal(38,0) holding the same value,
        // so the two expressions are the same comparison with the operands swapped.
        var batch = WideDecimalBatch(("d", System.Numerics.BigInteger.Pow(10, 29)));

        Assert.True(Eval(Ansi, "nullif(1e29, d)", batch).IsNull(0));
        Assert.True(Eval(Ansi, "nullif(d, 1e29)", batch).IsNull(0));
    }

    /// <summary>
    /// The double fallback holds a NaN equal to itself, which is Spark's rule and not IEEE's.
    /// </summary>
    /// <remarks>
    /// <b>Only reachable once the crash above is fixed</b>, and it is the half that would have
    /// turned a loud error into a silent wrong answer: <c>==</c> on two NaNs is false, so a
    /// literal degrade to <c>ReadDouble(a) == ReadDouble(b)</c> would answer NaN where Spark
    /// answers NULL. Measured — <c>nullif(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE))</c> is
    /// NULL, and <c>=</c>, <c>&lt;=&gt;</c> and <c>IN</c> already agreed that NaN equals itself.
    /// </remarks>
    [Fact]
    public void TheDoubleFallbackUsesSparksNaNEquality()
    {
        var nan = Batch(("g", Doubles(double.NaN)));

        Assert.True(Eval(Ansi, "nullif(g, CAST('NaN' AS DOUBLE))", nan).IsNull(0));
        Assert.True(Eval(Ansi, "nullif(g, CAST('NaN' AS FLOAT))", nan).IsNull(0));
        Assert.True(Eval(Legacy, "nullif(g, CAST('NaN' AS DOUBLE))", nan).IsNull(0));

        // ...and a NaN against anything else is still unequal, so the rule has not become
        // "everything without an exact form is equal".
        Assert.False(Eval(Ansi, "nullif(g, 1e308)", nan).IsNull(0));

        // Signed zero is equal on both paths -- the exact one and this one.
        var zero = Batch(("g", Doubles(0d)));
        Assert.True(Eval(Ansi, "nullif(g, CAST(-0.0 AS DOUBLE))", zero).IsNull(0));
    }

    /// <summary>Two wide doubles that are not equal must not collapse to null.</summary>
    [Fact]
    public void TwoValuesWithNoExactFormAreStillCompared()
    {
        var batch = Batch(("g", Doubles(1e29)));

        Assert.False(Eval(Ansi, "nullif(g, 2e29)", batch).IsNull(0));
        Assert.True(Eval(Ansi, "nullif(g, 1e29)", batch).IsNull(0));
    }

    // ── nullif takes the ordinary comparison coercion (#298) ───────────────────────

    /// <summary>
    /// A string operand is CAST to the other operand's type, not compared as text.
    /// </summary>
    /// <remarks>
    /// Spark rewrites <c>nullif(a, b)</c> to <c>if(a = b, NULL, a)</c>, so the pair takes the
    /// #180/#259 rule. Every value below casts to the other operand while differing from it as
    /// text, so a text comparison answers the string where both dialects answer NULL — a wrong
    /// VALUE, not merely a different error class, which is what made this worth fixing rather
    /// than declaring.
    /// </remarks>
    [Theory]
    [InlineData("nullif(' 1', a)")]
    [InlineData("nullif('01', a)")]
    [InlineData("nullif('+1', a)")]
    [InlineData("nullif('1 ', a)")]
    [InlineData("nullif(a, ' 1')")]
    [InlineData("nullif(a, '01')")]
    public void NullIfCastsAStringOperandRatherThanComparingItAsText(string sql)
    {
        var batch = Batch(("a", Ints(1)));

        Assert.True(Eval(Ansi, sql, batch).IsNull(0));
        Assert.True(Eval(Legacy, sql, batch).IsNull(0));
    }

    /// <summary>
    /// The answer is the UNCOERCED first operand, at its own type.
    /// </summary>
    /// <remarks>
    /// The trailing <c>a</c> of <c>if(a = b, NULL, a)</c> is not the operand the comparison cast,
    /// so the coercion must not leak into the result. Two things could have leaked and both are
    /// asserted: the TEXT, since <c>' 2'</c> compared as the number 2 could have come back as
    /// "2", and the TYPE, since a date compared against a string could have come back as one.
    /// </remarks>
    [Fact]
    public void NullIfAnswersFromTheUncoercedFirstOperand()
    {
        var batch = Batch(("a", Ints(1)));

        var text = Assert.IsType<StringArray>(Eval(Ansi, "nullif(' 2', a)", batch));
        Assert.Equal(" 2", text.GetString(0));

        // The other order, where the result is the NUMBER and the string is what moved.
        var number = Assert.IsType<Int32Array>(Eval(Ansi, "nullif(a, ' 2')", batch));
        Assert.Equal(1, number.GetValue(0));

        // A date against a string: the string moves to the date, and the answer stays a date.
        var dates = Batch(("d", Eval(Ansi, "CAST('2026-08-11' AS DATE)", batch)));
        Assert.True(Eval(Ansi, "nullif(d, '2026-08-11')", dates).IsNull(0));
        Assert.IsType<Date32Array>(Eval(Ansi, "nullif(d, '1970-01-01')", dates));
    }

    /// <summary>
    /// A string opposite a NULL is never cast, and so is never refused.
    /// </summary>
    /// <remarks>
    /// <b>The mask, and the row that needs it cannot be written as a single-row corpus
    /// expression.</b> Spark's relational operators evaluate nothing once an operand is null, so
    /// a malformed string sitting opposite one is never read. Casting the column whole would
    /// refuse the whole BATCH — including the ordinary row beside it, which Spark answers.
    /// <para>
    /// Measured under ANSI: <c>nullif('abc', CAST(NULL AS INT))</c> is <c>'abc'</c> and
    /// <c>'abc' = CAST(NULL AS INT)</c> is null, while <c>'abc' &lt;=&gt; CAST(NULL AS INT)</c>,
    /// which has no such short-circuit, raises CAST_INVALID_INPUT.
    /// </para>
    /// </remarks>
    [Fact]
    public void NullIfDoesNotCastAStringOppositeANull()
    {
        // Row 0 is ordinary and equal; row 1 pairs a string no cast accepts with a null, which is
        // exactly the row Spark never reads.
        var batch = Batch(("s", Strings("1", "abc")), ("a", Ints(1, null)));

        var ansi = Eval(Ansi, "nullif(s, a)", batch);
        Assert.True(ansi.IsNull(0));
        Assert.Equal("abc", Assert.IsType<StringArray>(ansi).GetString(1));

        // ...and the batch still refuses when the malformed string really is opposite a value.
        var read = Batch(("s", Strings("abc")), ("a", Ints(1)));
        var ex = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, "nullif(s, a)", read));
        Assert.Equal("CAST_INVALID_INPUT", ex.ErrorClass);
    }

    /// <summary>
    /// Under the legacy dialect a string the cast refuses becomes null, so the comparison is null
    /// and the first operand comes back.
    /// </summary>
    /// <remarks>
    /// The left operand of the comparison can be null here without <c>args[0]</c> being null,
    /// which is why the row test asks about the COERCED operands and not the original ones.
    /// Measured: <c>nullif('abc', 1)</c> is <c>'abc'</c> under legacy and CAST_INVALID_INPUT
    /// under ANSI, and <c>nullif('1.0', 1)</c> splits the other way — legacy truncates to 1 and
    /// answers NULL where a text comparison answered <c>'1.0'</c>, which is neither dialect's.
    /// </remarks>
    [Fact]
    public void ALegacyStringThatWillNotCastLeavesTheFirstOperand()
    {
        var batch = Batch(("a", Ints(1)));

        Assert.Equal("abc", Assert.IsType<StringArray>(Eval(Legacy, "nullif('abc', a)", batch)).GetString(0));
        Assert.True(Eval(Legacy, "nullif('1.0', a)", batch).IsNull(0));
    }

    /// <summary>
    /// Two STRING operands are still compared as text, which is the pair that rule is right for.
    /// </summary>
    /// <remarks>
    /// The branch the fix had to leave alone. <c>nullif('1.0', '1')</c> is <c>'1.0'</c> in both
    /// dialects: neither operand moves, so nothing casts <c>'1.0'</c> to a number and the two
    /// texts simply differ. A BINARY against a string is the neighbouring case where the BINARY
    /// moves and is rendered as text (#259/#295), so it lands on the same branch from the other
    /// side — measured, <c>nullif(X'41', 'A')</c> is NULL.
    /// </remarks>
    [Fact]
    public void TwoStringOperandsAreStillComparedAsText()
    {
        var batch = Batch(("s", Strings("1.0")), ("t", Strings("1")));

        foreach (var registry in new[] { Ansi, Legacy })
        {
            Assert.Equal("1.0", Assert.IsType<StringArray>(Eval(registry, "nullif(s, t)", batch)).GetString(0));
            Assert.True(Eval(registry, "nullif(s, '1.0')", batch).IsNull(0));
            Assert.True(Eval(registry, "nullif(X'41', 'A')", batch).IsNull(0));
        }
    }

    /// <summary>
    /// Two exact numerics round to their least common type before being compared (#280).
    /// </summary>
    /// <remarks>
    /// The other half of what one <c>ComparisonTarget</c> call answers, and the half #298 did not
    /// name. #280 measured every comparison operator over this pair without a <c>nullif</c> among
    /// them, so the site kept an exact comparison where Spark rounds: measured,
    /// <c>CAST(1.005 AS DECIMAL(4,3)) = CAST(1 AS DECIMAL(38,0))</c> is TRUE, and the
    /// <c>nullif</c> spelling of it is NULL.
    /// <para>
    /// <b>An integral counts and its WIDTH decides the answer</b>, because it unifies as the
    /// decimal that holds it: against a decimal(38,38) the common scale is 35 for a tinyint and
    /// 18 for a bigint, so 4E-32 survives the first and rounds away under the second.
    /// </para>
    /// </remarks>
    [Fact]
    public void TwoExactNumericsRoundToTheirLeastCommonTypeBeforeComparison()
    {
        var batch = Batch(("d", Decimals(4, 3, 1.005m)), ("w", Decimals(38, 0, 1m)));

        foreach (var registry in new[] { Ansi, Legacy })
        {
            Assert.True(Eval(registry, "nullif(d, w)", batch).IsNull(0));
            Assert.True(Eval(registry, "nullif(w, d)", batch).IsNull(0));

            // ...and the result keeps the first operand's own type, unrounded.
            Assert.IsType<Decimal128Array>(Eval(registry, "nullif(d, CAST(2 AS DECIMAL(38,0)))", batch));
        }

        // Written as a CAST rather than as a column, because System.Decimal's scale stops at 28:
        // `0.00000000000000000000000000000004m` is a literal ZERO, and building the operand that
        // way asserted nothing while passing for the wrong reason.
        Assert.False(
            Eval(Ansi, "nullif(CAST(4E-32 AS DECIMAL(38,38)), CAST(0 AS TINYINT))", batch).IsNull(0));
        Assert.True(
            Eval(Ansi, "nullif(CAST(4E-32 AS DECIMAL(38,38)), CAST(0 AS BIGINT))", batch).IsNull(0));
    }

    // ── round at the top of a type's range (#285) ───────────────────────────────────────────

    /// <summary>
    /// Rounding away from zero past a type's maximum raises under ANSI and WRAPS under legacy.
    /// </summary>
    /// <remarks>
    /// <b>The BIGINT rows are the ones that were invisible.</b> The overflow check that was here
    /// compared the rounded value against the target's width, which catches a TINYINT, a SMALLINT
    /// and an INT — their arithmetic still has room in the <see cref="long"/> it is done in — but
    /// never a BIGINT, where the addition had already wrapped before the check could see it.
    /// <para>
    /// <b>Legacy wraps rather than nulling</b>, which is the half #285 did not name and the same
    /// shape #243 found for an integral cast. Measured: <c>round(CAST(127 AS TINYINT), -1)</c> is
    /// -126 with ansi off, the byte wrap of 130.
    /// </para>
    /// <para>
    /// Plain <c>ARITHMETIC_OVERFLOW</c> at every width — unlike <c>a + b</c>, which reports
    /// <c>BINARY_ARITHMETIC_OVERFLOW</c> for the narrow ones.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("round(9223372036854775807, -1)", -9223372036854775806L)]
    [InlineData("round(9223372036854775806, -1)", -9223372036854775806L)]
    [InlineData("round(CAST(-9223372036854775808 AS BIGINT), -1)", 9223372036854775806L)]
    [InlineData("round(CAST(2147483647 AS INT), -1)", -2147483646L)]
    [InlineData("round(CAST(-2147483648 AS INT), -1)", 2147483646L)]
    [InlineData("round(CAST(32767 AS SMALLINT), -1)", -32766L)]
    [InlineData("round(CAST(127 AS TINYINT), -1)", -126L)]
    [InlineData("round(CAST(-128 AS TINYINT), -1)", 126L)]
    public void RoundingPastATypesMaximumRaisesUnderAnsiAndWrapsUnderLegacy(string sql, long wrapped)
    {
        var batch = Batch(("a", Ints(1)));

        var thrown = Assert.Throws<SparkEvaluationException>(() => Eval(Ansi, sql, batch));
        Assert.Equal("ARITHMETIC_OVERFLOW", thrown.ErrorClass);

        Assert.Equal(wrapped, SparkArrays.ReadInt64(Eval(Legacy, sql, batch), 0));
    }

    [Theory]
    // Inside the range, so nothing raises in either dialect.
    [InlineData("round(CAST(124 AS TINYINT), -1)", 120L)]
    [InlineData("round(9223372036854775807, -18)", 9000000000000000000L)]
    [InlineData("round(123, -3)", 0L)]
    // A non-negative scale on an integral has nothing to round.
    [InlineData("round(9223372036854775807)", 9223372036854775807L)]
    [InlineData("round(9223372036854775807, 2)", 9223372036854775807L)]
    public void RoundingInsideTheRangeIsUnchanged(string sql, long expected)
    {
        var batch = Batch(("a", Ints(1)));

        Assert.Equal(expected, SparkArrays.ReadInt64(Eval(Ansi, sql, batch), 0));
        Assert.Equal(expected, SparkArrays.ReadInt64(Eval(Legacy, sql, batch), 0));
    }

    /// <summary>
    /// The three bands a negative scale falls into, where only the middle one is subtle.
    /// </summary>
    /// <remarks>
    /// At 19 places the step is 10^19, which no <see cref="long"/> holds, so the only answers are
    /// 0 and ±10^19 and the second always overflows — but the TEST is exact, because half of
    /// 10^19 is 5e18 and that does fit. At 20 and beyond half the step is past a long's ceiling
    /// altogether and every value rounds to zero. The old code answered 0 for everything past 18,
    /// which got the middle band wrong in both dialects.
    /// </remarks>
    [Fact]
    public void ANegativeScalePastEighteenPlacesHasThreeBands()
    {
        var batch = Batch(("a", Ints(1)));

        // Below half of 10^19: zero, and no overflow.
        Assert.Equal(0L, SparkArrays.ReadInt64(Eval(Ansi, "round(4999999999999999999, -19)", batch), 0));

        // At half: rounds away to 10^19, which no BIGINT holds.
        Assert.Equal(
            "ARITHMETIC_OVERFLOW",
            Assert.Throws<SparkEvaluationException>(
                () => Eval(Ansi, "round(5000000000000000000, -19)", batch)).ErrorClass);

        // ...and legacy hands back the low 64 bits of 10^19.
        Assert.Equal(
            -8446744073709551616L,
            SparkArrays.ReadInt64(Eval(Legacy, "round(5000000000000000000, -19)", batch), 0));

        // Past 19, half the step is out of reach and everything is zero.
        Assert.Equal(0L, SparkArrays.ReadInt64(Eval(Ansi, "round(9223372036854775807, -20)", batch), 0));
        Assert.Equal(0L, SparkArrays.ReadInt64(Eval(Ansi, "round(123, -19)", batch), 0));
    }

    /// <summary>
    /// A negative scale on a DECIMAL rounds to a multiple of a power of ten, in one step.
    /// </summary>
    /// <remarks>
    /// It used to do nothing at all: the result scale clamps to 0 either way, so the old code
    /// rescaled to scale 0 and stopped, and every negative scale answered the same as scale 0.
    /// <para>
    /// <b>14.6 is the row that shows the rounding happens once.</b> Rounding to an integer first
    /// and to the multiple second would answer 20, by way of 15; Spark answers 10.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("round(CAST(14.6 AS DECIMAL(10,1)), -1)", 10, 0, "10")]
    [InlineData("round(CAST(-14.6 AS DECIMAL(10,1)), -1)", 10, 0, "-10")]
    [InlineData("round(CAST(15.0 AS DECIMAL(10,1)), -1)", 10, 0, "20")]
    [InlineData("round(CAST(14.9 AS DECIMAL(10,1)), -1)", 10, 0, "10")]
    [InlineData("round(CAST(4.6 AS DECIMAL(10,1)), -1)", 10, 0, "0")]
    [InlineData("round(CAST(12.34 AS DECIMAL(10,2)), -1)", 9, 0, "10")]
    [InlineData("round(CAST(12.34 AS DECIMAL(10,2)), -3)", 9, 0, "0")]
    [InlineData("round(CAST(15 AS DECIMAL(2,0)), -1)", 3, 0, "20")]
    [InlineData("round(CAST(5 AS DECIMAL(1,0)), -1)", 2, 0, "10")]
    // The carry the extra digit of precision exists for.
    [InlineData("round(CAST(99 AS DECIMAL(2,0)), -1)", 3, 0, "100")]
    // ...and the row that says the type reserves max(p - s, places) integral digits, not p - s.
    [InlineData("round(CAST(99 AS DECIMAL(2,0)), -20)", 21, 0, "0")]
    public void ANegativeScaleOnADecimalRoundsToAMultipleOfATenPower(
        string sql, int precision, int scale, string expected)
    {
        var batch = Batch(("a", Ints(1)));

        foreach (var registry in new[] { Ansi, Legacy })
        {
            var result = Assert.IsType<Decimal128Array>(Eval(registry, sql, batch));
            var type = (Decimal128Type)result.Data.DataType;

            Assert.Equal((precision, scale), (type.Precision, type.Scale));
            Assert.Equal(expected, SparkWideDecimals.Render(SparkWideDecimals.Read(result, 0)!.Value));
        }
    }

    /// <summary>
    /// An extreme scale rounds to zero rather than overflowing the arithmetic that reads it.
    /// </summary>
    /// <remarks>
    /// <paramref name="scale"/> is an <c>int</c>, so <c>-scale</c> overflows back to a negative
    /// for <c>int.MinValue</c>: the integral loop then ran zero times and returned the value
    /// UNROUNDED, and the decimal path built a <c>Decimal128Type</c> with a negative precision.
    /// <para>
    /// <b>Spark does not survive this corner either</b> — measured, <c>round(1, -2147483648)</c>
    /// is a bare <c>ArithmeticException: Underflow</c> with no error class, so there is no
    /// behaviour to match, only a crash to avoid. The scales that ARE defined agree:
    /// <c>round(1, -100)</c> is 0 and <c>round(CAST(12.34 AS DECIMAL(10,2)), -39)</c> is 0.
    /// </para>
    /// <para>
    /// The floating case is not an extreme at all. A scale below about -324 underflows
    /// <c>Math.Pow</c>'s factor to zero, and the final division turned a rounded 0 into 0/0 —
    /// measured, Spark answers 0.0 for <c>round(1.5, -400)</c>, and -400 is an ordinary number.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("round(1, -2147483648)")]
    [InlineData("round(1, -2147483647)")]
    [InlineData("round(1, -100)")]
    [InlineData("round(9223372036854775807, -2147483648)")]
    public void AnExtremeNegativeScaleRoundsToZero(string sql)
    {
        var batch = Batch(("a", Ints(1)));

        Assert.Equal(0L, SparkArrays.ReadInt64(Eval(Ansi, sql, batch), 0));
        Assert.Equal(0L, SparkArrays.ReadInt64(Eval(Legacy, sql, batch), 0));
    }

    [Theory]
    [InlineData("round(CAST(12.34 AS DECIMAL(10,2)), -39)")]
    [InlineData("round(CAST(12.34 AS DECIMAL(10,2)), -2147483647)")]
    [InlineData("round(CAST(12.34 AS DECIMAL(10,2)), -2147483648)")]
    public void AnExtremeNegativeScaleOnADecimalStaysAValidType(string sql)
    {
        var batch = Batch(("a", Ints(1)));
        var result = Assert.IsType<Decimal128Array>(Eval(Ansi, sql, batch));
        var type = (Decimal128Type)result.Data.DataType;

        Assert.Equal((38, 0), (type.Precision, type.Scale));
        Assert.Equal("0", SparkWideDecimals.Render(SparkWideDecimals.Read(result, 0)!.Value));
    }

    [Fact]
    public void AScaleThatUnderflowsTheFactorRoundsToZeroRatherThanNaN()
    {
        var batch = Batch(("g", Doubles(1.5)));

        Assert.Equal(0d, Assert.IsType<DoubleArray>(Eval(Ansi, "round(g, -400)", batch)).GetValue(0));
        Assert.Equal(0d, Assert.IsType<DoubleArray>(Eval(Ansi, "round(g, -300)", batch)).GetValue(0));

        // ...and a scale far past what a double can hold is still the value itself.
        Assert.Equal(1.5, Assert.IsType<DoubleArray>(Eval(Ansi, "round(g, 400)", batch)).GetValue(0));
    }

    /// <summary>A decimal that will not fit raises under BOTH dialects, unlike the integral one.</summary>
    /// <remarks>
    /// The asymmetry is measured, not assumed, and Spark names it itself: the sub-class is
    /// <c>WITHOUT_SUGGESTION</c>, where the variant used everywhere else is the one whose message
    /// offers to turn ANSI off and return null instead.
    /// </remarks>
    [Fact]
    public void ADecimalRoundThatOverflowsRaisesUnderBothDialects()
    {
        var batch = Batch(("a", Ints(1)));
        const string Sql = "round(99999999999999999999999999999999999999, -1)";

        foreach (var registry in new[] { Ansi, Legacy })
        {
            var thrown = Assert.Throws<SparkEvaluationException>(() => Eval(registry, Sql, batch));
            Assert.Equal("NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION", thrown.ErrorClass);
        }
    }

    // ── CAST to and from BINARY, and the conditionals that needed it (#295) ─────────────────

    private static RecordBatch BinaryBatch(params byte[]?[] values)
    {
        var b = new BinaryArray.Builder();
        foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(v.AsSpan()); }
        return Batch(("bin", b.Build()));
    }

    /// <summary>The hex of the single cell of a binary result.</summary>
    private static string Hex(IArrowArray array) =>
        BitConverter.ToString(
            Assert.IsType<BinaryArray>(array).GetBytes(0).ToArray()).Replace("-", string.Empty);

    [Theory]
    // A string is a UTF-8 encode, in both dialects, and the empty string is bytes rather than null.
    [InlineData("CAST('a' AS BINARY)", "61")]
    [InlineData("CAST('abc' AS BINARY)", "616263")]
    [InlineData("CAST('' AS BINARY)", "")]
    // Two bytes for one char, which is what says the encode is UTF-8 and not Latin-1.
    [InlineData("CAST('é' AS BINARY)", "C3A9")]
    // Binary to binary is the identity.
    [InlineData("CAST(X'00FF' AS BINARY)", "00FF")]
    [InlineData("CAST(bin AS BINARY)", "0102")]
    public void CastsToBinaryUnderBothDialects(string sql, string hex)
    {
        var batch = BinaryBatch(new byte[] { 0x01, 0x02 });

        Assert.Equal(hex, Hex(Eval(Ansi, sql, batch)));
        Assert.Equal(hex, Hex(Eval(Legacy, sql, batch)));
    }

    [Fact]
    public void ANullStringCastsToANullBinary()
    {
        var batch = BinaryBatch(new byte[] { 0x00 });

        Assert.True(Eval(Ansi, "CAST(CAST(NULL AS STRING) AS BINARY)", batch).IsNull(0));
    }

    /// <summary>
    /// An integral casts to binary under the LEGACY dialect only, big-endian at its own width.
    /// </summary>
    /// <remarks>
    /// <b>The dialect split here is the part #295 did not name</b>, and it runs the opposite way
    /// to the conditional's: ANSI refuses this cast (<c>DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION</c>,
    /// whose whole content is "turn ANSI off") while legacy performs it, where for
    /// <c>coalesce(X'00', '2')</c> it is ANSI that resolves and legacy that refuses.
    /// <para>
    /// The WIDTH is the source type's and not the value's — 1 as a BIGINT is eight bytes and 1 as
    /// a TINYINT is one — and a negative value is its two's complement.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("CAST(CAST(1 AS TINYINT) AS BINARY)", "01")]
    [InlineData("CAST(CAST(1 AS SMALLINT) AS BINARY)", "0001")]
    [InlineData("CAST(CAST(1 AS INT) AS BINARY)", "00000001")]
    [InlineData("CAST(CAST(1 AS BIGINT) AS BINARY)", "0000000000000001")]
    [InlineData("CAST(CAST(-2 AS SMALLINT) AS BINARY)", "FFFE")]
    [InlineData("CAST(9223372036854775807 AS BINARY)", "7FFFFFFFFFFFFFFF")]
    public void AnIntegralCastsToBinaryOnlyUnderTheLegacyDialect(string sql, string hex)
    {
        var batch = BinaryBatch(new byte[] { 0x00 });

        Assert.Equal(hex, Hex(Eval(Legacy, sql, batch)));
        Assert.Throws<NotSupportedException>(() => Eval(Ansi, sql, batch));
    }

    [Fact]
    public void TryCastRefusesAnIntegralToBinaryUnderEitherDialect()
    {
        // try_cast type-checks the way ANSI does, so the legacy allowance above does not reach it
        // — measured, TRY_CAST(CAST(1 AS INT) AS BINARY) is CAST_WITHOUT_SUGGESTION with ansi off.
        // This is why CastToBinary keys on `legacy` rather than on `!raising`.
        var batch = BinaryBatch(new byte[] { 0x00 });

        Assert.Throws<NotSupportedException>(
            () => Eval(Legacy, "TRY_CAST(CAST(1 AS INT) AS BINARY)", batch));

        // ...while a string reaches try_cast perfectly well.
        Assert.Equal("61", Hex(Eval(Legacy, "TRY_CAST('a' AS BINARY)", batch)));
    }

    [Theory]
    [InlineData("CAST(CAST(1.5 AS DOUBLE) AS BINARY)")]
    [InlineData("CAST(CAST(1.5 AS FLOAT) AS BINARY)")]
    [InlineData("CAST(CAST(1.5 AS DECIMAL(10,2)) AS BINARY)")]
    [InlineData("CAST(true AS BINARY)")]
    [InlineData("CAST(DATE'2026-08-11' AS BINARY)")]
    public void NoOtherSourceTypeCastsToBinaryInEitherDialect(string sql)
    {
        var batch = BinaryBatch(new byte[] { 0x00 });

        Assert.Throws<NotSupportedException>(() => Eval(Ansi, sql, batch));
        Assert.Throws<NotSupportedException>(() => Eval(Legacy, sql, batch));
    }

    /// <summary>
    /// A binary/string conditional resolves to BINARY under ANSI and is refused under legacy.
    /// </summary>
    /// <remarks>
    /// The pair #295 was filed for. #278 measured this rule and had to decline the binary half,
    /// because naming the type would have promised a column neither <c>Cast</c> nor
    /// <c>SparkFunctions.Unify</c> could then build. Both exist now.
    /// <para>
    /// Either operand order resolves — <c>coalesce('2', X'00')</c> is binary too, holding the
    /// string's UTF-8 — which is what makes this a unification rather than "the first argument
    /// wins".
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("coalesce(X'00', '2')", "00")]
    [InlineData("coalesce('2', X'00')", "32")]
    [InlineData("coalesce(CAST(NULL AS BINARY), '2')", "32")]
    [InlineData("if(true, X'00', '2')", "00")]
    [InlineData("if(false, X'00', '2')", "32")]
    [InlineData("CASE WHEN false THEN X'00' ELSE '2' END", "32")]
    [InlineData("ifnull(X'00', '2')", "00")]
    [InlineData("nvl(X'00', '2')", "00")]
    public void AConditionalOverBinaryAndStringResolvesUnderAnsiOnly(string sql, string hex)
    {
        var batch = BinaryBatch(new byte[] { 0x00 });

        Assert.Equal(hex, Hex(Eval(Ansi, sql, batch)));
        Assert.Throws<NotSupportedException>(() => Eval(Legacy, sql, batch));
    }

    [Fact]
    public void AConditionalOverTwoBinariesNeedsNoStringRuleAndWorksInBothDialects()
    {
        // Not a coercion question at all, and broken for the same reason: `Unify` had no binary
        // branch, so even `coalesce(X'00', X'01')` — where both branches already agree — could
        // not build its result.
        var batch = BinaryBatch(new byte[] { 0x00 });

        foreach (var registry in new[] { Ansi, Legacy })
        {
            Assert.Equal("00", Hex(Eval(registry, "coalesce(X'00', X'01')", batch)));
            Assert.Equal("01", Hex(Eval(registry, "coalesce(CAST(NULL AS BINARY), X'01')", batch)));
            Assert.Equal("0102", Hex(Eval(registry, "coalesce(bin, X'01')", BinaryBatch(new byte[] { 0x01, 0x02 }))));
        }

        // ...and binary still does not absorb everything: these are refused in both dialects.
        Assert.Throws<NotSupportedException>(() => Eval(Ansi, "coalesce(X'00', 1)", batch));
        Assert.Throws<NotSupportedException>(() => Eval(Ansi, "coalesce(X'00', true)", batch));
    }

    /// <summary>
    /// greatest/least order a binary pair as UNSIGNED bytes, with a prefix sorting first.
    /// </summary>
    /// <remarks>
    /// Measured, and a signed reading gets the first two backwards: Java's <c>byte</c> is signed,
    /// so a careless port would call <c>X'FF'</c> less than <c>X'00'</c>. Spark answers
    /// <c>greatest(X'00', X'FF')</c> = <c>FF</c> and <c>greatest(X'7F', X'80')</c> = <c>80</c>.
    /// <para>
    /// They also keep #278's split: a binary against a STRING is refused by both dialects, where
    /// the conditional family above coerces it.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("greatest(X'00', X'01')", "01")]
    [InlineData("least(X'00', X'01')", "00")]
    [InlineData("greatest(X'00', X'FF')", "FF")]
    [InlineData("greatest(X'7F', X'80')", "80")]
    [InlineData("greatest(X'01', X'0100')", "0100")]
    [InlineData("least(X'01', X'0100')", "01")]
    public void GreatestAndLeastOrderBinaryAsUnsignedBytes(string sql, string hex)
    {
        var batch = BinaryBatch(new byte[] { 0x00 });

        Assert.Equal(hex, Hex(Eval(Ansi, sql, batch)));
        Assert.Equal(hex, Hex(Eval(Legacy, sql, batch)));
    }

    [Fact]
    public void GreatestRefusesABinaryAgainstAStringInBothDialects()
    {
        var batch = BinaryBatch(new byte[] { 0x00 });

        Assert.Throws<NotSupportedException>(() => Eval(Ansi, "greatest(X'00', '2')", batch));
        Assert.Throws<NotSupportedException>(() => Eval(Legacy, "least(X'00', '2')", batch));
    }

    /// <summary>
    /// nullif compares two binaries by BYTES, and a binary against a string as TEXT.
    /// </summary>
    /// <remarks>
    /// The two routes give different answers and both are Spark's. <c>X'FF'</c> and <c>X'FE'</c>
    /// decode to the same U+FFFD, and against another BINARY they are still unequal — but
    /// <c>nullif(X'FF', CAST(X'FF' AS STRING))</c> is NULL, because a binary meeting a STRING is
    /// the pair where the BINARY is rendered as text (#262) rather than the string encoded.
    /// So the byte branch has to sit after the string one, not before it.
    /// </remarks>
    [Fact]
    public void NullIfComparesBinaryByBytesAndAStringByText()
    {
        var batch = BinaryBatch(new byte[] { 0x00 });

        Assert.Equal("00", Hex(Eval(Ansi, "nullif(X'00', X'01')", batch)));
        Assert.True(Eval(Ansi, "nullif(X'00', X'00')", batch).IsNull(0));

        // Both decode to U+FFFD; as bytes they differ, and Spark says they differ.
        Assert.Equal("FF", Hex(Eval(Ansi, "nullif(X'FF', X'FE')", batch)));

        // ...and the string route, which calls the same two bytes equal because both render as
        // U+FFFD. Measured: NULL.
        Assert.True(Eval(Ansi, "nullif(X'FF', CAST(X'FF' AS STRING))", batch).IsNull(0));

        // nullif takes the FIRST argument's type, so this one answers binary in BOTH dialects
        // even though `coalesce` over the same pair is refused under legacy.
        Assert.Equal("00", Hex(Eval(Legacy, "nullif(X'00', '2')", batch)));
    }
}
