// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Arithmetic over a string operand, which Spark resolves by casting the string.
/// </summary>
/// <remarks>
/// #296, and the arithmetic half of what <see cref="StringComparisonCoercionTests"/> covers for
/// comparison. The corpus pins these against Spark's own answers — the
/// <c>arithmetic-string-coercion</c> group, in both dialects — and this file states the rules
/// directly, because the corpus compares VALUES and the sharpest half of the rule is a TYPE:
/// <c>'1' + 1</c> is 2 either way and is a <c>bigint</c> under ANSI and a <c>double</c> without
/// it.
/// </remarks>
public class ArithmeticStringCoercionTests
{
    private static readonly SparkFunctionRegistry Ansi =
        new(new SparkDialectOptions { Ansi = true });

    private static readonly SparkFunctionRegistry Legacy =
        new(new SparkDialectOptions { Ansi = false });

    /// <summary>One row of every type an arithmetic string coercion can meet.</summary>
    private static RecordBatch Row()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, true))
            .Field(new Field("sh", Int16Type.Default, true))
            .Field(new Field("f", FloatType.Default, true))
            .Field(new Field("g", DoubleType.Default, true))
            .Field(new Field("d", new Decimal128Type(38, 0), true))
            .Field(new Field("s", StringType.Default, true))
            .Field(new Field("ns", StringType.Default, true))
            .Field(new Field("bl", BooleanType.Default, true))
            .Build();

        return new RecordBatch(schema, new IArrowArray[]
        {
            new Int32Array.Builder().Append(1).Build(),
            new Int16Array.Builder().Append(2).Build(),
            new FloatArray.Builder().Append(0.5f).Build(),
            new DoubleArray.Builder().Append(2.5).Build(),
            Decimal(new Decimal128Type(38, 0), BigInteger.Pow(10, 30)),
            new StringArray.Builder().Append("abc").Build(),
            new StringArray.Builder().Append("1").Build(),
            new BooleanArray.Builder().Append(true).Build(),
        }, 1);
    }

    private static Decimal128Array Decimal(Decimal128Type type, BigInteger unscaled)
    {
        var bytes = new byte[16];
        var raw = unscaled.ToByteArray();
        System.Array.Copy(raw, bytes, raw.Length);

        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true);
        return new Decimal128Array(new ArrayData(
            type, 1, 0, 0, new[] { validity.Build(), new ArrowBuffer(bytes) }));
    }

    private static IArrowArray Evaluate(SparkFunctionRegistry registry, string expression) =>
        new ArrowRowEvaluator(registry)
            .EvaluateExpression(SparkSqlParser.ParseExpression(expression), Row());

    /// <summary>The first row's value, or null, whatever numeric array it arrived in.</summary>
    private static double? Value(IArrowArray array) => array switch
    {
        Int64Array longs => longs.GetValue(0),
        DoubleArray doubles => doubles.GetValue(0),
        FloatArray floats => floats.GetValue(0),
        _ => throw new NotSupportedException($"unexpected result array {array.GetType().Name}"),
    };

    // ── The defect itself: every one of these threw before #296 ──

    [Theory]
    [InlineData("'1' + 1", 2.0)]
    [InlineData("1 + '1'", 2.0)]           // either operand order
    [InlineData("ns + a", 2.0)]            // either operand may be the column
    [InlineData("a + ns", 2.0)]
    [InlineData("'1' - 1", 0.0)]
    [InlineData("'2' * 3", 6.0)]
    [InlineData("'7' % 3", 1.0)]
    [InlineData("'6' / 2", 3.0)]
    [InlineData("' 1 ' + 1", 2.0)]         // the cast trims
    [InlineData("-'1'", -1.0)]
    [InlineData("'1' + g", 3.5)]
    public void AValidNumericStringIsArithmeticUnderBothDialects(string expression, double expected)
    {
        // Before #296 `SparkNumericTypes.ArithmeticResult` had no string branch at all, so each
        // of these reached one of its three "not a number" throws. The issue named only
        // `IntegralRank`'s; the decimal and floating-point paths refused with their own messages.
        Assert.Equal(expected, Value(Evaluate(Ansi, expression)));
        Assert.Equal(expected, Value(Evaluate(Legacy, expression)));
    }

    // ── The dialects choose DIFFERENT targets, and the type is where it shows ──

    [Fact]
    public void AnsiReadsAStringAsTheOtherOperandsFamilyAndLegacyAlwaysAsADouble()
    {
        // The values agree, which is why this is asserted on the TYPE: an integral operand makes
        // the string a bigint under ANSI and a double without it.
        Assert.IsType<Int64Array>(Evaluate(Ansi, "'1' + 1"));
        Assert.IsType<DoubleArray>(Evaluate(Legacy, "'1' + 1"));

        // The ANSI target is BIGINT at every integral width, not the other operand's own width.
        // Measured: `'32768' + CAST(0 AS SMALLINT)` is 32768 rather than an overflow.
        Assert.Equal(32770.0, Value(Evaluate(Ansi, "'32768' + sh")));
        Assert.Equal(32770.0, Value(Evaluate(Legacy, "'32768' + sh")));

        // Anything else numeric is a double in both, so only the integral family separates them.
        Assert.IsType<DoubleArray>(Evaluate(Ansi, "'1' + g"));
        Assert.IsType<DoubleArray>(Evaluate(Ansi, "'1' + f"));
    }

    [Fact]
    public void ADecimalOperandTakesTheStringToDoubleUnderBothDialects()
    {
        // The discriminator, and the place arithmetic parts company with COMPARISON: a comparison
        // under the legacy dialect casts the string to the decimal's own type and keeps it exact,
        // where arithmetic sends it to double in either dialect. An exact addition would come
        // back as a Decimal128Array still carrying the 31st digit; this comes back as a double
        // that lost it on the way in.
        const string sum = "'1000000000000000000000000000001' + CAST(0 AS DECIMAL(38,0))";

        Assert.IsType<DoubleArray>(Evaluate(Ansi, sum));
        Assert.Equal(1e30, Value(Evaluate(Ansi, sum)));
        Assert.IsType<DoubleArray>(Evaluate(Legacy, sum));
        Assert.Equal(1e30, Value(Evaluate(Legacy, sum)));
    }

    [Fact]
    public void AFloatOperandTakesTheStringToDoubleUnderBothDialects()
    {
        // The float discriminator, same shape: 0.5f + the double 0.1 is 0.6 exactly as doubles,
        // where a float addition would round to 0.60000002384185791.
        Assert.Equal(0.6, Value(Evaluate(Ansi, "'0.1' + f")));
        Assert.Equal(0.6, Value(Evaluate(Legacy, "'0.1' + f")));
    }

    // ── The target decides which strings are ACCEPTED, not only the result type ──

    [Theory]
    [InlineData("'1.5' + 1", 2.5)]
    [InlineData("'1e3' + 1", 1001.0)]
    [InlineData("'1.5' / 3", 0.5)]
    [InlineData("'1.5' % 2", 1.5)]
    public void AnsiRefusesAFractionalStringAgainstAnIntegralWhereLegacyReadsItAsADouble(
        string expression, double legacyValue)
    {
        // ANSI's integral target inherits #258's integral TEXT rule, under which a decimal point
        // and an exponent are both refusals. So this is not a choice of result type only.
        var thrown = Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, expression));
        Assert.Equal("CAST_INVALID_INPUT", thrown.ErrorClass);

        Assert.Equal(legacyValue, Value(Evaluate(Legacy, expression)));
    }

    [Fact]
    public void DivisionTakesTheSameTargetAsEveryOtherOperatorDespiteItsDoubleResult()
    {
        // THE ROW THAT SEPARATES THE TWO READINGS OF `/`. Its result is a double whatever the
        // operands are, which makes "the string goes to double" look right -- and the string is
        // still read as a BIGINT against an integral, one expression apart:
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "'1.5' / 3"));
        Assert.Equal(0.6, Value(Evaluate(Ansi, "'1.5' / g")));
    }

    [Theory]
    [InlineData("s + a")]
    [InlineData("s * 2")]
    [InlineData("'abc' + 1")]
    [InlineData("'' + 1")]
    [InlineData("-s")]
    public void AnsiRefusesAStringTheTargetCastRefusesAndLegacyNullsIt(string expression)
    {
        var thrown = Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, expression));
        Assert.Equal("CAST_INVALID_INPUT", thrown.ErrorClass);

        Assert.Null(Value(Evaluate(Legacy, expression)));
    }

    // ── Pairs with no rule at all ──

    [Theory]
    [InlineData("'1' + '2'")]
    [InlineData("'1' - '2'")]
    [InlineData("'1' * '2'")]
    [InlineData("'1' / '2'")]
    [InlineData("'1' % '2'")]
    [InlineData("ns + ns")]
    public void TwoStringsAreNotAnArithmeticPairUnderAnsiAndAreDoublesWithoutIt(string expression)
    {
        // Spark refuses with DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE, which is an analysis error
        // rather than a per-row one; a throw is how this registry spells a refusal.
        Assert.Throws<NotSupportedException>(() => Evaluate(Ansi, expression));
        Assert.NotNull(Value(Evaluate(Legacy, expression)));
    }

    [Fact]
    public void AStringAgainstABareNullRefusesUnderAnsiAndIsADoubleNullWithoutIt()
    {
        // It is the absence of a TYPE that refuses and not the nullness: a typed null one
        // expression away is a perfectly good bigint null under the same dialect.
        Assert.Throws<NotSupportedException>(() => Evaluate(Ansi, "'1' + NULL"));
        Assert.Throws<NotSupportedException>(() => Evaluate(Ansi, "NULL + '1'"));

        var typed = Evaluate(Ansi, "'1' + CAST(NULL AS INT)");
        Assert.IsType<Int64Array>(typed);
        Assert.Null(Value(typed));

        Assert.IsType<DoubleArray>(Evaluate(Legacy, "'1' + NULL"));
        Assert.Null(Value(Evaluate(Legacy, "'1' + NULL")));
    }

    [Theory]
    [InlineData("'1' + bl")]
    [InlineData("bl + '1'")]
    public void APairSparkRefusesInBothDialectsIsRefusedInBoth(string expression)
    {
        Assert.Throws<NotSupportedException>(() => Evaluate(Ansi, expression));
        Assert.Throws<NotSupportedException>(() => Evaluate(Legacy, expression));
    }

    // ── Unary minus, the one rule the two dialects share ──

    [Theory]
    [InlineData("-'1'", -1.0)]
    [InlineData("-'1.5'", -1.5)]
    [InlineData("-'1e3'", -1000.0)]     // a double target, so an exponent is a number here
    [InlineData("-'  1  '", -1.0)]
    [InlineData("-ns", -1.0)]
    [InlineData("- -'1'", 1.0)]
    public void UnaryMinusReadsAStringAsADoubleInBothDialects(string expression, double expected)
    {
        Assert.IsType<DoubleArray>(Evaluate(Ansi, expression));
        Assert.Equal(expected, Value(Evaluate(Ansi, expression)));
        Assert.Equal(expected, Value(Evaluate(Legacy, expression)));
    }

    [Fact]
    public void UnaryMinusDisagreesWithTheBinaryOperatorsUnderAnsi()
    {
        // One exponent, two answers, one dialect: the unary target is a double and the binary one
        // against an integral is a bigint, which #258's integral text rule refuses.
        Assert.Equal(-1000.0, Value(Evaluate(Ansi, "-'1e3'")));
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "'1e3' + 1"));
    }

    [Fact]
    public void NegatingAStringZeroKeepsTheSign()
    {
        // The string really reached a DOUBLE rather than a parse that dropped the sign: #282's
        // negative zero survives, and 1 / -0.0 is what says so.
        Assert.Equal(double.NegativeInfinity, 1.0 / Value(Evaluate(Ansi, "-'0'"))!.Value);
        Assert.Equal(double.NegativeInfinity, 1.0 / Value(Evaluate(Legacy, "-'0'"))!.Value);
    }
}
