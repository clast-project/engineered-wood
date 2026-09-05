// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Reading a decimal out of a string, across the whole of Spark's precision range.
/// </summary>
/// <remarks>
/// The corpus already pins the answers that came from Spark — see the <c>string-to-decimal</c>
/// group, which <see cref="SparkEvaluationCorpusTests"/> asserts every one of. What is here
/// instead is the surrounding shape those answers imply but do not each demonstrate: the forms
/// the parse must refuse, the leading zeros and absurd exponents no sane expression contains, and
/// the sign handling, which on netstandard2.0 goes through a two's complement written by hand
/// because <see cref="Int128"/> there is database-decimal's polyfill rather than the BCL type.
/// <para>
/// Values are asserted as the UNSCALED integer, which is what the cast actually produces and what
/// a rendering at the target scale would hide behind formatting.
/// </para>
/// </remarks>
public sealed class SparkDecimalTextTests
{
    /// <summary>The unscaled integer <paramref name="text"/> reads as, at the given target type.</summary>
    private static string Unscaled(string text, int precision = 38, int scale = 0)
    {
        var outcome = SparkDecimalText.TryRead(text, new Decimal128Type(precision, scale), out var value);
        Assert.Equal(SparkDecimalText.Result.Ok, outcome);

        // Rendered at scale 0 so the assertion reads as the mantissa itself.
        return SparkWideDecimals.Render(new SparkWideDecimals.Operand(value, 38, 0));
    }

    private static SparkDecimalText.Result Outcome(string text, int precision = 38, int scale = 0) =>
        SparkDecimalText.TryRead(text, new Decimal128Type(precision, scale), out _);

    [Theory]
    // Past System.Decimal's ~7.9e28 ceiling, which is the whole point, and both signs — the
    // negative one is the only route through the hand-written two's complement.
    [InlineData("123456789012345678901234567890", "123456789012345678901234567890")]
    [InlineData("-123456789012345678901234567890", "-123456789012345678901234567890")]
    [InlineData("99999999999999999999999999999999999999", "99999999999999999999999999999999999999")]
    // Exponent notation, which is a form the value can only be spelled in.
    [InlineData("1e30", "1000000000000000000000000000000")]
    [InlineData("1E30", "1000000000000000000000000000000")]
    [InlineData("1.5e3", "1500")]
    [InlineData("15e-1", "2")]
    // The forms around a number that Spark accepts. Measured for each of these except the tab,
    // which is here because Spark trims by "at or below the space" rather than by kind.
    [InlineData(" 42 ", "42")]
    [InlineData("\t42\n", "42")]
    [InlineData("+42", "42")]
    [InlineData("42.", "42")]
    [InlineData("-0", "0")]
    // Leading zeros are not digits for the too-many-digits rule: 43 characters, one digit.
    [InlineData("0000000000000000000000000000000000000000042", "42")]
    [InlineData("0", "0")]
    [InlineData("0.000", "0")]
    public void ReadsAValueExactly(string text, string expected) =>
        Assert.Equal(expected, Unscaled(text));

    [Theory]
    // Half AWAY FROM ZERO, matching the rest of this path. Half-even would answer 2, -2 and 14
    // to the first three.
    [InlineData("2.5", 0, "3")]
    [InlineData("-2.5", 0, "-3")]
    [InlineData("1.45", 1, "15")]
    [InlineData("-1.45", 1, "-15")]
    [InlineData("3.5", 0, "4")]
    // The quotient alone is zero here and carries no sign, so the sign has to come from the
    // mantissa. Getting that wrong gives 0 and 1 instead of 1 and -1.
    [InlineData("0.5", 0, "1")]
    [InlineData("-0.5", 0, "-1")]
    [InlineData("0.4", 0, "0")]
    [InlineData("-0.4", 0, "0")]
    // Only the first discarded digit decides a half-up rounding, so a long tail below it changes
    // nothing either way.
    [InlineData("0.4999999999999999999999999999999999999999", 0, "0")]
    [InlineData("0.5000000000000000000000000000000000000001", 0, "1")]
    // Trailing zeros are part of the unscaled value, not of the rounding.
    [InlineData("1.00", 2, "100")]
    // Digits well past System.Decimal's 28-29 significant ones, which it would have rounded away
    // while reporting success.
    [InlineData("1.0000000000000000000000000000001", 31, "10000000000000000000000000000001")]
    public void RoundsHalfAwayFromZero(string text, int scale, string expected) =>
        Assert.Equal(expected, Unscaled(text, 38, scale));

    [Fact]
    public void AnExponentFarBelowTheScaleRoundsToZeroWithoutFormingTheDivisor()
    {
        // A billion-place shift would be a 10^1000000000 divisor if it were formed. Nothing at
        // that depth can reach the target's last place, including the digit that would decide the
        // rounding, so the answer is zero and the divisor is never built.
        Assert.Equal("0", Unscaled("1e-1000000000", 38, 10));
        Assert.Equal("0", Unscaled("-1e-1000000000", 38, 10));
    }

    [Theory]
    // More integral digits than ANY Spark decimal has. A property of the string, not of the
    // target — the 30-digit case below reaches a different class against a narrower target.
    [InlineData("999999999999999999999999999999999999999")]
    [InlineData("-999999999999999999999999999999999999999")]
    [InlineData("1e39")]
    // Zero counts as one digit, because that is a BigDecimal's precision for it — so a zero with
    // a large positive exponent trips the rule while one with a large negative exponent does not.
    [InlineData("0E40")]
    public void RefusesMoreThanThirtyEightIntegralDigits(string text) =>
        Assert.Equal(SparkDecimalText.Result.TooManyDigits, Outcome(text));

    [Fact]
    public void ThirtyEightIntegralDigitsIsAccepted()
    {
        // The boundary, on the accepting side: the rule is "more than 38", not "38 or more".
        Assert.Equal(SparkDecimalText.Result.Ok, Outcome("99999999999999999999999999999999999999"));

        // ...and the same digits with a fraction, which has 39 digits in total but 38 integral
        // ones. It is refused, but for fitting the TARGET rather than for its length.
        Assert.Equal(
            SparkDecimalText.Result.OutOfRange, Outcome("99999999999999999999999999999999999999.5"));
    }

    [Theory]
    // A number Spark reads, but not one the target holds.
    [InlineData("12345", 3, 0)]
    [InlineData("-12345", 3, 0)]
    [InlineData("123456789012345678901234567890", 10, 0)]
    // Rounding CARRIES past the precision: the value has 38 integral digits, its rounded form has
    // 39.
    [InlineData("99999999999999999999999999999999999999.5", 38, 0)]
    // The scale leaves no room for the integral digits it already has.
    [InlineData("123", 38, 36)]
    public void RefusesAValueTheTargetCannotHold(string text, int precision, int scale) =>
        Assert.Equal(SparkDecimalText.Result.OutOfRange, Outcome(text, precision, scale));

    [Theory]
    [InlineData("abc")]
    [InlineData("")]
    [InlineData("   ")]
    // Java's BigDecimal grammar, which is what Spark hands the string to — not .NET's
    // NumberStyles.Float, which differs on every line below.
    [InlineData("1,000")]
    [InlineData("NaN")]
    [InlineData("Infinity")]
    [InlineData("-Infinity")]
    // .NET trims Unicode whitespace; Spark trims only characters at or below the space, so a
    // non-breaking space is part of the number and makes it malformed.
    [InlineData("\u00A042")]
    [InlineData("42\u00A0")]
    [InlineData(".")]
    [InlineData("+")]
    [InlineData("-")]
    [InlineData("1.2.3")]
    [InlineData("--1")]
    [InlineData("1e")]
    [InlineData("1e+")]
    [InlineData("1e2.5")]
    [InlineData("1d")]
    [InlineData("1 000")]
    [InlineData("0x1f")]
    // An exponent past int, which Java refuses rather than saturating.
    [InlineData("1e99999999999999")]
    [InlineData("1e-99999999999999")]
    public void RefusesWhatSparkCallsMalformed(string text) =>
        Assert.Equal(SparkDecimalText.Result.Malformed, Outcome(text));

    [Theory]
    // A leading point and a trailing point are both accepted, which NumberStyles.Float also does
    // — recorded because the refusal list above is only meaningful next to what is allowed.
    [InlineData(".5", 1, "5")]
    [InlineData("-.5", 1, "-5")]
    [InlineData("+.5", 1, "5")]
    public void AcceptsAPointWithoutDigitsOnOneSide(string text, int scale, string expected) =>
        Assert.Equal(expected, Unscaled(text, 38, scale));

    // ── Through the evaluator, which is where the error classes become visible ────────────────

    private static readonly SparkFunctionRegistry Ansi = new();

    private static readonly SparkFunctionRegistry Legacy = new(new SparkDialectOptions { Ansi = false });

    private static IArrowArray Evaluate(SparkFunctionRegistry registry, string expression, string? value)
    {
        var strings = new StringArray.Builder();
        if (value is null) strings.AppendNull();
        else strings.Append(value);

        var array = strings.Build();
        var batch = new RecordBatch(
            new Schema.Builder().Field(new Field("s", StringType.Default, true)).Build(),
            new IArrowArray[] { array },
            1);

        return new ArrowRowEvaluator(registry)
            .EvaluateExpression(SparkSqlParser.ParseExpression(expression), batch);
    }

    [Theory]
    // Three failures, three classes. The middle one is reached by no other path in the library,
    // and is the one hand-reasoning would have folded into the third.
    [InlineData("abc", "CAST_INVALID_INPUT")]
    [InlineData("999999999999999999999999999999999999999", "NUMERIC_OUT_OF_SUPPORTED_RANGE")]
    [InlineData("123456789012345678901234567890", "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION")]
    public void TheErrorClassNamesTheConditionSparkNames(string value, string errorClass)
    {
        // The third value fits DECIMAL(38,0) and only overflows this narrower target, which is
        // what separates it from the second.
        var thrown = Assert.Throws<SparkEvaluationException>(
            () => Evaluate(Ansi, "CAST(s AS DECIMAL(10,0))", value));

        Assert.Equal(errorClass, thrown.ErrorClass);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("999999999999999999999999999999999999999")]
    [InlineData("123456789012345678901234567890")]
    public void TheLegacyDialectNullsWhereAnsiRaises(string value)
    {
        // Measured under a second harvest with ansi off: all three yield null rather than an
        // error, and unlike an INTEGRAL cast none of them wraps. See the `legacy` section of the
        // corpus, and #243 for the integral case that does not follow this pattern.
        var result = Evaluate(Legacy, "CAST(s AS DECIMAL(10,0))", value);

        Assert.True(result.IsNull(0));
    }

    [Fact]
    public void ANullStringStaysNullUnderBothDialects()
    {
        Assert.True(Evaluate(Ansi, "CAST(s AS DECIMAL(38,0))", null).IsNull(0));
        Assert.True(Evaluate(Legacy, "CAST(s AS DECIMAL(38,0))", null).IsNull(0));
    }

    [Fact]
    public void TryCastNeverRaises()
    {
        // try_cast is the non-raising path under EITHER dialect, so the ANSI registry is the
        // interesting one to ask.
        Assert.True(Evaluate(Ansi, "TRY_CAST(s AS DECIMAL(38,0))", "abc").IsNull(0));

        var wide = (Decimal128Array)Evaluate(
            Ansi, "TRY_CAST(s AS DECIMAL(38,0))", "123456789012345678901234567890");

        Assert.Equal(
            "123456789012345678901234567890",
            SparkWideDecimals.Render(SparkWideDecimals.Read(wide, 0)!.Value));
    }
}
