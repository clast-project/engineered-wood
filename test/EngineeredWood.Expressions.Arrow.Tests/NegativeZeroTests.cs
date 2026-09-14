// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The sign of a zero — #282.
/// </summary>
/// <remarks>
/// What Spark answers is pinned by the corpus's <c>negative-zero</c> group, which is the gate.
/// This asserts the same rules on the BITS rather than on a rendering, which is the form the
/// three defects actually had, and it says which of the three each row is about:
/// <list type="number">
/// <item><description>unary minus MAKES a negative zero, where subtracting from zero cannot;</description></item>
/// <item><description><c>round</c> DESTROYS one, because Spark rounds through BigDecimal;</description></item>
/// <item><description>a parse from TEXT carries the sign, which .NET Framework's number parser
/// drops — so this file is only meaningful when it is run on net472 as well.</description></item>
/// </list>
/// <para>
/// Every assertion goes through <see cref="IsNegativeZero"/> rather than <c>Assert.Equal</c>,
/// which would pass on every row here: <c>-0.0 == 0.0</c> and <c>(-0.0).Equals(0.0)</c> are both
/// true in .NET, which is exactly why the corpus's value channel could not see any of this until
/// <c>CorpusEvaluation.SameDouble</c> started comparing by the bits.
/// </para>
/// </remarks>
public sealed class NegativeZeroTests
{
    private static readonly SparkFunctionRegistry Ansi = new();

    /// <summary>By the sign bit; <c>&lt; 0</c> is false for -0.0 and <c>IsNegative</c> is not on netstandard2.0.</summary>
    private static bool IsNegativeZero(double value) =>
        value == 0d && BitConverter.DoubleToInt64Bits(value) < 0;

    private static bool IsPositiveZero(double value) =>
        value == 0d && BitConverter.DoubleToInt64Bits(value) >= 0;

    private static RecordBatch Batch()
    {
        var doubles = new DoubleArray.Builder();
        doubles.Append(2.5);
        var floats = new FloatArray.Builder();
        floats.Append(1.5f);
        var strings = new StringArray.Builder();
        strings.Append("x");

        var schema = new Schema.Builder()
            .Field(new Field("g", DoubleType.Default, true))
            .Field(new Field("f", FloatType.Default, true))
            .Field(new Field("s", StringType.Default, true))
            .Build();

        return new RecordBatch(
            schema,
            new IArrowArray[] { doubles.Build(), floats.Build(), strings.Build() },
            1);
    }

    private static double Eval(string sql)
    {
        var array = new ArrowRowEvaluator(Ansi)
            .EvaluateExpression(SparkSqlParser.ParseExpression(sql), Batch());

        return array switch
        {
            DoubleArray a => a.GetValue(0)!.Value,
            FloatArray a => a.GetValue(0)!.Value,
            _ => throw new InvalidOperationException($"{sql} produced {array.Data.DataType.Name}"),
        };
    }

    /// <summary>
    /// The assumption every fix in this group rests on: C#'s unary minus is a sign-bit flip, so
    /// <c>-0d</c> really is the other zero.
    /// </summary>
    /// <remarks>
    /// Asserted rather than assumed because a reader cannot tell <c>-0d</c> from <c>0d</c> by
    /// looking, and a compiler that folded it to a positive zero would make three fixes silently
    /// no-ops whose corpus rows would then fail with no hint of the cause.
    /// </remarks>
    [Fact]
    public void NegativeZeroLiteralIsNegative()
    {
        Assert.True(IsNegativeZero(-0d));
        Assert.True(IsPositiveZero(0d));
        Assert.True(IsPositiveZero(0d - 0d));
    }

    // ── 1. Unary minus MAKES one ───────────────────────────────────────────────────────────

    [Theory]
    [InlineData("negative(CAST(0.0 AS DOUBLE))")]
    [InlineData("-CAST(0.0 AS DOUBLE)")]
    [InlineData("-0.0D")]
    [InlineData("negative(CAST(0.0 AS FLOAT))")]
    // ...over a zero no constant folding produced, so the rule is about evaluation.
    [InlineData("negative(g - g)")]
    [InlineData("negative(f - f)")]
    // ...and reached through arithmetic, which IEEE already got right and must keep.
    [InlineData("CAST(0.0 AS DOUBLE) * CAST(-1.0 AS DOUBLE)")]
    [InlineData("negative(CAST(0.0 AS DOUBLE)) + negative(CAST(0.0 AS DOUBLE))")]
    public void UnaryMinusProducesNegativeZero(string sql) => Assert.True(IsNegativeZero(Eval(sql)));

    /// <summary>
    /// The control. Subtracting from zero agrees with negation at every double except this one,
    /// which is why reusing it for unary minus was wrong in exactly one place.
    /// </summary>
    [Theory]
    [InlineData("CAST(0.0 AS DOUBLE) - CAST(0.0 AS DOUBLE)")]
    [InlineData("0.0D")]
    [InlineData("negative(negative(CAST(0.0 AS DOUBLE)))")]
    [InlineData("negative(CAST(0.0 AS DOUBLE)) + CAST(0.0 AS DOUBLE)")]
    // A decimal literal is what `-0.0` actually is in Spark, and a decimal has no signed zero.
    [InlineData("CAST(-0.0 AS DOUBLE)")]
    public void SubtractionFromZeroDoesNotProduceOne(string sql) =>
        Assert.True(IsPositiveZero(Eval(sql)));

    /// <summary>Unary minus over an integral still SUBTRACTS, which is what makes it raise.</summary>
    /// <remarks>
    /// The range check lives on the subtraction, so negating in place there would have turned
    /// <c>-(-2147483648)</c> back into itself instead of raising. #285's group covers the value;
    /// this covers the reason the floating-point change stopped where it did.
    /// </remarks>
    [Fact]
    public void NegatingAnIntegralMinimumStillRaises() =>
        Assert.Throws<SparkEvaluationException>(
            () => new ArrowRowEvaluator(Ansi).EvaluateExpression(
                SparkSqlParser.ParseExpression("negative(CAST(-2147483648 AS INT))"), Batch()));

    // ── 2. round DESTROYS one ──────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("round(CAST(-0.4 AS DOUBLE))")]
    [InlineData("round(CAST(-0.04 AS DOUBLE), 1)")]
    [InlineData("round(CAST(-0.0001 AS DOUBLE), 2)")]
    [InlineData("round(CAST(-0.4 AS DOUBLE), -1)")]
    [InlineData("round(CAST(-0.4 AS FLOAT))")]
    // ...and over a negative zero itself. These are the rows that say the first two rules have to
    // be fixed TOGETHER: each agreed before #282 only because the negation had already lost the
    // sign, so closing that half alone would have opened these.
    [InlineData("round(negative(CAST(0.0 AS DOUBLE)), 0)")]
    [InlineData("round(negative(CAST(0.0 AS DOUBLE)), 1)")]
    [InlineData("round(negative(CAST(0.0 AS FLOAT)), 2)")]
    [InlineData("round(negative(g - g), 2)")]
    public void RoundingToZeroProducesAPositiveZero(string sql) =>
        Assert.True(IsPositiveZero(Eval(sql)));

    /// <summary>
    /// A zero rounds to a positive zero at EVERY scale, including the ones whose power of ten is
    /// not a double.
    /// </summary>
    /// <remarks>
    /// Raised in review of #282. Above about 308 the scaling factor overflows to infinity, and
    /// `0 * infinity` is NaN -- so a zero came back NaN, a value out of nowhere for an input with
    /// a perfectly ordinary answer. That is the same failure #285 fixed at the OTHER end, where a
    /// scale below -324 underflows the factor to zero; only that end had been measured, and this
    /// one was wrong before #282 as well, for a positive zero as much as a negative one. Spark
    /// answers 0.0 at every scale an Int can hold.
    /// </remarks>
    [Theory]
    [InlineData("round(CAST(0.0 AS DOUBLE), 400)")]
    [InlineData("round(negative(CAST(0.0 AS DOUBLE)), 400)")]
    [InlineData("round(g - g, 400)")]
    [InlineData("round(negative(g - g), 400)")]
    [InlineData("round(CAST(0.0 AS FLOAT), 400)")]
    // The boundary: 10^308 is a double and 10^309 is not.
    [InlineData("round(CAST(0.0 AS DOUBLE), 308)")]
    [InlineData("round(CAST(0.0 AS DOUBLE), 309)")]
    [InlineData("round(negative(CAST(0.0 AS DOUBLE)), 309)")]
    // ...and the other end, which #285 covered for a value and not for a zero.
    [InlineData("round(CAST(0.0 AS DOUBLE), -400)")]
    [InlineData("round(negative(CAST(0.0 AS DOUBLE)), -400)")]
    public void RoundingAZeroAtAnyScaleProducesAPositiveZero(string sql) =>
        Assert.True(IsPositiveZero(Eval(sql)));

    /// <summary>The control: a NON-zero value at the same scales is untouched.</summary>
    /// <remarks>
    /// These are what say the short-circuit is about the zero rather than about the scale. The
    /// scaled value overflows to an infinity for them, which the range guard already caught --
    /// only a zero reached the NaN, because only `0 * infinity` is one.
    /// </remarks>
    [Theory]
    [InlineData("round(CAST(-1.5 AS DOUBLE), 400)", -1.5d)]
    [InlineData("round(g, 400)", 2.5d)]
    [InlineData("round(CAST(2.5 AS DOUBLE), 309)", 2.5d)]
    // A huge NEGATIVE scale rounds a value away to zero, and to the positive one. #285.
    [InlineData("round(CAST(-1.5 AS DOUBLE), -400)", 0d)]
    public void RoundingAValueAtAnExtremeScaleIsUntouched(string sql, double expected)
    {
        var actual = Eval(sql);
        Assert.Equal(expected, actual);
        Assert.False(double.IsNaN(actual));
    }

    /// <summary>A rounded value that does not land on zero keeps its sign.</summary>
    [Theory]
    [InlineData("round(CAST(-1.5 AS DOUBLE))", -2d)]
    [InlineData("round(CAST(-0.5 AS DOUBLE))", -1d)]
    [InlineData("round(CAST(-0.4 AS DOUBLE), 3)", -0.4d)]
    public void RoundingAwayFromZeroIsUntouched(string sql, double expected) =>
        Assert.Equal(expected, Eval(sql));

    // ── 3. A parse from TEXT carries the sign ──────────────────────────────────────────────

    /// <summary>
    /// .NET Framework's number parser answers a POSITIVE zero for "-0.0", so before #282 this
    /// whole theory passed on net10.0 and net8.0 and failed on net472 — the answer depended on
    /// which runtime loaded the library rather than on the value.
    /// </summary>
    [Theory]
    [InlineData("CAST('-0.0' AS DOUBLE)")]
    [InlineData("CAST('-0' AS DOUBLE)")]
    [InlineData("CAST('-.0' AS DOUBLE)")]
    [InlineData("CAST('-0E5' AS DOUBLE)")]
    [InlineData("CAST('  -0.0  ' AS DOUBLE)")]
    [InlineData("CAST('-0.0' AS FLOAT)")]
    // The sign is read off the TEXT and it has to be: this underflows to a zero whose sign
    // appears nowhere in the digits.
    [InlineData("CAST('-1e-400' AS DOUBLE)")]
    // ...and through the Java type suffix of #258, which is a second parse needing the same rule.
    [InlineData("CAST('-0.0d' AS DOUBLE)")]
    [InlineData("CAST('-0.0f' AS FLOAT)")]
    public void ParsingSignedZeroTextKeepsTheSign(string sql) =>
        Assert.True(IsNegativeZero(Eval(sql)));

    [Theory]
    [InlineData("CAST('0.0' AS DOUBLE)")]
    [InlineData("CAST('+0.0' AS DOUBLE)")]
    [InlineData("CAST('1e-400' AS DOUBLE)")]
    [InlineData("CAST('0.0d' AS DOUBLE)")]
    public void ParsingUnsignedZeroTextDoesNot(string sql) => Assert.True(IsPositiveZero(Eval(sql)));

    // ── What the sign does NOT reach ───────────────────────────────────────────────────────

    /// <summary>
    /// Every cast out of a double drops the sign, and the decimal one is the reason this test
    /// exists: #244 made it go through the value's RENDERING, which is the one place the sign is
    /// written down — so making the negation right could have started producing "-0.00".
    /// </summary>
    [Theory]
    [InlineData("CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS DECIMAL(10,2)) AS STRING)", "0.00")]
    [InlineData("CAST(CAST(negative(CAST(0.0 AS FLOAT)) AS DECIMAL(10,2)) AS STRING)", "0.00")]
    [InlineData("CAST(CAST('-0.0' AS DECIMAL(10,2)) AS STRING)", "0.00")]
    [InlineData("CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS BIGINT) AS STRING)", "0")]
    [InlineData("CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS BOOLEAN) AS STRING)", "false")]
    // ...but it does survive a cast to the other float width, which is a sign-bit copy, and the
    // rendering itself, which is the only channel that can see any of this.
    [InlineData("CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS FLOAT) AS STRING)", "-0.0")]
    [InlineData("CAST(negative(CAST(0.0 AS DOUBLE)) AS STRING)", "-0.0")]
    public void CastingOutOfADoubleRendersAsSparkDoes(string sql, string expected)
    {
        var array = new ArrowRowEvaluator(Ansi)
            .EvaluateExpression(SparkSqlParser.ParseExpression(sql), Batch());

        Assert.Equal(expected, ((StringArray)array).GetString(0));
    }

    /// <summary>
    /// The two zeros compare EQUAL, in Spark and in .NET alike, which is what made this defect
    /// invisible to everything but a rendering.
    /// </summary>
    [Theory]
    [InlineData("negative(CAST(0.0 AS DOUBLE)) = CAST(0.0 AS DOUBLE)", true)]
    [InlineData("negative(CAST(0.0 AS DOUBLE)) < CAST(0.0 AS DOUBLE)", false)]
    [InlineData("negative(CAST(0.0 AS DOUBLE)) <=> CAST(0.0 AS DOUBLE)", true)]
    [InlineData("negative(CAST(0.0 AS DOUBLE)) IN (CAST(0.0 AS DOUBLE))", true)]
    public void ComparisonCannotSeeTheSign(string sql, bool expected)
    {
        var array = new ArrowRowEvaluator(Ansi)
            .EvaluatePredicate(SparkSqlParser.ParsePredicate(sql), Batch());

        Assert.Equal(expected, array.GetValue(0));
    }

    /// <summary>
    /// So <c>greatest</c> and <c>least</c> cannot choose between them by value, and both keep
    /// whichever argument came FIRST. Asserted in both orders, because one order alone would look
    /// like a rule about the sign.
    /// </summary>
    [Theory]
    [InlineData("greatest(negative(CAST(0.0 AS DOUBLE)), CAST(0.0 AS DOUBLE))", true)]
    [InlineData("least(negative(CAST(0.0 AS DOUBLE)), CAST(0.0 AS DOUBLE))", true)]
    [InlineData("greatest(CAST(0.0 AS DOUBLE), negative(CAST(0.0 AS DOUBLE)))", false)]
    [InlineData("least(CAST(0.0 AS DOUBLE), negative(CAST(0.0 AS DOUBLE)))", false)]
    public void ExtremesKeepTheFirstArgumentOnATie(string sql, bool negative) =>
        Assert.Equal(negative, IsNegativeZero(Eval(sql)));

    /// <summary>
    /// A sign-bit flip reaches the non-finite values too, and only two of the three show it:
    /// Java prints a negated NaN as "NaN", so the sign bit unary minus really does set there is
    /// written down nowhere.
    /// </summary>
    [Theory]
    [InlineData("CAST(negative(CAST('NaN' AS DOUBLE)) AS STRING)", "NaN")]
    [InlineData("CAST(negative(CAST('Infinity' AS DOUBLE)) AS STRING)", "-Infinity")]
    [InlineData("CAST(negative(CAST('-Infinity' AS DOUBLE)) AS STRING)", "Infinity")]
    public void NonFiniteNegationRendersAsSparkDoes(string sql, string expected)
    {
        var array = new ArrowRowEvaluator(Ansi)
            .EvaluateExpression(SparkSqlParser.ParseExpression(sql), Batch());

        Assert.Equal(expected, ((StringArray)array).GetString(0));
    }

    /// <summary>Unary minus over a null column is still a column of nulls, and still a double.</summary>
    /// <remarks>
    /// <c>-NULL</c> is a double in Spark (#293) and reaches the floating-point path as a
    /// <see cref="NullArray"/>, which has no value to negate. It had a case in the subtraction it
    /// no longer goes through, so the replacement has to keep answering.
    /// </remarks>
    [Fact]
    public void NegatingNullIsANullDouble()
    {
        var array = new ArrowRowEvaluator(Ansi)
            .EvaluateExpression(SparkSqlParser.ParseExpression("negative(NULL)"), Batch());

        Assert.IsType<DoubleType>(array.Data.DataType);
        Assert.Null(((DoubleArray)array).GetValue(0));
    }
}
