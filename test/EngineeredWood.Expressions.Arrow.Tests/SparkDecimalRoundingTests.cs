// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow.Spark;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The pruning rounding rule against the unification it was derived from.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="SparkDecimalRounding"/> does not reproduce Spark's least common type; it answers the
/// narrower question of whether comparing through that type could give a different answer from
/// comparing exactly. A derivation can go stale in a way a copy cannot — change
/// <c>SparkNumericTypes.ClampPreferringIntegralDigits</c> and the rule is silently wrong — so it
/// is checked here against the real unification, which is why this test lives in the assembly that
/// can see both. #323.
/// </para>
/// <para>
/// <b>The property is not "refuse whenever something rounds".</b> That was the first thing asserted
/// here and it is the wrong contract: a <c>decimal(2,0)</c> column against a
/// <c>decimal(38,37)</c> literal unifies to <c>decimal(38,36)</c>, which rounds the literal — by
/// 5e-37, on values that are eight apart. Rounding that cannot reach across the gap between the two
/// values cannot change the comparison, and refusing there would give up pruning for nothing.
/// </para>
/// <para>
/// The contract is one-sided in the way that matters: answering <c>Unknown</c> where an exact
/// answer would have done costs a file that could have been pruned, while answering exactly where
/// Spark's answer differs DROPS ROWS. So every exact answer is held to Spark's over the whole grid,
/// and what the refusals cost is held separately — by asserting that no ORDINARY decimal type is
/// refused at all, which is the bound that matters and the one a blunter rule would fail.
/// </para>
/// </remarks>
public class SparkDecimalRoundingTests
{
    /// <summary>
    /// Wherever the rule allows an exact comparison, it is the comparison Spark makes.
    /// </summary>
    [Fact]
    public void AnExactAnswerAlwaysMatchesSparks()
    {
        var wrong = new List<string>();
        var refused = 0;
        var total = 0;

        foreach (var (columnPrecision, columnScale) in DecimalTypes())
        foreach (var (literalPrecision, literalScale) in DecimalTypes())
        {
            var common = SparkNumericTypes.CommonType(
                new Decimal128Type(columnPrecision, columnScale),
                new Decimal128Type(literalPrecision, literalScale)) as Decimal128Type;

            if (common is null)
                continue;

            var literalUnscaled = BigInteger.Parse(new string('9', literalPrecision));

            foreach (var boundUnscaled in BoundsNear(literalUnscaled, literalScale, columnScale))
            {
                total++;

                var literal = LiteralValue.HighPrecisionDecimalOf(literalUnscaled, literalScale);
                var bound = LiteralValue.HighPrecisionDecimalOf(boundUnscaled, columnScale);

                if (SparkDecimalRounding.Rounds(literal, bound))
                {
                    refused++;
                    continue;
                }

                // Spark casts BOTH sides to the common type and compares there.
                var sparks = Rescale(literalUnscaled, literalScale, common.Scale)
                    .CompareTo(Rescale(boundUnscaled, columnScale, common.Scale));

                var ours = Compare(literalUnscaled, literalScale, boundUnscaled, columnScale);

                if (Math.Sign(sparks) != Math.Sign(ours))
                {
                    wrong.Add(
                        $"decimal({columnPrecision},{columnScale}) bound {boundUnscaled} against "
                        + $"decimal({literalPrecision},{literalScale}) literal, via {common}: "
                        + $"Spark {Math.Sign(sparks)}, exact {Math.Sign(ours)}");
                }
            }
        }

        Assert.Empty(wrong);

        // Refusals are sound by construction; what they cost is pruning. Over this grid — which
        // deliberately over-samples the extreme scales the clamp turns on, half of them at 10 or
        // above — the rule gives up a little over a quarter of all comparisons. What matters is
        // that none of them is an ordinary type, which is the next test.
        Assert.True(refused < total, $"the rule refused all {total} comparisons");
    }

    /// <summary>
    /// The decimal types a schema actually declares are never refused.
    /// </summary>
    /// <remarks>
    /// The rule's whole cost falls on extreme scales: a bound is only rounded when the literal's
    /// integral digits and the column's scale together exceed 38, and a literal is only at risk
    /// when it carries more decimal places than the column AND the two values sit within half a
    /// unit of each other. Neither reaches a <c>decimal(18,2)</c> holding a price.
    /// <para>
    /// This is the test that would fail if the rule were tightened into a blunt "refuse whenever
    /// the scales differ", which is the form #323 assumed option (2) would have to take and the
    /// reason it preferred plumbing the column's declared width through a public seam.
    /// </para>
    /// </remarks>
    [Fact]
    public void AnOrdinaryDecimalTypeIsNeverRefused()
    {
        var refusals = new List<string>();

        foreach (var columnScale in new[] { 0, 1, 2, 4, 6 })
        foreach (var literalScale in new[] { 0, 1, 2, 4, 6 })
        foreach (var literalDigits in new[] { 1, 3, 9, 15 })
        {
            var literalUnscaled = BigInteger.Parse(new string('9', literalDigits + literalScale));
            var literal = LiteralValue.HighPrecisionDecimalOf(literalUnscaled, literalScale);

            // Bounds a long way from the literal, which is where pruning is decided at all.
            foreach (var boundUnscaled in new[] { BigInteger.Zero, BigInteger.One, new BigInteger(123456) })
            {
                var bound = LiteralValue.HighPrecisionDecimalOf(boundUnscaled, columnScale);

                if (SparkDecimalRounding.Rounds(literal, bound))
                {
                    refusals.Add(
                        $"decimal(?,{columnScale}) bound {boundUnscaled} against a "
                        + $"{literalDigits}-digit literal at scale {literalScale}");
                }
            }
        }

        Assert.Empty(refusals);
    }

    /// <summary>
    /// An integral column is decided exactly, because its width comes from its kind.
    /// </summary>
    /// <remarks>
    /// The case that would otherwise have cost the most pruning: a <c>bigint</c> column carries no
    /// scale, so every predicate with a decimal point in it has more scale than the bound. Spark
    /// types the column <c>decimal(20,0)</c>, which leaves eighteen digits of room before the clamp
    /// can bite, so nothing rounds and the comparison stays exact.
    /// </remarks>
    [Theory]
    [InlineData(1.5)]
    [InlineData(0.125)]
    [InlineData(1234.5678)]
    public void AnIntegralColumnIsNeverRefused(decimal literal)
    {
        var bound = LiteralValue.Of(9_000_000_000L);

        Assert.False(SparkDecimalRounding.Rounds(LiteralValue.Of(literal), bound));
        Assert.False(SparkDecimalRounding.Rounds(LiteralValue.Of(1), bound));
    }

    /// <summary>The issue's own pair, and the control beside it.</summary>
    [Fact]
    public void TheMeasuredPairIsRefusedAndAnOrdinaryOneIsNot()
    {
        var nines = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse(new string('9', 38)), 38);

        // decimal(38,38) against the literal 1 unifies to decimal(38,37), where the bound rounds
        // up to exactly 1 — so Spark matches the row and an exact comparison prunes the file.
        Assert.True(SparkDecimalRounding.Rounds(LiteralValue.Of(1), nines));

        // decimal(10,2) against 1 unifies without giving up a thing.
        var ordinary = LiteralValue.HighPrecisionDecimalOf(new BigInteger(250), 2);
        Assert.False(SparkDecimalRounding.Rounds(LiteralValue.Of(1), ordinary));
    }

    /// <summary>
    /// A rounding too small to reach across the gap between the values is not refused.
    /// </summary>
    /// <remarks>
    /// The case that made the first version of this test wrong, kept as the pin for it: the
    /// literal really is rounded, and it cannot matter.
    /// </remarks>
    [Fact]
    public void ARoundingSmallerThanTheGapIsNotRefused()
    {
        var literal = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse(new string('9', 38)), 37);
        var bound = LiteralValue.HighPrecisionDecimalOf(BigInteger.One, 0);

        Assert.False(SparkDecimalRounding.Rounds(literal, bound));
    }

    /// <summary>Bounds at, beside and a long way from the literal, at the column's scale.</summary>
    private static IEnumerable<BigInteger> BoundsNear(BigInteger literal, int literalScale, int columnScale)
    {
        var at = Rescale(literal, literalScale, columnScale);

        yield return at;
        yield return at + BigInteger.One;
        yield return at - BigInteger.One;
        yield return BigInteger.Zero;
        yield return BigInteger.One;
    }

    /// <summary>The same value at another scale, rounding half away from zero as Spark does.</summary>
    private static BigInteger Rescale(BigInteger unscaled, int from, int to)
    {
        if (to >= from)
            return unscaled * BigInteger.Pow(10, to - from);

        var divisor = BigInteger.Pow(10, from - to);
        var quotient = BigInteger.DivRem(unscaled, divisor, out var remainder);

        return BigInteger.Abs(remainder) * 2 >= divisor ? quotient + unscaled.Sign : quotient;
    }

    /// <summary>The exact comparison, which is what the evaluator makes when the rule allows it.</summary>
    private static int Compare(BigInteger left, int leftScale, BigInteger right, int rightScale)
    {
        var common = Math.Max(leftScale, rightScale);

        return (left * BigInteger.Pow(10, common - leftScale))
            .CompareTo(right * BigInteger.Pow(10, common - rightScale));
    }

    /// <summary>Spark's decimal types, at the widths and scales the clamp turns on.</summary>
    private static IEnumerable<(int Precision, int Scale)> DecimalTypes()
    {
        int[] precisions = { 1, 2, 5, 10, 18, 20, 28, 30, 35, 37, 38 };

        foreach (var precision in precisions)
        foreach (var scale in new[] { 0, 1, 2, 6, 10, 18, 30, 37, 38 })
        {
            if (scale <= precision)
                yield return (precision, scale);
        }
    }
}
