// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// <see cref="SparkDecimalRounding"/>'s half-unit test, which #902 changed from lifting both sides to
/// the literal's scale to truncating the literal one place past the bound's, held to the exact
/// arithmetic it replaced.
/// </summary>
/// <remarks>
/// The rounding rule's agreement with Spark's unification is checked in
/// <c>EngineeredWood.Expressions.Arrow.Tests.SparkDecimalRoundingTests</c>; this checks only that the
/// cheaper arithmetic answers as the old did. The literal is kept to a few integral digits against a
/// decimal bound of scale <c>bs</c> and the literal's scale is past it, so every case reaches the
/// half-unit test and <c>Rounds</c> is exactly its negation.
/// </remarks>
public class SparkDecimalRoundingHalfUnitTests
{
    [Fact]
    public void TheHalfUnitTestMatchesTheExactArithmetic()
    {
        var wrong = new List<string>();

        foreach (var boundScale in new[] { 0, 1, 2, 5 })
        foreach (var extra in new[] { 1, 2, 3, 7, 40, 80 })
        foreach (var boundUnscaled in new BigInteger[] { 0, 1, -1, 3, -3, 125, -125, 999 })
        {
            var literalScale = boundScale + extra;
            var lift = BigInteger.Pow(10, extra);
            var half = lift / 2; // exact: extra >= 1
            var centre = boundUnscaled * lift;

            // Around the bound, and around half a unit either side of it, where truncation matters.
            var offsets = new List<BigInteger> { 0, 1, -1 };
            foreach (var edge in new[] { half, -half, lift, -lift, 3 * half, -3 * half })
            {
                offsets.Add(edge);
                offsets.Add(edge + 1);
                offsets.Add(edge - 1);
            }

            foreach (var offset in offsets)
            {
                var literalUnscaled = centre + offset;
                var literal = LiteralValue.HighPrecisionDecimalOf(literalUnscaled, literalScale);
                var bound = LiteralValue.HighPrecisionDecimalOf(boundUnscaled, boundScale);

                // At the literal's scale, half a unit at the bound's is half of 10^extra.
                var fartherApart = 2 * BigInteger.Abs(literalUnscaled - centre) > lift;

                if (SparkDecimalRounding.Rounds(literal, bound) == fartherApart)
                {
                    wrong.Add($"literal {literalUnscaled}e-{literalScale} vs bound {boundUnscaled}e-{boundScale}: "
                        + $"farther apart {fartherApart}");
                }
            }
        }

        Assert.Empty(wrong);
    }

    /// <summary>A literal as a System.Decimal takes the same path, its scale from its representation.</summary>
    [Theory]
    [InlineData("1.255", 125, 2, true)] // exactly half a unit from 1.25: not farther apart
    [InlineData("1.2551", 125, 2, false)]
    [InlineData("1.2449", 125, 2, false)]
    [InlineData("1.245", 125, 2, true)]
    [InlineData("-1.255", -125, 2, true)]
    [InlineData("-1.2551", -125, 2, false)]
    [InlineData("0.0000000000000000000000000001", 0, 0, true)]
    public void ASystemDecimalLiteral(string text, int boundUnscaled, int boundScale, bool rounds)
    {
        var literal = LiteralValue.Of(decimal.Parse(text, System.Globalization.CultureInfo.InvariantCulture));
        var bound = LiteralValue.HighPrecisionDecimalOf(boundUnscaled, boundScale);

        Assert.Equal(rounds, SparkDecimalRounding.Rounds(literal, bound));
    }
}
