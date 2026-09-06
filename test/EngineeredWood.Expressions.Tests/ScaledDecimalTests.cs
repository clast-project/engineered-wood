// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// Differential tests pinning <see cref="ScaledDecimal"/> against correctly rounded conversion.
/// </summary>
/// <remarks>
/// Two oracles, because no single one covers every target:
/// <list type="bullet">
/// <item>On net8.0 and later, <see cref="Reference"/> writes the value out in full and lets the
/// BCL's parser round it. That is an algorithm this repository did not write, so agreement means
/// something, and it is cheap enough to run over tens of thousands of generated values.</item>
/// <item>On every target including net472, <see cref="TheHarvestedAnswersHoldOnEveryTarget"/>
/// compares against bit patterns written into the source. .NET Framework's parser is NOT
/// correctly rounded, so it cannot be an oracle — see <see cref="Reference"/> — and a table is
/// the only thing that pins that target to the right answer rather than to its own parser.</item>
/// </list>
/// <para>
/// The routes the production code refuses are checked too, and they are the reason this file
/// exists: a test that cannot distinguish the fix from the defect proves nothing, so
/// <see cref="TheRejectedRoutesReallyDoRoundTwice"/> shows both of them answering wrongly on
/// inputs these tests then get right.
/// </para>
/// </remarks>
public class ScaledDecimalTests
{
    private static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;

    /// <summary>Two doubles apart by how many representable steps, for a readable failure.</summary>
    /// <remarks>
    /// The gap is measured in <c>ulong</c> and saturated, because the mapped keys span very
    /// nearly the whole signed range and their difference does not fit one. Measured, the obvious
    /// <c>Math.Abs(ka - kb)</c> reports 9007199254740992 for +Infinity against -Infinity (a
    /// wrapped value, not a distance) and THROWS OverflowException on the pair whose difference
    /// lands exactly on long.MinValue. This runs only while building a failure message, so either
    /// one replaces a real mismatch with noise or with a second exception.
    /// </remarks>
    private static long Ulps(double a, double b)
    {
        long ka = BitConverter.DoubleToInt64Bits(a), kb = BitConverter.DoubleToInt64Bits(b);
        if (ka < 0) ka = unchecked(long.MinValue - ka);
        if (kb < 0) kb = unchecked(long.MinValue - kb);

        var distance = ka >= kb ? (ulong)ka - (ulong)kb : (ulong)kb - (ulong)ka;
        return distance > long.MaxValue ? long.MaxValue : (long)distance;
    }

    [Theory]
    // The failure-message helper needs its own test, for the same reason the corpus's comparison
    // does: it runs only when something has already gone wrong, so a defect in it destroys the
    // evidence rather than announcing itself. Given as raw bit patterns because two of these
    // cases cannot be written as literals.
    [InlineData(0x3FF0000000000000L, 0x3FF0000000000000L, 0L)]                  // 1.0 vs itself
    [InlineData(0x46293E5939A08CEAL, 0x46293E5939A08CE9L, 1L)]                  // the #202 pair
    // Opposite infinities. The keys are nearly the whole signed range apart, so subtracting them
    // as long WRAPPED and reported 9007199254740992 -- a plausible-looking number, and not a
    // distance at all.
    [InlineData(0x7FF0000000000000L, unchecked((long)0xFFF0000000000000), long.MaxValue)]
    // The pair whose difference lands exactly on long.MinValue, where Math.Abs THREW
    // OverflowException on top of the assertion failure that called it.
    [InlineData(0x7FFFFFFFFFFFFFFFL, unchecked((long)0x8000000000000001), long.MaxValue)]
    public void TheUlpHelperSaturatesRatherThanWrappingOrThrowing(long a, long b, long expected) =>
        Assert.Equal(
            expected,
            Ulps(BitConverter.Int64BitsToDouble(a), BitConverter.Int64BitsToDouble(b)));

    [Fact]
    public void TheWideDecimalFromTheIssueRoundsRatherThanTruncating()
    {
        // #202's headline case: a decimal(38,0) holding 1e30, which Spark casts to exactly 1E30.
        var unscaled = BigInteger.Pow(10, 30);

        Assert.Equal(1e30d, ScaledDecimal.ToDouble(unscaled, 0));

        // And the value it used to give, named so the regression is unmistakable.
        Assert.NotEqual(9.999999999999999e29d, ScaledDecimal.ToDouble(unscaled, 0));
    }

    [Fact]
    public void TheRejectedRoutesReallyDoRoundTwice()
    {
        // Neither of these is a strawman: one was the code in SparkArrays and the other was the
        // narrow path it preferred. If .NET ever fixes them this test fails, which is the right
        // moment to revisit the comments in ScaledDecimal.
        var unscaled = BigInteger.Pow(10, 30);
        Assert.Equal(9.999999999999999e29d, (double)unscaled / Math.Pow(10, 0));

        // (double)decimal double-rounds too, on a value well inside decimal's range. The decimal
        // holds 5814944.01700257601 to the digit, so the ulp is lost in the conversion and not
        // before it: 5814944.017002576 is the correctly rounded double and the cast answers
        // ...577. Measured, the cast is wrong on 17.4% of the decimals that fit exactly, and on
        // integral ones -- scale 0, where nothing has to be divided away -- on 0.2%.
        const decimal narrow = 5814944.01700257601m;
        var exact = BitConverter.Int64BitsToDouble(0x41562EA8011691F9L);

        Assert.False(
            exact.Equals((double)narrow),
            $"(double){narrow}m no longer double-rounds: it now gives {(double)narrow:R}");
        Assert.Equal(exact, ScaledDecimal.ToDouble(narrow));
    }

    [Theory]
    // Answers harvested from an independent correctly-rounded implementation (CPython's
    // decimal-to-float conversion) and written down as BIT PATTERNS, so that checking them
    // involves no parsing and no formatting by the platform under test. That is the whole point:
    // net472 cannot parse its way to the right answer, so it must be handed one.
    //
    // #202's case, and the value SparkArrays used to answer 9.999999999999999E+29 for.
    [InlineData("1000000000000000000000000000000", 0, 0x46293E5939A08CEAL)]
    // Two the .NET Framework parser gets wrong by an ulp, which is why this table exists.
    [InlineData("419659064020406523871147", 10, 0x42C3157978CD7C54L)]
    [InlineData("-33862", 17, unchecked((long)0xBD57D4091E9B79F7))]
    // The one (double)decimal gets wrong by an ulp.
    [InlineData("581494401700257601", 11, 0x41562EA8011691F9L)]
    // Either side of the exact fast path's two guards: 2^53 and 10^22.
    [InlineData("9007199254740993", 22, 0x3EAE392010175EE7L)]
    [InlineData("9007199254740992", 23, 0x3E782DB34012B251L)]
    // Spark's widest decimal, at full scale and at none.
    [InlineData("12345678901234567890123456789012345678", 38, 0x3FBF9ADD3746F65FL)]
    [InlineData("-99999999999999999999999999999999999999", 0, unchecked((long)0xC7D2CED32A16A1B1))]
    // Subnormals, which the exact path reaches by pinning the exponent rather than the
    // significand, and the smallest of them.
    [InlineData("1", 310, 0x000012688B70E62BL)]
    [InlineData("5", 324, 0x0000000000000001L)]
    // Off both ends, and a negative scale.
    [InlineData("1", 400, 0x0000000000000000L)]
    [InlineData("123", -2, 0x40C8060000000000L)]
    [InlineData("1", -400, 0x7FF0000000000000L)]
    public void TheHarvestedAnswersHoldOnEveryTarget(string unscaled, int scale, long expected)
    {
        var got = ScaledDecimal.ToDouble(BigInteger.Parse(unscaled, Invariant), scale);

        Assert.True(
            expected == BitConverter.DoubleToInt64Bits(got),
            $"{unscaled}E-{scale}: expected {BitConverter.Int64BitsToDouble(expected):R}, "
                + $"got {got:R} ({Ulps(BitConverter.Int64BitsToDouble(expected), got)} ulp)");
    }

    [Fact]
    public void OutOfRangeSaturatesRatherThanThrowing()
    {
        // A comparison is not a place that may throw, and an absurd scale must not ask for an
        // absurd power of ten either -- int.MinValue has no int negation, and BigInteger.Pow
        // would happily try to build 10^2147483648 before anything noticed.
        Assert.Equal(double.PositiveInfinity, ScaledDecimal.ToDouble(BigInteger.One, int.MinValue));
        Assert.Equal(double.NegativeInfinity, ScaledDecimal.ToDouble(BigInteger.MinusOne, int.MinValue));
        Assert.Equal(0d, ScaledDecimal.ToDouble(BigInteger.One, int.MaxValue));
        Assert.Equal(0d, ScaledDecimal.ToDouble(BigInteger.Zero, int.MinValue));
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// The independent oracle: the value written out in full, rounded once by the BCL's parser.
    /// </summary>
    /// <remarks>
    /// Positional rather than the mantissa-and-exponent pair the implementation works in, and
    /// rounded by an algorithm this repository did not write, so agreement between the two means
    /// something. Deliberately NOT a second copy of the implementation's own integer arithmetic —
    /// that would agree with itself and prove nothing.
    /// <para>
    /// net472 is excluded, and finding out why is half of what this file learned: .NET
    /// Framework's parser is NOT correctly rounded. It reads <c>-0.00000000000033862</c> an ulp
    /// away from the nearest double, so on that target this reference is the wrong one and would
    /// fail the implementation for being right — which is exactly what it did, before the
    /// implementation stopped parsing and started rounding in integer arithmetic.
    /// </para>
    /// </remarks>
    private static double Reference(BigInteger unscaled, int scale)
    {
        var sign = unscaled.Sign < 0 ? "-" : string.Empty;
        var digits = BigInteger.Abs(unscaled).ToString(Invariant);

        var text = scale switch
        {
            0 => digits,
            < 0 => digits + new string('0', -scale),
            _ when digits.Length > scale => digits.Insert(digits.Length - scale, "."),
            _ => "0." + digits.PadLeft(scale, '0'),
        };

        return double.Parse(sign + text, NumberStyles.Float, Invariant);
    }

    private static void AssertSameDouble(BigInteger unscaled, int scale)
    {
        var want = Reference(unscaled, scale);
        var got = ScaledDecimal.ToDouble(unscaled, scale);

        Assert.True(
            BitConverter.DoubleToInt64Bits(want) == BitConverter.DoubleToInt64Bits(got),
            $"{unscaled}E-{scale}: expected {want:R}, got {got:R} ({Ulps(want, got)} ulp)");
    }

    [Theory]
    // The exact fast path's edges, where an off-by-one in either guard shows up.
    [InlineData(9007199254740992L, 22)]     // 2^53 and 10^22: the last pair it may take
    [InlineData(9007199254740993L, 22)]     // one past 2^53, so the mantissa is no longer exact
    [InlineData(9007199254740992L, 23)]     // 10^23 is not exact, so the divisor disqualifies it
    [InlineData(-9007199254740992L, 22)]
    [InlineData(-9007199254740993L, 0)]
    [InlineData(0L, 0)]
    [InlineData(0L, 38)]
    [InlineData(1L, 0)]
    [InlineData(-1L, 28)]
    // A negative scale, which HighPrecisionDecimalOf accepts and the fast path declines.
    [InlineData(123L, -2)]
    [InlineData(-123L, -5)]
    public void TheFastPathAgreesWithTheReferenceAtItsEdges(long unscaled, int scale) =>
        AssertSameDouble(unscaled, scale);

    [Fact]
    public void EveryGeneratedDecimalConvertsToTheReferenceBitPattern()
    {
        // Deterministic, and wide enough to straddle the fast path in both directions: 1..40
        // digits spans decimal's ceiling and Spark's precision 38, and scale 0..40 spans the
        // exact-power-of-ten cutoff at 22.
        var rng = new Random(202);

        for (var i = 0; i < 20_000; i++)
        {
            var digits = rng.Next(1, 41);
            var scale = rng.Next(0, 41);

            var text = new System.Text.StringBuilder(digits);
            text.Append((char)('1' + rng.Next(9)));
            for (var d = 1; d < digits; d++)
                text.Append((char)('0' + rng.Next(10)));

            var unscaled = BigInteger.Parse(text.ToString(), Invariant);
            if (rng.Next(2) == 0)
                unscaled = -unscaled;

            AssertSameDouble(unscaled, scale);
        }
    }

    [Fact]
    public void EveryGeneratedDecimalAgreesThroughTheDecimalOverloadToo()
    {
        // The decimal overload takes the value apart with GetBits, so this pins the unpacking --
        // three UNSIGNED words and a sign that lives elsewhere -- as much as the rounding.
        var rng = new Random(2020);

        for (var i = 0; i < 20_000; i++)
        {
            var scale = rng.Next(0, 29);
            var value = new decimal(
                rng.Next(int.MinValue, int.MaxValue),
                rng.Next(int.MinValue, int.MaxValue),
                rng.Next(int.MinValue, int.MaxValue),
                isNegative: rng.Next(2) == 0,
                (byte)scale);

            var bits = decimal.GetBits(value);
            var unscaled = ((BigInteger)(uint)bits[2] << 64)
                + ((BigInteger)(uint)bits[1] << 32)
                + (uint)bits[0];
            if (value < 0m)
                unscaled = -unscaled;

            var want = Reference(unscaled, scale);
            var got = ScaledDecimal.ToDouble(value);

            Assert.True(
                BitConverter.DoubleToInt64Bits(want) == BitConverter.DoubleToInt64Bits(got),
                $"{value}: expected {want:R}, got {got:R} ({Ulps(want, got)} ulp)");
        }
    }
#endif
}
