// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions.Arrow.Spark;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The scaled shortest-form generator against the exact one it stands in front of.
/// </summary>
/// <remarks>
/// <see cref="SparkFloatScaling"/> answers in machine words what <c>SparkFloatText.ExactDigits</c>
/// answers in <see cref="System.Numerics.BigInteger"/>, and declines rather than guessing wherever
/// the approximation cannot settle the question. So there are exactly two things to hold it to:
/// when it answers it must agree, and it must answer often enough to be worth having.
/// <para>
/// The exact path is itself pinned against <c>Double.toString</c> on JDK 21 by the sweeps in
/// <c>SparkFunctionRegistryTests</c>, so agreeing with it is agreeing with the JVM. Checked
/// outside the test run over every one of the 2,130,706,432 finite non-negative floats and many
/// millions of doubles; what is here is the sample small enough to run on every build.
/// </para>
/// </remarks>
public class SparkFloatScalingTests
{
    private const int DoubleMaxDigits = 17;

    private const int FloatMaxDigits = 9;

    /// <summary>Every value the scaled path answers, it answers the way the exact path does.</summary>
    [Theory]
    [InlineData("powers of two")]
    [InlineData("subnormals")]
    [InlineData("random doubles")]
    [InlineData("rounded magnitudes")]
    [InlineData("small integers")]
    [InlineData("floats")]
    public void TheScaledPathAgreesWithTheExactPath(string population)
    {
        var disagreements = new List<string>();

        foreach (var (mantissa, exponent, narrowBelow, maxDigits, source) in Population(population))
        {
            if (!SparkFloatScaling.TryShortestDigits(
                    mantissa, exponent, narrowBelow, maxDigits, out var digits, out var pointAt))
            {
                continue;
            }

            var exact = SparkFloatText.ExactDigits(mantissa, exponent, narrowBelow, maxDigits);

            if (digits != exact.Digits || pointAt != exact.PointAt)
            {
                disagreements.Add(
                    $"{source}: scaled {digits}e{pointAt} against exact {exact.Digits}e{exact.PointAt}");
            }
        }

        Assert.Empty(disagreements);
    }

    /// <summary>
    /// An ordinary rounded magnitude is answered in machine words, not handed to the exact path.
    /// </summary>
    /// <remarks>
    /// <b>This pins the rounding DIRECTION of the stored powers of five, which is not obvious and
    /// is easy to undo.</b> A value that is a short decimal — every price, every measurement
    /// rounded to a few places — scales to an exact integer, so its fixed-point fraction comes out
    /// at zero. That is the commonest case there is, and it is only safe to read the integer part
    /// there because the scaling can never land ABOVE the true value: the stored power is
    /// truncated and so is the shift that places it, so both err downward and a zero fraction
    /// means the integer part is right.
    /// <para>
    /// Rounding the stored power to nearest, or up, breaks that argument and the fraction has to
    /// be treated as ambiguous at both ends. Measured when it was: 21% of ordinary rounded
    /// magnitudes handed off to the exact path, for no wrong answers and several times the cost.
    /// </para>
    /// </remarks>
    [Fact]
    public void AnOrdinaryRoundedMagnitudeIsAnsweredWithoutTheExactPath()
    {
        var random = new Random(4242);
        var handedOff = 0;
        var total = 0;

        for (var i = 0; i < 20000; i++)
        {
            var value = Math.Round(random.NextDouble() * 10000, random.Next(0, 6));
            if (value == 0) continue;

            total++;
            var (mantissa, exponent, narrowBelow) = Decompose(value);

            if (!SparkFloatScaling.TryShortestDigits(
                    mantissa, exponent, narrowBelow, DoubleMaxDigits, out _, out _))
            {
                handedOff++;
            }
        }

        Assert.Equal(0, handedOff);
        Assert.True(total > 19000, $"only {total} values were generated");
    }

    /// <summary>A hand-off is still a right answer, because the exact path takes it.</summary>
    /// <remarks>
    /// Nothing here needs the scaled path to answer: <c>Render</c> is the same for a value it
    /// declines as for one it takes. These are the two widths' named edges, which run through
    /// whichever path claims them.
    /// </remarks>
    [Theory]
    [InlineData(1L, "4.9E-324")]
    [InlineData(4503599627370496L, "2.2250738585072014E-308")]
    [InlineData(4499096027743125504L, "5.960464477539063E-8")]
    [InlineData(4634874968833006078L, "73.53480521226444")]
    [InlineData(9218868437227405311L, "1.7976931348623157E308")]
    public void ADeclinedValueStillRendersTheWayJavaDoes(long bits, string expected) =>
        Assert.Equal(expected, SparkFloatText.Render(BitConverter.Int64BitsToDouble(bits)));

    private static IEnumerable<(long Mantissa, int Exponent, bool NarrowBelow, int MaxDigits, string Source)>
        Population(string population)
    {
        var random = new Random(20260916);

        switch (population)
        {
            case "powers of two":
                for (var e = -1022; e <= 1023; e++)
                    yield return FromDouble(Math.Pow(2, e));
                break;

            case "subnormals":
                for (var i = 0; i < 20000; i++)
                    yield return FromDouble(BitConverter.Int64BitsToDouble(
                        (long)(random.NextDouble() * 4503599627370495L) + 1));
                break;

            case "random doubles":
                for (var i = 0; i < 20000; i++)
                {
                    var value = BitConverter.Int64BitsToDouble(((long)random.Next() << 32) | (uint)random.Next());
                    if (double.IsNaN(value) || double.IsInfinity(value) || value == 0) continue;
                    yield return FromDouble(value);
                }

                break;

            case "rounded magnitudes":
                for (var i = 0; i < 20000; i++)
                {
                    var value = Math.Round(random.NextDouble() * 10000, random.Next(0, 6));
                    if (value == 0) continue;
                    yield return FromDouble(value);
                }

                break;

            case "small integers":
                for (var i = 1; i < 20000; i++)
                    yield return FromDouble(i);
                break;

            default:
                for (var i = 0; i < 20000; i++)
                {
                    var value = BitConverter.ToSingle(
                        BitConverter.GetBytes(random.Next(int.MinValue, int.MaxValue)), 0);
                    if (float.IsNaN(value) || float.IsInfinity(value) || value == 0) continue;
                    yield return FromFloat(value);
                }

                break;
        }
    }

    private static (long, int, bool, int, string) FromDouble(double value)
    {
        var (mantissa, exponent, narrowBelow) = Decompose(value);
        return (mantissa, exponent, narrowBelow, DoubleMaxDigits, value.ToString("R"));
    }

    private static (long, int, bool, int, string) FromFloat(float value)
    {
        var bits = BitConverter.ToInt32(BitConverter.GetBytes(value), 0) & int.MaxValue;
        var rawExponent = bits >> 23;
        var rawMantissa = bits & 0x7F_FFFF;

        var mantissa = rawExponent == 0 ? rawMantissa : rawMantissa | (1 << 23);
        var exponent = rawExponent == 0 ? -149 : rawExponent - 150;

        return (mantissa, exponent, rawMantissa == 0 && rawExponent > 1, FloatMaxDigits, value.ToString("R"));
    }

    /// <summary>The same decomposition <c>SparkFloatText</c> makes, so both paths see one value.</summary>
    private static (long Mantissa, int Exponent, bool NarrowBelow) Decompose(double value)
    {
        var magnitude = BitConverter.DoubleToInt64Bits(value) & long.MaxValue;
        var rawExponent = (int)(magnitude >> 52);
        var rawMantissa = magnitude & 0xF_FFFF_FFFF_FFFFL;

        return (
            rawExponent == 0 ? rawMantissa : rawMantissa | (1L << 52),
            rawExponent == 0 ? -1074 : rawExponent - 1075,
            rawMantissa == 0 && rawExponent > 1);
    }
}
