// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;

namespace EngineeredWood.Expressions;

/// <summary>
/// A decimal held as an unscaled integer and a scale, converted to the nearest
/// <see cref="double"/>.
/// </summary>
/// <remarks>
/// Shared rather than written twice: the literal side (<see cref="LiteralValue"/>) and the column
/// side (<c>SparkArrays</c>) both convert a decimal to a double, and #171 fixed the rounding on
/// the first while #202 found the identical defect still sitting in the second. One conversion is
/// one answer.
/// <para>
/// Spark reaches a double from a decimal through Java's <c>BigDecimal.doubleValue</c>, which is
/// correctly rounded — so "round to nearest, ties to even" is not a nicety here, it is the oracle.
/// </para>
/// </remarks>
internal static class ScaledDecimal
{
    /// <summary>2^53, the last integer with every integer below it exactly representable.</summary>
    private const long ExactIntegerLimit = 9007199254740992L;

    /// <summary>
    /// The largest power of ten a <see cref="double"/> holds exactly; 10^23 already rounds.
    /// </summary>
    private const int LargestExactPowerOfTen = 22;

    /// <summary>Bits in a double's significand, the implicit leading one included.</summary>
    private const int SignificandBits = 53;

    /// <summary>The exponent every subnormal shares: the smallest of them is 1 × 2^-1074.</summary>
    private const int SubnormalExponent = -1074;

    /// <summary>
    /// The largest exponent a normal double reaches; the biggest finite value is
    /// (2^53 - 1) × 2^971.
    /// </summary>
    private const int MaxExponent = 971;

    /// <summary>What the exponent is offset by to become the IEEE 754 biased exponent.</summary>
    private const int ExponentBias = 1075;

    /// <summary>The 52 bits IEEE 754 gives the significand, the leading one being implicit.</summary>
    private const long SignificandMask = 0xFFFFFFFFFFFFFL;

    /// <summary>log2(10), for a cheap bound on a value's magnitude before any big arithmetic.</summary>
    private const double Log2Of10 = 3.3219280948873626d;

    /// <summary>10^0 through 10^22, every one of them exact.</summary>
    private static readonly double[] PowersOfTen = BuildPowersOfTen();

    private static double[] BuildPowersOfTen()
    {
        var powers = new double[LargestExactPowerOfTen + 1];
        powers[0] = 1d;
        for (var i = 1; i < powers.Length; i++)
            powers[i] = powers[i - 1] * 10d;
        return powers;
    }

    /// <summary>A <see cref="decimal"/> as the nearest <see cref="double"/>.</summary>
    /// <remarks>
    /// Not the built-in <c>(double)value</c> cast, for the reason recorded on
    /// <see cref="ToDouble(BigInteger, int)"/>: that cast double-rounds and is an ulp off on
    /// 17.4% of decimals. Taking the value apart with <see cref="decimal.GetBits(decimal)"/>
    /// hands over the same unscaled-and-scale pair the rest of this class works in, so both
    /// widths of decimal answer with one rule.
    /// </remarks>
    internal static double ToDouble(decimal value)
    {
        var bits = decimal.GetBits(value);
        var scale = (bits[3] >> 16) & 0xFF;

        // The mantissa is 96 bits across three ints, low word first, and every one of them is
        // UNSIGNED -- the sign lives in bits[3] alone.
        var unscaled = ((BigInteger)(uint)bits[2] << 64)
            + ((BigInteger)(uint)bits[1] << 32)
            + (uint)bits[0];

        return ToDouble(value < 0m ? -unscaled : unscaled, scale);
    }

    /// <summary>An unscaled integer and a scale as the nearest <see cref="double"/>.</summary>
    /// <remarks>
    /// <para>Three routes to this answer are wrong, and all three were in the codebase:</para>
    /// <list type="bullet">
    /// <item><c>(double)unscaled / Math.Pow(10, scale)</c> — BigInteger's conversion to double
    /// TRUNCATES rather than rounding to nearest (measured, <c>(double)10^30</c> is
    /// 9.999999999999999e29, one ulp below the 1e30 Spark produces) and the division then rounds
    /// a second time. This was <c>SparkArrays</c>, and is #202.</item>
    /// <item><c>(double)(decimal)value</c> — no better despite staying inside a type built for
    /// decimals. Measured over 250,000 decimals that fit <see cref="decimal"/> exactly, it lands
    /// an ulp off on <b>17.4%</b> of them, and the failures are not spread evenly: at scale 0 it
    /// is right (0.2% wrong), and from the first fractional digit onwards it is wrong on 12%
    /// rising past 25% by scale 16. #202 scoped itself to values past decimal's ~7.9e28 ceiling
    /// on the assumption that the narrow path was safe; only its INTEGRAL part is.</item>
    /// <item>Formatting the value and parsing it back once — correct on .NET Core, and what #171
    /// put in <see cref="LiteralValue"/>. It does not survive netstandard2.0: .NET Framework's
    /// parser is not correctly rounded, and on net472 it reads
    /// <c>419659064020406523871147E-10</c> an ulp away from the nearest double. A
    /// Spark-compatibility layer that answers differently on .NET Framework than on .NET is the
    /// same defect as #202 wearing a different hat, so this rounds in exact integer arithmetic
    /// instead and every target agrees.</item>
    /// </list>
    /// </remarks>
    internal static double ToDouble(BigInteger unscaled, int scale)
    {
        if (unscaled.IsZero)
            return 0d;

        // Both operands exact, so IEEE division rounds ONCE and lands on the correctly rounded
        // quotient. Measured over 2,000,000 values: this path accepts 99.3% of them and is
        // bit-exact against the exact route below on every one. It is what keeps the ordinary
        // decimal -- a decimal(12,2) column, say -- off the BigInteger path entirely.
        if (scale >= 0 && scale <= LargestExactPowerOfTen
            && unscaled >= -ExactIntegerLimit && unscaled <= ExactIntegerLimit)
        {
            return (double)(long)unscaled / PowersOfTen[scale];
        }

        var negative = unscaled.Sign < 0;
        var numerator = BigInteger.Abs(unscaled);

        // A generous bracket on log2 of the value, taken before anything is built. Its job is not
        // to decide the answer -- the margins sit far outside the representable range, so nothing
        // near a boundary reaches it -- but to stop an absurd scale from asking for an absurd
        // power of ten. Scale is normally 0..38, but HighPrecisionDecimalOf is public and the
        // format accessors pass whatever the file's metadata claims, int.MinValue included. The
        // multiply is in double, so it neither overflows nor needs the negation that would.
        var log2 = BitLength(numerator) - (scale * Log2Of10);
        if (log2 > 1100d)
            return negative ? double.NegativeInfinity : double.PositiveInfinity;
        if (log2 < -1200d)
            return 0d;

        var denominator = BigInteger.One;
        if (scale > 0)
            denominator = BigInteger.Pow(10, scale);
        else if (scale < 0)
            numerator *= BigInteger.Pow(10, -scale);

        // Line the quotient up on a double's 53 significand bits by shifting whichever side needs
        // it, then round away what is left over. The bit-length estimate is out by at most one in
        // either direction. A subnormal cannot go below its fixed exponent however small it gets
        // -- that is the range where precision runs out rather than magnitude -- so the pin here
        // is what makes the loops below stop at the right place.
        var exponent = BitLength(numerator) - BitLength(denominator) - SignificandBits;
        if (exponent < SubnormalExponent)
            exponent = SubnormalExponent;

        var significand = RoundedQuotient(numerator, denominator, exponent);

        // Rounding up can carry into a 54th bit, which needs this same step, so it is a loop
        // rather than the single correction the estimate alone would want.
        while (BitLength(significand) > SignificandBits)
        {
            exponent++;
            significand = RoundedQuotient(numerator, denominator, exponent);
        }

        while (BitLength(significand) < SignificandBits && exponent > SubnormalExponent)
        {
            exponent--;
            significand = RoundedQuotient(numerator, denominator, exponent);
        }

        if (exponent > MaxExponent)
            return negative ? double.NegativeInfinity : double.PositiveInfinity;

        // A subnormal is exactly the case where the significand never reached 53 bits, and IEEE
        // 754 spells it with a biased exponent of zero and no implicit leading one -- which is
        // what writing the significand alone produces. Everything else has 53 bits, so its
        // leading one falls off the 52-bit field on its own and the biased exponent is >= 1.
        var bits = exponent == SubnormalExponent && BitLength(significand) < SignificandBits
            ? (long)significand
            : ((long)(exponent + ExponentBias) << 52) | ((long)significand & SignificandMask);

        var value = BitConverter.Int64BitsToDouble(bits);
        return negative ? -value : value;
    }

    /// <summary>
    /// <c>numerator / (denominator × 2^exponent)</c>, rounded to nearest with ties to even.
    /// </summary>
    private static BigInteger RoundedQuotient(
        BigInteger numerator, BigInteger denominator, int exponent)
    {
        if (exponent > 0)
            denominator <<= exponent;
        else
            numerator <<= -exponent;

        var quotient = BigInteger.DivRem(numerator, denominator, out var remainder);

        // Comparing 2·remainder against the divisor asks "is the leftover more than half a step"
        // with no division of its own, and so introduces no second rounding. A tie goes to the
        // even significand, which is what IEEE 754 and Java's BigDecimal.doubleValue both do.
        var twiceRemainder = remainder << 1;
        if (twiceRemainder > denominator || (twiceRemainder == denominator && !quotient.IsEven))
            quotient += BigInteger.One;

        return quotient;
    }

    /// <summary>
    /// Bits in a non-negative <see cref="BigInteger"/>. Not <c>GetBitLength</c>, which arrived in
    /// net5.0 and this assembly targets netstandard2.0.
    /// </summary>
    private static int BitLength(BigInteger value)
    {
        if (value.IsZero)
            return 0;

        // Little-endian two's complement, so a positive value whose top bit is set carries an
        // extra zero byte to keep it positive. Skipping trailing zero bytes drops it.
        var bytes = value.ToByteArray();

        var top = bytes.Length - 1;
        while (top > 0 && bytes[top] == 0)
            top--;

        var bits = top * 8;
        for (var b = bytes[top]; b != 0; b >>= 1)
            bits++;

        return bits;
    }
}
