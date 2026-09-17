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

    /// <summary>log2(10), for a cheap bound on a value's magnitude before any big arithmetic.</summary>
    private const double Log2Of10 = 3.3219280948873626d;

    /// <summary>10^0 through 10^22, every one of them exact.</summary>
    private static readonly double[] PowersOfTen = BuildPowersOfTen();

    /// <summary>2^24, the float counterpart of <see cref="ExactIntegerLimit"/>.</summary>
    private const int ExactIntegerLimitSingle = 1 << 24;

    /// <summary>The largest power of ten a <see cref="float"/> holds exactly: 5^10 fits 24 bits.</summary>
    private const int LargestExactPowerOfTenSingle = 10;

    /// <summary>10^0 through 10^10, every one of them exact.</summary>
    private static readonly float[] PowersOfTenSingle =
        [1f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f];

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

        var bits = RoundToBinary(unscaled, scale, DoubleFormat, out var infinite);
        if (infinite)
            return unscaled.Sign < 0 ? double.NegativeInfinity : double.PositiveInfinity;

        var value = BitConverter.Int64BitsToDouble(bits);
        return unscaled.Sign < 0 ? -value : value;
    }

    /// <summary>An unscaled integer and a scale as the nearest <see cref="float"/>.</summary>
    /// <remarks>
    /// <para>
    /// <b>Rounded ONCE, from the exact value.</b> Spark reaches a float from a decimal through
    /// Java's <c>BigDecimal.floatValue</c> and from text through <c>Float.parseFloat</c>, and both
    /// are correctly rounded: measured on 4.0.3 over 2,253 decimals and 8,400 strings placed
    /// beside a float's rounding ties, not one differed from the exact answer. Going through
    /// <see cref="ToDouble(BigInteger, int)"/> and narrowing rounds twice, and the first rounding
    /// can land exactly on a tie the value itself was not on -- a third of those cases came back
    /// as the float next door that way. #372.
    /// </para>
    /// <para>
    /// The fast path is Java's own: an unscaled value below 2^24 and a scale up to 10 are both
    /// exact floats (5^10 still fits in 24 bits), so one IEEE division rounds once. A zero keeps
    /// the sign of the value it came from, which only text can carry -- a decimal zero has none.
    /// </para>
    /// </remarks>
    internal static float ToSingle(BigInteger unscaled, int scale, bool negativeZero = false)
    {
        if (unscaled.IsZero)
            return negativeZero ? -0f : 0f;

        if (scale >= 0 && scale <= LargestExactPowerOfTenSingle
            && unscaled > -ExactIntegerLimitSingle && unscaled < ExactIntegerLimitSingle)
        {
            return (float)(int)unscaled / PowersOfTenSingle[scale];
        }

        var bits = RoundToBinary(unscaled, scale, SingleFormat, out var infinite);
        var negative = unscaled.Sign < 0;
        if (infinite)
            return negative ? float.NegativeInfinity : float.PositiveInfinity;

#if NETSTANDARD2_0
        // No Int32BitsToSingle before netstandard2.1; the slow path is already BigInteger work.
        var value = BitConverter.ToSingle(BitConverter.GetBytes((int)bits), 0);
#else
        var value = BitConverter.Int32BitsToSingle((int)bits);
#endif
        return negative ? -value : value;
    }

    /// <summary>The shape of an IEEE 754 binary format, as far as rounding into it needs.</summary>
    private sealed class BinaryFormat(
        int significandBits, int subnormalExponent, int maxExponent, int exponentBias)
    {
        /// <summary>Bits in the significand, the implicit leading one included.</summary>
        public int SignificandBits { get; } = significandBits;

        /// <summary>The exponent every subnormal shares.</summary>
        public int SubnormalExponent { get; } = subnormalExponent;

        /// <summary>The largest exponent a normal value reaches.</summary>
        public int MaxExponent { get; } = maxExponent;

        /// <summary>What the exponent is offset by to become the IEEE 754 biased exponent.</summary>
        public int ExponentBias { get; } = exponentBias;

        public long SignificandMask => (1L << (SignificandBits - 1)) - 1;
    }

    private static readonly BinaryFormat DoubleFormat =
        new(SignificandBits, SubnormalExponent, MaxExponent, ExponentBias);

    /// <summary>A float: 24 significand bits, the smallest subnormal 2^-149, the largest
    /// finite value (2^24 - 1) × 2^104.</summary>
    private static readonly BinaryFormat SingleFormat = new(24, -149, 104, 150);

    /// <summary>
    /// The IEEE 754 bits of <c>|unscaled| × 10^-scale</c> in <paramref name="format"/>, rounded
    /// to nearest with ties to even; the sign is the caller's to apply.
    /// </summary>
    private static long RoundToBinary(
        BigInteger unscaled, int scale, BinaryFormat format, out bool infinite)
    {
        infinite = false;
        var numerator = BigInteger.Abs(unscaled);

        // A generous bracket on log2 of the value, taken before anything is built. Its job is not
        // to decide the answer -- the margins sit far outside the representable range, so nothing
        // near a boundary reaches it -- but to stop an absurd scale from asking for an absurd
        // power of ten. Scale is normally 0..38, but HighPrecisionDecimalOf is public and the
        // format accessors pass whatever the file's metadata claims, int.MinValue included. The
        // multiply is in double, so it neither overflows nor needs the negation that would.
        var log2 = BitLength(numerator) - (scale * Log2Of10);
        if (log2 > 1100d)
        {
            infinite = true;
            return 0L;
        }

        if (log2 < -1200d)
            return 0L;

        var denominator = BigInteger.One;
        if (scale > 0)
            denominator = BigInteger.Pow(10, scale);
        else if (scale < 0)
            numerator *= BigInteger.Pow(10, -scale);

        // Line the quotient up on the format's significand bits by shifting whichever side needs
        // it, then round away what is left over. The bit-length estimate is out by at most one in
        // either direction. A subnormal cannot go below its fixed exponent however small it gets
        // -- that is the range where precision runs out rather than magnitude -- so the pin here
        // is what makes the loops below stop at the right place.
        var exponent = BitLength(numerator) - BitLength(denominator) - format.SignificandBits;
        if (exponent < format.SubnormalExponent)
            exponent = format.SubnormalExponent;

        var significand = RoundedQuotient(numerator, denominator, exponent);

        // Rounding up can carry into one bit more, which needs this same step, so it is a loop
        // rather than the single correction the estimate alone would want.
        while (BitLength(significand) > format.SignificandBits)
        {
            exponent++;
            significand = RoundedQuotient(numerator, denominator, exponent);
        }

        while (BitLength(significand) < format.SignificandBits && exponent > format.SubnormalExponent)
        {
            exponent--;
            significand = RoundedQuotient(numerator, denominator, exponent);
        }

        if (exponent > format.MaxExponent)
        {
            infinite = true;
            return 0L;
        }

        // A subnormal is exactly the case where the significand never reached full width, and IEEE
        // 754 spells it with a biased exponent of zero and no implicit leading one -- which is
        // what writing the significand alone produces. Everything else is full width, so its
        // leading one falls off the field on its own and the biased exponent is >= 1.
        return exponent == format.SubnormalExponent && BitLength(significand) < format.SignificandBits
            ? (long)significand
            : ((long)(exponent + format.ExponentBias) << (format.SignificandBits - 1))
                | ((long)significand & format.SignificandMask);
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
