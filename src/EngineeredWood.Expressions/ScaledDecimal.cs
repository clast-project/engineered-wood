// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;

namespace EngineeredWood.Expressions;

/// <summary>
/// A decimal held as an unscaled integer and a scale, converted to the nearest
/// <see cref="double"/>.
/// </summary>
/// <remarks>
/// Shared by the literal side (<see cref="LiteralValue"/>) and the column side
/// (<c>SparkArrays</c>) so that both convert a decimal to a double by one rule.
/// <para>
/// Spark reaches a double from a decimal through Java's <c>BigDecimal.doubleValue</c>, which is
/// correctly rounded, so round-to-nearest, ties-to-even is required, not a nicety.
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
    /// Not the built-in <c>(double)value</c> cast, which double-rounds; see
    /// <see cref="ToDouble(BigInteger, int)"/>. <see cref="decimal.GetBits(decimal)"/> yields the
    /// unscaled-and-scale pair the rest of this class works in, so both widths of decimal answer
    /// by one rule.
    /// </remarks>
    internal static double ToDouble(decimal value)
    {
        var bits = decimal.GetBits(value);
        var scale = (bits[3] >> 16) & 0xFF;

        // The mantissa is 96 bits across three ints, low word first, each unsigned -- the sign
        // lives in bits[3] alone.
        var unscaled = ((BigInteger)(uint)bits[2] << 64)
            + ((BigInteger)(uint)bits[1] << 32)
            + (uint)bits[0];

        return ToDouble(value < 0m ? -unscaled : unscaled, scale);
    }

    /// <summary>An unscaled integer and a scale as the nearest <see cref="double"/>.</summary>
    /// <remarks>
    /// <para>Three simpler routes to this answer are wrong:</para>
    /// <list type="bullet">
    /// <item><c>(double)unscaled / Math.Pow(10, scale)</c> — BigInteger's conversion to double
    /// truncates rather than rounding to nearest (<c>(double)10^30</c> is 9.999999999999999e29,
    /// one ulp below the 1e30 Spark produces), and the division then rounds a second time.</item>
    /// <item><c>(double)(decimal)value</c> — an ulp off on about 17% of decimals that fit
    /// <see cref="decimal"/> exactly. It is right at scale 0, but from the first fractional digit
    /// onwards it is wrong on 12%, rising past 25% by scale 16.</item>
    /// <item>Formatting the value and parsing it back — correct on .NET Core, but .NET Framework's
    /// parser is not correctly rounded (on net472 it reads <c>419659064020406523871147E-10</c> an
    /// ulp away from the nearest double). Rounding in exact integer arithmetic makes every target
    /// agree.</item>
    /// </list>
    /// </remarks>
    internal static double ToDouble(BigInteger unscaled, int scale)
    {
        if (unscaled.IsZero)
            return 0d;

        // Both operands exact, so IEEE division rounds once and lands on the correctly rounded
        // quotient. This keeps the ordinary decimal -- a decimal(12,2) column, say -- off the
        // BigInteger path entirely.
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
    /// Rounded once, from the exact value. Spark reaches a float from a decimal through Java's
    /// <c>BigDecimal.floatValue</c> and from text through <c>Float.parseFloat</c>, and both are
    /// correctly rounded. Going through <see cref="ToDouble(BigInteger, int)"/> and narrowing
    /// rounds twice, and the first rounding can land exactly on a tie the value itself was not on,
    /// giving the adjacent float.
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
