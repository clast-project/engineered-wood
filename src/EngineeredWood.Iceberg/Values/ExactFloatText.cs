// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;

namespace EngineeredWood.Iceberg.Values;

/// <summary>
/// Reads a JSON number as a correctly rounded <see cref="double"/> or <see cref="float"/> on every target.
/// </summary>
/// <remarks>
/// .NET Core's parsers have been correctly rounded since 3.0, and Java's are. .NET Framework's are not: about
/// 1% of 15- and 16-digit texts read one ulp out, and its float parser is worse. The netstandard2.0 build of
/// this library can run there, so it reads the text exactly instead, as a ratio of integers rounded to
/// nearest-even. The .NET Core builds use the platform parser, which tests check this against.
/// </remarks>
internal static class ExactFloatText
{
    internal static bool TryParseDouble(string text, out double value)
    {
#if NETSTANDARD2_0
        if (TryParseRational(text, out bool negative, out var num, out var den))
        {
            long bits = RoundToBinary(num, den, mantissaBits: 53, minExponent: -1022, maxExponent: 1023);
            value = BitConverter.Int64BitsToDouble(bits | (negative ? long.MinValue : 0));
            return true;
        }

        value = 0;
        return false;
#else
        return double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value);
#endif
    }

    internal static bool TryParseSingle(string text, out float value)
    {
#if NETSTANDARD2_0
        if (TryParseRational(text, out bool negative, out var num, out var den))
        {
            int bits = (int)RoundToBinary(num, den, mantissaBits: 24, minExponent: -126, maxExponent: 127);
            value = Int32BitsToSingle(bits | (negative ? int.MinValue : 0));
            return true;
        }

        value = 0;
        return false;
#else
        return float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value);
#endif
    }

    /// <summary>JSON number text as <paramref name="num"/> / <paramref name="den"/>, both non-negative.</summary>
    internal static bool TryParseRational(string text, out bool negative, out BigInteger num, out BigInteger den)
    {
        negative = text.StartsWith("-", StringComparison.Ordinal);
        num = den = BigInteger.One;
        if (!SingleValueJson.TryParseDecimalText(negative ? text.Substring(1) : text, out var unscaled, out int scale)
            || unscaled.Sign < 0 || Math.Abs((long)scale) > 10_000)
            return false;

        // A long exponent is a magnitude far past either type's range; bounding it keeps the powers small.
        if (scale >= 0)
        {
            num = unscaled;
            den = BigInteger.Pow(10, scale);
        }
        else
        {
            num = unscaled * BigInteger.Pow(10, -scale);
            den = BigInteger.One;
        }

        return true;
    }

    /// <summary>
    /// The bits of the nearest binary float to <paramref name="num"/> / <paramref name="den"/>, ties to even,
    /// without the sign; an infinity past the largest finite value.
    /// </summary>
    internal static long RoundToBinary(BigInteger num, BigInteger den, int mantissaBits, int minExponent, int maxExponent)
    {
        if (num.IsZero)
            return 0;

        int fractionBits = mantissaBits - 1;
        long infinity = (long)((2 * maxExponent) + 1) << fractionBits;

        // Scale so the quotient carries at least two bits past the mantissa: a guard bit, and the remainder
        // as the sticky bit.
        long shift = mantissaBits + 2 - (BitLength(num) - BitLength(den));
        if (shift > 0)
            num <<= (int)shift;
        else if (shift < 0)
            den <<= (int)-shift;

        var quotient = BigInteger.DivRem(num, den, out var remainder);
        long exponent = BitLength(quotient) - 1 - shift; // of the value's leading bit

        if (exponent > maxExponent)
            return infinity;

        // Below the normal range, the mantissa loses a bit for every step down. Past the last one the value is
        // under half the smallest subnormal, which rounds to zero.
        long bitsLeft = exponent >= minExponent ? mantissaBits : mantissaBits - (minExponent - exponent);
        if (bitsLeft < 0)
            return 0;
        int bits = (int)bitsLeft;
        int drop = (int)(BitLength(quotient) - bits);
        var kept = quotient >> drop;
        var dropped = quotient - (kept << drop);
        var half = BigInteger.One << (drop - 1);
        int cmp = dropped.CompareTo(half);
        if (cmp > 0 || (cmp == 0 && (!remainder.IsZero || !kept.IsEven)))
            kept += 1;

        // The value is kept * 2^(drop - shift).
        long unitExponent = drop - shift;
        if (kept.IsZero)
            return 0;

        long leading = BitLength(kept) - 1 + unitExponent;
        if (leading > maxExponent)
            return infinity;
        if (leading >= minExponent)
        {
            // Normal: kept has mantissaBits bits, or one more when rounding carried into a new power of two.
            var mantissa = BitLength(kept) > mantissaBits ? kept >> 1 : kept;
            long fraction = (long)(mantissa - (BigInteger.One << fractionBits));
            return ((leading + maxExponent) << fractionBits) | fraction;
        }

        // Subnormal: the value is kept * 2^(minExponent - fractionBits), and kept is the fraction itself.
        return (long)kept;
    }

    private static long BitLength(BigInteger value)
    {
        // BigInteger.GetBitLength is .NET 5+.
        var bytes = value.ToByteArray();
        int top = bytes.Length - 1;
        while (top > 0 && bytes[top] == 0)
            top--;
        int b = bytes[top];
        int bits = 0;
        while (b != 0)
        {
            bits++;
            b >>= 1;
        }

        return (top * 8L) + bits;
    }

    private static unsafe float Int32BitsToSingle(int bits) => *(float*)&bits;
}
