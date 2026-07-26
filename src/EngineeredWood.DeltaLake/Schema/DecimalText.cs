// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Parses the text of a decimal number into its EXACT digits, as an unscaled <see cref="BigInteger"/> at the
/// value's own scale.
///
/// <para>Never route a Delta decimal through <see cref="decimal"/> on the way in.
/// <c>decimal.TryParse</c> and <c>JsonElement.TryGetDecimal</c> silently ROUND a value carrying more than
/// ~28-29 significant digits and report success — measured, not theorised: a <c>decimal(38,10)</c> value of
/// <c>1234567890123456789012345678.1234567890</c> comes back as
/// <c>1234567890123456789012345678.1</c>. Anything wider still throws <see cref="OverflowException"/> even
/// though Delta's own <c>decimal(38,x)</c> range is entirely legal. Both failures matter wherever the value
/// is an identity or a bound rather than an approximation — a rounded statistics bound prunes a file it
/// should have kept, and a rounded partition value is simply the wrong data.</para>
///
/// <para>Shared so the statistics decoder and the partition-column materialiser cannot drift apart on it.
/// They differ only in what they do about a value they cannot parse: pruning treats it as unknown, while
/// materialising a column has to fail.</para>
/// </summary>
internal static class DecimalText
{
    private static readonly char[] ExponentChars = { 'e', 'E' };

    /// <summary>
    /// Parses <paramref name="text"/> as an optionally-signed decimal number with an optional fractional part
    /// and an optional exponent (<c>1.23e4</c>), yielding
    /// <paramref name="unscaled"/> / 10^<paramref name="scale"/>. <paramref name="scale"/> is always
    /// non-negative: a negative one is absorbed into <paramref name="unscaled"/>.
    /// </summary>
    /// <returns>False when the text is absent, malformed, or carries a non-numeric exponent.</returns>
    public static bool TryParse(string? text, out BigInteger unscaled, out int scale)
    {
        unscaled = BigInteger.Zero;
        scale = 0;

        if (string.IsNullOrWhiteSpace(text))
            return false;

        string s = text!.Trim();

        int e = s.IndexOfAny(ExponentChars);
        int exponent = 0;
        string mantissa = s;
        if (e >= 0)
        {
            mantissa = s.Substring(0, e);
            if (!int.TryParse(s.Substring(e + 1), NumberStyles.AllowLeadingSign,
                    CultureInfo.InvariantCulture, out exponent))
                return false;
        }

        bool negative = false;
        if (mantissa.Length > 0 && (mantissa[0] == '-' || mantissa[0] == '+'))
        {
            negative = mantissa[0] == '-';
            mantissa = mantissa.Substring(1);
        }

        int dot = mantissa.IndexOf('.');
        int fractionalDigits = dot < 0 ? 0 : mantissa.Length - dot - 1;
        string digits = dot < 0 ? mantissa : mantissa.Substring(0, dot) + mantissa.Substring(dot + 1);

        if (digits.Length == 0
            || !BigInteger.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out unscaled))
            return false;

        if (negative)
            unscaled = -unscaled;

        // value = unscaled * 10^(exponent - fractionalDigits); a negative resulting scale is absorbed into
        // the integer so the reported scale is always >= 0.
        scale = fractionalDigits - exponent;
        if (scale < 0)
        {
            unscaled *= BigInteger.Pow(10, -scale);
            scale = 0;
        }

        return true;
    }

    /// <summary>
    /// Restates <paramref name="unscaled"/>, currently at <paramref name="fromScale"/>, at
    /// <paramref name="toScale"/>.
    ///
    /// <para>Widening the scale is exact. Narrowing it is only allowed when the digits being dropped are
    /// zeros — a writer may pad a value past the column's declared scale, and that is representable, but
    /// genuinely rounding here would produce a value that does not round-trip.</para>
    /// </summary>
    /// <returns>False when narrowing would discard a non-zero digit.</returns>
    public static bool TryRescale(BigInteger unscaled, int fromScale, int toScale, out BigInteger rescaled)
    {
        if (toScale >= fromScale)
        {
            rescaled = unscaled * BigInteger.Pow(10, toScale - fromScale);
            return true;
        }

        var divisor = BigInteger.Pow(10, fromScale - toScale);
        rescaled = BigInteger.DivRem(unscaled, divisor, out var remainder);
        return remainder.IsZero;
    }
}
