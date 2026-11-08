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
    /// The widest precision, and so the largest scale, any Delta decimal can have; every caller enforces
    /// it. Text describing a value outside it is refused before the value is built: an exponent is
    /// otherwise bounded only by <see cref="int"/>, and <c>1e50000000</c> took 81 s and 910 MB to build a
    /// value the caller then discarded (#760).
    /// </summary>
    internal const int MaxPrecision = 38;

    /// <summary>
    /// Parses <paramref name="text"/> as an optionally-signed decimal number with an optional fractional part
    /// and an optional exponent (<c>1.23e4</c>), yielding
    /// <paramref name="unscaled"/> / 10^<paramref name="scale"/>. <paramref name="scale"/> is always
    /// non-negative: a negative one is absorbed into <paramref name="unscaled"/>.
    /// </summary>
    /// <remarks>
    /// <paramref name="scale"/> is also at most <see cref="MaxPrecision"/>: trailing zeros past it are dropped,
    /// which leaves the value unchanged.
    /// </remarks>
    /// <returns>
    /// False when the text is absent, malformed, or carries a non-numeric exponent, and when the value it
    /// describes cannot be any Delta decimal: one needing more than <see cref="MaxPrecision"/> digits of
    /// precision once trailing fractional zeros are discounted, or with a non-zero digit past the
    /// <see cref="MaxPrecision"/>th decimal place.
    /// </returns>
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

        if (digits.Length == 0)
            return false;
        foreach (char c in digits)
        {
            if (c is < '0' or > '9')
                return false;
        }

        // value = digits * 10^(exponent - fractionalDigits). In long: an exponent near either end of int
        // overflows the subtraction.
        long valueScale = fractionalDigits - (long)exponent;

        int first = 0;
        while (first < digits.Length && digits[first] == '0')
            first++;
        if (first == digits.Length)
        {
            // Zero at any exponent is zero; keep the scale the text gave it, within range.
            scale = (int)Math.Max(0, Math.Min(valueScale, MaxPrecision));
            return true;
        }

        // Trailing zeros past the largest scale change nothing about the value: drop them.
        int last = digits.Length;
        while (valueScale > MaxPrecision && digits[last - 1] == '0')
        {
            last--;
            valueScale--;
        }

        // Refuse BEFORE building anything (#760). A non-zero digit past the 38th decimal place, or a value
        // needing more than 38 digits of precision, is no Delta decimal, and every caller would throw the
        // value away. The precision a value needs is its significant digits, plus the zeros a negative
        // scale appends, minus the trailing FRACTIONAL zeros a narrower column's rescale may drop:
        // 12345678901234567890123456789012345678.1 needs 39, but 1.000...0 padded to scale 38 needs 1.
        if (valueScale > MaxPrecision)
            return false;
        long significantDigits = last - first;
        long precision;
        if (valueScale < 0)
        {
            precision = significantDigits - valueScale;
        }
        else
        {
            int droppable = 0;
            while (droppable < valueScale && digits[last - 1 - droppable] == '0')
                droppable++;
            precision = significantDigits - droppable;
        }
        if (precision > MaxPrecision)
            return false;

        unscaled = BigInteger.Parse(digits.Substring(first, last - first), NumberStyles.None,
            CultureInfo.InvariantCulture);
        if (negative)
            unscaled = -unscaled;

        // A negative scale is absorbed into the integer so the reported scale is always >= 0. The bound
        // above keeps this power below 10^38.
        if (valueScale < 0)
        {
            unscaled *= BigInteger.Pow(10, (int)-valueScale);
            valueScale = 0;
        }

        scale = (int)valueScale;
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
    /// <returns>
    /// False when narrowing would discard a non-zero digit, or when either scale is outside
    /// 0..<see cref="MaxPrecision"/>, which no Delta decimal has.
    /// </returns>
    public static bool TryRescale(BigInteger unscaled, int fromScale, int toScale, out BigInteger rescaled)
    {
        rescaled = BigInteger.Zero;
        if (fromScale is < 0 or > MaxPrecision || toScale is < 0 or > MaxPrecision)
            return false;

        if (toScale >= fromScale)
        {
            rescaled = unscaled * BigInteger.Pow(10, toScale - fromScale);
            return true;
        }

        var divisor = BigInteger.Pow(10, fromScale - toScale);
        rescaled = BigInteger.DivRem(unscaled, divisor, out var remainder);
        return remainder.IsZero;
    }

    /// <summary>
    /// Renders the decimal at <paramref name="row"/> of a fixed-width decimal array (<paramref name="width"/> bytes
    /// per value) as its exact digits: <c>-0.05</c>, <c>1.50</c>, every digit of a decimal(38,x). Never goes through
    /// <see cref="decimal"/>, which holds ~28-29 significant digits and throws on Delta's own decimal(38,0) range.
    /// Used for statistics bounds, where a rounded bound shifts what prunes, and partition values, where a rounded
    /// value is the wrong partition.
    /// </summary>
    public static string Format(Apache.Arrow.IArrowArray array, int row, int width, int scale)
    {
        var bytes = array.Data.Buffers[1].Span.Slice((row + array.Data.Offset) * width, width);
#if NET6_0_OR_GREATER
        var unscaled = new BigInteger(bytes, isUnsigned: false, isBigEndian: false);
#else
        var unscaled = new BigInteger(bytes.ToArray());
#endif
        // The array's buffers may be native memory the span does not root. See doc/arrow-span-lifetime.md.
        GC.KeepAlive(array);
        if (scale == 0)
            return unscaled.ToString(CultureInfo.InvariantCulture);

        string sign = unscaled.Sign < 0 ? "-" : "";
        string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture);
        if (digits.Length <= scale)
            digits = digits.PadLeft(scale + 1, '0');
        return sign + digits.Substring(0, digits.Length - scale)
            + "." + digits.Substring(digits.Length - scale);
    }
}
