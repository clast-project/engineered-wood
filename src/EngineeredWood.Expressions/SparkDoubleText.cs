// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text;
using System.Threading;

namespace EngineeredWood.Expressions;

/// <summary>
/// Reading a double out of text the way Java's <c>Double.parseDouble</c> does, which is not what
/// every target's own parser does.
/// </summary>
/// <remarks>
/// <para>
/// <b>Down here rather than beside the renderer</b>, because a double is read in two places and
/// only one of them is the Arrow cast: <c>SparkLiteral</c> reads the exponent-bearing SQL literal,
/// and <c>SELECT 49.0793458194787E0</c> carried the same defect as the column value did. Nothing
/// in this type needs Arrow.
/// </para>
/// <para>
/// The mirror of <c>SparkFloatText</c>, and it has the same problem from the other
/// direction: the answer must not depend on which target framework loaded the library, and the
/// platform parse makes it depend on exactly that.
/// </para>
/// <para>
/// <b>.NET Framework REFUSES a value too large to represent</b> where .NET Core returns an
/// infinity and Java returns one too. So <c>CAST('1e400' AS DOUBLE)</c> was <c>Infinity</c> on
/// net10.0 and CAST_INVALID_INPUT on net472. It reached further than the double cast, because the
/// refusal goes through <c>IsNumeric</c> — the flag that decides whether a string is coerced to a
/// number or compared as text — so <c>s &gt; 1.0</c> over <c>'1e400'</c> was true on one framework
/// and not on the other. Measured. The integral cast is NOT among the things it moved: that is
/// CAST_INVALID_INPUT either way, because a magnitude outside <c>decimal</c>'s range has no exact
/// form to truncate whether it parsed as an infinity or not at all. #326.
/// </para>
/// <para>
/// The underflow side needs nothing: <c>'1e-400'</c> is zero on both, measured.
/// </para>
/// <para>
/// <b>And on the text it does accept, .NET Framework is not correctly rounded</b> — it reads about
/// 1% of ordinary fifteen- and sixteen-digit numbers as the double next door, silently. That is
/// #350, and <see cref="TryParseExact"/> is the answer to it.
/// </para>
/// </remarks>
internal static class SparkDoubleText
{
    /// <summary>
    /// Reads a double, saturating to an infinity where the magnitude is out of range.
    /// </summary>
    /// <remarks>
    /// The overflow branch costs nothing on the path that already works: it is reached only when
    /// the platform parse has already refused the text, which on .NET Core it never does for a
    /// number, and on .NET Framework only for one too large.
    /// </remarks>
    internal static bool TryParse(string text, out double value)
    {
#if NETSTANDARD2_0
        if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return Correct(text, ref value);
#else
        if (double.TryParse(text.AsSpan(), NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return true;
#endif

        return TryOverflow(text, out value);
    }

#if NETSTANDARD2_0
    /// <summary>
    /// Replaces the platform's answer with the correctly-rounded one, where there is one to have.
    /// </summary>
    /// <remarks>
    /// Only on the framework whose parser is not correctly rounded, and only for the plain decimal
    /// text <see cref="TryParseExact"/> claims: an infinity, a NaN or anything else it declines is
    /// a shape with no rounding in it, and the platform reads those exactly. #350.
    /// </remarks>
    private static bool Correct(string text, ref double value)
    {
        if (!double.IsNaN(value) && !double.IsInfinity(value) && TryParseExact(text, out var exact))
            value = exact;

        return true;
    }
#endif

#if !NETSTANDARD2_0
    /// <inheritdoc cref="TryParse(string, out double)"/>
    internal static bool TryParse(ReadOnlySpan<char> text, out double value)
    {
        if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return true;

        return TryOverflow(text.ToString(), out value);
    }
#endif

    /// <summary>
    /// Whether the text is a well-formed number whose magnitude exceeds a double.
    /// </summary>
    /// <remarks>
    /// Asked of the PARSER rather than of the digits, because "too large" and "not a number" are
    /// the two ways a parse can fail and only the parser can tell them apart without re-reading
    /// the grammar. .NET Framework raises <see cref="OverflowException"/> for the first and
    /// <see cref="FormatException"/> for the second; .NET Core raises neither, which is why it
    /// never reaches here.
    /// <para>
    /// The sign comes from the text, because there is no value to take it from.
    /// </para>
    /// </remarks>
    private static bool TryOverflow(string text, out double value)
    {
        try
        {
            value = double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
            return true;
        }
        catch (OverflowException)
        {
            value = text.TrimStart().StartsWith("-", StringComparison.Ordinal)
                ? double.NegativeInfinity
                : double.PositiveInfinity;

            return true;
        }
        catch (FormatException)
        {
            value = 0d;
            return false;
        }
    }

    /// <summary>
    /// The correctly-rounded double for plain decimal text, by exact arithmetic.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>.NET Framework's parser is not correctly rounded.</b> It is documented as accurate to
    /// about fifteen significant digits and it is not exact even there: measured over 100,000
    /// random doubles rendered three ways and re-read on net472, 964 of the fifteen-digit forms and
    /// 986 of the sixteen-digit ones came back as the double NEXT DOOR, while the seventeen-digit
    /// forms were clean. .NET Core has been correctly rounded since 3.0 and Java's
    /// <c>Double.parseDouble</c> always was, so net472 is the odd one out and
    /// <c>CAST('&lt;a sixteen-digit number&gt;' AS DOUBLE)</c> read a different VALUE there, with
    /// nothing raised. #350.
    /// </para>
    /// <para>
    /// <b>Compiled on every target and used on one.</b> The platform parse is already correct on
    /// .NET Core, so nothing there needs replacing — but a routine only ever exercised on the
    /// framework that cannot check it is a routine with no oracle. Building it everywhere lets
    /// the tests run it against .NET Core's own parser over millions of inputs, on every target,
    /// on every build.
    /// </para>
    /// <para>
    /// <b>Exact rather than fast, because of where it runs.</b> The value is
    /// <c>digits x 10^exponent</c>, which is a ratio of two integers, and the answer is the
    /// mantissa that ratio rounds to — one division, at whatever width the exponent demands.
    /// Eisel–Lemire would do it in machine words with a 128-bit power table, and this path already
    /// has one in <c>SparkFloatScaling</c>; it is not used because the correctness argument
    /// for the fast route is the part that takes the work, and this runs only on the framework
    /// that is already the slower one at everything.
    /// </para>
    /// </remarks>
    internal static bool TryParseExact(string text, out double value)
    {
        value = 0d;

        if (!TryScan(text, out var negative, out var digits, out var exponent))
            return false;

        value = FromRatio(digits, exponent, negative);
        return true;
    }

    /// <summary>
    /// Reads a float the way Java's <c>Float.parseFloat</c> does: rounded once, from the text.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Not a double parse narrowed to float</b>, which rounds twice and lands on the float next
    /// door whenever the first rounding reaches a tie the text was not on -- a third of the
    /// tie-adjacent strings measured for #372. .NET Core's <see cref="float"/> parse is correctly
    /// rounded and is used as it stands; .NET Framework's is not (it missed the same third), so
    /// that build replaces its answer with the exact one, as <see cref="TryParse(string, out double)"/>
    /// does for a double. A magnitude too large is an infinity on every target.
    /// </para>
    /// </remarks>
    internal static bool TryParseSingle(string text, out float value)
    {
#if NETSTANDARD2_0
        // WHETHER the text is a number stays the platform's question, so the accepted shapes and
        // whitespace are exactly what they were; only the VALUE is replaced. An infinite platform
        // answer is corrected too -- the NaN and Infinity words are shapes TryScan declines, so
        // they keep the platform's reading.
        if (float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
        {
            if (TryParseExactSingle(text, out var exact))
                value = exact;

            return true;
        }

        // .NET Framework REFUSES a float magnitude at or above the midpoint between float.MaxValue
        // and 2^128 -- including text just below that midpoint, which Java rounds DOWN to
        // float.MaxValue. The double parse still decides acceptance, but its value cannot stand
        // in: narrowing it is the double rounding this reader exists to avoid, and one below the
        // midpoint reads as the midpoint and then as infinity. Found in review of #374.
        if (!TryOverflow(text, out var asDouble))
        {
            value = 0f;
            return false;
        }

        value = TryParseExactSingle(text, out var exactOverflow) ? exactOverflow : (float)asDouble;
        return true;
#else
        if (float.TryParse(text.AsSpan(), NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return true;

        // .NET Core reads an overflowing float as an infinity rather than refusing it, so a
        // refusal here is a malformed number and the double parse refuses it too.
        var parsed = TryOverflow(text, out var asDouble);
        value = (float)asDouble;
        return parsed;
#endif
    }

#if !NETSTANDARD2_0
    /// <inheritdoc cref="TryParseSingle(string, out float)"/>
    internal static bool TryParseSingle(ReadOnlySpan<char> text, out float value)
    {
        if (float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return true;

        var parsed = TryOverflow(text.ToString(), out var asDouble);
        value = (float)asDouble;
        return parsed;
    }
#endif

    /// <summary>The correctly-rounded float for plain decimal text, by exact arithmetic.</summary>
    /// <remarks>
    /// The float counterpart of <see cref="TryParseExact"/>, and compiled on every target for the
    /// same reason: so the tests can hold it against .NET Core's parser. #372.
    /// </remarks>
    internal static bool TryParseExactSingle(string text, out float value)
    {
        value = 0f;

        if (!TryScan(text, out var negative, out var digits, out var exponent))
            return false;

        value = ScaledDecimal.ToSingle(negative ? -digits : digits, -exponent, negativeZero: negative);
        return true;
    }

    /// <summary>
    /// Reads a plain decimal number into its digits and a base-ten exponent.
    /// </summary>
    /// <remarks>
    /// Deliberately narrow. Everything it refuses — the infinity and NaN words, a hexadecimal
    /// form, a thousands separator, anything with a character left over — the caller keeps the
    /// platform's answer for, and every one of those is a shape the platform reads exactly anyway
    /// because there is no rounding in it. Widening the grammar here would put the ACCEPTANCE
    /// question on this routine as well as the value, which is a much larger surface than the
    /// defect.
    /// </remarks>
    private static bool TryScan(string text, out bool negative, out BigInteger digits, out int exponent)
    {
        negative = false;
        digits = BigInteger.Zero;
        exponent = 0;

        var index = 0;
        var end = text.Length;

        while (index < end && char.IsWhiteSpace(text[index])) index++;
        while (end > index && char.IsWhiteSpace(text[end - 1])) end--;

        if (index >= end)
            return false;

        if (text[index] == '+' || text[index] == '-')
        {
            negative = text[index] == '-';
            index++;
        }

        var written = new StringBuilder(end - index);
        var fraction = 0;
        var seenPoint = false;

        for (; index < end; index++)
        {
            var c = text[index];

            if (c >= '0' && c <= '9')
            {
                written.Append(c);
                if (seenPoint) fraction++;
            }
            else if (c == '.' && !seenPoint)
            {
                seenPoint = true;
            }
            else
            {
                break;
            }
        }

        if (written.Length == 0)
            return false;

        var scaled = 0L;

        if (index < end && (text[index] == 'e' || text[index] == 'E'))
        {
            index++;

            var negativeExponent = false;
            if (index < end && (text[index] == '+' || text[index] == '-'))
            {
                negativeExponent = text[index] == '-';
                index++;
            }

            var any = false;
            for (; index < end; index++)
            {
                var c = text[index];
                if (c < '0' || c > '9')
                    break;

                any = true;

                // Clamped far outside anything a double can reach, so a long exponent cannot wrap
                // and still lands on an infinity or a zero.
                if (scaled < 1_000_000)
                    scaled = (scaled * 10) + (c - '0');
            }

            if (!any)
                return false;

            if (negativeExponent)
                scaled = -scaled;
        }

        if (index != end)
            return false;

        digits = BigInteger.Parse(written.ToString(), NumberStyles.None, CultureInfo.InvariantCulture);
        exponent = (int)(scaled - fraction);

        return true;
    }

    /// <summary>
    /// The double nearest <c>digits x 10^exponent</c>, with ties to even.
    /// </summary>
    /// <remarks>
    /// The value is <c>num / den</c> with both sides integers, so the mantissa is the quotient of
    /// that ratio scaled to 53 bits, and the rounding decision is the remainder against half the
    /// divisor. Everything is exact; nothing is estimated and then corrected.
    /// </remarks>
    private static double FromRatio(BigInteger digits, int exponent, bool negative)
    {
        if (digits.IsZero)
            return negative ? -0d : 0d;

        // Far outside the range before any big power is built, so a wild exponent costs nothing.
        if (exponent > 400)
            return negative ? double.NegativeInfinity : double.PositiveInfinity;

        if (exponent < -400 - DigitCount(digits))
            return negative ? -0d : 0d;

        var numerator = exponent >= 0 ? digits * Powers.Ten(exponent) : digits;
        var denominator = exponent >= 0 ? BigInteger.One : Powers.Ten(-exponent);

        // Line the quotient up to 53 bits, then correct — the estimate is off by at most one.
        var binary = BitLength(numerator) - BitLength(denominator) - 53;

        if (binary < MinExponent)
            binary = MinExponent;

        var mantissa = Quotient(numerator, denominator, binary, out var remainder, out var divisor);

        while (mantissa >= Bit53 && binary < MaxExponent + 1)
        {
            binary++;
            mantissa = Quotient(numerator, denominator, binary, out remainder, out divisor);
        }

        while (mantissa < Bit52 && binary > MinExponent)
        {
            binary--;
            mantissa = Quotient(numerator, denominator, binary, out remainder, out divisor);
        }

        // Half to even, on the exact remainder.
        var twice = remainder * 2;
        if (twice > divisor || (twice == divisor && !mantissa.IsEven))
            mantissa += BigInteger.One;

        if (mantissa >= Bit53)
        {
            mantissa >>= 1;
            binary++;
        }

        if (binary > MaxExponent)
            return negative ? double.NegativeInfinity : double.PositiveInfinity;

        return Assemble(mantissa, binary, negative);
    }

    /// <summary>The integer part of <c>num / den / 2^binary</c>, with what is left over.</summary>
    /// <summary>
    /// The powers of ten the ratio is built from, held on first use rather than rebuilt per value.
    /// </summary>
    /// <remarks>
    /// This is a PER-ROW path on netstandard2.0 — every string cast to a double reaches it — and a
    /// value with an exponent far from zero wants a power hundreds of digits long. Rebuilding one
    /// per row is what the same nested-class cache in <c>SparkFloatText</c> and
    /// <c>SparkIntegralCasts</c> exists to avoid, so this follows them, down to publishing each row
    /// through <see cref="Volatile"/>: a reference assignment is atomic, but a plain store carries
    /// no ordering against the writes that filled the box, and a reader on arm64 could otherwise
    /// follow a non-null slot to digits it cannot see yet.
    /// <para>
    /// Sized for every exponent a finite double can be written with. Beyond that the value has
    /// already been answered as a zero or an infinity, and the fallback is there only so that an
    /// absurd literal cannot reach past the end of the table.
    /// </para>
    /// </remarks>
    private static class Powers
    {
        private const int Highest = 1100;

        private static readonly object?[] Tens = new object?[Highest + 1];

        internal static BigInteger Ten(int power)
        {
            if (power < 0 || power > Highest)
                return BigInteger.Pow(10, power);

            if (Volatile.Read(ref Tens[power]) is BigInteger cached)
                return cached;

            var computed = BigInteger.Pow(10, power);
            Volatile.Write(ref Tens[power], computed);

            return computed;
        }
    }

    private static BigInteger Quotient(
        BigInteger numerator, BigInteger denominator, int binary,
        out BigInteger remainder, out BigInteger divisor)
    {
        var top = numerator;
        divisor = denominator;

        if (binary >= 0)
            divisor <<= binary;
        else
            top <<= -binary;

        return BigInteger.DivRem(top, divisor, out remainder);
    }

    /// <summary>A mantissa and a binary exponent, as the bits of a double.</summary>
    /// <remarks>
    /// By hand rather than through a scaling multiply, which would round a second time. A mantissa
    /// short of the 53rd bit is a subnormal, whose exponent field is zero and whose bits ARE the
    /// mantissa.
    /// </remarks>
    private static double Assemble(BigInteger mantissa, int binary, bool negative)
    {
        var bits = (long)mantissa;

        if (bits >= (1L << 52))
            bits = ((long)(binary + 1075) << 52) | (bits & 0xF_FFFF_FFFF_FFFFL);

        if (negative)
            bits |= long.MinValue;

        return BitConverter.Int64BitsToDouble(bits);
    }

    /// <summary>How many bits an integer is written with.</summary>
    /// <remarks>
    /// By hand because netstandard2.0 has no <c>BigInteger.GetBitLength</c>, and from the byte
    /// array rather than a logarithm, which would be an estimate exactly where the estimate
    /// matters.
    /// </remarks>
    private static int BitLength(BigInteger value)
    {
        var bytes = value.ToByteArray();
        var index = bytes.Length - 1;

        while (index > 0 && bytes[index] == 0)
            index--;

        var top = bytes[index];
        var bits = index * 8;

        while (top != 0)
        {
            bits++;
            top >>= 1;
        }

        return bits;
    }

    /// <summary>How many decimal digits an integer is written with.</summary>
    private static int DigitCount(BigInteger value)
    {
        var digits = 1;
        value = BigInteger.Abs(value);

        while (value >= 10)
        {
            value /= 10;
            digits++;
        }

        return digits;
    }

    /// <summary>The smallest exponent a double can be written at, which is every subnormal's.</summary>
    private const int MinExponent = -1074;

    /// <summary>The largest, above which the value is an infinity.</summary>
    private const int MaxExponent = 971;

    private static readonly BigInteger Bit52 = BigInteger.One << 52;

    private static readonly BigInteger Bit53 = BigInteger.One << 53;
}
