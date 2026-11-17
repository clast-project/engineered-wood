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
/// In this assembly rather than beside the Arrow cast, because <c>SparkLiteral</c> reads
/// exponent-bearing SQL literals too. Nothing in this type needs Arrow.
/// </para>
/// <para>
/// The reading counterpart of <c>SparkFloatText</c>, with the same goal: the answer must not
/// depend on which target framework loaded the library. The platform parse differs in two ways:
/// </para>
/// <list type="bullet">
/// <item><description>
/// .NET Framework refuses a value too large to represent, where .NET Core and Java return an
/// infinity. That matters beyond the double cast, because it feeds <c>CastInput.IsNumeric</c>,
/// which decides whether a string is coerced to a number or compared as text: without the
/// saturation, <c>s &gt; 1.0</c> over <c>'1e400'</c> would differ by framework. Underflow needs
/// nothing; <c>'1e-400'</c> is zero on both.
/// </description></item>
/// <item><description>
/// On the text it accepts, .NET Framework is not correctly rounded; see
/// <see cref="TryParseExact"/>.
/// </description></item>
/// </list>
/// </remarks>
internal static class SparkDoubleText
{
    /// <summary>
    /// Reads a double, saturating to an infinity where the magnitude is out of range.
    /// </summary>
    /// <remarks>
    /// The overflow branch is reached only when the platform parse has refused the text, which
    /// on .NET Core it never does for a number, and on .NET Framework only for one too large.
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
    /// a shape with no rounding in it, and the platform reads those exactly.
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
    /// Asked of the parser rather than of the digits, because only the parser can tell "too large"
    /// from "not a number" without re-reading the grammar. .NET Framework raises
    /// <see cref="OverflowException"/> for the first and <see cref="FormatException"/> for the
    /// second; .NET Core returns an infinity instead, so it reaches here only for malformed text.
    /// The sign comes from the text, because there is no value to take it from.
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
    /// .NET Framework's parser is not correctly rounded, even within its documented fifteen
    /// significant digits: over 100,000 random doubles re-read on net472, about 1% of the
    /// fifteen- and sixteen-digit forms came back as the adjacent double, with nothing raised.
    /// .NET Core (since 3.0) and Java's <c>Double.parseDouble</c> are correctly rounded.
    /// </para>
    /// <para>
    /// Compiled on every target but used only on netstandard2.0, so that the tests can check it
    /// against .NET Core's own parser.
    /// </para>
    /// <para>
    /// Exact rather than fast: the value <c>digits x 10^exponent</c> is a ratio of two integers,
    /// and the answer is the mantissa that ratio rounds to. Eisel–Lemire would do it in machine
    /// words (<c>SparkFloatScaling</c> has a suitable 128-bit power table), but its correctness
    /// argument is the hard part, and this runs only on the framework that is slower anyway.
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
    /// Not a double parse narrowed to float, which rounds twice and lands on the adjacent float
    /// whenever the first rounding reaches a tie the text was not on. .NET Core's
    /// <see cref="float"/> parse is correctly rounded and is used as it stands; .NET Framework's
    /// is not, so that build replaces its answer with the exact one, as
    /// <see cref="TryParse(string, out double)"/> does for a double. A magnitude too large is an
    /// infinity on every target.
    /// </para>
    /// </remarks>
    internal static bool TryParseSingle(string text, out float value)
    {
#if NETSTANDARD2_0
        // Whether the text is a number stays the platform's question, so the accepted shapes and
        // whitespace are the platform's; only the value is replaced. Unlike Correct, an infinite
        // answer is not exempted: the NaN and Infinity words are shapes TryScan declines, so they
        // keep the platform's reading anyway.
        if (float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
        {
            if (TryParseExactSingle(text, out var exact))
                value = exact;

            return true;
        }

        // .NET Framework refuses a float magnitude at or above the midpoint between float.MaxValue
        // and 2^128 -- including text just below that midpoint, which Java rounds down to
        // float.MaxValue. The double parse still decides acceptance, but its value cannot stand
        // in: narrowing it is the double rounding this reader exists to avoid, and text just
        // below the midpoint would read as the midpoint and then as infinity.
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
    /// The float counterpart of <see cref="TryParseExact"/>, compiled on every target for the
    /// same reason: so the tests can hold it against .NET Core's parser.
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
    /// because there is no rounding in it. Widening the grammar here would make this routine
    /// decide acceptance as well as the value.
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

        // Significant digits only, and no more than MaxSignificantDigits of them; see there. A
        // leading zero is not written, though one after the point still moves the exponent, and a
        // digit past the cap is not written either: one in the integer part moves the exponent the
        // other way, and any that is not zero leaves a sticky digit behind.
        var written = new StringBuilder(Math.Min(end - index, MaxSignificantDigits + 1));
        var fraction = 0L;
        var dropped = 0L;
        var sticky = false;
        var anyDigit = false;
        var seenPoint = false;

        for (; index < end; index++)
        {
            var c = text[index];

            if (c >= '0' && c <= '9')
            {
                anyDigit = true;

                if (written.Length == 0 && c == '0')
                {
                    if (seenPoint) fraction++;
                }
                else if (written.Length < MaxSignificantDigits)
                {
                    written.Append(c);
                    if (seenPoint) fraction++;
                }
                else
                {
                    if (!seenPoint) dropped++;
                    sticky |= c != '0';
                }
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

        if (!anyDigit)
            return false;

        // Standing in for everything dropped: it is not zero exactly when the dropped digits were
        // not, and it sits below every digit kept, which is all the rounding can see of them.
        if (sticky)
        {
            written.Append('1');
            fraction++;
        }

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

        // An all-zero significand writes nothing, and is zero.
        digits = written.Length == 0
            ? BigInteger.Zero
            : BigInteger.Parse(written.ToString(), NumberStyles.None, CultureInfo.InvariantCulture);

        // fraction and dropped are bounded by the text's length, and scaled is clamped, so the sum
        // is far inside a long; clamped into an int far past anything a double or float can reach.
        exponent = (int)Math.Max(-int.MaxValue, Math.Min(int.MaxValue, scaled - fraction + dropped));

        return true;
    }

    /// <summary>
    /// The most significant digits <see cref="TryScan"/> keeps, the rest standing behind one
    /// sticky digit.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Without a cap, every digit was parsed into one <see cref="BigInteger"/> and then counted,
    /// both quadratic in the length, and text like <c>0.1234…</c> built a power of ten as long as
    /// it. This is a per-row path on netstandard2.0, so one long numeric string in a column paid
    /// that on every row (#905).
    /// </para>
    /// <para>
    /// The cap loses nothing. Rounding to a double or a float asks only how the value compares with
    /// the representable values and the midpoints between them, and each of those is written with
    /// at most 768 significant digits (fast_float keeps 768 for the same reason; a float needs
    /// 113). Against any
    /// number of 800 significant digits or fewer, the value and its stand-in - the first 800 digits
    /// and, if anything after them was not zero, a 1 beneath them - compare the same way: they
    /// share the leading digit's position and those 800 digits, and differ only in whether there
    /// is anything more, which the sticky digit preserves.
    /// </para>
    /// </remarks>
    private const int MaxSignificantDigits = 800;

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

        if (exponent < -400 - ScaledDecimal.DigitCount(digits))
            return negative ? -0d : 0d;

        var numerator = exponent >= 0 ? digits * Powers.Ten(exponent) : digits;
        var denominator = exponent >= 0 ? BigInteger.One : Powers.Ten(-exponent);

        // Line the quotient up to 53 bits, then correct — the estimate is off by at most one.
        var binary = ScaledDecimal.BitLength(numerator) - ScaledDecimal.BitLength(denominator) - 53;

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

    /// <summary>
    /// The powers of ten the ratio is built from, held on first use rather than rebuilt per value.
    /// </summary>
    /// <remarks>
    /// This is a per-row path on netstandard2.0 — every string cast to a double reaches it — and a
    /// value with an exponent far from zero wants a power hundreds of digits long. It follows the
    /// nested <c>Powers</c> cache in <c>SparkFloatText</c>, including publishing each entry
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

    /// <summary>The integer part of <c>num / den / 2^binary</c>, with what is left over.</summary>
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
    /// short of the 53rd bit is a subnormal, whose exponent field is zero and whose bits are the
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

    /// <summary>The smallest exponent a double can be written at, which is every subnormal's.</summary>
    private const int MinExponent = -1074;

    /// <summary>The largest, above which the value is an infinity.</summary>
    private const int MaxExponent = 971;

    private static readonly BigInteger Bit52 = BigInteger.One << 52;

    private static readonly BigInteger Bit53 = BigInteger.One << 53;
}
