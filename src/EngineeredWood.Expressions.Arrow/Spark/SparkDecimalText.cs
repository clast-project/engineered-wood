// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Reads a decimal out of a string exactly, across the whole of Spark's precision range.
/// </summary>
/// <remarks>
/// <para>
/// Spark decimals reach precision 38, past <see cref="decimal"/>'s ~7.9e28, so this parses to the
/// unscaled integer. <see cref="decimal"/> would be wrong inside its range too:
/// <c>decimal.TryParse</c> silently rounds a string of more than 28 significant digits, which
/// makes <c>CAST('1.0000000000000000000000000000001' AS DECIMAL(38,31))</c> 1 and
/// <c>CAST('5e-39' AS DECIMAL(38,38))</c> 0 where Spark rounds to 1e-38. The rules below come from
/// the <c>string-to-decimal</c> group of <c>Fixtures/spark-expression-corpus.json</c>.
/// </para>
/// <para>
/// Three failures, three error classes:
/// </para>
/// <list type="table">
/// <item>
///   <term>not a number</term>
///   <description><c>CAST_INVALID_INPUT</c> — <c>'abc'</c>, <c>''</c>, <c>'1,000'</c>.</description>
/// </item>
/// <item>
///   <term>more than 38 integral digits</term>
///   <description>
///     <c>NUMERIC_OUT_OF_SUPPORTED_RANGE</c>, which no other cast reports. It is a property of
///     the string and not of the target: 39 nines and <c>'1e39'</c> both report it against
///     <c>DECIMAL(38,0)</c>, while a 30-digit string against <c>DECIMAL(10,0)</c> reports the
///     class below.
///   </description>
/// </item>
/// <item>
///   <term>does not fit the target</term>
///   <description><c>NUMERIC_VALUE_OUT_OF_RANGE</c>, as every other cast to a decimal does.</description>
/// </item>
/// </list>
/// <para>
/// Rounding is HALF_UP, as on the rest of the decimal path: <c>'2.5'</c> to scale 0 is 3,
/// <c>'-2.5'</c> is -3, and <c>'1.45'</c> to scale 1 is 1.5.
/// </para>
/// <para>
/// Computed over <see cref="BigInteger"/> rather than <see cref="Int128"/> because, unlike the
/// operands of <see cref="SparkWideDecimals"/>, which come out of a 128-bit Arrow buffer, a string
/// has no bounded width: 38 integral digits against a scale-38 target need 77 digits before
/// rounding, and nothing stops a caller writing more. The result narrows to
/// <see cref="Int128"/> once the range check has proved it fits.
/// </para>
/// </remarks>
internal static class SparkDecimalText
{
    /// <summary>How a decimal string ended, when it did not end as a value.</summary>
    internal enum Result
    {
        /// <summary>Read, rounded to the target's scale, and inside the target's precision.</summary>
        Ok,

        /// <summary>Not a number Spark reads at all. Reported as <c>CAST_INVALID_INPUT</c>.</summary>
        Malformed,

        /// <summary>
        /// More integral digits than any Spark decimal has, whatever the target.
        /// Reported as <c>NUMERIC_OUT_OF_SUPPORTED_RANGE</c>.
        /// </summary>
        TooManyDigits,

        /// <summary>
        /// A number Spark reads, but not one the target type holds.
        /// Reported as <c>NUMERIC_VALUE_OUT_OF_RANGE</c>.
        /// </summary>
        OutOfRange,
    }

    private static readonly BigInteger Ten = new(10);

    private static readonly BigInteger LowWordMask = new(ulong.MaxValue);

    /// <summary>
    /// Reads <paramref name="text"/> as an unscaled integer at <paramref name="target"/>'s scale.
    /// </summary>
    internal static Result TryRead(string text, Decimal128Type target, out Int128 unscaled)
    {
        unscaled = default;

        if (!TrySplit(text, out var digits, out var negative, out var exponent))
            return Result.Malformed;

        // Spark's own check, on the string rather than the target: `numDigitsInIntegralPart` is a
        // BigDecimal's precision less its scale, which here is the length of `digits` plus the
        // exponent. Zero counts as one digit because a BigDecimal holding zero has precision 1,
        // so '0E40' reports too many digits and '0E-40' does not.
        var significant = digits.Length == 0 ? 1 : digits.Length;
        if (significant + exponent > SparkNumericTypes.MaxPrecision)
            return Result.TooManyDigits;

        var mantissa = digits.Length == 0
            ? BigInteger.Zero
            : BigInteger.Parse(digits, NumberStyles.None, CultureInfo.InvariantCulture);

        if (negative)
            mantissa = -mantissa;

        var shift = target.Scale + exponent;
        BigInteger result;

        if (shift >= 0)
        {
            // Bounded: the check above leaves the exponent at most 38 - significant, and a scale is
            // at most 38, so the power is at most 10^75 however long the input was.
            result = mantissa * BigInteger.Pow(Ten, (int)shift);
        }
        else if (-shift > significant + 1)
        {
            // Everything, including the digit that would decide the rounding, is below the
            // target's last place, so the answer is zero without forming a divisor that a huge
            // negative exponent would make absurd.
            result = BigInteger.Zero;
        }
        else
        {
            var divisor = BigInteger.Pow(Ten, (int)-shift);
            result = BigInteger.DivRem(mantissa, divisor, out var remainder);

            // Half away from zero, so the sign comes from the mantissa rather than the quotient:
            // -0.5 at scale 0 is -1, where the quotient alone is 0 and carries no sign.
            if (BigInteger.Abs(remainder) * 2 >= divisor)
                result += mantissa.Sign;
        }

        if (BigInteger.Abs(result) >= BigInteger.Pow(Ten, target.Precision))
            return Result.OutOfRange;

        unscaled = ToInt128(result);
        return Result.Ok;
    }

    /// <summary>
    /// Splits decimal text into its unscaled digits, sign and base-10 exponent.
    /// </summary>
    /// <remarks>
    /// This is Java's <c>BigDecimal</c> grammar, which Spark hands the string to, and not .NET's
    /// <c>NumberStyles.Float</c>: Spark accepts a trailing point (<c>'42.'</c>), a leading point
    /// (<c>'.5'</c>), an explicit plus and surrounding space, and refuses a thousands separator, an
    /// empty string and the words .NET's parser reads as infinities and NaN.
    /// <para>
    /// The digits are Unicode-wide (see <see cref="DigitValue"/>) but the structure is not: the
    /// sign, the point and the exponent marker must be ASCII. An ARABIC DECIMAL SEPARATOR
    /// (U+066B), a FULLWIDTH FULL STOP (U+FF0E), a MINUS SIGN (U+2212), a FULLWIDTH PLUS SIGN
    /// (U+FF0B) and a FULLWIDTH LATIN CAPITAL LETTER E (U+FF25) are all CAST_INVALID_INPUT.
    /// Scripts may be mixed: an ARABIC-INDIC DIGIT THREE beside a DEVANAGARI DIGIT THREE reads
    /// as 33.
    /// </para>
    /// </remarks>
    private static bool TrySplit(string text, out string digits, out bool negative, out long exponent)
    {
        digits = string.Empty;
        negative = false;
        exponent = 0;

        var (start, end) = SparkText.TrimBounds(text.AsSpan());
        var i = start;

        if (i < end && (text[i] == '+' || text[i] == '-'))
        {
            negative = text[i] == '-';
            i++;
        }

        var integerStart = i;
        while (i < end && DigitValue(text[i]) >= 0) i++;
        var integerEnd = i;

        var fractionStart = i;
        var fractionEnd = i;
        if (i < end && text[i] == '.')
        {
            i++;
            fractionStart = i;
            while (i < end && DigitValue(text[i]) >= 0) i++;
            fractionEnd = i;
        }

        // At least one digit somewhere, so '.', '+' and '' are all refused.
        var fractionLength = fractionEnd - fractionStart;
        if (integerEnd - integerStart + fractionLength == 0)
            return false;

        long parsedExponent = 0;
        if (i < end && (text[i] == 'e' || text[i] == 'E'))
        {
            i++;
            var negativeExponent = false;
            if (i < end && (text[i] == '+' || text[i] == '-'))
            {
                negativeExponent = text[i] == '-';
                i++;
            }

            // The exponent takes the wide digit set too; only its marker and sign are ASCII-bound.
            // '1e<ARABIC-INDIC THREE>' to DECIMAL(10,2) is 1000.00.
            var exponentStart = i;
            long value = 0;
            int exponentDigit;
            while (i < end && (exponentDigit = DigitValue(text[i])) >= 0)
            {
                // Stops accumulating once past `int`, so a long exponent cannot wrap; it is
                // refused just below, as Java refuses an exponent outside `int`.
                if (value <= int.MaxValue)
                    value = (value * 10) + exponentDigit;

                i++;
            }

            if (i == exponentStart || value > int.MaxValue)
                return false;

            parsedExponent = negativeExponent ? -value : value;
        }

        // Trailing junk. Everything Spark accepts has been consumed by here.
        if (i != end)
            return false;

        digits = StripLeadingZeros(text, integerStart, integerEnd, fractionStart, fractionEnd);
        exponent = parsedExponent - fractionLength;
        return true;
    }

    /// <summary>
    /// The unscaled digit string: the integer and fraction digits joined, without leading zeros.
    /// </summary>
    /// <remarks>
    /// Leading zeros go because they are not part of a BigDecimal's precision, which the digit
    /// check reads; trailing zeros stay because they are: <c>1.00</c> has an unscaled value of 100
    /// and a precision of 3. An all-zero value returns the empty string and is counted as one
    /// digit at the call site.
    /// <para>
    /// Digits are normalised to ASCII on the way in, because <c>BigInteger.Parse</c> under
    /// <see cref="NumberStyles.None"/> and the invariant culture reads only <c>[0-9]</c>. For the
    /// same reason a leading zero is recognised by value, so an ARABIC-INDIC DIGIT ZERO counts.
    /// </para>
    /// </remarks>
    private static string StripLeadingZeros(
        string text, int integerStart, int integerEnd, int fractionStart, int fractionEnd)
    {
        var joined = new char[(integerEnd - integerStart) + (fractionEnd - fractionStart)];
        var length = 0;

        for (var i = integerStart; i < integerEnd; i++)
        {
            var digit = DigitValue(text[i]);
            if (length == 0 && digit == 0) continue;
            joined[length++] = (char)('0' + digit);
        }

        for (var i = fractionStart; i < fractionEnd; i++)
        {
            var digit = DigitValue(text[i]);
            if (length == 0 && digit == 0) continue;
            joined[length++] = (char)('0' + digit);
        }

        return new string(joined, 0, length);
    }

    /// <summary>
    /// The value of <paramref name="c"/> as a decimal digit, or -1 when it is not one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Not only <c>[0-9]</c>: Spark hands the string to Java's <c>BigDecimal</c>, which reads
    /// whatever <c>Character.digit(c, 10)</c> reads — every BMP character in Unicode category Nd —
    /// so an ARABIC-INDIC DIGIT THREE (U+0663) cast to <c>DECIMAL(10,0)</c> is 3.
    /// </para>
    /// <para>
    /// The decimal target is the only one that reads the wide set, which is why it lives here
    /// rather than in the shared parse. INT, BIGINT, SMALLINT, TINYINT, DOUBLE and FLOAT all answer
    /// CAST_INVALID_INPUT for the same string, because their parses (<c>UTF8String.toLong</c> and
    /// <c>Double.parseDouble</c>) compare against <c>'0'</c>..<c>'9'</c>. So
    /// <see cref="SparkIntegralCasts.Classify"/> keeps its ASCII digit test on purpose.
    /// </para>
    /// <para>
    /// <see cref="CharUnicodeInfo.GetDecimalDigitValue(char)"/> is the same set: every BMP
    /// character each accepts is the identical 370 characters with identical values on JDK 17,
    /// .NET 10 and .NET Framework 4.7.2, so the netstandard2.0 build does not read a narrower
    /// Unicode table.
    /// </para>
    /// <para>
    /// Taking a <see cref="char"/> rather than a code point matches Java: BigDecimal walks UTF-16
    /// units, so U+1D7D1 MATHEMATICAL BOLD DIGIT THREE — which <c>Character.isDigit(int)</c>
    /// accepts — is CAST_INVALID_INPUT, and each half of its surrogate pair fails this test.
    /// </para>
    /// <para>
    /// The ASCII case is answered before the table lookup because this runs once per character per
    /// row and nearly every string is ASCII.
    /// </para>
    /// </remarks>
    private static int DigitValue(char c) =>
        c >= '0' && c <= '9' ? c - '0' : CharUnicodeInfo.GetDecimalDigitValue(c);

    /// <summary>Narrows a value the range check has already proved fits 128 bits.</summary>
    /// <remarks>
    /// Built from its two halves rather than converted: the netstandard2.0 build takes
    /// <see cref="Int128"/> from database-decimal's polyfill, which carries a smaller surface than
    /// the BCL type (as with <c>SparkWideDecimals.FromInt64</c>). The negative case is a two's
    /// complement by hand for the same reason.
    /// </remarks>
    private static Int128 ToInt128(BigInteger value)
    {
        var magnitude = BigInteger.Abs(value);
        var low = (ulong)(magnitude & LowWordMask);
        var high = (ulong)(magnitude >> 64);

        if (value.Sign >= 0)
            return new Int128(high, low);

        var negatedLow = unchecked(~low + 1);
        var negatedHigh = unchecked(~high + (negatedLow == 0 ? 1UL : 0UL));

        return new Int128(negatedHigh, negatedLow);
    }
}
