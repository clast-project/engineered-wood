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
/// Casting a string to a decimal used to go through <see cref="decimal"/>, which stops near
/// 7.9e28 where Spark decimals reach precision 38. After #131 every other route to a wide decimal
/// — arithmetic, casts from a decimal or an integer, unification, equality — was on the unscaled
/// integer already, and this was the one that was not, because what Spark does with an over-long
/// string was unmeasured. #174 measured it; the <c>string-to-decimal</c> group of
/// <c>Fixtures/spark-expression-corpus.json</c> is the record, and every rule below comes from
/// there rather than from reasoning about Spark's integer behaviour.
/// </para>
/// <para>
/// <b>The ceiling was not only a refusal.</b> Two of the answers it got wrong were wrong VALUES,
/// not refusals: <c>decimal.TryParse</c> accepts a string carrying more than 28 significant
/// digits, silently rounds it, and reports success — so
/// <c>CAST('1.0000000000000000000000000000001' AS DECIMAL(38,31))</c> returned 1 exactly, and
/// <c>CAST('5e-39' AS DECIMAL(38,38))</c> returned 0 where Spark rounds to 1e-38. A magnitude
/// check alone would not have caught either, because both are inside <see cref="decimal"/>'s
/// range.
/// </para>
/// <para>
/// <b>Three failures, three error classes</b>, and the third was not guessable from the other two:
/// </para>
/// <list type="table">
/// <item>
///   <term>not a number</term>
///   <description><c>CAST_INVALID_INPUT</c> — <c>'abc'</c>, <c>''</c>, <c>'1,000'</c>.</description>
/// </item>
/// <item>
///   <term>more than 38 integral digits</term>
///   <description>
///     <c>NUMERIC_OUT_OF_SUPPORTED_RANGE</c>, which no other path in this library reaches. It is
///     a property of the STRING and not of the target: 39 nines and <c>'1e39'</c> both report it
///     against <c>DECIMAL(38,0)</c>, while a 30-digit string against <c>DECIMAL(10,0)</c> — far
///     wider than its target — reports the class below instead.
///   </description>
/// </item>
/// <item>
///   <term>does not fit the target</term>
///   <description><c>NUMERIC_VALUE_OUT_OF_RANGE</c>, as every other cast to a decimal does.</description>
/// </item>
/// </list>
/// <para>
/// Rounding is HALF_UP, agreeing with the rest of this path rather than diverging from it:
/// <c>'2.5'</c> to scale 0 is 3, <c>'-2.5'</c> is -3, and <c>'1.45'</c> to scale 1 is 1.5. All
/// measured, because the corpus's own rounding group exists precisely because half-up and
/// half-even disagree and only one of them is Spark's.
/// </para>
/// <para>
/// <b>Computed over <see cref="BigInteger"/> rather than <see cref="Int128"/>.</b> Unlike the
/// arithmetic in <see cref="SparkWideDecimals"/>, whose operands both came out of a 128-bit Arrow
/// buffer and so have a bounded intermediate, a string has no width at all: 38 integral digits
/// against a scale-38 target needs 77 digits before rounding, which is already at the edge of a
/// 256-bit mantissa, and nothing stops a caller writing more. The cost is irrelevant here — the
/// path is parsing text either way — and the result narrows to <see cref="Int128"/> once the
/// range check has proved it fits.
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

        // Spark's own check, and it is on the STRING: `numDigitsInIntegralPart` is a BigDecimal's
        // precision less its scale, which for the value this parse produced is the length of
        // `digits` plus the exponent. Zero counts as one digit, because a BigDecimal holding zero
        // has precision 1 — which is why '0E40' reports too many digits and '0E-40' does not.
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
            // Everything is below the target's last place, INCLUDING the digit that would decide
            // the rounding — so the answer is zero without forming a divisor that a huge negative
            // exponent would make absurd.
            result = BigInteger.Zero;
        }
        else
        {
            var divisor = BigInteger.Pow(Ten, (int)-shift);
            result = BigInteger.DivRem(mantissa, divisor, out var remainder);

            // Half AWAY FROM ZERO, so the sign comes from the mantissa rather than from the
            // quotient: -0.5 at scale 0 is -1, where the quotient alone is 0 and carries no sign.
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
    /// This is Java's <c>BigDecimal</c> grammar, which is what Spark hands the string to, and not
    /// .NET's <c>NumberStyles.Float</c>. They differ where it matters: measured, Spark accepts a
    /// trailing point (<c>'42.'</c>), a leading point (<c>'.5'</c>), an explicit plus and
    /// surrounding space, and refuses a thousands separator, an empty string and the words .NET's
    /// parser reads as infinities and NaN.
    /// </remarks>
    private static bool TrySplit(string text, out string digits, out bool negative, out long exponent)
    {
        digits = string.Empty;
        negative = false;
        exponent = 0;

        var (start, end) = TrimAscii(text);
        var i = start;

        if (i < end && (text[i] == '+' || text[i] == '-'))
        {
            negative = text[i] == '-';
            i++;
        }

        var integerStart = i;
        while (i < end && IsDigit(text[i])) i++;
        var integerEnd = i;

        var fractionStart = i;
        var fractionEnd = i;
        if (i < end && text[i] == '.')
        {
            i++;
            fractionStart = i;
            while (i < end && IsDigit(text[i])) i++;
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

            var exponentStart = i;
            long value = 0;
            while (i < end && IsDigit(text[i]))
            {
                // Clamped rather than overflowed. Java refuses an exponent outside `int`, and this
                // stops before it can wrap; the clamp is far enough out that no value survives the
                // digit check below either way.
                if (value <= int.MaxValue)
                    value = (value * 10) + (text[i] - '0');

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
    /// check reads; TRAILING zeros stay, because they are — <c>1.00</c> has an unscaled value of
    /// 100 and a precision of 3. An all-zero value returns the empty string and is counted as one
    /// digit at the call site.
    /// </remarks>
    private static string StripLeadingZeros(
        string text, int integerStart, int integerEnd, int fractionStart, int fractionEnd)
    {
        var joined = new char[(integerEnd - integerStart) + (fractionEnd - fractionStart)];
        var length = 0;

        for (var i = integerStart; i < integerEnd; i++)
        {
            if (length == 0 && text[i] == '0') continue;
            joined[length++] = text[i];
        }

        for (var i = fractionStart; i < fractionEnd; i++)
        {
            if (length == 0 && text[i] == '0') continue;
            joined[length++] = text[i];
        }

        return new string(joined, 0, length);
    }

    /// <summary>
    /// The bounds of <paramref name="text"/> with ASCII whitespace and control characters removed
    /// from both ends.
    /// </summary>
    /// <remarks>
    /// Spark trims with <c>UTF8String.trimAll</c>, which removes characters at or below the space,
    /// and nothing else. <c>string.Trim()</c> would also remove Unicode whitespace such as a
    /// non-breaking space, which Spark refuses — so trimming the way .NET does would accept a
    /// string Spark rejects.
    /// </remarks>
    private static (int Start, int End) TrimAscii(string text)
    {
        var start = 0;
        var end = text.Length;

        while (start < end && text[start] <= ' ') start++;
        while (end > start && text[end - 1] <= ' ') end--;

        return (start, end);
    }

    private static bool IsDigit(char c) => c >= '0' && c <= '9';

    /// <summary>Narrows a value the range check has already proved fits 128 bits.</summary>
    /// <remarks>
    /// Built from its two halves rather than converted, for the reason
    /// <c>SparkWideDecimals.FromInt64</c> gives: the netstandard2.0 build takes
    /// <see cref="Int128"/> from database-decimal's polyfill, which carries a smaller surface than
    /// the BCL type. The negative case is a two's complement by hand for the same reason.
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
