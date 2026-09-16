// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Printing a float or a double the way Spark prints it, which is the way Java does.
/// </summary>
/// <remarks>
/// <para>
/// Measured, and every row of the corpus's <c>float-to-string</c> group is exactly
/// <c>Double.toString</c> or <c>Float.toString</c>: Spark hands the value straight to Java. That
/// makes <c>CAST(&lt;a double&gt; AS STRING)</c> a different question from casting one to a
/// decimal, where only the value survives and the spelling does not. Three conventions have to
/// be reproduced, and .NET shares none of them:
/// </para>
/// <list type="bullet">
/// <item><description>
///   <b>Where scientific notation starts.</b> Java uses plain digits only while the magnitude is
///   in <c>[1e-3, 1e7)</c>: <c>1234567.0</c> prints plainly and <c>1.2345678E7</c> does not,
///   <c>0.001</c> prints plainly and <c>1.0E-4</c> does not. .NET switches elsewhere and would
///   print <c>12345678</c> and <c>0.0001</c>.
/// </description></item>
/// <item><description>
///   <b>There is always a digit after the point.</b> <c>1.0</c>, <c>1234567.0</c>, <c>1.0E30</c>
///   — where .NET prints <c>1</c>, <c>1234567</c> and <c>1E+30</c>.
/// </description></item>
/// <item><description>
///   <b>The exponent carries no sign and no padding when positive.</b> <c>E30</c> and
///   <c>E-4</c>, against .NET's <c>E+30</c> and <c>E-04</c>.
/// </description></item>
/// </list>
/// <para>
/// <b>A float prints as a float.</b> <c>Float.toString(0.3333333f)</c> is <c>0.3333333</c>, not
/// the widened double's <c>0.3333333134651184</c> — which is the opposite of the cast to a
/// decimal, where the widened double is exactly what Spark converts. The two widths therefore
/// decompose separately, even though one routine picks the digits for both.
/// </para>
/// <para>
/// The JDK band from #244 reaches here too, because this is the same <c>Double.toString</c>: the
/// digits are the shortest round-trip form on JDK 19 and later, and can be one longer before it.
/// The corpus records <c>java_version</c> beside <c>conf</c>, and the two rows that land in the
/// band are declared differences.
/// </para>
/// <para>
/// <b>Nothing here asks the platform for a digit.</b> Not the formatter, not the parser — see
/// <see cref="ShortestDigits"/>. #288, #337, #338.
/// </para>
/// </remarks>
internal static class SparkFloatText
{
    /// <summary>The exponent every subnormal double is written at: <c>double.Epsilon</c> is 2^-1074.</summary>
    private const int DoubleMinExponent = -1074;

    /// <summary>The exponent every subnormal float is written at: <c>float.Epsilon</c> is 2^-149.</summary>
    private const int FloatMinExponent = -149;

    /// <summary>Seventeen significant digits always identify a double, and nine always a float.</summary>
    private const int DoubleMaxDigits = 17;

    private const int FloatMaxDigits = 9;

    private static CultureInfo Invariant => CultureInfo.InvariantCulture;

    /// <summary>Java's rendering of a double.</summary>
    internal static string Render(double value)
    {
        if (double.IsNaN(value)) return "NaN";
        if (double.IsPositiveInfinity(value)) return "Infinity";
        if (double.IsNegativeInfinity(value)) return "-Infinity";

        return ShortestRoundTrip(value);
    }

    /// <summary>Java's rendering of a float, whose digits are the float's own and not the double's.</summary>
    internal static string Render(float value)
    {
        if (float.IsNaN(value)) return "NaN";
        if (float.IsPositiveInfinity(value)) return "Infinity";
        if (float.IsNegativeInfinity(value)) return "-Infinity";

        return ShortestRoundTrip(value);
    }

    /// <summary>
    /// The shortest decimal text that round-trips <paramref name="value"/>, in Java's shape.
    /// </summary>
    /// <remarks>
    /// <b>Deliberately not <c>ToString("R")</c></b>, which is the shortest form only on .NET Core
    /// — and not even reliably there: <c>(2^-25).ToString("R")</c> is
    /// <c>2.980232238769531E-08</c>, which reads back as a DIFFERENT double. It is also
    /// deliberately not a ladder of <c>G15</c>/<c>G16</c>/<c>G17</c> round-trip probes, which is
    /// what this was until #337 and #338; see <see cref="ShortestDigits"/> for why both halves of
    /// that had to go.
    /// <para>
    /// This is also what Spark's <c>BigDecimal.valueOf(d)</c> reads — up to the JVM's own version
    /// of the question, since <c>Double.toString</c> did not produce the shortest form before
    /// JDK 19. See <c>SparkFunctionRegistry.CastFloatingToDecimal</c> and #244.
    /// </para>
    /// </remarks>
    internal static string ShortestRoundTrip(double value)
    {
        var bits = BitConverter.DoubleToInt64Bits(value);
        var negative = bits < 0;
        var magnitude = bits & long.MaxValue;

        if (magnitude == 0)
            return negative ? "-0.0" : "0.0";

        // 11 exponent bits over 52 mantissa bits, biased by 1023 and written with an implicit
        // leading one: the value is `mantissa x 2^(rawExponent - 1023 - 52)`.
        var rawExponent = (int)(magnitude >> 52);
        var rawMantissa = magnitude & 0xF_FFFF_FFFF_FFFFL;

        var subnormal = rawExponent == 0;
        var mantissa = subnormal ? rawMantissa : rawMantissa | (1L << 52);
        var exponent = subnormal ? DoubleMinExponent : rawExponent - 1075;

        return Format(
            negative,
            ShortestDigits(mantissa, exponent, NarrowBelow(rawExponent, rawMantissa), DoubleMaxDigits));
    }

    /// <summary>The shortest decimal text that round-trips a float, which needs at most nine digits.</summary>
    internal static string ShortestRoundTrip(float value)
    {
        // Through the bits rather than through `(double)value`, because the question is which
        // FLOATS a decimal can land between: the widened double's neighbours are 2^29 times closer.
        var bits = SingleToInt32Bits(value);
        var negative = bits < 0;
        var magnitude = bits & int.MaxValue;

        if (magnitude == 0)
            return negative ? "-0.0" : "0.0";

        var rawExponent = magnitude >> 23;
        var rawMantissa = magnitude & 0x7F_FFFF;

        var subnormal = rawExponent == 0;
        var mantissa = subnormal ? rawMantissa : rawMantissa | (1 << 23);
        var exponent = subnormal ? FloatMinExponent : rawExponent - 150;

        return Format(
            negative,
            ShortestDigits(mantissa, exponent, NarrowBelow(rawExponent, rawMantissa), FloatMaxDigits));
    }

    /// <summary>
    /// Whether the gap below the value is half the gap above it, which only a power of two has.
    /// </summary>
    /// <remarks>
    /// <b>This is #337.</b> A value whose mantissa bits are all zero sits at the bottom of its
    /// binade, so its predecessor comes from the binade below where the step is half as long. Its
    /// rounding interval is therefore <c>[v - ulp/4, v + ulp/2]</c> and not
    /// <c>[v - ulp/2, v + ulp/2]</c> — and a lopsided interval can contain a k-digit decimal
    /// while excluding the CLOSEST k-digit decimal, which is the one a rounding ladder tests.
    /// Measured: 46 of the 2,046 normal double powers of two and 3 of the 254 normal float ones
    /// rendered one digit longer than Java's for exactly that reason. A non-zero mantissa makes
    /// the interval symmetric, so nothing else can be affected, and a sweep of all 2,130,706,432
    /// normal floats turns up no case that is not a power of two.
    /// <para>
    /// The smallest normal value is the exception the raw exponent catches: its predecessor is the
    /// largest subnormal, one ordinary step below, so its interval is symmetric after all.
    /// </para>
    /// </remarks>
    private static bool NarrowBelow(int rawExponent, long rawMantissa) =>
        rawMantissa == 0 && rawExponent > 1;

    /// <summary>
    /// The shortest decimal that reads back as <c>mantissa x 2^exponent</c>, by exact arithmetic.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Neither the platform's formatter nor its parser may be asked any part of this.</b> Both
    /// are wrong on .NET Framework and right on .NET Core, which is the one thing a cast may not
    /// depend on. Measured: <c>double.Parse("2.12E-322")</c> returns the wrong double there, so a
    /// round-trip probe answers falsely and a ladder stops a rung early; <c>ToString("G16")</c> of
    /// bits 3109743661010044618 ends <c>...729E-101</c> against .NET Core's <c>...728E-101</c>, so
    /// the digits it hands over are not the value's. Over a 200,000-value sweep against JDK 21 the
    /// old ladder disagreed with itself across target frameworks on 6,428 doubles and 5,957
    /// floats. #338.
    /// </para>
    /// <para>
    /// So the digits are computed. Writing the value as <c>mantissa x scale x 10^shift</c> —
    /// <c>scale = 5^-exponent, shift = exponent</c> when the exponent is negative, and
    /// <c>scale = 2^exponent, shift = 0</c> when it is not — makes <c>mantissa x scale</c> an
    /// integer whose digits ARE the value's, exactly and in full. One big multiplication per
    /// value, and no division anywhere.
    /// </para>
    /// <para>
    /// <b>Both neighbours are tested at every length, not just the rounded one.</b> That is the
    /// other half of #337: rounding to k digits finds the CLOSEST k-digit decimal, which is the
    /// right candidate only while the rounding interval is symmetric. Truncating gives the
    /// k-digit decimal below and incrementing gives the one above, and those two are the only
    /// candidates there can be at that length — so asking about both settles the length whatever
    /// shape the interval has.
    /// </para>
    /// <para>
    /// Where both read back, Java takes the closer, and the even significand where they are
    /// equally close. That is the JDK 19+ <c>Double.toString</c> specification, and it is also why
    /// <c>Double.toString(Double.MIN_VALUE)</c> is <c>4.9E-324</c> rather than <c>5E-324</c>: at a
    /// length of one the javadoc widens the field to the one- AND two-digit decimals before
    /// choosing, which <see cref="TwoDigitsCanBeatOne"/> below reproduces.
    /// </para>
    /// </remarks>
    private static (string Digits, int PointAt) ShortestDigits(
        long mantissa, int exponent, bool narrowBelow, int maxDigits)
    {
        var scale = Powers.Scale(exponent);
        var exact = (scale * mantissa).ToString(Invariant);

        // The product is the digit string of `0.<exact> x 10^pointAt`, with no leading zero to
        // discard: BigInteger does not write one.
        var pointAt = exponent < 0 ? exact.Length + exponent : exact.Length;

        var interval = new ReadsBack(scale, narrowBelow, mantissa % 2 == 0);

        for (var length = 1; length < maxDigits; length++)
        {
            if (!interval.TryChoose(exact, length, out var candidate))
                continue;

            if (length == 1 && TwoDigitsCanBeatOne(interval, exact, ref candidate))
                return Trim(candidate, pointAt);

            return Trim(candidate, pointAt);
        }

        // The closest decimal of maxDigits digits is within half a step of the value by
        // construction, so the last rung always reads back and needs no test.
        interval.TryChoose(exact, maxDigits, out var last);
        return Trim(last, pointAt);
    }

    /// <summary>
    /// Java's one exception to shortest-wins, applied where a single digit already round-trips.
    /// </summary>
    /// <remarks>
    /// The javadoc defines the field of candidates as the decimals of minimal length p, EXCEPT
    /// that when p is 1 it takes those of length 1 and 2 together and picks the closest of all of
    /// them. It is the difference between <c>9.9E-324</c> and <c>1.0E-323</c> for the double just
    /// above the smallest, both of which read back.
    /// <para>
    /// The two-digit answer is always available when a one-digit one is: every one-digit decimal
    /// sits on the two-digit grid as well, the nearest two-digit decimal is therefore at least as
    /// close to the value, and the interval is convex — so if the one-digit decimal is inside it,
    /// anything between it and the value is too. The call below cannot fail; it is written as a
    /// test rather than an assertion so that a future width with a different grid cannot trip on it.
    /// </para>
    /// </remarks>
    private static bool TwoDigitsCanBeatOne(in ReadsBack interval, string exact, ref Candidate candidate)
    {
        if (!interval.TryChoose(exact, 2, out var pair))
            return false;

        candidate = pair;
        return true;
    }

    /// <summary>A decimal of a chosen length, and whether carrying moved the point one place right.</summary>
    /// <remarks>
    /// Incrementing 999 gives 1000, which is the four-digit spelling of a three-digit grid point:
    /// the digits are 100 and the value is ten times what the same digits meant before. Carrying
    /// the shift here rather than re-deriving it keeps <see cref="Trim"/> a pure string operation.
    /// </remarks>
    private readonly struct Candidate(string digits, int pointShift)
    {
        internal string Digits { get; } = digits;

        internal int PointShift { get; } = pointShift;
    }

    /// <summary>Drops the trailing zeros Java does not count, and places the point.</summary>
    /// <remarks>
    /// A trailing zero is not a significant digit, and Java counts the length of a decimal without
    /// one: <c>1.0E-310</c> is one digit long, not two.
    /// </remarks>
    private static (string Digits, int PointAt) Trim(in Candidate candidate, int pointAt) =>
        (candidate.Digits.TrimEnd('0'), pointAt + candidate.PointShift);

    /// <summary>
    /// Which decimals read back as one value, as an exact test a candidate can be put to.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A candidate reads back when it sits within half a step of the value, and the arithmetic
    /// stays small because the candidates are measured against the value's own exact expansion
    /// rather than against the value. Writing the expansion as <c>0.&lt;exact&gt; x 10^pointAt</c>,
    /// its last place is worth <c>10^(pointAt - exact.Length)</c>, and in THOSE units half a step
    /// up is exactly <c>scale / 2</c> whichever sign the exponent had:
    /// </para>
    /// <code>
    ///     exponent &lt; 0:  2^(e-1) / 10^e = 2^(e-1) x 2^-e x 5^-e = 5^-e / 2 = scale / 2
    ///     exponent >= 0:  2^(e-1) / 10^0 =                          2^e  / 2 = scale / 2
    /// </code>
    /// <para>
    /// So the whole test is a comparison against the same <c>scale</c> the digits were built from
    /// — no second big power, and nothing per candidate but the tail it discards. Everything is
    /// multiplied by four so that the quarter-step below a power of two stays an integer.
    /// </para>
    /// <para>
    /// The boundary itself counts only for an even mantissa, which is where round-half-to-even
    /// sends a decimal landing exactly between two values. It is the same rule on both sides: at a
    /// power of two the value's mantissa is even and its predecessor's is odd, so the midpoint
    /// below belongs to the value.
    /// </para>
    /// </remarks>
    private readonly struct ReadsBack
    {
        private readonly BigInteger _above;
        private readonly BigInteger _below;
        private readonly bool _boundaryCounts;

        internal ReadsBack(BigInteger scale, bool narrowBelow, bool evenMantissa)
        {
            _above = 2 * scale;
            _below = narrowBelow ? scale : _above;
            _boundaryCounts = evenMantissa;
        }

        /// <summary>
        /// The decimal of <paramref name="length"/> digits that Java would choose, if any reads back.
        /// </summary>
        internal bool TryChoose(string exact, int length, out Candidate candidate)
        {
            // The expansion already fits, so this length holds the value itself and nothing is
            // discarded: no neighbour can be closer than a distance of zero.
            if (exact.Length <= length)
            {
                candidate = new Candidate(exact, 0);
                return true;
            }

            var head = exact.Substring(0, length);
            var tail = exact.Substring(length);

            // What truncating discarded, and what incrementing would overshoot by: the two sum to
            // one unit of the chosen length's last place.
            var down = BigInteger.Parse(tail, NumberStyles.None, Invariant);
            var up = TensComplement(tail);

            var downReads = Within(down, _below);
            var upReads = Within(up, _above);

            if (!downReads && !upReads)
            {
                candidate = default;
                return false;
            }

            if (downReads != upReads)
            {
                candidate = downReads ? new Candidate(head, 0) : Increment(head);
                return true;
            }

            // Both read back, so Java takes the closer of them — and the even significand where
            // they are equally close, which a terminating expansion like this one can genuinely
            // reach.
            var comparison = down.CompareTo(up);
            var keepHead = comparison < 0 || (comparison == 0 && (head[length - 1] - '0') % 2 == 0);

            candidate = keepHead ? new Candidate(head, 0) : Increment(head);
            return true;
        }

        private bool Within(BigInteger distance, BigInteger half)
        {
            var scaled = 4 * distance;
            var comparison = scaled.CompareTo(half);

            return comparison < 0 || (comparison == 0 && _boundaryCounts);
        }
    }

    /// <summary>
    /// <c>10^n - value</c> for the n-digit <paramref name="tail"/>, without forming <c>10^n</c>.
    /// </summary>
    /// <remarks>
    /// The nines complement plus one, which is the same number: <c>10^n - 1</c> is n nines, so
    /// subtracting each digit from nine and adding one lands on it. It matters because n reaches
    /// 750 for a subnormal double and a power of ten that long would be a per-candidate cost,
    /// where this is a walk over a string that has already been built.
    /// </remarks>
    private static BigInteger TensComplement(string tail)
    {
        var complement = new char[tail.Length];

        for (var i = 0; i < tail.Length; i++)
            complement[i] = (char)('9' - (tail[i] - '0'));

        return BigInteger.Parse(new string(complement), NumberStyles.None, Invariant) + BigInteger.One;
    }

    /// <summary>The next decimal up at the same length, carrying into a shifted point if it must.</summary>
    private static Candidate Increment(string digits)
    {
        var carried = digits.ToCharArray();

        for (var i = carried.Length - 1; i >= 0; i--)
        {
            if (carried[i] != '9')
            {
                carried[i]++;
                return new Candidate(new string(carried), 0);
            }

            carried[i] = '0';
        }

        // Every digit was a nine, so the answer is a one followed by the zeros already written —
        // one digit too many for this length, and the point has moved.
        return new Candidate("1" + new string(carried, 0, carried.Length - 1), 1);
    }

    /// <summary>
    /// The powers the digits are built from, held on first use rather than at type load.
    /// </summary>
    /// <remarks>
    /// <c>5^1074</c> is a 751-digit number and only a subnormal double wants it, so the table is
    /// held by a nested class: a caller that never renders one never builds it, which a plain
    /// <c>static readonly</c> on <see cref="SparkFloatText"/> could not promise. The same reason
    /// <see cref="SparkIntegralCasts"/> holds its powers of ten.
    /// <para>
    /// Boxed rather than a <c>BigInteger[]</c> because the slots are published without a lock: a
    /// reference assignment is atomic where a two-field struct is not, so a reader can never see a
    /// sign paired with another power's digits. Two threads racing on a cold slot compute the same
    /// number and one of them wins, which costs nothing and is always correct.
    /// </para>
    /// </remarks>
    private static class Powers
    {
        private static readonly object?[] Fives = new object?[-DoubleMinExponent + 1];

        /// <summary>
        /// <c>5^-exponent</c> below zero and <c>2^exponent</c> at or above it.
        /// </summary>
        /// <remarks>
        /// Only the fives are worth a table. A power of two is a shift, which BigInteger does in
        /// one pass over a buffer it has to allocate anyway.
        /// </remarks>
        internal static BigInteger Scale(int exponent)
        {
            if (exponent >= 0)
                return BigInteger.One << exponent;

            var index = -exponent;
            if (Fives[index] is BigInteger cached)
                return cached;

            var computed = BigInteger.Pow(5, index);
            Fives[index] = computed;

            return computed;
        }
    }

    /// <summary>
    /// Re-spells the chosen digits in Java's shape.
    /// </summary>
    /// <remarks>
    /// The digits are already decided by the time this runs; all that is left is where the point
    /// goes and whether an exponent is written. Working from the digits rather than from the value
    /// keeps this one function for both widths.
    /// </remarks>
    private static string Format(bool negative, (string Digits, int PointAt) value)
    {
        var (digits, pointAt) = value;
        var sign = negative ? "-" : string.Empty;

        if (digits.Length == 0)
            return sign + "0.0";

        // Java's own boundary: plain digits while the magnitude is in [1e-3, 1e7). `pointAt` is
        // the exponent of the value written as 0.<digits>, so 1e-3 is 0.1e-2 and 9999999 is
        // 0.9999999e7 — the last values on each side that print plainly.
        if (pointAt is >= -2 and <= 7)
            return sign + Plain(digits, pointAt);

        var rest = digits.Length > 1 ? digits.Substring(1) : "0";
        return $"{sign}{digits[0]}.{rest}E{(pointAt - 1).ToString(Invariant)}";
    }

    private static string Plain(string digits, int pointAt)
    {
        var builder = new StringBuilder(digits.Length + 4);

        if (pointAt <= 0)
        {
            builder.Append("0.");
            builder.Append('0', -pointAt);
            builder.Append(digits);
            return builder.ToString();
        }

        if (pointAt >= digits.Length)
        {
            builder.Append(digits);
            builder.Append('0', pointAt - digits.Length);
            builder.Append(".0");
            return builder.ToString();
        }

        builder.Append(digits, 0, pointAt);
        builder.Append('.');
        builder.Append(digits, pointAt, digits.Length - pointAt);
        return builder.ToString();
    }

    /// <summary>
    /// A float's bits, by hand because netstandard2.0 has no <c>BitConverter.SingleToInt32Bits</c>.
    /// </summary>
    private static int SingleToInt32Bits(float value)
    {
#if NETSTANDARD2_0
        return BitConverter.ToInt32(BitConverter.GetBytes(value), 0);
#else
        return BitConverter.SingleToInt32Bits(value);
#endif
    }
}
