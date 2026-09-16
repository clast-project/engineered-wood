// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text;
using System.Threading;

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
    internal static string Render(double value) => ShortestRoundTrip(value);

    /// <summary>Java's rendering of a float, whose digits are the float's own and not the double's.</summary>
    internal static string Render(float value) => ShortestRoundTrip(value);

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
        // BEFORE the bits are read, not after: an all-ones exponent decodes as a perfectly
        // ordinary finite mantissa, so without this NaN renders as -2.696539702293474E308 and an
        // infinity as the largest double. The old ladder got the spellings from the platform's
        // formatter and so never had to say this; reading the bits means owning it.
        if (double.IsNaN(value)) return "NaN";
        if (double.IsPositiveInfinity(value)) return "Infinity";
        if (double.IsNegativeInfinity(value)) return "-Infinity";

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
        // The same non-finite guard the double overload needs, and for the same reason: exponent
        // 255 decodes as a finite value.
        if (float.IsNaN(value)) return "NaN";
        if (float.IsPositiveInfinity(value)) return "Infinity";
        if (float.IsNegativeInfinity(value)) return "-Infinity";

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
    /// value, and the answer is a prefix of it.
    /// </para>
    /// <para>
    /// <b>Both neighbours are tested at every length, not just the rounded one.</b> That is the
    /// other half of #337: rounding to k digits finds the CLOSEST k-digit decimal, which is the
    /// right candidate only while the rounding interval is symmetric. Truncating the expansion
    /// gives the k-digit decimal below and one more unit in its last place gives the one above;
    /// those two are the only candidates there can be at that length, so asking about both settles
    /// the length whatever shape the interval has.
    /// </para>
    /// <para>
    /// Where both read back, Java takes the closer, and the even significand where they are
    /// equally close.
    /// </para>
    /// <para>
    /// <b>A length of one is never asked for, and that is deliberate.</b> The JDK 19+
    /// specification widens the field to the one- AND two-digit decimals before choosing whenever
    /// a single digit would do, which is why <c>Double.toString(Double.MIN_VALUE)</c> is
    /// <c>4.9E-324</c> and not <c>5E-324</c>. Starting at two and letting <see cref="Trim"/> drop a
    /// trailing zero IS that rule: every one-digit decimal sits on the two-digit grid, so the
    /// nearer two-digit decimal is never further from the value, and the interval is convex — if
    /// the one-digit decimal is inside it then so is anything between that and the value. The
    /// answer comes back one digit long exactly when the closest two-digit decimal ends in a zero.
    /// </para>
    /// <para>
    /// <b>The length is found by bisection, which is what makes this affordable.</b> Reading back
    /// is monotone in length — a decimal that fits at k digits is still there at k+1 with a zero
    /// after it — so the shortest length can be bisected rather than walked up to. Four probes
    /// instead of up to seventeen, and each probe is one division rather than a digit's worth of
    /// bookkeeping. This runs for every row of every cast, which is what #337 and #338 both
    /// deferred over.
    /// </para>
    /// </remarks>
    private static (string Digits, int PointAt) ShortestDigits(
        long mantissa, int exponent, bool narrowBelow, int maxDigits)
    {
        // The same question in machine words, which answers it for all but a handful of values
        // and declines rather than guessing on those. Everything below is what it declines to.
        if (SparkFloatScaling.TryShortestDigits(
                mantissa, exponent, narrowBelow, maxDigits, out var fast, out var fastPoint))
        {
            return (fast, fastPoint);
        }

        return ExactDigits(mantissa, exponent, narrowBelow, maxDigits);
    }

    /// <summary>The same answer by exact expansion, for whatever the scaled path would not call.</summary>
    internal static (string Digits, int PointAt) ExactDigits(
        long mantissa, int exponent, bool narrowBelow, int maxDigits)
    {
        var scale = Powers.Scale(exponent);
        var product = scale * mantissa;
        var length = DigitCount(mantissa, exponent, product);

        // The product is the digit string of `0.<product> x 10^pointAt`.
        var pointAt = exponent < 0 ? length + exponent : length;

        var interval = new Interval(scale, exponent, narrowBelow, mantissa % 2 == 0);

        var shortest = 2;
        var longest = maxDigits;

        while (shortest < longest)
        {
            var middle = (shortest + longest) / 2;

            if (interval.Reads(product, length, middle))
                longest = middle;
            else
                shortest = middle + 1;
        }

        return Trim(interval.Choose(product, length, shortest), pointAt);
    }

    /// <summary>
    /// How many decimal digits <c>mantissa x scale</c> is written with.
    /// </summary>
    /// <remarks>
    /// From the logarithm of the factors rather than of the product, because the factors are a
    /// <c>long</c> and a power: <c>log10(m x 5^k)</c> is <c>log10 m + k log10 5</c>, and a double
    /// carries that to about fourteen places over the whole exponent range. The estimate is then
    /// corrected against the powers themselves, which matters because the product IS an exact
    /// power of ten for some values — <c>1.0</c> is <c>2^52 x 5^52</c>.
    /// <para>
    /// Asking <c>product.ToString().Length</c> instead would be exact, and would also be the
    /// single most expensive thing on this path: formatting the 767-digit expansion of a subnormal
    /// double costs more than every other step of the render put together, and at most seventeen
    /// of those digits are ever read.
    /// </para>
    /// </remarks>
    private static int DigitCount(long mantissa, int exponent, BigInteger product)
    {
        const double Log10Two = 0.30102999566398120;
        const double Log10Five = 0.69897000433601880;

        var logarithm = Math.Log10(mantissa)
            + (exponent < 0 ? -exponent * Log10Five : exponent * Log10Two);

        var count = (int)logarithm + 1;

        while (product >= Powers.Ten(count)) count++;
        while (count > 1 && product < Powers.Ten(count - 1)) count--;

        return count;
    }

    /// <summary>
    /// Which decimals read back as one value, as an exact test a length can be put to.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A candidate reads back when it sits within half a step of the value, and the arithmetic
    /// stays small because candidates are measured against the value's own exact expansion rather
    /// than against the value. Writing the expansion as <c>0.&lt;product&gt; x 10^pointAt</c>, its
    /// last place is worth <c>10^(pointAt - length)</c>, and in THOSE units half a step up is
    /// exactly <c>scale / 2</c> whichever sign the exponent had:
    /// </para>
    /// <code>
    ///     exponent &lt;  0:  2^(e-1) / 10^e = 2^(e-1) x 2^-e x 5^-e = 5^-e / 2 = scale / 2
    ///     exponent >= 0:  2^(e-1) / 10^0 =                          2^e  / 2 = scale / 2
    /// </code>
    /// <para>
    /// So the whole test comes back to the same <c>scale</c> the digits were built from — no
    /// second big power is ever formed, and nothing is needed per candidate but the tail it
    /// discards.
    /// </para>
    /// <para>
    /// <b>The halving is done once, to the bound, and not per candidate to the distance.</b>
    /// <c>4d &lt; scale</c> would be the direct spelling, but it allocates a shifted copy of a
    /// number that reaches 750 digits, and a probe would pay for two of them. Writing
    /// <c>scale = 4q + r</c> instead makes it <c>d &lt; q</c>, or <c>d == q</c> while <c>r</c> is
    /// non-zero — a comparison, which allocates nothing. Measured: the shift and the subtraction
    /// are the two most expensive operations on this path at 0.37us each against 0.008us for a
    /// comparison, and dropping the pair of shifts is most of what makes bisection worth having.
    /// </para>
    /// <para>
    /// The boundary itself counts only for an even mantissa, which is where round-half-to-even
    /// sends a decimal landing exactly between two values. It is the same rule on both sides: at a
    /// power of two the value's mantissa is even and its predecessor's is odd, so the midpoint
    /// below belongs to the value. It can only ever be reached from a non-negative exponent, where
    /// <c>scale</c> is a power of two; below zero <c>scale</c> is a power of five and no doubling
    /// of an integer can land on it.
    /// </para>
    /// </remarks>
    private readonly struct Interval
    {
        /// <summary>Half a step down, which is a QUARTER of a step at a power of two.</summary>
        private readonly BigInteger _below;

        /// <summary>Half a step up, which the value always has in full.</summary>
        private readonly BigInteger _above;

        /// <summary>Whether the halving was exact, so that the bound is reachable at all.</summary>
        private readonly bool _belowIsExact;

        private readonly bool _aboveIsExact;

        /// <summary>Whether a candidate exactly on the boundary rounds inwards.</summary>
        private readonly bool _boundaryCounts;

        internal Interval(BigInteger scale, int exponent, bool narrowBelow, bool evenMantissa)
        {
            // Off the table, because halving a 750-digit number costs as much as any other step
            // here and every value at a given exponent wants the same answer. The QUARTER is not
            // worth a row of its own: only a power of two asks for one, and there are 2,046 of
            // those against every double there is.
            _above = Powers.Half(exponent);
            _aboveIsExact = scale.IsEven;

            _below = narrowBelow ? scale >> 2 : _above;
            _belowIsExact = narrowBelow ? (scale & 3).IsZero : _aboveIsExact;

            _boundaryCounts = evenMantissa;
        }

        /// <summary>Whether any decimal of <paramref name="take"/> digits reads back as the value.</summary>
        internal bool Reads(BigInteger product, int length, int take)
        {
            // The expansion already fits, so this length holds the value itself.
            if (take >= length) return true;

            var place = Powers.Ten(length - take);
            var down = BigInteger.Remainder(product, place);

            // The subtraction is on the right of `||` so that it is skipped whenever truncating
            // already reads back, which is the common way out.
            return Within(down, _below, _belowIsExact)
                || Within(place - down, _above, _aboveIsExact);
        }

        /// <summary>The decimal of <paramref name="take"/> digits that Java would choose.</summary>
        internal Candidate Choose(BigInteger product, int length, int take)
        {
            if (take >= length)
                return new Candidate(product.ToString(Invariant), 0);

            var place = Powers.Ten(length - take);
            var head = BigInteger.DivRem(product, place, out var down).ToString(Invariant);
            var up = place - down;

            var downReads = Within(down, _below, _belowIsExact);
            var upReads = Within(up, _above, _aboveIsExact);

            if (downReads != upReads)
                return downReads ? new Candidate(head, 0) : Increment(head);

            // Both read back, so Java takes the closer of them — and the even significand where
            // they are equally close, which a terminating expansion can genuinely reach.
            var comparison = down.CompareTo(up);
            var keepHead = comparison < 0
                || (comparison == 0 && (head[head.Length - 1] - '0') % 2 == 0);

            return keepHead ? new Candidate(head, 0) : Increment(head);
        }

        /// <summary>
        /// Whether <paramref name="distance"/> is inside a bound that was rounded down to reach it.
        /// </summary>
        /// <remarks>
        /// Landing ON the bound means one of two things. If the halving that produced it threw
        /// digits away then the true bound is larger and the candidate is comfortably inside; if
        /// it was exact then the candidate is the midpoint itself, and only an even mantissa
        /// claims it.
        /// </remarks>
        private bool Within(BigInteger distance, BigInteger bound, bool boundIsExact)
        {
            var comparison = distance.CompareTo(bound);

            if (comparison != 0)
                return comparison < 0;

            return !boundIsExact || _boundaryCounts;
        }
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
    /// <para>
    /// <b>Through <see cref="Volatile"/> on both sides, though, and atomicity is not the reason.</b>
    /// A plain store publishes the reference with no ordering against the writes that filled the box,
    /// so a reader on a weakly-ordered target — arm64 — may follow a non-null slot to a
    /// <c>BigInteger</c> whose digit array it cannot yet see. The release on the write and the
    /// acquire on the read are what forbid that. Neither is measurable here: on x64 both are ordinary
    /// instructions the JIT merely declines to move, and a slot is written once however many values
    /// read it.
    /// </para>
    /// </remarks>
    private static class Powers
    {
        private static readonly object?[] Fives = new object?[-DoubleMinExponent + 1];

        private static readonly object?[] Tens = new object?[-DoubleMinExponent + 1];

        private static readonly object?[] Halves = new object?[-DoubleMinExponent + 1];

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
            if (Volatile.Read(ref Fives[index]) is BigInteger cached)
                return cached;

            var computed = BigInteger.Pow(5, index);
            Volatile.Write(ref Fives[index], computed);

            return computed;
        }

        /// <summary>
        /// Half of <see cref="Scale"/>, which is half a step in the expansion's own units.
        /// </summary>
        /// <remarks>
        /// A row of its own because every candidate at every length is compared against it, and
        /// for a subnormal double it is a 750-digit number that would otherwise be rebuilt once
        /// per value. Above zero the scale is a power of two and halving it is another shift of a
        /// small number, so nothing is stored.
        /// </remarks>
        internal static BigInteger Half(int exponent)
        {
            if (exponent >= 0)
                return exponent == 0 ? BigInteger.Zero : BigInteger.One << (exponent - 1);

            var index = -exponent;
            if (Volatile.Read(ref Halves[index]) is BigInteger cached)
                return cached;

            var computed = Scale(exponent) >> 1;
            Volatile.Write(ref Halves[index], computed);

            return computed;
        }

        /// <summary>
        /// <c>10^power</c>, for the place a candidate's last digit sits in.
        /// </summary>
        /// <remarks>
        /// Derived from the fives — <c>10^n</c> is <c>5^n</c> shifted left by n — but held in its
        /// own row, because bisection asks for four or five different places per value and the
        /// shift is a pass over a buffer it has to allocate each time.
        /// <para>
        /// The power asked for is at most the digit count of <c>mantissa x scale</c>, which is 767
        /// for a subnormal double and 309 for the largest normal one, so both rows are inside a
        /// table sized for <c>5^1074</c>. Neither fills up: a row is written only for an exponent
        /// some value actually had.
        /// </para>
        /// </remarks>
        internal static BigInteger Ten(int power)
        {
            if (Volatile.Read(ref Tens[power]) is BigInteger cached)
                return cached;

            var computed = Scale(-power) << power;
            Volatile.Write(ref Tens[power], computed);

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
