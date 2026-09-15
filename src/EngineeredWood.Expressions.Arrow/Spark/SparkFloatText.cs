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
/// decimal, where the widened double is exactly what Spark converts. The two paths need separate
/// ladders for that reason and not merely for width.
/// </para>
/// <para>
/// The JDK band from #244 reaches here too, because this is the same <c>Double.toString</c>: the
/// digits are the shortest round-trip form on JDK 19 and later, and can be one longer before it.
/// The corpus records <c>java_version</c> beside <c>conf</c>, and the two rows that land in the
/// band are declared differences.
/// </para>
/// <para>
/// <b>A subnormal is a different question and gets its own answer.</b> Every rule above is about
/// where the point goes; <see cref="Subnormal"/> is about which digits there are at all, and it
/// reaches them without the platform's formatter or its parser. #288.
/// </para>
/// </remarks>
internal static class SparkFloatText
{
    /// <summary>The smallest positive normal double, below which a double loses significant bits.</summary>
    private const double DoubleMinNormal = 2.2250738585072014E-308;

    /// <summary>The smallest positive normal float.</summary>
    private const float FloatMinNormal = 1.17549435E-38f;

    /// <summary>The exponent every subnormal double is written at: <c>double.Epsilon</c> is 2^-1074.</summary>
    private const int DoubleMinExponent = -1074;

    /// <summary>The exponent every subnormal float is written at: <c>float.Epsilon</c> is 2^-149.</summary>
    private const int FloatMinExponent = -149;

    private static CultureInfo Invariant => CultureInfo.InvariantCulture;

    /// <summary>Java's rendering of a double.</summary>
    internal static string Render(double value)
    {
        if (double.IsNaN(value)) return "NaN";
        if (double.IsPositiveInfinity(value)) return "Infinity";
        if (double.IsNegativeInfinity(value)) return "-Infinity";

        return Format(IsNegative(value), ShortestRoundTrip(Math.Abs(value)));
    }

    /// <summary>Java's rendering of a float, whose digits are the float's own and not the double's.</summary>
    internal static string Render(float value)
    {
        if (float.IsNaN(value)) return "NaN";
        if (float.IsPositiveInfinity(value)) return "Infinity";
        if (float.IsNegativeInfinity(value)) return "-Infinity";

        return Format(IsNegative(value), ShortestRoundTrip(Math.Abs(value)));
    }

    /// <summary>
    /// The shortest decimal text that round-trips <paramref name="value"/>.
    /// </summary>
    /// <remarks>
    /// <b>Deliberately not <c>ToString("R")</c></b>, which is the shortest form only on .NET Core.
    /// Measured: on net472 the double 0.3333333333333333 renders as seventeen digits there and as
    /// sixteen on net10.0, which made <c>CAST(g AS DECIMAL(38,20))</c> answer differently per
    /// target framework — a cast is not allowed to depend on which build of this library is
    /// loaded. The G15/G16/G17 ladder is the portable spelling of the same thing, and produces the
    /// identical value on every target: checked against <c>"R"</c> over ~1e6 doubles on net10.0
    /// with no disagreement at all.
    /// <para>
    /// This is also what Spark's <c>BigDecimal.valueOf(d)</c> reads — up to the JVM's own version
    /// of the question, since <c>Double.toString</c> did not produce the shortest form before
    /// JDK 19. See <c>SparkFunctionRegistry.CastFloatingToDecimal</c> and #244.
    /// </para>
    /// </remarks>
    internal static string ShortestRoundTrip(double value)
    {
        var magnitude = Math.Abs(value);
        if (magnitude > 0 && magnitude < DoubleMinNormal)
            return Sign(value) + Subnormal((long)(magnitude / double.Epsilon), DoubleMinExponent, 17);

        // Unrolled onto constant format strings rather than built per iteration: this runs for
        // every row of every cast, and there are only ever three rungs.
        if (RoundTrips(value, "G15", out var fifteen))
            return fifteen;

        if (RoundTrips(value, "G16", out var sixteen))
            return sixteen;

        // Seventeen significant digits always round-trip a double, so there is nothing to check.
        return value.ToString("G17", Invariant);
    }

    /// <summary>The shortest decimal text that round-trips a float, which needs at most nine digits.</summary>
    internal static string ShortestRoundTrip(float value)
    {
        var magnitude = Math.Abs(value);
        if (magnitude > 0 && magnitude < FloatMinNormal)
            return Sign(value) + Subnormal((long)((double)magnitude / float.Epsilon), FloatMinExponent, 9);

        // Six, not seven. Half a step is at most 2^-24 of a normal float, and half of the
        // seven-digit grid's step is as little as 0.05e-6 = 5.0e-8 where the leading digit is a 9
        // -- SMALLER than 2^-24 = 5.96e-8, so rounding to seven digits does not always land on a
        // six-digit shortest form. Measured: it prints 9.458641E-10 where Java prints 9.45864E-10,
        // and the six mismatches in a 200,000-float sweep all began with a 9. The same arithmetic
        // is what makes fifteen right for a double: half of its sixteen-digit step can be 5.0e-17
        // against a half-step of 2^-53 = 1.11e-16, and half of the fifteen-digit step cannot.
        if (RoundTrips(value, "G6", out var six))
            return six;

        if (RoundTrips(value, "G7", out var seven))
            return seven;

        if (RoundTrips(value, "G8", out var eight))
            return eight;

        return value.ToString("G9", Invariant);
    }

    private static bool RoundTrips(double value, string format, out string text)
    {
        text = value.ToString(format, Invariant);
        return double.TryParse(text, NumberStyles.Float, Invariant, out var parsed) && parsed == value;
    }

    private static bool RoundTrips(float value, string format, out string text)
    {
        text = value.ToString(format, Invariant);
        return float.TryParse(text, NumberStyles.Float, Invariant, out var parsed) && parsed == value;
    }

    /// <summary>
    /// The shortest decimal text that reads back as the subnormal <c>mantissa × 2^minExponent</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The ladder above starts at fifteen digits because that is where a NORMAL double's shortest
    /// form can first appear: half a step is at most 2^-53 of the value, so rounding to fifteen
    /// digits lands on the shortest form whenever the shortest form is that short or shorter. A
    /// subnormal breaks the premise. Its step is a fixed 2^-1074 no matter how small the value
    /// gets, so at the bottom of the range half a step is half the value itself and ONE digit
    /// round-trips — which is why we printed <c>9.88131291682493E-324</c> where Spark prints two
    /// digits. #288.
    /// </para>
    /// <para>
    /// <b>Neither the platform's formatter nor its parser can be asked here.</b> Both are wrong
    /// about subnormals on .NET Framework and right on .NET Core, which is the one thing a cast
    /// may not depend on. Measured: <c>double.Parse("2.12E-322")</c> returns the wrong double
    /// there, so a round-trip test answers falsely, and <c>ToString("G16")</c> of the double just
    /// below the smallest normal one is a digit out, so the digits it hands over are not the
    /// value's. So the digits come from exact integer arithmetic instead: a subnormal is
    /// <c>mantissa × 2^minExponent</c>, which is <c>(mantissa × 5^-minExponent) × 10^minExponent</c>,
    /// and the significant digits are simply the digits of that integer product.
    /// </para>
    /// <para>
    /// Checked against <c>Float.toString</c> on JDK 21 over all 8,388,607 subnormal floats and
    /// <c>Double.toString</c> over 22,000 subnormal doubles: identical on every one, on net10.0
    /// and on net472 alike.
    /// </para>
    /// </remarks>
    private static string Subnormal(long mantissa, int minExponent, int maxDigits)
    {
        var exact = (Powers.Scale(minExponent) * mantissa).ToString(Invariant);

        // The product is the digit string of `0.<exact> × 10^pointAt`, with no leading zero to
        // discard: BigInteger does not write one.
        var pointAt = exact.Length + minExponent;

        // Built once for the value rather than once per rung. Measured over 100,000 subnormal
        // doubles, that and the cached 5^1074 take a value from 17.2us to 7.8us; a normal one is
        // 0.27us. What is left is dominated by the ToString above -- 11us of a 751-digit
        // BigInteger -- which only goes away by not forming the whole expansion at all.
        var reads = new ReadsBack(mantissa, minExponent, maxDigits - pointAt);

        for (var length = 1; length < maxDigits; length++)
        {
            var candidate = Round(exact, pointAt, length);
            if (!reads.Contains(candidate))
                continue;

            // Java's one exception to shortest-wins, and the reason Double.toString(4.9E-324) is
            // not `5E-324`: where a single digit suffices, the answer is the closer of the best
            // one- and two-digit decimals rather than the one-digit one. Measured -- it is the
            // difference between `9.9E-324` and `1.0E-323` for the double just above the
            // smallest, and both of those round-trip.
            if (length == 1)
            {
                var pair = Round(exact, pointAt, 2);
                if (reads.Contains(pair))
                    return Scientific(pair);
            }

            return Scientific(candidate);
        }

        // Seventeen digits always identify a double and nine always identify a float, subnormal
        // or not, so the last rung needs no test.
        return Scientific(Round(exact, pointAt, maxDigits));
    }

    /// <summary>
    /// Which decimals read back as one subnormal, as an exact test a candidate can be put to.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A candidate is <c>c × 10^-s</c>, and it reads back as the value when it sits within half a
    /// step of it — half a step being <c>2^(minExponent-1)</c>, the same for every subnormal:
    /// </para>
    /// <code>
    ///     |c × 10^-s − mantissa × 2^minExponent| ≤ 2^(minExponent-1)
    /// </code>
    /// <para>
    /// Multiplying through by <c>2^(1-minExponent) × 5^s</c> leaves nothing but integers. That is
    /// the whole test, but it puts a <c>5^s</c> of some 230 digits on a per-candidate footing, and
    /// the ladder asks up to seventeen candidates. Multiplying by <c>2^(1-minExponent) × 5^S</c>
    /// for a fixed <c>S ≥ s</c> instead scales both sides of the SAME comparison by
    /// <c>5^(S-s)</c>, which cannot change its answer, and leaves the large power on a per-VALUE
    /// footing:
    /// </para>
    /// <code>
    ///     |c × 2^(1-minExponent-s) × 5^(S-s) − 2 × mantissa × 5^S| ≤ 5^S
    /// </code>
    /// <para>
    /// <c>S</c> is <c>maxDigits - pointAt</c>, which no rung's <c>s</c> can exceed: a candidate
    /// has at most <c>maxDigits</c> digits, and rounding only ever moves the point right. So
    /// <c>S - s</c> stays within <c>[0, maxDigits]</c> and its power comes from a table.
    /// </para>
    /// <para>
    /// The boundary itself counts only for an even mantissa, which is where round-half-to-even
    /// sends a decimal landing exactly between two doubles.
    /// </para>
    /// </remarks>
    private readonly struct ReadsBack
    {
        private readonly BigInteger _half;
        private readonly BigInteger _middle;
        private readonly int _shift;
        private readonly int _ceiling;
        private readonly bool _boundaryCounts;

        internal ReadsBack(long mantissa, int minExponent, int scaleCeiling)
        {
            _shift = 1 - minExponent;
            _ceiling = scaleCeiling;
            _half = BigInteger.Pow(5, scaleCeiling);
            _middle = 2 * mantissa * _half;
            _boundaryCounts = mantissa % 2 == 0;
        }

        internal bool Contains((string Digits, int PointAt) candidate)
        {
            var scale = candidate.Digits.Length - candidate.PointAt;
            if (scale <= 0 || scale > _shift || scale > _ceiling)
                return false;

            // At most seventeen digits by construction, so this is a long and not a BigInteger.
            var c = long.Parse(candidate.Digits, NumberStyles.Integer, Invariant);
            var scaled = new BigInteger(c) * Powers.Small[_ceiling - scale];
            var distance = BigInteger.Abs((scaled << (_shift - scale)) - _middle);

            return distance < _half || (distance == _half && _boundaryCounts);
        }
    }

    /// <summary>
    /// The powers of five the subnormal path reads, built on first use rather than at type load.
    /// </summary>
    /// <remarks>
    /// <c>5^1074</c> is a 751-digit number and nothing but a subnormal wants it, so it is held by
    /// a nested class: a value that never leaves the normal range never builds it, which a plain
    /// <c>static readonly</c> on the class itself could not promise. The same reason
    /// <see cref="SparkIntegralCasts"/> holds its powers of ten, one row further down: this is a
    /// per-row path for any column that holds subnormals at all.
    /// </remarks>
    private static class Powers
    {
        /// <summary>10^minExponent written as a power of five: the exact digits of 2^minExponent.</summary>
        internal static BigInteger Scale(int minExponent) =>
            minExponent == DoubleMinExponent ? DoubleScale : FloatScale;

        /// <summary>5^0 through 5^18, which covers every gap between a rung's scale and the ceiling.</summary>
        internal static readonly BigInteger[] Small = BuildSmall();

        private static readonly BigInteger DoubleScale = BigInteger.Pow(5, -DoubleMinExponent);

        private static readonly BigInteger FloatScale = BigInteger.Pow(5, -FloatMinExponent);

        private static BigInteger[] BuildSmall()
        {
            var powers = new BigInteger[19];
            powers[0] = BigInteger.One;

            for (var i = 1; i < powers.Length; i++)
                powers[i] = powers[i - 1] * 5;

            return powers;
        }
    }

    /// <summary>
    /// Rounds the exact digit string to <paramref name="length"/> significant digits, half to even.
    /// </summary>
    /// <returns>The digits with trailing zeros dropped, and the exponent of <c>0.&lt;digits&gt;</c>.</returns>
    private static (string Digits, int PointAt) Round(string exact, int pointAt, int length)
    {
        if (exact.Length > length)
        {
            var head = exact.Substring(0, length);

            if (RoundsUp(exact, length))
            {
                head = Increment(head);

                // 99 -> 100: one digit too many, and the point has moved.
                if (head.Length > length)
                {
                    head = head.Substring(0, length);
                    pointAt++;
                }
            }

            exact = head;
        }

        // A trailing zero is not a significant digit, and Java counts the length of a decimal
        // without one: 1.0E-310 is one digit long, not two.
        return (exact.TrimEnd('0'), pointAt);
    }

    private static bool RoundsUp(string exact, int length)
    {
        var next = exact[length];
        if (next != '5')
            return next > '5';

        for (var i = length + 1; i < exact.Length; i++)
            if (exact[i] != '0')
                return true;

        // An exact tie, which a terminating expansion like this one can genuinely reach.
        return (exact[length - 1] - '0') % 2 == 1;
    }

    private static string Increment(string digits)
    {
        var carried = digits.ToCharArray();

        for (var i = carried.Length - 1; i >= 0; i--)
        {
            if (carried[i] != '9')
            {
                carried[i]++;
                return new string(carried);
            }

            carried[i] = '0';
        }

        return "1" + new string(carried);
    }

    /// <summary>Writes the digits in the shape a "G" format would, for <see cref="Split"/> to read.</summary>
    private static string Scientific((string Digits, int PointAt) value)
    {
        var exponent = (value.PointAt - 1).ToString(Invariant);

        return value.Digits.Length == 1
            ? value.Digits + "E" + exponent
            : value.Digits.Substring(0, 1) + "." + value.Digits.Substring(1) + "E" + exponent;
    }

    private static string Sign(double value) => value < 0 ? "-" : string.Empty;

    /// <summary>
    /// Whether the value carries a negative sign, including negative zero.
    /// </summary>
    /// <remarks>
    /// By the sign bit rather than <c>&lt; 0</c>, which is false for -0.0, and by hand rather
    /// than through <c>double.IsNegative</c>, which netstandard2.0 does not have.
    /// </remarks>
    private static bool IsNegative(double value) => BitConverter.DoubleToInt64Bits(value) < 0;

    private static bool IsNegative(float value) => IsNegative((double)value);

    /// <summary>
    /// Re-spells shortest-round-trip text in Java's shape.
    /// </summary>
    /// <remarks>
    /// The digits are already decided by the time this runs; all that is left is where the point
    /// goes and whether an exponent is written. Working from the text rather than from the value
    /// keeps this one function for both widths.
    /// </remarks>
    private static string Format(bool negative, string shortest)
    {
        var (digits, pointAt) = Split(shortest);
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
    /// The significant digits of unsigned decimal text, and the base-10 exponent of the point.
    /// </summary>
    /// <remarks>
    /// The value is <c>0.&lt;digits&gt; × 10^pointAt</c>. Zero comes back as an empty digit
    /// string, which the caller answers directly.
    /// </remarks>
    private static (string Digits, int PointAt) Split(string text)
    {
        var exponent = 0;

        // Two IndexOf calls rather than IndexOfAny(new[] { 'e', 'E' }), which allocates its
        // needle array on every call — and this runs for every rendered value. Uppercase first
        // because that is what the G formats above produce; lowercase is accepted so the method
        // reads any well-formed decimal text.
        var e = text.IndexOf('E');
        if (e < 0)
            e = text.IndexOf('e');

        if (e >= 0)
        {
            exponent = int.Parse(text.Substring(e + 1), NumberStyles.Integer, Invariant);
            text = text.Substring(0, e);
        }

        var dot = text.IndexOf('.');
        if (dot >= 0)
        {
            exponent -= text.Length - dot - 1;
            text = text.Remove(dot, 1);
        }

        var digits = text.TrimStart('0');
        exponent += digits.Length;

        return (digits.TrimEnd('0'), exponent);
    }
}
