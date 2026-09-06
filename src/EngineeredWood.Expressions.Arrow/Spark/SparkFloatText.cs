// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
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
/// </remarks>
internal static class SparkFloatText
{
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

        var e = text.IndexOfAny(new[] { 'e', 'E' });
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
