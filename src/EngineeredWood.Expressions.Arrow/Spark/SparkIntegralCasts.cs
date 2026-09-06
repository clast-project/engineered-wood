// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// What the legacy dialect answers when a cast to an integral type overflows.
/// </summary>
/// <remarks>
/// <para>
/// Under ANSI an overflowing integral cast raises <c>CAST_OVERFLOW</c> and there is nothing to
/// decide. With <see cref="SparkDialectOptions.Ansi"/> false Spark ANSWERS, and #243 was filed
/// on the belief that it answers with one rule — the wrap that <see cref="SparkArrays.Truncate"/>
/// already applies to arithmetic overflow. It does not. Measured into the corpus's
/// <c>integral-cast-overflow</c> group, harvested under both configurations, there are FOUR
/// source families and no two of them agree:
/// </para>
/// <list type="table">
/// <item>
///   <term>decimal or integral</term>
///   <description>
///     WRAPS. <c>CAST(<i>10^30 as decimal(38,0)</i> AS INT)</c> is 1073741824, which is
///     10^30 mod 2^32, and the same value as a BIGINT is 5076944270305263616, which is
///     10^30 mod 2^64. A fraction truncates toward zero first.
///   </description>
/// </item>
/// <item>
///   <term>float or double</term>
///   <description>
///     SATURATES, because Scala's <c>toInt</c> does. <c>CAST(1e30 AS INT)</c> is
///     <c>int.MaxValue</c> where the decimal of the same value wraps to 1073741824.
///   </description>
/// </item>
/// <item>
///   <term>string</term>
///   <description>
///     NULLS. <c>CAST('4294967298' AS INT)</c> is null rather than 2 — the parser simply fails,
///     which is also why every string failure is <c>CAST_INVALID_INPUT</c> under ANSI and never
///     <c>CAST_OVERFLOW</c>, whether the text was malformed or merely too large.
///   </description>
/// </item>
/// <item>
///   <term>timestamp</term>
///   <description>
///     NULLS, on a round-trip check rather than a range one.
///     <c>CAST(TIMESTAMP'9999-12-31 23:59:59' AS INT)</c> is null while the same value as a
///     BIGINT is 253402300799. This needs no code here — refusing is what the evaluator already
///     did — but it is the family that would have been wrong had the wrap been generalised.
///   </description>
/// </item>
/// </list>
/// </remarks>
internal static class SparkIntegralCasts
{
    /// <summary>What Spark's integral parse makes of a piece of text.</summary>
    internal enum TextForm
    {
        /// <summary>Not an integral literal at all, whatever the dialect.</summary>
        Invalid,

        /// <summary>A sign and digits.</summary>
        Integer,

        /// <summary>A sign, digits and a decimal point — which the two dialects treat apart.</summary>
        Fractional,
    }

    /// <summary>
    /// Reads <paramref name="text"/> as Spark's integral parse does: a sign, digits, and an
    /// optional decimal point with digits. NO exponent, in either dialect.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Measured, and not what .NET's parse accepts. <c>CAST('1e3' AS BIGINT)</c> is
    /// CAST_INVALID_INPUT under ANSI and NULL under the legacy dialect, where
    /// <c>double.TryParse</c> reads 1000 and we answered it — the fail-open half of #258. The
    /// floating and decimal targets DO take an exponent, which is why this rule belongs to the
    /// integral cast rather than to the value that reaches it.
    /// </para>
    /// <para>
    /// The <see cref="TextForm.Fractional"/> case is syntactic, and deliberately: measured,
    /// ANSI refuses <c>'1.0'</c>, <c>'0.0'</c> and <c>'10.'</c> as readily as <c>'1.5'</c>, so
    /// the point itself is what it objects to and not a non-zero fraction. Testing the VALUE
    /// instead — whether it survives truncation — accepted all three. The legacy dialect
    /// truncates every one of them.
    /// </para>
    /// </remarks>
    public static TextForm Classify(string text)
    {
        var span = text.Trim();
        var index = 0;

        if (index < span.Length && (span[index] == '+' || span[index] == '-'))
            index++;

        var digits = 0;
        while (index < span.Length && span[index] >= '0' && span[index] <= '9')
        {
            index++;
            digits++;
        }

        var fractional = false;
        if (index < span.Length && span[index] == '.')
        {
            fractional = true;
            index++;
            while (index < span.Length && span[index] >= '0' && span[index] <= '9')
            {
                index++;
                digits++;
            }
        }

        // Anything left over is what rules out an exponent, a separator, a type suffix and a
        // trailing letter alike -- and a form with no digits at all is not a number.
        if (index != span.Length || digits == 0)
            return TextForm.Invalid;

        return fractional ? TextForm.Fractional : TextForm.Integer;
    }

    private static readonly BigInteger LowWordMask = new(ulong.MaxValue);

    /// <summary>10^0 through 10^38, so the divisor below is not rebuilt for every row.</summary>
    /// <remarks>
    /// Unlike the parse-time work in <see cref="SparkDecimalText"/>, this really is a per-row
    /// path: a wide decimal column cast to an <c>int</c> overflows on every row it holds. 38 is
    /// Spark's largest decimal scale, so the fallback past the end of the table is unreachable
    /// through a Spark type — it is there because the Arrow type is not ours to constrain.
    /// </remarks>
    private static readonly BigInteger[] PowersOfTen = BuildPowersOfTen();

    private static BigInteger[] BuildPowersOfTen()
    {
        var powers = new BigInteger[SparkNumericTypes.MaxPrecision + 1];
        powers[0] = BigInteger.One;

        for (var i = 1; i < powers.Length; i++)
            powers[i] = powers[i - 1] * 10;

        return powers;
    }

    /// <summary>Which of Spark's four overflow rules a source type takes.</summary>
    internal enum Source
    {
        /// <summary>
        /// Decimal and integral: the value has an exact integer form, and wraps.
        /// </summary>
        /// <remarks>
        /// Boolean lands here too, and never reaches the wrap: 0 and 1 fit every integral type,
        /// so the overflow branches it would take are unreachable for one.
        /// </remarks>
        Exact,

        /// <summary>Float and double, which saturate.</summary>
        Floating,

        /// <summary>String, where an out-of-range value is a failed parse and yields null.</summary>
        Text,

        /// <summary>Date and timestamp, which yield null.</summary>
        Temporal,
    }

    internal static Source FamilyOf(IArrowType type) => type switch
    {
        FloatType or DoubleType => Source.Floating,
        StringType => Source.Text,
        Date32Type or Date64Type or TimestampType => Source.Temporal,
        _ => Source.Exact,
    };

    /// <summary>
    /// The low bits of an exact source, which is what Spark's legacy dialect answers for one.
    /// </summary>
    /// <remarks>
    /// Read from the source array rather than from the <see cref="decimal"/> the evaluator
    /// already holds, because that form covers only part of the range: a decimal past
    /// <see cref="decimal"/>'s ~7.9e28 has none at all, and one past <c>long</c>'s ~9.2e18 has no
    /// <c>long</c> to truncate. Both are exactly the values that overflow an integral target, so
    /// the unscaled integer is the only form that can answer here.
    /// </remarks>
    internal static long Wrap(IArrowArray source, int index, IArrowType target) =>
        SparkArrays.Truncate(
            source is Decimal128Array decimals
                ? LowBits(decimals, index)
                : SparkArrays.ReadInt64(source, index)!.Value,
            target);

    /// <summary>The low 64 bits of a decimal cell's integer part, truncated toward zero.</summary>
    private static long LowBits(Decimal128Array array, int index)
    {
        var scale = ((Decimal128Type)array.Data.DataType).Scale;
        var unscaled = SparkArrays.Unscaled(array, index);

        // BigInteger division truncates toward zero, which is the order Spark uses: the fraction
        // goes before the width does, so decimal(20,1) holding 4294967298.5 casts to INT as 2.
        // Scale 0 is the common shape of a column cast to an integral type, and skips the divide.
        var truncated = scale == 0
            ? unscaled
            : unscaled / (scale < PowersOfTen.Length ? PowersOfTen[scale] : BigInteger.Pow(10, scale));

        // Two's complement, which BigInteger's bitwise operators already use: the mask of a
        // negative value is its low 64 bits as an unsigned number.
        return unchecked((long)(ulong)(truncated & LowWordMask));
    }

    /// <summary>
    /// A floating-point source clamped the way Scala's <c>toInt</c> and <c>toLong</c> clamp it.
    /// </summary>
    /// <remarks>
    /// <b>The saturation happens at INT even for a narrower target, and the narrowing after it
    /// wraps.</b> Spark casts a double to a byte as <c>numeric.toInt(d).toByte</c>, and the
    /// corpus separates the two possibilities: <c>CAST(300.0 AS TINYINT)</c> is 44, so the
    /// narrowing is not a clamp, and <c>CAST(4294967298.5 AS TINYINT)</c> is -1 rather than 127,
    /// so the clamp before it is at <c>int</c> and not at the target.
    /// </remarks>
    internal static long Saturate(double value, IArrowType target) =>
        target is Int64Type ? ToInt64(value) : SparkArrays.Truncate(ToInt32(value), target);

    /// <summary>Java's <c>(int)</c> conversion: NaN is zero and the ends clamp.</summary>
    private static long ToInt32(double value) =>
        double.IsNaN(value) ? 0L
        : value >= int.MaxValue ? int.MaxValue
        : value <= int.MinValue ? int.MinValue
        : (long)value;

    /// <summary>Java's <c>(long)</c> conversion, which C#'s own cast leaves undefined out of range.</summary>
    private static long ToInt64(double value) =>
        double.IsNaN(value) ? 0L
        : value >= long.MaxValue ? long.MaxValue
        : value <= long.MinValue ? long.MinValue
        : (long)value;
}
