// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Clast.DatabaseDecimal;
using Clast.DatabaseDecimal.Arithmetic;
using Clast.DatabaseDecimal.Values;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Exact decimal arithmetic over the unscaled integer, across the whole of Spark's precision range.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="decimal"/> holds only about 7.9e28 where Spark decimals reach precision 38. The
/// Arrow buffer already stores an unscaled two's complement integer and a scale, so working on
/// those directly covers the whole range.
/// </para>
/// <para>
/// The arithmetic itself is database-decimal's. Every operation goes through the widening kernel
/// rather than the same-width one, so the exact result is formed in 256 bits and rounded once to
/// the result type <see cref="SparkNumericTypes"/> produced. Spark computes the exact result and
/// then rounds it, so rounding in the middle — which a same-width kernel has to do when an
/// intermediate will not fit — would be a different function.
/// </para>
/// </remarks>
internal static class SparkWideDecimals
{
    /// <summary>
    /// Spark rounds a discarded half away from zero.
    /// </summary>
    /// <remarks>
    /// <c>CAST(2.5 AS DECIMAL(3,0))</c> is 3 and <c>CAST(1.45 AS DECIMAL(3,1))</c> is
    /// 1.5. Passed explicitly at every call because database-decimal defaults to
    /// <see cref="DecimalRounding.HalfEven"/>, which would give 2 and 1.4.
    /// </remarks>
    private const DecimalRounding Rounding = DecimalRounding.HalfUp;

    /// <summary>
    /// Overflow of the declared precision is reported rather than thrown.
    /// </summary>
    /// <remarks>
    /// The result is still range-checked, through <see cref="DecimalRange"/>, and reported as
    /// null; see <see cref="Evaluate"/> for why the caller decides what an overflow means.
    /// </remarks>
    private const DecimalOverflow Overflow = DecimalOverflow.Ignore;

    /// <summary>An operand as its unscaled integer, with the decimal type that integer carries.</summary>
    internal readonly struct Operand
    {
        internal Operand(Int128 unscaled, int precision, int scale)
        {
            Unscaled = unscaled;
            Type = DecimalType.Numeric(precision, scale);
        }

        internal Int128 Unscaled { get; }

        internal DecimalType Type { get; }

        internal bool IsZero => Unscaled == Int128.Zero;
    }

    /// <summary>
    /// Reads an operand of a decimal operation, or null where the cell is null.
    /// </summary>
    /// <remarks>
    /// Integral arrays appear here because decimal is contagious in Spark's type rules: an
    /// <c>int</c> mixed with a decimal is read as the <c>decimal(10,0)</c> that holds it exactly,
    /// so the operand arriving at a decimal operation may still be an <see cref="Int32Array"/>.
    /// Floating point never arrives: a decimal mixed with a float or double is typed double.
    /// </remarks>
    internal static Operand? Read(IArrowArray array, int index) => array switch
    {
        // A bare NULL, typed `void`: no value at any row, and reachable wherever the other
        // operand made the result a decimal -- `d1 + NULL`, `if(c, d1, NULL)`.
        NullArray => null,

        // The precisions are the ones SparkNumericTypes.AsDecimal assigns, so an operand carries
        // the same type here that the result-type rules gave it.
        Int8Array a => a.IsNull(index) ? null : new Operand(FromInt64(a.GetValue(index)!.Value), 3, 0),
        Int16Array a => a.IsNull(index) ? null : new Operand(FromInt64(a.GetValue(index)!.Value), 5, 0),
        Int32Array a => a.IsNull(index) ? null : new Operand(FromInt64(a.GetValue(index)!.Value), 10, 0),
        Int64Array a => a.IsNull(index) ? null : new Operand(FromInt64(a.GetValue(index)!.Value), 20, 0),
        Decimal128Array a => a.IsNull(index)
            ? null
            : new Operand(
                Unscaled(a, index),
                ((Decimal128Type)a.Data.DataType).Precision,
                ((Decimal128Type)a.Data.DataType).Scale),
        _ => throw new NotSupportedException(
            $"{array.Data.DataType.Name} has no exact decimal representation"),
    };

    /// <summary>
    /// The result of <paramref name="op"/> at <paramref name="resultType"/>, or null when the
    /// exact result does not fit that type.
    /// </summary>
    /// <remarks>
    /// Null rather than an exception because the two dialects disagree on what an overflow is:
    /// ANSI raises <c>ARITHMETIC_OVERFLOW</c> and the legacy dialect yields null, and that choice
    /// belongs to the caller holding the <see cref="SparkDialectOptions"/> rather than here.
    /// A zero divisor is the caller's to check too, for the same reason.
    /// </remarks>
    internal static Int128? Evaluate(string op, Operand left, Operand right, Decimal128Type resultType)
    {
        var result = AsDecimalType(resultType);
        var l = new Decimal128(left.Unscaled);
        var r = new Decimal128(right.Unscaled);

        Decimal256 exact;

        try
        {
            exact = op switch
            {
                "+" => AddKernel.AddWiden(l, left.Type, r, right.Type, result, Rounding, Overflow),

                // The 256-bit tier rather than a widening subtract, which the package does not
                // offer. Equivalent here: both operands came out of a 128-bit mantissa, so raising
                // them to the wider scale cannot leave the 256-bit range.
                "-" => AddKernel.Subtract(
                    (Decimal256)l, left.Type, (Decimal256)r, right.Type, result, Rounding, Overflow),

                // MultiplyWiden, not the 256-bit Multiply: with no 512-bit intermediate the 256
                // tier splits a scale reduction across the operands and rounds each independently,
                // which the package documents as good to a unit in the last place. Spark wants the
                // exact product rounded once, and forming it in 256 bits from 128-bit operands
                // gives exactly that.
                "*" => MultiplyKernel.MultiplyWiden(l, left.Type, r, right.Type, result, Rounding, Overflow),

                // DivideWiden pre-scales the dividend into 256 bits. The same-width divide would
                // pre-scale within 128 and lose digits: decimal(38,38) / decimal(38,38) pre-scales
                // by 10^44, which no 128-bit mantissa holds even where the quotient is 1.
                "/" => DivideKernel.DivideWiden(l, left.Type, r, right.Type, result, Rounding, Overflow),

                "%" => ModulusKernel.ModulusWiden(l, left.Type, r, right.Type, result, Rounding, Overflow),

                _ => throw new NotSupportedException($"'{op}' over decimals"),
            };
        }
        catch (OverflowException)
        {
            // The width check, which the kernels apply regardless of DecimalOverflow. For operand
            // types capped at precision 38 a result that will not fit 256 bits was never going to
            // fit the result type either.
            return null;
        }

        return DecimalRange.IsInRange(exact.Mantissa, result) ? (Int128)exact.Mantissa : null;
    }

    /// <summary>Spark's spelling of a decimal type as database-decimal's.</summary>
    private static DecimalType AsDecimalType(Decimal128Type type) =>
        DecimalType.Numeric(type.Precision, type.Scale);

    /// <summary>
    /// Whether values of this type have an exact unscaled-integer form.
    /// </summary>
    /// <remarks>
    /// Floating point does not, which is the line this draws: a double reaches 1.8e308 and carries
    /// binary fractions no decimal represents, so bringing one here would need a lossy conversion
    /// rather than a widening one and it keeps its existing path instead.
    /// </remarks>
    internal static bool IsExact(IArrowType type) =>
        type is Decimal128Type or Int8Type or Int16Type or Int32Type or Int64Type;

    /// <summary>
    /// Brings a value to <paramref name="target"/>, or null where it does not fit.
    /// </summary>
    internal static Int128? Cast(Operand value, Decimal128Type target)
    {
        var type = AsDecimalType(target);

        try
        {
            // In 256 bits because the rescale runs before the range check: casting decimal(38,0) to
            // decimal(38,2) multiplies by 100 first, and the intermediate leaves 128 bits even
            // though nothing that survives the check ever will.
            var exact = ScaleHelper.Rescale256(
                (Int256)value.Unscaled, value.Type.Scale, type.Scale, Rounding);

            return DecimalRange.IsInRange(exact, type) ? (Int128)exact : null;
        }
        catch (OverflowException)
        {
            return null;
        }
    }

    /// <summary>
    /// Rounds to a multiple of 10^<paramref name="places"/>, half away from zero, at scale 0.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One rounding, not two. Declaring the operand's scale to be <c>Scale + places</c> is the
    /// same as dividing it by 10^<paramref name="places"/>, so a single rescale to 0 rounds at the
    /// place that matters. Rounding to an integer first and to the multiple second would answer
    /// 20 for <c>round(14.6, -1)</c>, by way of 15; Spark answers 10.
    /// </para>
    /// <para>
    /// Null means the result does not fit <paramref name="target"/> — the caller decides what
    /// that is, exactly as for <see cref="Cast"/>.
    /// </para>
    /// </remarks>
    internal static Int128? RoundToPowerOfTen(Operand value, int places, Decimal128Type target)
    {
        // No Spark decimal reaches half of 10^39, so everything past 38 rounds to zero -- and
        // saying so here is what keeps `round(x, -2000000000)` from forming that power at all.
        if (places > SparkNumericTypes.MaxPrecision)
            return Int128.Zero;

        var type = AsDecimalType(target);

        try
        {
            var quotient = ScaleHelper.Rescale256(
                (Int256)value.Unscaled, value.Type.Scale + places, 0, Rounding);

            if (quotient == Int256.Zero)
                return Int128.Zero;

            var factor = Int256.One;
            for (var i = 0; i < places; i++) factor *= Ten;

            var exact = quotient * factor;
            return DecimalRange.IsInRange(exact, type) ? (Int128)exact : null;
        }
        catch (OverflowException)
        {
            return null;
        }
    }

    private static readonly Int256 Ten = (Int256)10;

    /// <summary>
    /// Whether two operands denote the same number, whatever scales they carry.
    /// </summary>
    /// <remarks>
    /// Compared at the wider of the two scales, so a <c>decimal(10,2)</c> holding 1.00 equals an
    /// <c>int</c> holding 1, as Spark says.
    /// </remarks>
    internal static bool AreEqual(Operand left, Operand right)
    {
        // Raising a scale multiplies and is exact, so the rounding mode never comes up here.
        var working = Math.Max(left.Type.Scale, right.Type.Scale);

        return ScaleHelper.Widen128To256(left.Unscaled, left.Type.Scale, working, Rounding)
            == ScaleHelper.Widen128To256(right.Unscaled, right.Type.Scale, working, Rounding);
    }

    // ── Arrow buffers ──────────────────────────────────────────────────────────────────────────

    /// <summary>The raw unscaled integer behind a Decimal128 cell.</summary>
    /// <remarks>
    /// A reinterpret rather than a copy. Arrow stores a decimal128 as 16 bytes of little-endian
    /// two's complement, which is <see cref="Int128"/>'s own layout, so the whole column is already
    /// an <c>Int128</c> array and no cell needs an allocation.
    /// </remarks>
    private static Int128 Unscaled(Decimal128Array array, int index)
    {
        RequireLittleEndian();

        // GC.KeepAlive because `array` is otherwise dead once the span is taken, and the span
        // points into its buffer. See doc/arrow-span-lifetime.md.
        var value = MemoryMarshal.Cast<byte, Int128>(array.ValueBuffer.Span)[index];
        GC.KeepAlive(array);

        return value;
    }

    /// <summary>Builds a Decimal128 column from unscaled integers, null where the value is null.</summary>
    /// <remarks>
    /// Built as a buffer rather than through <c>Decimal128Array.Builder</c>, whose only exact entry
    /// points are <see cref="decimal"/>, whose range is too small, and a string, which would mean
    /// formatting and reparsing every value.
    /// </remarks>
    internal static Decimal128Array Build(Int128?[] values, Decimal128Type type, int rowCount)
    {
        RequireLittleEndian();

        var buffer = new byte[rowCount * 16];
        var mantissas = MemoryMarshal.Cast<byte, Int128>(buffer.AsSpan());
        var validity = new ArrowBuffer.BitmapBuilder(rowCount);
        var nulls = 0;

        for (var i = 0; i < rowCount; i++)
        {
            if (values[i] is { } mantissa)
            {
                mantissas[i] = mantissa;
                validity.Append(true);
            }
            else
            {
                validity.Append(false);
                nulls++;
            }
        }

        return new Decimal128Array(new ArrayData(
            type, rowCount, nulls, 0,
            new[] { validity.Build(), new ArrowBuffer(buffer) }));
    }

    /// <summary>
    /// Orders two decimal cells that carry the same scale, without allocating.
    /// </summary>
    /// <remarks>
    /// Same scale by construction — the caller unifies first — so the unscaled integers order the
    /// values directly. Compared as two 64-bit halves rather than through
    /// <see cref="System.Numerics.BigInteger"/>, which allocates, because <c>greatest</c> and
    /// <c>least</c> call this once per argument per row; and rather than through
    /// <see cref="Int128"/>'s own operators because the netstandard2.0 build takes that type from
    /// database-decimal's polyfill, which carries no ordering.
    /// </remarks>
    internal static int Compare(Decimal128Array left, Decimal128Array right, int index)
    {
        var (leftLow, leftHigh) = Halves(left, index);
        var (rightLow, rightHigh) = Halves(right, index);

        // The high half carries the sign, so it is compared as signed and the low half as
        // unsigned — which is the whole of two's complement ordering.
        return leftHigh != rightHigh
            ? ((long)leftHigh).CompareTo((long)rightHigh)
            : leftLow.CompareTo(rightLow);
    }

    /// <summary>The two 64-bit halves of a Decimal128 cell, little-endian as Arrow stores them.</summary>
    private static (ulong Low, ulong High) Halves(Decimal128Array array, int index)
    {
        RequireLittleEndian();

        // GC.KeepAlive because `array` is otherwise dead once the span is taken, and the span
        // points into its buffer. See doc/arrow-span-lifetime.md.
        var words = MemoryMarshal.Cast<byte, ulong>(array.ValueBuffer.Span);
        var halves = (words[index * 2], words[(index * 2) + 1]);
        GC.KeepAlive(array);

        return halves;
    }

    /// <summary>How Spark prints a decimal, across the whole precision range.</summary>
    internal static string Render(Operand value) =>
        new Decimal128(value.Unscaled).ToString(value.Type.Scale);

    /// <summary>
    /// How the legacy dialect prints a decimal: Java's <c>BigDecimal.toString</c>, which goes
    /// scientific once the adjusted exponent drops below -6.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The adjusted exponent is the coefficient's digit count, less one, less the scale. It
    /// depends on the value as well as the scale, so a zero at scale 7 is <c>0E-7</c> while
    /// <c>0.0000010</c> at the same scale stays plain. The coefficient keeps every digit it has,
    /// trailing zeros included: <c>-0.000000120</c> at scale 9 is <c>-1.20E-7</c>. The ANSI
    /// dialect, and <c>try_cast</c> in either dialect, use <c>toPlainString</c> instead, which is
    /// <see cref="Render"/>.
    /// </para>
    /// <para>
    /// A Spark decimal never has a negative scale, so the exponent here is always negative and
    /// Java's <c>E+</c> spelling never arises. The coefficient is read from the plain rendering,
    /// so both spellings share the same digits.
    /// </para>
    /// </remarks>
    internal static string RenderScientific(Operand value)
    {
        var plain = Render(value);
        var scale = value.Type.Scale;
        if (scale == 0)
            return plain;

        // Read the exponent off the plain text rather than rebuilding the coefficient, so a value
        // that stays plain -- nearly all of them -- costs no more than the plain rendering. That
        // text is `[-]I.F` with exactly `scale` fraction digits, so a non-zero integer part means
        // an exponent of at least zero, and otherwise the first non-zero fraction digit fixes it.
        var start = plain[0] == '-' ? 1 : 0;
        var point = plain.Length - scale - 1;
        if (point - start > 1 || plain[start] != '0')
            return plain;

        var first = point + 1;
        while (first < plain.Length && plain[first] == '0')
            first++;

        var isZero = first == plain.Length;
        var adjusted = isZero ? -scale : point - first;
        if (adjusted >= -6)
            return plain;

        var text = new System.Text.StringBuilder(plain.Length - first + 8);
        if (start == 1)
            text.Append('-');
        if (isZero)
        {
            text.Append('0');
        }
        else
        {
            text.Append(plain[first]);
            if (first + 1 < plain.Length)
                text.Append('.').Append(plain, first + 1, plain.Length - first - 1);
        }

        return text.Append('E')
            .Append(adjusted.ToString(System.Globalization.CultureInfo.InvariantCulture))
            .ToString();
    }

    // ── Conversions the netstandard2.0 polyfill does not offer ─────────────────────────────────

    /// <summary>
    /// Sign-extends a 64-bit integer, without depending on a conversion operator.
    /// </summary>
    /// <remarks>
    /// The netstandard2.0 build gets <see cref="Int128"/> from database-decimal's polyfill rather
    /// than from the BCL, and the polyfill carries a smaller surface than the real type. Building
    /// the halves by hand is exact on every target and costs nothing.
    /// </remarks>
    private static Int128 FromInt64(long value) =>
        new(value < 0 ? ulong.MaxValue : 0UL, unchecked((ulong)value));

    /// <summary>
    /// Refuses a big-endian host rather than producing wrong numbers on one.
    /// </summary>
    /// <remarks>
    /// Every reinterpret here assumes the little-endian layout Arrow's decimal buffers are defined
    /// to use and that <see cref="Int128"/> and <see cref="Int256"/> happen to share. .NET does not
    /// currently ship a big-endian runtime, so this is a guard rather than a supported path — a
    /// wrong number on table data is worse than a refusal.
    /// </remarks>
    private static void RequireLittleEndian()
    {
        if (!BitConverter.IsLittleEndian)
            throw new NotSupportedException("wide decimal arithmetic requires a little-endian host");
    }
}
