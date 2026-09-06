// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Reading values out of Arrow arrays, and building them back, for the Spark kernels.
/// </summary>
internal static class SparkArrays
{
    private static CultureInfo Invariant => CultureInfo.InvariantCulture;

    /// <summary>A value about to be cast, kept in whichever form is faithful to its source.</summary>
    internal readonly struct CastInput
    {
        private readonly string? _text;
        private readonly IArrowArray? _array;
        private readonly int _index;

        /// <summary>A value whose text is already known, and cheap.</summary>
        /// <remarks>
        /// The boolean and temporal sources, where the rendering is a constant or an already
        /// formatted instant. Nothing is deferred because there is nothing to defer.
        /// </remarks>
        public CastInput(double number, decimal? exact, string text)
        {
            AsDouble = number;
            Exact = exact;
            _text = text;
            _array = null;
            _index = 0;
            IsNumeric = true;
            FromString = false;
        }

        /// <summary>A numeric cell whose text is rendered only if something asks for it.</summary>
        public CastInput(double number, decimal? exact, IArrowArray array, int index)
        {
            AsDouble = number;
            Exact = exact;
            _text = null;
            _array = array;
            _index = index;
            IsNumeric = true;
            FromString = false;
        }

        /// <summary>A binary value, which renders and takes part in no numeric cast.</summary>
        /// <remarks>
        /// Spark decodes the bytes as UTF-8 and replaces what is not valid rather than refusing,
        /// so <c>CAST(X'FF' AS STRING)</c> is U+FFFD. <see cref="System.Text.Encoding.UTF8"/>
        /// replaces on the same terms as Java's <c>new String(bytes, UTF_8)</c>.
        /// <para>
        /// <see cref="IsNumeric"/> is false, which is what makes every other cast refuse it: a
        /// binary is not a number in Spark either, and reading its rendering as one would accept
        /// <c>CAST(X'3132' AS INT)</c> as 12.
        /// </para>
        /// </remarks>
        public static CastInput FromBinary(byte[] bytes) => new(bytes);

        private CastInput(byte[] bytes)
        {
            _text = System.Text.Encoding.UTF8.GetString(bytes);
            _array = null;
            _index = 0;
            FromString = false;
            IsNumeric = false;
            AsDouble = 0d;
            Exact = null;
        }

        public CastInput(string text)
        {
            _text = text;
            _array = null;
            _index = 0;
            FromString = true;
            IsNumeric = double.TryParse(
                text.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var asDouble);
            AsDouble = IsNumeric ? asDouble : 0d;
            Exact = IsNumeric
                && decimal.TryParse(text.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : null;
        }

        /// <summary>
        /// The value as an exact decimal, or null when it lies outside <see cref="decimal"/>'s
        /// range.
        /// </summary>
        /// <remarks>
        /// A double reaches roughly 1.8e308 where a decimal stops near 7.9e28, so
        /// <c>CAST(1e30 AS INT)</c> has a perfectly good source value with no decimal form. It
        /// must still be refused as CAST_OVERFLOW rather than escaping as a raw
        /// <see cref="OverflowException"/> — a crash on table metadata is exactly the failure
        /// mode the fail-closed design exists to avoid.
        /// </remarks>
        public decimal? Exact { get; }

        /// <summary>Whether the value arrived as text rather than as a number.</summary>
        /// <remarks>
        /// It changes what an integral cast accepts UNDER ANSI. A number truncates toward zero,
        /// so <c>CAST(1.7 AS INT)</c> is 1, while a string must already be an integer and
        /// <c>CAST('12.5' AS INT)</c> is refused rather than becoming 12. Both measured — but the
        /// refusal is ANSI's alone: the legacy dialect truncates a string too, and answers 12.
        /// </remarks>
        public bool FromString { get; }

        public double AsDouble { get; }

        /// <summary>
        /// The source rendered as Spark would render it, for error messages and casts to string.
        /// </summary>
        /// <remarks>
        /// <b>Rendered on demand for a numeric source, which is what #251 was.</b> It used to be
        /// formatted for every row of every cast, and only two things ever read it: a cast whose
        /// target is a string, where it is the answer, and an error message, which fires on a row
        /// that is being refused. <c>CAST(g AS INT)</c> over a million rows was formatting a
        /// million doubles and discarding all of them — measured at 265 MB of the 1M-row cast's
        /// allocation.
        /// <para>
        /// Nothing memoises it, because a <c>readonly struct</c> has nowhere to put the result.
        /// That costs nothing: every path that reads it reads it once, on the row it is about to
        /// refuse or convert.
        /// </para>
        /// <para>
        /// Holding the array also keeps it alive across the deferral, which is what the span
        /// taken inside <see cref="Render"/> needs — see <c>doc/arrow-span-lifetime.md</c>.
        /// </para>
        /// </remarks>
        public string Text => _text ?? Render(_array!, _index, Exact);

        /// <summary>Whether the value can take part in a numeric cast at all.</summary>
        public bool IsNumeric { get; }

        /// <summary>The instant this value denotes, for a date or timestamp source.</summary>
        public DateTimeOffset? Instant { get; init; }

        /// <summary>Whether the source was a calendar date rather than a timestamp.</summary>
        /// <remarks>
        /// The two render differently and cast differently: a timestamp becomes epoch seconds
        /// as a number, while Spark refuses a date-to-integer cast outright.
        /// </remarks>
        public bool IsDate { get; init; }
    }

    public static long? ReadInt64(IArrowArray array, int index) => array switch
    {
        Int8Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int16Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int32Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int64Array a => a.IsNull(index) ? null : a.GetValue(index),
        _ => throw new NotSupportedException(
            $"{array.Data.DataType.Name} is not an integral array"),
    };

    public static double? ReadDouble(IArrowArray array, int index) => array switch
    {
        Int8Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int16Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int32Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int64Array a => a.IsNull(index) ? null : a.GetValue(index),
        FloatArray a => a.IsNull(index) ? null : a.GetValue(index),
        DoubleArray a => a.IsNull(index) ? null : a.GetValue(index),
        Decimal128Array a => a.IsNull(index) ? null : WideDecimalAsDouble(a, index),
        _ => throw new NotSupportedException(
            $"{array.Data.DataType.Name} is not a numeric array"),
    };

    /// <summary>
    /// A Decimal128 cell as a double, going through the unscaled integer when the value is too
    /// wide for <see cref="decimal"/>.
    /// </summary>
    /// <remarks>
    /// Spark decimals reach precision 38 where <see cref="decimal"/> stops near 7.9e28, so
    /// <c>Decimal128Array.GetValue</c> raises on a legitimate column value. Converting to double
    /// is lossy either way, so the fallback costs nothing that the target type was going to keep.
    /// </remarks>
    private static double WideDecimalAsDouble(Decimal128Array array, int index)
    {
        try
        {
            return (double)array.GetValue(index)!.Value;
        }
        catch (OverflowException)
        {
            var scale = ((Decimal128Type)array.Data.DataType).Scale;
            return (double)Unscaled(array, index) / Math.Pow(10, scale);
        }
    }

    /// <summary>The raw unscaled integer behind a Decimal128 cell.</summary>
    internal static System.Numerics.BigInteger Unscaled(Decimal128Array array, int index)
    {
        // GC.KeepAlive because `array` is otherwise dead once the span is taken, and the span
        // points into its buffer. See doc/arrow-span-lifetime.md.
        var bytes = array.ValueBuffer.Span.Slice(index * 16, 16).ToArray();
        GC.KeepAlive(array);
#if NETSTANDARD2_0
        return new System.Numerics.BigInteger(bytes);
#else
        return new System.Numerics.BigInteger(bytes, isUnsigned: false, isBigEndian: false);
#endif
    }

    public static decimal? ReadDecimal(IArrowArray array, int index) => array switch
    {
        Int8Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int16Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int32Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int64Array a => a.IsNull(index) ? null : a.GetValue(index),
        FloatArray a => a.IsNull(index) ? null : (decimal?)a.GetValue(index),
        DoubleArray a => a.IsNull(index) ? null : (decimal?)a.GetValue(index),
        Decimal128Array a => a.IsNull(index) ? null : ExactDecimal(a, index),
        _ => throw new NotSupportedException(
            $"{array.Data.DataType.Name} is not a numeric array"),
    };

    /// <summary>
    /// A Decimal128 cell as an exact <see cref="decimal"/>, refusing when it does not fit.
    /// </summary>
    /// <remarks>
    /// Decimal arithmetic no longer runs through <see cref="decimal"/>: <c>SparkWideDecimals</c>
    /// computes on the unscaled integer and covers Spark's full precision 38. This stays for the
    /// callers that want a <see cref="decimal"/> specifically, where a value past decimal's
    /// ceiling near 7.9e28 has no exact form at all.
    /// <para>
    /// Refusing rather than rounding is load-bearing in both directions. It is what lets equality
    /// fall back to a double comparison when one side is a float and cannot be exact anyway, and
    /// it is what keeps a cast needing exactness fail-closed — a CHECK constraint that silently
    /// rounds is worse than one that refuses.
    /// </para>
    /// </remarks>
    private static decimal ExactDecimal(Decimal128Array array, int index)
    {
        try
        {
            return array.GetValue(index)!.Value;
        }
        catch (OverflowException)
        {
            var type = (Decimal128Type)array.Data.DataType;
            throw new NotSupportedException(
                $"a {Describe(type)} value is too wide for an exact System.Decimal; " +
                "a caller that can degrade to a double does so, and one needing exactness refuses");
        }
    }

    /// <summary>The Unix epoch, as the instant a Date32 counts days from.</summary>
    private static readonly DateTimeOffset Epoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The instant a temporal cell denotes, or null if the array is not temporal.</summary>
    public static DateTimeOffset? ReadInstant(IArrowArray array, int index) => array switch
    {
        // Date32 counts days from the epoch, and a calendar date is UTC midnight of that day —
        // which is how ArrowRowEvaluator already reads one, so literals and columns agree.
        Date32Array a => a.IsNull(index) ? null : Epoch.AddDays(a.GetValue(index)!.Value),
        Date64Array a => a.IsNull(index) ? null : Epoch.AddMilliseconds(a.GetValue(index)!.Value),
        TimestampArray a => a.IsNull(index) ? null : a.GetTimestamp(index),
        _ => null,
    };

    public static bool IsTemporal(IArrowType type) =>
        type is Date32Type or Date64Type or TimestampType;

    public static bool IsDateType(IArrowType type) => type is Date32Type or Date64Type;

    /// <summary>Reads a value for casting, keeping strings as strings.</summary>
    public static CastInput? ReadForCast(IArrowArray array, int index)
    {
        if (IsTemporal(array.Data.DataType))
        {
            var instant = ReadInstant(array, index);
            if (instant is null)
                return null;

            var isDate = IsDateType(array.Data.DataType);
            return new CastInput(
                instant.Value.ToUnixTimeSeconds(), instant.Value.ToUnixTimeSeconds(),
                RenderInstant(instant.Value, isDate))
            {
                Instant = instant,
                IsDate = isDate,
            };
        }

        if (array is StringArray strings)
            return strings.IsNull(index) ? null : new CastInput(strings.GetString(index));

        // AFTER the string case, deliberately: Apache.Arrow's StringArray derives from
        // BinaryArray, so this pattern matches one too and would take its bytes instead of its
        // text. Every other cast refuses a binary, which is what CastInput.FromBinary encodes.
        if (array is BinaryArray binary)
        {
            return binary.IsNull(index)
                ? null
                : CastInput.FromBinary(binary.GetBytes(index).ToArray());
        }

        if (array is BooleanArray booleans)
        {
            if (booleans.IsNull(index)) return null;
            var flag = booleans.GetValue(index)!.Value;
            return new CastInput(flag ? 1d : 0d, flag ? 1m : 0m, flag ? "true" : "false");
        }

        var asDouble = ReadDouble(array, index);
        if (asDouble is null)
            return null;

        // Only in-range values get an exact form; the rest travel as a double and are refused by
        // whichever cast needs exactness.
        //
        // The bound is what makes an unguarded ReadDecimal safe here, and the two are a pair. The
        // ONLY condition under which reading an exact decimal raises is a magnitude past
        // System.Decimal's ceiling of ~7.9228e28, and this bound is stricter than that, so nothing
        // that passes it can raise. Excess significant DIGITS do not raise — Decimal128Array
        // rounds them to 28 and reports success. Loosen this bound and the exception becomes
        // reachable again.
        //
        // Both limits are why rendering no longer consults this value: it is null past the bound
        // and quietly rounded inside it, so Render works from the buffer instead. What remains
        // here serves the casts that need an exact System.Decimal specifically, and those refuse
        // when it is null rather than rendering anything.
        decimal? exact = asDouble.Value is >= -7.9e28 and <= 7.9e28
            ? ReadDecimal(array, index)
            : null;

        return new CastInput(asDouble.Value, exact, array, index);
    }

    /// <summary>Renders an instant the way Spark prints it.</summary>
    /// <remarks>
    /// Measured: a timestamp prints as <c>2026-08-11 03:00:00</c> and a date as
    /// <c>2026-08-11</c>, both in the resolved timezone.
    /// </remarks>
    public static string RenderInstant(DateTimeOffset instant, bool isDate)
    {
        var local = TimeZoneInfo.ConvertTime(instant, SparkDialectOptions.TimeZone);
        return isDate
            ? local.ToString("yyyy-MM-dd", Invariant)
            : local.ToString("yyyy-MM-dd HH:mm:ss", Invariant);
    }

    /// <summary>Builds a Date32 array from instants, taking the calendar date in the zone.</summary>
    public static IArrowArray BuildDate32(DateTimeOffset?[] values, int rowCount)
    {
        var builder = new Date32Array.Builder();
        for (var i = 0; i < rowCount; i++)
        {
            if (values[i] is { } instant)
            {
                var local = TimeZoneInfo.ConvertTime(instant, SparkDialectOptions.TimeZone);
                builder.Append(new DateTimeOffset(local.Date, TimeSpan.Zero));
            }
            else
            {
                builder.AppendNull();
            }
        }

        return builder.Build();
    }

    /// <summary>Builds a microsecond UTC timestamp array from instants.</summary>
    public static IArrowArray BuildTimestamp(DateTimeOffset?[] values, int rowCount)
    {
        var builder = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        for (var i = 0; i < rowCount; i++)
        {
            if (values[i] is { } instant) builder.Append(instant);
            else builder.AppendNull();
        }

        return builder.Build();
    }

    /// <summary>Renders a numeric cell the way Spark would print it.</summary>
    /// <remarks>
    /// Floating point goes through <see cref="SparkFloatText"/>, which reproduces Java's own
    /// spelling — where the exponent starts, the digit that always follows the point, and an
    /// unsigned exponent. Measured: every row of the corpus's <c>float-to-string</c> group is
    /// exactly what <c>Double.toString</c> or <c>Float.toString</c> prints, and .NET's <c>"R"</c>
    /// matched almost none of it. #248.
    /// <para>
    /// A decimal renders from its unscaled integer and scale rather than from the
    /// <see cref="decimal"/> exact form, because that form covers only part of the range: past
    /// decimal's ceiling it is null, and inside the ceiling it silently rounds a value carrying
    /// more than 28 significant digits. Rendering from the buffer is exact across all of
    /// precision 38, and is byte-identical to the old rendering everywhere the old one was
    /// correct — verified over signs, trailing zeros and every scale.
    /// </para>
    /// </remarks>
    private static string Render(IArrowArray array, int index, decimal? value) => array switch
    {
        FloatArray a => SparkFloatText.Render(a.GetValue(index)!.Value),
        DoubleArray a => SparkFloatText.Render(a.GetValue(index)!.Value),
        Decimal128Array => SparkWideDecimals.Render(SparkWideDecimals.Read(array, index)!.Value),

        // Integral arrays only, and their exact form is never null: the widest is Int64 at about
        // 9.2e18, far inside the bound that decides whether an exact form is taken at all. There
        // is deliberately no placeholder string here — emitting one as a value is what #175 was.
        _ => value?.ToString(Invariant)
            ?? throw new NotSupportedException(
                $"{array.Data.DataType.Name} reached Render with no exact value"),
    };

    /// <summary>Whether an integral Arrow type is narrower than <c>int</c>.</summary>
    /// <remarks>Spark reports overflow of those under a different error class.</remarks>
    public static bool NarrowerThanInt(IArrowType type) => type is Int8Type or Int16Type;

    /// <summary>Whether <paramref name="value"/> fits the width of an integral Arrow type.</summary>
    public static bool FitsIn(long value, IArrowType type) => type switch
    {
        Int8Type => value is >= sbyte.MinValue and <= sbyte.MaxValue,
        Int16Type => value is >= short.MinValue and <= short.MaxValue,
        Int32Type => value is >= int.MinValue and <= int.MaxValue,
        Int64Type => true,
        _ => throw new NotSupportedException($"{type.Name} is not integral"),
    };

    /// <summary>Wraps a value into an integral type's width, for the non-ANSI dialect.</summary>
    public static long Truncate(long value, IArrowType type) => type switch
    {
        Int8Type => unchecked((sbyte)value),
        Int16Type => unchecked((short)value),
        Int32Type => unchecked((int)value),
        _ => value,
    };

    public static IArrowArray BuildIntegral(long?[] values, IArrowType type, int rowCount)
    {
        switch (type)
        {
            case Int8Type:
                var i8 = new Int8Array.Builder();
                for (var i = 0; i < rowCount; i++)
                {
                    if (values[i] is { } v) i8.Append((sbyte)v); else i8.AppendNull();
                }

                return i8.Build();

            case Int16Type:
                var i16 = new Int16Array.Builder();
                for (var i = 0; i < rowCount; i++)
                {
                    if (values[i] is { } v) i16.Append((short)v); else i16.AppendNull();
                }

                return i16.Build();

            case Int32Type:
                var i32 = new Int32Array.Builder();
                for (var i = 0; i < rowCount; i++)
                {
                    if (values[i] is { } v) i32.Append((int)v); else i32.AppendNull();
                }

                return i32.Build();

            default:
                var i64 = new Int64Array.Builder();
                for (var i = 0; i < rowCount; i++)
                {
                    if (values[i] is { } v) i64.Append(v); else i64.AppendNull();
                }

                return i64.Build();
        }
    }

    /// <summary>
    /// Brings a value to an exact scale, rounding half away from zero as Spark does.
    /// </summary>
    public static decimal Rescale(decimal value, int scale) =>
        Math.Round(value, Math.Min(scale, 28), MidpointRounding.AwayFromZero);

    /// <summary>The Spark spelling of an Arrow type, for error messages.</summary>
    public static string Describe(IArrowType type) => type switch
    {
        Int8Type => "TINYINT",
        Int16Type => "SMALLINT",
        Int32Type => "INT",
        Int64Type => "BIGINT",
        FloatType => "FLOAT",
        DoubleType => "DOUBLE",
        StringType => "STRING",
        BooleanType => "BOOLEAN",
        Decimal128Type d => $"DECIMAL({d.Precision},{d.Scale})",
        Decimal256Type d => $"DECIMAL({d.Precision},{d.Scale})",
        _ => type.Name.ToUpperInvariant(),
    };

    /// <summary>Parses a cast target as the parser spelled it, e.g. <c>DECIMAL(10,2)</c>.</summary>
    public static IArrowType ParseTypeName(string name)
    {
        var text = name.Trim();

        var open = text.IndexOf('(');
        if (open >= 0)
        {
            var head = text[..open].Trim();
            if (!head.Equals("decimal", StringComparison.OrdinalIgnoreCase)
                && !head.Equals("dec", StringComparison.OrdinalIgnoreCase)
                && !head.Equals("numeric", StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException($"cast to '{text}' is not implemented");
            }

            // Validated rather than sliced on faith: this is reachable from the public
            // IFunctionRegistry surface, not only from the parser, so a malformed spelling must
            // produce a message naming the problem instead of an ArgumentOutOfRangeException or
            // a FormatException from somewhere inside.
            var close = text.LastIndexOf(')');
            if (close < open)
                throw new NotSupportedException($"cast target '{text}' is missing its closing ')'");

            var parts = text.Substring(open + 1, close - open - 1).Split(',');
            if (parts.Length > 2
                || !int.TryParse(parts[0].Trim(), NumberStyles.Integer, Invariant, out var precision)
                || (parts.Length == 2
                    && !int.TryParse(parts[1].Trim(), NumberStyles.Integer, Invariant, out _)))
            {
                throw new NotSupportedException(
                    $"cast target '{text}' does not have a valid precision and scale");
            }

            var scale = parts.Length > 1 ? int.Parse(parts[1].Trim(), Invariant) : 0;

            if (precision is < 1 or > SparkNumericTypes.MaxPrecision || scale < 0 || scale > precision)
            {
                throw new NotSupportedException(
                    $"cast target '{text}' is outside the supported range " +
                    $"(precision 1..{SparkNumericTypes.MaxPrecision}, scale 0..precision)");
            }

            return new Decimal128Type(precision, scale);
        }

        return text.ToUpperInvariant() switch
        {
            "TINYINT" or "BYTE" => Int8Type.Default,
            "SMALLINT" or "SHORT" => Int16Type.Default,
            "INT" or "INTEGER" => Int32Type.Default,
            "BIGINT" or "LONG" => Int64Type.Default,
            "FLOAT" or "REAL" => FloatType.Default,
            "DOUBLE" => DoubleType.Default,
            "STRING" => StringType.Default,
            "BOOLEAN" or "BOOL" => BooleanType.Default,
            "DATE" => Date32Type.Default,
            // Microseconds in UTC, matching what the readers produce and the fixed timezone
            // policy. TIMESTAMP_NTZ is a distinct Spark type and is deliberately not aliased
            // here: it has no offset at all, and pretending otherwise would silently reinterpret
            // values rather than refuse them.
            "TIMESTAMP" => new TimestampType(TimeUnit.Microsecond, "UTC"),
            "DECIMAL" or "DEC" or "NUMERIC" => new Decimal128Type(10, 0),
            _ => throw new NotSupportedException($"cast to '{text}' is not implemented"),
        };
    }
}
