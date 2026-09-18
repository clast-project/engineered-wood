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

        /// <summary>A binary cell, decoded only if something asks for its text.</summary>
        /// <remarks>
        /// Spark decodes the bytes as UTF-8 and replaces what is not valid rather than refusing,
        /// so <c>CAST(X'FF' AS STRING)</c> is U+FFFD. <see cref="System.Text.Encoding.UTF8"/>
        /// replaces on the same terms as Java's <c>new String(bytes, UTF_8)</c>.
        /// <para>
        /// Decoding is deferred for the reason <see cref="Text"/> gives: only a cast to string and
        /// an error message read the text, so decoding here would allocate a string per row for
        /// every cast that then refuses it. Holding the array rather than a copy of the bytes
        /// also keeps it alive for the span <see cref="Render"/> takes.
        /// </para>
        /// <para>
        /// <see cref="IsNumeric"/> is false, which makes every other cast refuse it: a binary is
        /// not a number in Spark either, and reading its rendering as one would accept
        /// <c>CAST(X'3132' AS INT)</c> as 12.
        /// </para>
        /// </remarks>
        public static CastInput FromBinary(IArrowArray array, int index) => new(array, index);

        private CastInput(IArrowArray array, int index)
        {
            _text = null;
            _array = array;
            _index = index;
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

            // Trimmed once and shared: both parses want the same text, and this runs for every
            // row of every cast and comparison over a string column.
            //
            // Spark's trim, not .NET's; the two sets cross rather than nest (see
            // SparkText.TrimBounds). .NET's number parser skips only 0x20 and 0x09-0x0D on its
            // own, all of which this has already removed, so it sees exactly what Spark's parse
            // would. The temporal parses trim for themselves.
            //
            // A span on net8.0 and net10.0; only the netstandard2.0 build takes the string, and
            // only it allocates, for a padded cell. The two TryParse calls below are the same
            // source on both sides: `trimmed` is a string there and a ReadOnlySpan<char> here, and
            // each binds to its own overload -- the same trap as TryReadTypeSuffixed and
            // DecodeUtf8 below.
#if NETSTANDARD2_0
            var trimmed = SparkText.Trim(text);
#else
            var trimmed = SparkText.Trim(text.AsSpan());
#endif
            // Through SparkDoubleText, not double.TryParse: .NET Framework refuses a magnitude too
            // large to represent where .NET Core and Java both return an infinity, and through
            // IsNumeric that refusal would decide the error class of every other numeric cast from
            // this string too.
            IsNumeric = SparkDoubleText.TryParse(trimmed, out var asDouble);
            AsDouble = IsNumeric
                ? WithSignOfZero(asDouble, trimmed.Length > 0 && trimmed[0] == '-')
                : 0d;
            Exact = IsNumeric
                && decimal.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
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
        /// It changes what an integral cast accepts under ANSI. A number truncates toward zero,
        /// so <c>CAST(1.7 AS INT)</c> is 1, while a string must already be an integer and
        /// <c>CAST('12.5' AS INT)</c> is refused. The legacy dialect truncates a string too, and
        /// answers 12.
        /// </remarks>
        public bool FromString { get; }

        public double AsDouble { get; }

        /// <summary>
        /// The source rendered as Spark would render it, for error messages and casts to string.
        /// </summary>
        /// <remarks>
        /// Rendered on demand for a numeric source. Only two things read it — a cast whose target
        /// is a string, where it is the answer, and an error message on a row being refused — so
        /// formatting it eagerly would format and discard a double per row: 265 MB of allocation
        /// over a 1M-row <c>CAST(g AS INT)</c>.
        /// <para>
        /// Nothing memoises it, because a <c>readonly struct</c> has nowhere to put the result;
        /// every path that reads it reads it once. Holding the array keeps it alive across the
        /// deferral, which the span taken inside <see cref="Render"/> needs — see
        /// <c>doc/arrow-span-lifetime.md</c>.
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
        // A bare NULL literal, which Spark types `void`: null at every row, and every reader here
        // answers so rather than refusing.
        NullArray => null,
        Int8Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int16Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int32Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int64Array a => a.IsNull(index) ? null : a.GetValue(index),
        _ => throw new NotSupportedException(
            $"{array.Data.DataType.Name} is not an integral array"),
    };

    /// <summary>A numeric cell as a <see cref="float"/>, rounded once.</summary>
    /// <remarks>
    /// <para>
    /// A bigint and a decimal are converted directly, not through <see cref="ReadDouble"/>.
    /// Rounding to 53 bits and then to 24 can land exactly on a tie the direct conversion does not
    /// see: 2^60 + 2^36 + 1 is 2^60 + 2^37 as a float in Spark (Java's <c>(float) long</c>), and
    /// 2^60 by way of a double. .NET's <c>(float) long</c> is correctly rounded on every target
    /// framework -- checked over ten million random longs on .NET 10, and on this value on .NET
    /// Framework. A decimal rounds once from its exact value, as Java's
    /// <c>BigDecimal.floatValue</c> does.
    /// </para>
    /// <para>
    /// The narrower integrals and a float are exact in a double, and a double rounds once
    /// either way, so those keep the double route.
    /// </para>
    /// </remarks>
    public static float? ReadFloat(IArrowArray array, int index) => array switch
    {
        Int64Array a => a.IsNull(index) ? null : a.GetValue(index)!.Value,

        Decimal128Array a => a.IsNull(index)
            ? null
            : ScaledDecimal.ToSingle(Unscaled(a, index), ((Decimal128Type)a.Data.DataType).Scale),

        _ => ReadDouble(array, index) is { } value ? (float)value : null,
    };

    /// <summary>
    /// A numeric string as a <see cref="float"/>, read once from the text as Java's
    /// <c>Float.parseFloat</c> reads it, after Spark's trim.
    /// </summary>
    /// <remarks>
    /// For a string <see cref="CastInput"/> has already decided is numeric; it holds only the
    /// double reading, and narrowing that would round twice.
    /// </remarks>
    public static bool TryReadFloat(string text, out float value)
    {
        var trimmed = SparkText.Trim(text);
        var parsed = SparkDoubleText.TryParseSingle(trimmed, out value);
        if (parsed)
            value = WithSignOfZero(value, trimmed.Length > 0 && trimmed[0] == '-');

        return parsed;
    }

    public static double? ReadDouble(IArrowArray array, int index) => array switch
    {
        NullArray => null,
        Int8Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int16Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int32Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int64Array a => a.IsNull(index) ? null : a.GetValue(index),
        FloatArray a => a.IsNull(index) ? null : a.GetValue(index),
        DoubleArray a => a.IsNull(index) ? null : a.GetValue(index),
        Decimal128Array a => a.IsNull(index) ? null : DecimalAsDouble(a, index),
        _ => throw new NotSupportedException(
            $"{array.Data.DataType.Name} is not a numeric array"),
    };

    /// <summary>A Decimal128 cell as the nearest <see cref="double"/>.</summary>
    /// <remarks>
    /// Through the unscaled integer for every width, not only the wide ones: do not shortcut
    /// through <c>(double)GetValue(index)</c>, because <c>(double)decimal</c> is an ulp off on
    /// 17.4% of the values it accepts.
    /// <see cref="ScaledDecimal.ToDouble(System.Numerics.BigInteger, int)"/> carries the whole
    /// range, with an exact fast path for the ordinary narrow decimal.
    /// </remarks>
    private static double DecimalAsDouble(Decimal128Array array, int index) =>
        ScaledDecimal.ToDouble(Unscaled(array, index), ((Decimal128Type)array.Data.DataType).Scale);

    /// <summary>The raw unscaled integer behind a Decimal128 cell.</summary>
    internal static System.Numerics.BigInteger Unscaled(Decimal128Array array, int index)
    {
        // GC.KeepAlive because `array` is otherwise dead once the span is taken, and the span
        // points into its buffer. See doc/arrow-span-lifetime.md.
#if NETSTANDARD2_0
        var bytes = array.ValueBuffer.Span.Slice(index * 16, 16).ToArray();
        GC.KeepAlive(array);
        return new System.Numerics.BigInteger(bytes);
#else
        // No ToArray: the span overload consumes the bytes inside the call, and the KeepAlive
        // below already covers the span. Every decimal-to-double conversion comes through here,
        // so a copy would be an allocation per row.
        var value = new System.Numerics.BigInteger(
            array.ValueBuffer.Span.Slice(index * 16, 16), isUnsigned: false, isBigEndian: false);
        GC.KeepAlive(array);
        return value;
#endif
    }

    public static decimal? ReadDecimal(IArrowArray array, int index) => array switch
    {
        NullArray => null,
        Int8Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int16Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int32Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int64Array a => a.IsNull(index) ? null : a.GetValue(index),
        FloatArray a => a.IsNull(index) ? null : ExactDecimal(a.GetValue(index)!.Value, a),
        DoubleArray a => a.IsNull(index) ? null : ExactDecimal(a.GetValue(index)!.Value, a),
        Decimal128Array a => a.IsNull(index) ? null : ExactDecimal(a, index),
        _ => throw new NotSupportedException(
            $"{array.Data.DataType.Name} is not a numeric array"),
    };

    /// <summary>
    /// A floating-point value as an exact <see cref="decimal"/>, refusing when it has no such form.
    /// </summary>
    /// <remarks>
    /// Refuses with the same exception as <see cref="ExactDecimal(Decimal128Array,int)"/>. A
    /// checked conversion from a double signals out-of-range with
    /// <see cref="OverflowException"/>, but callers such as <see cref="SparkFunctions.AreEqual"/>
    /// catch only <see cref="NotSupportedException"/>, so letting it through would escape the
    /// evaluator as a bare BCL exception.
    /// <para>
    /// Everything <see cref="decimal"/> cannot hold arrives here: a magnitude past its ceiling near
    /// 7.9228e28, and every NaN and infinity. A caught conversion rather than a range test,
    /// because the boundary is <see cref="decimal"/>'s own and the conversion already knows it
    /// exactly; it costs an exception only on values that have no exact form anyway.
    /// </para>
    /// </remarks>
    private static decimal ExactDecimal(double value, IArrowArray array)
    {
        try
        {
            return (decimal)value;
        }
        catch (OverflowException)
        {
            throw new NotSupportedException(
                $"a {array.Data.DataType.Name} value of {value} has no exact System.Decimal form; " +
                "a caller that can degrade to a double does so, and one needing exactness refuses");
        }
    }

    /// <summary>
    /// A Decimal128 cell as an exact <see cref="decimal"/>, refusing when it does not fit.
    /// </summary>
    /// <remarks>
    /// Decimal arithmetic does not run through <see cref="decimal"/>: <c>SparkWideDecimals</c>
    /// computes on the unscaled integer and covers Spark's full precision 38. This serves the
    /// callers that want a <see cref="decimal"/> specifically, where a value past decimal's
    /// ceiling near 7.9e28 has no exact form.
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

    /// <summary>The one zoned timestamp type this evaluator produces.</summary>
    /// <remarks>
    /// <para>
    /// Microseconds in UTC, which is what <see cref="BuildTimestamp(DateTimeOffset?[], int)"/>
    /// builds. A rule that resolves a zoned timestamp must name this instance rather than
    /// construct its own or echo a source column's type: a source may carry another unit or zone
    /// (Parquet writes milliseconds), and echoing it would promise a type the built array does not
    /// have. With <see cref="NaiveTimestamp"/>, these are the only two timestamp types anything
    /// here produces; a source column's type is only read for its zone.
    /// </para>
    /// <para>
    /// <c>ArrowRowEvaluator</c> spells the same type out for itself when it materialises a
    /// <c>TIMESTAMP'…'</c> literal: it is registry-agnostic, and reaching into the Spark namespace
    /// for a constant would couple it to the dialect it dispatches to.
    /// </para>
    /// </remarks>
    public static readonly TimestampType Timestamp = new(TimeUnit.Microsecond, "UTC");

    /// <summary>The one naive timestamp type this evaluator produces — Spark's TIMESTAMP_NTZ.</summary>
    /// <remarks>
    /// <para>
    /// Microseconds with no zone, which is how Delta's <c>SchemaConverter</c> spells
    /// <c>timestamp_ntz</c> in Arrow. A rule that resolves a naive timestamp must name this
    /// instance and build with it through
    /// <see cref="BuildTimestamp(DateTimeOffset?[], int, TimestampType)"/>. Under the pinned UTC
    /// zone a naive and a zoned array hold the same micros, so only the label differs, but a
    /// Delta generated column takes its schema from the array's type: a naive result labelled
    /// UTC would silently turn a naive column into a zoned one.
    /// </para>
    /// <para>
    /// The two are not interchangeable to Spark, and the corpus cannot see the difference:
    /// <c>coalesce(ntz, dt)</c> is <c>timestamp_ntz</c> while <c>coalesce(ntz, ts)</c> is zoned —
    /// a naive operand promotes to zoned once a zoned one is present. Under
    /// <c>America/Los_Angeles</c>, <c>CAST(ntz AS TIMESTAMP)</c> reinterprets the wall clock in the
    /// session zone exactly as a string cast does.
    /// </para>
    /// </remarks>
    public static readonly TimestampType NaiveTimestamp =
        new(TimeUnit.Microsecond, (string?)null);

    /// <summary>A timestamp that carries a zone, as opposed to Spark's TIMESTAMP_NTZ.</summary>
    /// <remarks>
    /// An empty zone string — which the Delta converter never produces but nothing here rules
    /// out — reads as naive, because that direction cannot silently relabel a wall clock as an
    /// instant.
    /// </remarks>
    public static bool IsZonedTimestamp(IArrowType type) =>
        type is TimestampType timestamp && !string.IsNullOrEmpty(timestamp.Timezone);

    /// <summary>A timestamp with no zone — Spark's TIMESTAMP_NTZ.</summary>
    public static bool IsNaiveTimestamp(IArrowType type) =>
        type is TimestampType && !IsZonedTimestamp(type);

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
        // First, because a `void` column has no value at any row and every later branch would
        // have to answer the same thing.
        if (array is NullArray)
            return null;

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

        // After the string case: Apache.Arrow's StringArray derives from
        // BinaryArray, so this pattern matches one too and would take its bytes instead of its
        // text. Every other cast refuses a binary, which is what CastInput.FromBinary encodes.
        if (array is BinaryArray binary)
            return binary.IsNull(index) ? null : CastInput.FromBinary(binary, index);

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
        // The bound is what makes an unguarded ReadDecimal safe here. The only condition under
        // which reading an exact decimal raises is a magnitude past System.Decimal's ceiling of
        // ~7.9228e28, and this bound is stricter, so nothing that passes it can raise. Excess
        // significant digits do not raise — Decimal128Array rounds them to 28 and reports
        // success. Loosening this bound makes the exception reachable.
        //
        // Since the value is null past the bound and quietly rounded inside it, Render works from
        // the buffer instead; this serves only the casts that need an exact System.Decimal, and
        // those refuse when it is null.
        decimal? exact = asDouble.Value is >= -7.9e28 and <= 7.9e28
            ? ReadDecimal(array, index)
            : null;

        return new CastInput(asDouble.Value, exact, array, index);
    }

    /// <summary>Renders an instant the way Spark prints it.</summary>
    /// <remarks>
    /// A timestamp prints as <c>2026-08-11 03:00:00</c> and a date as <c>2026-08-11</c>, both in
    /// the resolved timezone. The sub-second is printed without its trailing zeros:
    /// <c>.100000</c> prints as <c>.1</c>, <c>.010000</c> as <c>.01</c>, <c>.123400</c> as
    /// <c>.1234</c>, <c>.000001</c> as <c>.000001</c> and <c>.000000</c> not at all. A corpus row
    /// cannot tell a fraction dropped here from one lost in the parse.
    /// </remarks>
    public static string RenderInstant(DateTimeOffset instant, bool isDate)
    {
        var local = TimeZoneInfo.ConvertTime(instant, SparkDialectOptions.TimeZone);
        if (isDate)
            return local.ToString("yyyy-MM-dd", Invariant);

        var seconds = local.ToString("yyyy-MM-dd HH:mm:ss", Invariant);
        var microseconds = (int)(local.Ticks % TimeSpan.TicksPerSecond / TicksPerMicrosecond);

        return microseconds == 0
            ? seconds
            : seconds + "." + SparkText.TrimTrailingZeros(microseconds.ToString("D6", Invariant));
    }

    /// <summary>The <see cref="DateTime"/> ticks in one microsecond.</summary>
    private const long TicksPerMicrosecond = 10L;

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
    public static IArrowArray BuildTimestamp(DateTimeOffset?[] values, int rowCount) =>
        BuildTimestamp(values, rowCount, Timestamp);

    /// <summary>Builds a microsecond timestamp array from instants, at <paramref name="type"/>.</summary>
    /// <remarks>
    /// The zone is only a label here: the micros written are the instants handed in, whichever
    /// type is asked for, because under the pinned UTC session zone a naive timestamp and a zoned
    /// one hold the same number for the same wall clock. A configurable session zone (#133) would
    /// make this a conversion rather than a label.
    /// </remarks>
    public static IArrowArray BuildTimestamp(
        DateTimeOffset?[] values, int rowCount, TimestampType type)
    {
        var builder = new TimestampArray.Builder(type);
        for (var i = 0; i < rowCount; i++)
        {
            if (values[i] is { } instant) builder.Append(instant);
            else builder.AppendNull();
        }

        return builder.Build();
    }

    /// <summary>
    /// Reads a floating literal carrying Java's trailing type suffix, as in <c>1d</c>.
    /// </summary>
    /// <remarks>
    /// The suffix belongs to Java's FloatingPointLiteral grammar, which requires digits, so it
    /// attaches to a numeric form and not to a named one: <c>'1e3d'</c> is 1000 and
    /// <c>'NaNd'</c> is refused. Requiring a digit or a point before the suffix is what draws
    /// that line -- measured, Spark refuses <c>'NaNd'</c>, <c>'Infinityf'</c>, <c>'1l'</c> and
    /// <c>'1 d'</c> alike.
    /// </remarks>
    public static bool TryReadTypeSuffixed(string text, out double value)
    {
        value = 0d;
        if (!IsTypeSuffixed(text, out var trimmed))
            return false;

        // Read without the suffix, and without copying the text to drop it. The span overload
        // lands on net8.0 and net10.0; netstandard2.0 has only the string one.
#if NETSTANDARD2_0
        var parsed = SparkDoubleText.TryParse(trimmed.Substring(0, trimmed.Length - 1), out value);
#else
        var parsed = SparkDoubleText.TryParse(trimmed.AsSpan(0, trimmed.Length - 1), out value);
#endif

        if (parsed)
            value = WithSignOfZero(value, trimmed[0] == '-');

        return parsed;
    }

    /// <summary>
    /// <see cref="TryReadTypeSuffixed(string, out double)"/> for a float target, rounded once from
    /// the text rather than narrowed from the double reading.
    /// </summary>
    public static bool TryReadTypeSuffixed(string text, out float value)
    {
        value = 0f;
        if (!IsTypeSuffixed(text, out var trimmed))
            return false;

#if NETSTANDARD2_0
        var parsed = SparkDoubleText.TryParseSingle(trimmed.Substring(0, trimmed.Length - 1), out value);
#else
        var parsed = SparkDoubleText.TryParseSingle(trimmed.AsSpan(0, trimmed.Length - 1), out value);
#endif

        if (parsed)
            value = WithSignOfZero(value, trimmed[0] == '-');

        return parsed;
    }

    private static bool IsTypeSuffixed(string text, out string trimmed)
    {
        trimmed = SparkText.Trim(text);
        if (trimmed.Length < 2)
            return false;

        var suffix = trimmed[trimmed.Length - 1];
        if (suffix is not ('d' or 'D' or 'f' or 'F'))
            return false;

        var previous = trimmed[trimmed.Length - 2];
        return (previous >= '0' && previous <= '9') || previous == '.';
    }

    /// <summary>A float with the sign its text carried; see the double overload.</summary>
    private static float WithSignOfZero(float value, bool negative) =>
        value == 0f && negative ? -0f : value;

    /// <summary>
    /// A parsed value with the sign its text carried, which only a zero can have lost.
    /// </summary>
    /// <remarks>
    /// .NET Framework's number parser returns a positive zero for <c>"-0.0"</c>, where .NET Core
    /// and Java both return the negative one; without this, <c>CAST('-0.0' AS DOUBLE)</c> renders
    /// as <c>0.0</c> under net472 and <c>-0.0</c> under net10.0, against Spark's <c>-0.0</c>.
    /// <para>
    /// Applied to every parse because it is a no-op wherever the parser already got it right: a
    /// leading <c>-</c> on text that reads as zero means a negative zero on every runtime. The sign
    /// has to come from the text rather than the parse — <c>-1e-400</c> underflows to a zero whose
    /// sign is not in the digits.
    /// </para>
    /// </remarks>
    private static double WithSignOfZero(double value, bool negative) =>
        // `-0d` is a compile-time flip of the sign bit. Not `0d - 0d`, which is a positive zero
        // under round-to-nearest -- the same trap SparkFunctionRegistry.NegateFloating avoids.
        // NegativeZeroTests asserts the constant really is negative, since a reader cannot tell
        // the two apart by looking.
        value == 0d && negative ? -0d : value;

    /// <summary>UTF-8 with replacement, straight off the buffer where the runtime allows it.</summary>
    /// <remarks>
    /// The span overload lands on net8.0 and net10.0; netstandard2.0 has only the array one, so
    /// that build alone pays for a copy.
    /// </remarks>
    private static string DecodeUtf8(ReadOnlySpan<byte> bytes)
    {
#if NETSTANDARD2_0
        return System.Text.Encoding.UTF8.GetString(bytes.ToArray());
#else
        return System.Text.Encoding.UTF8.GetString(bytes);
#endif
    }

    /// <summary>Renders a numeric cell the way Spark would print it.</summary>
    /// <remarks>
    /// Floating point goes through <see cref="SparkFloatText"/>, which reproduces Java's own
    /// spelling — where the exponent starts, the digit that always follows the point, and an
    /// unsigned exponent. .NET's <c>"R"</c> matches almost none of the corpus's
    /// <c>float-to-string</c> group.
    /// <para>
    /// A decimal renders from its unscaled integer and scale rather than from the
    /// <see cref="decimal"/> exact form, which is null past decimal's ceiling and, inside it,
    /// silently rounds a value carrying more than 28 significant digits. Rendering from the buffer
    /// is exact across all of precision 38.
    /// </para>
    /// </remarks>
    private static string Render(IArrowArray array, int index, decimal? value) => array switch
    {
        FloatArray a => SparkFloatText.Render(a.GetValue(index)!.Value),
        DoubleArray a => SparkFloatText.Render(a.GetValue(index)!.Value),
        Decimal128Array => SparkWideDecimals.Render(SparkWideDecimals.Read(array, index)!.Value),

        // A StringArray is a BinaryArray too, and never reaches here: its text is known when the
        // cell is read, so it takes the `_text` path instead of this one.
        BinaryArray a => DecodeUtf8(a.GetBytes(index)),

        // Integral arrays only, and their exact form is never null: the widest is Int64 at about
        // 9.2e18, far inside the bound that decides whether an exact form is taken at all. There
        // is deliberately no placeholder string here: it would be emitted as if it were the value.
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

    /// <summary>The array as bytes, or a refusal naming the type that cannot be read as binary.</summary>
    /// <remarks>
    /// A <see cref="StringArray"/> passes, deliberately: it derives from <see cref="BinaryArray"/>
    /// and its value buffer already holds the UTF-8, which is Spark's string-to-binary
    /// conversion — <c>CAST('é' AS BINARY)</c> is <c>C3A9</c>, and <c>CAST('' AS BINARY)</c> is
    /// empty bytes rather than null. So the derivation guarded against elsewhere in this file,
    /// where a string's text is wanted rather than its bytes, is exactly what is wanted here.
    /// <para>
    /// Every other source type is refused. The integral-to-binary cast, where the two dialects
    /// disagree, is handled by the cast itself and does not come through here.
    /// </para>
    /// </remarks>
    private static BinaryArray AsBinary(IArrowArray array) =>
        array as BinaryArray
        ?? throw new NotSupportedException(
            $"{Describe(array.Data.DataType)} cannot be read as binary");

    /// <summary>Appends a cell's bytes to <paramref name="builder"/>, or a null.</summary>
    /// <remarks>
    /// Takes the builder rather than returning the bytes so that nothing is materialised: this
    /// runs once per row, and returning a <c>byte[]</c> would allocate one per row and then copy
    /// it into the builder a second time. The span never leaves this method, which is also what
    /// keeps the <c>GC.KeepAlive</c> rule in <c>doc/arrow-span-lifetime.md</c> local and honest.
    /// </remarks>
    public static void AppendBytes(BinaryArray.Builder builder, IArrowArray array, int index)
    {
        // A `void` branch carries no bytes at any row, and is reachable here whenever the other
        // branch made the unified type binary -- `if(c, X'00', NULL)`.
        if (array is NullArray)
        {
            builder.AppendNull();
            return;
        }

        var bytes = AsBinary(array);

        if (bytes.IsNull(index))
        {
            builder.AppendNull();
        }
        else
        {
            builder.Append(bytes.GetBytes(index));
        }

        GC.KeepAlive(bytes);
    }

    /// <summary>
    /// Orders one row of two binary columns the way Spark orders a binary column.
    /// </summary>
    /// <remarks>
    /// Unsigned: <c>greatest(X'00', X'FF')</c> is <c>FF</c> and <c>greatest(X'7F', X'80')</c> is
    /// <c>80</c>. A signed reading gets both backwards, which is the mistake a port from Java
    /// invites — Java's <c>byte</c> is signed where .NET's is not. A shorter array that is a prefix
    /// of a longer one sorts first: <c>greatest(X'01', X'0100')</c> is <c>0100</c>.
    /// <para>
    /// Compares in place for the reason <see cref="AppendBytes"/> gives.
    /// </para>
    /// </remarks>
    public static int CompareBytes(IArrowArray left, IArrowArray right, int index)
    {
        var first = AsBinary(left);
        var second = AsBinary(right);

        var result = CompareBytes(first.GetBytes(index), second.GetBytes(index));

        GC.KeepAlive(first);
        GC.KeepAlive(second);
        return result;
    }

    private static int CompareBytes(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        var shared = Math.Min(left.Length, right.Length);
        for (var i = 0; i < shared; i++)
        {
            if (left[i] != right[i])
                return left[i] < right[i] ? -1 : 1;
        }

        return left.Length.CompareTo(right.Length);
    }

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
        BinaryType => "BINARY",
        BooleanType => "BOOLEAN",
        Decimal128Type d => $"DECIMAL({d.Precision},{d.Scale})",
        Decimal256Type d => $"DECIMAL({d.Precision},{d.Scale})",

        // Spark's name for the type of a bare NULL. Arrow calls it `null`, which would read as
        // "no type at all" in an error message.
        NullType => "VOID",

        // Spark has one date type and Arrow has two widths of it; the fallback below would spell
        // them "DATE32" and "DATE64", which no Spark user has seen, in every message this feeds.
        Date32Type or Date64Type => "DATE",

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
            "BINARY" => BinaryType.Default,
            "BOOLEAN" or "BOOL" => BooleanType.Default,
            "DATE" => Date32Type.Default,
            // Microseconds in UTC, matching what the readers produce and the fixed timezone
            // policy. TIMESTAMP_NTZ is a distinct Spark type and is deliberately not aliased
            // here: it has no offset at all, and pretending otherwise would silently reinterpret
            // values rather than refuse them.
            "TIMESTAMP" => Timestamp,
            "DECIMAL" or "DEC" or "NUMERIC" => new Decimal128Type(10, 0),
            _ => throw new NotSupportedException($"cast to '{text}' is not implemented"),
        };
    }
}
