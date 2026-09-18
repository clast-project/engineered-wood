// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// The named Spark functions — string, pattern, date-part and conditional.
/// </summary>
/// <remarks>
/// Split from <see cref="SparkFunctionRegistry"/>, which keeps the arithmetic and cast kernels.
/// Every behaviour here was measured from Spark; the ones that are not what an implementer would
/// assume are commented where they occur.
/// </remarks>
internal static class SparkFunctions
{
    private static CultureInfo Invariant => CultureInfo.InvariantCulture;

    // ── Strings ────────────────────────────────────────────────────────────────────────────

    public static IArrowArray Length(IArrowArray source, int rowCount)
    {
        var builder = new Int32Array.Builder();
        for (var i = 0; i < rowCount; i++)
        {
            var text = ReadString(source, i);
            if (text is null) builder.AppendNull();
            else builder.Append(text.Length);
        }

        return builder.Build();
    }

    public static IArrowArray MapString(IArrowArray source, int rowCount, Func<string, string> map)
    {
        var builder = new StringArray.Builder();
        for (var i = 0; i < rowCount; i++)
        {
            var text = ReadString(source, i);
            if (text is null) builder.AppendNull();
            else builder.Append(map(text));
        }

        return builder.Build();
    }

    /// <summary>
    /// <c>substring(str, pos[, len])</c>, with Spark's position rules.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Positions are 1-based, position 0 behaves as 1, a negative position counts back from the
    /// end (<c>-2</c> starts at the second-to-last character), a length past the end clamps
    /// rather than failing, and a start past the end gives an empty string rather than null.
    /// </para>
    /// <para>
    /// The end is taken from the unclamped start, as in Spark's <c>UTF8String.substringSQL</c>: a
    /// negative position before the beginning still spends its length from where it would have
    /// started, so <c>substring('abcdef', -8, 3)</c> is <c>a</c> and
    /// <c>substring('abcdef', -7, 1)</c> is empty (clamping first would give <c>abc</c> and
    /// <c>a</c>). The sum is taken in 64 bits and clamped to an int, as Spark does, so
    /// <c>substring('abcdef', -2147483648, 2147483647)</c> is <c>abcde</c>.
    /// </para>
    /// <para>
    /// The position and length arrive as <c>int</c>: the registry casts them first, because the
    /// cast is the dialect's. With no length, the length is <see cref="int.MaxValue"/>.
    /// </para>
    /// </remarks>
    public static IArrowArray Substring(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        var builder = new StringArray.Builder();

        for (var i = 0; i < rowCount; i++)
        {
            var text = ReadString(args[0], i);
            var position = SparkArrays.ReadInt64(args[1], i);

            long? length = args.Count > 2 ? SparkArrays.ReadInt64(args[2], i) : int.MaxValue;
            if (text is null || position is null || length is null)
            {
                builder.AppendNull();
                continue;
            }

            var pos = checked((int)position.Value);
            var start = pos > 0 ? pos - 1 : pos < 0 ? text.Length + pos : 0;
            var end = Math.Min(Math.Max((long)start + checked((int)length.Value), int.MinValue), int.MaxValue);
            start = Math.Max(start, 0);

            if (start >= end || start >= text.Length)
            {
                builder.Append(string.Empty);
                continue;
            }

            var until = (int)Math.Min(end, text.Length);
            builder.Append(text.Substring(start, until - start));
        }

        return builder.Build();
    }

    /// <summary>
    /// <c>concat</c>, which is also what <c>||</c> lowers to.
    /// </summary>
    /// <remarks>
    /// Null propagates: <c>concat('abc', NULL)</c> is null, not <c>'abc'</c>. Non-string
    /// arguments render as Spark renders them, so <c>concat(s, a)</c> over an int gives
    /// <c>'abc1'</c>.
    /// </remarks>
    public static IArrowArray Concat(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        var builder = new StringArray.Builder();

        for (var i = 0; i < rowCount; i++)
        {
            var text = new StringBuilder();
            var isNull = false;

            foreach (var arg in args)
            {
                var part = ReadString(arg, i);
                if (part is null) { isNull = true; break; }
                text.Append(part);
            }

            if (isNull) builder.AppendNull();
            else builder.Append(text.ToString());
        }

        return builder.Build();
    }

    // ── Pattern matching ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>LIKE</c>, <c>ILIKE</c> and <c>RLIKE</c>.
    /// </summary>
    /// <remarks>
    /// <c>RLIKE</c> is a regular expression and is passed through. <c>LIKE</c> is translated:
    /// <c>%</c> matches any run, <c>_</c> matches one character, everything else is literal, and
    /// a backslash escapes the next character, so <c>'100%' LIKE '100\%'</c> is true.
    /// </remarks>
    public static IArrowArray Match(string name, IReadOnlyList<IArrowArray> args, int rowCount)
    {
        var builder = new BooleanArray.Builder();
        var isRegex = name == "rlike";
        var options = name == "ilike" ? RegexOptions.IgnoreCase : RegexOptions.None;

        // Case folding must not depend on the process culture: without CultureInvariant a
        // Turkish locale folds 'I' to a dotless lowercase, so ILIKE would match different rows
        // on different machines. Not RegexOptions.Compiled, which trades startup and AOT
        // friendliness for throughput this does not need.
        options |= RegexOptions.CultureInvariant;

        // Patterns are almost always constant, so the constructed Regex is reused across rows.
        Regex? cached = null;
        string? cachedPattern = null;

        for (var i = 0; i < rowCount; i++)
        {
            var text = ReadString(args[0], i);
            var pattern = ReadString(args[1], i);

            if (text is null || pattern is null)
            {
                builder.AppendNull();
                continue;
            }

            if (cached is null || cachedPattern != pattern)
            {
                cachedPattern = pattern;
                cached = new Regex(isRegex ? pattern : LikeToRegex(pattern), options);
            }

            builder.Append(cached.IsMatch(text));
        }

        return builder.Build();
    }

    private static string LikeToRegex(string pattern)
    {
        var regex = new StringBuilder("^");

        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            switch (c)
            {
                case '\\' when i + 1 < pattern.Length:
                    regex.Append(Regex.Escape(pattern[++i].ToString()));
                    break;
                case '%':
                    regex.Append(".*");
                    break;
                case '_':
                    regex.Append('.');
                    break;
                default:
                    regex.Append(Regex.Escape(c.ToString()));
                    break;
            }
        }

        return regex.Append('$').ToString();
    }

    // ── Date parts ─────────────────────────────────────────────────────────────────────────

    public static IArrowArray DatePart(string name, IArrowArray source, int rowCount)
    {
        var builder = new Int32Array.Builder();

        for (var i = 0; i < rowCount; i++)
        {
            var instant = SparkArrays.ReadInstant(source, i);
            if (instant is null)
            {
                builder.AppendNull();
                continue;
            }

            var local = TimeZoneInfo.ConvertTime(instant.Value, SparkDialectOptions.TimeZone);
            builder.Append(name switch
            {
                "year" => local.Year,
                "month" => local.Month,
                "day" or "dayofmonth" => local.Day,
                "hour" => local.Hour,
                "minute" => local.Minute,
                "second" => local.Second,
                _ => throw new NotSupportedException($"'{name}' is not a date part"),
            });
        }

        return builder.Build();
    }

    /// <summary>
    /// <c>date_format(temporal, pattern)</c>, rendered from Java's pattern language by
    /// <see cref="SparkDatePattern"/>.
    /// </summary>
    /// <remarks>
    /// The pattern is compiled here rather than handed to .NET's formatter; see
    /// <see cref="SparkDatePattern"/> for why.
    /// <para>
    /// A null row is answered without looking at the pattern. <c>date_format(NULL, 'ddd')</c>
    /// and <c>date_format(CAST(NULL AS TIMESTAMP), 'ddd')</c> both answer NULL in Spark, because
    /// a null-literal argument folds the whole expression away before the formatter is built,
    /// while <c>date_format(ts, 'ddd')</c> over a column refuses. Unmeasured, because the corpus
    /// has no all-null timestamp column: Spark would presumably refuse a bad pattern over a
    /// column whose every row is null, where this answers nulls.
    /// </para>
    /// <para>
    /// The pattern is an array, so nothing stops it varying per row; the compiled form is reused
    /// while it does not.
    /// </para>
    /// </remarks>
    public static IArrowArray DateFormat(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        var builder = new StringArray.Builder();
        string? compiledFor = null;
        SparkDatePattern? compiled = null;

        for (var i = 0; i < rowCount; i++)
        {
            var instant = SparkArrays.ReadInstant(args[0], i);
            var pattern = ReadString(args[1], i);

            if (instant is null || pattern is null)
            {
                builder.AppendNull();
                continue;
            }

            if (!string.Equals(compiledFor, pattern, StringComparison.Ordinal))
            {
                compiled = SparkDatePattern.Compile(pattern);
                compiledFor = pattern;
            }

            var local = TimeZoneInfo.ConvertTime(instant.Value, SparkDialectOptions.TimeZone);
            builder.Append(compiled!.Format(local));
        }

        return builder.Build();
    }

    // ── Conditionals ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Picks a value per row from candidate arrays, unified to one type.
    /// </summary>
    /// <param name="choice">Index into <paramref name="sources"/> per row, or -1 for null.</param>
    public static IArrowArray Unify(
        IArrowType type, IReadOnlyList<IArrowArray> sources, int[] choice, int rowCount)
    {
        // Every branch was `void`, so there is no value to pick and no type to build one at:
        // Spark types `coalesce(NULL, NULL)` and `if(c, NULL, NULL)` as void too.
        if (type is NullType)
            return new NullArray(rowCount);

        if (type is StringType)
        {
            var strings = new StringArray.Builder();
            for (var i = 0; i < rowCount; i++)
            {
                var text = choice[i] < 0 ? null : ReadString(sources[choice[i]], i);
                if (text is null) strings.AppendNull(); else strings.Append(text);
            }

            return strings.Build();
        }

        // Order does not matter here, unlike everywhere else binary appears: these branches are
        // selected by the unified type, and BinaryType and StringType are disjoint. A string
        // branch is read by `AppendBytes` as its UTF-8 bytes, since a StringArray is also a
        // BinaryArray.
        if (type is BinaryType)
        {
            var bytes = new BinaryArray.Builder();
            for (var i = 0; i < rowCount; i++)
            {
                if (choice[i] < 0) bytes.AppendNull();
                else SparkArrays.AppendBytes(bytes, sources[choice[i]], i);
            }

            return bytes.Build();
        }

        if (type is BooleanType)
        {
            var booleans = new BooleanArray.Builder();
            for (var i = 0; i < rowCount; i++)
            {
                var flag = choice[i] < 0 ? null : ReadBoolean(sources[choice[i]], i);
                if (flag is null) booleans.AppendNull(); else booleans.Append(flag.Value);
            }

            return booleans.Build();
        }

        if (SparkArrays.IsTemporal(type))
        {
            var instants = new DateTimeOffset?[rowCount];
            for (var i = 0; i < rowCount; i++)
                instants[i] = choice[i] < 0 ? null : SparkArrays.ReadInstant(sources[choice[i]], i);

            // Built at the canonical type for the zone, not at `type` itself. The zone must come
            // from `type` -- `BuildTimestamp`'s no-zone overload labels every result UTC, which
            // would turn a `timestamp_ntz` fold into a zoned array.
            //
            // But `type` is not always canonical: `NullIf` hands over `args[0].Data.DataType`
            // directly and `ConditionalType` keeps a sole surviving branch's own type, so a
            // millisecond column (which is what Parquet writes) arrives here as
            // `timestamp(ms, UTC)`. Passing that through would make `coalesce(tsms)` and
            // `nullif(tsms, ts)` answer in milliseconds where every other route produces
            // microseconds.
            if (SparkArrays.IsDateType(type))
                return SparkArrays.BuildDate32(instants, rowCount);

            return SparkArrays.BuildTimestamp(
                instants, rowCount,
                SparkArrays.IsZonedTimestamp(type) ? SparkArrays.Timestamp : SparkArrays.NaiveTimestamp);
        }

        if (type is Decimal128Type decimalType)
        {
            var mantissas = new Int128?[rowCount];
            for (var i = 0; i < rowCount; i++)
            {
                if (choice[i] < 0) continue;

                if (SparkWideDecimals.Read(sources[choice[i]], i) is { } value)
                    mantissas[i] = SparkWideDecimals.Cast(value, decimalType);
            }

            return SparkWideDecimals.Build(mantissas, decimalType, rowCount);
        }

        if (type is FloatType)
        {
            var floats = new FloatArray.Builder();
            for (var i = 0; i < rowCount; i++)
            {
                var value = choice[i] < 0 ? null : SparkArrays.ReadFloat(sources[choice[i]], i);
                if (value is null) floats.AppendNull(); else floats.Append(value.Value);
            }

            return floats.Build();
        }

        if (type is DoubleType)
        {
            var doubles = new DoubleArray.Builder();
            for (var i = 0; i < rowCount; i++)
            {
                var value = choice[i] < 0 ? null : SparkArrays.ReadDouble(sources[choice[i]], i);
                if (value is null) doubles.AppendNull(); else doubles.Append(value.Value);
            }

            return doubles.Build();
        }

        var integers = new long?[rowCount];
        for (var i = 0; i < rowCount; i++)
            integers[i] = choice[i] < 0 ? null : SparkArrays.ReadInt64(sources[choice[i]], i);

        return SparkArrays.BuildIntegral(integers, type, rowCount);
    }

    /// <summary>Whether a cell is null, for the conditional functions.</summary>
    public static bool IsNull(IArrowArray array, int index) => array switch
    {
        // A `void` column is null at every row.
        NullArray => true,
        StringArray a => a.IsNull(index),
        BooleanArray a => a.IsNull(index),
        _ => SparkArrays.IsTemporal(array.Data.DataType)
            ? SparkArrays.ReadInstant(array, index) is null
            : IsNullNumeric(array, index),
    };

    private static bool IsNullNumeric(IArrowArray array, int index)
    {
        try
        {
            return SparkArrays.ReadDouble(array, index) is null;
        }
        catch (NotSupportedException)
        {
            return array.IsNull(index);
        }
    }

    /// <summary>
    /// Orders two cells of the same Arrow type, for <c>greatest</c> and <c>least</c>.
    /// </summary>
    /// <remarks>
    /// Same type on both sides by construction — the caller unifies first — so this never has to
    /// promote, and a decimal can be compared on its unscaled integer alone because the scales
    /// already match (see <see cref="SparkWideDecimals.Compare"/>).
    /// </remarks>
    public static int CompareAt(IArrowArray left, IArrowArray right, int index) => left switch
    {
        Int8Array or Int16Array or Int32Array or Int64Array =>
            SparkArrays.ReadInt64(left, index)!.Value.CompareTo(SparkArrays.ReadInt64(right, index)!.Value),

        FloatArray or DoubleArray => CompareDoubles(
            SparkArrays.ReadDouble(left, index)!.Value, SparkArrays.ReadDouble(right, index)!.Value),

        Decimal128Array a => SparkWideDecimals.Compare(a, (Decimal128Array)right, index),

        StringArray a => string.CompareOrdinal(a.GetString(index), ((StringArray)right).GetString(index)),

        // Must stay after the string case: a StringArray is a BinaryArray, and matching it here
        // would order two strings by their UTF-8 bytes instead of by their ordinals. Only a
        // binary/binary pair reaches this -- greatest/least refuse a binary against a string in
        // both dialects, unlike coalesce, which coerces it.
        BinaryArray => SparkArrays.CompareBytes(left, right, index),

        BooleanArray a => a.GetValue(index)!.Value.CompareTo(((BooleanArray)right).GetValue(index)!.Value),

        _ when SparkArrays.IsTemporal(left.Data.DataType) =>
            SparkArrays.ReadInstant(left, index)!.Value.CompareTo(
                SparkArrays.ReadInstant(right, index)!.Value),

        _ => throw new NotSupportedException(
            $"{left.Data.DataType.Name} cannot be ordered"),
    };

    /// <summary>Orders two floating-point values the way Spark does, which is not .NET's way.</summary>
    /// <remarks>
    /// Spark's <c>SQLOrderingUtil.compareDoubles</c> puts NaN above everything, +Infinity
    /// included, and treats <c>-0.0</c> and <c>0.0</c> as equal. <see cref="double.CompareTo(double)"/>
    /// does the opposite on both counts: NaN sorts below everything and -0.0 below 0.0. So
    /// <c>greatest(NaN, 2.0)</c> is NaN, <c>greatest(NaN, Infinity)</c> is NaN,
    /// <c>least(NaN, -Infinity)</c> is -Infinity, and NaN against itself is equal.
    /// <para>
    /// <c>greatest</c>, <c>least</c> and the double fallback in <see cref="AreEqual"/> use this.
    /// The comparison operators have their own implementation, and they too hold NaN equal to
    /// itself.
    /// </para>
    /// </remarks>
    private static int CompareDoubles(double x, double y)
    {
        if (x < y) return -1;
        if (x > y) return 1;

        // Settles -0.0 against 0.0 as equal, which `CompareTo` does not.
        if (x == y) return 0;

        // Nothing but a NaN reaches here: it is false against every comparison, itself included.
        var leftIsNaN = double.IsNaN(x);
        var rightIsNaN = double.IsNaN(y);
        if (leftIsNaN && rightIsNaN) return 0;
        return leftIsNaN ? 1 : -1;
    }

    /// <summary>
    /// Whether two cells hold the same value, compared in their own terms.
    /// </summary>
    /// <remarks>
    /// Deliberately not a comparison of rendered text. A <c>decimal(10,2)</c> holding 1.00 and an
    /// <c>int</c> holding 1 render as "1.00" and "1" but are equal, and Spark agrees —
    /// <c>nullif(CAST(1.00 AS DECIMAL(10,2)), 1)</c> is null.
    /// </remarks>
    public static bool AreEqual(IArrowArray left, IArrowArray right, int index)
    {
        if (SparkArrays.IsTemporal(left.Data.DataType) || SparkArrays.IsTemporal(right.Data.DataType))
            return SparkArrays.ReadInstant(left, index) == SparkArrays.ReadInstant(right, index);

        if (left is StringArray || right is StringArray)
            return ReadString(left, index) == ReadString(right, index);

        // After the string case, and the two answer differently. A binary against a binary
        // compares bytes: `nullif(X'FF', X'FE')` is X'FF', though both decode to the same U+FFFD
        // and the string route above would call them equal. A binary against a string keeps the
        // string route, which is Spark's answer too: `nullif(X'FF', CAST(X'FF' AS STRING))` is
        // NULL, because the binary is rendered as text rather than the string encoded.
        if (left is BinaryArray leftBytes && right is BinaryArray rightBytes)
        {
            return leftBytes.IsNull(index) || rightBytes.IsNull(index)
                ? leftBytes.IsNull(index) && rightBytes.IsNull(index)
                : SparkArrays.CompareBytes(left, right, index) == 0;
        }

        if (left is BooleanArray a && right is BooleanArray b)
            return ReadBoolean(a, index) == ReadBoolean(b, index);

        // Exact where both sides have an exact form, so scale differences do not separate equal
        // values; double only when one side cannot be exact anyway.
        if (SparkWideDecimals.IsExact(left.Data.DataType) && SparkWideDecimals.IsExact(right.Data.DataType))
        {
            var first = SparkWideDecimals.Read(left, index);
            var second = SparkWideDecimals.Read(right, index);

            return first is null || second is null
                ? first is null && second is null
                : SparkWideDecimals.AreEqual(first.Value, second.Value);
        }

        try
        {
            return SparkArrays.ReadDecimal(left, index) == SparkArrays.ReadDecimal(right, index);
        }
        catch (NotSupportedException)
        {
            // Reached when a value has no exact System.Decimal form: a magnitude past decimal's
            // ceiling near 7.9e28, or a NaN or an infinity. ReadDecimal must keep signalling that
            // with NotSupportedException for a float or double as well as for a Decimal128, or
            // it escapes the evaluator instead of landing here.
            //
            // Compared with Spark's NaN rule, not IEEE's: `nullif(CAST('NaN' AS DOUBLE),
            // CAST('NaN' AS DOUBLE))` is NULL, so Spark holds NaN equal to itself here exactly as
            // `=`, `<=>` and `IN` do, where `==` on two NaNs is false.
            var first = SparkArrays.ReadDouble(left, index);
            var second = SparkArrays.ReadDouble(right, index);

            return first is null || second is null
                ? first is null && second is null
                : CompareDoubles(first.Value, second.Value) == 0;
        }
    }

    public static string? ReadString(IArrowArray array, int index)
    {
        if (array is NullArray)
            return null;

        if (array is StringArray strings)
            return strings.IsNull(index) ? null : strings.GetString(index);

        var value = SparkArrays.ReadForCast(array, index);
        return value?.Text;
    }

    private static bool? ReadBoolean(IArrowArray array, int index) =>
        array switch
        {
            // Reachable wherever the other branch made the unified type boolean, as in
            // `if(c, bl, NULL)`.
            NullArray => null,
            BooleanArray booleans => booleans.IsNull(index) ? null : booleans.GetValue(index),
            _ => throw new NotSupportedException($"{array.Data.DataType.Name} is not boolean"),
        };
}
