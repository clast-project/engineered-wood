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
    /// Measured, and none of it is the obvious reading: positions are 1-based, position 0 behaves
    /// as 1, a negative position counts back from the end (<c>-2</c> starts at the second-to-last
    /// character), a length past the end clamps rather than failing, and a start past the end
    /// gives an empty string rather than null.
    /// </remarks>
    public static IArrowArray Substring(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        var builder = new StringArray.Builder();

        for (var i = 0; i < rowCount; i++)
        {
            var text = ReadString(args[0], i);
            var position = SparkArrays.ReadInt64(args[1], i);

            long? length = args.Count > 2 ? SparkArrays.ReadInt64(args[2], i) : null;
            if (text is null || position is null || (args.Count > 2 && length is null))
            {
                builder.AppendNull();
                continue;
            }

            var start = position.Value;
            if (start < 0)
                start = Math.Max(text.Length + start + 1, 1);
            else if (start == 0)
                start = 1;

            var zeroBased = (int)Math.Min(start - 1, text.Length);
            var take = length is null
                ? text.Length - zeroBased
                : (int)Math.Max(Math.Min(length.Value, text.Length - zeroBased), 0);

            builder.Append(text.Substring(zeroBased, take));
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
    /// a backslash escapes the next character — measured, <c>'100%' LIKE '100\%'</c> is true, so
    /// the escape has to be honoured rather than treated as a literal backslash.
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
    /// <c>date_format(temporal, pattern)</c>, over the subset of Java patterns that mean the same
    /// thing in .NET.
    /// </summary>
    /// <remarks>
    /// The two pattern languages overlap for the fields Delta expressions actually use —
    /// <c>y M d H m s</c> — but diverge elsewhere, so an unrecognised letter is refused rather
    /// than passed through to be silently reinterpreted. <c>yyyy</c> is the common case and means
    /// the same in both.
    /// </remarks>
    public static IArrowArray DateFormat(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        var builder = new StringArray.Builder();

        for (var i = 0; i < rowCount; i++)
        {
            var instant = SparkArrays.ReadInstant(args[0], i);
            var pattern = ReadString(args[1], i);

            if (instant is null || pattern is null)
            {
                builder.AppendNull();
                continue;
            }

            var local = TimeZoneInfo.ConvertTime(instant.Value, SparkDialectOptions.TimeZone);
            builder.Append(local.ToString(TranslatePattern(pattern), Invariant));
        }

        return builder.Build();
    }

    private static string TranslatePattern(string javaPattern)
    {
        // A single-quoted section is a literal in both dialects, so its letters carry no meaning
        // and must not be validated. Rejecting them would refuse `yyyy-MM-dd\'T\'HH:mm:ss`, which
        // is the ordinary way to write an ISO 8601 timestamp.
        var inLiteral = false;

        foreach (var c in javaPattern)
        {
            if (c == '\'')
            {
                inLiteral = !inLiteral;
                continue;
            }

            if (inLiteral || !char.IsLetter(c))
                continue;

            if (c is not ('y' or 'M' or 'd' or 'H' or 'm' or 's'))
            {
                throw new NotSupportedException(
                    $"date_format pattern letter '{c}' is not supported; " +
                    "only y, M, d, H, m and s are known to mean the same in both dialects");
            }
        }

        return javaPattern;
    }

    // ── Conditionals ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Picks a value per row from candidate arrays, unified to one type.
    /// </summary>
    /// <param name="choice">Index into <paramref name="sources"/> per row, or -1 for null.</param>
    public static IArrowArray Unify(
        IArrowType type, IReadOnlyList<IArrowArray> sources, int[] choice, int rowCount)
    {
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

            return SparkArrays.IsDateType(type)
                ? SparkArrays.BuildDate32(instants, rowCount)
                : SparkArrays.BuildTimestamp(instants, rowCount);
        }

        if (type is Decimal128Type decimalType)
        {
            var decimals = new Decimal128Array.Builder(decimalType);
            for (var i = 0; i < rowCount; i++)
            {
                var value = choice[i] < 0 ? null : SparkArrays.ReadDecimal(sources[choice[i]], i);
                if (value is null) decimals.AppendNull();
                else decimals.Append(SparkArrays.Rescale(value.Value, decimalType.Scale));
            }

            return decimals.Build();
        }

        if (type is FloatType)
        {
            var floats = new FloatArray.Builder();
            for (var i = 0; i < rowCount; i++)
            {
                var value = choice[i] < 0 ? null : SparkArrays.ReadDouble(sources[choice[i]], i);
                if (value is null) floats.AppendNull(); else floats.Append((float)value.Value);
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
    /// Whether two cells hold the same value, compared in their own terms.
    /// </summary>
    /// <remarks>
    /// Deliberately not a comparison of rendered text. A <c>decimal(10,2)</c> holding 1.00 and an
    /// <c>int</c> holding 1 render as "1.00" and "1" but are equal, and Spark agrees —
    /// <c>nullif(CAST(1.00 AS DECIMAL(10,2)), 1)</c> is null. Comparing the renderings would have
    /// returned the value instead.
    /// </remarks>
    public static bool AreEqual(IArrowArray left, IArrowArray right, int index)
    {
        if (SparkArrays.IsTemporal(left.Data.DataType) || SparkArrays.IsTemporal(right.Data.DataType))
            return SparkArrays.ReadInstant(left, index) == SparkArrays.ReadInstant(right, index);

        if (left is StringArray || right is StringArray)
            return ReadString(left, index) == ReadString(right, index);

        if (left is BooleanArray a && right is BooleanArray b)
            return ReadBoolean(a, index) == ReadBoolean(b, index);

        // Exact where both sides have an exact form, so scale differences do not separate equal
        // values; double only when one side cannot be exact anyway.
        try
        {
            return SparkArrays.ReadDecimal(left, index) == SparkArrays.ReadDecimal(right, index);
        }
        catch (NotSupportedException)
        {
            return SparkArrays.ReadDouble(left, index) == SparkArrays.ReadDouble(right, index);
        }
    }

    public static string? ReadString(IArrowArray array, int index)
    {
        if (array is StringArray strings)
            return strings.IsNull(index) ? null : strings.GetString(index);

        var value = SparkArrays.ReadForCast(array, index);
        return value?.Text;
    }

    private static bool? ReadBoolean(IArrowArray array, int index) =>
        array is BooleanArray booleans
            ? booleans.IsNull(index) ? null : booleans.GetValue(index)
            : throw new NotSupportedException($"{array.Data.DataType.Name} is not boolean");
}
