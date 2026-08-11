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
        public CastInput(double number, decimal? exact, string text)
        {
            AsDouble = number;
            Exact = exact;
            Text = text;
            IsNumeric = true;
            FromString = false;
        }

        public CastInput(string text)
        {
            Text = text;
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
        /// It changes what an integral cast accepts. A number truncates toward zero, so
        /// <c>CAST(1.7 AS INT)</c> is 1 — but a string must already be an integer, and
        /// <c>CAST('12.5' AS INT)</c> is refused rather than becoming 12. Both measured.
        /// </remarks>
        public bool FromString { get; }

        public double AsDouble { get; }

        /// <summary>The source rendered as Spark would render it, for error messages and casts to string.</summary>
        public string Text { get; }

        /// <summary>Whether the value can take part in a numeric cast at all.</summary>
        public bool IsNumeric { get; }
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
        Decimal128Array a => a.IsNull(index) ? null : (double?)a.GetValue(index),
        _ => throw new NotSupportedException(
            $"{array.Data.DataType.Name} is not a numeric array"),
    };

    public static decimal? ReadDecimal(IArrowArray array, int index) => array switch
    {
        Int8Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int16Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int32Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int64Array a => a.IsNull(index) ? null : a.GetValue(index),
        FloatArray a => a.IsNull(index) ? null : (decimal?)a.GetValue(index),
        DoubleArray a => a.IsNull(index) ? null : (decimal?)a.GetValue(index),
        Decimal128Array a => a.IsNull(index) ? null : a.GetValue(index),
        _ => throw new NotSupportedException(
            $"{array.Data.DataType.Name} is not a numeric array"),
    };

    /// <summary>Reads a value for casting, keeping strings as strings.</summary>
    public static CastInput? ReadForCast(IArrowArray array, int index)
    {
        if (array is StringArray strings)
            return strings.IsNull(index) ? null : new CastInput(strings.GetString(index));

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
        decimal? exact = asDouble.Value is >= -7.9e28 and <= 7.9e28
            ? ReadDecimalOrNull(array, index)
            : null;

        return new CastInput(asDouble.Value, exact, Render(array, index, exact));
    }

    private static decimal? ReadDecimalOrNull(IArrowArray array, int index)
    {
        try
        {
            return ReadDecimal(array, index);
        }
        catch (OverflowException)
        {
            return null;
        }
    }

    /// <summary>Renders a numeric cell the way Spark would print it.</summary>
    private static string Render(IArrowArray array, int index, decimal? value) => array switch
    {
        FloatArray a => a.GetValue(index)!.Value.ToString("R", Invariant),
        DoubleArray a => a.GetValue(index)!.Value.ToString("R", Invariant),
        _ => value?.ToString(Invariant) ?? "<out of range>",
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

            var parts = text[(open + 1)..text.LastIndexOf(')')].Split(',');
            var precision = int.Parse(parts[0].Trim(), Invariant);
            var scale = parts.Length > 1 ? int.Parse(parts[1].Trim(), Invariant) : 0;
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
            "DECIMAL" or "DEC" or "NUMERIC" => new Decimal128Type(10, 0),
            _ => throw new NotSupportedException($"cast to '{text}' is not implemented"),
        };
    }
}
