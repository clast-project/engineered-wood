// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text.Json;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Decodes JSON values from <see cref="Actions.AddFile.Stats"/> and string
/// values from <see cref="Actions.AddFile.PartitionValues"/> into
/// <see cref="LiteralValue"/>, using the Delta primitive type name to choose
/// the encoding.
/// </summary>
internal static class DeltaLiteralDecoder
{
    /// <summary>
    /// Decodes a JSON element from a stats <c>minValues</c>/<c>maxValues</c>
    /// map. Returns null when the element is null, the type is unknown, or
    /// decoding fails (treated as Unknown by the evaluator).
    /// </summary>
    public static LiteralValue? FromJson(JsonElement value, string typeName)
    {
        if (value.ValueKind == JsonValueKind.Null)
            return null;

        try
        {
            switch (typeName)
            {
                case "long":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of(value.GetInt64()) : null;
                case "integer":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of(value.GetInt32()) : null;
                case "short":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of((int)value.GetInt16()) : null;
                case "byte":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of((int)value.GetSByte()) : null;
                case "float":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of(value.GetSingle()) : null;
                case "double":
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of(value.GetDouble()) : null;
                case "boolean":
                    return value.ValueKind switch
                    {
                        JsonValueKind.True => (LiteralValue?)LiteralValue.Of(true),
                        JsonValueKind.False => (LiteralValue?)LiteralValue.Of(false),
                        _ => null,
                    };
                case "string":
                    return value.ValueKind == JsonValueKind.String
                        ? (LiteralValue?)LiteralValue.Of(value.GetString()!) : null;
                case "binary":
                    // Delta stats for binary columns are uncommon; if present, decode as base64.
                    if (value.ValueKind != JsonValueKind.String) return null;
                    try { return LiteralValue.Of(Convert.FromBase64String(value.GetString()!)); }
                    catch { return null; }
                case "date":
                    return value.ValueKind == JsonValueKind.String
                        ? ParseDate(value.GetString()!) : null;
                case "timestamp":
                case "timestamp_ntz":
                    return value.ValueKind == JsonValueKind.String
                        ? ParseTimestamp(value.GetString()!) : null;
                default:
                    if (typeName.StartsWith("decimal(", StringComparison.Ordinal))
                        return ParseDecimalJson(value);
                    return null;
            }
        }
        catch (FormatException) { return null; }
        catch (InvalidOperationException) { return null; }
        catch (OverflowException) { return null; }
    }

    /// <summary>
    /// Decodes a partition-column string value (from
    /// <see cref="Actions.AddFile.PartitionValues"/>) per the column's Delta
    /// type. Partition values are always serialized as strings; null partitions
    /// are conventionally <c>null</c> in the dictionary value.
    /// </summary>
    public static LiteralValue? FromPartitionString(string? value, string typeName)
    {
        if (value is null) return LiteralValue.Null;

        try
        {
            switch (typeName)
            {
                case "long":
                    return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long l)
                        ? (LiteralValue?)LiteralValue.Of(l) : null;
                case "integer":
                    return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int i)
                        ? (LiteralValue?)LiteralValue.Of(i) : null;
                case "short":
                    return short.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out short s)
                        ? (LiteralValue?)LiteralValue.Of((int)s) : null;
                case "byte":
                    return sbyte.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out sbyte b)
                        ? (LiteralValue?)LiteralValue.Of((int)b) : null;
                case "float":
                    return float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out float f)
                        ? (LiteralValue?)LiteralValue.Of(f) : null;
                case "double":
                    return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double d)
                        ? (LiteralValue?)LiteralValue.Of(d) : null;
                case "boolean":
                    return bool.TryParse(value, out bool bo)
                        ? (LiteralValue?)LiteralValue.Of(bo) : null;
                case "string":
                    return LiteralValue.Of(value);
                case "date":
                    return ParseDate(value);
                case "timestamp":
                case "timestamp_ntz":
                    return ParseTimestamp(value);
                default:
                    if (typeName.StartsWith("decimal(", StringComparison.Ordinal))
                        return ParseDecimalText(value);
                    return null;
            }
        }
        catch (FormatException) { return null; }
        catch (OverflowException) { return null; }
    }

    /// <summary>
    /// Decodes one value of a checkpoint's typed <c>stats_parsed</c> column — the same bound
    /// <see cref="FromJson"/> would produce from the JSON copy, read straight from the Arrow array
    /// instead of parsed from text.
    /// </summary>
    /// <remarks>
    /// Dispatch is on the DELTA type name, not on the Arrow array type, so the literal's
    /// <c>Kind</c> matches the JSON path exactly: a <c>short</c> bound becomes an Int32 literal
    /// either way, and comparison semantics cannot drift between the two sources.
    /// </remarks>
    public static LiteralValue? FromArrow(Apache.Arrow.IArrowArray array, int row, string typeName)
    {
        if (array.IsNull(row))
            return null;

        try
        {
            switch (typeName)
            {
                case "long":
                    return array is Apache.Arrow.Int64Array i64
                        ? (LiteralValue?)LiteralValue.Of(i64.GetValue(row)!.Value) : null;
                case "integer":
                    return array is Apache.Arrow.Int32Array i32
                        ? (LiteralValue?)LiteralValue.Of(i32.GetValue(row)!.Value) : null;
                case "short":
                    return array is Apache.Arrow.Int16Array i16
                        ? (LiteralValue?)LiteralValue.Of((int)i16.GetValue(row)!.Value) : null;
                case "byte":
                    return array is Apache.Arrow.Int8Array i8
                        ? (LiteralValue?)LiteralValue.Of((int)i8.GetValue(row)!.Value) : null;
                case "float":
                    return array is Apache.Arrow.FloatArray f32
                        ? (LiteralValue?)LiteralValue.Of(f32.GetValue(row)!.Value) : null;
                case "double":
                    return array is Apache.Arrow.DoubleArray f64
                        ? (LiteralValue?)LiteralValue.Of(f64.GetValue(row)!.Value) : null;
                case "boolean":
                    return array is Apache.Arrow.BooleanArray b
                        ? (LiteralValue?)LiteralValue.Of(b.GetValue(row)!.Value) : null;
                case "string":
                    return array is Apache.Arrow.StringArray s
                        ? (LiteralValue?)LiteralValue.Of(s.GetString(row)) : null;
                case "date":
                    // Date32 holds days since the Unix epoch; the JSON path yields a UTC DateTimeOffset.
                    return array is Apache.Arrow.Date32Array d32
                        ? (LiteralValue?)LiteralValue.Of(
                            new DateTimeOffset(
                                s_epoch.AddDays(d32.GetValue(row)!.Value), TimeSpan.Zero))
                        : null;
                case "timestamp":
                case "timestamp_ntz":
                    return array is Apache.Arrow.TimestampArray ts
                        ? (LiteralValue?)LiteralValue.Of(
                            new DateTimeOffset(
                                s_epoch.AddTicks(ts.GetValue(row)!.Value * 10), TimeSpan.Zero))
                        : null;
                default:
                    return typeName.StartsWith("decimal(", StringComparison.Ordinal)
                        ? FromArrowDecimal(array, row)
                        : null;
            }
        }
        catch (ArgumentOutOfRangeException) { return null; }
        catch (InvalidOperationException) { return null; }
        catch (OverflowException) { return null; }
    }

    private static readonly DateTime s_epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Decimal bounds keep their exact unscaled digits, as in <see cref="ParseDecimalText"/>: the
    /// reader may narrow a decimal column to any of the four Arrow widths, and 128/256 can hold values
    /// <c>System.Decimal</c> cannot.
    /// </summary>
    private static LiteralValue? FromArrowDecimal(Apache.Arrow.IArrowArray array, int row)
    {
        // Width and scale first: a Span cannot travel through a tuple or a switch expression.
        int width, scale;
        Apache.Arrow.ArrowBuffer buffer;
        switch (array)
        {
            case Apache.Arrow.Decimal32Array d:
                (width, scale, buffer) =
                    (4, ((Apache.Arrow.Types.Decimal32Type)d.Data.DataType).Scale, d.ValueBuffer);
                break;
            case Apache.Arrow.Decimal64Array d:
                (width, scale, buffer) =
                    (8, ((Apache.Arrow.Types.Decimal64Type)d.Data.DataType).Scale, d.ValueBuffer);
                break;
            case Apache.Arrow.Decimal128Array d:
                (width, scale, buffer) =
                    (16, ((Apache.Arrow.Types.Decimal128Type)d.Data.DataType).Scale, d.ValueBuffer);
                break;
            case Apache.Arrow.Decimal256Array d:
                (width, scale, buffer) =
                    (32, ((Apache.Arrow.Types.Decimal256Type)d.Data.DataType).Scale, d.ValueBuffer);
                break;
            default:
                return null;
        }

        var bytes = buffer.Span.Slice((row + array.Data.Offset) * width, width);
#if NET6_0_OR_GREATER
        var unscaled = new BigInteger(bytes, isUnsigned: false, isBigEndian: false);
#else
        var unscaled = new BigInteger(bytes.ToArray());
#endif
        return MakeDecimalLiteral(unscaled, scale);
    }

    private static LiteralValue? ParseDate(string s) =>
        DateTimeOffset.TryParseExact(s, "yyyy-MM-dd",
            CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto)
            ? (LiteralValue?)LiteralValue.Of(dto)
            : (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal, out dto)
                ? (LiteralValue?)LiteralValue.Of(dto) : null);

    private static LiteralValue? ParseTimestamp(string s) =>
        DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto)
            ? (LiteralValue?)LiteralValue.Of(dto) : null;

    private static LiteralValue? ParseDecimalJson(JsonElement value) => value.ValueKind switch
    {
        // Decode the EXACT digits, never System.Decimal — decimal.TryParse and JsonElement.TryGetDecimal
        // silently ROUND a value with more than ~28-29 significant digits (e.g. a decimal(38,30) stat) to
        // System.Decimal's precision, which would shift a min/max bound and could wrongly skip a file.
        JsonValueKind.Number => ParseDecimalText(value.GetRawText()),
        JsonValueKind.String => ParseDecimalText(value.GetString()),
        _ => null,
    };

    /// <summary>
    /// Parses a decimal number's text (from a stats JSON number or a partition string) into a
    /// <see cref="LiteralValue"/> WITHOUT loss of precision: the exact digits become an unscaled
    /// <see cref="BigInteger"/> at the value's own scale (see <see cref="DecimalText"/> for why never through
    /// <see cref="decimal"/>), materialized as a <c>System.Decimal</c> only when that representation is
    /// exact, otherwise as a high-precision decimal.
    /// </summary>
    private static LiteralValue? ParseDecimalText(string? text) =>
        DecimalText.TryParse(text, out var unscaled, out int scale)
            ? (LiteralValue?)MakeDecimalLiteral(unscaled, scale)
            : null;

    // The largest magnitude a System.Decimal can hold (96-bit unscaled integer).
    private static readonly BigInteger Decimal96Max = (BigInteger.One << 96) - 1;

    /// <summary>
    /// Builds a <c>System.Decimal</c> from <paramref name="unscaled"/> / 10^<paramref name="scale"/> when
    /// it is exactly representable (scale 0-28, magnitude within 96 bits); otherwise keeps the full value
    /// as a high-precision decimal. Both forms compare exactly via <see cref="LiteralValue"/>.
    /// </summary>
    private static LiteralValue MakeDecimalLiteral(BigInteger unscaled, int scale)
    {
        if (scale >= 0 && scale <= 28)
        {
            BigInteger magnitude = BigInteger.Abs(unscaled);
            if (magnitude <= Decimal96Max)
            {
                uint lo = (uint)(magnitude & uint.MaxValue);
                uint mid = (uint)((magnitude >> 32) & uint.MaxValue);
                uint hi = (uint)((magnitude >> 64) & uint.MaxValue);
                return LiteralValue.Of(
                    new decimal((int)lo, (int)mid, (int)hi, unscaled.Sign < 0, (byte)scale));
            }
        }

        return LiteralValue.HighPrecisionDecimalOf(unscaled, scale);
    }
}
