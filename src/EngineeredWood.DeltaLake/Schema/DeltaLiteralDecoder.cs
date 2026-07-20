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
    /// <see cref="BigInteger"/> at the value's own scale, materialized as a <c>System.Decimal</c> only when
    /// that representation is exact, otherwise as a high-precision decimal. Accepts an optional sign,
    /// fractional part, and exponent (e.g. <c>1.23e4</c>).
    /// </summary>
    private static LiteralValue? ParseDecimalText(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return null;

        string s = text!.Trim();

        int e = s.IndexOfAny(ExponentChars);
        int exponent = 0;
        string mantissa = s;
        if (e >= 0)
        {
            mantissa = s.Substring(0, e);
            if (!int.TryParse(s.Substring(e + 1), NumberStyles.AllowLeadingSign,
                    CultureInfo.InvariantCulture, out exponent))
                return null;
        }

        bool negative = false;
        if (mantissa.Length > 0 && (mantissa[0] == '-' || mantissa[0] == '+'))
        {
            negative = mantissa[0] == '-';
            mantissa = mantissa.Substring(1);
        }

        int dot = mantissa.IndexOf('.');
        int fractionalDigits = dot < 0 ? 0 : mantissa.Length - dot - 1;
        string digits = dot < 0 ? mantissa : mantissa.Substring(0, dot) + mantissa.Substring(dot + 1);

        if (digits.Length == 0
            || !BigInteger.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var unscaled))
            return null;

        if (negative)
            unscaled = -unscaled;

        // value = unscaled * 10^(exponent - fractionalDigits); a negative resulting scale is absorbed into
        // the integer so the stored scale is always >= 0.
        int scale = fractionalDigits - exponent;
        if (scale < 0)
        {
            unscaled *= BigInteger.Pow(10, -scale);
            scale = 0;
        }

        return MakeDecimalLiteral(unscaled, scale);
    }

    private static readonly char[] ExponentChars = { 'e', 'E' };

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
