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
                    if (value.ValueKind == JsonValueKind.String)
                        return NonFinite(value.GetString()) is double sf
                            ? (LiteralValue?)LiteralValue.Of((float)sf) : null;
                    return value.ValueKind == JsonValueKind.Number
                        ? (LiteralValue?)LiteralValue.Of(value.GetSingle()) : null;
                case "double":
                    if (value.ValueKind == JsonValueKind.String)
                        return NonFinite(value.GetString()) is double sd
                            ? (LiteralValue?)LiteralValue.Of(sd) : null;
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
                        return ParseDecimalJson(value, typeName);
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
                        return ParseDecimalText(value, typeName);
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
                    // Checked: an unchecked product wraps to a small valid tick count, a bound far from the
                    // value, which could prune a file holding it. Overflow is caught below as no bound.
                    return array is Apache.Arrow.TimestampArray ts
                        ? (LiteralValue?)LiteralValue.Of(
                            new DateTimeOffset(
                                s_epoch.AddTicks(checked(ts.GetValue(row)!.Value * 10)), TimeSpan.Zero))
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
    /// One field of a variant column's bound (an object keyed by normalized path, see
    /// <see cref="VariantStatsEncoding"/>) as a bound on <c>variant_get</c>'s result in
    /// <paramref name="targetType"/>, a canonical Spark type name (<see cref="VariantGetCall.TargetType"/>), or
    /// null when the bound has no such field or its value does not convert EXACTLY.
    /// </summary>
    /// <remarks>
    /// The rules, and the literal kinds, are <see cref="VariantBound.ToLiteral"/>'s, which the Parquet reader's
    /// pruning on shredded <c>typed_value</c> statistics shares.
    /// </remarks>
    public static LiteralValue? FromVariantField(
        ReadOnlySpan<byte> metadata, ReadOnlySpan<byte> value, string key, string targetType)
    {
        try
        {
            var bound = new Apache.Arrow.Scalars.Variant.VariantReader(metadata, value);
            if (!bound.IsObject)
                return null;
            var fields = new Apache.Arrow.Scalars.Variant.VariantObjectReader(metadata, value);
            return fields.TryGetField(key, out var field) ? FromVariant(field, targetType) : null;
        }
        catch (Exception e) when (e is ArgumentException or InvalidOperationException or IndexOutOfRangeException
            or OverflowException or FormatException)
        {
            return null;
        }
    }

    // The bound as the Variant primitive it is, converted under the rules the Parquet reader's variant pruning
    // shares (VariantBound.ToLiteral).
    private static LiteralValue? FromVariant(Apache.Arrow.Scalars.Variant.VariantReader reader, string targetType) =>
        ToVariantBound(reader) is { } bound ? bound.ToLiteral(targetType) : null;

    private static VariantBound? ToVariantBound(Apache.Arrow.Scalars.Variant.VariantReader reader)
    {
        if (reader.IsNull)
            return null;
        if (reader.IsBoolean)
            return VariantBound.Boolean(reader.GetBoolean());
        if (reader.IsString)
            return VariantBound.String(reader.GetString());
        if (reader.BasicType != Apache.Arrow.Scalars.Variant.VariantBasicType.Primitive)
            return null;

        switch (reader.PrimitiveType)
        {
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Int8:
                return VariantBound.Integer(VariantBoundKind.Int8, reader.GetInt8());
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Int16:
                return VariantBound.Integer(VariantBoundKind.Int16, reader.GetInt16());
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Int32:
                return VariantBound.Integer(VariantBoundKind.Int32, reader.GetInt32());
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Int64:
                return VariantBound.Integer(VariantBoundKind.Int64, reader.GetInt64());
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Float:
                return VariantBound.Float(reader.GetFloat());
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Double:
                return VariantBound.Double(reader.GetDouble());
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Date:
                return VariantBound.Date(reader.GetDateDays());
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Timestamp:
                return VariantBound.Timestamp(reader.GetTimestampMicros());
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.TimestampNtz:
                return VariantBound.TimestampNtz(reader.GetTimestampNtzMicros());
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Decimal4:
                return VariantDecimal(reader, 4);
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Decimal8:
                return VariantDecimal(reader, 8);
            case Apache.Arrow.Scalars.Variant.VariantPrimitiveType.Decimal16:
                return VariantDecimal(reader, 16);
            default:
                return null;
        }
    }

    // Read from the value bytes rather than through VariantReader's decimal getters: the digits and the scale
    // must both survive exactly, and the scale is a fact about the type here (see ParseDecimalText).
    private static VariantBound? VariantDecimal(Apache.Arrow.Scalars.Variant.VariantReader reader, int width)
    {
        var bytes = reader.Value;
        if (bytes.Length < 2 + width)
            return null;
#if NET6_0_OR_GREATER
        var unscaled = new BigInteger(bytes.Slice(2, width), isUnsigned: false, isBigEndian: false);
#else
        var unscaled = new BigInteger(bytes.Slice(2, width).ToArray());
#endif
        return VariantBound.Decimal(unscaled, bytes[1]);
    }

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

    /// <summary>
    /// The three float bounds JSON cannot spell as numbers, or <see langword="null"/> for any other
    /// string.
    /// </summary>
    /// <remarks>
    /// JSON has no NaN and no infinity, so Delta's stats carry them QUOTED. Measured against Spark
    /// 4.0: a double column holding <c>[3.0, NaN]</c> commits
    /// <c>"minValues":{"g":3.0},"maxValues":{"g":"NaN"}</c>, and one holding both infinities commits
    /// <c>"-Infinity"</c> / <c>"Infinity"</c>. Those are Java's <c>Double.toString</c> spellings.
    /// <para>
    /// Refusing them was safe -- an undecodable bound reads as Unknown and keeps the file -- but it
    /// gave up ALL pruning on any float column reaching either end of the range, which is not rare.
    /// Reading them is also what makes a Spark-written NaN maximum visible as a maximum, rather than
    /// as a missing bound.
    /// </para>
    /// </remarks>
    private static double? NonFinite(string? text) => text switch
    {
        "NaN" => double.NaN,
        "Infinity" => double.PositiveInfinity,
        "-Infinity" => double.NegativeInfinity,
        _ => null,
    };

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

    private static LiteralValue? ParseDecimalJson(JsonElement value, string typeName) => value.ValueKind switch
    {
        // Decode the EXACT digits, never System.Decimal — decimal.TryParse and JsonElement.TryGetDecimal
        // silently ROUND a value with more than ~28-29 significant digits (e.g. a decimal(38,30) stat) to
        // System.Decimal's precision, which would shift a min/max bound and could wrongly skip a file.
        JsonValueKind.Number => ParseDecimalText(value.GetRawText(), typeName),
        JsonValueKind.String => ParseDecimalText(value.GetString(), typeName),
        _ => null,
    };

    /// <summary>
    /// Parses a decimal number's text (from a stats JSON number or a partition string) into a
    /// <see cref="LiteralValue"/> WITHOUT loss of precision: the exact digits become an unscaled
    /// <see cref="BigInteger"/> at the value's own scale (see <see cref="DecimalText"/> for why never through
    /// <see cref="decimal"/>), materialized as a <c>System.Decimal</c> only when that representation is
    /// exact, otherwise as a high-precision decimal.
    /// </summary>
    private static LiteralValue? ParseDecimalText(string? text, string typeName)
    {
        if (!DecimalText.TryParse(text, out var unscaled, out int scale))
            return null;

        // AT THE COLUMN'S DECLARED SCALE, not the text's own. JSON does not keep a trailing zero,
        // so a decimal(10,2) bound of 1.00 arrives as "1" and would carry scale 0; a writer that
        // pads instead sends "1.00" for a decimal(38,0) and would carry scale 2. Either way the
        // scale on the value is a fact about the FORMATTING, and StatisticsEvaluator reads it as a
        // fact about the TYPE -- it is what tells the pruner whether Spark would round this
        // comparison. #323.
        // A type name with no scale part keeps the text's own scale. One that HAS a scale part must
        // parse to a scale a Delta decimal can have, or the value is unknown: adopting an unchecked
        // scale such as decimal(38,50000000) would carry it into LiteralValue comparisons, which size a
        // power of ten by it (#760).
        if (typeName.IndexOf(',') >= 0)
        {
            // An invalid scale, or more decimals than the column declares: the statistic contradicts
            // its own type, and rescaling would move the bound. Unknown is the only safe answer.
            if (!TryDeclaredScale(typeName, out var declared) || !TryRescale(ref unscaled, scale, declared))
                return null;
            scale = declared;
        }

        return MakeDecimalLiteral(unscaled, scale);
    }

    /// <summary>The scale out of a <c>decimal(p,s)</c> type name, or -1 when it does not parse.</summary>
    private static bool TryDeclaredScale(string typeName, out int scale)
    {
        scale = -1;

        var comma = typeName.IndexOf(',');
        var close = typeName.IndexOf(')');
        if (comma < 0 || close <= comma)
            return false;

#if NETSTANDARD2_0
        var digits = typeName.Substring(comma + 1, close - comma - 1).Trim();
#else
        var digits = typeName.AsSpan(comma + 1, close - comma - 1).Trim();
#endif

        // Bounded like the precision: the scale sizes a power of ten in TryRescale, and the type name is
        // text another writer put in the log (#760).
        // On failure the out value is -1, never the rejected scale, so no caller can adopt it.
        if (int.TryParse(digits, NumberStyles.Integer, CultureInfo.InvariantCulture, out scale)
            && scale is >= 0 and <= DecimalText.MaxPrecision)
            return true;

        scale = -1;
        return false;
    }

    /// <summary>
    /// Moves an unscaled value to another scale, or false when that cannot be done exactly.
    /// </summary>
    /// <remarks>
    /// Padding is always exact. Trimming is exact only when the digits being dropped are zeros,
    /// and a value that fails that is one the declared type could not have held.
    /// </remarks>
    private static bool TryRescale(ref BigInteger unscaled, int from, int to)
    {
        if (to == from)
            return true;

        // No Delta decimal has a scale outside 0..38, and either one sizes a power of ten (#760).
        if (from is < 0 or > DecimalText.MaxPrecision || to is < 0 or > DecimalText.MaxPrecision)
            return false;

        if (to > from)
        {
            unscaled *= BigInteger.Pow(10, to - from);
            return true;
        }

        var divisor = BigInteger.Pow(10, from - to);
        var quotient = BigInteger.DivRem(unscaled, divisor, out var remainder);
        if (!remainder.IsZero)
            return false;

        unscaled = quotient;
        return true;
    }

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
