// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using EngineeredWood.Expressions;

namespace EngineeredWood.Iceberg.Values;

/// <summary>
/// The Iceberg spec's JSON single-value serialization (Appendix D): a field's <c>initial-default</c> and
/// <c>write-default</c> in table metadata, and single values elsewhere in JSON.
/// </summary>
/// <remarks>
/// <para>
/// Reading follows what Iceberg Java 1.12 accepts, which is stricter than "any JSON that means the value":
/// a <c>float</c> or <c>double</c> must be written with a decimal point or an exponent (<c>1</c> is refused,
/// <c>1.0</c> read), an <c>int</c> or <c>long</c> without either; a decimal must be a string at exactly the
/// type's scale (<c>"14.20"</c> for <c>decimal(9, 2)</c>, not <c>"14.2"</c>, though <c>"1420E-2"</c> is that
/// scale too); a <c>timestamptz</c> must carry an offset and the offset must be zero (<c>Z</c>,
/// <c>+00:00</c> or <c>-00:00</c>), and a <c>timestamp</c> must carry none. A time or timestamp may give up
/// to nine fractional digits; those finer than the type are dropped, toward zero, as Java does.
/// </para>
/// <para>
/// Two places are stricter than Java, which reads the value and loses it: a decimal with more digits than
/// its precision is refused, where Java accepts it; and a float or double too large for its type is
/// refused, where Java reads an infinity it cannot then write back. NaN and the infinities have no JSON
/// number and are refused both ways, as Java refuses to read them.
/// </para>
/// <para>
/// Writing produces what Java writes, except that binary and fixed values are lower-case hex, as the spec's
/// example is (Java writes upper case and reads either).
/// </para>
/// <para>
/// Nested values (struct, list, map) have no <see cref="LiteralValue"/> form. <see cref="Validate"/> checks
/// one against its type; keep the <see cref="JsonElement"/> itself as the value.
/// </para>
/// </remarks>
public static class SingleValueJson
{
    /// <summary>Reads a JSON single value of a primitive type.</summary>
    /// <param name="type">The value's Iceberg type.</param>
    /// <param name="json">The JSON value. A JSON null reads as <see cref="LiteralValue.Null"/>.</param>
    /// <returns>The value, in the kind <see cref="IcebergLiterals.KindOf"/> gives for the type.</returns>
    /// <exception cref="FormatException">The JSON is not a value of the type.</exception>
    /// <exception cref="NotSupportedException">The type is nested, or has no non-null single value
    /// (<c>unknown</c>, <c>variant</c>; <c>geometry</c> and <c>geography</c>, whose WKT is not parsed), or the
    /// value is outside what .NET's date types hold.</exception>
    public static LiteralValue Parse(IcebergType type, JsonElement json)
    {
        if (json.ValueKind == JsonValueKind.Null)
            return LiteralValue.Null;

        switch (type)
        {
            case BooleanType:
                return json.ValueKind switch
                {
                    JsonValueKind.True => LiteralValue.Of(true),
                    JsonValueKind.False => LiteralValue.Of(false),
                    _ => throw Invalid(type, json, null),
                };
            case IntegerType:
                return IsIntegralToken(json) && json.TryGetInt32(out int i)
                    ? LiteralValue.Of(i)
                    : throw Invalid(type, json, null);
            case LongType:
                return IsIntegralToken(json) && json.TryGetInt64(out long l)
                    ? LiteralValue.Of(l)
                    : throw Invalid(type, json, null);
            case FloatType:
                if (!IsFloatingToken(json) || !ExactFloatText.TryParseSingle(json.GetRawText(), out float f))
                    throw Invalid(type, json, "a float is a JSON number with a decimal point or an exponent");
                return float.IsInfinity(f) ? throw Invalid(type, json, "it is out of range") : LiteralValue.Of(f);
            case DoubleType:
                if (!IsFloatingToken(json) || !ExactFloatText.TryParseDouble(json.GetRawText(), out double d))
                    throw Invalid(type, json, "a double is a JSON number with a decimal point or an exponent");
                return double.IsInfinity(d) ? throw Invalid(type, json, "it is out of range") : LiteralValue.Of(d);
            case DecimalType dt:
                return ParseDecimal(dt, json);
            case DateType:
                return IsoTemporalText.TryParseDate(RequireString(type, json), out long days)
                    ? IcebergLiterals.DateLiteral(days)
                    : throw Invalid(type, json, "a date is an ISO-8601 date, yyyy-MM-dd");
            case TimeType:
                return IsoTemporalText.TryParseTime(RequireString(type, json), out long nanosOfDay)
                    ? IcebergLiterals.TimeLiteral(nanosOfDay / 1000)
                    : throw Invalid(type, json, "a time is an ISO-8601 time, HH:mm:ss.ffffff");
            case TimestampType or TimestamptzType:
                return IcebergLiterals.MicrosLiteral(ParseTimestamp(type, json, nanosPerUnit: 1000));
            case TimestampNsType or TimestamptzNsType:
                return LiteralValue.TimestampNanosOf(ParseTimestamp(type, json, nanosPerUnit: 1));
            case StringType:
                return LiteralValue.Of(RequireString(type, json));
            case UuidType:
                return Guid.TryParseExact(RequireString(type, json), "D", out var g)
                    ? LiteralValue.Of(g)
                    : throw Invalid(type, json, "a uuid is a hyphenated hex string");
            case FixedType fx:
                var fixedBytes = ParseHex(type, json);
                return fixedBytes.Length == fx.Length
                    ? LiteralValue.Of(fixedBytes)
                    : throw Invalid(type, json, $"it holds {fixedBytes.Length} bytes, not {fx.Length}");
            case BinaryType:
                return LiteralValue.Of(ParseHex(type, json));
            case GeometryType or GeographyType:
                RequireString(type, json);
                throw new NotSupportedException(
                    $"A {IcebergLiterals.TypeName(type)} single value is WKT, which is not parsed; keep the JSON value.");
            case UnknownType or VariantType:
                throw Invalid(type, json, "its only single value is null");
            case NestedType:
                throw new NotSupportedException(
                    $"A {IcebergLiterals.TypeName(type)} value has no LiteralValue form; validate it with "
                    + $"{nameof(SingleValueJson)}.{nameof(Validate)} and keep the JSON value.");
        }

        throw IcebergLiterals.Unsupported(type);
    }

    /// <summary>
    /// Checks that <paramref name="json"/> is a JSON single value of <paramref name="type"/>, nested types included.
    /// </summary>
    /// <remarks>
    /// A struct is an object keyed by field id (as a string), a list an array, a map an object holding
    /// <c>keys</c> and <c>values</c> arrays of the same length. A required field, element or value may not be
    /// null; an optional struct field may be missing. A <c>geometry</c> or <c>geography</c> value must be a
    /// string; its WKT is not checked.
    /// </remarks>
    /// <exception cref="FormatException">It is not, with the path to the first value that is not.</exception>
    public static void Validate(IcebergType type, JsonElement json) => ValidateAt(type, json, "$");

    /// <summary>Writes a value of a primitive type as a JSON single value. A null literal writes JSON null.</summary>
    /// <exception cref="ArgumentException">The value is not one of the type's, or is a NaN or an infinity.</exception>
    /// <exception cref="NotSupportedException">The type is nested, or has no non-null single value.</exception>
    public static void Write(Utf8JsonWriter writer, IcebergType type, LiteralValue value)
    {
#if NET6_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(writer);
#else
        if (writer is null) throw new ArgumentNullException(nameof(writer));
#endif
        if (value.IsNull)
        {
            writer.WriteNullValue();
            return;
        }

        if (type is GeometryType or GeographyType or UnknownType or VariantType or NestedType)
            throw new NotSupportedException($"A non-null {IcebergLiterals.TypeName(type)} value cannot be written as a JSON single value.");

        var v = IcebergLiterals.Coerce(type, value);
        switch (type)
        {
            case BooleanType:
                writer.WriteBooleanValue(v.AsBoolean);
                return;
            case IntegerType:
                writer.WriteNumberValue(v.AsInt32);
                return;
            case LongType:
                writer.WriteNumberValue(v.AsInt64);
                return;
            case FloatType:
                writer.WriteRawValue(FloatText(v.AsFloat), skipInputValidation: true);
                return;
            case DoubleType:
                writer.WriteRawValue(DoubleText(v.AsDouble), skipInputValidation: true);
                return;
            case DecimalType:
                var (unscaled, scale) = v.AsHighPrecisionDecimal;
                writer.WriteStringValue(DecimalText(unscaled, scale));
                return;
            case DateType:
                writer.WriteStringValue(IsoTemporalText.FormatDate(IcebergLiterals.ToEpochDays(type, v)));
                return;
            case TimeType:
                writer.WriteStringValue(IsoTemporalText.FormatTime(IcebergLiterals.ToTimeMicros(type, v) * 1000));
                return;
            case TimestampType or TimestamptzType:
                long micros = IcebergLiterals.ToEpochMicros(type, v);
                writer.WriteStringValue(IsoTemporalText.FormatTimestamp(
                    IcebergLiterals.FloorDiv(micros, IcebergLiterals.MicrosPerDay),
                    IcebergLiterals.FloorMod(micros, IcebergLiterals.MicrosPerDay) * 1000,
                    utcOffset: type is TimestamptzType));
                return;
            case TimestampNsType or TimestamptzNsType:
                long nanos = v.AsTimestampNanos;
                writer.WriteStringValue(IsoTemporalText.FormatTimestamp(
                    IcebergLiterals.FloorDiv(nanos, IcebergLiterals.NanosPerDay),
                    IcebergLiterals.FloorMod(nanos, IcebergLiterals.NanosPerDay),
                    utcOffset: type is TimestamptzNsType));
                return;
            case StringType:
                writer.WriteStringValue(v.AsString);
                return;
            case UuidType:
                writer.WriteStringValue(v.AsGuid.ToString("D"));
                return;
            case FixedType or BinaryType:
                writer.WriteStringValue(HexText(v.AsBinary));
                return;
        }

        throw IcebergLiterals.Unsupported(type);
    }

    /// <summary>A value of a primitive type as JSON single-value text.</summary>
    /// <inheritdoc cref="Write" path="/exception"/>
    public static string ToJsonString(IcebergType type, LiteralValue value)
    {
        using var stream = new MemoryStream();
        // Non-ASCII text unescaped, as Java writes it; the default encoder escapes it.
        var options = new JsonWriterOptions { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };
        using (var writer = new Utf8JsonWriter(stream, options))
            Write(writer, type, value);
        return Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length);
    }

    /// <summary>A value of a primitive type as a JSON single value.</summary>
    /// <inheritdoc cref="Write" path="/exception"/>
    public static JsonElement ToJsonElement(IcebergType type, LiteralValue value)
    {
        using var document = JsonDocument.Parse(ToJsonString(type, value));
        return document.RootElement.Clone();
    }

    // ── Reading pieces ──

    private static void ValidateAt(IcebergType type, JsonElement json, string path)
    {
        if (json.ValueKind == JsonValueKind.Null)
            return;

        switch (type)
        {
            case StructType st:
                if (json.ValueKind != JsonValueKind.Object)
                    throw InvalidAt(path, type, json, "a struct is a JSON object keyed by field id");
                foreach (var property in json.EnumerateObject())
                {
                    if (!int.TryParse(property.Name, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int id)
                        || st.Fields.FirstOrDefault(f => f.Id == id) is not { } field)
                        throw InvalidAt(path, type, json, $"'{property.Name}' is not the id of one of its fields");
                    string fieldPath = $"{path}['{property.Name}']";
                    if (field.IsRequired && property.Value.ValueKind == JsonValueKind.Null)
                        throw InvalidAt(fieldPath, field.Type, property.Value, "the field is required");
                    ValidateAt(field.Type, property.Value, fieldPath);
                }

                return;
            case ListType lt:
                if (json.ValueKind != JsonValueKind.Array)
                    throw InvalidAt(path, type, json, "a list is a JSON array");
                int index = 0;
                foreach (var element in json.EnumerateArray())
                {
                    string elementPath = $"{path}[{index++}]";
                    if (lt.ElementRequired && element.ValueKind == JsonValueKind.Null)
                        throw InvalidAt(elementPath, lt.ElementType, element, "the element is required");
                    ValidateAt(lt.ElementType, element, elementPath);
                }

                return;
            case MapType mt:
                if (json.ValueKind != JsonValueKind.Object
                    || !json.TryGetProperty("keys", out var keys) || keys.ValueKind != JsonValueKind.Array
                    || !json.TryGetProperty("values", out var values) || values.ValueKind != JsonValueKind.Array
                    || json.EnumerateObject().Count() != 2)
                    throw InvalidAt(path, type, json, "a map is a JSON object of 'keys' and 'values' arrays");
                if (keys.GetArrayLength() != values.GetArrayLength())
                    throw InvalidAt(path, type, json, "its 'keys' and 'values' differ in length");
                int k = 0;
                foreach (var key in keys.EnumerateArray())
                {
                    string keyPath = $"{path}.keys[{k++}]";
                    if (key.ValueKind == JsonValueKind.Null)
                        throw InvalidAt(keyPath, mt.KeyType, key, "a map key is never null");
                    ValidateAt(mt.KeyType, key, keyPath);
                }

                int vi = 0;
                foreach (var value in values.EnumerateArray())
                {
                    string valuePath = $"{path}.values[{vi++}]";
                    if (mt.ValueRequired && value.ValueKind == JsonValueKind.Null)
                        throw InvalidAt(valuePath, mt.ValueType, value, "the value is required");
                    ValidateAt(mt.ValueType, value, valuePath);
                }

                return;
            case GeometryType or GeographyType:
                if (json.ValueKind != JsonValueKind.String)
                    throw InvalidAt(path, type, json, "a geometry or geography value is a WKT string");
                return;
            default:
                try
                {
                    Parse(type, json);
                }
                catch (FormatException e)
                {
                    throw new FormatException(path == "$" ? e.Message : $"At {path}: {e.Message}", e);
                }
                catch (NotSupportedException) when (type is TimestampType or TimestamptzType or DateType)
                {
                    // A well-formed date or timestamp outside the years .NET's types hold is still a value.
                }

                return;
        }
    }

    private static LiteralValue ParseDecimal(DecimalType type, JsonElement json)
    {
        string text = RequireString(type, json);
        if (!TryParseDecimalText(text, out var unscaled, out int scale))
            throw Invalid(type, json, "a decimal is a string of digits");
        if (scale != type.Scale)
            throw Invalid(type, json, $"its scale is {scale}, not {type.Scale}");
        if (!IcebergLiterals.FitsPrecision(unscaled, type.Precision))
            throw Invalid(type, json, $"it has more than {type.Precision} digits");
        return LiteralValue.HighPrecisionDecimalOf(unscaled, scale);
    }

    /// <summary>Java <c>BigDecimal</c> text: a sign, digits with an optional point, an optional exponent.</summary>
    internal static bool TryParseDecimalText(string text, out BigInteger unscaled, out int scale)
    {
        unscaled = default;
        scale = 0;
        int pos = 0;
        bool negative = false;
        if (pos < text.Length && text[pos] is '+' or '-')
            negative = text[pos++] == '-';

        var digits = new StringBuilder(text.Length);
        int fractionDigits = 0;
        bool point = false;
        for (; pos < text.Length; pos++)
        {
            char c = text[pos];
            if (c is >= '0' and <= '9')
            {
                digits.Append(c);
                if (point)
                    fractionDigits++;
            }
            else if (c == '.' && !point)
            {
                point = true;
            }
            else
            {
                break;
            }
        }

        if (digits.Length == 0)
            return false;

        long exponent = 0;
        if (pos < text.Length && text[pos] is 'e' or 'E')
        {
            pos++;
            bool expNegative = false;
            if (pos < text.Length && text[pos] is '+' or '-')
                expNegative = text[pos++] == '-';
            int start = pos;
            while (pos < text.Length && text[pos] is >= '0' and <= '9' && pos - start < 10)
                pos++;
            if (pos == start)
                return false;
            exponent = long.Parse(text.Substring(start, pos - start), NumberStyles.None, CultureInfo.InvariantCulture);
            if (expNegative)
                exponent = -exponent;
        }

        if (pos != text.Length)
            return false;

        long s = fractionDigits - exponent;
        if (s is < int.MinValue or > int.MaxValue)
            return false;

        unscaled = BigInteger.Parse(digits.ToString(), NumberStyles.None, CultureInfo.InvariantCulture);
        if (negative)
            unscaled = -unscaled;
        scale = (int)s;
        return true;
    }

    private static long ParseTimestamp(IcebergType type, JsonElement json, long nanosPerUnit)
    {
        string text = RequireString(type, json);
        bool zoned = type is TimestamptzType or TimestamptzNsType;
        if (!IsoTemporalText.TryParseTimestamp(text, out long days, out long nanosOfDay, out var offset))
            throw Invalid(type, json, "a timestamp is an ISO-8601 date and time" + (zoned ? " with an offset" : ""));
        if (zoned && offset == IsoTemporalText.Offset.None)
            throw Invalid(type, json, "a timestamptz value carries an offset, +00:00");
        if (zoned && offset == IsoTemporalText.Offset.NonZero)
            throw Invalid(type, json, "the offset must be +00:00");
        if (!zoned && offset != IsoTemporalText.Offset.None)
            throw Invalid(type, json, "a timestamp without a zone carries no offset");

        // Truncated toward zero, as Java's ChronoUnit.between is, not floored.
        var nanos = (new BigInteger(days) * IcebergLiterals.NanosPerDay) + nanosOfDay;
        var units = nanos / nanosPerUnit;
        if (units < long.MinValue || units > long.MaxValue)
            throw Invalid(type, json, "it is out of range");
        return (long)units;
    }

    private static byte[] ParseHex(IcebergType type, JsonElement json)
    {
        string text = RequireString(type, json);
        if (text.Length % 2 != 0)
            throw Invalid(type, json, "hex text has an even number of digits");
        var bytes = new byte[text.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            int hi = HexDigit(text[2 * i]), lo = HexDigit(text[(2 * i) + 1]);
            if (hi < 0 || lo < 0)
                throw Invalid(type, json, "it is not hex");
            bytes[i] = (byte)((hi << 4) | lo);
        }

        return bytes;
    }

    private static int HexDigit(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _ => -1,
    };

    private static string RequireString(IcebergType type, JsonElement json) =>
        json.ValueKind == JsonValueKind.String
            ? json.GetString()!
            : throw Invalid(type, json, $"a {IcebergLiterals.TypeName(type)} value is a JSON string");

    private static bool IsIntegralToken(JsonElement json) =>
        json.ValueKind == JsonValueKind.Number && json.GetRawText().IndexOfAny(['.', 'e', 'E']) < 0;

    private static bool IsFloatingToken(JsonElement json) =>
        json.ValueKind == JsonValueKind.Number && json.GetRawText().IndexOfAny(['.', 'e', 'E']) >= 0;

    // ── Writing pieces ──

    private static string FloatText(float value)
    {
        if (float.IsNaN(value) || float.IsInfinity(value))
            throw new ArgumentException($"{value} has no JSON single value: JSON has no NaN or infinity.", nameof(value));
        // .NET Framework formats -0.0 as "0", dropping the sign.
        if (value == 0)
            return SingleBits(value) < 0 ? "-0.0" : "0.0";
        string text = value.ToString("R", CultureInfo.InvariantCulture);
        if (!RoundTrips(text))
            text = value.ToString("G9", CultureInfo.InvariantCulture);
        return RoundTrips(text)
            ? EnsureFloating(text)
            : throw new InvalidOperationException($"No text found that reads back as the float {text}.");

        bool RoundTrips(string t) => ExactFloatText.TryParseSingle(t, out float back) && SingleBits(back) == SingleBits(value);
    }

    private static string DoubleText(double value)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
            throw new ArgumentException($"{value} has no JSON single value: JSON has no NaN or infinity.", nameof(value));
        if (value == 0)
            return BitConverter.DoubleToInt64Bits(value) < 0 ? "-0.0" : "0.0";
        string text = value.ToString("R", CultureInfo.InvariantCulture);
        if (!RoundTrips(text))
            text = value.ToString("G17", CultureInfo.InvariantCulture);
        return RoundTrips(text)
            ? EnsureFloating(text)
            : throw new InvalidOperationException($"No text found that reads back as the double {text}.");

        bool RoundTrips(string t) => ExactFloatText.TryParseDouble(t, out double back)
            && BitConverter.DoubleToInt64Bits(back) == BitConverter.DoubleToInt64Bits(value);
    }

    private static unsafe int SingleBits(float value) => *(int*)&value;

    /// <summary>Java reads a float or double only from a token with a point or an exponent.</summary>
    private static string EnsureFloating(string text) =>
        text.IndexOfAny(['.', 'E', 'e']) >= 0 ? text : text + ".0";

    /// <summary>Plain decimal text at exactly <paramref name="scale"/> digits after the point.</summary>
    internal static string DecimalText(BigInteger unscaled, int scale)
    {
        if (scale < 0)
            throw new NotSupportedException($"A decimal of negative scale {scale} is not written.");
        string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture);
        string sign = unscaled.Sign < 0 ? "-" : "";
        if (scale == 0)
            return sign + digits;
        if (digits.Length <= scale)
            digits = new string('0', scale - digits.Length + 1) + digits;
        return sign + digits.Substring(0, digits.Length - scale) + "." + digits.Substring(digits.Length - scale);
    }

    private static string HexText(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (byte b in bytes)
            sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    private static FormatException Invalid(IcebergType type, JsonElement json, string? why) =>
        new($"{Shorten(json.GetRawText())} is not a JSON single value of Iceberg type {IcebergLiterals.TypeName(type)}"
            + (why is null ? "." : $": {why}."));

    private static FormatException InvalidAt(string path, IcebergType type, JsonElement json, string why) =>
        new($"At {path}: {Shorten(json.GetRawText())} is not a JSON single value of Iceberg type "
            + $"{IcebergLiterals.TypeName(type)}: {why}.");

    private static string Shorten(string text) => text.Length <= 80 ? text : text.Substring(0, 77) + "...";
}
