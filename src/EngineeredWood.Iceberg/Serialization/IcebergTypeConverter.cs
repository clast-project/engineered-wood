// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace EngineeredWood.Iceberg.Serialization;

internal sealed class IcebergTypeConverter : JsonConverter<IcebergType>
{
    public override bool CanConvert(Type typeToConvert) =>
        typeof(IcebergType).IsAssignableFrom(typeToConvert);

    public override IcebergType Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        return ReadElement(doc.RootElement, options);
    }

    // Reads an IcebergType from an already-parsed element. Nested element/key/value types
    // recurse here directly rather than through JsonSerializer, keeping the path trim/AOT safe
    // without registering the polymorphic IcebergType hierarchy in the source-generated context.
    private static IcebergType ReadElement(JsonElement element, JsonSerializerOptions options)
    {
        if (element.ValueKind == JsonValueKind.String)
            return ParsePrimitive(element.GetString()!);

        if (element.ValueKind == JsonValueKind.Object)
        {
            var type = element.GetProperty("type").GetString()!;

            return type switch
            {
                "struct" => ReadStruct(element, options),
                "list" => ReadList(element, options),
                "map" => ReadMap(element, options),
                _ => throw new JsonException($"Unknown nested type: {type}")
            };
        }

        throw new JsonException($"Unexpected value kind: {element.ValueKind}");
    }

    public override void Write(Utf8JsonWriter writer, IcebergType value, JsonSerializerOptions options)
        => WriteType(writer, value, options);

    private static void WriteType(Utf8JsonWriter writer, IcebergType value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case BooleanType: writer.WriteStringValue("boolean"); break;
            case IntegerType: writer.WriteStringValue("int"); break;
            case LongType: writer.WriteStringValue("long"); break;
            case FloatType: writer.WriteStringValue("float"); break;
            case DoubleType: writer.WriteStringValue("double"); break;
            case DateType: writer.WriteStringValue("date"); break;
            case TimeType: writer.WriteStringValue("time"); break;
            case TimestampType: writer.WriteStringValue("timestamp"); break;
            case TimestamptzType: writer.WriteStringValue("timestamptz"); break;
            case TimestampNsType: writer.WriteStringValue("timestamp_ns"); break;
            case TimestamptzNsType: writer.WriteStringValue("timestamptz_ns"); break;
            case StringType: writer.WriteStringValue("string"); break;
            case UuidType: writer.WriteStringValue("uuid"); break;
            case BinaryType: writer.WriteStringValue("binary"); break;
            case UnknownType: writer.WriteStringValue("unknown"); break;
            case VariantType: writer.WriteStringValue("variant"); break;
            // Canonical spellings, as Java writes them: a space after each comma.
            case GeometryType g: writer.WriteStringValue($"geometry({g.Crs})"); break;
            case GeographyType g: writer.WriteStringValue($"geography({g.Crs}, {g.Algorithm})"); break;
            case DecimalType d: writer.WriteStringValue(
                "decimal(" + d.Precision.ToString(CultureInfo.InvariantCulture) + ", " +
                d.Scale.ToString(CultureInfo.InvariantCulture) + ")"); break;
            case FixedType f: writer.WriteStringValue($"fixed[{f.Length}]"); break;
            case StructType s: WriteStruct(writer, s, options); break;
            case ListType l: WriteList(writer, l, options); break;
            case MapType m: WriteMap(writer, m, options); break;
            default: throw new JsonException($"Unknown Iceberg type: {value.GetType().Name}");
        }
    }

    // Parameterized type strings. The spec's canonical forms put a space after each comma
    // ("decimal(9, 2)", "geography(srid:4326, spherical)"), and readers must accept optional whitespace
    // around parameters and separators. These are Java's own patterns (Types.java, 1.10 and main).
    private static readonly Regex DecimalPattern =
        new(@"^decimal\(\s*([0-9]+)\s*,\s*([0-9]+)\s*\)\z", RegexOptions.CultureInvariant);
    private static readonly Regex FixedPattern =
        new(@"^fixed\[\s*([0-9]+)\s*\]\z", RegexOptions.CultureInvariant);
    private static readonly Regex GeometryPattern =
        new(@"^geometry\s*(?:\(\s*([^)]*?)\s*\))?\z", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
    private static readonly Regex GeographyPattern =
        new(@"^geography\s*(?:\(\s*([^,]*?)\s*(?:,\s*(\w*)\s*)?\))?\z",
            RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    /// <summary>The CRS a geometry or geography type has when its type string names none.</summary>
    internal const string DefaultCrs = "OGC:CRS84";

    /// <summary>The widest decimal precision Iceberg allows.</summary>
    internal const int MaxDecimalPrecision = 38;

    /// <summary>The edge algorithm a geography type has when its type string names none.</summary>
    internal const string DefaultEdgeAlgorithm = "spherical";

    internal static IcebergType ParsePrimitive(string typeString)
    {
        var trimmed = typeString.Trim();

        if (trimmed.StartsWith("decimal", StringComparison.Ordinal))
        {
            var m = DecimalPattern.Match(trimmed);
            if (!m.Success)
                throw new JsonException($"Invalid decimal type: {typeString}");
            int precision = ParseInt(m.Groups[1].Value, typeString);
            int scale = ParseInt(m.Groups[2].Value, typeString);
            // Iceberg decimals hold at most 38 digits; Java refuses anything wider. Precision 0, or a scale
            // above the precision, is no decimal Avro or Parquet can store either (both require
            // 0 <= scale <= precision and precision >= 1), so such metadata could not be written back.
            if (precision is < 1 or > MaxDecimalPrecision)
                throw new JsonException(
                    $"Invalid decimal type: {typeString}; precision must be 1 to {MaxDecimalPrecision}.");
            if (scale > precision)
                throw new JsonException($"Invalid decimal type: {typeString}; scale exceeds precision.");
            return new DecimalType(precision, scale);
        }

        if (trimmed.StartsWith("fixed", StringComparison.Ordinal))
        {
            var m = FixedPattern.Match(trimmed);
            if (!m.Success)
                throw new JsonException($"Invalid fixed type: {typeString}");
            return new FixedType(ParseInt(m.Groups[1].Value, typeString));
        }

        if (trimmed.StartsWith("geometry", StringComparison.OrdinalIgnoreCase))
        {
            var m = GeometryPattern.Match(trimmed);
            if (!m.Success)
                throw new JsonException($"Invalid geometry type: {typeString}");
            // Bare "geometry" takes the default CRS; "geometry()" names an empty one, which Java refuses.
            var crs = m.Groups[1].Success ? NonEmpty(m.Groups[1].Value, "CRS", typeString) : DefaultCrs;
            return new GeometryType(crs);
        }

        if (trimmed.StartsWith("geography", StringComparison.OrdinalIgnoreCase))
        {
            var m = GeographyPattern.Match(trimmed);
            if (!m.Success)
                throw new JsonException($"Invalid geography type: {typeString}");
            // Omitted parameters take the defaults; present but empty ones ("geography()", "geography(c, )")
            // are refused.
            var crs = m.Groups[1].Success ? NonEmpty(m.Groups[1].Value, "CRS", typeString) : DefaultCrs;
            var algorithm = m.Groups[2].Success
                ? NonEmpty(m.Groups[2].Value, "edge algorithm", typeString)
                : DefaultEdgeAlgorithm;
            return new GeographyType(crs, algorithm);
        }

        typeString = trimmed;
        return typeString switch
        {
            "boolean" => IcebergType.Boolean,
            "int" => IcebergType.Int,
            "long" => IcebergType.Long,
            "float" => IcebergType.Float,
            "double" => IcebergType.Double,
            "date" => IcebergType.Date,
            "time" => IcebergType.Time,
            "timestamp" => IcebergType.Timestamp,
            "timestamptz" => IcebergType.Timestamptz,
            "timestamp_ns" => IcebergType.TimestampNs,
            "timestamptz_ns" => IcebergType.TimestamptzNs,
            "string" => IcebergType.String,
            "uuid" => IcebergType.Uuid,
            "binary" => IcebergType.Binary,
            "unknown" => IcebergType.Unknown,
            "variant" => IcebergType.Variant,
            _ => throw new JsonException($"Unknown primitive type: {typeString}")
        };
    }

    private static string NonEmpty(string value, string what, string typeString) =>
        value.Length > 0 ? value : throw new JsonException($"Invalid type {typeString}: empty {what}.");

    private static int ParseInt(string digits, string typeString) =>
        int.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new JsonException($"Type parameter out of range: {typeString}");

    private static StructType ReadStruct(JsonElement element, JsonSerializerOptions options)
    {
        var fields = element.GetProperty("fields").Deserialize(options.TypeInfo<List<NestedField>>())!;
        return new StructType(fields);
    }

    private static ListType ReadList(JsonElement element, JsonSerializerOptions options)
    {
        var elementId = element.GetProperty("element-id").GetInt32();
        var elementType = ReadElement(element.GetProperty("element"), options);
        var elementRequired = element.GetProperty("element-required").GetBoolean();
        return new ListType(elementId, elementType, elementRequired);
    }

    private static MapType ReadMap(JsonElement element, JsonSerializerOptions options)
    {
        var keyId = element.GetProperty("key-id").GetInt32();
        var keyType = ReadElement(element.GetProperty("key"), options);
        var valueId = element.GetProperty("value-id").GetInt32();
        var valueType = ReadElement(element.GetProperty("value"), options);
        var valueRequired = element.GetProperty("value-required").GetBoolean();
        return new MapType(keyId, keyType, valueId, valueType, valueRequired);
    }

    private static void WriteStruct(Utf8JsonWriter writer, StructType structType, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("type", "struct");
        writer.WritePropertyName("fields");
        JsonSerializer.Serialize(writer, structType.Fields, options.TypeInfo<IReadOnlyList<NestedField>>());
        writer.WriteEndObject();
    }

    private static void WriteList(Utf8JsonWriter writer, ListType listType, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("type", "list");
        writer.WriteNumber("element-id", listType.ElementId);
        writer.WritePropertyName("element");
        WriteType(writer, listType.ElementType, options);
        writer.WriteBoolean("element-required", listType.ElementRequired);
        writer.WriteEndObject();
    }

    private static void WriteMap(Utf8JsonWriter writer, MapType mapType, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("type", "map");
        writer.WriteNumber("key-id", mapType.KeyId);
        writer.WritePropertyName("key");
        WriteType(writer, mapType.KeyType, options);
        writer.WriteNumber("value-id", mapType.ValueId);
        writer.WritePropertyName("value");
        WriteType(writer, mapType.ValueType, options);
        writer.WriteBoolean("value-required", mapType.ValueRequired);
        writer.WriteEndObject();
    }
}
