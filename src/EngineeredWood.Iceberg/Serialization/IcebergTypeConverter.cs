// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

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
            case GeometryType g: writer.WriteStringValue($"geometry({g.Crs})"); break;
            case GeographyType g: writer.WriteStringValue($"geography({g.Crs},{g.Algorithm})"); break;
            case DecimalType d: writer.WriteStringValue($"decimal({d.Precision},{d.Scale})"); break;
            case FixedType f: writer.WriteStringValue($"fixed[{f.Length}]"); break;
            case StructType s: WriteStruct(writer, s, options); break;
            case ListType l: WriteList(writer, l, options); break;
            case MapType m: WriteMap(writer, m, options); break;
            default: throw new JsonException($"Unknown Iceberg type: {value.GetType().Name}");
        }
    }

    private static IcebergType ParsePrimitive(string typeString)
    {
        if (typeString.StartsWith("decimal(", StringComparison.Ordinal))
        {
            var inner = typeString[8..^1];
            var comma = inner.IndexOf(',');
            var precision = int.Parse(inner[..comma]);
            var scale = int.Parse(inner[(comma + 1)..].Trim());
            return new DecimalType(precision, scale);
        }

        if (typeString.StartsWith("fixed[", StringComparison.Ordinal))
        {
            var length = int.Parse(typeString[6..^1]);
            return new FixedType(length);
        }

        if (typeString.StartsWith("geometry(", StringComparison.Ordinal))
        {
            var crs = typeString[9..^1];
            return new GeometryType(crs);
        }

        if (typeString.StartsWith("geography(", StringComparison.Ordinal))
        {
            var inner = typeString[10..^1];
            var comma = inner.IndexOf(',');
            var crs = inner[..comma];
            var algorithm = inner[(comma + 1)..].Trim();
            return new GeographyType(crs, algorithm);
        }

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
