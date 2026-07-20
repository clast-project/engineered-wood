// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Parses and serializes Delta Lake JSON schema representations.
/// </summary>
internal static class DeltaSchemaSerializer
{
    /// <summary>
    /// Parses a Delta schema JSON string into a <see cref="StructType"/>.
    /// </summary>
    public static StructType Parse(string json)
    {
        var doc = JsonDocument.Parse(json);
        return ParseStructType(doc.RootElement);
    }

    /// <summary>
    /// Serializes a <see cref="StructType"/> to a JSON string.
    /// </summary>
    public static string Serialize(StructType schema)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        WriteType(writer, schema);
        writer.Flush();
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    private static DeltaDataType ParseType(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.String)
            return new PrimitiveType { TypeName = element.GetString()! };

        if (element.ValueKind != JsonValueKind.Object)
            throw new DeltaLake.DeltaFormatException(
                $"Expected string or object for type, got {element.ValueKind}.");

        string typeName = element.GetProperty("type").GetString()!;
        return typeName switch
        {
            "struct" => ParseStructType(element),
            "array" => ParseArrayType(element),
            "map" => ParseMapType(element),
            _ => throw new DeltaLake.DeltaFormatException(
                $"Unknown complex type: {typeName}"),
        };
    }

    private static StructType ParseStructType(JsonElement element)
    {
        var fieldsElement = element.GetProperty("fields");
        var fields = new List<StructField>();

        foreach (var fieldElement in fieldsElement.EnumerateArray())
        {
            Dictionary<string, string>? metadata = null;
            if (fieldElement.TryGetProperty("metadata", out var metaElement) &&
                metaElement.ValueKind == JsonValueKind.Object)
            {
                metadata = new Dictionary<string, string>();
                foreach (var prop in metaElement.EnumerateObject())
                {
                    metadata[prop.Name] = prop.Value.ValueKind == JsonValueKind.String
                        ? prop.Value.GetString()!
                        : prop.Value.GetRawText();
                }
                if (metadata.Count == 0)
                    metadata = null;
            }

            fields.Add(new StructField
            {
                Name = fieldElement.GetProperty("name").GetString()!,
                Type = ParseType(fieldElement.GetProperty("type")),
                Nullable = fieldElement.GetProperty("nullable").GetBoolean(),
                Metadata = metadata,
            });
        }

        return new StructType { Fields = fields };
    }

    private static ArrayType ParseArrayType(JsonElement element) =>
        new()
        {
            ElementType = ParseType(element.GetProperty("elementType")),
            ContainsNull = element.GetProperty("containsNull").GetBoolean(),
        };

    private static MapType ParseMapType(JsonElement element) =>
        new()
        {
            KeyType = ParseType(element.GetProperty("keyType")),
            ValueType = ParseType(element.GetProperty("valueType")),
            ValueContainsNull = element.GetProperty("valueContainsNull").GetBoolean(),
        };

    private static void WriteType(Utf8JsonWriter writer, DeltaDataType type)
    {
        switch (type)
        {
            case PrimitiveType p:
                writer.WriteStringValue(p.TypeName);
                break;

            case StructType s:
                writer.WriteStartObject();
                writer.WriteString("type", "struct");
                writer.WritePropertyName("fields");
                writer.WriteStartArray();
                foreach (var field in s.Fields)
                {
                    writer.WriteStartObject();
                    writer.WriteString("name", field.Name);
                    writer.WritePropertyName("type");
                    WriteType(writer, field.Type);
                    writer.WriteBoolean("nullable", field.Nullable);
                    writer.WritePropertyName("metadata");
                    if (field.Metadata is not null && field.Metadata.Count > 0)
                    {
                        writer.WriteStartObject();
                        foreach (var kvp in field.Metadata)
                        {
                            // delta.columnMapping.id is a NUMERIC field-id in the Delta spec — a strict reader
                            // (Spark: Metadata.getLong) fails if it's serialized as a string. Emit it as a JSON
                            // number; every other metadata value (physicalName, comments, …) stays a string.
                            if (kvp.Key == ColumnMapping.FieldIdKey && long.TryParse(kvp.Value, out var fieldId))
                                writer.WriteNumber(kvp.Key, fieldId);
                            else
                                writer.WriteString(kvp.Key, kvp.Value);
                        }
                        writer.WriteEndObject();
                    }
                    else
                    {
                        writer.WriteStartObject();
                        writer.WriteEndObject();
                    }
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
                break;

            case ArrayType a:
                writer.WriteStartObject();
                writer.WriteString("type", "array");
                writer.WritePropertyName("elementType");
                WriteType(writer, a.ElementType);
                writer.WriteBoolean("containsNull", a.ContainsNull);
                writer.WriteEndObject();
                break;

            case MapType m:
                writer.WriteStartObject();
                writer.WriteString("type", "map");
                writer.WritePropertyName("keyType");
                WriteType(writer, m.KeyType);
                writer.WritePropertyName("valueType");
                WriteType(writer, m.ValueType);
                writer.WriteBoolean("valueContainsNull", m.ValueContainsNull);
                writer.WriteEndObject();
                break;
        }
    }
}
