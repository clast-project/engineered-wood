// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.Avro.Schema;

/// <summary>
/// Computes the Parsing Canonical Form (PCF) of an Avro schema per the Avro specification.
/// The PCF is a normalized JSON representation used to compute schema fingerprints.
/// </summary>
internal static class ParsingCanonicalForm
{
    /// <summary>
    /// Returns the Parsing Canonical Form JSON string for the given schema node.
    /// </summary>
    public static string ToCanonicalJson(AvroSchemaNode schema)
    {
        using var ms = new MemoryStream();
        using (var writer = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = false }))
        {
            var namedTypes = new HashSet<string>();
            WriteNode(writer, schema, namedTypes);
        }
        return System.Text.Encoding.UTF8.GetString(ms.ToArray());
    }

    private static void WriteNode(Utf8JsonWriter writer, AvroSchemaNode node, HashSet<string> namedTypes)
    {
        switch (node)
        {
            case AvroPrimitiveSchema p:
                writer.WriteStringValue(PrimitiveTypeName(p.Type));
                break;

            case AvroRecordSchema r:
                WriteRecord(writer, r, namedTypes);
                break;

            case AvroEnumSchema e:
                WriteEnum(writer, e, namedTypes);
                break;

            case AvroArraySchema a:
                writer.WriteStartObject();
                writer.WriteString("type", "array");
                writer.WritePropertyName("items");
                WriteNode(writer, a.Items, namedTypes);
                writer.WriteEndObject();
                break;

            case AvroMapSchema m:
                writer.WriteStartObject();
                writer.WriteString("type", "map");
                writer.WritePropertyName("values");
                WriteNode(writer, m.Values, namedTypes);
                writer.WriteEndObject();
                break;

            case AvroFixedSchema f:
                WriteFixed(writer, f, namedTypes);
                break;

            case AvroUnionSchema u:
                writer.WriteStartArray();
                foreach (var branch in u.Branches)
                    WriteNode(writer, branch, namedTypes);
                writer.WriteEndArray();
                break;
        }
    }

    private static void WriteRecord(Utf8JsonWriter writer, AvroRecordSchema r, HashSet<string> namedTypes)
    {
        if (!namedTypes.Add(r.FullName))
        {
            // Already defined — emit as a reference by full name
            writer.WriteStringValue(r.FullName);
            return;
        }

        writer.WriteStartObject();
        // Canonical order for records: name, type, fields
        writer.WriteString("name", r.FullName);
        writer.WriteString("type", "record");
        writer.WriteStartArray("fields");
        foreach (var field in r.Fields)
        {
            writer.WriteStartObject();
            // Canonical order for fields: name, type
            writer.WriteString("name", field.Name);
            writer.WritePropertyName("type");
            WriteNode(writer, field.Schema, namedTypes);
            // No default, doc, aliases, order in canonical form
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    private static void WriteEnum(Utf8JsonWriter writer, AvroEnumSchema e, HashSet<string> namedTypes)
    {
        if (!namedTypes.Add(e.FullName))
        {
            writer.WriteStringValue(e.FullName);
            return;
        }

        writer.WriteStartObject();
        // Canonical order for enums: name, type, symbols
        writer.WriteString("name", e.FullName);
        writer.WriteString("type", "enum");
        writer.WriteStartArray("symbols");
        foreach (var sym in e.Symbols)
            writer.WriteStringValue(sym);
        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    private static void WriteFixed(Utf8JsonWriter writer, AvroFixedSchema f, HashSet<string> namedTypes)
    {
        if (!namedTypes.Add(f.FullName))
        {
            writer.WriteStringValue(f.FullName);
            return;
        }

        writer.WriteStartObject();
        // Canonical order for fixed: name, type, size
        writer.WriteString("name", f.FullName);
        writer.WriteString("type", "fixed");
        writer.WriteNumber("size", f.Size);
        writer.WriteEndObject();
    }

    private static string PrimitiveTypeName(AvroType type) => type switch
    {
        AvroType.Null => "null",
        AvroType.Boolean => "boolean",
        AvroType.Int => "int",
        AvroType.Long => "long",
        AvroType.Float => "float",
        AvroType.Double => "double",
        AvroType.Bytes => "bytes",
        AvroType.String => "string",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };
}
