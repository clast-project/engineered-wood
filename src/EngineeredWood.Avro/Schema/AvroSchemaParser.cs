// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.Avro.Schema;

/// <summary>
/// Parses Avro JSON schemas into an <see cref="AvroSchemaNode"/> tree.
/// Handles named type references and nested type definitions.
/// </summary>
internal static class AvroSchemaParser
{
    public static AvroSchemaNode Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var namedTypes = new Dictionary<string, AvroSchemaNode>();
        return ParseElement(doc.RootElement, namedTypes, enclosingNamespace: null);
    }

    internal static AvroSchemaNode ParseElement(
        JsonElement element, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                return ParseTypeName(element.GetString()!, namedTypes, enclosingNamespace);

            case JsonValueKind.Array:
                return ParseUnion(element, namedTypes, enclosingNamespace);

            case JsonValueKind.Object:
                return ParseComplex(element, namedTypes, enclosingNamespace);

            default:
                throw new InvalidOperationException($"Unexpected JSON element kind: {element.ValueKind}");
        }
    }

    private static AvroSchemaNode ParseTypeName(
        string name, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        return name switch
        {
            "null" => AvroPrimitiveSchema.Null,
            "boolean" => AvroPrimitiveSchema.Boolean,
            "int" => AvroPrimitiveSchema.Int,
            "long" => AvroPrimitiveSchema.Long,
            "float" => AvroPrimitiveSchema.Float,
            "double" => AvroPrimitiveSchema.Double,
            "bytes" => AvroPrimitiveSchema.Bytes,
            "string" => AvroPrimitiveSchema.String,
            _ => ResolveNamedType(name, namedTypes, enclosingNamespace),
        };
    }

    private static AvroSchemaNode ResolveNamedType(
        string name, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        // Try exact name first
        if (namedTypes.TryGetValue(name, out var schema))
            return schema;

        // Try with enclosing namespace
        if (enclosingNamespace != null)
        {
            var fullName = $"{enclosingNamespace}.{name}";
            if (namedTypes.TryGetValue(fullName, out schema))
                return schema;
        }

        throw new InvalidOperationException($"Unknown type: '{name}'");
    }

    private static AvroUnionSchema ParseUnion(
        JsonElement array, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        var branches = new List<AvroSchemaNode>();
        foreach (var item in array.EnumerateArray())
            branches.Add(ParseElement(item, namedTypes, enclosingNamespace));
        return new AvroUnionSchema(branches);
    }

    private static AvroSchemaNode ParseComplex(
        JsonElement obj, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        var typeName = obj.GetProperty("type").GetString()!;

        // Check for primitive type with logical type annotation
        if (IsPrimitiveName(typeName) && obj.TryGetProperty("logicalType", out var logicalEl))
        {
            var logicalType = logicalEl.GetString();
            var baseSchema = ParseTypeName(typeName, namedTypes, enclosingNamespace);
            return new AvroPrimitiveSchema(baseSchema.Type)
            {
                LogicalType = logicalType,
                Precision = logicalType == "decimal" && obj.TryGetProperty("precision", out var precEl)
                    ? precEl.GetInt32() : null,
                Scale = logicalType == "decimal" && obj.TryGetProperty("scale", out var scaleEl)
                    ? scaleEl.GetInt32() : null,
            };
        }

        // Check for primitive type as object (e.g. {"type": "string"})
        if (IsPrimitiveName(typeName) && !obj.TryGetProperty("logicalType", out _))
        {
            return ParseTypeName(typeName, namedTypes, enclosingNamespace);
        }

        return typeName switch
        {
            "record" => ParseRecord(obj, namedTypes, enclosingNamespace),
            "enum" => ParseEnum(obj, namedTypes, enclosingNamespace),
            "array" => ParseArray(obj, namedTypes, enclosingNamespace),
            "map" => ParseMap(obj, namedTypes, enclosingNamespace),
            "fixed" => ParseFixed(obj, namedTypes, enclosingNamespace),
            _ => throw new InvalidOperationException($"Unknown complex type: '{typeName}'"),
        };
    }

    private static bool IsPrimitiveName(string name) => name is
        "null" or "boolean" or "int" or "long" or "float" or "double" or "bytes" or "string";

    private static AvroRecordSchema ParseRecord(
        JsonElement obj, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        var name = obj.GetProperty("name").GetString()!;
        var ns = obj.TryGetProperty("namespace", out var nsEl) ? nsEl.GetString() : enclosingNamespace;
        var fullName = ns != null ? $"{ns}.{name}" : name;
        var effectiveNamespace = ns;

        // Register a placeholder for self-referencing schemas
        var placeholder = new AvroRecordSchema(name, ns, []);
        namedTypes[fullName] = placeholder;
        namedTypes[name] = placeholder;

        var fields = new List<AvroFieldNode>();
        foreach (var fieldEl in obj.GetProperty("fields").EnumerateArray())
        {
            var fieldName = fieldEl.GetProperty("name").GetString()!;
            var fieldSchema = ParseElement(fieldEl.GetProperty("type"), namedTypes, effectiveNamespace);

            var field = new AvroFieldNode(fieldName, fieldSchema)
            {
                Doc = fieldEl.TryGetProperty("doc", out var docEl) ? docEl.GetString() : null,
                Default = fieldEl.TryGetProperty("default", out var defEl) ? defEl.Clone() : null,
                Aliases = ParseStringArray(fieldEl, "aliases"),
            };
            fields.Add(field);
        }

        var record = new AvroRecordSchema(name, ns, fields)
        {
            Doc = obj.TryGetProperty("doc", out var rdocEl) ? rdocEl.GetString() : null,
            Aliases = ParseStringArray(obj, "aliases"),
        };

        // Replace placeholder with real record
        namedTypes[fullName] = record;
        namedTypes[name] = record;

        return record;
    }

    private static AvroEnumSchema ParseEnum(
        JsonElement obj, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        var name = obj.GetProperty("name").GetString()!;
        var ns = obj.TryGetProperty("namespace", out var nsEl) ? nsEl.GetString() : enclosingNamespace;
        var fullName = ns != null ? $"{ns}.{name}" : name;

        var symbols = new List<string>();
        foreach (var sym in obj.GetProperty("symbols").EnumerateArray())
            symbols.Add(sym.GetString()!);

        var schema = new AvroEnumSchema(name, ns, symbols)
        {
            Default = obj.TryGetProperty("default", out var defEl) ? defEl.GetString() : null,
            Doc = obj.TryGetProperty("doc", out var docEl) ? docEl.GetString() : null,
            Aliases = ParseStringArray(obj, "aliases"),
        };

        namedTypes[fullName] = schema;
        namedTypes[name] = schema;
        return schema;
    }

    private static AvroArraySchema ParseArray(
        JsonElement obj, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        var items = ParseElement(obj.GetProperty("items"), namedTypes, enclosingNamespace);
        return new AvroArraySchema(items);
    }

    private static AvroMapSchema ParseMap(
        JsonElement obj, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        var values = ParseElement(obj.GetProperty("values"), namedTypes, enclosingNamespace);
        return new AvroMapSchema(values);
    }

    private static AvroFixedSchema ParseFixed(
        JsonElement obj, Dictionary<string, AvroSchemaNode> namedTypes, string? enclosingNamespace)
    {
        var name = obj.GetProperty("name").GetString()!;
        var ns = obj.TryGetProperty("namespace", out var nsEl) ? nsEl.GetString() : enclosingNamespace;
        var fullName = ns != null ? $"{ns}.{name}" : name;
        var size = obj.GetProperty("size").GetInt32();

        var logicalType = obj.TryGetProperty("logicalType", out var ltEl) ? ltEl.GetString() : null;
        var schema = new AvroFixedSchema(name, ns, size)
        {
            Aliases = ParseStringArray(obj, "aliases"),
            LogicalType = logicalType,
            Precision = logicalType == "decimal" && obj.TryGetProperty("precision", out var precEl)
                ? precEl.GetInt32() : null,
            Scale = logicalType == "decimal" && obj.TryGetProperty("scale", out var scaleEl)
                ? scaleEl.GetInt32() : null,
        };

        namedTypes[fullName] = schema;
        namedTypes[name] = schema;
        return schema;
    }

    private static IReadOnlyList<string> ParseStringArray(JsonElement obj, string property)
    {
        if (!obj.TryGetProperty(property, out var arr))
            return [];
        var result = new List<string>();
        foreach (var item in arr.EnumerateArray())
            result.Add(item.GetString()!);
        return result;
    }
}
