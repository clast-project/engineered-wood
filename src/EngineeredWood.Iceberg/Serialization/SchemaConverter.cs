// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg.Serialization;

internal sealed class SchemaConverter : JsonConverter<Schema>
{
    public override Schema Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;

        var schemaId = root.GetProperty("schema-id").GetInt32();
        var fields = root.GetProperty("fields").Deserialize(options.TypeInfo<List<NestedField>>())!;

        IReadOnlyList<int>? identifierFieldIds = null;
        if (root.TryGetProperty("identifier-field-ids", out var idsElement))
            identifierFieldIds = idsElement.Deserialize(options.TypeInfo<List<int>>());

        return new Schema(schemaId, fields, identifierFieldIds);
    }

    public override void Write(Utf8JsonWriter writer, Schema value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteNumber("schema-id", value.SchemaId);
        writer.WriteString("type", "struct");
        writer.WritePropertyName("fields");
        JsonSerializer.Serialize(writer, value.Fields, options.TypeInfo<IReadOnlyList<NestedField>>());

        if (value.IdentifierFieldIds is not null)
        {
            writer.WritePropertyName("identifier-field-ids");
            JsonSerializer.Serialize(writer, value.IdentifierFieldIds, options.TypeInfo<IReadOnlyList<int>>());
        }

        writer.WriteEndObject();
    }
}
