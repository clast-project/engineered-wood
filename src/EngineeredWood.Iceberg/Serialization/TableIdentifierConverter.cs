// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg.Serialization;

internal sealed class TableIdentifierConverter : JsonConverter<TableIdentifier>
{
    public override TableIdentifier Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        var ns = root.GetProperty("namespace").Deserialize(options.TypeInfo<Namespace>())!;
        var name = root.GetProperty("name").GetString()!;
        return new TableIdentifier(ns, name);
    }

    public override void Write(Utf8JsonWriter writer, TableIdentifier value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WritePropertyName("namespace");
        JsonSerializer.Serialize(writer, value.Namespace, options.TypeInfo<Namespace>());
        writer.WriteString("name", value.Name);
        writer.WriteEndObject();
    }
}
