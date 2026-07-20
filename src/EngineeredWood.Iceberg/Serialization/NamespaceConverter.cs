// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg.Serialization;

internal sealed class NamespaceConverter : JsonConverter<Namespace>
{
    public override Namespace Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartArray)
            throw new JsonException($"Expected array for Namespace, got {reader.TokenType}");

        var levels = new List<string>();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                break;

            levels.Add(reader.GetString()!);
        }

        return new Namespace(levels);
    }

    public override void Write(Utf8JsonWriter writer, Namespace value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();

        foreach (var level in value.Levels)
            writer.WriteStringValue(level);

        writer.WriteEndArray();
    }
}
