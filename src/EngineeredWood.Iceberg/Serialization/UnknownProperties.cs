// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.Iceberg.Serialization;

/// <summary>
/// Unknown-property passthrough for the hand-written converters, matching what
/// <see cref="System.Text.Json.Serialization.JsonExtensionDataAttribute"/> does for the generated ones.
/// </summary>
internal static class UnknownProperties
{
    /// <summary>The properties of <paramref name="obj"/> not in <paramref name="known"/>, or null if none.</summary>
    public static Dictionary<string, JsonElement>? Collect(JsonElement obj, HashSet<string> known)
    {
        Dictionary<string, JsonElement>? extra = null;
        foreach (var property in obj.EnumerateObject())
        {
            if (known.Contains(property.Name))
                continue;
            extra ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            // Clone: the element must outlive the JsonDocument it was read from.
            extra[property.Name] = property.Value.Clone();
        }
        return extra;
    }

    /// <summary>Writes <paramref name="extra"/>, skipping any name the converter writes itself.</summary>
    public static void Write(Utf8JsonWriter writer, IDictionary<string, JsonElement>? extra, HashSet<string> known)
    {
        if (extra is null)
            return;
        foreach (var pair in extra)
        {
            if (known.Contains(pair.Key))
                continue;
            writer.WritePropertyName(pair.Key);
            pair.Value.WriteTo(writer);
        }
    }
}
