// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg.Serialization;

/// <summary>
/// Serializes and deserializes Iceberg metadata objects to and from JSON using kebab-case naming
/// and Iceberg-specific converters for types, transforms, schemas, and table identifiers.
/// </summary>
public static class IcebergJsonSerializer
{
    private static readonly JsonSerializerOptions Options = CreateOptions();

    public static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = KebabCaseNamingPolicy.Instance,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            // Source-generated context only — no reflection fallback, so the library stays trim/AOT safe.
            TypeInfoResolver = IcebergJsonContext.Default,
        };

        options.Converters.Add(new IcebergTypeConverter());
        options.Converters.Add(new TransformConverter());
        options.Converters.Add(new SchemaConverter());
        options.Converters.Add(new TableIdentifierConverter());

        return options;
    }

    /// <summary>Serializes a value to its Iceberg JSON representation.</summary>
    public static string Serialize<T>(T value) => value switch
    {
        // IcebergType / Transform are polymorphic hierarchies handled by a custom converter.
        // Route them through their (converter-backed) base type info so any concrete subtype
        // serializes correctly without registering each one in the source-generated context.
        IcebergType icebergType => JsonSerializer.Serialize(icebergType, Options.TypeInfo<IcebergType>()),
        Transform transform => JsonSerializer.Serialize(transform, Options.TypeInfo<Transform>()),
        _ => JsonSerializer.Serialize(value, Options.TypeInfo<T>()),
    };

    /// <summary>Deserializes an Iceberg JSON string into the specified type.</summary>
    public static T Deserialize<T>(string json) =>
        JsonSerializer.Deserialize(json, Options.TypeInfo<T>())
        ?? throw new JsonException($"Failed to deserialize {typeof(T).Name}");

    /// <summary>Returns the shared <see cref="JsonSerializerOptions"/> configured for Iceberg serialization.</summary>
    public static JsonSerializerOptions GetOptions() => Options;
}
