// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// A named, typed column in an Iceberg schema. Each field has a unique <paramref name="Id"/>
/// that is stable across schema evolution.
/// </summary>
/// <param name="Id">The unique field identifier, stable across schema evolution.</param>
/// <param name="Name">The field name.</param>
/// <param name="Type">The Iceberg data type of this field.</param>
/// <param name="IsRequired">Whether the field is required (non-nullable).</param>
/// <param name="Doc">Optional documentation string.</param>
/// <param name="InitialDefault">Optional initial default value as a JSON element.</param>
/// <param name="WriteDefault">Optional write default value as a JSON element.</param>
public sealed record NestedField(
    int Id,
    string Name,
    IcebergType Type,
    [property: JsonPropertyName("required")] bool IsRequired,
    string? Doc = null,
    [property: JsonPropertyName("initial-default")] JsonElement? InitialDefault = null,
    [property: JsonPropertyName("write-default")] JsonElement? WriteDefault = null);
