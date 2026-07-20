// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json.Serialization;
using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Source-generated JSON context for the small set of POCO types that Delta action
/// (de)serialization routes through <see cref="System.Text.Json.JsonSerializer"/>.
/// Keeps the serializer free of reflection-based metadata so the library is trim/AOT safe.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(DeletionVector))]
internal partial class DeltaJsonContext : JsonSerializerContext;
