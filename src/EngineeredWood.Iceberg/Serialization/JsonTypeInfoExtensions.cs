// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace EngineeredWood.Iceberg.Serialization;

internal static class JsonTypeInfoExtensions
{
    /// <summary>
    /// Resolves the strongly-typed <see cref="JsonTypeInfo{T}"/> for <typeparamref name="T"/> from
    /// the given options' (source-generated) resolver. Unlike the reflection-based
    /// <c>JsonSerializer</c> overloads, <see cref="JsonSerializerOptions.GetTypeInfo(Type)"/> carries
    /// no <c>RequiresUnreferencedCode</c>/<c>RequiresDynamicCode</c> annotation, so callers stay
    /// trim/AOT safe. Throws at runtime if <typeparamref name="T"/> is not registered in the context.
    /// </summary>
    public static JsonTypeInfo<T> TypeInfo<T>(this JsonSerializerOptions options) =>
        (JsonTypeInfo<T>)options.GetTypeInfo(typeof(T));
}
