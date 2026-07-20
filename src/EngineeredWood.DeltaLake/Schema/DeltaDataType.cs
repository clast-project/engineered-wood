// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Base type for the Delta Lake JSON schema type system.
/// Delta schemas use a nested JSON representation with primitive types
/// as strings and complex types as objects.
/// </summary>
public abstract class DeltaDataType
{
}

/// <summary>
/// A primitive Delta type represented as a string name.
/// Supported names: <c>string</c>, <c>long</c>, <c>integer</c>, <c>short</c>,
/// <c>byte</c>, <c>float</c>, <c>double</c>, <c>boolean</c>, <c>binary</c>,
/// <c>date</c>, <c>timestamp</c>, <c>timestamp_ntz</c>, and
/// <c>decimal(precision,scale)</c>.
/// </summary>
public sealed class PrimitiveType : DeltaDataType
{
    /// <summary>The type name string (e.g., <c>"string"</c>, <c>"decimal(10,2)"</c>).</summary>
    public required string TypeName { get; init; }

    public override string ToString() => TypeName;
}

/// <summary>
/// A struct type containing an ordered list of named fields.
/// </summary>
public sealed class StructType : DeltaDataType
{
    /// <summary>The fields of the struct.</summary>
    public required IReadOnlyList<StructField> Fields { get; init; }

    public override string ToString() => "struct";
}

/// <summary>
/// A single field within a <see cref="StructType"/>.
/// </summary>
public sealed class StructField
{
    /// <summary>Field name.</summary>
    public required string Name { get; init; }

    /// <summary>Field data type.</summary>
    public required DeltaDataType Type { get; init; }

    /// <summary>Whether the field can contain null values.</summary>
    public required bool Nullable { get; init; }

    /// <summary>
    /// Optional metadata (e.g., column mapping ID and physical name).
    /// </summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }
}

/// <summary>
/// An array type containing elements of a single type.
/// </summary>
public sealed class ArrayType : DeltaDataType
{
    /// <summary>The data type of array elements.</summary>
    public required DeltaDataType ElementType { get; init; }

    /// <summary>Whether elements can be null.</summary>
    public required bool ContainsNull { get; init; }

    public override string ToString() => "array";
}

/// <summary>
/// A map type with typed keys and values.
/// </summary>
public sealed class MapType : DeltaDataType
{
    /// <summary>The key data type.</summary>
    public required DeltaDataType KeyType { get; init; }

    /// <summary>The value data type.</summary>
    public required DeltaDataType ValueType { get; init; }

    /// <summary>Whether values can be null.</summary>
    public required bool ValueContainsNull { get; init; }

    public override string ToString() => "map";
}
