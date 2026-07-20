// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Base type for all Iceberg data types.
/// </summary>
public abstract record IcebergType
{
    public static readonly PrimitiveType Boolean = new BooleanType();
    public static readonly PrimitiveType Int = new IntegerType();
    public static readonly PrimitiveType Long = new LongType();
    public static readonly PrimitiveType Float = new FloatType();
    public static readonly PrimitiveType Double = new DoubleType();
    public static readonly PrimitiveType Date = new DateType();
    public static readonly PrimitiveType Time = new TimeType();
    public static readonly PrimitiveType Timestamp = new TimestampType();
    public static readonly PrimitiveType Timestamptz = new TimestamptzType();
    public static readonly PrimitiveType TimestampNs = new TimestampNsType();
    public static readonly PrimitiveType TimestamptzNs = new TimestamptzNsType();
    public static readonly PrimitiveType String = new StringType();
    public static readonly PrimitiveType Uuid = new UuidType();
    public static readonly PrimitiveType Binary = new BinaryType();

    // v3 types
    public static readonly PrimitiveType Unknown = new UnknownType();
    public static readonly PrimitiveType Variant = new VariantType();

    /// <summary>Creates a decimal type with the specified precision and scale.</summary>
    public static DecimalType Decimal(int precision, int scale) => new(precision, scale);

    /// <summary>Creates a fixed-length binary type with the specified length.</summary>
    public static FixedType Fixed(int length) => new(length);

    /// <summary>Creates a geometry type with the specified coordinate reference system.</summary>
    public static GeometryType Geometry(string crs) => new(crs);

    /// <summary>Creates a geography type with the specified CRS and edge algorithm.</summary>
    public static GeographyType Geography(string crs, string algorithm) => new(crs, algorithm);
}

/// <summary>
/// Base type for Iceberg primitive (non-nested) data types.
/// </summary>
public abstract record PrimitiveType : IcebergType;

/// <summary>
/// Base type for Iceberg nested (struct, list, map) data types.
/// </summary>
public abstract record NestedType : IcebergType;

// Primitive types

/// <summary>Iceberg boolean primitive type.</summary>
public sealed record BooleanType : PrimitiveType;

/// <summary>Iceberg 32-bit signed integer type.</summary>
public sealed record IntegerType : PrimitiveType;

/// <summary>Iceberg 64-bit signed integer type.</summary>
public sealed record LongType : PrimitiveType;

/// <summary>Iceberg 32-bit IEEE 754 floating point type.</summary>
public sealed record FloatType : PrimitiveType;

/// <summary>Iceberg 64-bit IEEE 754 floating point type.</summary>
public sealed record DoubleType : PrimitiveType;

/// <summary>Iceberg date type (days since 1970-01-01).</summary>
public sealed record DateType : PrimitiveType;

/// <summary>Iceberg time type (microseconds since midnight).</summary>
public sealed record TimeType : PrimitiveType;

/// <summary>Iceberg timestamp type without timezone (microsecond precision).</summary>
public sealed record TimestampType : PrimitiveType;

/// <summary>Iceberg timestamp type with timezone (microsecond precision).</summary>
public sealed record TimestamptzType : PrimitiveType;

/// <summary>Iceberg timestamp type without timezone (nanosecond precision).</summary>
public sealed record TimestampNsType : PrimitiveType;

/// <summary>Iceberg timestamp type with timezone (nanosecond precision).</summary>
public sealed record TimestamptzNsType : PrimitiveType;

/// <summary>Iceberg UTF-8 string type.</summary>
public sealed record StringType : PrimitiveType;

/// <summary>Iceberg UUID type.</summary>
public sealed record UuidType : PrimitiveType;

/// <summary>Iceberg arbitrary-length binary type.</summary>
public sealed record BinaryType : PrimitiveType;

/// <summary>Iceberg fixed-precision decimal type.</summary>
/// <param name="Precision">The number of significant digits.</param>
/// <param name="Scale">The number of digits to the right of the decimal point.</param>
public sealed record DecimalType(int Precision, int Scale) : PrimitiveType;

/// <summary>Iceberg fixed-length binary type.</summary>
/// <param name="Length">The length in bytes.</param>
public sealed record FixedType(int Length) : PrimitiveType;

// v3 primitive types

/// <summary>Iceberg v3 unknown type, used as a placeholder for unresolved types.</summary>
public sealed record UnknownType : PrimitiveType;

/// <summary>Iceberg v3 semi-structured variant type.</summary>
public sealed record VariantType : PrimitiveType;

/// <summary>Iceberg v3 geometry type with a coordinate reference system.</summary>
/// <param name="Crs">The coordinate reference system identifier.</param>
public sealed record GeometryType(string Crs) : PrimitiveType;

/// <summary>Iceberg v3 geography type with a coordinate reference system and edge algorithm.</summary>
/// <param name="Crs">The coordinate reference system identifier.</param>
/// <param name="Algorithm">The edge interpolation algorithm (e.g., spherical or planar).</param>
public sealed record GeographyType(string Crs, string Algorithm) : PrimitiveType;

// Nested types

/// <summary>Iceberg struct type composed of named, typed fields.</summary>
/// <param name="Fields">The ordered list of fields in the struct.</param>
public sealed record StructType(IReadOnlyList<NestedField> Fields) : NestedType;

/// <summary>Iceberg list type containing a single element type.</summary>
/// <param name="ElementId">The field ID assigned to the list element.</param>
/// <param name="ElementType">The data type of list elements.</param>
/// <param name="ElementRequired">Whether list elements are required (non-null).</param>
public sealed record ListType(int ElementId, IcebergType ElementType, bool ElementRequired) : NestedType;

/// <summary>Iceberg map type with typed keys and values.</summary>
/// <param name="KeyId">The field ID assigned to the map key.</param>
/// <param name="KeyType">The data type of map keys.</param>
/// <param name="ValueId">The field ID assigned to the map value.</param>
/// <param name="ValueType">The data type of map values.</param>
/// <param name="ValueRequired">Whether map values are required (non-null).</param>
public sealed record MapType(int KeyId, IcebergType KeyType, int ValueId, IcebergType ValueType, bool ValueRequired) : NestedType;
