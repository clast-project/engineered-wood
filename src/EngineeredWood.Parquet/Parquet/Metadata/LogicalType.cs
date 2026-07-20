// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Time unit precision for logical time/timestamp types.
/// </summary>
public enum TimeUnit
{
    Millis,
    Micros,
    Nanos,
}

/// <summary>
/// Logical types provide richer semantics on top of physical types.
/// Modeled as an abstract record with sealed variants.
/// </summary>
public abstract record LogicalType
{
    private LogicalType() { }

    public sealed record StringType : LogicalType;
    public sealed record MapType : LogicalType;
    public sealed record ListType : LogicalType;
    public sealed record EnumType : LogicalType;
    public sealed record DateType : LogicalType;
    public sealed record JsonType : LogicalType;
    public sealed record BsonType : LogicalType;
    public sealed record UuidType : LogicalType;
    public sealed record Float16Type : LogicalType;
    public sealed record VariantType : LogicalType;

    public sealed record DecimalType(int Scale, int Precision) : LogicalType;
    public sealed record TimeType(bool IsAdjustedToUtc, TimeUnit Unit) : LogicalType;
    public sealed record TimestampType(bool IsAdjustedToUtc, TimeUnit Unit) : LogicalType;
    public sealed record IntType(int BitWidth, bool IsSigned) : LogicalType;

    /// <summary>Forward-compatibility: an unrecognized logical type.</summary>
    public sealed record UnknownLogicalType(short ThriftFieldId) : LogicalType;
}
