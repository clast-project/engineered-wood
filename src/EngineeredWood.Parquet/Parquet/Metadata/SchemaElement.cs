// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// A single element from the flattened Parquet schema.
/// Leaf elements have a physical type; group elements have num_children > 0.
/// </summary>
public sealed class SchemaElement
{
    /// <summary>Name of this schema element.</summary>
    public required string Name { get; init; }

    /// <summary>Physical (primitive) type. Null for group nodes.</summary>
    public PhysicalType? Type { get; init; }

    /// <summary>Fixed length for FIXED_LEN_BYTE_ARRAY type.</summary>
    public int? TypeLength { get; init; }

    /// <summary>Repetition of this field (required, optional, repeated). Null for the root.</summary>
    public FieldRepetitionType? RepetitionType { get; init; }

    /// <summary>Number of children (for group nodes). Null or 0 for leaf nodes.</summary>
    public int? NumChildren { get; init; }

    /// <summary>Deprecated converted type.</summary>
    public ConvertedType? ConvertedType { get; init; }

    /// <summary>Decimal scale (for DECIMAL converted type).</summary>
    public int? Scale { get; init; }

    /// <summary>Decimal precision (for DECIMAL converted type).</summary>
    public int? Precision { get; init; }

    /// <summary>Column ordering identifier.</summary>
    public int? FieldId { get; init; }

    /// <summary>Logical type annotation (preferred over ConvertedType).</summary>
    public LogicalType? LogicalType { get; init; }
}
