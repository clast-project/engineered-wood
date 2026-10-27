// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Schema;

/// <summary>
/// Describes a leaf column in the schema with its path, type, and definition/repetition levels.
/// </summary>
public sealed class ColumnDescriptor
{
    /// <summary>Dot-separated path from root to this leaf column.</summary>
    public required IReadOnlyList<string> Path { get; init; }

    /// <summary>Physical type of this column.</summary>
    public required PhysicalType PhysicalType { get; init; }

    /// <summary>Fixed byte length for FIXED_LEN_BYTE_ARRAY type.</summary>
    public int? TypeLength { get; init; }

    /// <summary>Maximum definition level for this column.</summary>
    public required int MaxDefinitionLevel { get; init; }

    /// <summary>Maximum repetition level for this column.</summary>
    public required int MaxRepetitionLevel { get; init; }

    /// <summary>The schema element for this leaf column.</summary>
    public required SchemaElement SchemaElement { get; init; }

    /// <summary>The corresponding tree node.</summary>
    public required SchemaNode SchemaNode { get; init; }

    /// <summary>The column path as a dot-separated string.</summary>
    public string DottedPath => string.Join(".", Path);

    /// <summary>The Parquet field_id for this column, if set.</summary>
    public int? FieldId => SchemaElement.FieldId;

    /// <summary>
    /// The order this column's min/max statistics and page-index bounds are in, as the reader honours
    /// it: the file's <c>column_orders</c> entry, or <see cref="ColumnOrder.TypeDefined"/> when the
    /// file has none. <see cref="ColumnOrder.Undefined"/> — an order this reader does not recognize,
    /// one the spec does not allow on this column's type, or a column whose type annotation was
    /// ignored — means the bounds are not used.
    /// </summary>
    public ColumnOrder ColumnOrder { get; internal init; } = ColumnOrder.TypeDefined;

    internal ColumnDescriptor WithColumnOrder(ColumnOrder order) => new()
    {
        Path = Path,
        PhysicalType = PhysicalType,
        TypeLength = TypeLength,
        MaxDefinitionLevel = MaxDefinitionLevel,
        MaxRepetitionLevel = MaxRepetitionLevel,
        SchemaElement = SchemaElement,
        SchemaNode = SchemaNode,
        ColumnOrder = order,
    };
}
