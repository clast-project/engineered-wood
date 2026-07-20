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
}
