// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Top-level file metadata from the Parquet footer.
/// </summary>
public sealed class FileMetaData
{
    /// <summary>Parquet format version.</summary>
    public required int Version { get; init; }

    /// <summary>Flattened schema elements (pre-order traversal).</summary>
    public required IReadOnlyList<SchemaElement> Schema { get; init; }

    /// <summary>Total number of rows across all row groups.</summary>
    public required long NumRows { get; init; }

    /// <summary>Row groups in this file.</summary>
    public required IReadOnlyList<RowGroup> RowGroups { get; init; }

    /// <summary>Key-value metadata set by the writer.</summary>
    public IReadOnlyList<KeyValue>? KeyValueMetadata { get; init; }

    /// <summary>Application that created this file.</summary>
    public string? CreatedBy { get; init; }

    /// <summary>
    /// Sort order for each leaf column's min/max statistics (footer field 7),
    /// one entry per column in row-group column order. <see langword="null"/>
    /// when the file omitted the field, in which case readers assume
    /// <see cref="ColumnOrder.TypeDefined"/>.
    /// </summary>
    public IReadOnlyList<ColumnOrder>? ColumnOrders { get; init; }
}
