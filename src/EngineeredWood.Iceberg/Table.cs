// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Represents a loaded Iceberg table, combining its <see cref="TableIdentifier"/>
/// with the current <see cref="TableMetadata"/>.
/// </summary>
public sealed class Table
{
    /// <summary>The fully-qualified identifier for this table.</summary>
    public TableIdentifier Identifier { get; }

    /// <summary>The current metadata snapshot for this table.</summary>
    public TableMetadata Metadata { get; }

    /// <summary>Initializes a new <see cref="Table"/> with the given identifier and metadata.</summary>
    /// <param name="identifier">The table identifier.</param>
    /// <param name="metadata">The table metadata.</param>
    public Table(TableIdentifier identifier, TableMetadata metadata)
    {
        Identifier = identifier;
        Metadata = metadata;
    }

    /// <summary>The active schema, resolved from <see cref="TableMetadata.CurrentSchemaId"/>.</summary>
    public Schema CurrentSchema =>
        Metadata.Schemas.First(s => s.SchemaId == Metadata.CurrentSchemaId);

    /// <summary>The active partition spec, resolved from <see cref="TableMetadata.DefaultSpecId"/>.</summary>
    public PartitionSpec CurrentSpec =>
        Metadata.PartitionSpecs.First(s => s.SpecId == Metadata.DefaultSpecId);

    /// <summary>The active sort order, resolved from <see cref="TableMetadata.DefaultSortOrderId"/>.</summary>
    public SortOrder CurrentSortOrder =>
        Metadata.SortOrders.FirstOrDefault(s => s.OrderId == Metadata.DefaultSortOrderId)
        ?? SortOrder.Unsorted;

    /// <summary>The root location URI for this table's data and metadata files.</summary>
    public string Location => Metadata.Location;
}
