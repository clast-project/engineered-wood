// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// The immutable metadata for an Iceberg table at a specific version, including schemas,
/// partition specs, sort orders, snapshots, and table properties.
/// </summary>
public sealed record TableMetadata
{
    /// <summary>Creates a new <see cref="TableMetadata"/> for an initial table with the given schema and optional configuration.</summary>
    /// <param name="schema">The initial table schema.</param>
    /// <param name="partitionSpec">The partition spec, or <see langword="null"/> for unpartitioned.</param>
    /// <param name="sortOrder">The sort order, or <see langword="null"/> for unsorted.</param>
    /// <param name="location">The table root location URI.</param>
    /// <param name="properties">Optional table properties.</param>
    /// <param name="formatVersion">The Iceberg format version (1, 2, or 3).</param>
    /// <returns>A new <see cref="TableMetadata"/> instance.</returns>
    public static TableMetadata Create(
        Schema schema,
        PartitionSpec? partitionSpec = null,
        SortOrder? sortOrder = null,
        string? location = null,
        IReadOnlyDictionary<string, string>? properties = null,
        int formatVersion = 2)
    {
        if (formatVersion is < 1 or > 3)
            throw new ArgumentOutOfRangeException(nameof(formatVersion), "Format version must be 1, 2, or 3");

        var spec = partitionSpec ?? PartitionSpec.Unpartitioned;
        var order = sortOrder ?? SortOrder.Unsorted;

        return new TableMetadata
        {
            FormatVersion = formatVersion,
            TableUuid = Guid.NewGuid().ToString(),
            Location = location ?? "",
            LastSequenceNumber = 0,
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            LastColumnId = ComputeMaxFieldId(schema),
            CurrentSchemaId = schema.SchemaId,
            Schemas = [schema],
            DefaultSpecId = spec.SpecId,
            PartitionSpecs = [spec],
            LastPartitionId = spec.Fields.Count > 0
                ? spec.Fields.Max(f => f.FieldId)
                : 0,
            DefaultSortOrderId = order.OrderId,
            SortOrders = [order],
            Properties = properties ?? new Dictionary<string, string>(),
            // v3 row lineage: start assigning row IDs from 0
            NextRowId = formatVersion >= 3 ? 0 : null,
        };
    }

    private static int ComputeMaxFieldId(Schema schema)
    {
        if (schema.Fields.Count == 0)
            return 0;

        return schema.Fields.Max(f => MaxFieldIdInType(f.Id, f.Type));
    }

    private static int MaxFieldIdInType(int fieldId, IcebergType type)
    {
        return type switch
        {
            StructType s => s.Fields.Count > 0
                ? s.Fields.Max(f => MaxFieldIdInType(f.Id, f.Type))
                : fieldId,
            ListType l => Math.Max(l.ElementId, MaxFieldIdInType(l.ElementId, l.ElementType)),
            MapType m => new[]
            {
                m.KeyId, m.ValueId,
                MaxFieldIdInType(m.KeyId, m.KeyType),
                MaxFieldIdInType(m.ValueId, m.ValueType)
            }.Max(),
            _ => fieldId,
        };
    }

    /// <summary>The Iceberg format version (1, 2, or 3).</summary>
    public required int FormatVersion { get; init; }

    /// <summary>A UUID that uniquely identifies this table.</summary>
    public required string TableUuid { get; init; }

    /// <summary>The root location URI for this table's data and metadata files.</summary>
    public required string Location { get; init; }

    /// <summary>The highest sequence number assigned to any snapshot in this table.</summary>
    public long LastSequenceNumber { get; init; }

    /// <summary>Timestamp in milliseconds from epoch when this metadata was last updated.</summary>
    public required long LastUpdatedMs { get; init; }

    /// <summary>The highest field ID assigned across all schemas in this table.</summary>
    public required int LastColumnId { get; init; }

    /// <summary>The schema ID of the current active schema.</summary>
    public required int CurrentSchemaId { get; init; }

    /// <summary>All schema versions that have been part of this table.</summary>
    public required IReadOnlyList<Schema> Schemas { get; init; }

    /// <summary>The spec ID of the default partition spec for new data.</summary>
    public required int DefaultSpecId { get; init; }

    /// <summary>All partition specs that have been part of this table.</summary>
    public required IReadOnlyList<PartitionSpec> PartitionSpecs { get; init; }

    /// <summary>The highest partition field ID assigned across all partition specs.</summary>
    public required int LastPartitionId { get; init; }

    /// <summary>The order ID of the default sort order for new data.</summary>
    public int DefaultSortOrderId { get; init; }

    /// <summary>All sort orders that have been part of this table.</summary>
    public IReadOnlyList<SortOrder> SortOrders { get; init; } = [];

    /// <summary>User-defined key-value properties for this table.</summary>
    public IReadOnlyDictionary<string, string> Properties { get; init; } = new Dictionary<string, string>();

    /// <summary>The snapshot ID of the current table state, or <see langword="null"/> if no snapshots exist.</summary>
    public long? CurrentSnapshotId { get; init; }

    /// <summary>All snapshots in this table's history.</summary>
    public IReadOnlyList<Snapshot> Snapshots { get; init; } = [];

    /// <summary>A log of snapshot changes, recording when each snapshot became current.</summary>
    public IReadOnlyList<SnapshotLogEntry> SnapshotLog { get; init; } = [];

    /// <summary>A log of metadata file locations, recording each metadata version change.</summary>
    public IReadOnlyList<MetadataLogEntry> MetadataLog { get; init; } = [];

    /// <summary>
    /// Named snapshot references (branches and tags). The "main" branch
    /// tracks the current snapshot and is always present after the first commit.
    /// </summary>
    public IReadOnlyDictionary<string, SnapshotRef> Refs { get; init; } = new Dictionary<string, SnapshotRef>();

    /// <summary>The next row ID to assign for row-level lineage tracking (v3 only).</summary>
    public long? NextRowId { get; init; }
}
