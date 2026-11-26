// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// The immutable metadata for an Iceberg table at a specific version, including schemas,
/// partition specs, sort orders, snapshots, and table properties.
/// </summary>
public sealed record TableMetadata : IJsonOnDeserialized, IJsonOnSerializing
{
    /// <summary>The newest table format version this library reads and writes.</summary>
    public const int MaxSupportedFormatVersion = 3;

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
        if (formatVersion is < 1 or > MaxSupportedFormatVersion)
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

    /// <summary>Table-statistics (Puffin) files, or <see langword="null"/> when the metadata lists none.</summary>
    public IReadOnlyList<StatisticsFile>? Statistics { get; init; }

    /// <summary>Partition-statistics files, or <see langword="null"/> when the metadata lists none.</summary>
    public IReadOnlyList<PartitionStatisticsFile>? PartitionStatistics { get; init; }

    /// <summary>
    /// The table's encryption keys (v3), or <see langword="null"/> when it has none. A table with any is
    /// encrypted, and this library refuses to read or write its data.
    /// </summary>
    public IReadOnlyList<EncryptedKey>? EncryptionKeys { get; init; }

    /// <summary>The table's stats-only fields (v3), or <see langword="null"/> when it has none.</summary>
    public IReadOnlyList<StatsField>? StatsFields { get; init; }

    /// <summary>
    /// Top-level properties this library does not model, kept so that rewriting another engine's metadata
    /// does not drop them. <see langword="null"/> when there are none.
    /// </summary>
    [JsonExtensionData, JsonInclude]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; internal set; }

    /// <summary>
    /// Whether the table is encrypted: it has <see cref="EncryptionKeys"/>, or a snapshot names a
    /// <see cref="Snapshot.KeyId"/>.
    /// </summary>
    [JsonIgnore]
    public bool IsEncrypted =>
        EncryptionKeys is { Count: > 0 } || Snapshots.Any(s => s.KeyId is not null);

    /// <summary>
    /// Throws <see cref="UnsupportedTableFeatureException"/> when <paramref name="snapshot"/>'s data (or, with
    /// none, the table's) cannot be read safely by this library: the table, or the snapshot, is encrypted. Reading an encrypted manifest
    /// list as plaintext would misread it.
    /// </summary>
    internal void ThrowIfDataInaccessible(Snapshot? snapshot)
    {
        // Table keys refuse every read, including one of an empty table: there is no plaintext view of it.
        ThrowIfTableKeys("read");
        if (snapshot?.KeyId is { } keyId)
            throw new UnsupportedTableFeatureException(
                $"Snapshot {snapshot.SnapshotId} of table {TableUuid} is encrypted (key-id '{keyId}'); " +
                "table encryption is not supported, so its data cannot be read.");
    }

    /// <summary>
    /// Throws <see cref="UnsupportedTableFeatureException"/> when the table is encrypted in any way: it has
    /// <see cref="EncryptionKeys"/>, or any snapshot has a <see cref="Snapshot.KeyId"/>. Writing plaintext
    /// manifests into an encrypted table would leave its data unprotected, so every write checks this before
    /// it writes anything.
    /// </summary>
    internal void ThrowIfNotWritable()
    {
        ThrowIfTableKeys("written");
        if (Snapshots.FirstOrDefault(s => s.KeyId is not null) is { } keyed)
            throw new UnsupportedTableFeatureException(
                $"Table {TableUuid} is encrypted (snapshot {keyed.SnapshotId} has key-id '{keyed.KeyId}'); " +
                "table encryption is not supported, so the table cannot be written.");
    }

    private void ThrowIfTableKeys(string verb)
    {
        if (EncryptionKeys is { Count: > 0 })
            throw new UnsupportedTableFeatureException(
                $"Table {TableUuid} is encrypted (its metadata lists encryption-keys); " +
                $"table encryption is not supported, so its data cannot be {verb}.");
    }

    /// <summary>
    /// The current snapshot, or <see langword="null"/> when the table has none. Throws
    /// <see cref="InvalidDataException"/> when <see cref="CurrentSnapshotId"/> names a snapshot the metadata
    /// does not hold: reading that as an empty table would silently drop every row.
    /// </summary>
    internal Snapshot? CurrentSnapshotOrNull()
    {
        if (CurrentSnapshotId is not { } id)
            return null;
        return Snapshots.FirstOrDefault(s => s.SnapshotId == id)
            ?? throw new InvalidDataException(
                $"Table {TableUuid}'s current-snapshot-id {id} names no snapshot in its metadata.");
    }

    void IJsonOnDeserialized.OnDeserialized() => ThrowIfUnsupportedVersion("read");

    // Writing is checked too: metadata built with `with { FormatVersion = 4 }` would otherwise be written as a
    // table this library (and any v3 reader) then refuses, while the files it describes follow v3 rules.
    void IJsonOnSerializing.OnSerializing() => ThrowIfUnsupportedVersion("written");

    private void ThrowIfUnsupportedVersion(string verb)
    {
        // "Implementations must throw an exception if a table's version is higher than the supported
        // version." Opening a newer table anyway would misread it, and a commit would corrupt it.
        if (FormatVersion is < 1 or > MaxSupportedFormatVersion)
            throw new UnsupportedTableFeatureException(
                $"Table format-version {FormatVersion} is not supported, so it cannot be {verb}; this library " +
                $"supports format versions 1 to {MaxSupportedFormatVersion}.");
    }
}
