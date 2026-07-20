// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Represents an immutable snapshot of a table at a point in time.
/// Each snapshot references a manifest list that describes the data files belonging to the table.
/// </summary>
/// <param name="SnapshotId">Unique identifier for this snapshot.</param>
/// <param name="ParentSnapshotId">Identifier of the parent snapshot, or null for the initial snapshot.</param>
/// <param name="SequenceNumber">Monotonically increasing sequence number assigned when the snapshot was created.</param>
/// <param name="TimestampMs">Timestamp in milliseconds from epoch when the snapshot was created.</param>
/// <param name="ManifestList">Path to the manifest list file for this snapshot.</param>
/// <param name="Summary">Key-value summary of the snapshot operation (e.g. operation type, record counts).</param>
/// <param name="SchemaId">Identifier of the table schema at the time the snapshot was created.</param>
/// <param name="FirstRowId">First row ID assigned to rows added in this snapshot (v3).</param>
/// <param name="AddedRows">Number of rows added in this snapshot (v3).</param>
public sealed record Snapshot(
    long SnapshotId,
    long? ParentSnapshotId,
    long SequenceNumber,
    long TimestampMs,
    string ManifestList,
    IReadOnlyDictionary<string, string> Summary,
    int? SchemaId = null,
    // v3 row lineage
    long? FirstRowId = null,
    long? AddedRows = null);

/// <summary>
/// Entry in the snapshot log recording when a snapshot became the current snapshot.
/// </summary>
public sealed record SnapshotLogEntry(long TimestampMs, long SnapshotId);

/// <summary>
/// Entry in the metadata log recording when a metadata file was written.
/// </summary>
public sealed record MetadataLogEntry(long TimestampMs, string MetadataFile);
