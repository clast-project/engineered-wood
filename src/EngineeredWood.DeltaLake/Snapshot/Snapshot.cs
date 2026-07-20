// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Snapshot;

/// <summary>
/// An immutable point-in-time view of a Delta table's state.
/// Contains the reconciled metadata, protocol, and active file set
/// computed by replaying the transaction log.
/// </summary>
public sealed class Snapshot
{
    /// <summary>The log version this snapshot represents.</summary>
    public required long Version { get; init; }

    /// <summary>The active table metadata.</summary>
    public required MetadataAction Metadata { get; init; }

    /// <summary>The active protocol version.</summary>
    public required ProtocolAction Protocol { get; init; }

    /// <summary>The parsed Delta schema from <see cref="Metadata.SchemaString"/>.</summary>
    public required StructType Schema { get; init; }

    /// <summary>The Arrow schema converted from the Delta schema.</summary>
    public required Apache.Arrow.Schema ArrowSchema { get; init; }

    /// <summary>
    /// The set of active (non-removed) data files.
    /// Keyed by <see cref="AddFile.ReconciliationKey"/>.
    /// </summary>
    public required IReadOnlyDictionary<string, AddFile> ActiveFiles { get; init; }

    /// <summary>
    /// Application transaction versions, keyed by app ID.
    /// </summary>
    public required IReadOnlyDictionary<string, TransactionId> AppTransactions { get; init; }

    /// <summary>
    /// Active domain metadata, keyed by domain name.
    /// </summary>
    public required IReadOnlyDictionary<string, DomainMetadata> DomainMetadata { get; init; }

    /// <summary>
    /// Remove tombstones (files removed from the table but not yet expired), keyed by
    /// <see cref="RemoveFile.ReconciliationKey"/>. The spec requires tombstones within the retention
    /// window to be preserved in checkpoints so concurrent readers / VACUUM stay correct.
    /// </summary>
    public IReadOnlyDictionary<string, RemoveFile> Tombstones { get; init; } =
        new Dictionary<string, RemoveFile>();

    /// <summary>
    /// The in-commit timestamp for this snapshot's version, if available.
    /// Milliseconds since epoch. Only present when the table has
    /// <c>delta.enableInCommitTimestamps</c> enabled.
    /// </summary>
    public long? InCommitTimestamp { get; init; }

    /// <summary>
    /// The next available row ID (high water mark).
    /// Computed from <c>max(baseRowId + numRows)</c> across all active files.
    /// Only meaningful when row tracking is enabled.
    /// </summary>
    public long RowIdHighWaterMark { get; init; }

    /// <summary>Total number of active files.</summary>
    public int FileCount => ActiveFiles.Count;
}
