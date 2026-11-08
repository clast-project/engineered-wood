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

    /// <summary>The parsed Delta schema from <see cref="MetadataAction.SchemaString"/>.</summary>
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

    /// <summary>
    /// When a listing of the log last showed this version to be the latest (this machine's clock, taken just
    /// BEFORE the listing, so it errs early); null for a snapshot built at an explicit version, or one whose
    /// freshness is unknown.
    /// </summary>
    /// <remarks>What lets a commit on a snapshot with no <see cref="LogAnchor"/> skip
    /// <see cref="Log.CleanedBaseGuard"/>'s cleanup check (#697): every commit after this
    /// version was written after this moment, so none can be older than the log retention until that long has
    /// passed.</remarks>
    internal DateTimeOffset? ListedLatestAt { get; init; }

    /// <summary>
    /// Every log file that named <see cref="Version"/> in the listing this snapshot was built from (its commit, and
    /// each checkpoint file at that version), or null for a snapshot built in memory with no listing behind it.
    /// </summary>
    /// <remarks>How an update recognises a log replaced under this snapshot (#740): a table dropped and re-created
    /// at the same path can reach this version again, and only different objects at these paths show it.</remarks>
    internal Log.LogVersionAnchor? LogAnchor { get; init; }

    /// <summary>This snapshot, recorded as still the latest at <paramref name="listedAt"/>.</summary>
    internal Snapshot WithListedLatestAt(DateTimeOffset listedAt) => new()
    {
        Version = Version,
        Metadata = Metadata,
        Protocol = Protocol,
        Schema = Schema,
        ArrowSchema = ArrowSchema,
        ActiveFiles = ActiveFiles,
        AppTransactions = AppTransactions,
        DomainMetadata = DomainMetadata,
        Tombstones = Tombstones,
        InCommitTimestamp = InCommitTimestamp,
        RowIdHighWaterMark = RowIdHighWaterMark,
        ListedLatestAt = listedAt,
        LogAnchor = LogAnchor,
    };
}
