// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

// The FIELD InCommitTimestamp below shadows the type of the same name for the whole of this file, so the
// helper has to be reached under another one.
using IctFeature = EngineeredWood.DeltaLake.Log.InCommitTimestamp;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// The contents of a <c>_delta_log/&lt;version&gt;.crc</c> version-checksum file: a summary of the table
/// state at one version, written beside the commit that produced it.
///
/// <para>The file exists so a reader can learn a version's size and file count — and its metadata,
/// protocol, live app transactions and live domain metadata — without replaying the log or opening a
/// checkpoint, and so a writer computing table state incrementally can compare its own view against a
/// recorded one and notice the two have drifted. PROTOCOL.md makes it optional and requires every reader
/// to cope with its absence, so nothing breaks where it is missing; what it buys is the shortcut and the
/// cross-check.</para>
///
/// <para><b>A wrong checksum is worse than an absent one</b> — a reader that validates would reject a
/// good table — so this type is only ever built from a <see cref="Snapshot.Snapshot"/>, which is the
/// reconciled state at exactly one version, and <see cref="TryFromSnapshot"/> declines rather than
/// approximates when the snapshot cannot describe its version faithfully.</para>
/// </summary>
/// <remarks>
/// <para><b>Fields this library does not write.</b> The spec defines five more optional fields, every one
/// of them omitted here and by delta-kernel-rs alike: <c>txnId</c> (this library attaches no transaction
/// identifier to a commit), <c>allFiles</c> (the entire live file set inline — that is a checkpoint, and
/// writing one per commit would be the wrong shape), <c>numDeletedRecordsOpt</c> /
/// <c>numDeletionVectorsOpt</c>, and <c>deletedRecordCountsHistogramOpt</c>. <c>fileSizeHistogram</c> is
/// omitted too; see <see cref="VersionChecksumWriter"/> for that one, which is the only omission with a
/// live consumer.</para>
/// <para><c>numMetadata</c> and <c>numProtocol</c> are not modelled: the spec fixes both at 1, so they
/// are written as constants, and on read a file is rejected unless it carries both AND both are 1.</para>
/// </remarks>
public sealed record VersionChecksum
{
    /// <summary>
    /// The version this checksum describes. Carried by the FILE NAME rather than the body — the on-disk
    /// JSON has no version field — so a checksum read back takes it from the path it was read from.
    /// </summary>
    public required long Version { get; init; }

    /// <summary>
    /// Total size of the table in bytes: the sum of <see cref="AddFile.Size"/> over every live
    /// <c>add</c> at this version.
    /// </summary>
    public required long TableSizeBytes { get; init; }

    /// <summary>Number of live <c>add</c> actions at this version, after action reconciliation.</summary>
    public required long NumFiles { get; init; }

    /// <summary>The table metadata at this version.</summary>
    public required MetadataAction Metadata { get; init; }

    /// <summary>The table protocol at this version.</summary>
    public required ProtocolAction Protocol { get; init; }

    /// <summary>
    /// The in-commit timestamp of this version, in milliseconds since the epoch. Present if and ONLY if
    /// <c>delta.enableInCommitTimestamps</c> is enabled — the spec ties the field to the feature rather
    /// than making it independently optional, so one without the other is malformed in both directions.
    /// </summary>
    public long? InCommitTimestamp { get; init; }

    /// <summary>
    /// Live <c>txn</c> (transaction identifier) actions at this version, or null when the writer did not
    /// record them.
    /// </summary>
    /// <remarks>
    /// Null and empty are DIFFERENT here, which is why this is a nullable list rather than a possibly
    /// empty one: an absent array means "this writer is not telling you", while a present empty array is
    /// the authoritative statement that the table has no live app transactions — the difference between a
    /// reader having to replay the log on a miss and being able to trust the miss. This library always
    /// records them, because a <see cref="Snapshot.Snapshot"/> holds the complete set.
    /// </remarks>
    public IReadOnlyList<TransactionId>? SetTransactions { get; init; }

    /// <summary>
    /// Live <c>domainMetadata</c> actions at this version, tombstones excluded, or null when the writer
    /// did not record them. Absent and empty differ for the same reason as
    /// <see cref="SetTransactions"/>.
    /// </summary>
    public IReadOnlyList<DomainMetadata>? DomainMetadata { get; init; }

    /// <summary>
    /// Describes <paramref name="snapshot"/>'s version, or returns null when it cannot be described
    /// faithfully.
    /// </summary>
    /// <remarks>
    /// <para>There is exactly one way to fail, and it is worth spelling out: the table has in-commit
    /// timestamps enabled and the snapshot carries no timestamp for its version. That happens when the
    /// snapshot was built from a CHECKPOINT, which holds no <c>commitInfo</c> and therefore no in-commit
    /// timestamp. The field is required-if-enabled, so the choice is between a checksum missing a
    /// mandatory field and no checksum at all, and the second is the safe one. delta-kernel-rs refuses
    /// the same case (<c>ChecksumWriteUnsupported</c>); it raises an error where this returns null,
    /// because this is reached from a post-commit path where the commit is already durable and throwing
    /// would report a failure that did not happen.</para>
    ///
    /// <para>Everything else is unconditional: a snapshot's live file set, metadata, protocol, app
    /// transactions and domain metadata ARE the reconciled state at its version, so the counts cannot be
    /// stale the way an incrementally maintained tally can. delta-kernel-rs and delta-spark both carry a
    /// "degraded" / "incremental is unsafe for this operation" state for exactly that reason; computing
    /// from the whole snapshot costs a pass over the live files and removes the concept.</para>
    /// </remarks>
    public static VersionChecksum? TryFromSnapshot(Snapshot.Snapshot snapshot)
    {
        if (snapshot is null)
            throw new ArgumentNullException(nameof(snapshot));

        bool ictEnabled = IctFeature.IsEnabled(snapshot.Metadata.Configuration);
        if (ictEnabled && snapshot.InCommitTimestamp is null)
            return null;

        long tableSizeBytes = 0;
        foreach (var add in snapshot.ActiveFiles.Values)
            tableSizeBytes += add.Size;

        // Sorted so the bytes are a function of the state and nothing else: both collections come from
        // hash maps, whose enumeration order is not part of the state being described. A checksum that
        // reorders between two writers describing the same version reads as a difference where there is
        // none — to a diff, to a byte comparison in a test, and to anyone eyeballing two log directories.
        var setTransactions = snapshot.AppTransactions.Values
            .OrderBy(static txn => txn.AppId, StringComparer.Ordinal)
            .ToArray();
        var domainMetadata = snapshot.DomainMetadata.Values
            .OrderBy(static dm => dm.Domain, StringComparer.Ordinal)
            .ToArray();

        return new VersionChecksum
        {
            Version = snapshot.Version,
            TableSizeBytes = tableSizeBytes,
            NumFiles = snapshot.ActiveFiles.Count,
            Metadata = snapshot.Metadata,
            Protocol = snapshot.Protocol,
            // Written only when the feature is on. A table without it may still carry a snapshot
            // timestamp read off some earlier commitInfo, and emitting that would claim a feature the
            // protocol does not declare.
            InCommitTimestamp = ictEnabled ? snapshot.InCommitTimestamp : null,
            SetTransactions = setTransactions,
            DomainMetadata = domainMetadata,
        };
    }
}
