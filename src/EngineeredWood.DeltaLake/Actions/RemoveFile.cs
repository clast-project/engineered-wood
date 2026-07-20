// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Removes a data file from the table. The removed file is retained as a
/// tombstone until it is safe to physically delete (vacuum).
/// </summary>
public sealed record RemoveFile : DeltaAction
{
    /// <summary>URI-encoded relative or absolute path to the data file.</summary>
    public required string Path { get; init; }

    /// <summary>Timestamp of the removal in milliseconds since epoch.</summary>
    public long? DeletionTimestamp { get; init; }

    /// <summary>
    /// Whether the removal represents a logical change to the table data
    /// (as opposed to a file rearrangement like compaction).
    /// </summary>
    public required bool DataChange { get; init; }

    /// <summary>
    /// Whether this remove action contains extended file metadata
    /// (partition values, size, tags) from the corresponding add action.
    /// </summary>
    public bool? ExtendedFileMetadata { get; init; }

    /// <summary>Partition column values (present when <see cref="ExtendedFileMetadata"/> is true).</summary>
    public IReadOnlyDictionary<string, string>? PartitionValues { get; init; }

    /// <summary>File size in bytes (present when <see cref="ExtendedFileMetadata"/> is true).</summary>
    public long? Size { get; init; }

    /// <summary>Optional metadata tags.</summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    /// <summary>Optional deletion vector for this file.</summary>
    public DeletionVector? DeletionVector { get; init; }

    /// <summary>Default generated row ID of the first row (row tracking).</summary>
    public long? BaseRowId { get; init; }

    /// <summary>First commit version that contains this file path (row tracking).</summary>
    public long? DefaultRowCommitVersion { get; init; }

    /// <summary>
    /// Gets the reconciliation key for this file action.
    /// </summary>
    internal string ReconciliationKey =>
        DeletionVector is not null ? $"{Path}|{DeletionVector.UniqueId}" : Path;
}
