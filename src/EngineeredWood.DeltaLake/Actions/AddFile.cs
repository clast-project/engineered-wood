// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Adds a data file to the table. The file is a logical file consisting
/// of the data file itself plus an optional deletion vector.
/// </summary>
public sealed record AddFile : DeltaAction
{
    /// <summary>URI-encoded relative or absolute path to the data file.</summary>
    public required string Path { get; init; }

    /// <summary>
    /// Partition column values for this file.
    /// Keys are partition column names, values are their string-serialized values.
    /// </summary>
    public required IReadOnlyDictionary<string, string> PartitionValues { get; init; }

    /// <summary>File size in bytes.</summary>
    public required long Size { get; init; }

    /// <summary>File creation/modification time in milliseconds since epoch.</summary>
    public required long ModificationTime { get; init; }

    /// <summary>
    /// Whether the data in this file represents a logical change to the table
    /// (as opposed to a file rearrangement like compaction).
    /// </summary>
    public required bool DataChange { get; init; }

    /// <summary>
    /// JSON-encoded column statistics. Parsed lazily into <see cref="ParsedStats"/>.
    /// </summary>
    public string? Stats { get; init; }

    /// <summary>Optional metadata tags.</summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }

    /// <summary>Optional deletion vector for this file.</summary>
    public DeletionVector? DeletionVector { get; init; }

    /// <summary>Default generated row ID of the first row in this file (row tracking).</summary>
    public long? BaseRowId { get; init; }

    /// <summary>First commit version that contains this file path (row tracking).</summary>
    public long? DefaultRowCommitVersion { get; init; }

    /// <summary>Clustering implementation name for clustered tables.</summary>
    public string? ClusteringProvider { get; init; }

    /// <summary>
    /// Gets the reconciliation key for this file action.
    /// Actions are reconciled by <c>(path, deletionVector.uniqueId)</c>.
    /// </summary>
    internal string ReconciliationKey =>
        DeletionVector is not null ? $"{Path}|{DeletionVector.UniqueId}" : Path;
}
