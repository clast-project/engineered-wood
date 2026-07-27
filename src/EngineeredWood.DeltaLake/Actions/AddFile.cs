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

    /// <summary>
    /// Set when this action came from a checkpoint carrying typed <c>stats_parsed</c>: points at the
    /// file's row in that checkpoint's statistics columns, so a bound can be read from the Arrow array
    /// rather than parsed out of <see cref="Stats"/>. Internal — the JSON string stays the public
    /// contract, and every consumer works unchanged when this is null.
    /// </summary>
    internal Checkpoint.ParsedStatsRef? TypedStats { get; init; }

    /// <summary>
    /// The file's statistics as a Delta <c>stats</c> JSON string, synthesised from the checkpoint's
    /// typed columns when there is no string of its own. Callers that WRITE statistics back — into a
    /// commit, or widened onto a rewritten file — must use this rather than <see cref="Stats"/>, or a
    /// file read from a checkpoint with <c>writeStatsAsJson=false</c> silently loses its statistics
    /// the moment it moves.
    /// </summary>
    internal string? GetStatsJson() =>
        Stats ?? (TypedStats is { } typed ? typed.View.BuildStatsJson(typed.Row) : null);

    /// <summary>
    /// The file's row count from whichever copy of its statistics carries one, or null when neither
    /// does. Callers must not reach for <see cref="Stats"/> directly: a checkpoint written with
    /// <c>delta.checkpoint.writeStatsAsJson=false</c> has no JSON string at all, and a row count
    /// silently read as zero from it would mis-assign row ids and mis-size compaction groups.
    /// </summary>
    internal long? GetNumRecords()
    {
        if (TypedStats is { } typed && typed.View.GetNumRecords(typed.Row) is { } records)
            return records;

        return ColumnStats.Parse(Stats)?.NumRecords;
    }

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
