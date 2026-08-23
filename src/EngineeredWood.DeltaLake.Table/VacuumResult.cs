// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Result of a vacuum operation.
/// </summary>
public sealed record VacuumResult
{
    /// <summary>Files that were (or would be, in dry-run mode) deleted.</summary>
    public IReadOnlyList<string> FilesToDelete { get; init; } = [];

    /// <summary>Number of files actually deleted (0 in dry-run mode).</summary>
    public int FilesDeleted { get; init; }

    /// <summary>
    /// The version of the <c>VACUUM END</c> commit, or null in dry-run mode (which commits nothing).
    /// </summary>
    /// <remarks>
    /// A vacuum brackets its deletions with two <c>commitInfo</c>-only commits, so it advances the table's
    /// version by two while changing no state. Reported because the caller otherwise has no way to learn
    /// the version it landed at — and because the table's post-commit work (the version checksum, the
    /// interval checkpoint) is defined in terms of a committed version and has to be given one.
    /// </remarks>
    public long? LastCommittedVersion { get; init; }
}
