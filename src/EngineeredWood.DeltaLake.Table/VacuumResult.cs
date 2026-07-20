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
}
