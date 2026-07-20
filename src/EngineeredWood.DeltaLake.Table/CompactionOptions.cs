// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Options for file compaction operations.
/// </summary>
public sealed record CompactionOptions
{
    /// <summary>Default compaction options.</summary>
    public static CompactionOptions Default { get; } = new();

    /// <summary>
    /// Files smaller than this size are candidates for compaction.
    /// Default: 32 MB.
    /// </summary>
    public long MinFileSize { get; init; } = 32L * 1024 * 1024;

    /// <summary>
    /// Target size for compacted output files. Default: 128 MB.
    /// </summary>
    public long TargetFileSize { get; init; } = 128L * 1024 * 1024;

    /// <summary>
    /// Maximum number of input files to compact in a single commit.
    /// Default: 100.
    /// </summary>
    public int MaxFilesPerCommit { get; init; } = 100;
}
