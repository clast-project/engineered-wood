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
    /// <remarks>
    /// <para>With the built-in Parquet writer, the sources' rows are joined into row groups of up to
    /// <see cref="EngineeredWood.Parquet.ParquetWriteOptions.RowGroupMaxRows"/> rows or (estimated from the Arrow
    /// buffers) <see cref="EngineeredWood.Parquet.ParquetWriteOptions.RowGroupMaxBytes"/>, however the source files
    /// were cut. A file is closed once its written size reaches this target, after a row group, so it can overshoot
    /// by up to one row group.</para>
    /// <para>With a host <see cref="IDataFileWriter"/>, a file is cut when its batches' Arrow size reaches this
    /// target instead, and the host chooses its own row groups. Arrow data is larger than the Parquet it becomes,
    /// so those files come out smaller than the target.</para>
    /// </remarks>
    public long TargetFileSize { get; init; } = 128L * 1024 * 1024;

    /// <summary>
    /// Maximum number of input files to compact in a single commit.
    /// Default: 100.
    /// </summary>
    public int MaxFilesPerCommit { get; init; } = 100;
}
