// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Parquet write options for checkpoint files, which are held to a stricter bar than data files:
/// every engine that opens the table must be able to read them.
/// </summary>
internal static class CheckpointParquetOptions
{
    /// <summary>
    /// Narrows <paramref name="options"/> to encodings every Delta reader supports.
    /// </summary>
    /// <remarks>
    /// Spark's vectorized Parquet reader rejects <c>BYTE_STREAM_SPLIT</c> outright ("Unsupported
    /// encoding: BYTE_STREAM_SPLIT"). A checkpoint is read as one file, so a single such column makes
    /// the WHOLE checkpoint unreadable — and with it the table, once the commits it summarises age
    /// out. Any table with a float, double or decimal column produces one, because <c>stats_parsed</c>
    /// carries per-file bounds for those columns.
    /// <para>Since #269 that encoding is no longer EW's default, so for a caller who passes nothing
    /// this narrowing is a no-op. It stays because a caller MAY still select it, and the trade-off is
    /// not the same on both sides: a data file's encoding is theirs to choose, the transaction log's
    /// is not.</para>
    /// </remarks>
    public static ParquetWriteOptions For(ParquetWriteOptions? options) =>
        (options ?? ParquetWriteOptions.Default) with
        {
            FloatingPointEncoding = FloatingPointEncoding.Plain,
        };
}
