// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics.CodeAnalysis;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Configuration options for Delta table operations.
/// </summary>
public sealed record DeltaTableOptions
{
    /// <summary>Default options.</summary>
    public static DeltaTableOptions Default { get; } = new();

    /// <summary>Parquet write options for data files.</summary>
    public ParquetWriteOptions ParquetWriteOptions { get; init; } = ParquetWriteOptions.Default;

    /// <summary>Parquet read options for data files.</summary>
    public ParquetReadOptions ParquetReadOptions { get; init; } = ParquetReadOptions.Default;

    /// <summary>Target size for individual data files in bytes. Default: 128 MB.</summary>
    public long TargetFileSize { get; init; } = 128L * 1024 * 1024;

    /// <summary>
    /// Number of commits between automatic checkpoints.
    /// Set to 0 to disable automatic checkpointing. Default: 10.
    /// </summary>
    public int CheckpointInterval { get; init; } = 10;

    /// <summary>
    /// Default retention period for vacuum operations. Default: 7 days.
    /// </summary>
    public TimeSpan VacuumRetention { get; init; } = TimeSpan.FromDays(7);

    /// <summary>Whether to collect per-column statistics on write. Default: true.</summary>
    public bool CollectStats { get; init; } = true;

    /// <summary>
    /// Whether file pruning reads a checkpoint's typed <c>add.stats_parsed</c> columns in preference
    /// to its JSON <c>stats</c> string when the checkpoint carries both. Default: true.
    /// </summary>
    /// <remarks>
    /// <para>Only the tie is being broken here. A checkpoint that carries just one copy is read from
    /// that one either way — including the typed-only shape a table with
    /// <c>delta.checkpoint.writeStatsAsJson=false</c> produces, which has no JSON to fall back to.
    /// A column the typed struct does not cover (booleans, which carry no bounds there) also falls
    /// back to the JSON regardless of this setting.</para>
    ///
    /// <para>The typed path avoids parsing a file's whole statistics blob to answer a predicate that
    /// names one column, and does so on every query: measured at ~14x faster with ~100x less
    /// allocation over a 100,000-file checkpoint. Set to false to force the JSON path — the two are
    /// expected to agree, and any disagreement is a bug worth reporting.</para>
    /// </remarks>
    public bool PreferTypedCheckpointStats { get; init; } = true;

    /// <summary>
    /// Whether a <c>variant</c> column's parquet group carries the <c>VARIANT</c> logical-type
    /// annotation. Default: <see langword="true"/>.
    /// <para>The Delta spec defines a variant's physical layout as a plain <c>struct&lt;value,
    /// metadata&gt;</c> and does not require the parquet annotation, so both settings produce
    /// spec-conforming files that this library and delta-rs read either way.</para>
    /// <para><b>true (default)</b> emits the annotation — what Databricks/Spark 4.1+ (where variant is
    /// GA) and DuckDB write and expect. <b>false</b> omits it, writing the bare struct-of-binary for
    /// compatibility with Spark 4.0.x, whose parquet reader predates the VARIANT logical type and
    /// throws a <c>NullPointerException</c> on an annotated group. Set false only when targeting a
    /// reader stuck on that experimental-variant era; it costs nothing with modern readers but also
    /// buys nothing there.</para>
    /// </summary>
    public bool EmitVariantLogicalType { get; init; } = true;

    /// <summary>Optional pluggable writer for data-file bytes. When set, the table delegates parquet file
    /// production to it (e.g. a host's native parquet writer) instead of the built-in <c>ParquetFileWriter</c>;
    /// all other write logic (partitioning, row tracking, stats, the <c>add</c> action, the commit) is unchanged.
    /// Default: null (use the built-in writer). <b>Experimental</b> (<c>EWDELTA0001</c>) — the codec seam's
    /// contract is not settled; see <see cref="IDataFileWriter"/>.</summary>
    [Experimental("EWDELTA0001")]
    public IDataFileWriter? DataFileWriter { get; init; }

    /// <summary>Optional pluggable reader for data-file bytes — the read-side counterpart of
    /// <see cref="DataFileWriter"/>. When set, the table decodes each data file through it (raw physical
    /// batches in file order; see <see cref="IDataFileReader"/>) instead of the built-in
    /// <c>ParquetFileReader</c>; all processing above the decode (column-mapping rename, DV filtering,
    /// backfill, partition re-add) is unchanged. Default: null (use the built-in reader). <b>Experimental</b>
    /// (<c>EWDELTA0001</c>) — see <see cref="IDataFileReader"/>.</summary>
    [Experimental("EWDELTA0001")]
    public IDataFileReader? DataFileReader { get; init; }
}
