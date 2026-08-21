// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics.CodeAnalysis;
using EngineeredWood.DeltaLake.Checkpoint;
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
    /// Number of commits between automatic checkpoints, used when the table declares no interval of its
    /// own. Set to 0 to disable automatic checkpointing. Default: 10.
    /// </summary>
    /// <remarks>
    /// <para>A table's <c>delta.checkpointInterval</c> property takes precedence over this: the property
    /// is the table's own statement about how often it wants checkpointing, and another engine may be
    /// tuning it deliberately. This value applies when the table declares none, or declares one that
    /// cannot be read as a positive integer.</para>
    ///
    /// <para><b>Zero is the exception, and it wins outright.</b> Disabling checkpointing is a decision the
    /// caller takes — a host may be driving checkpoints on a cadence of its own, through
    /// <see cref="DeltaTable.CheckpointAsync"/> or otherwise — so no table property switches it back on.</para>
    ///
    /// <para>Resolved once when the table is opened or created. A property changed by a later commit takes
    /// effect on the next open, the same granularity as every other configuration read here.</para>
    /// </remarks>
    public int CheckpointInterval { get; init; } = 10;

    /// <summary>
    /// Which checkpoint spec automatic checkpointing writes. Default:
    /// <see cref="CheckpointFormat.Automatic"/> — a UUID-named V2 checkpoint on a table whose
    /// <c>delta.checkpointPolicy</c> is <c>v2</c> and which has enabled the <c>v2Checkpoint</c> feature,
    /// and a classic V1 checkpoint otherwise.
    /// </summary>
    /// <remarks>
    /// The default is what delta-spark would do with the same table, which is the point: a table created
    /// by another engine keeps the checkpoint form its own configuration asks for when EW maintains it.
    /// <see cref="CheckpointFormat.Classic"/> and <see cref="CheckpointFormat.V2"/> pin one form
    /// regardless; <see cref="CheckpointFormat.V2WhenSupported"/> takes delta-kernel-rs's rule instead of
    /// delta-spark's. See <see cref="CheckpointFormat"/> for when each is worth choosing. For the V2
    /// writer's own settings (sidecar policy, body format), supply a configured writer through
    /// <see cref="Checkpoint.CheckpointWriter.V2Writer"/>.
    /// </remarks>
    public CheckpointFormat CheckpointFormat { get; init; } = CheckpointFormat.Automatic;

    /// <summary>
    /// Default retention period for vacuum operations. Default: 7 days.
    /// </summary>
    public TimeSpan VacuumRetention { get; init; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Whether vacuum leaves a top-level <c>metadata/</c> directory alone. Default: true, as in Delta.
    /// </summary>
    /// <remarks>
    /// <para>UniForm writes converted Iceberg metadata to <c>metadata/</c>, and the name is reserved for
    /// it. Nothing in the Delta log references those files, so no keep-set can protect them and a sweep
    /// would collect the lot — which is why Delta hides the directory by NAME rather than by the
    /// underscore convention every other protected directory relies on. This library can enable
    /// <c>icebergCompatV1</c>/<c>V2</c> itself, so the case is reachable on a table we maintain.</para>
    ///
    /// <para>Set false to sweep it — appropriate only when nothing is generating Iceberg metadata for
    /// this table and the directory is known to be junk. Delta exposes the same switch
    /// (<c>shouldIcebergMetadataDirBeHidden</c>) and defaults it the same way.</para>
    /// </remarks>
    public bool HideIcebergMetadataDirectory { get; init; } = true;

    /// <summary>Whether to collect per-column statistics on write. Default: true.</summary>
    public bool CollectStats { get; init; } = true;

    /// <summary>
    /// <para>How Hive-style partition DIRECTORY NAMES are spelled. Default:
    /// <see cref="PartitionPathSpelling.SparkCompatible"/> — byte-identical to what Spark would write for
    /// the storage this table lives on.</para>
    ///
    /// <para>Set <see cref="PartitionPathSpelling.Portable"/> when the directory tree itself has to be
    /// movable — copied onto a Win32 volume, or written by a mixed fleet over a share that constrains
    /// names — at the cost of directory names that no longer match Spark's for ordinary values like
    /// <c>a b</c> or <c>a+b</c>.</para>
    ///
    /// <para><b>This is cosmetic and per-writer, not a table-level invariant.</b> Delta defines no
    /// property for it, so nothing is persisted and nothing stops the next writer choosing differently.
    /// That is safe: MEASURED, no reader recovers a partition value from a directory name — values come
    /// from <c>add.partitionValues</c> and files are located through <c>add.path</c> — and Spark itself,
    /// pointed at a table written under the other spelling, reads it correctly and appends into a second
    /// directory beside the first. Changing this setting on an existing table is therefore allowed and
    /// merely untidy; it never orphans or rewrites existing data.</para>
    /// </summary>
    public PartitionPathSpelling PartitionPathSpelling { get; init; } = PartitionPathSpelling.SparkCompatible;

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
