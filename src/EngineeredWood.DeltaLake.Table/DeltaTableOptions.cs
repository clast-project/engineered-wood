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

    /// <summary>
    /// Whether the read pipeline presents each <c>variant</c> column in its TRANSPORT form: one
    /// self-delimiting BINARY per row (the parquet-variant metadata bytes immediately followed by the
    /// value bytes), tagged with the <see cref="Schema.SchemaConverter.VariantTransportExtensionName"/>
    /// field-metadata marker — instead of the canonical <c>arrow.parquet.variant</c>
    /// <see cref="Apache.Arrow.VariantArray"/>. Default: <see langword="false"/> (canonical).
    /// <para>For embedding hosts whose Arrow boundary cannot carry an extension type over struct storage
    /// (the transport is a LEAF binary, safe to cross any C-data boundary). The WRITE side accepts the
    /// transport form unconditionally — marker-keyed, a no-op for canonical input — so only the read
    /// direction needs selecting. A pluggable <see cref="DataFileReader"/> that already delivers the
    /// transport form (a host decoding variant itself) passes through unchanged. See
    /// <see cref="VariantTransport"/>.</para>
    /// </summary>
    public bool VariantTransportBlob { get; init; }

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
