// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Pluggable data-file <b>reader</b> — the read-side counterpart of <see cref="IDataFileWriter"/>. When set on
/// <see cref="DeltaTableOptions.DataFileReader"/>, the Delta table delegates the decoding of each parquet data
/// file to this reader (e.g. a host's native <c>read_parquet</c>) instead of its built-in
/// <c>ParquetFileReader</c>; everything ABOVE the raw decode stays in the Delta layer — column-mapping rename,
/// deletion-vector filtering, schema-evolution backfill, partition-column re-add, type widening, the transient
/// rowid, and row-tracking materialization. Together with <see cref="IDataFileWriter"/> this completes the
/// codec seam: the Delta layer owns the <c>_delta_log</c> protocol, the host owns the parquet bytes in BOTH
/// directions (which is what makes copy-on-write rewrites and compaction preserve codec-specific column
/// representations the built-in reader is blind to, e.g. the parquet VARIANT logical-type annotation).
///
/// <para><b>Experimental</b> (shares diagnostic <c>EWDELTA0001</c> with <see cref="IDataFileWriter"/>): the
/// codec seam has no in-tree implementation and an unsettled contract, and may change or be removed. See
/// <c>doc/codec-seam-investigation.md</c>.</para>
/// </summary>
[Experimental("EWDELTA0001")]
public interface IDataFileReader
{
    /// <summary>
    /// Reads the parquet data file at <paramref name="relativePath"/> (relative to the table root, URL-decoded)
    /// RAW: batches exactly as stored — PHYSICAL column names, <b>file order</b>, deletion-vector rows
    /// INCLUDED. File order is a correctness requirement, not a preference: every consumer is position-keyed
    /// (DV filtering, transient rowids, row-tracking materialization all count absolute row positions in read
    /// order). <paramref name="physicalColumns"/> optionally projects (by physical name); null reads all
    /// columns. Batch boundaries are the implementation's choice.
    /// </summary>
    IAsyncEnumerable<RecordBatch> ReadAsync(string relativePath, IReadOnlyList<string>? physicalColumns,
                                            CancellationToken cancellationToken);
}
