// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Pluggable data-file writer. When set on <see cref="DeltaTableOptions.DataFileWriter"/>, the Delta table
/// delegates the production of each parquet data file to this writer instead of using its built-in
/// <c>ParquetFileWriter</c> — everything else (partition split, row tracking, column mapping, stats collection,
/// the <c>add</c> action, and the commit) stays in the Delta layer. This is the seam that lets a host embed its
/// own parquet writer (e.g. DuckDB's native <c>COPY … TO … (FORMAT parquet)</c>) for the data bytes while the
/// engineered-wood <c>_delta_log</c> layer owns the protocol.
///
/// <para><b>Experimental.</b> The codec seam ships with no in-tree implementation and its contract is not
/// settled (the <c>relativePath</c> encoding, the partition-directory-creation obligation, and the
/// load-bearing field metadata are all still under-specified — see <c>doc/codec-seam-investigation.md</c>).
/// It may change or be removed. Set <c>#pragma warning disable EWDELTA0001</c> (or <c>&lt;NoWarn&gt;</c>) to
/// use it.</para>
/// </summary>
[Experimental("EWDELTA0001")]
public interface IDataFileWriter
{
    /// <summary>Writes <paramref name="batches"/> as a single parquet file at <paramref name="relativePath"/>
    /// (relative to the table root, including any partition subdirectory), and returns the written file's byte
    /// size (stored on the <c>add</c> action). The batches are <b>streamed</b> — a fresh write yields one batch,
    /// a copy-on-write rewrite yields the surviving batches of one source file — so an implementation may consume
    /// them incrementally (e.g. a streaming DuckDB <c>COPY</c>) instead of holding the whole file in memory; one
    /// that wants a materialized list can collect them itself. This is symmetric with the streaming
    /// <see cref="IDataFileReader.ReadAsync"/>. The implementation is responsible for placing the bytes at the
    /// location the table's filesystem maps <paramref name="relativePath"/> to.</summary>
    ValueTask<long> WriteAsync(IAsyncEnumerable<RecordBatch> batches, string relativePath,
                               CancellationToken cancellationToken);
}
