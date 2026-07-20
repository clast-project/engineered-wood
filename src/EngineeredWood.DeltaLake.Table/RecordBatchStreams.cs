// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Adapts an already-materialized batch sequence to the streaming shape <see cref="IDataFileWriter.WriteAsync"/>
/// takes. The Delta layer sometimes has the batches in hand (a copy-on-write rewrite collects a file's
/// survivors so <c>StatsCollector</c> can see them all); the seam still takes <see cref="IAsyncEnumerable{T}"/>
/// so a streaming host — e.g. a DuckDB <c>COPY</c> — is never forced to materialize, symmetric with the
/// streaming <see cref="IDataFileReader.ReadAsync"/>.
/// </summary>
internal static class RecordBatchStreams
{
    public static async IAsyncEnumerable<RecordBatch> ToAsyncEnumerable(this IEnumerable<RecordBatch> batches)
    {
        foreach (var batch in batches)
            yield return batch;
        await Task.CompletedTask; // async iterator with no genuine await — keeps the streaming signature
    }
}
