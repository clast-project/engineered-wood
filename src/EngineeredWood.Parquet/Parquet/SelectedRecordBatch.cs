// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Parquet;

/// <summary>
/// A batch returned by <see cref="ParquetFileReader.ReadSelectedAsync"/>, with the file position of each of
/// its rows. Its rows need not be contiguous in the file: rows a filter dropped may lie between them.
/// </summary>
/// <param name="Batch">The rows.</param>
/// <param name="Positions">Where each row is in the file.</param>
public readonly record struct SelectedRecordBatch(RecordBatch Batch, RowPositions Positions) : IDisposable
{
    /// <summary>Disposes the batch and the positions.</summary>
    public void Dispose()
    {
        Batch?.Dispose();
        Positions.Explicit?.Dispose();
    }
}
