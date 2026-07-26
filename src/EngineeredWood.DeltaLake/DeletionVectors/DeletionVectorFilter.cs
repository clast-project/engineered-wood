// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Filters deleted rows from a RecordBatch using a deletion vector.
/// </summary>
public static class DeletionVectorFilter
{
    /// <summary>
    /// Returns a new RecordBatch with rows marked as deleted removed.
    /// Row indices in <paramref name="deletedRows"/> are relative to the
    /// start of the data file (absolute row positions).
    /// </summary>
    /// <param name="batch">The source batch to filter.</param>
    /// <param name="deletedRows">Set of absolute row indices that are deleted.</param>
    /// <param name="batchStartRow">
    /// The absolute row index of the first row in this batch within the data file.
    /// Used to translate absolute DV row indices to batch-relative indices.
    /// </param>
    public static RecordBatch Filter(
        RecordBatch batch, HashSet<long> deletedRows, long batchStartRow)
    {
        if (deletedRows.Count == 0)
            return batch;

        // Find which rows in this batch are NOT deleted
        var keepRows = new List<int>();
        for (int i = 0; i < batch.Length; i++)
        {
            long absoluteRow = batchStartRow + i;
            if (!deletedRows.Contains(absoluteRow))
                keepRows.Add(i);
        }

        if (keepRows.Count == batch.Length)
            return batch; // No rows deleted in this batch

        // An all-deleted batch takes zero rows rather than being special-cased: gathering an empty
        // selection already yields a correctly-typed zero-length column for every type, where the old
        // per-type construction fell back to a StringArray and produced a batch whose columns
        // contradicted its own schema.
        var columns = new IArrowArray[batch.ColumnCount];
        for (int col = 0; col < batch.ColumnCount; col++)
            columns[col] = ArrowCompute.Take(batch.Column(col), keepRows);

        return new RecordBatch(batch.Schema, columns, keepRows.Count);
    }
}
