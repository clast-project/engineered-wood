// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;

namespace EngineeredWood.Lance.Table.Deletions;

/// <summary>
/// Builds a new <see cref="RecordBatch"/> containing only the rows that
/// are NOT marked deleted by the given <see cref="DeletionMask"/>.
/// </summary>
internal static class RecordBatchRowFilter
{
    public static RecordBatch Apply(
        RecordBatch batch, Apache.Arrow.Schema schema, DeletionMask mask)
    {
        if (mask.DeletedCount == 0)
            return batch;

        var keepRows = new List<int>(batch.Length - mask.DeletedCount);
        for (int i = 0; i < batch.Length; i++)
            if (!mask.IsDeleted(i)) keepRows.Add(i);

        return ApplyKeepList(batch, schema, keepRows);
    }

    /// <summary>
    /// Apply a pre-computed list of kept row indices. Used by callers
    /// that combine multiple filter sources (e.g., deletion mask plus
    /// predicate evaluation) into a single keep-list to avoid two
    /// take passes.
    /// </summary>
    public static RecordBatch ApplyKeepList(
        RecordBatch batch, Apache.Arrow.Schema schema, List<int> keepRows)
    {
        if (keepRows.Count == batch.Length) return batch;

        var columns = new IArrowArray[batch.ColumnCount];
        for (int c = 0; c < batch.ColumnCount; c++)
            columns[c] = ArrowCompute.Take(batch.Column(c), keepRows);

        return new RecordBatch(schema, columns, keepRows.Count);
    }
}
