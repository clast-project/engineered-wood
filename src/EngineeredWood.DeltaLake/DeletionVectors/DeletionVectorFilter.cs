// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using EngineeredWood.Arrow;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Filters deleted rows from a RecordBatch using a deletion vector.
/// </summary>
public static class DeletionVectorFilter
{
    /// <summary>
    /// Returns <paramref name="batch"/> without the rows <paramref name="deletedRows"/> deletes, or
    /// <paramref name="batch"/> itself when it holds none of them. Positions in the deletion vector are absolute
    /// in the data file; <paramref name="batchStartRow"/> is the absolute position of the batch's first row.
    /// </summary>
    public static RecordBatch Filter(RecordBatch batch, DeletionVectorBitmap deletedRows, long batchStartRow)
    {
        if (batch is null)
            throw new ArgumentNullException(nameof(batch));
        if (deletedRows is null)
            throw new ArgumentNullException(nameof(deletedRows));

        using var kept = KeptRows.For(deletedRows, batchStartRow, batch.Length);
        return kept.AllKept ? batch : ArrowCompute.Take(batch, batch.Schema, kept.Indices);
    }

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

/// <summary>
/// The rows of one batch a deletion vector leaves: either all of them (<see cref="AllKept"/>, nothing
/// allocated) or the kept batch-relative indices in ascending order, in a pooled array that
/// <see cref="Dispose"/> returns. Computing it once per batch serves both the gather and anything else that
/// must stay row-aligned with it (row ids, positions).
/// </summary>
internal readonly struct KeptRows : IDisposable
{
    private readonly int[]? _indices;

    private KeptRows(int[]? indices, int count)
    {
        _indices = indices;
        Count = count;
    }

    /// <summary>True when the deletion vector deletes no row of the batch.</summary>
    public bool AllKept => _indices is null;

    /// <summary>How many rows are kept.</summary>
    public int Count { get; }

    /// <summary>The kept indices; only meaningful when <see cref="AllKept"/> is false.</summary>
    public ReadOnlySpan<int> Indices => _indices is null ? default : _indices.AsSpan(0, Count);

    /// <summary>The batch-relative index of the <paramref name="n"/>th kept row.</summary>
    public int this[int n] => _indices is null ? n : _indices[n];

    public static KeptRows For(DeletionVectorBitmap deletedRows, long batchStartRow, int batchLength)
    {
        if (!deletedRows.Intersects(batchStartRow, batchLength))
            return new KeptRows(null, batchLength);

        int words = SelectionMask.WordCount(batchLength);
        var selection = ArrayPool<ulong>.Shared.Rent(words);
        try
        {
            var span = selection.AsSpan(0, words);
            SelectionMask.SelectAll(span, batchLength);
            deletedRows.ClearDeleted(batchStartRow, batchLength, span);
            int count = SelectionMask.Count(span);
            var indices = ArrayPool<int>.Shared.Rent(Math.Max(count, 1));
            SelectionMask.ToIndices(span, indices);
            return new KeptRows(indices, count);
        }
        finally
        {
            ArrayPool<ulong>.Shared.Return(selection);
        }
    }

    public void Dispose()
    {
        if (_indices is not null)
            ArrayPool<int>.Shared.Return(_indices);
    }
}
