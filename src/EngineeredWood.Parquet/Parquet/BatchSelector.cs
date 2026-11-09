// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using EngineeredWood.Arrow;

namespace EngineeredWood.Parquet;

/// <summary>
/// Turns decoded batches into the rows a <see cref="ParquetScan"/> selects: removes excluded rows, applies the
/// <see cref="RowFilter"/> with SQL's three-valued AND short-circuited per row, gathers the rows left once,
/// and drops the columns read only for the filter. Stateless across batches, so one instance serves the
/// row groups of a scan concurrently.
/// </summary>
internal sealed class BatchSelector(RowFilter? filter, IExcludedRows? excluded, IReadOnlyCollection<string> dropped)
{
    /// <summary>Selects the rows of each batch of one row group's read, skipping batches left empty.</summary>
    public async IAsyncEnumerable<SelectedRecordBatch> SelectAsync(IAsyncEnumerable<PositionedRecordBatch> batches)
    {
        await foreach (var positioned in batches.ConfigureAwait(false))
        {
            if (Select(positioned) is { } selected)
                yield return selected;
        }
    }

    /// <summary>
    /// The selected rows of <paramref name="positioned"/>, or null when none are. Takes ownership of the
    /// batch: it is disposed, or its arrays move to the result.
    /// </summary>
    public SelectedRecordBatch? Select(PositionedRecordBatch positioned)
    {
        var batch = positioned.Batch;
        int rows = batch.Length;
        if (rows == 0 || (filter is null && excluded is null))
            return rows == 0 ? Discard(batch) : Whole(batch, positioned.FirstRow);

        int wordCount = SelectionMask.WordCount(rows);
        ulong[] liveBuffer = ArrayPool<ulong>.Shared.Rent(wordCount);
        ulong[]? trueBuffer = filter is null ? null : ArrayPool<ulong>.Shared.Rent(wordCount);
        int[]? indexBuffer = null;
        bool owned = true;
        try
        {
            var live = liveBuffer.AsSpan(0, wordCount);
            SelectionMask.SelectAll(live, rows);
            excluded?.ClearExcluded(positioned.FirstRow, rows, live);

            Span<ulong> selection = live;
            if (filter is not null)
            {
                var allTrue = trueBuffer!.AsSpan(0, wordCount);
                live.CopyTo(allTrue);
                foreach (var predicate in filter.Predicates)
                {
                    int liveCount = SelectionMask.Count(live);
                    if (liveCount == 0)
                        break;

                    ReadOnlySpan<int> liveIndices = default;
                    if (liveCount < rows)
                    {
                        indexBuffer ??= ArrayPool<int>.Shared.Rent(rows);
                        SelectionMask.ToIndices(live, indexBuffer);
                        liveIndices = indexBuffer.AsSpan(0, liveCount);
                    }

                    Evaluate(predicate, batch, liveIndices, live, allTrue, rows);
                }

                selection = allTrue;
            }

            int kept = SelectionMask.Count(selection);
            if (kept == 0)
                return Discard(batch);
            if (kept == rows)
            {
                owned = false;
                return Whole(batch, positioned.FirstRow);
            }

            indexBuffer ??= ArrayPool<int>.Shared.Rent(rows);
            SelectionMask.ToIndices(selection, indexBuffer);
            var keptIndices = indexBuffer.AsSpan(0, kept);
            var output = Gather(batch, keptIndices);

            // Contiguous survivors (a run) need no explicit positions.
            Int64Array? positions = null;
            if (keptIndices[kept - 1] - keptIndices[0] + 1 != kept)
            {
                var builder = new Int64Array.Builder().Reserve(kept);
                foreach (int i in keptIndices)
                    builder.Append(positioned.FirstRow + i);
                positions = builder.Build();
            }

            return new SelectedRecordBatch(output, new RowPositions(positioned.FirstRow + keptIndices[0], positions));
        }
        finally
        {
            if (owned)
                batch.Dispose();
            ArrayPool<ulong>.Shared.Return(liveBuffer);
            if (trueBuffer is not null)
                ArrayPool<ulong>.Shared.Return(trueBuffer);
            if (indexBuffer is not null)
                ArrayPool<int>.Shared.Return(indexBuffer);
        }
    }

    // Evaluates one predicate over the live rows and folds its answers into the two selections.
    private static void Evaluate(
        RowPredicate predicate, RecordBatch batch, ReadOnlySpan<int> liveIndices,
        Span<ulong> live, Span<ulong> allTrue, int rows)
    {
        var fields = new List<Field>(predicate.Columns.Count);
        var arrays = new List<IArrowArray>(predicate.Columns.Count);
        foreach (var name in predicate.Columns)
        {
            int index = FieldIndex(batch.Schema, name);
            fields.Add(batch.Schema.GetFieldByIndex(index));
            arrays.Add(batch.Column(index));
        }

        // The predicate's own view of the batch. Over every row it shares the batch's arrays and is not disposed;
        // over some rows it is a gathered copy, disposed after.
        var schema = new Apache.Arrow.Schema(fields, null);
        var view = new RecordBatch(schema, arrays, rows);
        RecordBatch? gathered = liveIndices.IsEmpty ? null : ArrowCompute.Take(view, schema, liveIndices);
        try
        {
            var input = gathered ?? view;
            using var mask = predicate.Evaluate(input)
                ?? throw new InvalidOperationException("A row predicate returned no mask.");
            if (mask.Length != input.Length)
            {
                throw new InvalidOperationException(
                    $"A row predicate returned a mask of {mask.Length} rows for a batch of {input.Length}.");
            }

            SelectionMask.AndThreeValued(live, allTrue, rows, mask, liveIndices);
        }
        finally
        {
            gathered?.Dispose();
        }
    }

    // The whole batch, less the dropped columns.
    private SelectedRecordBatch Whole(RecordBatch batch, long firstRow) =>
        new(Project(batch), new RowPositions(firstRow, null));

    private static SelectedRecordBatch? Discard(RecordBatch batch)
    {
        batch.Dispose();
        return null;
    }

    // The rows at `indices` of the output columns, as a new batch; the input batch is left for the caller.
    private RecordBatch Gather(RecordBatch batch, ReadOnlySpan<int> indices)
    {
        var fields = new List<Field>(batch.ColumnCount);
        var arrays = new List<IArrowArray>(batch.ColumnCount);
        try
        {
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                var field = batch.Schema.GetFieldByIndex(i);
                if (dropped.Contains(field.Name))
                    continue;
                fields.Add(field);
                arrays.Add(ArrowCompute.Take(batch.Column(i), indices));
            }
        }
        catch
        {
            foreach (var array in arrays)
                array.Dispose();
            throw;
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, batch.Schema.Metadata), arrays, indices.Length);
    }

    // The batch without the dropped columns: the kept arrays move to the result, the dropped ones are disposed.
    private RecordBatch Project(RecordBatch batch)
    {
        if (dropped.Count == 0)
            return batch;

        var fields = new List<Field>(batch.ColumnCount);
        var arrays = new List<IArrowArray>(batch.ColumnCount);
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var field = batch.Schema.GetFieldByIndex(i);
            if (dropped.Contains(field.Name))
            {
                batch.Column(i).Dispose();
                continue;
            }

            fields.Add(field);
            arrays.Add(batch.Column(i));
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, batch.Schema.Metadata), arrays, batch.Length);
    }

    private static int FieldIndex(Apache.Arrow.Schema schema, string name)
    {
        for (int i = 0; i < schema.FieldsList.Count; i++)
        {
            if (string.Equals(schema.FieldsList[i].Name, name, StringComparison.Ordinal))
                return i;
        }

        throw new InvalidOperationException($"The decoded batch has no column '{name}'.");
    }
}
