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
                    {
                        // No row left to answer, but every predicate is still asked, over no rows, as EvalAnd
                        // asks every operand over an empty selection: one that cannot be evaluated at all (an
                        // unresolvable column, a comparison with no common type) still raises, rather than
                        // passing whenever an earlier predicate happened to reject every row of the batch.
                        EvaluateOverNoRows(predicate, batch);
                        continue;
                    }

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
            owned = false;
            ReleaseAfterGather(batch, output);

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
        var view = PredicateView(predicate, batch);

        // Over some rows the predicate gets a gathered copy. It is NOT disposed: a gather may share buffers with
        // the batch (a view column's data buffers, a dictionary), and disposing the copy would free them under
        // the predicates after this one and the output gather. Its own buffers are managed, so the GC has them.
        var input = liveIndices.IsEmpty ? view : ArrowCompute.Take(view, view.Schema, liveIndices);
        var mask = Ask(predicate, input);
        try
        {
            SelectionMask.AndThreeValued(live, allTrue, rows, mask, liveIndices);
        }
        finally
        {
            Release(mask, input);
        }
    }

    private static void EvaluateOverNoRows(RowPredicate predicate, RecordBatch batch)
    {
        var view = PredicateView(predicate, batch);
        var input = ArrowCompute.Take(view, view.Schema, ReadOnlySpan<int>.Empty);
        Release(Ask(predicate, input), input);
    }

    // The predicate's columns of the batch, in its order, sharing the batch's arrays: never disposed.
    private static RecordBatch PredicateView(RowPredicate predicate, RecordBatch batch)
    {
        var fields = new List<Field>(predicate.Columns.Count);
        var arrays = new List<IArrowArray>(predicate.Columns.Count);
        foreach (var name in predicate.Columns)
        {
            int index = FieldIndex(batch.Schema, name);
            fields.Add(batch.Schema.GetFieldByIndex(index));
            arrays.Add(batch.Column(index));
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, null), arrays, batch.Length);
    }

    private static BooleanArray Ask(RowPredicate predicate, RecordBatch input)
    {
        var mask = predicate.Evaluate(input)
            ?? throw new InvalidOperationException("A row predicate returned no mask.");
        if (mask.Length != input.Length)
        {
            int length = mask.Length;
            Release(mask, input);
            throw new InvalidOperationException(
                $"A row predicate returned a mask of {length} rows for a batch of {input.Length}.");
        }

        return mask;
    }

    /// <summary>
    /// Disposes a predicate's mask, unless it shares a buffer with the batch the predicate was given: a predicate
    /// may answer with one of its own input columns (<c>WHERE flag</c>), and over every live row that column IS
    /// the decoded batch's, which the read still gathers from or returns.
    /// </summary>
    private static void Release(BooleanArray mask, RecordBatch input)
    {
        var buffers = new HashSet<ReadOnlyMemory<byte>>();
        CollectBuffers(mask.Data, buffers);
        for (int i = 0; i < input.ColumnCount; i++)
        {
            if (SharesAny(input.Column(i).Data, buffers))
                return;
        }

        mask.Dispose();
    }

    /// <summary>
    /// Releases the decoded batch once <paramref name="output"/> is gathered from it: every column except those
    /// the output still shares a buffer with. <see cref="ArrowCompute.Take(IArrowArray, ReadOnlySpan{int})"/>
    /// copies most types but shares a view column's data buffers (and a dictionary), and the reader builds
    /// those in native memory, so disposing such a column would free bytes the output reads. A column left
    /// undisposed is released by its buffers' finalizers once nothing references them.
    /// </summary>
    private static void ReleaseAfterGather(RecordBatch batch, RecordBatch output)
    {
        var shared = new HashSet<ReadOnlyMemory<byte>>();
        for (int i = 0; i < output.ColumnCount; i++)
            CollectBuffers(output.Column(i).Data, shared);

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var column = batch.Column(i);
            if (!SharesAny(column.Data, shared))
                column.Dispose();
        }
    }

    private static void CollectBuffers(ArrayData data, HashSet<ReadOnlyMemory<byte>> into)
    {
        foreach (var buffer in data.Buffers ?? [])
        {
            if (!buffer.IsEmpty)
                into.Add(buffer.Memory);
        }

        if (data.Children is { } children)
        {
            foreach (var child in children)
                CollectBuffers(child, into);
        }
        if (data.Dictionary is { } dictionary)
            CollectBuffers(dictionary, into);
    }

    private static bool SharesAny(ArrayData data, HashSet<ReadOnlyMemory<byte>> shared)
    {
        foreach (var buffer in data.Buffers ?? [])
        {
            if (!buffer.IsEmpty && shared.Contains(buffer.Memory))
                return true;
        }

        if (data.Children is { } children)
        {
            foreach (var child in children)
            {
                if (SharesAny(child, shared))
                    return true;
            }
        }

        return data.Dictionary is { } dictionary && SharesAny(dictionary, shared);
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
