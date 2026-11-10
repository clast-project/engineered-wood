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
/// <param name="fileColumns">The file's top-level column names, when known: a returned column is not answered from
/// its dictionary if its answer column's name is one of them (review round 1).</param>
/// <param name="wholeBatchLiveShare">See <see cref="DefaultWholeBatchLiveShare"/>.</param>
/// <param name="fusesAnswers">Whether adjacent value-wise predicates over one column are answered from its dictionary
/// together (#502 phase 5b); see <see cref="AnswerPlan"/>.</param>
internal sealed class BatchSelector(
    RowFilter? filter, IExcludedRows? excluded, IReadOnlyCollection<string> dropped,
    IReadOnlyCollection<string>? fileColumns = null, double wholeBatchLiveShare = BatchSelector.DefaultWholeBatchLiveShare,
    bool fusesAnswers = true)
{
    /// <summary>
    /// The predicates the reader may answer from a column chunk's dictionary (#502 phases 4b, 4d and 5b), by the column
    /// they read: a run of adjacent value-wise predicates over that one column, which no other predicate reads. The
    /// reader decides per chunk whether it does (Data.DictionaryAnswer); a batch column of answers says it did. For a
    /// column the read drops, the answers stand in its place; for one it returns, they come as an extra column
    /// (<see cref="Data.DictionaryAnswer.AnswerColumnName"/>), which the read drops.
    /// </summary>
    public IReadOnlyDictionary<string, AnswerPlan>? Answerable { get; } =
        AnswerableOf(filter, dropped, fileColumns, fusesAnswers);

    // The columns the read drops: the predicates' own, and the answer columns of returned ones.
    private readonly IReadOnlyCollection<string> _dropped = DroppedWithAnswers(dropped, filter, fileColumns, fusesAnswers);

    private static IReadOnlyCollection<string> DroppedWithAnswers(
        IReadOnlyCollection<string> dropped, RowFilter? filter, IReadOnlyCollection<string>? fileColumns, bool fuse)
    {
        if (AnswerableOf(filter, dropped, fileColumns, fuse) is not { } answerable
            || answerable.Values.All(p => !p.KeepValues))
        {
            return dropped;
        }

        var all = new HashSet<string>(dropped, StringComparer.Ordinal);
        foreach (var (name, plan) in answerable)
        {
            if (plan.KeepValues)
                all.Add(Data.DictionaryAnswer.AnswerColumnName(name));
        }

        return all;
    }

    /// <summary>
    /// The answerable runs. A run is a value-wise predicate over one column, and (when <paramref name="fuse"/>) the
    /// value-wise predicates over that same column that directly follow it, none dropping earlier NULLs (a host mask
    /// after the filter is its own condition). Only adjacent ones: answering a later predicate where the first stands
    /// would hide rows from the predicates between them, and with those rows the errors they would raise. A column is
    /// answerable when its run is every predicate that reads it.
    /// </summary>
    private static Dictionary<string, AnswerPlan>? AnswerableOf(
        RowFilter? filter, IReadOnlyCollection<string> dropped, IReadOnlyCollection<string>? fileColumns, bool fuse)
    {
        if (filter is null)
            return null;

        var readers = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var predicate in filter.Predicates)
        {
            foreach (var name in predicate.Columns)
                readers[name] = readers.TryGetValue(name, out int n) ? n + 1 : 1;
        }

        Dictionary<string, AnswerPlan>? result = null;
        var predicates = filter.Predicates;
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!IsAnswerable(predicates[i]))
                continue;

            string name = predicates[i].Columns[0];
            int end = i + 1;
            while (fuse && end < predicates.Count && IsAnswerable(predicates[end]) && !predicates[end].DropsEarlierNulls
                   && string.Equals(predicates[end].Columns[0], name, StringComparison.Ordinal))
            {
                end++;
            }

            int length = end - i;
            if (readers[name] == length
                // A returned column's answers need a column name of their own; one the file already has is not.
                && (dropped.Contains(name) || fileColumns is null
                    || !fileColumns.Contains(Data.DictionaryAnswer.AnswerColumnName(name))))
            {
                var run = new RowPredicate[length];
                for (int j = 0; j < length; j++)
                    run[j] = predicates[i + j];
                (result ??= new Dictionary<string, AnswerPlan>(StringComparer.Ordinal))[name] =
                    new AnswerPlan(run, KeepValues: !dropped.Contains(name));
            }

            i = end - 1;
        }

        return result;

        static bool IsAnswerable(RowPredicate predicate) => predicate.ValueWise && predicate.Columns.Count == 1;
    }

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
        ulong[] selectionBuffer = ArrayPool<ulong>.Shared.Rent(wordCount);
        int[]? indexBuffer = null;
        bool owned = true;
        try
        {
            var selection = selectionBuffer.AsSpan(0, wordCount);
            int kept = ComputeSelection(batch, positioned.FirstRow, selection);
            if (kept == 0)
                return Discard(batch);
            if (kept == rows)
            {
                owned = false;
                return Whole(batch, positioned.FirstRow);
            }

            indexBuffer = ArrayPool<int>.Shared.Rent(rows);
            SelectionMask.ToIndices(selection, indexBuffer);
            var keptIndices = indexBuffer.AsSpan(0, kept);
            var output = Gather(batch, keptIndices, name => !_dropped.Contains(name));
            owned = false;
            ReleaseAfterGather(batch, output);
            return new SelectedRecordBatch(output, PositionsOf(positioned.FirstRow, keptIndices));
        }
        finally
        {
            if (owned)
                batch.Dispose();
            ArrayPool<ulong>.Shared.Return(selectionBuffer);
            if (indexBuffer is not null)
                ArrayPool<int>.Shared.Return(indexBuffer);
        }
    }

    /// <summary>
    /// Writes the rows of <paramref name="batch"/> (whose first row is at file position
    /// <paramref name="firstRow"/>) that the scan selects into <paramref name="selection"/>, one bit per row, and
    /// returns how many there are: excluded rows removed, then the predicates applied in order. The batch must
    /// hold every predicate's columns; it is only read.
    /// </summary>
    public int ComputeSelection(RecordBatch batch, long firstRow, Span<ulong> selection)
    {
        int rows = batch.Length;
        int wordCount = SelectionMask.WordCount(rows);
        var live = selection.Slice(0, wordCount);
        SelectionMask.SelectAll(live, rows);
        excluded?.ClearExcluded(firstRow, rows, live);
        if (filter is null)
            return SelectionMask.Count(live);

        ulong[] trueBuffer = ArrayPool<ulong>.Shared.Rent(wordCount);
        int[]? indexBuffer = null;
        try
        {
            var allTrue = trueBuffer.AsSpan(0, wordCount);
            live.CopyTo(allTrue);
            foreach (var predicate in filter.Predicates)
            {
                // A new condition: the rows answered NULL so far are out (all-true is always a subset of live).
                if (predicate.DropsEarlierNulls)
                    allTrue.CopyTo(live);

                // Answered from the column's dictionary: the batch holds its run's answers for every row (the Kleene
                // AND of the run's predicates, which is what asking them in turn decides, see AnswerPlan). Applied at
                // the run's first predicate to all of them, a row no longer live is unchanged (its live and all-true
                // bits are already clear), so this is the step for the live rows alone; the run's other predicates are
                // answered with it. The dictionary was evaluated whole, by every predicate of the run, so one that
                // cannot be evaluated at all has raised already, or the column was read as values.
                if (AnswerColumn(predicate, batch, out bool first) is { } answers)
                {
                    if (first)
                        SelectionMask.AndThreeValued(live, allTrue, rows, answers, default);
                    continue;
                }

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

                // Most rows still live, and a predicate that can answer every row without raising: asked about the
                // whole batch, with no gather. Rows no longer live are unchanged by its answers, as above.
                if (liveCount < rows && predicate.EvaluateWholeBatch is { } whole
                    && liveCount >= rows * wholeBatchLiveShare
                    && TryEvaluateWholeBatch(predicate, whole, batch, live, allTrue, rows))
                {
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

            allTrue.CopyTo(live);
            return SelectionMask.Count(live);
        }
        finally
        {
            ArrayPool<ulong>.Shared.Return(trueBuffer);
            if (indexBuffer is not null)
                ArrayPool<int>.Shared.Return(indexBuffer);
        }
    }

    /// <summary>The positions of the rows at <paramref name="keptIndices"/> (ascending) of a batch whose first row
    /// is at <paramref name="firstRow"/>: no explicit list when they are contiguous.</summary>
    internal static RowPositions PositionsOf(long firstRow, ReadOnlySpan<int> keptIndices)
    {
        int kept = keptIndices.Length;
        if (keptIndices[kept - 1] - keptIndices[0] + 1 == kept)
            return new RowPositions(firstRow + keptIndices[0], null);

        var builder = new Int64Array.Builder().Reserve(kept);
        foreach (int i in keptIndices)
            builder.Append(firstRow + i);
        return new RowPositions(firstRow + keptIndices[0], builder.Build());
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

    /// <summary>
    /// The smallest share of a batch's rows that must still be live for a predicate with
    /// <see cref="RowPredicate.EvaluateWholeBatch"/> to be asked about the whole batch rather than a gather of the
    /// live rows (#502 phase 5a). Above 1, never. A reader may set its own (<see cref="ParquetFileReader.WholeBatchLiveShare"/>).
    /// Measured (doc/row-level-filtering.md §24): the whole batch costs 0-9% more at 25% live or fewer, and saves
    /// 10-23% from 40% live up.
    /// </summary>
    internal const double DefaultWholeBatchLiveShare = 0.4;

    // Asks the predicate about every row of the batch; false when it declined, having changed nothing.
    private static bool TryEvaluateWholeBatch(
        RowPredicate predicate, Func<RecordBatch, BooleanArray?> whole, RecordBatch batch,
        Span<ulong> live, Span<ulong> allTrue, int rows)
    {
        var view = PredicateView(predicate, batch);
        if (whole(view) is not { } mask)
            return false;
        try
        {
            if (mask.Length != rows)
            {
                throw new InvalidOperationException(
                    $"A row predicate returned a mask of {mask.Length} rows for a batch of {rows}.");
            }

            SelectionMask.AndThreeValued(live, allTrue, rows, mask, default);
            return true;
        }
        finally
        {
            Release(mask, view);
        }
    }

    private static void EvaluateOverNoRows(RowPredicate predicate, RecordBatch batch)
    {
        var view = PredicateView(predicate, batch);
        var input = ArrowCompute.Take(view, view.Schema, ReadOnlySpan<int>.Empty);
        Release(Ask(predicate, input), input);
    }

    // The answers the reader put in place of the predicate's one column, when it answered its run from the column's
    // dictionary; `first` says whether the predicate begins that run.
    private BooleanArray? AnswerColumn(RowPredicate predicate, RecordBatch batch, out bool first)
    {
        first = false;
        if (Answerable is null || predicate.Columns.Count != 1
            || !Answerable.TryGetValue(predicate.Columns[0], out var plan))
        {
            return null;
        }

        int position = -1;
        for (int i = 0; i < plan.Predicates.Count; i++)
        {
            if (ReferenceEquals(plan.Predicates[i], predicate))
            {
                position = i;
                break;
            }
        }

        if (position < 0)
            return null;
        first = position == 0;

        // A returned column's answers come as a column of their own, absent when the chunk was read as values.
        string name = plan.KeepValues ? Data.DictionaryAnswer.AnswerColumnName(predicate.Columns[0]) : predicate.Columns[0];
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var field = batch.Schema.GetFieldByIndex(i);
            if (string.Equals(field.Name, name, StringComparison.Ordinal))
                return Data.DictionaryAnswer.IsAnswerField(field) ? (BooleanArray)batch.Column(i) : null;
        }

        return null;
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
    internal static void Release(BooleanArray mask, RecordBatch input)
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
    internal static void ReleaseAfterGather(RecordBatch batch, RecordBatch output)
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

    internal static void CollectBuffers(ArrayData data, HashSet<ReadOnlyMemory<byte>> into)
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

    internal static bool SharesAny(ArrayData data, HashSet<ReadOnlyMemory<byte>> shared)
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

    /// <summary>
    /// The rows at <paramref name="indices"/> of the columns <paramref name="keep"/> accepts (by field name), as a
    /// new batch. The input batch is left for the caller, who releases it with <see cref="ReleaseAfterGather"/>.
    /// </summary>
    internal static RecordBatch Gather(RecordBatch batch, ReadOnlySpan<int> indices, Func<string, bool> keep)
    {
        var fields = new List<Field>(batch.ColumnCount);
        var arrays = new List<IArrowArray>(batch.ColumnCount);
        try
        {
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                var field = batch.Schema.GetFieldByIndex(i);
                if (!keep(field.Name))
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
        if (_dropped.Count == 0)
            return batch;

        var fields = new List<Field>(batch.ColumnCount);
        var arrays = new List<IArrowArray>(batch.ColumnCount);
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var field = batch.Schema.GetFieldByIndex(i);
            if (_dropped.Contains(field.Name))
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

/// <summary>
/// The predicates the reader may answer from a column's dictionary, and whether the read also returns the column, so
/// its values must be built as well as the answers.
/// </summary>
/// <remarks>
/// <para><see cref="Predicates"/> is a run of adjacent predicates over the one column, in filter order (#502 phase 5b).
/// Each is evaluated over the dictionary, and an entry's answer is their Kleene AND: FALSE if any answers FALSE, else
/// NULL if any answers NULL, else TRUE. Asking them in turn decides the same: a row one answers FALSE for is dropped
/// whatever the others say, and a row answered NULL stays live but is never returned. Every predicate of the run is
/// evaluated over every entry, which the dictionary allows: an entry error falls back to asking the rows.</para>
/// </remarks>
internal sealed record AnswerPlan(IReadOnlyList<RowPredicate> Predicates, bool KeepValues);
