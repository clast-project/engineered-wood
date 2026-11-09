// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.Parquet.Schema;
using EngineeredWood.Scans;

namespace EngineeredWood.Parquet;

public sealed partial class ParquetFileReader
{
    /// <summary>
    /// The columns of a selected read that decodes in two passes (#502 phase 1b): the predicates' columns first,
    /// then the other output columns only for the batches where a row was selected.
    /// </summary>
    /// <param name="Filter">The predicates' columns, decoded in the first pass.</param>
    /// <param name="OutputOnly">The output columns no predicate reads, decoded in the second.</param>
    /// <param name="FilterOutputs">The predicates' columns the read also returns.</param>
    /// <param name="OutputOrder">Every returned column, in the order a one-pass read returns them.</param>
    internal sealed record TwoPassColumns(
        IReadOnlyList<string> Filter,
        IReadOnlyList<string> OutputOnly,
        HashSet<string> FilterOutputs,
        IReadOnlyList<string> OutputOrder);

    /// <summary>
    /// The columns for a two-pass read, or null when the scan cannot be read so: no filter, no output column
    /// beyond the predicates' (one pass already decodes only those), or a column that is not a flat top-level
    /// column (the nested read decodes a row group whole; the batched read below is for flat columns).
    /// </summary>
    /// <remarks>
    /// Nor when a returned predicate column comes back as a type whose gather shares the source's buffers (a view
    /// or dictionary type, see <see cref="ArrowCompute.Take(IArrowArray, ReadOnlySpan{int})"/>): the first pass
    /// decodes it once for the whole row group and every result batch gathers from that one array, so a caller
    /// disposing one result would free buffers the next still shares.
    /// </remarks>
    internal static TwoPassColumns? TwoPassColumnsFor(
        SchemaDescriptor schema, IReadOnlyList<string>? readColumns, IReadOnlyCollection<string> dropped, RowFilter? filter,
        ParquetReadOptions? options = null)
    {
        if (filter is null)
            return null;

        var flat = new HashSet<string>(StringComparer.Ordinal);
        foreach (var child in schema.Root.Children)
        {
            if (child.IsLeaf && child.Element.RepetitionType != FieldRepetitionType.Repeated)
                flat.Add(child.Name);
        }

        var all = readColumns ?? schema.Root.Children.Select(c => c.Name).ToList();
        foreach (var name in all)
        {
            if (!flat.Contains(name))
                return null;
        }

        var filterColumns = new List<string>();
        var filterSet = new HashSet<string>(StringComparer.Ordinal);
        foreach (var predicate in filter.Predicates)
        {
            foreach (var name in predicate.Columns)
            {
                if (filterSet.Add(name))
                    filterColumns.Add(name);
            }
        }

        var outputOrder = all.Where(n => !dropped.Contains(n)).ToList();
        var outputOnly = outputOrder.Where(n => !filterSet.Contains(n)).ToList();
        if (outputOnly.Count == 0)
            return null;

        var filterOutputs = new HashSet<string>(outputOrder.Where(filterSet.Contains), StringComparer.Ordinal);
        foreach (var column in schema.Columns)
        {
            if (column.Path.Count == 1 && filterOutputs.Contains(column.Path[0])
                && GatherShares(Data.ArrowSchemaConverter.ToArrowField(column, options).DataType))
            {
                return null;
            }
        }

        return new TwoPassColumns(filterColumns, outputOnly, filterOutputs, outputOrder);
    }

    // Whether ArrowCompute.Take shares buffers with its source for this type, rather than copying.
    private static bool GatherShares(Apache.Arrow.Types.IArrowType type) => type switch
    {
        Apache.Arrow.Types.StringViewType or Apache.Arrow.Types.BinaryViewType => true,
        Apache.Arrow.Types.DictionaryType => true,
        Apache.Arrow.ExtensionType extension => GatherShares(extension.StorageType),
        _ => false,
    };

    /// <summary>Whether <paramref name="plan"/> is read in two passes: only a whole row group, and only when the caller set no byte limit on batches, which would cut
    /// the two passes' batches differently.</summary>
    /// <remarks>Pruning by the page index hands back the whole group as one range, not null, when it narrows
    /// nothing; that is still the whole group.</remarks>
    /// <remarks>Nor a row group with a chunk larger than one Arrow array can hold: the first pass decodes its
    /// predicate columns whole, where a one-pass read splits such a chunk into batches.</remarks>
    private bool ReadsInTwoPasses(RowGroupPlan plan, TwoPassColumns? columns, Metadata.RowGroup rowGroup) =>
        TwoPassReads && columns is not null && _options.MaxBatchByteSize is not > 0
        && (plan.Ranges is null
            || (plan.Ranges.Count == 1 && plan.Ranges[0].Start == 0 && plan.Ranges[0].End == rowGroup.NumRows))
        && rowGroup.Columns.All(c => c.MetaData is not { } m || m.TotalUncompressedSize < TwoPassMaxChunkBytes);

    /// <summary>The largest column chunk, uncompressed, the first pass decodes whole: well under what one Arrow
    /// array can address, as the batched read's own implicit cap is.</summary>
    internal const long TwoPassMaxChunkBytes = 1L << 30;

    /// <summary>Whether a selected read may decode in two passes; false reads every row group in one, as
    /// phase 1a did. For tests and A/B measurement.</summary>
    internal bool TwoPassReads { get; set; } = true;

    /// <summary>
    /// The block of rows a two-pass read decides to decode or not as one: a block with no selected row has its
    /// output columns skipped. Coarser blocks skip less; finer ones cut more ranges, and a range's pages are
    /// decoded whole, so a page split between two ranges is decoded twice.
    /// </summary>
    internal const int TwoPassBlockRows = 8192;

    /// <summary>
    /// The largest share of a row group's rows that may lie in blocks holding a selected row for the second pass
    /// to decode only those blocks. Above it, decoding the output columns over ranges costs more than it saves
    /// (measured: the batched read over a dense selection was 40-170% slower than one whole-group decode), so the
    /// row group's output columns are decoded whole instead.
    /// </summary>
    internal const double TwoPassMaxCoveredShare = 0.5;

    /// <summary>
    /// Reads one planned row group in two passes. The first decodes the predicates' columns over the whole row
    /// group and works out its selection. The second decodes the other output columns: only over the blocks of
    /// <see cref="TwoPassBlockRows"/> rows holding a selected row when those are few enough
    /// (<see cref="TwoPassMaxCoveredShare"/>), otherwise over the whole group, as a one-pass read does. Its bytes
    /// are requested when the first pass starts, alongside the first pass's own, so that skipping a block saves
    /// its decoding without costing a round trip (doc/row-level-filtering.md §11, F5).
    /// </summary>
    private async IAsyncEnumerable<SelectedRecordBatch> ReadTwoPassAsync(
        RowGroupPlan plan,
        TwoPassColumns columns,
        BatchSelector selector,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        var outputCtx = await PrepareRowGroupAsync(plan.Index, columns.OutputOnly, cancellationToken).ConfigureAwait(false);
        int groupRows = outputCtx.RowCount;
        await using var prefetched = new PrefetchedFile(_file, outputCtx.Ranges, cancellationToken);

        // Pass 1: the predicates' columns, the whole row group as one batch.
        RecordBatch? first = null;
        await foreach (var batch in ReadBatchesAsync(
                           plan.Index, columns.Filter, ranges: null, offsetIndexes: null, cancellationToken,
                           batchRows: groupRows).ConfigureAwait(false))
        {
            if (first is not null)
            {
                batch.Dispose();
                throw new InvalidOperationException($"Row group {plan.Index} was read as more than one batch.");
            }

            first = batch;
        }

        if (first is null || first.Length == 0)
        {
            first?.Dispose();
            yield break;
        }

        // Buffers the returned predicate columns' gathered rows still share with `first`, and the columns of
        // `first` handed to a result whole (every row kept); see the finally.
        var shared = new HashSet<ReadOnlyMemory<byte>>();
        var moved = new bool[first.ColumnCount];
        try
        {
            var selection = new ulong[SelectionMask.WordCount(groupRows)];
            int kept = selector.ComputeSelection(first, plan.GroupStart, selection);
            if (kept == 0)
                yield break;

            // Pass 2: only the blocks holding a selected row, when they are few enough; otherwise everything.
            var ranges = SelectedBlocks(selection, groupRows, out long covered);
            IReadOnlyList<RowRange>? passTwoRanges = covered <= groupRows * TwoPassMaxCoveredShare ? ranges : null;

            int range = 0;
            long cursor = passTwoRanges?[0].Start ?? 0;
            await foreach (var batch in ReadBatchesAsync(
                               plan.Index, columns.OutputOnly, passTwoRanges, offsetIndexes: null, cancellationToken,
                               io: prefetched).ConfigureAwait(false))
            {
                int start = checked((int)cursor);
                cursor += batch.Length;
                if (passTwoRanges is not null && cursor == passTwoRanges[range].End && range + 1 < passTwoRanges.Count)
                    cursor = passTwoRanges[++range].Start;

                if (SecondPass(batch, start, selection, first, plan.GroupStart, columns, shared, moved) is { } selected)
                    yield return selected;
            }
        }
        finally
        {
            // `first` goes, except the returned predicate columns whose buffers a gathered result still shares
            // (a view column's data buffers): those are left to their buffers' finalizers.
            // A column handed to a result whole is the result's now, and may already be disposed: not touched.
            for (int i = 0; i < first.ColumnCount; i++)
            {
                if (moved[i])
                    continue;
                var column = first.Column(i);
                if (!BatchSelector.SharesAny(column.Data, shared))
                    column.Dispose();
            }
        }
    }

    /// <summary>
    /// The row ranges of the blocks of <see cref="TwoPassBlockRows"/> rows that hold a selected row, adjacent
    /// blocks merged; <paramref name="covered"/> is how many rows they span.
    /// </summary>
    internal static List<RowRange> SelectedBlocks(ReadOnlySpan<ulong> selection, int rows, out long covered)
    {
        var ranges = new List<RowRange>();
        covered = 0;
        const int wordsPerBlock = TwoPassBlockRows / 64;
        for (int block = 0; block * TwoPassBlockRows < rows; block++)
        {
            int firstWord = block * wordsPerBlock;
            int endWord = Math.Min(firstWord + wordsPerBlock, selection.Length);
            bool any = false;
            for (int w = firstWord; w < endWord && !any; w++)
                any = selection[w] != 0;
            if (!any)
                continue;

            long start = (long)block * TwoPassBlockRows;
            long end = Math.Min(start + TwoPassBlockRows, rows);
            covered += end - start;
            if (ranges.Count > 0 && ranges[^1].End == start)
                ranges[^1] = ranges[^1] with { End = end };
            else
                ranges.Add(new RowRange(start, end));
        }

        return ranges;
    }

    /// <summary>
    /// The selected rows of a second-pass batch (the output columns no predicate reads, rows
    /// [<paramref name="start"/>, start + its length) of the group), joined with the same rows of the returned
    /// predicate columns from <paramref name="first"/>, in the order a one-pass read returns them; null when the
    /// batch holds no selected row. Takes ownership of <paramref name="batch"/>; adds to
    /// <paramref name="shared"/> the buffers the result shares with <paramref name="first"/>.
    /// </summary>
    private static SelectedRecordBatch? SecondPass(
        RecordBatch batch, int start, ulong[] selection, RecordBatch first, long groupStart,
        TwoPassColumns columns, HashSet<ReadOnlyMemory<byte>> shared, bool[] moved)
    {
        int rows = batch.Length;
        int[] indices = ArrayPool<int>.Shared.Rent(Math.Max(1, rows));
        RecordBatch owned = batch; // released on failure: the batch, or once gathered, its copy
        RecordBatch? predicateColumns = null;
        bool done = false, passedThrough = false;
        try
        {
            int kept = SelectedIn(selection, start, rows, indices);
            if (kept == 0)
                return null;

            var local = indices.AsSpan(0, kept);
            RecordBatch output = batch;
            if (kept < rows)
            {
                output = BatchSelector.Gather(batch, local, static _ => true);
                BatchSelector.ReleaseAfterGather(batch, output);
                owned = output;
            }

            if (columns.FilterOutputs.Count > 0 && kept == rows && start == 0 && rows == first.Length)
            {
                // Every row of the whole group is kept: the predicate columns are returned as they are, as a
                // one-pass read returns such a batch, rather than gathered row by row. They move to the result
                // (`moved`), so the caller's release of `first` leaves them alone.
                var fieldsKept = new List<Field>();
                var arraysKept = new List<IArrowArray>();
                for (int i = 0; i < first.ColumnCount; i++)
                {
                    var field = first.Schema.GetFieldByIndex(i);
                    if (!columns.FilterOutputs.Contains(field.Name))
                        continue;
                    fieldsKept.Add(field);
                    arraysKept.Add(first.Column(i));
                    moved[i] = true;
                }

                predicateColumns = new RecordBatch(new Apache.Arrow.Schema(fieldsKept, null), arraysKept, rows);
                passedThrough = true;
            }
            else if (columns.FilterOutputs.Count > 0)
            {
                // The same rows of the predicate columns, which hold the whole group: shifted by `start`.
                int[] absolute = new int[kept];
                for (int i = 0; i < kept; i++)
                    absolute[i] = start + local[i];
                predicateColumns = BatchSelector.Gather(first, absolute, columns.FilterOutputs.Contains);
                for (int i = 0; i < predicateColumns.ColumnCount; i++)
                    BatchSelector.CollectBuffers(predicateColumns.Column(i).Data, shared);
            }

            var fields = new List<Field>(columns.OutputOrder.Count);
            var arrays = new List<IArrowArray>(columns.OutputOrder.Count);
            foreach (var name in columns.OutputOrder)
            {
                var source = columns.FilterOutputs.Contains(name) ? predicateColumns! : output;
                int i = source.Schema.GetFieldIndex(name);
                fields.Add(source.Schema.GetFieldByIndex(i));
                arrays.Add(source.Column(i));
            }

            var joined = new RecordBatch(new Apache.Arrow.Schema(fields, output.Schema.Metadata), arrays, kept);
            var result = new SelectedRecordBatch(joined, BatchSelector.PositionsOf(groupStart + start, local));
            done = true;
            return result;
        }
        finally
        {
            ArrayPool<int>.Shared.Return(indices);
            if (!done)
            {
                // A gathered copy is ours; the group's own columns (every row kept) stay with `first`'s release.
                if (!passedThrough)
                    predicateColumns?.Dispose();
                else
                    System.Array.Clear(moved, 0, moved.Length); // never reached a result: `first`'s release has them
                owned.Dispose();
            }
        }
    }

    // The positions, relative to `start`, of the selected rows among [start, start + rows) of the group's
    // selection, written to `into`; returns how many.
    private static int SelectedIn(ulong[] selection, int start, int rows, int[] into)
    {
        int n = 0;
        int end = start + rows;
        for (int w = start >> 6; w <= (end - 1) >> 6; w++)
        {
            ulong bits = selection[w];
            int baseRow = w << 6;
            if (baseRow < start)
                bits &= ulong.MaxValue << (start - baseRow);
            if (baseRow + 64 > end)
                bits &= ulong.MaxValue >> (baseRow + 64 - end);
            while (bits != 0)
            {
                into[n++] = baseRow + System.Numerics.BitOperations.TrailingZeroCount(bits) - start;
                bits &= bits - 1;
            }
        }

        return n;
    }

    /// <summary>
    /// A two-pass read as one unit of a parallel read: reserves scan memory as
    /// <see cref="ReadPlannedAsync(RowGroupPlan, IReadOnlyList{string}?, ScanReservation, DecodedGrowth, CancellationToken)"/>
    /// does, holding the row group's compressed bytes for the whole read.
    /// </summary>
    private async IAsyncEnumerable<SelectedRecordBatch> ReadTwoPassAsync(
        RowGroupPlan plan,
        IReadOnlyList<string>? readColumns,
        TwoPassColumns columns,
        BatchSelector selector,
        ScanReservation reservation,
        DecodedGrowth growth,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var (uncompressed, compressed) = PlannedBytes(metadata.RowGroups[plan.Index], schema, readColumns, plan.Ranges);
        reservation.SetInitialEstimate(growth.Apply(uncompressed));

        var e = ReadTwoPassAsync(plan, columns, selector, cancellationToken).GetAsyncEnumerator(cancellationToken);
        try
        {
            await reservation.HoldAsync(compressed, cancellationToken).ConfigureAwait(false);
            while (true)
            {
                await reservation.ReserveNextAsync(cancellationToken).ConfigureAwait(false);
                if (!await e.MoveNextAsync().ConfigureAwait(false))
                    break;
                reservation.Observe(SelectedBytes(e.Current));
                yield return e.Current;
            }
        }
        finally
        {
            await e.DisposeAsync().ConfigureAwait(false);
            reservation.ReleaseHeld();
        }
    }

    /// <summary>A selected batch's size for the scan budget: its columns and its explicit positions.</summary>
    internal static long SelectedBytes(SelectedRecordBatch batch) =>
        ArrowSizes.EstimatedBytes(batch.Batch)
        + (batch.Positions.Explicit is { } positions ? positions.Length * (long)sizeof(long) : 0);
}
