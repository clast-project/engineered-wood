// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.IO;
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

    /// <summary>
    /// Whether <paramref name="plan"/> is read in two passes. Only a whole row group (pruning by the page index
    /// hands back the whole group as one range, not null, when it narrows nothing), and only when the caller set
    /// no byte limit on batches.
    /// </summary>
    /// <remarks>
    /// The first pass never decodes more rows at once than a one-pass read would. It follows
    /// <see cref="ParquetReadOptions.BatchSize"/> unless it must keep the predicate columns it returns, which it
    /// decodes for the whole group; a group a batch size would split is then read in one pass. And a group with a
    /// BYTE_ARRAY chunk past what one Arrow array can hold, which the one-pass read splits under its implicit
    /// budget (<see cref="HasChunkOverArrowLimit"/>), is read in one pass too.
    /// </remarks>
    /// <param name="proofs">Whether the scan's predicates carry statistics proofs (see <see cref="UsesTwoPasses"/>).</param>
    private bool ReadsInTwoPasses(RowGroupPlan plan, TwoPassColumns? columns, Metadata.RowGroup rowGroup, bool proofs) =>
        UsesTwoPasses(proofs) && columns is not null && _options.MaxBatchByteSize is not > 0
        && (plan.Ranges is null
            || (plan.Ranges.Count == 1 && plan.Ranges[0].Start == 0 && plan.Ranges[0].End == rowGroup.NumRows))
        && !rowGroup.Columns.Any(c => c.MetaData is { Type: PhysicalType.ByteArray } m
                                      && m.TotalUncompressedSize > Data.ByteArrayCapacity.MaxBytes)
        && (columns.FilterOutputs.Count == 0 || !SplitsGroup(rowGroup.NumRows));

    // Whether BatchSize cuts a group of `rows` rows into more than one batch.
    private bool SplitsGroup(long rows) => _options.BatchSize is int size && size > 0 && size < rows;

    /// <summary>
    /// Whether a selected read may decode in two passes: true or false forces it, for tests and A/B measurement;
    /// null (the default) decides by the file and the scan, see <see cref="UsesTwoPasses"/>.
    /// </summary>
    internal bool? TwoPassReads { get; set; }

    /// <summary>
    /// Whether a selected read of this file may go in two passes. By default, for a file on an object store (one that
    /// reports <see cref="IReadConcurrencyHint.PreferredConcurrentReads"/>, as the S3, Azure and GCS readers do,
    /// through the coalescing and known-length wrappers too), and for a local file when the scan's predicates carry
    /// statistics proofs (<paramref name="proofs"/>, <see cref="RowPredicate.Proof"/>).
    /// </summary>
    /// <remarks>
    /// Locally, two passes win 4-33% wherever a row group is not kept whole, and lose 12-31% on one kept whole, where
    /// the second pass reads everything the first did not and the two decode one after the other
    /// (doc/row-level-filtering.md §23). A proven row group is read in one pass without its predicate (§26), so that
    /// loss is gone where proofs are offered, and only there (decided 2026-11-10).
    /// </remarks>
    private bool UsesTwoPasses(bool proofs) =>
        TwoPassReads ?? ((_file as IReadConcurrencyHint)?.PreferredConcurrentReads is not null || proofs);

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
    /// <param name="reservation">A parallel read's reservation, or null for a serial read. The first pass's
    /// decoded predicate columns, kept for the whole group when the read returns them, are held against it, and
    /// every decoded batch's RAW size sets its estimate, as the one-pass read's raw batches do.</param>
    /// <param name="growth">A parallel read's decoded-size learning, or null; taught from a group decoded in one
    /// batch per pass, against <paramref name="plannedDecoded"/>, the estimate its first batch reserved
    /// (<see cref="DecodedBytes"/>).</param>
    /// <param name="filterIo">The predicate columns' chunks fetched ahead, or null to fetch them as pass 1 reads.
    /// Not disposed here.</param>
    /// <param name="outputIo">The output columns' chunks fetched ahead, or null to start fetching them here. Not
    /// disposed here.</param>
    private async IAsyncEnumerable<SelectedRecordBatch> ReadTwoPassAsync(
        RowGroupPlan plan,
        TwoPassColumns columns,
        BatchSelector selector,
        ScanReservation? reservation,
        DecodedGrowth? growth,
        long plannedDecoded,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken,
        PrefetchedFile? filterIo = null,
        PrefetchedFile? outputIo = null)
    {
        var outputCtx = await PrepareRowGroupAsync(plan.Index, columns.OutputOnly, cancellationToken).ConfigureAwait(false);
        int groupRows = outputCtx.RowCount;
        await using var ownPrefetch = outputIo is null ? new PrefetchedFile(_file, outputCtx.Ranges, cancellationToken) : null;
        var prefetched = outputIo ?? ownPrefetch!;

        // Pass 1: the predicates' columns, in BatchSize batches unless the returned ones must be kept for the
        // whole group (then one batch, which eligibility allowed only where BatchSize would not split it).
        bool keep = columns.FilterOutputs.Count > 0;
        var selection = new ulong[SelectionMask.WordCount(groupRows)];
        RecordBatch? first = null;
        bool[] moved = [];
        long decoded = 0;
        int firstPassBatches = 0, secondPassBatches = 0;
        try
        {
            long kept = 0;
            int row = 0;
            await foreach (var batch in ReadBatchesAsync(
                               // The OffsetIndexes planning read with the page index, reused as ReadPlannedAsync
                               // does: a first pass cut into batches builds page maps from them, and would otherwise
                               // fetch them again.
                               plan.Index, columns.Filter, ranges: null, plan.OffsetIndexes, cancellationToken,
                               io: filterIo,
                               batchRows: keep || !SplitsGroup(groupRows) ? groupRows : null,
                               answers: selector.Answerable).ConfigureAwait(false))
            {
                int start = row;
                row += batch.Length;
                firstPassBatches++;
                long size = ArrowSizes.EstimatedBytes(batch);
                decoded += size;
                reservation?.Observe(size);
                try
                {
                    if (start == 0 && batch.Length == groupRows)
                    {
                        kept += selector.ComputeSelection(batch, plan.GroupStart, selection);
                    }
                    else
                    {
                        var words = new ulong[SelectionMask.WordCount(batch.Length)];
                        kept += selector.ComputeSelection(batch, plan.GroupStart + start, words);
                        OrInto(selection, start, words, batch.Length);
                    }
                }
                catch
                {
                    batch.Dispose();
                    throw;
                }

                if (keep && first is null && start == 0 && batch.Length == groupRows)
                {
                    first = batch;
                    moved = new bool[batch.ColumnCount];
                }
                else
                {
                    batch.Dispose();
                }
            }

            if (kept == 0)
                yield break;
            if (keep && first is null)
                throw new InvalidOperationException($"Row group {plan.Index}'s predicate columns were not read as one batch.");

            // What the first pass keeps for the whole group is held for as long.
            if (first is not null && reservation is not null)
                await reservation.HoldAsync(ArrowSizes.EstimatedBytes(first), cancellationToken).ConfigureAwait(false);

            // Pass 2: only the blocks holding a selected row, when they are few enough; otherwise everything. Page
            // maps come from the prefetched chunks' headers, never from an OffsetIndex read now: that would be the
            // dependent request the prefetch is there to avoid.
            var ranges = SelectedBlocks(selection, groupRows, out long covered);
            IReadOnlyList<RowRange>? passTwoRanges = covered <= groupRows * TwoPassMaxCoveredShare ? ranges : null;

            // A dense second pass read as one batch keeps the selected rows as it decodes (phase 2): each page drops
            // its deselected rows, skipping their values where the decoder can, so the batch comes back holding
            // only the selected rows, with nothing to gather. With decoders that skip, this beats the gather at
            // every selectivity measured (doc/row-level-filtering.md §15).
            byte[]? keepRows = passTwoRanges is null && !SplitsGroup(groupRows) && kept < groupRows
                ? KeepMask(selection, groupRows)
                : null;
            try
            {
                int range = 0;
                long cursor = passTwoRanges?[0].Start ?? 0;
                await foreach (var batch in ReadBatchesAsync(
                                   plan.Index, columns.OutputOnly, passTwoRanges, offsetIndexes: null, cancellationToken,
                                   io: prefetched, useOffsetIndex: false, keepRows: keepRows).ConfigureAwait(false))
                {
                    int start = checked((int)cursor);
                    cursor += batch.Length;
                    if (passTwoRanges is not null && cursor == passTwoRanges[range].End && range + 1 < passTwoRanges.Count)
                        cursor = passTwoRanges[++range].Start;

                    secondPassBatches++;
                    long size = ArrowSizes.EstimatedBytes(batch);
                    decoded += size;
                    reservation?.Observe(size);
                    var selected = keepRows is null
                        ? SecondPass(batch, start, selection, first, plan.GroupStart, columns, moved)
                        : SecondPass(batch, start: 0, selection, first, plan.GroupStart, columns, moved, groupRows);
                    if (selected is { } result)
                        yield return result;
                }
            }
            finally
            {
                if (keepRows is not null)
                    ArrayPool<byte>.Shared.Return(keepRows);
            }

            // Learnt only from a group each pass decoded whole in one batch, every row built: then `decoded` is the
            // group's. A second pass that kept rows as it decoded measured less than it decoded.
            if (growth is not null && passTwoRanges is null && keepRows is null && firstPassBatches == 1 && secondPassBatches == 1)
                growth.Observe(plannedDecoded, decoded);
        }
        finally
        {
            // `first` goes, except a column handed to a result whole (every row kept), which is the result's now and
            // may already be disposed. No gathered result shares its buffers: a returned predicate column of a type
            // whose gather would share them never takes this path (TwoPassColumnsFor).
            if (first is not null)
            {
                for (int i = 0; i < first.ColumnCount; i++)
                {
                    if (!moved[i])
                        first.Column(i).Dispose();
                }
            }
        }
    }

    /// <summary>
    /// Row group <paramref name="rowGroupIndex"/>'s rows that <paramref name="keepRows"/> keeps (one byte per row, 1
    /// to keep), of flat columns, cut as they are decoded: the second pass's path, for tests to compare against a
    /// whole read gathered afterwards.
    /// </summary>
    internal async ValueTask<RecordBatch> ReadRowGroupKeepingAsync(
        int rowGroupIndex, IReadOnlyList<string> columnNames, byte[] keepRows, CancellationToken cancellationToken = default)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        int rows = checked((int)metadata.RowGroups[rowGroupIndex].NumRows);
        RecordBatch? result = null;
        await foreach (var batch in ReadBatchesAsync(
                           rowGroupIndex, columnNames, ranges: null, offsetIndexes: null, cancellationToken,
                           batchRows: Math.Max(1, rows), keepRows: keepRows).ConfigureAwait(false))
        {
            result?.Dispose();
            result = batch;
        }

        return result ?? throw new InvalidOperationException($"Row group {rowGroupIndex} returned no batch.");
    }

    /// <summary>The selection as one byte per row (1 keep, 0 drop), from the pool: the decoders' form of it.</summary>
    private static byte[] KeepMask(ulong[] selection, int rows)
    {
        var keep = ArrayPool<byte>.Shared.Rent(rows);
        for (int r = 0; r < rows; r++)
            keep[r] = (byte)((selection[r >> 6] >> (r & 63)) & 1);
        return keep;
    }

    /// <summary>ORs the first <paramref name="rows"/> bits of <paramref name="source"/> into
    /// <paramref name="target"/> from bit <paramref name="offset"/>.</summary>
    private static void OrInto(ulong[] target, int offset, ReadOnlySpan<ulong> source, int rows)
    {
        int shift = offset & 63, baseWord = offset >> 6;
        for (int j = 0; j < SelectionMask.WordCount(rows); j++)
        {
            ulong v = source[j];
            int tail = rows - (j << 6);
            if (tail < 64)
                v &= (1UL << tail) - 1;
            target[baseWord + j] |= v << shift;
            if (shift != 0 && baseWord + j + 1 < target.Length)
                target[baseWord + j + 1] |= v >> (64 - shift);
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
    /// batch holds no selected row. Takes ownership of <paramref name="batch"/>. <paramref name="first"/> is null
    /// when the read returns none of the predicate columns.
    /// </summary>
    /// <param name="compactedFrom">When the batch's rows were already cut to the selection as they were decoded
    /// (it holds only the selected rows of a whole group of this many rows), that group's size; otherwise null,
    /// and the batch holds every row of its range.</param>
    private static SelectedRecordBatch? SecondPass(
        RecordBatch batch, int start, ulong[] selection, RecordBatch? first, long groupStart,
        TwoPassColumns columns, bool[] moved, int? compactedFrom = null)
    {
        int rows = compactedFrom ?? batch.Length;
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
            if (compactedFrom is not null)
            {
                if (batch.Length != kept)
                {
                    throw new InvalidOperationException(
                        $"The second pass kept {batch.Length} rows of a selection of {kept}.");
                }
            }
            else if (kept < rows)
            {
                output = BatchSelector.Gather(batch, local, static _ => true);
                BatchSelector.ReleaseAfterGather(batch, output);
                owned = output;
            }

            if (first is not null && kept == rows && start == 0 && rows == first.Length)
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
            else if (first is not null)
            {
                // The same rows of the predicate columns, which hold the whole group: shifted by `start`.
                int[] absolute = new int[kept];
                for (int i = 0; i < kept; i++)
                    absolute[i] = start + local[i];
                predicateColumns = BatchSelector.Gather(first, absolute, columns.FilterOutputs.Contains);
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
    /// <see cref="ReadPlannedAsync(RowGroupPlan, IReadOnlyList{string}?, ScanReservation, DecodedGrowth, CancellationToken, IReadOnlyDictionary{string, AnswerPlan}?)"/>
    /// does, holding the row group's compressed bytes for the whole read; the read itself holds what its first
    /// pass keeps and reports raw decoded sizes. As there, both passes' chunks start fetching once the hold is
    /// granted, before the read waits for room to decode (#829).
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
        var (_, compressed) = PlannedBytes(metadata.RowGroups[plan.Index], schema, readColumns, plan.Ranges);
        long planned = DecodedBytes(metadata.RowGroups[plan.Index], schema, readColumns, plan.Ranges);
        reservation.SetInitialEstimate(
            FirstBatchBytes(growth.Apply(planned), metadata.RowGroups[plan.Index], schema, readColumns, plan.Ranges));

        PrefetchedFile? filterIo = null, outputIo = null;
        IAsyncEnumerator<SelectedRecordBatch>? e = null;
        try
        {
            await reservation.HoldAsync(compressed, cancellationToken).ConfigureAwait(false);
            var filterCtx = await PrepareRowGroupAsync(plan.Index, columns.Filter, cancellationToken).ConfigureAwait(false);
            // Null when a chunk is too large for one buffer: pass 1 then reads it as it decodes, as before.
            filterIo = TryPrefetch(filterCtx.Ranges, cancellationToken);
            var outputCtx = await PrepareRowGroupAsync(plan.Index, columns.OutputOnly, cancellationToken).ConfigureAwait(false);
            // Null likewise: the read then starts its own output prefetch, as before #829.
            outputIo = TryPrefetch(outputCtx.Ranges, cancellationToken);
            e = ReadTwoPassAsync(plan, columns, selector, reservation, growth, planned, cancellationToken, filterIo, outputIo)
                .GetAsyncEnumerator(cancellationToken);
            while (true)
            {
                await reservation.ReserveNextAsync(cancellationToken).ConfigureAwait(false);
                if (!await e.MoveNextAsync().ConfigureAwait(false))
                    break;
                yield return e.Current;
            }
        }
        finally
        {
            if (e is not null)
                await e.DisposeAsync().ConfigureAwait(false);
            if (outputIo is not null)
                await outputIo.DisposeAsync().ConfigureAwait(false);
            if (filterIo is not null)
                await filterIo.DisposeAsync().ConfigureAwait(false);
            reservation.ReleaseHeld();
        }
    }

    /// <summary>A selected batch's size for the scan budget: its columns and its explicit positions.</summary>
    internal static long SelectedBytes(SelectedRecordBatch batch) =>
        ArrowSizes.EstimatedBytes(batch.Batch)
        + (batch.Positions.Explicit is { } positions ? positions.Length * (long)sizeof(long) : 0);
}
