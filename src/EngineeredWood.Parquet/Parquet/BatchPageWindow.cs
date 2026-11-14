// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Parquet;

/// <summary>
/// The pages of a run of consecutive batches of one row group, fetched in one request, so that a read
/// returning several batches pays one round trip per window rather than one per batch (#622).
/// </summary>
/// <remarks>
/// <para>The window is planned by replaying, from where each column's decoded rows stand, the choice
/// <see cref="PagesToDecode"/> makes for every batch, so it holds exactly the pages the batches will
/// ask for: neither the pages between ranges nor pages already decoded. A column's pages are merged
/// into runs of adjacent pages, and a page map still waiting for its side pages gets them too.</para>
/// <para>Windows are cut at a budget of compressed bytes (side pages and any gaps between pages
/// included), at <see cref="MaxRangesPerWindow"/> ranges, and at <see cref="MaxBatchesPerWindow"/>
/// batches, but always hold at least one batch.</para>
/// </remarks>
internal sealed class BatchPageWindow : IDisposable
{
    /// <summary>How many compressed bytes one window may ask for, unless a single batch needs more.</summary>
    /// <remarks>
    /// Measured (BatchWindowAb, 1M rows, 33 MB): over a simulated store of 20 ms per GET and 100 MB/s,
    /// 8 MiB and 32 MiB save about the same (20-72%); on local disk 32 MiB cost 3-6%, as pages read
    /// long before they are decoded have left the cache, and 8 MiB is within noise.
    /// </remarks>
    internal const long DefaultBudgetBytes = 8L * 1024 * 1024;

    /// <summary>The window budget for a file from an object store (one that gives a concurrency hint).</summary>
    /// <remarks>
    /// Measured (#652, 12M lineitem rows in 6M-row groups of 128 MiB compressed, read one row group at a time over a
    /// simulated store of 20 ms per GET and 100 MB/s per request, 122,880-row batches): 8 MiB windows took 3.07 s in
    /// 758 GETs, 32 MiB 1.74 s in 152, against 1.85 s unbatched; the windows held 387 MiB at peak against 2 GiB
    /// unbatched. Local disk keeps <see cref="DefaultBudgetBytes"/>, where 32 MiB cost 3-6% (above).
    /// </remarks>
    internal const long ObjectStoreBudgetBytes = 32L * 1024 * 1024;

    /// <summary>
    /// How many ranges one window may ask for, unless a single batch needs more. A coalescing reader
    /// issues a GET per range it cannot merge, all at once; this bounds how many, as
    /// <see cref="MembershipPrefetch.MaxRangesPerWindow"/> does for its read-ahead.
    /// </summary>
    internal const int MaxRangesPerWindow = MembershipPrefetch.MaxRangesPerWindow;

    /// <summary>
    /// How many batches one window may plan. A batch inside pages already decoded adds no bytes, so
    /// without this a small <see cref="ParquetReadOptions.BatchSize"/> over well-compressed pages would
    /// plan millions of batches before returning the first. A window of such batches asks for nothing,
    /// so cutting it costs no request.
    /// </summary>
    internal const int MaxBatchesPerWindow = 1024;

    private readonly FileRange[] _ranges;
    private readonly IReadOnlyList<IMemoryOwner<byte>> _buffers;

    private BatchPageWindow(
        List<(int Start, int End)> batches, FileRange[] ranges, IReadOnlyList<IMemoryOwner<byte>> buffers,
        int nextSpan, int nextRow)
    {
        Batches = batches;
        _ranges = ranges;
        _buffers = buffers;
        NextSpan = nextSpan;
        NextRow = nextRow;
    }

    /// <summary>The window's batches, as row spans within the row group, in order. Empty past the last.</summary>
    public List<(int Start, int End)> Batches { get; }

    /// <summary>The span the batch after this window starts in.</summary>
    public int NextSpan { get; }

    /// <summary>The first row of the batch after this window.</summary>
    public int NextRow { get; }

    /// <summary>
    /// The pages a column decodes for the batch of rows [<paramref name="batchStart"/>,
    /// <paramref name="batchEnd"/>), given that its decoded rows end at <paramref name="endRow"/> (rows
    /// before the batch already skipped) and the page after them is <paramref name="nextPage"/>; or
    /// (-1, -1) when the decoded rows already cover the batch.
    /// </summary>
    public static (int Start, int End) PagesToDecode(
        ColumnPageMap map, int endRow, int nextPage, int batchStart, int batchEnd)
    {
        if (endRow >= batchEnd)
            return (-1, -1);

        int endPage = map.FindPageForRow(batchEnd - 1);

        // A batch that starts in rows already decoded and ends in pages not yet decoded spans two runs,
        // and would be copied. When the batch holds more rows than decoding its first page again
        // repeats, decode again from that page instead: the batch then lies in one run and is returned
        // without a copy.
        int startPage = nextPage;
        if (endRow > batchStart)
        {
            int batchFirstPage = map.FindPageForRow(batchStart);
            if (endRow - map.CumulativeRows[batchFirstPage] < batchEnd - batchStart)
                startPage = batchFirstPage;
        }
        else if (startPage >= map.Pages.Length || map.CumulativeRows[startPage] != batchStart)
        {
            // The batch begins past the rows decoded so far, after a gap between ranges: go straight
            // to its page, leaving the pages between unread.
            startPage = map.FindPageForRow(batchStart);
        }

        return (startPage, endPage);
    }

    /// <summary>
    /// Plans and reads the window whose first batch starts at row <paramref name="nextRow"/> of span
    /// <paramref name="nextSpan"/>.
    /// </summary>
    /// <param name="chunkOffsets">Each column chunk's offset in the file.</param>
    /// <param name="spans">The rows the read returns, as ascending row spans within the row group.</param>
    /// <param name="endRows">Where each column's decoded rows end now. Not changed.</param>
    /// <param name="nextPages">The page after each column's decoded rows now. Not changed.</param>
    /// <param name="batchRows">The rows of the batch starting at a row, given the end of its span.</param>
    /// <param name="heldColumns">The columns whose chunks the caller already holds whole, which the window asks
    /// nothing for; null for none.</param>
    public static async ValueTask<BatchPageWindow> ReadAsync(
        IRandomAccessFile file,
        ColumnPageMap[] pageMaps,
        long[] chunkOffsets,
        (int Start, int End)[] spans,
        int nextSpan,
        int nextRow,
        int[] endRows,
        int[] nextPages,
        Func<int, int, int> batchRows,
        long budgetBytes,
        bool[]? heldColumns,
        CancellationToken cancellationToken)
    {
        int columns = pageMaps.Length;
        endRows = (int[])endRows.Clone();
        nextPages = (int[])nextPages.Clone();

        // Each column's page runs, as (first, last) page indices, ascending and disjoint.
        var runs = new List<(int First, int Last)>[columns];
        for (int i = 0; i < columns; i++)
            runs[i] = [];

        var batches = new List<(int Start, int End)>();
        var planned = new (int Start, int End)[columns];
        long bytes = 0;
        int rangeCount = 0;
        while (nextSpan < spans.Length && batches.Count < MaxBatchesPerWindow)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var (_, spanEnd) = spans[nextSpan];
            if (nextRow >= spanEnd)
            {
                if (++nextSpan < spans.Length)
                    nextRow = spans[nextSpan].Start;
                continue;
            }

            int start = nextRow;
            int end = start + batchRows(start, spanEnd);

            // What the batch adds to the request: the bytes by which it widens the column's last run,
            // or a new run, with the side pages when it is the column's first; any gap between pages
            // inside a run is fetched with them, so it counts.
            long added = 0;
            int addedRanges = 0;
            for (int i = 0; i < columns; i++)
            {
                var map = pageMaps[i];
                planned[i] = PagesToDecode(map, Math.Max(endRows[i], start), nextPages[i], start, end);
                var (first, last) = planned[i];
                if (first < 0 || heldColumns?[i] == true)
                    continue;

                var columnRuns = runs[i];
                if (columnRuns.Count > 0 && first <= columnRuns[^1].Last + 1)
                {
                    var held = columnRuns[^1];
                    added += Span(map, Math.Min(held.First, first), Math.Max(held.Last, last))
                        - Span(map, held.First, held.Last);
                }
                else
                {
                    added += Span(map, first, last);
                    addedRanges++;
                    if (columnRuns.Count == 0 && map.PendingPrefixLength > 0)
                    {
                        added += map.PendingPrefixLength;
                        addedRanges++;
                    }
                }
            }

            if (batches.Count > 0
                && (bytes + added > budgetBytes || rangeCount + addedRanges > MaxRangesPerWindow))
            {
                break;
            }

            for (int i = 0; i < columns; i++)
            {
                var (first, last) = planned[i];
                if (first < 0)
                {
                    endRows[i] = Math.Max(endRows[i], start);
                    continue;
                }

                // A column whose chunk the caller holds asks for nothing, but its decoded rows move on.
                endRows[i] = pageMaps[i].CumulativeRows[last + 1];
                nextPages[i] = last + 1;
                if (heldColumns?[i] == true)
                    continue;

                var columnRuns = runs[i];
                if (columnRuns.Count > 0 && first <= columnRuns[^1].Last + 1)
                {
                    var held = columnRuns[^1];
                    columnRuns[^1] = (Math.Min(held.First, first), Math.Max(held.Last, last));
                }
                else
                {
                    columnRuns.Add((first, last));
                }
            }

            bytes += added;
            rangeCount += addedRanges;
            batches.Add((start, end));
            nextRow = end;
        }

        var ranges = new List<FileRange>();
        for (int i = 0; i < columns; i++)
        {
            var map = pageMaps[i];
            if (runs[i].Count == 0)
                continue;

            if (map.PendingPrefixLength > 0)
                ranges.Add(new FileRange(chunkOffsets[i], map.PendingPrefixLength));

            foreach (var (first, last) in runs[i])
            {
                long rangeStart = chunkOffsets[i] + map.Pages[first].Offset;
                long rangeEnd = chunkOffsets[i] + map.Pages[last].Offset + map.Pages[last].CompressedSize;
                ranges.Add(new FileRange(rangeStart, rangeEnd - rangeStart));
            }
        }

        // Sorted by offset, so a lookup can search them. Ranges of different chunks never overlap, nor
        // do a chunk's side pages and its data pages.
        ranges.Sort((a, b) => a.Offset.CompareTo(b.Offset));
        var sorted = ranges.ToArray();
        IReadOnlyList<IMemoryOwner<byte>> buffers = sorted.Length > 0
            ? await file.ReadRangesAsync(sorted, cancellationToken).ConfigureAwait(false)
            : [];
        return new BatchPageWindow(batches, sorted, buffers, nextSpan, nextRow);
    }

    /// <summary>The bytes from the start of page <paramref name="first"/> to the end of page <paramref name="last"/>.</summary>
    private static long Span(ColumnPageMap map, int first, int last) =>
        map.Pages[last].Offset + (long)map.Pages[last].CompressedSize - map.Pages[first].Offset;

    /// <summary>
    /// The bytes of <paramref name="range"/>, when the window holds all of them.
    /// </summary>
    public bool TryGet(FileRange range, out ReadOnlyMemory<byte> bytes)
    {
        // The last range starting at or before the one asked for.
        int lo = 0, hi = _ranges.Length - 1, found = -1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (_ranges[mid].Offset <= range.Offset)
            {
                found = mid;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        if (found >= 0)
        {
            var held = _ranges[found];
            if (range.Offset + range.Length <= held.Offset + held.Length)
            {
                bytes = _buffers[found].Memory.Slice(
                    checked((int)(range.Offset - held.Offset)), checked((int)range.Length));
                return true;
            }
        }

        bytes = default;
        return false;
    }

    public void Dispose()
    {
        for (int i = 0; i < _buffers.Count; i++)
            _buffers[i].Dispose();
    }
}
