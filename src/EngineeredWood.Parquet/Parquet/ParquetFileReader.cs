// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Text.RegularExpressions;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// Reads Parquet file metadata and schema via an <see cref="IRandomAccessFile"/>.
/// </summary>
public sealed partial class ParquetFileReader : IAsyncDisposable, IDisposable
{
    private static readonly byte[] Par1Magic = "PAR1"u8.ToArray();
    private const int MagicSize = 4;
    private const int FooterSuffixSize = 8; // 4-byte footer length + 4-byte magic
    private const int MinFileSize = MagicSize + FooterSuffixSize; // leading PAR1 + trailing 8

    /// <summary>
    /// Maximum size of a dictionary page Thrift header that could be missing from
    /// TotalCompressedSize due to the PARQUET-816 bug in parquet-mr &lt;= 1.2.8.
    /// </summary>
    private const int MaxDictHeaderPadding = 100;

    private readonly IRandomAccessFile _file;
    private readonly bool _ownsFile;
    private readonly ParquetReadOptions _options;
    private FileMetaData? _metadata;
    private SchemaDescriptor? _schema;
    private Apache.Arrow.Schema? _declaredArrowSchema;
    private bool _declaredArrowSchemaResolved;
    private long _fileLength;
    private bool _disposed;

    /// <summary>
    /// Creates a new reader over the given file.
    /// </summary>
    /// <param name="file">The random access file to read from.</param>
    /// <param name="ownsFile">If true, the file will be disposed when this reader is disposed.</param>
    /// <param name="options">Read options that control Arrow type mapping. Defaults to <see cref="ParquetReadOptions.Default"/>.</param>
    public ParquetFileReader(IRandomAccessFile file, bool ownsFile = true, ParquetReadOptions? options = null)
    {
        _file = file;
        _ownsFile = ownsFile;
        _options = options ?? ParquetReadOptions.Default;
    }

    /// <summary>
    /// Reads and caches the file metadata from the Parquet footer.
    /// </summary>
    public async ValueTask<FileMetaData> ReadMetadataAsync(
        CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        if (_metadata != null)
            return _metadata;

        long fileLength = await _file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        if (fileLength < MinFileSize)
            throw new ParquetFormatException(
                $"File is too small to be a valid Parquet file ({fileLength} bytes).");

        // Read the last 8 bytes: 4-byte footer length (LE) + 4-byte PAR1 magic
        using var suffixBuffer = await _file.ReadAsync(
            new FileRange(fileLength - FooterSuffixSize, FooterSuffixSize),
            cancellationToken).ConfigureAwait(false);

        var suffix = suffixBuffer.Memory.Span;

        // Validate trailing magic
        if (suffix[4] != Par1Magic[0] || suffix[5] != Par1Magic[1] ||
            suffix[6] != Par1Magic[2] || suffix[7] != Par1Magic[3])
            throw new ParquetFormatException("Invalid Parquet file: missing trailing PAR1 magic.");

        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(suffix);
        if (footerLength <= 0 || footerLength > fileLength - MinFileSize)
            throw new ParquetFormatException(
                $"Invalid Parquet footer length: {footerLength}.");

        // Read the footer (Thrift-encoded FileMetaData)
        long footerOffset = fileLength - FooterSuffixSize - footerLength;
        using var footerBuffer = await _file.ReadAsync(
            new FileRange(footerOffset, footerLength),
            cancellationToken).ConfigureAwait(false);

        _fileLength = fileLength;
        _metadata = MetadataDecoder.DecodeFileMetaData(footerBuffer.Memory.Span);
        return _metadata;
    }

    /// <summary>
    /// Gets the schema descriptor, building it from cached metadata.
    /// </summary>
    public async ValueTask<SchemaDescriptor> GetSchemaAsync(
        CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        if (_schema != null)
            return _schema;

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        _schema = new SchemaDescriptor(metadata.Schema);
        return _schema;
    }

    /// <summary>
    /// Gets the file's schema as an Arrow <see cref="Apache.Arrow.Schema"/>, without reading
    /// any data. This is the schema every <see cref="RecordBatch"/> from
    /// <see cref="ReadAllAsync"/> carries — and the only way to observe it for a file with no
    /// row groups, which yields no batches at all.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask<Apache.Arrow.Schema> GetArrowSchemaAsync(
        CancellationToken cancellationToken = default)
    {
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var declared = DeclaredArrowSchema(metadata);
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in ArrowSchemaConverter.ToArrowFields(schema.Root, _options))
            builder.Field(RestoreDeclaredUnits(field, declared));

        return builder.Build();
    }

    /// <summary>
    /// Reads a single row group and returns the data as an Arrow <see cref="RecordBatch"/>.
    /// </summary>
    /// <param name="rowGroupIndex">Zero-based index of the row group to read.</param>
    /// <param name="columnNames">
    /// Optional list of column names to read. If null, reads all columns.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An Arrow RecordBatch containing the requested columns.</returns>
    public async ValueTask<RecordBatch> ReadRowGroupAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default)
    {
        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        // Read all column chunks in parallel via ReadRangesAsync
        var buffers = await _file.ReadRangesAsync(ctx.Ranges, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            var results = new ColumnResult[ctx.Count];
            ForEachColumn(ctx.Count, i =>
            {
                results[i] = ColumnChunkReader.ReadColumn(
                    buffers[i].Memory.Span, ctx.Columns[i],
                    ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                    ctx.HasNestedColumns,
                    _options.PageChecksumValidation,
                    _options.FixedListFastPath,
                    _options.MaxPageUncompressedSize);
            });

            return AssembleRecordBatch(ctx, results);
        }
        finally
        {
            for (int i = 0; i < buffers.Count; i++)
                buffers[i].Dispose();
        }
    }

    /// <summary>
    /// Streams all row groups as an async sequence of <see cref="RecordBatch"/>.
    /// Each row group is read, decoded, and yielded one at a time.
    /// </summary>
    /// <param name="columnNames">
    /// Optional list of column names to read. If null, reads all columns.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of RecordBatches, one per row group.</returns>
    public async IAsyncEnumerable<RecordBatch> ReadAllAsync(
        IReadOnlyList<string>? columnNames = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken = default)
    {
        await foreach (var positioned in ReadPositionedAsync(_options.Filter, columnNames, cancellationToken)
            .ConfigureAwait(false))
        {
            yield return positioned.Batch;
        }
    }

    /// <summary>
    /// Streams the rows <paramref name="filter"/> might match, pruned as <see cref="ReadAllAsync"/> prunes
    /// under <see cref="ParquetReadOptions.Filter"/>, pairing each batch with the file position of its
    /// first row: for a layer that keys rows by position (deletion vectors, row ids, positional deletes)
    /// and so cannot count them itself once rows are skipped.
    /// </summary>
    /// <remarks>
    /// <para>Row groups are pruned by statistics, then by dictionary pages and Bloom filters as
    /// <see cref="ParquetReadOptions.FilterUseDictionaries"/> and
    /// <see cref="ParquetReadOptions.FilterUseBloomFilters"/> say; with
    /// <see cref="ParquetReadOptions.FilterUsePageIndex"/>, rows within a kept row group are pruned by
    /// the page index too. The result is a superset: rows are not filtered. Batches come in file order
    /// and never span skipped rows.</para>
    /// </remarks>
    /// <param name="filter">The predicate; null reads every row under
    /// <see cref="ParquetReadOptions.Filter"/> instead, as <see cref="ReadAllAsync"/> does.</param>
    /// <param name="columnNames">Optional list of column names to read. If null, reads all columns.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public IAsyncEnumerable<PositionedRecordBatch> ReadWithPositionsAsync(
        EngineeredWood.Expressions.Predicate? filter,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default) =>
        ReadPositionedAsync(filter ?? _options.Filter, columnNames, cancellationToken);

    private async IAsyncEnumerable<PositionedRecordBatch> ReadPositionedAsync(
        EngineeredWood.Expressions.Predicate? filter,
        IReadOnlyList<string>? columnNames,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);

        ParquetStatisticsAccessor? accessor = null;
        SchemaDescriptor? schema = null;
        Prefetches prefetch = default;
        if (filter is not null)
        {
            schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
            accessor = new ParquetStatisticsAccessor(schema);
            prefetch = CreatePrefetches(filter, metadata, schema, accessor);
        }

        // Page indexes are read a window of row groups at a time: one request, where one per row group
        // would pay a round trip each, and most for a group the index cannot narrow.
        var indexReadAhead = accessor is not null && _options.FilterUsePageIndex
            ? new Dictionary<long, byte[]>()
            : null;
        int readAheadEnd = 0;

        long nextGroupStart = 0;
        for (int i = 0; i < metadata.RowGroups.Count; i++)
        {
            // A skipped row group's rows still count towards the positions after it.
            long groupStart = nextGroupStart;
            nextGroupStart += metadata.RowGroups[i].NumRows;

            if (accessor is not null
                && !await MightMatchAsync(filter!, i, metadata, schema!, accessor, prefetch, cancellationToken)
                    .ConfigureAwait(false))
            {
                continue;
            }

            // Within a kept row group, the page index can narrow the rows further. Null means the whole
            // row group; the read below takes it as such.
            // The OffsetIndexes read with the ColumnIndexes serve the read's page maps.
            IReadOnlyList<RowRange>? ranges = null;
            Dictionary<long, byte[]>? offsetIndexes = null;
            if (indexReadAhead is not null)
            {
                if (i >= readAheadEnd)
                {
                    readAheadEnd = await ReadPageIndexesAheadAsync(
                            filter!, i, metadata, schema!, accessor!, columnNames, indexReadAhead, cancellationToken)
                        .ConfigureAwait(false);
                }

                (ranges, offsetIndexes) = await NarrowByPageIndexAsync(
                        filter!, i, metadata.RowGroups[i], schema!, accessor!,
                        readProjection: true, projection: columnNames, indexReadAhead, cancellationToken)
                    .ConfigureAwait(false);
                if (ranges.Count == 0)
                    continue;
            }

            // Batches follow the ranges in order and never span the gap between two, so a cursor over
            // the ranges places each one: its first row is the cursor, and the cursor moves on to the
            // next range once a range's rows have all been returned.
            int range = 0;
            long cursor = ranges is null ? 0 : ranges[0].Start;

            // Always via the batching entry point, even with no batch limit configured: it falls back to
            // the single-batch read itself when there is nothing to split, and it is where the implicit
            // cap for an over-sized chunk is decided. Routing around it here would put that decision in
            // two places and leave ReadAllAsync unable to read a file ReadRowGroupBatchesAsync can.
            await foreach (var batch in ReadBatchesAsync(i, columnNames, ranges, offsetIndexes, cancellationToken)
                .ConfigureAwait(false))
            {
                yield return new PositionedRecordBatch(batch, groupStart + cursor);
                cursor += batch.Length;
                if (ranges is not null && cursor == ranges[range].End && range + 1 < ranges.Count)
                    cursor = ranges[++range].Start;
            }
        }
    }

    /// <summary>
    /// Streams a single row group as a sequence of <see cref="RecordBatch"/> instances, each
    /// containing at most <see cref="ParquetReadOptions.BatchSize"/> rows. When
    /// <see cref="ParquetReadOptions.BatchSize"/> is null, yields a single batch containing
    /// the entire row group.
    /// </summary>
    /// <param name="rowGroupIndex">Zero-based index of the row group to read.</param>
    /// <param name="columnNames">
    /// Optional list of column names to read. If null, reads all columns.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of RecordBatches.</returns>
    public IAsyncEnumerable<RecordBatch> ReadRowGroupBatchesAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default) =>
        ReadBatchesAsync(rowGroupIndex, columnNames, ranges: null, offsetIndexes: null, cancellationToken);

    /// <summary>
    /// Streams only the rows of a row group that lie in <paramref name="ranges"/>, as
    /// <see cref="GetCandidateRowRangesAsync"/> returns them, reading and decoding only the pages
    /// those rows are in wherever a column has an OffsetIndex.
    /// </summary>
    /// <remarks>
    /// <para>Batches follow the ranges in order, and a batch never spans the gap between two ranges;
    /// a range longer than <see cref="ParquetReadOptions.BatchSize"/> or
    /// <see cref="ParquetReadOptions.MaxBatchByteSize"/> allows is split as
    /// <see cref="ReadRowGroupBatchesAsync"/> splits a row group. So the rows so far, counted across
    /// the ranges in order, locate each batch's first row in the file.</para>
    /// <para>A column without a usable OffsetIndex is read whole and cut to the ranges, and so is
    /// every column of a row group with a nested column: the rows are the same, only the saving is
    /// lost.</para>
    /// </remarks>
    /// <param name="rowGroupIndex">Zero-based index of the row group to read.</param>
    /// <param name="ranges">Ascending, disjoint, non-empty ranges of rows within the row group.</param>
    /// <param name="columnNames">Optional list of column names to read. If null, reads all columns.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException">A range is empty, outside the row group, or out of order.</exception>
    public async IAsyncEnumerable<RecordBatch> ReadRowRangesAsync(
        int rowGroupIndex,
        IReadOnlyList<RowRange> ranges,
        IReadOnlyList<string>? columnNames = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken = default)
    {
        if (ranges is null) throw new ArgumentNullException(nameof(ranges));
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        if (rowGroupIndex < 0 || rowGroupIndex >= metadata.RowGroups.Count)
            throw new ArgumentOutOfRangeException(nameof(rowGroupIndex), rowGroupIndex,
                $"The file has {metadata.RowGroups.Count} row groups.");

        long rows = metadata.RowGroups[rowGroupIndex].NumRows;
        long previousEnd = 0;
        for (int i = 0; i < ranges.Count; i++)
        {
            var range = ranges[i];
            if (range.Start < previousEnd || range.Start >= range.End || range.End > rows)
            {
                throw new ArgumentException(
                    $"Range {i} ({range.Start}, {range.End}) is empty, overlaps or precedes the one before it, "
                    + $"or ends past the row group's {rows} rows.", nameof(ranges));
            }

            previousEnd = range.End;
        }

        if (ranges.Count == 0)
            yield break;

        await foreach (var batch in ReadBatchesAsync(rowGroupIndex, columnNames, ranges, offsetIndexes: null, cancellationToken)
            .ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    /// <summary>
    /// The batched read behind <see cref="ReadRowGroupBatchesAsync"/> and <see cref="ReadRowRangesAsync"/>:
    /// the rows in <paramref name="ranges"/>, or the whole row group when that is null.
    /// <paramref name="offsetIndexes"/> holds OffsetIndex bytes already read, by file offset.
    /// </summary>
    private async IAsyncEnumerable<RecordBatch> ReadBatchesAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames,
        IReadOnlyList<RowRange>? ranges,
        IReadOnlyDictionary<long, byte[]>? offsetIndexes,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        int? batchSize = _options.BatchSize;
        long? maxBytes = _options.MaxBatchByteSize;

        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        // The rows to return, as int spans: the whole row group unless ranges narrow it.
        bool whole = ranges is null
            || (ranges.Count == 1 && ranges[0].Start == 0 && ranges[0].End == ctx.RowCount);
        (int Start, int End)[] spans = whole
            ? [(0, ctx.RowCount)]
            : ranges!.Select(r => (checked((int)r.Start), checked((int)r.End))).ToArray();

        // A BYTE_ARRAY chunk holding more bytes than one Arrow array can address cannot be returned as a
        // single batch at all. Splitting it is the only way to read it, so a caller who asked for no
        // particular batch size still gets one here rather than an error they can do nothing about. The
        // test is an over-estimate (see HasChunkOverArrowLimit) and splitting a chunk that would have fit
        // is harmless, so erring towards splitting is the right direction to be wrong in.
        //
        // Deliberately engaged only when a chunk is ALREADY over the limit, not at some margin below it:
        // a file that reads as one batch today keeps doing so, and the implicit cap can only turn a
        // failure into a success. Nested columns are excluded because the nested path decodes the whole
        // row group before slicing, so splitting does not help them (issue #157) — they still get the
        // NotSupportedException, which says so.
        bool implicitBudget = false;
        if (batchSize is not > 0 && maxBytes is not > 0
            && !ctx.HasNestedColumns
            && HasChunkOverArrowLimit(ctx))
        {
            maxBytes = ImplicitLargeChunkBatchBytes;
            implicitBudget = true;
        }

        // No delegation back to ReadRowGroupAsync when there is no batch limit: it would call
        // PrepareRowGroupAsync a second time for the row group already prepared above, and since
        // ReadAllAsync now always comes through here that is every row group of every ordinary read. With
        // no limit set nothing below narrows the batch, so the single-pass branch handles it unchanged.
        // Only a read of the whole row group can take the single-pass path: it has no way to skip rows.
        bool fitsInOneBatch = whole;
        if (batchSize is > 0 && ctx.RowCount > batchSize.Value)
            fitsInOneBatch = false;
        if (maxBytes is > 0)
        {
            long totalUncompressed = 0;
            for (int i = 0; i < ctx.Count; i++)
            {
                var meta = ctx.Chunks[i].MetaData!;
                totalUncompressed += meta.TotalUncompressedSize;
            }
            if (totalUncompressed > maxBytes.Value)
                fitsInOneBatch = false;
        }

        if (fitsInOneBatch)
        {
            // Row group fits in a single batch — use the optimised single-pass path.
            var buffers = await _file.ReadRangesAsync(ctx.Ranges, cancellationToken)
                .ConfigureAwait(false);
            try
            {
                var results = new ColumnResult[ctx.Count];
                ForEachColumn(ctx.Count, i =>
                {
                    results[i] = ColumnChunkReader.ReadColumn(
                        buffers[i].Memory.Span, ctx.Columns[i],
                        ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                        ctx.HasNestedColumns,
                        _options.PageChecksumValidation,
                        _options.FixedListFastPath,
                        _options.MaxPageUncompressedSize);
                });
                yield return AssembleRecordBatch(ctx, results);
            }
            finally
            {
                for (int i = 0; i < buffers.Count; i++)
                    buffers[i].Dispose();
            }
            yield break;
        }

        if (ctx.HasNestedColumns)
        {
            // Per-page subsetting slices each column's decoded array by row, which is only valid for
            // flat columns: a list/map leaf array is indexed by element, not row, and its records may
            // straddle both page and cross-column boundaries. Rather than reconstruct records from
            // partial pages, decode the whole row group once, assemble it (which also runs the
            // fixed-list fast path), then yield row-sliced views — Arrow's ArrayData.Slice adjusts
            // offsets and validity correctly for nested types. The trade-off is that the full row
            // group is decoded up front; true per-page subsetting for nested columns is future work.
            await foreach (var b in ReadNestedRowGroupInBatchesAsync(
                ctx, spans, batchSize, maxBytes, cancellationToken).ConfigureAwait(false))
            {
                yield return b;
            }
            yield break;
        }

        // Multi-batch path (flat columns): build page maps, then read only the pages needed per batch.
        //
        // Phase 1: Build each column's page map, from its OffsetIndex where it has one, otherwise by
        //          reading the whole chunk to scan its page headers.
        // Phase 2: For each batch, read and decode only the pages no earlier batch has decoded, and
        //          take the batch's rows from what is decoded.
        //
        // An OffsetIndex map estimates each page's uncompressed size, which the implicit budget
        // cannot use: it exists to keep a batch under the Arrow limit, so it reads the headers.
        var pageMaps = await BuildPageMapsAsync(ctx, useOffsetIndex: !implicitBudget, offsetIndexes, cancellationToken)
            .ConfigureAwait(false);

        // Pages are not cut where batches are, so a page usually holds rows for more than one batch.
        // Each column keeps the decoded rows that it has not yet returned, so a page is fetched and
        // decoded by the first batch that reaches it, and at most once more (see below), rather
        // than once for every batch it overlaps (#408). That cost 8x on dictionary-encoded data,
        // whose pages hold many rows.
        var cursors = new DecodedRows[ctx.Count];
        for (int i = 0; i < ctx.Count; i++)
            cursors[i] = new DecodedRows();

        try
        {
            foreach (var (spanStart, spanEnd) in spans)
            {
                int rowsEmitted = spanStart;
                while (rowsEmitted < spanEnd)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    int batchStartRow = rowsEmitted;
                    int actualBatchRows = ComputeBatchRowCount(
                        pageMaps, batchStartRow, spanEnd, batchSize, maxBytes);
                    int batchEndRow = batchStartRow + actualBatchRows;

                    // Rows before this batch that no batch returns: the gap since the last range.
                    foreach (var cursor in cursors)
                        cursor.SkipTo(batchStartRow);

                    // The pages each column still has to decode for this batch, read in one request.
                    var pageRanges = new List<FileRange>(ctx.Count);
                    // Each column's data range, and its side pages when the map still waits for them,
                    // as positions in pageRanges; -1 for none.
                    var dataSlots = new int[ctx.Count];
                    var prefixSlots = new int[ctx.Count];
                    dataSlots.AsSpan().Fill(-1);
                    prefixSlots.AsSpan().Fill(-1);
                    var startPages = new int[ctx.Count];
                    var endPages = new int[ctx.Count];
                    for (int i = 0; i < ctx.Count; i++)
                    {
                        var cursor = cursors[i];
                        if (cursor.EndRow >= batchEndRow)
                            continue;

                        int endPage = pageMaps[i].FindPageForRow(batchEndRow - 1);
                        endPages[i] = endPage;

                        // A batch that starts in rows already decoded and ends in pages not yet decoded
                        // spans two runs, and would be copied. When the batch holds more rows than
                        // decoding its first page again repeats, decode again from that page instead:
                        // the batch then lies in one run and is returned without a copy.
                        int startPage = cursor.NextPage;
                        if (cursor.EndRow > batchStartRow)
                        {
                            int batchFirstPage = pageMaps[i].FindPageForRow(batchStartRow);
                            if (cursor.EndRow - pageMaps[i].CumulativeRows[batchFirstPage] < actualBatchRows)
                                startPage = batchFirstPage;
                        }
                        else if (startPage >= pageMaps[i].Pages.Length
                            || pageMaps[i].CumulativeRows[startPage] != batchStartRow)
                        {
                            // The batch begins past the rows decoded so far, after a gap between
                            // ranges: go straight to its page, leaving the pages between unread.
                            startPage = pageMaps[i].FindPageForRow(batchStartRow);
                        }

                        startPages[i] = startPage;
                        var firstEntry = pageMaps[i].Pages[startPage];
                        var lastEntry = pageMaps[i].Pages[endPage];
                        long rangeStart = ctx.Ranges[i].Offset + firstEntry.Offset;
                        long rangeEnd = ctx.Ranges[i].Offset + lastEntry.Offset + lastEntry.CompressedSize;
                        dataSlots[i] = pageRanges.Count;
                        pageRanges.Add(new FileRange(rangeStart, rangeEnd - rangeStart));

                        // Side pages the map waits for come in the same request as its first pages.
                        if (pageMaps[i].PendingPrefixLength > 0)
                        {
                            prefixSlots[i] = pageRanges.Count;
                            pageRanges.Add(new FileRange(ctx.Ranges[i].Offset, pageMaps[i].PendingPrefixLength));
                        }
                    }

                    var pageBuffers = pageRanges.Count > 0
                        ? await _file.ReadRangesAsync(pageRanges, cancellationToken).ConfigureAwait(false)
                        : [];

                    try
                    {
                        var results = new ColumnResult[ctx.Count];
                        ForEachColumn(ctx.Count, i =>
                        {
                            var cursor = cursors[i];
                            if (dataSlots[i] >= 0)
                            {
                                if (prefixSlots[i] >= 0)
                                {
                                    pageMaps[i].CompleteSidePages(
                                        pageBuffers[prefixSlots[i]].Memory.Span, ctx.Columns[i],
                                        ctx.Chunks[i].MetaData!, _options.PageChecksumValidation,
                                        _options.MaxPageUncompressedSize);
                                }

                                var buffer = pageBuffers[dataSlots[i]];
                                int startPage = startPages[i];
                                var decoded = ColumnChunkReader.ReadColumnBatchFromSlice(
                                    buffer.Memory.Span,
                                    pageMaps[i].Pages[startPage].Offset,
                                    ctx.Columns[i],
                                    ctx.Chunks[i].MetaData!,
                                    pageMaps[i],
                                    startPage, endPages[i],
                                    ctx.LeafArrowFields[i],
                                    ctx.HasNestedColumns,
                                    _options.PageChecksumValidation,
                                    _options.MaxPageUncompressedSize);
                                // Decoded rows that continue the held ones are appended; otherwise the
                                // pages start at or before the next row to return, and replace them.
                                if (pageMaps[i].CumulativeRows[startPage] == cursor.EndRow)
                                    cursor.Append(decoded.Array, endPages[i] + 1);
                                else
                                    cursor.Restart(decoded.Array, pageMaps[i].CumulativeRows[startPage], endPages[i] + 1);
                            }

                            results[i] = new ColumnResult(cursor.Take(actualBatchRows), null, null);
                        });

                        yield return AssembleRecordBatch(
                            ctx with { RowCount = actualBatchRows }, results);
                    }
                    finally
                    {
                        for (int i = 0; i < pageBuffers.Count; i++)
                            pageBuffers[i].Dispose();
                    }

                    rowsEmitted += actualBatchRows;
                }
            }
        }
        finally
        {
            // Rows decoded but not returned, when the caller stops early.
            foreach (var cursor in cursors)
                cursor.Dispose();
        }
    }

    /// <summary>
    /// Batches a row group that contains nested columns by decoding it once and yielding row-sliced
    /// views. Batch sizing honours <paramref name="batchSize"/> and <paramref name="maxBytes"/> the
    /// same way the flat path does; only the decode strategy differs (whole chunk vs per-page). Only
    /// the rows in <paramref name="spans"/> are yielded, a batch never spanning two of them.
    /// </summary>
    private async IAsyncEnumerable<RecordBatch> ReadNestedRowGroupInBatchesAsync(
        RowGroupContext ctx,
        (int Start, int End)[] spans,
        int? batchSize,
        long? maxBytes,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        // Decode and assemble the entire row group once. The assembled arrays own their buffers, so
        // the file buffers can be released before any batch is yielded. The page maps serve only the
        // byte budget, so without one they are not built: counting the rows of a V1 page of a
        // repeated column means decompressing it.
        RecordBatch full;
        ColumnPageMap[] pageMaps = new ColumnPageMap[ctx.Count];
        bool needPageMaps = maxBytes is > 0;

        var buffers = await _file.ReadRangesAsync(ctx.Ranges, cancellationToken).ConfigureAwait(false);
        try
        {
            var results = new ColumnResult[ctx.Count];
            ForEachColumn(ctx.Count, i =>
            {
                if (needPageMaps)
                {
                    pageMaps[i] = PageMapBuilder.Build(
                        buffers[i].Memory.Span, ctx.Columns[i], ctx.Chunks[i].MetaData!,
                        maxPageUncompressedSize: _options.MaxPageUncompressedSize);
                }

                results[i] = ColumnChunkReader.ReadColumn(
                    buffers[i].Memory.Span, ctx.Columns[i],
                    ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                    ctx.HasNestedColumns,
                    _options.PageChecksumValidation,
                    _options.FixedListFastPath,
                    _options.MaxPageUncompressedSize);
            });
            full = AssembleRecordBatch(ctx, results);
        }
        finally
        {
            for (int i = 0; i < buffers.Count; i++)
                buffers[i].Dispose();
        }

        // Each batch holds its own reference to the decoded buffers; this one is let go at the end, or
        // when the caller stops early, so they are freed once the last batch is disposed.
        try
        {
            foreach (var (start, end) in spans)
            {
                int rowsEmitted = start;
                while (rowsEmitted < end)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    int actualBatchRows = ComputeBatchRowCount(
                        pageMaps, rowsEmitted, end, batchSize, maxBytes);

                    yield return SliceRecordBatch(full, rowsEmitted, actualBatchRows);
                    rowsEmitted += actualBatchRows;
                }
            }
        }
        finally
        {
            full.Dispose();
        }
    }

    /// <summary>
    /// Returns a row-range view of <paramref name="batch"/> covering
    /// <c>[offset, offset + length)</c>, zero-copy and offset-correct for nested (list/struct/map)
    /// arrays. Each column is sliced with <see cref="ArrayData.SliceShared"/>, which takes a reference
    /// on the buffers: the view stays valid after <paramref name="batch"/> and every other view are
    /// disposed. <see cref="ArrayData.Slice"/> takes none, so disposing one batch freed the buffers
    /// under the next.
    /// </summary>
    private static RecordBatch SliceRecordBatch(RecordBatch batch, int offset, int length)
    {
        // Apache.Arrow's factory (not EW's leaf-only Data.ArrowArrayFactory) reconstructs every
        // type, including struct/list/map, from sliced ArrayData.
        var columns = new IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
            columns[i] = Apache.Arrow.ArrowArrayFactory.BuildArray(batch.Column(i).Data.SliceShared(offset, length));

        return new RecordBatch(batch.Schema, columns, length);
    }

    /// <summary>
    /// Decodes the row group's columns in parallel, letting a single failure surface as itself.
    /// </summary>
    /// <remarks>
    /// <see cref="Parallel.For(int, int, Action{int})"/> wraps whatever a body throws in an
    /// <see cref="AggregateException"/>, so a diagnostic written for the caller arrives as
    /// "One or more errors occurred" with the real message one level down — which is how the oversized
    /// BYTE_ARRAY column of issue #157 reached a caller even once it had a message worth reading. One
    /// column failing is the ordinary case, so that one is rethrown in place, stack intact. Genuine
    /// multi-column failures keep the aggregate, which is the only honest shape for them.
    /// </remarks>
    private static void ForEachColumn(int count, Action<int> body)
    {
        try
        {
            Parallel.For(0, count, body);
        }
        catch (AggregateException ex) when (ex.InnerExceptions.Count == 1)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo
                .Capture(ex.InnerExceptions[0]).Throw();
        }
    }

    /// <summary>
    /// Batch budget used when a chunk is too large to decode into one Arrow array and the caller set no
    /// budget of their own. Small enough that the sum across every column in a batch stays far below the
    /// limit, large enough not to shred a big read into thousands of batches.
    /// </summary>
    private const long ImplicitLargeChunkBatchBytes = 256L * 1024 * 1024;

    /// <summary>
    /// Whether any BYTE_ARRAY chunk in this row group holds more uncompressed bytes than one Arrow array
    /// can address.
    /// </summary>
    /// <remarks>
    /// Uncompressed size is an UPPER bound on the decoded data — it also counts page headers, level bytes
    /// and any dictionary page — so this can fire for a chunk whose values would in fact have fitted, when
    /// the overhead is what carried it over. That is deliberate here: being wrong in this direction only
    /// splits a read that could have been done in one batch, which costs an extra batch boundary and
    /// changes nothing about the data. Nothing REFUSES on this estimate; the refusal is made by the
    /// decoder, which counts the actual bytes.
    /// </remarks>
    private static bool HasChunkOverArrowLimit(RowGroupContext ctx)
    {
        for (int i = 0; i < ctx.Count; i++)
        {
            var meta = ctx.Chunks[i].MetaData!;
            if (meta.Type == PhysicalType.ByteArray
                && meta.TotalUncompressedSize > Data.ByteArrayCapacity.MaxBytes)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Builds the page map of each of a flat row group's columns. Where
    /// <paramref name="useOffsetIndex"/> is set and a chunk has a usable OffsetIndex, the map comes
    /// from that, so the data pages are not read to find them. Every other chunk is read whole to
    /// scan its page headers.
    /// </summary>
    /// <param name="offsetIndexes">
    /// OffsetIndex bytes already read, by the index's file offset, as the page-index filter reads them
    /// with the ColumnIndexes it prunes by; the rest are read here.
    /// </param>
    private async ValueTask<ColumnPageMap[]> BuildPageMapsAsync(
        RowGroupContext ctx, bool useOffsetIndex, IReadOnlyDictionary<long, byte[]>? offsetIndexes,
        CancellationToken cancellationToken)
    {
        var pageMaps = new ColumnPageMap?[ctx.Count];
        if (useOffsetIndex)
            await BuildPageMapsFromOffsetIndexesAsync(ctx, pageMaps, offsetIndexes, cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < ctx.Count; i++)
        {
            if (pageMaps[i] is not null)
                continue;

            using var buffer = await _file.ReadAsync(ctx.Ranges[i], cancellationToken)
                .ConfigureAwait(false);
            pageMaps[i] = PageMapBuilder.Build(
                buffer.Memory.Span,
                ctx.Columns[i],
                ctx.Chunks[i].MetaData!,
                _options.PageChecksumValidation,
                _options.MaxPageUncompressedSize);
        }

        return pageMaps!;
    }

    /// <summary>
    /// Fills in <paramref name="pageMaps"/> for each chunk whose OffsetIndex can describe its pages,
    /// leaving null those without one, or with one that is unreadable or cannot be right.
    /// </summary>
    /// <remarks>
    /// <para>The first data page is where the index puts it, not <see cref="ColumnMetaData.DataPageOffset"/>:
    /// some writers leave <see cref="ColumnMetaData.DictionaryPageOffset"/> unset and point
    /// <see cref="ColumnMetaData.DataPageOffset"/> at the dictionary page (alltypes_tiny_pages.parquet).</para>
    /// <para>The bytes before it (a dictionary or FSST symbol-table page) are not read here when the index
    /// and <see cref="ColumnMetaData.DataPageOffset"/> agree where the first data page is: the map then
    /// waits for them (<see cref="ColumnPageMap.PendingPrefixLength"/>), and the first data read fetches
    /// them in the same request, saving a round trip. When the two disagree they are read now, one
    /// request for every such chunk, so that an index they contradict can still be set aside for a
    /// header scan.</para>
    /// </remarks>
    private async ValueTask BuildPageMapsFromOffsetIndexesAsync(
        RowGroupContext ctx, ColumnPageMap?[] pageMaps, IReadOnlyDictionary<long, byte[]>? prefetched,
        CancellationToken cancellationToken)
    {
        var columns = new List<int>(ctx.Count);
        var indexRanges = new List<FileRange>(ctx.Count);
        for (int i = 0; i < ctx.Count; i++)
        {
            var chunk = ctx.Chunks[i];

            // A map must not be built for a repeated column. (A chunk stored elsewhere never gets
            // here: PrepareRowGroupAsync refused it, unless the caller chose to ignore file_path.)
            if (ctx.Columns[i].MaxRepetitionLevel > 0)
                continue;
            if (chunk.OffsetIndexOffset is not { } offset || chunk.OffsetIndexLength is not { } length
                || offset < 0 || length <= 0 || offset > _fileLength - length)
            {
                continue;
            }

            columns.Add(i);
            indexRanges.Add(new FileRange(offset, length));
        }

        if (columns.Count == 0)
            return;

        // Only the indexes not already read.
        var toRead = new List<FileRange>(columns.Count);
        foreach (var range in indexRanges)
        {
            if (prefetched is null || !prefetched.ContainsKey(range.Offset))
                toRead.Add(range);
        }

        var read = await ReadPageIndexBytesAsync(toRead, cancellationToken).ConfigureAwait(false);
        var indexBytes = new byte[columns.Count][];
        for (int k = 0, next = 0; k < columns.Count; k++)
        {
            indexBytes[k] = prefetched is not null && prefetched.TryGetValue(indexRanges[k].Offset, out var bytes)
                ? bytes
                : read[next++];
        }

        var indexes = new OffsetIndex?[columns.Count];
        var prefixRanges = new List<FileRange>(columns.Count);
        var readNow = new bool[columns.Count];
        for (int k = 0; k < columns.Count; k++)
        {
            int i = columns[k];
            var range = ctx.Ranges[i];
            try
            {
                indexes[k] = MetadataDecoder.DecodeOffsetIndex(indexBytes[k]);
            }
            catch (ParquetFormatException)
            {
                continue;
            }

            if (indexes[k]!.PageLocations is not [var first, ..]
                || first.Offset < range.Offset || first.Offset > range.Offset + range.Length)
            {
                indexes[k] = null;
                continue;
            }

            if (first.Offset == range.Offset)
                continue; // no side pages

            if (first.Offset == ctx.Chunks[i].MetaData!.DataPageOffset)
            {
                // The index and the metadata agree: the side pages come with the first data read.
                pageMaps[i] = PageMapBuilder.BuildLayoutFromOffsetIndex(
                    checked((int)(first.Offset - range.Offset)), range.Offset, range.Offset + range.Length,
                    indexes[k]!, ctx.RowCount, ctx.Columns[i], ctx.Chunks[i].MetaData!);
                indexes[k] = null;
                continue;
            }

            prefixRanges.Add(new FileRange(range.Offset, first.Offset - range.Offset));
            readNow[k] = true;
        }

        var prefixes = prefixRanges.Count > 0
            ? await _file.ReadRangesAsync(prefixRanges, cancellationToken).ConfigureAwait(false)
            : [];
        try
        {
            int nextPrefix = 0;
            for (int k = 0; k < columns.Count; k++)
            {
                if (indexes[k] is not { } index)
                    continue;

                int i = columns[k];
                var range = ctx.Ranges[i];
                var prefix = readNow[k]
                    ? prefixes[nextPrefix++].Memory.Span
                    : ReadOnlySpan<byte>.Empty;

                pageMaps[i] = PageMapBuilder.BuildFromOffsetIndex(
                    prefix, range.Offset, range.Offset + range.Length, index, ctx.RowCount,
                    ctx.Columns[i], ctx.Chunks[i].MetaData!, _options.PageChecksumValidation,
                    _options.MaxPageUncompressedSize);
            }
        }
        finally
        {
            for (int i = 0; i < prefixes.Count; i++)
                prefixes[i].Dispose();
        }
    }

    /// <summary>
    /// Determines how many rows the next batch should contain, respecting both the
    /// row-count limit (<paramref name="batchSize"/>) and the byte-size limit
    /// (<paramref name="maxBytes"/>). Always returns at least 1 row (so that a
    /// single very large page doesn't stall progress).
    /// </summary>
    private static int ComputeBatchRowCount(
        ColumnPageMap[] pageMaps,
        int batchStartRow,
        int totalRows,
        int? batchSize,
        long? maxBytes)
    {
        int remaining = totalRows - batchStartRow;

        // No limit: a range read with neither set, which returns each range as one batch.
        if (batchSize is not > 0 && maxBytes is not > 0)
            return remaining;

        // Row-count limit only.
        if (maxBytes is not > 0)
            return Math.Min(batchSize!.Value, remaining);

        // Walk forward row-by-row (at page granularity) accumulating uncompressed
        // page sizes across all columns until the byte budget is exceeded.
        long budget = maxBytes.Value;
        int rowLimit = batchSize is > 0 ? Math.Min(batchSize.Value, remaining) : remaining;
        int candidateRows = 0;

        // Track per-column "last included page" to avoid double-counting pages
        // that span across iteration steps.
        int colCount = pageMaps.Length;
        Span<int> lastIncludedPage = colCount <= 64
            ? stackalloc int[colCount]
            : new int[colCount];
        for (int c = 0; c < colCount; c++)
            lastIncludedPage[c] = -1;

        long accumulated = 0;
        int cursor = batchStartRow;

        while (cursor < totalRows && candidateRows < rowLimit)
        {
            // For each column, find the page covering 'cursor' and add any
            // newly-entered pages' uncompressed sizes to the accumulator.
            long stepBytes = 0;
            int stepEndRow = totalRows; // will be narrowed to the earliest page boundary

            for (int c = 0; c < colCount; c++)
            {
                var map = pageMaps[c];
                int pageIdx = map.FindPageForRow(cursor);

                if (pageIdx != lastIncludedPage[c])
                {
                    stepBytes += map.Pages[pageIdx].UncompressedSize;
                    lastIncludedPage[c] = pageIdx;
                }

                // The end of this page determines the next boundary to check.
                int pageEndRow = map.CumulativeRows[pageIdx + 1];
                if (pageEndRow < stepEndRow)
                    stepEndRow = pageEndRow;
            }

            int stepRows = stepEndRow - cursor;

            // Check if adding this step would exceed the budget.
            if (accumulated + stepBytes > budget && candidateRows > 0)
                break; // stop before this step — we already have at least one page

            accumulated += stepBytes;
            candidateRows += stepRows;
            cursor = stepEndRow;
        }

        // Clamp to remaining rows and ensure at least 1.
        return Math.Max(1, Math.Min(candidateRows, remaining));
    }

    /// <summary>
    /// Reads each column sequentially: read I/O buffer, decode, release buffer before the next.
    /// Only one column's I/O buffer in memory at a time.
    /// </summary>
    internal async ValueTask<RecordBatch> ReadRowGroupIncrementalAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default)
    {
        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        var results = new ColumnResult[ctx.Count];

        for (int i = 0; i < ctx.Count; i++)
        {
            using var buffer = await _file.ReadAsync(ctx.Ranges[i], cancellationToken)
                .ConfigureAwait(false);

            results[i] = ColumnChunkReader.ReadColumn(
                buffer.Memory.Span, ctx.Columns[i],
                ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                ctx.HasNestedColumns,
                validateCrc: _options.PageChecksumValidation,
                fixedListFastPath: _options.FixedListFastPath,
                maxPageUncompressedSize: _options.MaxPageUncompressedSize);
        }

        return AssembleRecordBatch(ctx, results);
    }

    /// <summary>
    /// Reads all I/O buffers upfront, then decodes columns in parallel using multiple cores.
    /// </summary>
    internal async ValueTask<RecordBatch> ReadRowGroupParallelAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default)
    {
        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        var buffers = await _file.ReadRangesAsync(ctx.Ranges, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            var results = new ColumnResult[ctx.Count];

            ForEachColumn(ctx.Count, i =>
            {
                results[i] = ColumnChunkReader.ReadColumn(
                    buffers[i].Memory.Span, ctx.Columns[i],
                    ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                    ctx.HasNestedColumns,
                    _options.PageChecksumValidation,
                    _options.FixedListFastPath,
                    _options.MaxPageUncompressedSize);
            });

            return AssembleRecordBatch(ctx, results);
        }
        finally
        {
            for (int i = 0; i < buffers.Count; i++)
                buffers[i].Dispose();
        }
    }

    /// <summary>
    /// Whether <paramref name="chunk"/> is to be treated as stored in another file: it has a
    /// <c>file_path</c> (empty or not) and the caller has not chosen to ignore it.
    /// </summary>
    private bool IsStoredElsewhere(ColumnChunk chunk) =>
        chunk.FilePath is not null && _options.ColumnChunkFilePath == ColumnChunkFilePathKind.Refuse;

    /// <summary>
    /// Refuses a chunk stored in another file (#405). Its offsets are into that file, so reading
    /// them from this one returns unrelated bytes.
    /// </summary>
    private void ThrowIfStoredElsewhere(ColumnChunk chunk, ColumnDescriptor column)
    {
        if (!IsStoredElsewhere(chunk))
            return;

        throw new NotSupportedException(
            $"Column '{column.DottedPath}' is stored in another file ('{chunk.FilePath}'), which this reader does not " +
            "open; this file may be a summary (_metadata) file. If the data is in fact in this file, set " +
            $"{nameof(ParquetReadOptions)}.{nameof(ParquetReadOptions.ColumnChunkFilePath)} to " +
            $"{nameof(ColumnChunkFilePathKind)}.{nameof(ColumnChunkFilePathKind.Ignore)}.");
    }

    /// <summary>
    /// Reads and decodes columns in parallel with bounded concurrency.
    /// Each iteration reads its I/O buffer, decodes, and releases the buffer immediately.
    /// </summary>
    internal async ValueTask<RecordBatch> ReadRowGroupIncrementalParallelAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default)
    {
        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        var results = new ColumnResult[ctx.Count];

#if NET8_0_OR_GREATER
        await Parallel.ForEachAsync(
            Enumerable.Range(0, ctx.Count),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                CancellationToken = cancellationToken,
            },
            async (i, ct) =>
            {
                using var buffer = await _file.ReadAsync(ctx.Ranges[i], ct)
                    .ConfigureAwait(false);

                results[i] = ColumnChunkReader.ReadColumn(
                    buffer.Memory.Span, ctx.Columns[i],
                    ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                    ctx.HasNestedColumns,
                    _options.PageChecksumValidation,
                    _options.FixedListFastPath,
                    _options.MaxPageUncompressedSize);
            }).ConfigureAwait(false);
#else
        for (int i = 0; i < ctx.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var buffer = await _file.ReadAsync(ctx.Ranges[i], cancellationToken)
                .ConfigureAwait(false);

            results[i] = ColumnChunkReader.ReadColumn(
                buffer.Memory.Span, ctx.Columns[i],
                ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                ctx.HasNestedColumns,
                validateCrc: _options.PageChecksumValidation,
                fixedListFastPath: _options.FixedListFastPath,
                maxPageUncompressedSize: _options.MaxPageUncompressedSize);
        }
#endif

        return AssembleRecordBatch(ctx, results);
    }

    private async ValueTask<RowGroupContext> PrepareRowGroupAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames,
        CancellationToken cancellationToken)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);

        if (rowGroupIndex < 0 || rowGroupIndex >= metadata.RowGroups.Count)
            throw new ArgumentOutOfRangeException(nameof(rowGroupIndex),
                $"Row group index {rowGroupIndex} is out of range (0..{metadata.RowGroups.Count - 1}).");

        var rowGroup = metadata.RowGroups[rowGroupIndex];
        int rowCount = checked((int)rowGroup.NumRows);

        var (selectedColumns, selectedChunks) = ResolveColumns(
            schema, rowGroup, columnNames);

        var ranges = new FileRange[selectedChunks.Count];
        var leafArrowFields = new Field[selectedColumns.Count];
        bool hasParquet816Bug = HasParquet816Bug(metadata.CreatedBy);

        for (int i = 0; i < selectedChunks.Count; i++)
        {
            ThrowIfStoredElsewhere(selectedChunks[i], selectedColumns[i]);

            var colMeta = selectedChunks[i].MetaData
                ?? throw new ParquetFormatException(
                    $"Column chunk {i} has no inline metadata.");

            ranges[i] = GetColumnChunkRange(colMeta, _fileLength, hasParquet816Bug);

            leafArrowFields[i] = ArrowSchemaConverter.ToArrowField(selectedColumns[i], _options);
        }

        // Detect nested columns: check if any selected top-level schema child
        // is a non-leaf (struct, list, or map) or a bare repeated leaf.
        bool hasNestedColumns = false;
        SchemaNode? schemaRoot = null;
        Field[]? topLevelFields = null;

        // Build the effective schema root: full root when reading all columns,
        // or a pruned root containing only the selected top-level children.
        var effectiveRoot = columnNames == null
            ? schema.Root
            : PruneSchemaRoot(schema.Root, selectedColumns);

        foreach (var child in effectiveRoot.Children)
        {
            if (!child.IsLeaf ||
                child.Element.RepetitionType == FieldRepetitionType.Repeated)
            {
                hasNestedColumns = true;
                break;
            }
        }

        if (hasNestedColumns)
        {
            schemaRoot = effectiveRoot;
            topLevelFields = ArrowSchemaConverter.ToArrowFields(effectiveRoot, _options);
        }

        return new RowGroupContext(selectedColumns, selectedChunks, ranges,
            leafArrowFields, rowCount, hasNestedColumns, schemaRoot, topLevelFields);
    }

    private RecordBatch AssembleRecordBatch(RowGroupContext ctx, ColumnResult[] results)
    {
        if (!ctx.HasNestedColumns)
        {
            // Fast path: flat columns only
            var arrowArrays = new IArrowArray[results.Length];
            for (int i = 0; i < results.Length; i++)
                arrowArrays[i] = results[i].Array;

            var leafFields = (Field[])ctx.LeafArrowFields.Clone();
            RestoreDeclaredUnits(leafFields, arrowArrays);
            return BuildRecordBatch(leafFields, arrowArrays, ctx.RowCount);
        }

        // Nested path: group leaf arrays into Struct/List/Map arrays
        var leafArrays = new IArrowArray[results.Length];
        var leafDefLevels = new int[]?[results.Length];
        var leafRepLevels = new int[]?[results.Length];
        int[]? leafFixedLengths = null;
        for (int i = 0; i < results.Length; i++)
        {
            leafArrays[i] = results[i].Array;
            leafDefLevels[i] = results[i].DefinitionLevels;
            leafRepLevels[i] = results[i].RepetitionLevels;
            if (results[i].FixedListLength > 0)
                (leafFixedLengths ??= new int[results.Length])[i] = results[i].FixedListLength;
        }

        var topLevelArrays = NestedAssembler.Assemble(
            ctx.SchemaRoot!, leafArrays, leafDefLevels, leafRepLevels, leafFixedLengths, ctx.RowCount,
            _options);

        // NestedAssembler wraps only top-level variant columns; wrap variants nested inside a
        // struct/list/map so the arrays match the schema's (variant-aware) field types at every depth.
        // Skipped entirely without a registry — nothing produced VariantType fields to reconcile.
        if (_options.ExtensionRegistry is not null)
        {
            for (int i = 0; i < topLevelArrays.Length; i++)
                topLevelArrays[i] = Data.VariantNestedWrapper.Wrap(
                    topLevelArrays[i], ctx.TopLevelFields![i].DataType);
        }

        var topLevelFields = (Field[])ctx.TopLevelFields!.Clone();
        RestoreDeclaredUnits(topLevelFields, topLevelArrays);
        return BuildRecordBatch(topLevelFields, topLevelArrays, ctx.RowCount);
    }

    /// <summary>
    /// The Arrow schema the writer recorded under <c>ARROW:schema</c>, decoded once per file.
    /// Absent for a file no Arrow-aware writer produced, and for anything DuckDB wrote.
    /// </summary>
    private Apache.Arrow.Schema? DeclaredArrowSchema(Metadata.FileMetaData metadata)
    {
        if (!_declaredArrowSchemaResolved)
        {
            _declaredArrowSchema = Data.ArrowSchemaMetadata.Decode(metadata.KeyValueMetadata);
            _declaredArrowSchemaResolved = true;
        }

        return _declaredArrowSchema;
    }

    /// <summary>
    /// Restores the timestamp and time ZONE names the declared schema carries for
    /// <paramref name="field"/>, matched by name so that a projected read still finds its own field.
    /// </summary>
    /// <remarks>
    /// Zone names only. The unit stays as the file encodes it, because PyArrow reads its own
    /// <c>timestamp[s]</c> back as <c>timestamp[ms]</c> and restoring the unit here would make us
    /// the outlier against every file anyone else wrote.
    /// </remarks>
    private static Field RestoreDeclaredUnits(Field field, Apache.Arrow.Schema? declared)
    {
        var target = DeclaredField(field.Name, declared);
        if (target is null)
            return field;

        var type = Data.TimeUnitRescaler.ToDeclaredUnits(field.DataType, target.DataType);
        return ReferenceEquals(type, field.DataType)
            ? field
            : new Field(field.Name, type, field.IsNullable, field.Metadata);
    }

    private static Field? DeclaredField(string name, Apache.Arrow.Schema? declared)
    {
        if (declared is null)
            return null;

        foreach (var candidate in declared.FieldsList)
        {
            if (string.Equals(candidate.Name, name, StringComparison.Ordinal))
                return candidate;
        }

        return null;
    }

    /// <summary>
    /// Rewrites each assembled column into the units the writer originally declared. Parquet keeps
    /// only MILLIS, MICROS or NANOS and no zone name, so without this a <c>timestamp[s]</c> reads
    /// back as milliseconds and every zone reads back as UTC.
    /// </summary>
    private void RestoreDeclaredUnits(Field[] fields, IArrowArray[] arrays)
    {
        // Assembly always follows a metadata read, so the footer is cached by now.
        var declared = _metadata is null ? null : DeclaredArrowSchema(_metadata);
        if (declared is null)
            return;

        for (int i = 0; i < fields.Length; i++)
        {
            var target = DeclaredField(fields[i].Name, declared);
            if (target is null)
                continue;

            var restored = Data.TimeUnitRescaler.ToDeclaredUnits(arrays[i], target.DataType);
            if (ReferenceEquals(restored, arrays[i]))
                continue;

            arrays[i] = restored;
            fields[i] = new Field(
                fields[i].Name, restored.Data.DataType, fields[i].IsNullable, fields[i].Metadata);
        }
    }

    private static RecordBatch BuildRecordBatch(
        Field[] arrowFields, IArrowArray[] arrowArrays, int rowCount)
    {
        var builder = new Apache.Arrow.Schema.Builder();
        for (int i = 0; i < arrowFields.Length; i++)
            builder.Field(arrowFields[i]);

        return new RecordBatch(builder.Build(), arrowArrays, rowCount);
    }

    /// <summary>
    /// One column's decoded rows that the batched read has not yet returned, and the next page
    /// to decode. The rows start at the batch being assembled, so <see cref="EndRow"/> is where
    /// the decoded rows stop, counted from the start of the row group.
    /// </summary>
    /// <remarks>
    /// A caller may dispose one batch and go on reading the next, so a batch must hold its own
    /// reference to whatever it uses. Rows within one decoded run are returned as a
    /// reference-counted slice (<see cref="ArrayData.SliceShared"/>): no copy, and the run's
    /// buffers are freed when the last batch using them and this cursor have both let go. Only a
    /// batch that spans two runs is copied, and only its own rows are.
    /// </remarks>
    private sealed class DecodedRows : IDisposable
    {
        // Decoded runs, in row order, with how many of each run's rows have been returned.
        private readonly List<(IArrowArray Run, int Taken)> _runs = new();
        private int _startRow;
        private int _length;

        /// <summary>The first page not yet decoded.</summary>
        public int NextPage { get; private set; }

        /// <summary>One past the last decoded row.</summary>
        public int EndRow => _startRow + _length;

        /// <summary>Adds the rows of the pages up to <paramref name="nextPage"/>, just decoded.</summary>
        public void Append(IArrowArray decoded, int nextPage)
        {
            _runs.Add((decoded, 0));
            _length += decoded.Length;
            NextPage = nextPage;
        }

        /// <summary>
        /// Replaces the decoded rows with <paramref name="decoded"/>, the pages from the one that
        /// starts at <paramref name="startRow"/>, which re-decodes rows already held: those before
        /// the next row to return are skipped.
        /// </summary>
        public void Restart(IArrowArray decoded, int startRow, int nextPage)
        {
            Dispose();
            int skip = _startRow - startRow;
            _runs.Add((decoded, skip));
            _length = decoded.Length - skip;
            NextPage = nextPage;
        }

        /// <summary>
        /// Drops the rows before <paramref name="row"/>, which no batch returns: the gap before a range.
        /// Past the decoded rows, drops them all and waits at <paramref name="row"/> for pages that start
        /// at or before it.
        /// </summary>
        public void SkipTo(int row)
        {
            if (row <= _startRow)
                return;

            if (row >= EndRow)
            {
                Dispose();
                _startRow = row;
                _length = 0;
                return;
            }

            int skip = row - _startRow;
            _startRow = row;
            _length -= skip;
            while (skip > 0)
            {
                var (run, taken) = _runs[0];
                int rows = Math.Min(run.Length - taken, skip);
                Advance(rows);
                skip -= rows;
            }
        }

        /// <summary>Returns the next <paramref name="count"/> rows, as an array the caller owns.</summary>
        public IArrowArray Take(int count)
        {
            _startRow += count;
            _length -= count;

            var (first, firstTaken) = _runs[0];
            if (first.Length - firstTaken >= count)
            {
                var shared = Data.ArrowArrayFactory.BuildArray(first.Data.SliceShared(firstTaken, count));
                Advance(count);
                return shared;
            }

            // The rows span runs. Copy them, then let go of the runs they used up.
            var pieces = new List<IArrowArray>(2);
            var consumed = new List<IArrowArray>(2);
            int needed = count;
            while (needed > 0)
            {
                var (run, taken) = _runs[0];
                int rows = Math.Min(run.Length - taken, needed);
                pieces.Add(Data.ArrowArrayFactory.BuildArray(run.Data.Slice(taken, rows)));
                needed -= rows;
                if (taken + rows == run.Length)
                    consumed.Add(run);
                Advance(rows, dispose: false);
            }

            // The used-up runs are already out of _runs, so Dispose() would no longer reach them:
            // let go of them here even if the copy throws.
            try
            {
                return Concatenate(pieces);
            }
            finally
            {
                foreach (var run in consumed)
                    run.Dispose();
            }
        }

        public void Dispose()
        {
            foreach (var (run, _) in _runs)
                run.Dispose();
            _runs.Clear();
        }

        /// <summary>Marks <paramref name="rows"/> of the first run returned, dropping it once all are.</summary>
        private void Advance(int rows, bool dispose = true)
        {
            var (run, taken) = _runs[0];
            if (taken + rows < run.Length)
            {
                _runs[0] = (run, taken + rows);
                return;
            }

            _runs.RemoveAt(0);
            if (dispose)
                run.Dispose();
        }

        /// <summary>
        /// Copies <paramref name="pieces"/> into one array with buffers of its own. That is
        /// <see cref="EngineeredWood.Arrow.ArrowCompute.Concatenate"/>, except for a view type: its result points at the
        /// inputs' data buffers without holding them (apache/arrow-dotnet#443), and the runs are
        /// disposed right after, so view values are copied one by one.
        /// </summary>
        private static IArrowArray Concatenate(List<IArrowArray> pieces)
        {
            switch (pieces[0].Data.DataType)
            {
                case Apache.Arrow.Types.StringViewType:
                {
                    var builder = new StringViewArray.Builder();
                    foreach (StringViewArray piece in pieces)
                    {
                        for (int i = 0; i < piece.Length; i++)
                        {
                            if (piece.IsNull(i))
                                builder.AppendNull();
                            else
                                builder.Append(piece.GetString(i));
                        }
                    }

                    return builder.Build();
                }

                case Apache.Arrow.Types.BinaryViewType:
                {
                    var builder = new BinaryViewArray.Builder();
                    foreach (BinaryViewArray piece in pieces)
                    {
                        for (int i = 0; i < piece.Length; i++)
                        {
                            if (piece.IsNull(i))
                                builder.AppendNull();
                            else
                                builder.Append(piece.GetBytes(i));
                        }
                    }

                    return builder.Build();
                }

                default:
                    return EngineeredWood.Arrow.ArrowCompute.Concatenate(pieces);
            }
        }
    }

    private sealed record RowGroupContext(
        IReadOnlyList<ColumnDescriptor> Columns,
        IReadOnlyList<ColumnChunk> Chunks,
        FileRange[] Ranges,
        Field[] LeafArrowFields,
        int RowCount,
        bool HasNestedColumns = false,
        SchemaNode? SchemaRoot = null,
        Field[]? TopLevelFields = null)
    {
        public int Count => Columns.Count;
    }

    private static (IReadOnlyList<ColumnDescriptor>, IReadOnlyList<ColumnChunk>) ResolveColumns(
        SchemaDescriptor schema,
        RowGroup rowGroup,
        IReadOnlyList<string>? columnNames)
    {
        if (columnNames == null)
        {
            // All leaf columns (flat, struct, list, map)
            var allColumns = new List<ColumnDescriptor>();
            var allChunks = new List<ColumnChunk>();
            for (int i = 0; i < schema.Columns.Count; i++)
            {
                allColumns.Add(schema.Columns[i]);
                allChunks.Add(rowGroup.Columns[i]);
            }
            return (allColumns, allChunks);
        }

        var columns = new List<ColumnDescriptor>(columnNames.Count);
        var chunks = new List<ColumnChunk>(columnNames.Count);

        foreach (var name in columnNames)
        {
            bool found = false;

            // First, try matching a leaf column by dotted path
            for (int i = 0; i < schema.Columns.Count; i++)
            {
                var col = schema.Columns[i];
                if (col.DottedPath == name)
                {
                    columns.Add(col);
                    chunks.Add(rowGroup.Columns[i]);
                    found = true;
                    break;
                }
            }

            if (found) continue;

            // Second, try matching a top-level group name → resolve to all descendant leaves
            foreach (var child in schema.Root.Children)
            {
                if (child.Name == name && !child.IsLeaf)
                {
                    // Add all descendant leaves
                    for (int i = 0; i < schema.Columns.Count; i++)
                    {
                        var col = schema.Columns[i];
                        if (col.Path.Count > 0 && col.Path[0] == name)
                        {
                            columns.Add(col);
                            chunks.Add(rowGroup.Columns[i]);
                        }
                    }
                    found = true;
                    break;
                }
            }

            if (!found)
                throw new ArgumentException(
                    $"Column '{name}' was not found in the schema.", nameof(columnNames));
        }

        return (columns, chunks);
    }

    /// <summary>
    /// Builds a pruned schema root containing only the top-level children
    /// that have at least one descendant leaf in the selected columns.
    /// Preserves the original subtrees — only filters at the top level.
    /// </summary>
    private static SchemaNode PruneSchemaRoot(
        SchemaNode fullRoot,
        IReadOnlyList<ColumnDescriptor> selectedColumns)
    {
        // Collect unique top-level names from selected columns (preserving order)
        var topLevelNames = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var col in selectedColumns)
        {
            if (col.Path.Count > 0 && seen.Add(col.Path[0]))
                topLevelNames.Add(col.Path[0]);
        }

        // Pick matching children from the full root in the requested order
        var children = new List<SchemaNode>(topLevelNames.Count);
        foreach (var name in topLevelNames)
        {
            foreach (var child in fullRoot.Children)
            {
                if (child.Name == name)
                {
                    children.Add(child);
                    break;
                }
            }
        }

        return new SchemaNode
        {
            Element = fullRoot.Element,
            Parent = null,
            Children = children,
        };
    }

#if NET8_0_OR_GREATER
    [GeneratedRegex(@"version\s+(\d+)\.(\d+)\.(\d+)")]
    private static partial Regex VersionRegex();
#else
    private static readonly Regex VersionRegexInstance = new(@"version\s+(\d+)\.(\d+)\.(\d+)", RegexOptions.Compiled);
    private static Regex VersionRegex() => VersionRegexInstance;
#endif

    /// <summary>
    /// Detects whether the file was written by a parquet-mr version affected by PARQUET-816,
    /// where TotalCompressedSize excludes the dictionary page header.
    /// The fix was in parquet-mr 1.2.9.
    /// </summary>
    internal static bool HasParquet816Bug(string? createdBy)
    {
        if (createdBy == null)
            return false;

        // Must start with "parquet-mr"
        if (!createdBy.StartsWith("parquet-mr", StringComparison.OrdinalIgnoreCase))
            return false;

        // "parquet-mr" with no version → pre-1.0, definitely buggy
        var match = VersionRegex().Match(createdBy);
        if (!match.Success)
            return true;

        int major = int.Parse(match.Groups[1].Value);
        int minor = int.Parse(match.Groups[2].Value);
        int patch = int.Parse(match.Groups[3].Value);

        // Bug was fixed in 1.2.9
        return major < 1 || (major == 1 && (minor < 2 || (minor == 2 && patch < 9)));
    }

    internal static FileRange GetColumnChunkRange(
        ColumnMetaData column,
        long fileLength,
        bool hasParquet816Bug)
    {
        // The chunk starts at whichever side-table page precedes the data pages — a
        // dictionary page, or an FSST symbol table page. Starting at DataPageOffset would
        // read past a symbol table the data pages cannot be decoded without.
        long start = column.DictionaryPageOffset is > 0 and long dpo
            ? dpo
            : column.SymbolTablePageOffset is > 0 and long stpo
                ? stpo
                : column.DataPageOffset;
        long length = column.TotalCompressedSize;

        // PARQUET-816 workaround: old parquet-mr writers (<= 1.2.8) exclude the
        // dictionary page header from TotalCompressedSize. Add padding to cover
        // the potentially missing header bytes. Extra trailing bytes are harmless
        // when the caller stops after consuming all values.
        if (start < column.DataPageOffset || hasParquet816Bug)
        {
            long bytesRemaining = fileLength - (start + length);
            if (bytesRemaining > 0)
                length += Math.Min(MaxDictHeaderPadding, bytesRemaining);
        }

        return new FileRange(start, length);
    }

    /// <summary>
    /// Returns a <see cref="BitArray"/> indicating which row groups might contain the given value
    /// in the specified column, based on Bloom filter data. Row groups without a Bloom filter
    /// for the column are conservatively marked as candidates.
    /// </summary>
    /// <param name="column">Column name (dotted path or top-level name for a leaf column).</param>
    /// <param name="value">The value to probe for. Must be type-compatible with the column's physical type.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A <see cref="BitArray"/> of length equal to the number of row groups.
    /// A <c>true</c> bit means the row group might contain the value;
    /// a <c>false</c> bit means it definitely does not.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The column name is not found or the value type is incompatible with the column's physical type.
    /// </exception>
    public ValueTask<BitArray> GetCandidateRowGroupsAsync(
        string column, object value,
        CancellationToken cancellationToken = default)
    {
        return GetCandidateRowGroupsAsync(column, new[] { value }, cancellationToken);
    }

    /// <summary>
    /// Returns a <see cref="BitArray"/> indicating which row groups might contain any of the given
    /// values in the specified column, based on Bloom filter data. Row groups without a Bloom filter
    /// for the column are conservatively marked as candidates.
    /// </summary>
    /// <param name="column">Column name (dotted path or top-level name for a leaf column).</param>
    /// <param name="values">
    /// The values to probe for. A row group is marked as a candidate if its Bloom filter indicates
    /// it might contain <em>any</em> of the values. All values must be type-compatible with the
    /// column's physical type.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A <see cref="BitArray"/> of length equal to the number of row groups.
    /// A <c>true</c> bit means the row group might contain at least one of the values;
    /// a <c>false</c> bit means it definitely does not contain any of them.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The column name is not found, no values are provided, or a value type is incompatible
    /// with the column's physical type.
    /// </exception>
    public async ValueTask<BitArray> GetCandidateRowGroupsAsync(
        string column, IReadOnlyList<object> values,
        CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        if (column is null) throw new ArgumentNullException(nameof(column));
        if (values is null) throw new ArgumentNullException(nameof(values));
        if (values.Count == 0)
            throw new ArgumentException("At least one value must be provided.", nameof(values));

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);

        // Resolve the column to a leaf column index.
        int columnIndex = ResolveLeafColumnIndex(schema, column);
        var physicalType = schema.Columns[columnIndex].PhysicalType;

        int numRowGroups = metadata.RowGroups.Count;
        var result = new BitArray(numRowGroups, true); // default to candidate

        // Pre-encode all values, each as every encoding an EQUAL value could have been hashed from (both
        // zeros for a zero). A NaN matches NaNs of any payload, which no probe covers: every row group
        // stays a candidate. Every value is encoded before that is decided, so an incompatible value is
        // refused whatever precedes it.
        var encoded = new List<byte[]>(values.Count);
        bool unprobeable = false;
        for (int i = 0; i < values.Count; i++)
        {
            if (BloomFilter.BloomFilterValueEncoder.TryEncodeEquivalents(values[i], physicalType, out var equivalents))
                encoded.AddRange(equivalents);
            else
                unprobeable = true;
        }
        if (unprobeable)
            return result;
        var encodedValues = encoded.ToArray();

        // Collect bloom filter file ranges for all row groups that have one.
        var rangeIndices = new List<int>(); // row group indices that have bloom filter data
        var ranges = new List<FileRange>();

        for (int rg = 0; rg < numRowGroups; rg++)
        {
            var chunk = metadata.RowGroups[rg].Columns[columnIndex];
            if (IsStoredElsewhere(chunk))
                continue; // its filter is in the other file: stay a candidate, and let the read refuse

            // The same test pruning applies (bounds, a missing length, the size cap), so this probe and
            // the predicate path agree on which filters are read at all.
            if (MembershipPredicateEvaluator.TryGetBloomFilterRange(
                    chunk, _fileLength, _options.ColumnChunkFilePath, out var range))
            {
                rangeIndices.Add(rg);
                ranges.Add(range);
            }
        }

        if (ranges.Count == 0)
            return result; // no bloom filters available

        // Batch-read all bloom filter blocks.
        var buffers = await _file.ReadRangesAsync(ranges, cancellationToken).ConfigureAwait(false);

        try
        {
            for (int i = 0; i < rangeIndices.Count; i++)
            {
                int rg = rangeIndices[i];

                // A filter that cannot be parsed is declined, as pruning declines it: the group stays a
                // candidate, and the read that follows is left to report what is wrong with the file.
                var filter = MembershipPredicateEvaluator.Decode(
                    MembershipSource.BloomFilter, buffers[i].Memory.Span,
                    metadata.RowGroups[rg].Columns[columnIndex].MetaData!, schema.Columns[columnIndex],
                    validateChecksums: false);
                if (filter is null)
                    continue;

                bool anyMatch = false;
                for (int v = 0; v < encodedValues.Length; v++)
                {
                    if (filter.MightContain(encodedValues[v]))
                    {
                        anyMatch = true;
                        break;
                    }
                }

                if (!anyMatch)
                    result[rg] = false;
            }
        }
        finally
        {
            for (int i = 0; i < buffers.Count; i++)
                buffers[i].Dispose();
        }

        return result;
    }

    /// <summary>
    /// Returns a <see cref="BitArray"/> indicating which row groups might contain a row matching
    /// <paramref name="filter"/>: the per-read form of <see cref="ParquetReadOptions.Filter"/>, deciding
    /// each row group exactly as <see cref="ReadAllAsync"/> does under that option (column statistics,
    /// then dictionary pages when <see cref="ParquetReadOptions.FilterUseDictionaries"/> is set, then
    /// Bloom filters when <see cref="ParquetReadOptions.FilterUseBloomFilters"/> is set) without reading
    /// any data pages.
    /// </summary>
    /// <remarks>
    /// Use this rather than <see cref="ParquetReadOptions.Filter"/> when one reader configuration serves
    /// reads with different intents, or when the caller needs each row's position in the file: read the
    /// candidates with <see cref="ReadRowGroupBatchesAsync"/> and count the rows of the skipped groups
    /// from <see cref="RowGroup.NumRows"/>. <see cref="ReadAllAsync"/> under a filter drops row groups
    /// silently, so a position computed by counting its batches is wrong after the first skipped group.
    /// <para>References are resolved against the file's leaf columns by dotted path (or bare name for a
    /// top-level leaf); one that resolves to nothing evaluates Unknown and keeps the row group. The
    /// result is a superset: rows of a candidate row group are not filtered.</para>
    /// </remarks>
    /// <param name="filter">The predicate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A <see cref="BitArray"/> of length equal to the number of row groups. A <c>false</c> bit means
    /// the row group provably holds no matching row.
    /// </returns>
    public async ValueTask<BitArray> GetCandidateRowGroupsAsync(
        EngineeredWood.Expressions.Predicate filter,
        CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        if (filter is null) throw new ArgumentNullException(nameof(filter));

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var accessor = new ParquetStatisticsAccessor(schema);

        var prefetch = CreatePrefetches(filter, metadata, schema, accessor);

        var result = new BitArray(metadata.RowGroups.Count, true);
        for (int i = 0; i < metadata.RowGroups.Count; i++)
        {
            result[i] = await MightMatchAsync(filter, i, metadata, schema, accessor, prefetch, cancellationToken)
                .ConfigureAwait(false);
        }
        return result;
    }

    /// <summary>
    /// The rows of one row group that might match <paramref name="filter"/>, narrowed by the page index:
    /// the row-level companion to <see cref="GetCandidateRowGroupsAsync(EngineeredWood.Expressions.Predicate, CancellationToken)"/>. Reads no data pages.
    /// </summary>
    /// <remarks>
    /// <para>The row group is judged first exactly as <see cref="GetCandidateRowGroupsAsync(EngineeredWood.Expressions.Predicate, CancellationToken)"/> judges it;
    /// one ruled out returns no ranges. Otherwise each column the predicate references that is not
    /// repeated, and has a ColumnIndex and OffsetIndex, contributes its pages' bounds and counts, one
    /// ranged read for all of them. Rows in a page whose bounds rule the predicate out are dropped.</para>
    /// <para>The result is a superset, like the row-group one: rows inside a range are not filtered.
    /// With no usable index, or a predicate the bounds cannot decide, the whole row group is one range.
    /// A page index that fails to decode or cannot describe the row group is ignored, not refused.</para>
    /// </remarks>
    /// <param name="rowGroupIndex">Zero-based index of the row group.</param>
    /// <param name="filter">The predicate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Ascending, disjoint, non-empty ranges of rows within the row group.</returns>
    public async ValueTask<IReadOnlyList<RowRange>> GetCandidateRowRangesAsync(
        int rowGroupIndex,
        EngineeredWood.Expressions.Predicate filter,
        CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        if (filter is null) throw new ArgumentNullException(nameof(filter));

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        if (rowGroupIndex < 0 || rowGroupIndex >= metadata.RowGroups.Count)
            throw new ArgumentOutOfRangeException(nameof(rowGroupIndex), rowGroupIndex,
                $"The file has {metadata.RowGroups.Count} row groups.");

        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var accessor = new ParquetStatisticsAccessor(schema);
        var rowGroup = metadata.RowGroups[rowGroupIndex];
        if (rowGroup.NumRows <= 0
            || !await MightMatchAsync(filter, rowGroupIndex, metadata, schema, accessor, default, cancellationToken)
                .ConfigureAwait(false))
        {
            return [];
        }

        var (ranges, _) = await NarrowByPageIndexAsync(
                filter, rowGroupIndex, rowGroup, schema, accessor, readProjection: false, projection: null,
                readAhead: null, cancellationToken)
            .ConfigureAwait(false);
        return ranges;
    }

    /// <summary>
    /// The page-index half of <see cref="GetCandidateRowRangesAsync"/>, for a row group already judged a
    /// candidate: the rows its pages cannot rule out, the whole row group when nothing can narrow it.
    /// With <paramref name="readProjection"/>, the same request reads the OffsetIndex of each column in
    /// <paramref name="projection"/> (null: all), returned by file offset for the read's page maps.
    /// </summary>
    private async ValueTask<(IReadOnlyList<RowRange> Ranges, Dictionary<long, byte[]>? OffsetIndexes)> NarrowByPageIndexAsync(
        EngineeredWood.Expressions.Predicate filter, int rowGroupIndex, RowGroup rowGroup,
        SchemaDescriptor schema, ParquetStatisticsAccessor accessor,
        bool readProjection, IReadOnlyList<string>? projection, IReadOnlyDictionary<long, byte[]>? readAhead,
        CancellationToken cancellationToken)
    {
        var whole = new[] { new RowRange(0, rowGroup.NumRows) };
        if (EngineeredWood.Expressions.StatisticsEvaluator.Evaluate(filter, rowGroup, accessor)
            == EngineeredWood.Expressions.FilterResult.AlwaysTrue)
        {
            return (whole, null);
        }

        var leaves = PageIndexPruner.PrunableColumns(filter, rowGroup, schema, _options.ColumnChunkFilePath);
        if (leaves.Count == 0)
            return (whole, null);

        // One request: the ColumnIndex and OffsetIndex of each column pruned by, and, for a read that
        // follows, the OffsetIndex of each column it projects, which its page maps need. Reading those
        // separately afterwards cost a round trip per row group.
        var ranges = new List<FileRange>();
        var columnIndexOf = new Dictionary<int, int>();
        var offsetIndexOf = new Dictionary<int, int>();
        var offsetIndexRanges = new Dictionary<long, int>();
        try
        {
            foreach (int leaf in leaves)
            {
                var chunk = rowGroup.Columns[leaf];
                var column = schema.Columns[leaf];
                columnIndexOf[leaf] = ranges.Count;
                ranges.Add(PageIndexRange(chunk.ColumnIndexOffset, chunk.ColumnIndexLength, "ColumnIndex", column)!.Value);
                var offsetIndex = PageIndexRange(chunk.OffsetIndexOffset, chunk.OffsetIndexLength, "OffsetIndex", column)!.Value;
                offsetIndexOf[leaf] = offsetIndexRanges[offsetIndex.Offset] = ranges.Count;
                ranges.Add(offsetIndex);
            }
        }
        catch (ParquetFormatException)
        {
            // A footer that places an index outside the file. As with an index that fails to decode,
            // the rows are not narrowed rather than the call failing: a read that never consults the
            // index would not fail on it either.
            return (whole, null);
        }

        if (readProjection)
        {
            foreach (var range in ProjectedOffsetIndexRanges(schema, rowGroup, projection))
            {
                if (offsetIndexRanges.ContainsKey(range.Offset))
                    continue;
                offsetIndexRanges[range.Offset] = ranges.Count;
                ranges.Add(range);
            }
        }

        // What the read-ahead holds is taken from it; the rest is read now.
        var bytes = new byte[ranges.Count][];
        var missing = new List<int>();
        for (int k = 0; k < ranges.Count; k++)
        {
            if (readAhead is not null && readAhead.TryGetValue(ranges[k].Offset, out var held) && held.Length == ranges[k].Length)
                bytes[k] = held;
            else
                missing.Add(k);
        }

        if (missing.Count > 0)
        {
            var read = await ReadPageIndexBytesAsync(missing.ConvertAll(k => ranges[k]), cancellationToken).ConfigureAwait(false);
            for (int m = 0; m < missing.Count; m++)
                bytes[missing[m]] = read[m];
        }

        var indexes = new ColumnChunkPageIndex[leaves.Count];
        for (int k = 0; k < leaves.Count; k++)
        {
            int leaf = leaves[k];
            indexes[k] = new ColumnChunkPageIndex(
                leaf, schema.Columns[leaf].Path, bytes[columnIndexOf[leaf]], bytes[offsetIndexOf[leaf]]);
        }

        Dictionary<long, byte[]>? offsetIndexes = null;
        if (readProjection)
        {
            offsetIndexes = new Dictionary<long, byte[]>(offsetIndexRanges.Count);
            foreach (var entry in offsetIndexRanges)
                offsetIndexes[entry.Key] = bytes[entry.Value];
        }

        return (PageIndexPruner.SelectRows(filter, rowGroup, schema, indexes), offsetIndexes);
    }

    /// <summary>
    /// The OffsetIndexes a read of <paramref name="projection"/> builds its page maps from: every
    /// projected column's that a map can use (flat, in this file under <see cref="ParquetReadOptions.ColumnChunkFilePath"/>,
    /// and wholly inside it). None when the projection
    /// has a nested column, since such a row group is decoded whole and builds no page maps. The narrowing
    /// and the read-ahead both ask this, so that the read-ahead holds exactly what the narrowing wants.
    /// </summary>
    private IEnumerable<FileRange> ProjectedOffsetIndexRanges(
        SchemaDescriptor schema, RowGroup rowGroup, IReadOnlyList<string>? projection)
    {
        var (descriptors, chunks) = ResolveColumns(schema, rowGroup, projection);
        if (descriptors.Any(d => d.Path.Count > 1 || d.MaxRepetitionLevel > 0))
            yield break;

        foreach (var chunk in chunks)
        {
            // A chunk stored in another file has offsets into that file (#405); under Refuse the read then
            // refuses it, so its index is not this file's to read.
            if (!IsStoredElsewhere(chunk)
                && chunk.OffsetIndexOffset is { } offset && chunk.OffsetIndexLength is { } length
                && offset >= 0 && length > 0 && offset <= _fileLength - length)
            {
                yield return new FileRange(offset, length);
            }
        }
    }

    /// <summary>At most this many row groups' page indexes are read ahead in one request.</summary>
    private const int PageIndexReadAheadRowGroups = 64;

    /// <summary>
    /// ...and at most this many bytes of them, unless the first row group's alone are more, which are read
    /// anyway. Tests lower it to make a file span several windows.
    /// </summary>
    internal long PageIndexReadAheadBudgetBytes { get; set; } = 8L * 1024 * 1024;

    /// <summary>
    /// Replaces <paramref name="readAhead"/>'s contents with the page-index bytes that
    /// <see cref="NarrowByPageIndexAsync"/> will want for row group <paramref name="first"/> and the row
    /// groups after it, read in one request, and returns the row group after the last one covered.
    /// </summary>
    /// <remarks>
    /// Only row groups statistics leave undecided are covered: one they rule out, or wholly in, never
    /// consults its index. A later group that dictionaries or Bloom filters then rule out wasted its
    /// share, which is small. An index this cannot place is left out, for the narrowing to read and
    /// refuse itself.
    /// </remarks>
    private async ValueTask<int> ReadPageIndexesAheadAsync(
        EngineeredWood.Expressions.Predicate filter, int first, FileMetaData metadata, SchemaDescriptor schema,
        ParquetStatisticsAccessor accessor, IReadOnlyList<string>? projection, Dictionary<long, byte[]> readAhead,
        CancellationToken cancellationToken)
    {
        readAhead.Clear();
        var ranges = new List<FileRange>();
        var seen = new HashSet<long>();
        long bytes = 0;

        // One row group's ranges, gathered before they are committed so that the budget can be checked
        // with them counted.
        var group = new List<FileRange>();
        long groupBytes = 0;
        void Add(long? offset, int? length)
        {
            if (offset is { } o && length is { } l && o >= 0 && l > 0 && o <= _fileLength - l
                && !seen.Contains(o) && !group.Exists(r => r.Offset == o))
            {
                group.Add(new FileRange(o, l));
                groupBytes += l;
            }
        }

        int end = first;
        for (; end < metadata.RowGroups.Count && end - first < PageIndexReadAheadRowGroups; end++)
        {
            group.Clear();
            groupBytes = 0;

            var rowGroup = metadata.RowGroups[end];
            if (rowGroup.NumRows <= 0)
                continue;
            var verdict = EngineeredWood.Expressions.StatisticsEvaluator.Evaluate(filter, rowGroup, accessor);
            if (verdict != EngineeredWood.Expressions.FilterResult.Unknown)
                continue;

            var leaves = PageIndexPruner.PrunableColumns(filter, rowGroup, schema, _options.ColumnChunkFilePath);
            if (leaves.Count == 0)
                continue;

            foreach (int leaf in leaves)
            {
                var chunk = rowGroup.Columns[leaf];
                Add(chunk.ColumnIndexOffset, chunk.ColumnIndexLength);
                Add(chunk.OffsetIndexOffset, chunk.OffsetIndexLength);
            }

            foreach (var range in ProjectedOffsetIndexRanges(schema, rowGroup, projection))
                Add(range.Offset, (int)range.Length);

            // A group that would take the window past its budget starts the next window instead. The
            // first group is read whatever its size: the narrowing would read its index anyway.
            if (end > first && bytes + groupBytes > PageIndexReadAheadBudgetBytes)
                break;

            foreach (var range in group)
            {
                seen.Add(range.Offset);
                ranges.Add(range);
            }

            bytes += groupBytes;
        }

        if (ranges.Count > 0)
        {
            var read = await ReadPageIndexBytesAsync(ranges, cancellationToken).ConfigureAwait(false);
            for (int k = 0; k < ranges.Count; k++)
                readAhead[ranges[k].Offset] = read[k];
        }

        return Math.Max(end, first + 1);
    }

    /// <summary>
    /// How many bytes one prefetch request may ask for, per membership source. Tests lower it to make a
    /// file span several windows.
    /// </summary>
    internal long MembershipPrefetchBudgetBytes { get; set; } = MembershipPrefetch.DefaultBudgetBytes;

    /// <summary>The read-ahead for each membership source the options turn on.</summary>
    private readonly record struct Prefetches(MembershipPrefetch? Dictionary, MembershipPrefetch? BloomFilter);

    private Prefetches CreatePrefetches(
        EngineeredWood.Expressions.Predicate filter, FileMetaData metadata, SchemaDescriptor schema,
        ParquetStatisticsAccessor accessor)
    {
        return new Prefetches(
            _options.FilterUseDictionaries ? Create(MembershipSource.Dictionary) : null,
            _options.FilterUseBloomFilters ? Create(MembershipSource.BloomFilter) : null);

        MembershipPrefetch Create(MembershipSource source) =>
            new(source, filter, metadata, schema, accessor, _file, _fileLength,
                _options.ColumnChunkFilePath, _options.PageChecksumValidation, MembershipPrefetchBudgetBytes,
                _options.MaxPageUncompressedSize);
    }

    /// <summary>
    /// The one row-group verdict behind both <see cref="ParquetReadOptions.Filter"/> and
    /// <see cref="GetCandidateRowGroupsAsync(EngineeredWood.Expressions.Predicate, CancellationToken)"/>:
    /// false only when statistics, or a dictionary page or Bloom filter asked about a predicate they left
    /// Unknown, prove no row of row group <paramref name="rowGroup"/> matches. The dictionary goes first,
    /// as in parquet-mr: its answer is exact, so it never leaves a row group the filter could still rule
    /// out, and a chunk that has one is usually small enough to read whole.
    /// </summary>
    private async ValueTask<bool> MightMatchAsync(
        EngineeredWood.Expressions.Predicate filter, int rowGroup, FileMetaData metadata,
        SchemaDescriptor schema, ParquetStatisticsAccessor accessor, Prefetches prefetch,
        CancellationToken cancellationToken)
    {
        var result = EngineeredWood.Expressions.StatisticsEvaluator.Evaluate(
            filter, metadata.RowGroups[rowGroup], accessor);
        if (result == EngineeredWood.Expressions.FilterResult.AlwaysFalse)
            return false;

        if (result != EngineeredWood.Expressions.FilterResult.Unknown)
            return true;

        // Each source reads ahead for this group and the undecided groups after it, in one request, rather
        // than one request per group: on object storage the round trip, not the bytes, is the cost.
        if (_options.FilterUseDictionaries
            && await MembershipRulesOutAsync(MembershipSource.Dictionary, prefetch.Dictionary).ConfigureAwait(false))
        {
            return false;
        }

        if (_options.FilterUseBloomFilters
            && await MembershipRulesOutAsync(MembershipSource.BloomFilter, prefetch.BloomFilter).ConfigureAwait(false))
        {
            return false;
        }

        return true;

        async ValueTask<bool> MembershipRulesOutAsync(MembershipSource source, MembershipPrefetch? readAhead)
        {
            var sets = readAhead is null
                ? null
                : await readAhead.ForRowGroupAsync(rowGroup, cancellationToken).ConfigureAwait(false);
            return await MembershipPredicateEvaluator.EvaluateAsync(
                    filter, source, rowGroup, metadata, schema, _file, _fileLength,
                    _options.ColumnChunkFilePath, _options.PageChecksumValidation, cancellationToken, sets,
                    _options.MaxPageUncompressedSize)
                .ConfigureAwait(false) == EngineeredWood.Expressions.FilterResult.AlwaysFalse;
        }
    }

    /// <summary>
    /// Reads the page index (the ColumnIndex and OffsetIndex) of column chunks in one row group.
    /// </summary>
    /// <param name="rowGroupIndex">The row group.</param>
    /// <param name="columnNames">
    /// Columns to read, named as for <see cref="ReadRowGroupAsync"/>: a leaf's dotted path, or a
    /// top-level name, which selects all of that column's leaves. Null reads every leaf column.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// One entry per selected leaf column, in the order selected. A chunk the writer gave no index
    /// reports <see cref="ColumnChunkPageIndex.HasColumnIndex"/> and
    /// <see cref="ColumnChunkPageIndex.HasOffsetIndex"/> as false.
    /// </returns>
    /// <remarks>
    /// Nothing is read unless this is called: reading a row group never touches its page index. The
    /// selected indexes are fetched together in as few ranged reads as their layout allows. Writers
    /// put a row group's ColumnIndexes next to each other and its OffsetIndexes next to each other,
    /// so that is usually two ranges in one request. Each index is decoded when first accessed.
    /// An index whose offset or length is missing is treated as absent.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The row group index is out of range.</exception>
    /// <exception cref="ArgumentException">A column name is not found in the schema.</exception>
    /// <exception cref="ParquetFormatException">An index lies outside the file.</exception>
    /// <exception cref="NotSupportedException">
    /// A selected column chunk is stored in another file (<see cref="ColumnChunk.FilePath"/>).
    /// </exception>
    public async ValueTask<IReadOnlyList<ColumnChunkPageIndex>> ReadPageIndexAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        if (rowGroupIndex < 0 || rowGroupIndex >= metadata.RowGroups.Count)
            throw new ArgumentOutOfRangeException(nameof(rowGroupIndex), rowGroupIndex,
                $"The file has {metadata.RowGroups.Count} row groups.");

        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var (descriptors, chunks) = ResolveColumns(schema, metadata.RowGroups[rowGroupIndex], columnNames);

        // Up to two ranges per column, remembering which entry and which index each one is.
        var ranges = new List<FileRange>(chunks.Count * 2);
        var owners = new List<(int Entry, bool IsColumnIndex)>(chunks.Count * 2);
        var leaves = new int[chunks.Count];
        for (int entry = 0; entry < chunks.Count; entry++)
        {
            var chunk = chunks[entry];
            leaves[entry] = IndexOf(schema.Columns, descriptors[entry]);

            ThrowIfStoredElsewhere(chunk, descriptors[entry]);

            if (PageIndexRange(chunk.ColumnIndexOffset, chunk.ColumnIndexLength, "ColumnIndex", descriptors[entry]) is { } ci)
            {
                ranges.Add(ci);
                owners.Add((entry, true));
            }

            if (PageIndexRange(chunk.OffsetIndexOffset, chunk.OffsetIndexLength, "OffsetIndex", descriptors[entry]) is { } oi)
            {
                ranges.Add(oi);
                owners.Add((entry, false));
            }
        }

        var bytes = await ReadPageIndexBytesAsync(ranges, cancellationToken).ConfigureAwait(false);

        var columnIndexes = new byte[]?[chunks.Count];
        var offsetIndexes = new byte[]?[chunks.Count];
        for (int i = 0; i < owners.Count; i++)
        {
            if (owners[i].IsColumnIndex)
                columnIndexes[owners[i].Entry] = bytes[i];
            else
                offsetIndexes[owners[i].Entry] = bytes[i];
        }

        var result = new ColumnChunkPageIndex[chunks.Count];
        for (int entry = 0; entry < chunks.Count; entry++)
        {
            result[entry] = new ColumnChunkPageIndex(
                leaves[entry], descriptors[entry].Path, columnIndexes[entry], offsetIndexes[entry]);
        }

        return result;
    }

    private static int IndexOf(IReadOnlyList<ColumnDescriptor> columns, ColumnDescriptor column)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (ReferenceEquals(columns[i], column))
                return i;
        }

        throw new InvalidOperationException($"Column '{column.DottedPath}' is not a leaf of this schema.");
    }

    /// <summary>
    /// The byte range of one page-index structure, or null when the chunk records none. A range
    /// with only one of its two fields set is treated as absent: it cannot be read either way.
    /// </summary>
    private FileRange? PageIndexRange(long? offset, int? length, string structure, ColumnDescriptor column)
    {
        if (offset is not { } o || length is not { } l)
            return null;

        if (o < 0 || l <= 0 || o > _fileLength - l)
        {
            throw new ParquetFormatException(
                $"Column '{column.DottedPath}''s {structure} lies outside the file: offset {o}, length {l}, file length {_fileLength}.");
        }

        return new FileRange(o, l);
    }

    /// <summary>
    /// Ranges closer than this are read as one. Page-index structures are tens to thousands of
    /// bytes, so reading a small gap between two costs less than a second request, especially
    /// against object storage.
    /// </summary>
    private const long PageIndexCoalesceGap = 64 * 1024;

    /// <summary>
    /// Reads page-index structures, merging nearby ranges into as few reads as possible, and returns
    /// each range's bytes in the order given. Every page-index read goes through here, so that
    /// encryption (module types 6 and 7) has one place to decrypt them.
    /// </summary>
    private async ValueTask<byte[][]> ReadPageIndexBytesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken)
    {
        var result = new byte[ranges.Count][];
        if (ranges.Count == 0)
            return result;

        var order = Enumerable.Range(0, ranges.Count).OrderBy(i => ranges[i].Offset).ToArray();
        var merged = new List<FileRange>();
        var mergedOf = new int[ranges.Count];
        long start = ranges[order[0]].Offset;
        long end = start + ranges[order[0]].Length;
        foreach (int i in order)
        {
            var range = ranges[i];
            if (range.Offset > end + PageIndexCoalesceGap)
            {
                merged.Add(new FileRange(start, end - start));
                start = range.Offset;
                end = range.Offset + range.Length;
            }
            else
            {
                end = Math.Max(end, range.Offset + range.Length);
            }

            mergedOf[i] = merged.Count;
        }

        merged.Add(new FileRange(start, end - start));

        var buffers = await _file.ReadRangesAsync(merged, cancellationToken).ConfigureAwait(false);
        try
        {
            for (int i = 0; i < ranges.Count; i++)
            {
                var within = merged[mergedOf[i]];
                result[i] = buffers[mergedOf[i]].Memory.Span
                    .Slice(checked((int)(ranges[i].Offset - within.Offset)), checked((int)ranges[i].Length))
                    .ToArray();
            }
        }
        finally
        {
            foreach (var buffer in buffers)
                buffer.Dispose();
        }

        return result;
    }

    /// <summary>
    /// Resolves a column name to a leaf column index in the schema.
    /// </summary>
    private static int ResolveLeafColumnIndex(SchemaDescriptor schema, string column)
    {
        // Try matching by dotted path first.
        for (int i = 0; i < schema.Columns.Count; i++)
        {
            if (schema.Columns[i].DottedPath == column)
                return i;
        }

        // Try matching a top-level leaf by name.
        for (int i = 0; i < schema.Columns.Count; i++)
        {
            if (schema.Columns[i].Path.Count == 1 && schema.Columns[i].Path[0] == column)
                return i;
        }

        throw new ArgumentException($"Column '{column}' was not found in the schema.", nameof(column));
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_ownsFile)
            await _file.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_ownsFile)
            _file.Dispose();
    }
}
