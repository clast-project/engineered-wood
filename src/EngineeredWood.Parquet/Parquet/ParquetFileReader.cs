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
            Parallel.For(0, ctx.Count, i =>
            {
                results[i] = ColumnChunkReader.ReadColumn(
                    buffers[i].Memory.Span, ctx.Columns[i],
                    ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                    ctx.HasNestedColumns,
                    _options.PageChecksumValidation);
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
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);

        ParquetStatisticsAccessor? accessor = null;
        SchemaDescriptor? schema = null;
        if (_options.Filter is not null)
        {
            schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
            accessor = new ParquetStatisticsAccessor(schema);
        }

        for (int i = 0; i < metadata.RowGroups.Count; i++)
        {
            if (accessor is not null && _options.Filter is not null)
            {
                var result = EngineeredWood.Expressions.StatisticsEvaluator.Evaluate(
                    _options.Filter, metadata.RowGroups[i], accessor);
                if (result == EngineeredWood.Expressions.FilterResult.AlwaysFalse)
                    continue;

                if (_options.FilterUseBloomFilters
                    && result == EngineeredWood.Expressions.FilterResult.Unknown)
                {
                    var bloomResult = await BloomFilterPredicateEvaluator.EvaluateAsync(
                        _options.Filter, i, metadata, schema!,
                        _file, _fileLength, cancellationToken).ConfigureAwait(false);
                    if (bloomResult == EngineeredWood.Expressions.FilterResult.AlwaysFalse)
                        continue;
                }
            }

            if (_options.BatchSize is > 0 || _options.MaxBatchByteSize is > 0)
            {
                await foreach (var batch in ReadRowGroupBatchesAsync(i, columnNames, cancellationToken)
                    .ConfigureAwait(false))
                {
                    yield return batch;
                }
            }
            else
            {
                yield return await ReadRowGroupAsync(i, columnNames, cancellationToken)
                    .ConfigureAwait(false);
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
    public async IAsyncEnumerable<RecordBatch> ReadRowGroupBatchesAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken = default)
    {
        int? batchSize = _options.BatchSize;
        long? maxBytes = _options.MaxBatchByteSize;
        bool hasBatchLimit = batchSize is > 0 || maxBytes is > 0;

        // When no batch limit is set, fall back to the standard single-batch path.
        if (!hasBatchLimit)
        {
            yield return await ReadRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
                .ConfigureAwait(false);
            yield break;
        }

        var ctx = await PrepareRowGroupAsync(rowGroupIndex, columnNames, cancellationToken)
            .ConfigureAwait(false);

        // Check whether the entire row group fits in one batch.
        bool fitsInOneBatch = true;
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
                Parallel.For(0, ctx.Count, i =>
                {
                    results[i] = ColumnChunkReader.ReadColumn(
                        buffers[i].Memory.Span, ctx.Columns[i],
                        ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                        ctx.HasNestedColumns,
                    _options.PageChecksumValidation);
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

        // Multi-batch path: build page maps, then read only the pages needed per batch.
        //
        // Phase 1: Read each column chunk to scan page headers and build page maps.
        //          The full column buffers are released after scanning.
        // Phase 2: For each batch, compute the file-level byte ranges for only the
        //          pages that overlap the target row range, read those, decode, yield.

        var pageMaps = new ColumnPageMap[ctx.Count];
        for (int i = 0; i < ctx.Count; i++)
        {
            using var buffer = await _file.ReadAsync(ctx.Ranges[i], cancellationToken)
                .ConfigureAwait(false);
            pageMaps[i] = PageMapBuilder.Build(
                buffer.Memory.Span,
                ctx.Columns[i],
                ctx.Chunks[i].MetaData!);
        }

        // Phase 2: yield batches, reading only the needed pages per batch.
        int rowsEmitted = 0;
        while (rowsEmitted < ctx.RowCount)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int batchStartRow = rowsEmitted;
            int actualBatchRows = ComputeBatchRowCount(
                pageMaps, batchStartRow, ctx.RowCount, batchSize, maxBytes);
            int lastRow = batchStartRow + actualBatchRows - 1;

            // Compute the file-level byte range covering the needed pages for
            // each column and read only those bytes.
            var pageRanges = new FileRange[ctx.Count];
            var pageOffsets = new int[ctx.Count]; // startPage per column
            var pageEnds = new int[ctx.Count];    // endPage per column
            var baseOffsets = new long[ctx.Count]; // file offset of first byte read per column

            for (int i = 0; i < ctx.Count; i++)
            {
                int startPage = pageMaps[i].FindPageForRow(batchStartRow);
                int endPage = pageMaps[i].FindPageForRow(lastRow);
                pageOffsets[i] = startPage;
                pageEnds[i] = endPage;

                var firstEntry = pageMaps[i].Pages[startPage];
                var lastEntry = pageMaps[i].Pages[endPage];
                long rangeStart = ctx.Ranges[i].Offset + firstEntry.Offset;
                long rangeEnd = ctx.Ranges[i].Offset + lastEntry.Offset + lastEntry.CompressedSize;
                pageRanges[i] = new FileRange(rangeStart, rangeEnd - rangeStart);
                baseOffsets[i] = firstEntry.Offset; // offset of startPage within column chunk
            }

            var pageBuffers = await _file.ReadRangesAsync(pageRanges, cancellationToken)
                .ConfigureAwait(false);

            try
            {
                var results = new ColumnResult[ctx.Count];
                Parallel.For(0, ctx.Count, i =>
                {
                    int startPage = pageOffsets[i];
                    int endPage = pageEnds[i];
                    int pageStartRow = pageMaps[i].CumulativeRows[startPage];
                    long baseOffset = baseOffsets[i];

                    var fullResult = ColumnChunkReader.ReadColumnBatchFromSlice(
                        pageBuffers[i].Memory.Span,
                        baseOffset,
                        ctx.Columns[i],
                        ctx.Chunks[i].MetaData!,
                        pageMaps[i],
                        startPage, endPage,
                        ctx.LeafArrowFields[i],
                        ctx.HasNestedColumns,
                    _options.PageChecksumValidation);

                    int skipRows = batchStartRow - pageStartRow;
                    if (skipRows > 0 || fullResult.Array.Length > actualBatchRows)
                    {
                        var slicedData = fullResult.Array.Data.Slice(skipRows, actualBatchRows);
                        var slicedArray = Data.ArrowArrayFactory.BuildArray(slicedData);
                        results[i] = new ColumnResult(slicedArray,
                            fullResult.DefinitionLevels, fullResult.RepetitionLevels);
                    }
                    else
                    {
                        results[i] = fullResult;
                    }
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
                ctx.HasNestedColumns);
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

            Parallel.For(0, ctx.Count, i =>
            {
                results[i] = ColumnChunkReader.ReadColumn(
                    buffers[i].Memory.Span, ctx.Columns[i],
                    ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                    ctx.HasNestedColumns,
                    _options.PageChecksumValidation);
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
                    _options.PageChecksumValidation);
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
                ctx.HasNestedColumns);
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
            var colMeta = selectedChunks[i].MetaData
                ?? throw new ParquetFormatException(
                    $"Column chunk {i} has no inline metadata.");

            long start = colMeta.DictionaryPageOffset is > 0 and long dpo
                ? dpo
                : colMeta.DataPageOffset;
            long length = colMeta.TotalCompressedSize;

            // PARQUET-816 workaround: old parquet-mr writers (<= 1.2.8) exclude the
            // dictionary page header from TotalCompressedSize. Add padding to cover
            // the potentially missing header bytes. Extra trailing bytes are harmless
            // — ColumnChunkReader stops after consuming all values.
            if (start < colMeta.DataPageOffset || hasParquet816Bug)
            {
                long bytesRemaining = _fileLength - (start + length);
                if (bytesRemaining > 0)
                    length += Math.Min(MaxDictHeaderPadding, bytesRemaining);
            }

            ranges[i] = new FileRange(start, length);

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

            return BuildRecordBatch(ctx.LeafArrowFields, arrowArrays, ctx.RowCount);
        }

        // Nested path: group leaf arrays into Struct/List/Map arrays
        var leafArrays = new IArrowArray[results.Length];
        var leafDefLevels = new int[]?[results.Length];
        var leafRepLevels = new int[]?[results.Length];
        for (int i = 0; i < results.Length; i++)
        {
            leafArrays[i] = results[i].Array;
            leafDefLevels[i] = results[i].DefinitionLevels;
            leafRepLevels[i] = results[i].RepetitionLevels;
        }

        var topLevelArrays = NestedAssembler.Assemble(
            ctx.SchemaRoot!, leafArrays, leafDefLevels, leafRepLevels, ctx.RowCount,
            _options.ExtensionRegistry);

        // NestedAssembler wraps only top-level variant columns; wrap variants nested inside a
        // struct/list/map so the arrays match the schema's (variant-aware) field types at every depth.
        // Skipped entirely without a registry — nothing produced VariantType fields to reconcile.
        if (_options.ExtensionRegistry is not null)
        {
            for (int i = 0; i < topLevelArrays.Length; i++)
                topLevelArrays[i] = Data.VariantNestedWrapper.Wrap(
                    topLevelArrays[i], ctx.TopLevelFields![i].DataType);
        }

        return BuildRecordBatch(ctx.TopLevelFields!, topLevelArrays, ctx.RowCount);
    }

    private static RecordBatch BuildRecordBatch(
        Field[] arrowFields, IArrowArray[] arrowArrays, int rowCount)
    {
        var builder = new Apache.Arrow.Schema.Builder();
        for (int i = 0; i < arrowFields.Length; i++)
            builder.Field(arrowFields[i]);

        return new RecordBatch(builder.Build(), arrowArrays, rowCount);
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
    private static bool HasParquet816Bug(string? createdBy)
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

        // Pre-encode all values.
        var encodedValues = new byte[values.Count][];
        for (int i = 0; i < values.Count; i++)
            encodedValues[i] = BloomFilter.BloomFilterValueEncoder.Encode(values[i], physicalType);

        int numRowGroups = metadata.RowGroups.Count;
        var result = new BitArray(numRowGroups, true); // default to candidate

        // Collect bloom filter file ranges for all row groups that have one.
        var rangeIndices = new List<int>(); // row group indices that have bloom filter data
        var ranges = new List<FileRange>();

        for (int rg = 0; rg < numRowGroups; rg++)
        {
            var colMeta = metadata.RowGroups[rg].Columns[columnIndex].MetaData;
            if (colMeta?.BloomFilterOffset is long offset and > 0)
            {
                rangeIndices.Add(rg);

                // If bloom_filter_length is present, use it directly.
                // Otherwise, read a generous chunk — the Thrift header tells us the actual size.
                // Clamp to file length to avoid reading past end of file.
                long length = colMeta.BloomFilterLength ?? Math.Min(4096, _fileLength - offset);
                ranges.Add(new FileRange(offset, length));
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
                var span = buffers[i].Memory.Span;
                var filter = BloomFilter.BloomFilterReader.Parse(span);

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
