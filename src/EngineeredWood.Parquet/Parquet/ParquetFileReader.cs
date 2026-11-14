// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;
using EngineeredWood.Scans;

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
    // Not readonly only for ReadStoredInt32, which may replace it before the first read.
    private ParquetReadOptions _options;
    // The lazy caches below may be filled by one thread and read by another (row groups are decoded
    // concurrently, #612), so each is PUBLISHED with Volatile.Write once everything it depends on is set,
    // and read with Volatile.Read: a reader that sees the reference also sees what was written before it.
    // _fileLength is written before _metadata and read only after _metadata has been seen.
    private FileMetaData? _metadata;
    private SchemaDescriptor? _schema;
    // A box rather than a value plus a "resolved" flag: the file may declare no Arrow schema (null), and
    // two separate fields could be observed in the wrong order.
    private StrongBox<Apache.Arrow.Schema?>? _declaredArrowSchema;
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
    /// Reads the given INT(8)/INT(16)-annotated INT32 leaves of this file as <see cref="Apache.Arrow.Types.Int32Type"/>, the values
    /// they store, instead of narrowing them (see <see cref="ParquetReadOptions.StoredInt32Columns"/>). The
    /// elements are this reader's own, from <see cref="GetSchemaAsync"/>. Call it before the first read: it
    /// changes the type every later batch and schema carries.
    /// </summary>
    internal void ReadStoredInt32(HashSet<SchemaElement> elements) =>
        _options = _options with { StoredInt32Columns = elements.Count > 0 ? elements : null };

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

        if (Volatile.Read(ref _metadata) is { } cached)
            return cached;

        long fileLength = await _file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        if (fileLength < MinFileSize)
            throw new ParquetFormatException(
                $"File is too small to be a valid Parquet file ({fileLength} bytes).");

        // One read of the tail: at least the 8-byte suffix (4-byte footer length, LE, + PAR1 magic), and by
        // default enough to hold a typical footer too, so that it needs no second request (#722).
        int tailLength = (int)Math.Min(fileLength, Math.Max(_options.FooterReadSize, FooterSuffixSize));
        using var tailBuffer = await _file.ReadAsync(
            new FileRange(fileLength - tailLength, tailLength),
            cancellationToken).ConfigureAwait(false);

        var tail = tailBuffer.Memory.Span;
        var suffix = tail.Slice(tailLength - FooterSuffixSize);

        // Validate trailing magic
        if (suffix[4] != Par1Magic[0] || suffix[5] != Par1Magic[1] ||
            suffix[6] != Par1Magic[2] || suffix[7] != Par1Magic[3])
            throw new ParquetFormatException("Invalid Parquet file: missing trailing PAR1 magic.");

        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(suffix);
        if (footerLength <= 0 || footerLength > fileLength - MinFileSize)
            throw new ParquetFormatException(
                $"Invalid Parquet footer length: {footerLength}.");

        // Decode the footer (Thrift-encoded FileMetaData) from the tail when it is all there; else read it.
        FileMetaData metadata;
        if (footerLength <= tailLength - FooterSuffixSize)
        {
            metadata = MetadataDecoder.DecodeFileMetaData(
                tail.Slice(tailLength - FooterSuffixSize - footerLength, footerLength));
        }
        else
        {
            long footerOffset = fileLength - FooterSuffixSize - footerLength;
            using var footerBuffer = await _file.ReadAsync(
                new FileRange(footerOffset, footerLength),
                cancellationToken).ConfigureAwait(false);
            metadata = MetadataDecoder.DecodeFileMetaData(footerBuffer.Memory.Span);
        }

        _fileLength = fileLength;
        Volatile.Write(ref _metadata, metadata);
        return metadata;
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

        if (Volatile.Read(ref _schema) is { } cached)
            return cached;

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = new SchemaDescriptor(metadata.Schema, metadata.ColumnOrders);
        Volatile.Write(ref _schema, schema);
        return schema;
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
        IReadOnlyList<Field> fields = ArrowSchemaConverter.ToArrowFields(schema.Root, _options);
        // What a batch carries: a shredded variant reads back reassembled (#691).
        if (_options.ExtensionRegistry is not null)
            fields = Data.VariantNestedWrapper.AssembledFields(fields);
        foreach (var field in fields)
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
        await foreach (var positioned in ReadPositionedAsync(_options.Filter, columnNames, skipRows: null, cancellationToken)
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
        ReadPositionedAsync(filter ?? _options.Filter, columnNames, skipRows: null, cancellationToken);

    /// <summary>
    /// As <see cref="ReadWithPositionsAsync(EngineeredWood.Expressions.Predicate?, IReadOnlyList{string}?, CancellationToken)"/>,
    /// also skipping, unread, the row groups <paramref name="skipRows"/> says the caller needs none of.
    /// </summary>
    /// <remarks>
    /// <para>Every argument is required, so that a three-argument call, <c>default</c> third included, still
    /// means the overload above.</para>
    /// <para><paramref name="skipRows"/> is asked once per non-empty row group, with the group's first row and
    /// row count, before anything is read for it. Nothing of a skipped group is requested: not its column
    /// chunks, and not its dictionary pages, Bloom filters or page indexes, which the read-ahead for the kept
    /// groups leaves out. Its rows still count towards the positions after it, as a group pruned by statistics
    /// does.</para>
    /// <para>Inside a group it keeps, long runs <paramref name="skipRows"/> skips are cut out of the read too, when
    /// every projected column is flat and has an OffsetIndex: the group is probed in 64 blocks (of at least 1,024
    /// rows), each run covering a whole block is widened to its exact ends, and only the pages of the rows outside
    /// the runs are fetched. Shorter runs, and scattered rows, are read and left to the caller to drop. Batches
    /// never span a cut run, and positions stay exact.</para>
    /// <para>Its bytes can still be read when they lie in a small gap between two ranges that ARE requested, and
    /// the read merges across the gap rather than pay another request: page indexes are read so across gaps up
    /// to 64 KiB, and the cloud readers' <see cref="EngineeredWood.IO.CoalescingFileReader"/> merges across
    /// <see cref="EngineeredWood.IO.CoalescingOptions.MaxGapBytes"/> (1 MiB by default), as they do for any
    /// pruned group or unprojected column. A skipped group larger than the gap is never read.</para>
    /// </remarks>
    /// <param name="filter">The predicate; null reads every row under
    /// <see cref="ParquetReadOptions.Filter"/> instead, as <see cref="ReadAllAsync"/> does.</param>
    /// <param name="columnNames">The columns to read; null reads all of them.</param>
    /// <param name="skipRows">The row groups to skip; null skips none beyond what pruning skips.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public IAsyncEnumerable<PositionedRecordBatch> ReadWithPositionsAsync(
        EngineeredWood.Expressions.Predicate? filter,
        IReadOnlyList<string>? columnNames,
        RowSkipPredicate? skipRows,
        CancellationToken cancellationToken) =>
        ReadPositionedAsync(filter ?? _options.Filter, columnNames, skipRows, cancellationToken);

    private async IAsyncEnumerable<PositionedRecordBatch> ReadPositionedAsync(
        EngineeredWood.Expressions.Predicate? filter,
        IReadOnlyList<string>? columnNames,
        RowSkipPredicate? skipRows,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var plans = PlanRowGroupsAsync(metadata, filter, columnNames, skipRows, cancellationToken);

        // No wider than the file: a window past the row-group count only sizes the scheduler for units that
        // cannot exist.
        int concurrency = Math.Min(EffectiveConcurrentRowGroups, metadata.RowGroups.Count);
        if (concurrency <= 1 || metadata.RowGroups.Count <= 1)
        {
            await foreach (var plan in plans.ConfigureAwait(false))
            {
                await foreach (var batch in ReadPlannedAsync(plan, columnNames, cancellationToken).ConfigureAwait(false))
                    yield return batch;
            }
            yield break;
        }

        // Several row groups at once (#612). The plans are still made one after another, by the scan's single
        // dispatcher, because pruning keeps state that is not thread-safe (the dictionary and Bloom filter
        // windows, the page-index read-ahead); only the reads and decodes of planned row groups run
        // concurrently, on this one reader, which is safe for them.
        // The schema is built before the fan-out, so that the decodes find it cached rather than race to.
        await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var budget = new ScanMemoryBudget(_options.MaxBufferedReadBytes, _options.MemoryBudget);
        LastScanBudget = budget;
        var growth = new DecodedGrowth();
        await foreach (var batch in OrderedScan.RunAsync(
                           plans,
                           (plan, reservation, token) => ReadPlannedAsync(plan, columnNames, reservation, growth, token),
                           static b => EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(b.Batch),
                           static b => b.Batch.Dispose(),
                           concurrency,
                           budget,
                           cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    /// <summary>
    /// The row groups a positioned read of <paramref name="columnNames"/> under <paramref name="filter"/> and
    /// <paramref name="skipRows"/> reads, in file order: the plan <see cref="ReadWithPositionsAsync(EngineeredWood.Expressions.Predicate?, IReadOnlyList{string}?, RowSkipPredicate?, CancellationToken)"/>
    /// follows, for a caller that schedules the row groups itself (a Delta scan, #612). Each plan is read with
    /// <see cref="ReadPlannedAsync(RowGroupPlan, IReadOnlyList{string}?, ScanReservation, DecodedGrowth, CancellationToken, IReadOnlyDictionary{string, AnswerPlan}?)"/>,
    /// which may run concurrently for different plans; the planning itself is NOT thread-safe and must be
    /// enumerated once, in order.
    /// <para>Only row groups <paramref name="firstRowGroup"/> (inclusive) to <paramref name="endRowGroup"/>
    /// (exclusive) are planned, and nothing is read ahead for the others: a caller can plan a file in parts,
    /// each enumeration holding the dictionary and page-index windows of its own part only.</para>
    /// </summary>
    internal async IAsyncEnumerable<RowGroupPlan> PlanRowGroupsAsync(
        EngineeredWood.Expressions.Predicate? filter,
        IReadOnlyList<string>? columnNames,
        RowSkipPredicate? skipRows,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken,
        int firstRowGroup = 0,
        int endRowGroup = int.MaxValue)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        // Built before any plan is handed out, so that concurrent reads of the plans find it cached.
        await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        await foreach (var plan in PlanRowGroupsAsync(
                           metadata, filter, columnNames, skipRows, cancellationToken, firstRowGroup, endRowGroup)
            .ConfigureAwait(false))
        {
            yield return plan;
        }
    }

    /// <summary>The row groups a positioned read reads at once: <see cref="ParquetReadOptions.MaxConcurrentRowGroups"/>;
    /// by default what the file suggests (<see cref="IReadConcurrencyHint"/>, an object store's requests in
    /// flight), and otherwise <c>min(4, Environment.ProcessorCount)</c>, where a local read stops gaining.</summary>
    private int EffectiveConcurrentRowGroups =>
        _options.MaxConcurrentRowGroups
        ?? ((_file as IReadConcurrencyHint)?.PreferredConcurrentReads is int hint and >= 1
            ? hint
            : Math.Min(4, Environment.ProcessorCount));

    /// <summary>The memory budget of the most recent parallel read, for tests to read its
    /// <see cref="ScanMemoryBudget.Peak"/>.</summary>
    internal ScanMemoryBudget? LastScanBudget { get; private set; }

    /// <summary>
    /// One row group a positioned read will read: which one, the file position of its first row, the rows
    /// within it (null for all of them), and the OffsetIndex bytes the pruning already read for its page maps.
    /// </summary>
    internal readonly record struct RowGroupPlan(
        int Index, long GroupStart, IReadOnlyList<RowRange>? Ranges, Dictionary<long, byte[]>? OffsetIndexes);

    /// <summary>
    /// Decides, in file order, which row groups a positioned read reads and which rows of each: the pruning by
    /// statistics, dictionaries, Bloom filters and page indexes, and the runs <paramref name="skipRows"/> cuts
    /// out. NOT thread-safe — the dictionary and Bloom filter windows and the page-index read-ahead are state of
    /// this one enumeration — so it is enumerated once, in order, whether the reads it plans run serially or not.
    /// Row groups outside <paramref name="firstRowGroup"/> to <paramref name="endRowGroup"/> are treated as
    /// skipped: never planned, and left out of every window.
    /// </summary>
    internal async IAsyncEnumerable<RowGroupPlan> PlanRowGroupsAsync(
        FileMetaData metadata,
        EngineeredWood.Expressions.Predicate? filter,
        IReadOnlyList<string>? columnNames,
        RowSkipPredicate? skipRows,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken,
        int firstRowGroup = 0,
        int endRowGroup = int.MaxValue)
    {
        // Decided for every group up front, from the footer alone, so that the read-ahead below can leave the
        // skipped groups out too: it would otherwise fetch their dictionaries or page indexes for nothing.
        var skipped = SkippedRowGroups(metadata, skipRows, firstRowGroup, endRowGroup);

        ParquetStatisticsAccessor? accessor = null;
        SchemaDescriptor? schema = null;
        Prefetches prefetch = default;
        if (filter is not null)
        {
            schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
            accessor = new ParquetStatisticsAccessor(schema);
            prefetch = CreatePrefetches(filter, metadata, schema, accessor, skipped);
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

            if (skipped is not null && skipped[i])
                continue;

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
                            filter!, i, metadata, schema!, accessor!, columnNames, skipped, indexReadAhead,
                            cancellationToken)
                        .ConfigureAwait(false);
                }

                (ranges, offsetIndexes) = await NarrowByPageIndexAsync(
                        filter!, i, metadata.RowGroups[i], schema!, accessor!,
                        readProjection: true, projection: columnNames, indexReadAhead, cancellationToken)
                    .ConfigureAwait(false);
                if (ranges.Count == 0)
                    continue;
            }

            // Within a kept row group, long runs of rows the caller needs none of (a deletion vector's
            // deleted runs) are cut out too, so that pages wholly inside them are not read (#618).
            if (skipRows is not null
                && SkippedRuns(skipRows, groupStart, metadata.RowGroups[i].NumRows) is { } runs)
            {
                schema ??= await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
                if (CanReadRowRanges(schema, metadata.RowGroups[i], columnNames))
                {
                    ranges = SubtractRanges(ranges ?? [new RowRange(0, metadata.RowGroups[i].NumRows)], runs);
                    if (ranges.Count == 0)
                        continue;
                }
            }

            yield return new RowGroupPlan(i, groupStart, ranges, offsetIndexes);
        }
    }

    /// <summary>Reads one planned row group, pairing each batch with the file position of its first row.
    /// <paramref name="io"/>, when given, is read in place of the file: one holding bytes fetched ahead.</summary>
    internal async IAsyncEnumerable<PositionedRecordBatch> ReadPlannedAsync(
        RowGroupPlan plan,
        IReadOnlyList<string>? columnNames,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken,
        IReadOnlyDictionary<string, AnswerPlan>? answers = null,
        IRandomAccessFile? io = null)
    {
        var ranges = plan.Ranges;
        PrefetchedFile? prefetched = io is null
            ? await TryPrefetchUnbudgetedAsync(plan, columnNames, cancellationToken).ConfigureAwait(false)
            : null;
        io ??= prefetched;

        // Batches follow the ranges in order and never span the gap between two, so a cursor over
        // the ranges places each one: its first row is the cursor, and the cursor moves on to the
        // next range once a range's rows have all been returned.
        int range = 0;
        long cursor = ranges is null ? 0 : ranges[0].Start;

        // Always via the batching entry point, even with no batch limit configured: it falls back to
        // the single-batch read itself when there is nothing to split, and it is where the implicit
        // cap for an over-sized chunk is decided. Routing around it here would put that decision in
        // two places and leave ReadAllAsync unable to read a file ReadRowGroupBatchesAsync can.
        try
        {
            await foreach (var batch in ReadBatchesAsync(
                                   plan.Index, columnNames, ranges, plan.OffsetIndexes, cancellationToken, io: io,
                                   answers: answers)
                .ConfigureAwait(false))
            {
                yield return new PositionedRecordBatch(batch, plan.GroupStart + cursor);
                cursor += batch.Length;
                if (ranges is not null && cursor == ranges[range].End && range + 1 < ranges.Count)
                    cursor = ranges[++range].Start;
            }
        }
        finally
        {
            if (prefetched is not null)
                await prefetched.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The most compressed bytes a read with no scan budget fetches whole ahead of a batched decode
    /// (<see cref="TryPrefetchUnbudgetedAsync"/>). Above it, the read keeps to page windows of
    /// <see cref="BatchPageWindowBudgetBytes"/>, so a foreign row group of a gigabyte on disk is not held whole.
    /// </summary>
    internal const long DefaultUnbudgetedPrefetchMaxBytes = 128L * 1024 * 1024;

    /// <summary><see cref="DefaultUnbudgetedPrefetchMaxBytes"/>, settable for tests.</summary>
    internal long UnbudgetedPrefetchMaxBytes { get; set; } = DefaultUnbudgetedPrefetchMaxBytes;

    /// <summary>
    /// For a read with no scan budget (one row group at a time), the row group's chunks fetched whole and at once,
    /// as a budgeted read fetches them (#843), when the batches would otherwise come from page windows: the file is an
    /// object store's (it gives a <see cref="IReadConcurrencyHint"/>), the read covers the whole group, a batch limit
    /// splits it, and the projected chunks fit <see cref="UnbudgetedPrefetchMaxBytes"/>. Windows fetched one after
    /// another cost a round trip each, and a coalescing reader merged each window's per-column pages across the
    /// columns' later pages, which later windows fetched again (#652: 487 MiB read for 259). Compressed chunks are a
    /// fraction of what the group decodes to, so holding them keeps what batching saves. Null otherwise.
    /// </summary>
    private async ValueTask<PrefetchedFile?> TryPrefetchUnbudgetedAsync(
        RowGroupPlan plan, IReadOnlyList<string>? columnNames, CancellationToken cancellationToken)
    {
        if (_file is not IReadConcurrencyHint { PreferredConcurrentReads: >= 1 })
            return null;
        if (_options.MaxBatchByteSize is null && (_options.BatchSize is not int size || size <= 0))
            return null;

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var rowGroup = metadata.RowGroups[plan.Index];
        if (!CoversWholeGroup(plan, rowGroup))
            return null;
        if (_options.MaxBatchByteSize is null && !SplitsGroup(rowGroup.NumRows))
            return null;

        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        if (PlannedBytes(rowGroup, schema, columnNames, plan.Ranges).Compressed > UnbudgetedPrefetchMaxBytes)
            return null;

        var ctx = await PrepareRowGroupAsync(plan.Index, columnNames, cancellationToken).ConfigureAwait(false);
        return TryPrefetch(ctx.Ranges, cancellationToken);
    }

    /// <summary>
    /// As above, as one unit of a parallel read: reserves scan memory before each batch is decoded.
    /// <list type="bullet">
    /// <item>Its compressed bytes are HELD for the whole read of the row group: the read keeps its column chunks
    /// (or page windows) across the batches it yields, not only while one decodes.</item>
    /// <item>A read of the whole row group starts fetching its column chunks as soon as the hold is granted, and
    /// only then waits for room to decode (#829). Reserving first made the fetch wait too: with a budget that
    /// takes one decoded row group at a time, nothing was fetched while the one before it decoded, and the read
    /// ran serially at fetch-plus-decode per row group. A read of some rows only fetches the pages it needs, as
    /// it decodes them, and so does a read with a chunk too large to fetch in one buffer
    /// (<see cref="TryPrefetch"/>).</item>
    /// <item>The first batch reserves what the pages it decodes hold when the read's page maps come from
    /// OffsetIndexes, else its share (<see cref="FirstBatchBytes"/>) of the row group's estimated decoded size
    /// (<see cref="DecodedBytes"/>) (<see cref="FirstBatchReservationAsync"/>); either grown by how far decoded row
    /// groups have exceeded theirs so far (<paramref name="growth"/>). Later batches of the same row group reserve
    /// the largest batch it has produced.</item>
    /// </list>
    /// </summary>
    internal async IAsyncEnumerable<PositionedRecordBatch> ReadPlannedAsync(
        RowGroupPlan plan,
        IReadOnlyList<string>? columnNames,
        ScanReservation reservation,
        DecodedGrowth growth,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken,
        IReadOnlyDictionary<string, AnswerPlan>? answers = null)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var compressed = PlannedBytes(metadata.RowGroups[plan.Index], schema, columnNames, plan.Ranges).Compressed;

        long decoded = 0;
        int batches = 0;
        FirstBatchEstimate estimate = default;
        PrefetchedFile? prefetched = null;
        IAsyncEnumerator<PositionedRecordBatch>? e = null;
        try
        {
            await reservation.HoldAsync(compressed, cancellationToken).ConfigureAwait(false);
            if (CoversWholeGroup(plan, metadata.RowGroups[plan.Index]))
            {
                var ctx = await PrepareRowGroupAsync(plan.Index, columnNames, cancellationToken).ConfigureAwait(false);
                prefetched = TryPrefetch(ctx.Ranges, cancellationToken);
            }

            // The first batch is sized once the fetch is under way: the OffsetIndexes it may read take a round trip.
            estimate = await FirstBatchReservationAsync(plan, columnNames, growth, cancellationToken)
                .ConfigureAwait(false);
            if (estimate.OffsetIndexes is { } offsetIndexes)
                plan = plan with { OffsetIndexes = offsetIndexes };
            reservation.SetInitialEstimate(
                estimate.Reserved, FirstBatchRowBound(metadata.RowGroups[plan.Index], plan.Ranges),
                metadata.RowGroups[plan.Index].NumRows);

            e = ReadPlannedAsync(plan, columnNames, cancellationToken, answers, prefetched)
                .GetAsyncEnumerator(cancellationToken);
            while (true)
            {
                await reservation.ReserveNextAsync(cancellationToken).ConfigureAwait(false);
                bool more = await e.MoveNextAsync().ConfigureAwait(false);
                // The read builds its page maps before its first batch and never reads the indexes again, so the
                // bytes read for the estimate, which the budget does not count, are let go (#847 review).
                if (batches == 0)
                    estimate.OffsetIndexes?.Clear();
                if (!more)
                    break;
                decoded = EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(e.Current.Batch);
                // The RAW batch is what the next decode of this row group will produce, so it is what the next
                // reservation must cover. The scheduler also learns from the item it is handed, but a caller
                // may shrink a batch before handing it on (a Delta scan drops a deletion vector's rows), and
                // a shrunken size must not lower the reservation for the full-size batch after it.
                reservation.Observe(decoded);
                // A first batch sized from its pages is measured against that size: the same buffers (#847 review).
                if (batches == 0 && estimate.FromPages)
                    growth.Observe(estimate.Baseline, decoded);
                batches++;
                yield return e.Current;
            }
        }
        finally
        {
            if (e is not null)
                await e.DisposeAsync().ConfigureAwait(false);
            if (prefetched is not null)
                await prefetched.DisposeAsync().ConfigureAwait(false);
            reservation.ReleaseHeld();
        }

        // Otherwise learnt only from a row group read as ONE batch, against the estimate of the whole read. Batches
        // split from one decode (a nested row group decoded whole, then sliced; a flat run shared by the batches
        // that slice it) each count the whole buffers they share, so summing them would count the same memory
        // many times over.
        if (batches == 1 && !estimate.FromPages)
            growth.Observe(estimate.Baseline, decoded);
    }

    /// <summary>Whether <paramref name="plan"/> reads every row of its group: no ranges, or the one range of
    /// every row that page-index pruning returns when it narrows nothing.</summary>
    private static bool CoversWholeGroup(RowGroupPlan plan, RowGroup rowGroup) =>
        CoversWholeGroup(plan, rowGroup.NumRows);

    private static bool CoversWholeGroup(RowGroupPlan plan, long rowCount) =>
        plan.Ranges is null
        || (plan.Ranges.Count == 1 && plan.Ranges[0].Start == 0 && plan.Ranges[0].End == rowCount);

    /// <summary>The largest byte array .NET allocates (<c>Array.MaxLength</c>, which .NET Framework lacks).</summary>
    private const long MaxPrefetchRangeBytes = 0x7FFFFFC7;

    /// <summary>
    /// Starts fetching <paramref name="ranges"/> ahead of their decode (#829), or returns null when one is too
    /// large to fetch into a single buffer. Such a chunk can still be read when a batch size and an OffsetIndex
    /// let the read take it in page windows, which a whole-chunk fetch would turn into an overflow.
    /// </summary>
    private PrefetchedFile? TryPrefetch(IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken)
    {
        foreach (var range in ranges)
        {
            if (range.Length > MaxPrefetchRangeBytes)
                return null;
        }
        return new PrefetchedFile(_file, ranges, cancellationToken);
    }

    /// <summary>
    /// The Parquet footer's uncompressed and compressed sizes of the column chunks the read decodes in
    /// <paramref name="rowGroup"/>, resolved as the read resolves <paramref name="columnNames"/>, dotted paths
    /// included. The uncompressed size, which stands for the decoded batches, is scaled to the share of rows
    /// <paramref name="ranges"/> keeps: an estimate made before anything is read, taking the share of a column's
    /// bytes on the pages kept to be the share of its rows. It is not scaled when a projected column is nested,
    /// because such a row group is decoded whole and then sliced. The compressed size is never scaled: a column
    /// with no usable OffsetIndex gets its page map from a header scan, which reads and holds the whole chunk.
    /// </summary>
    internal static (long Uncompressed, long Compressed) PlannedBytes(
        RowGroup rowGroup, SchemaDescriptor schema, IReadOnlyList<string>? columnNames, IReadOnlyList<RowRange>? ranges)
    {
        var (columns, chunks) = ResolveColumns(schema, rowGroup, columnNames);
        long uncompressed = 0, compressed = 0;
        bool nested = false;
        for (int c = 0; c < chunks.Count; c++)
        {
            if (chunks[c].MetaData is { } meta)
            {
                uncompressed += meta.TotalUncompressedSize;
                compressed += meta.TotalCompressedSize;
            }
            nested |= columns[c].Path.Count > 1 || columns[c].MaxRepetitionLevel > 0;
        }

        if (ranges is not null && !nested && rowGroup.NumRows > 0)
        {
            long rows = 0;
            foreach (var r in ranges)
                rows += r.End - r.Start;
            uncompressed = (long)(uncompressed * Math.Min(1.0, (double)rows / rowGroup.NumRows));
        }

        return (uncompressed, compressed);
    }

    /// <summary>
    /// What the read of <paramref name="columnNames"/> in <paramref name="rowGroup"/> will decode to, in bytes of
    /// Arrow buffers, before anything is read: the reservation for its first batch (#829). The footer's
    /// uncompressed size measures ENCODED pages, so it falls far short of the decoded size wherever an encoding
    /// shrinks the values: a dictionary- or run-length-encoded int64 column of 788K rows was 0.3 MiB in the
    /// footer and 6 MiB decoded. A whole first wave of a wide window reserved that little, decoded at once, and
    /// overran the budget several times over. So each flat column is sized by what Arrow will hold:
    /// <list type="bullet">
    /// <item>a fixed-width type, its width times the rows (exact), an extension type by its storage type;</item>
    /// <item>strings and binary, their offsets or views from the rows, plus the footer's uncompressed size for
    /// the bytes themselves (close for plain data, short for a dictionary, which <see cref="DecodedGrowth"/>
    /// then learns);</item>
    /// <item>anything else, the footer's uncompressed size;</item>
    /// </list>
    /// plus a validity bitmap for a column that may be null, and never less than the footer's size. Rows are the
    /// ones <paramref name="ranges"/> keeps. A read with a nested column is decoded whole and then sliced, and is
    /// sized by the footer as before (<see cref="PlannedBytes"/>).
    /// </summary>
    internal long DecodedBytes(
        RowGroup rowGroup, SchemaDescriptor schema, IReadOnlyList<string>? columnNames, IReadOnlyList<RowRange>? ranges)
    {
        var (columns, chunks) = ResolveColumns(schema, rowGroup, columnNames);
        for (int c = 0; c < columns.Count; c++)
        {
            if (columns[c].Path.Count > 1 || columns[c].MaxRepetitionLevel > 0)
                return PlannedBytes(rowGroup, schema, columnNames, ranges).Uncompressed;
        }

        long rows = KeptRows(rowGroup, ranges);
        double share = rowGroup.NumRows > 0 ? (double)rows / rowGroup.NumRows : 1.0;

        long total = 0;
        for (int c = 0; c < chunks.Count; c++)
        {
            if (chunks[c].MetaData is not { } meta)
                continue;
            long footer = (long)(meta.TotalUncompressedSize * share);
            total += ColumnDecodedBytes(columns[c], rows, footer, footer);
        }
        return total;
    }

    /// <summary>
    /// The reservation for the first batch of a read of whole row groups, before one has been measured: the largest
    /// first batch (<see cref="FirstBatchBytes"/> of <see cref="DecodedBytes"/>) of any row group the read decodes.
    /// <paramref name="skipped"/>, given a row group's first row and row count, says whether the read skips it unread
    /// (a deletion vector deleting it in full). For a host that reads the file through this reader but outside a
    /// planned parallel read, such as Delta's change feed and compaction (#846): the footer's uncompressed size it
    /// reserved before was 10-20x short for dictionary or run-length-encoded columns.
    /// <paramref name="addedBytesPerRow"/> is what the host adds to each row before it hands the batch on, which is
    /// the size its scan settles: a change feed's columns; <paramref name="addedBytesPerDecodedRow"/> what it adds per
    /// row decoded (<see cref="ScanReservation.AddedBytesPerDecodedRow"/>).
    /// </summary>
    internal async ValueTask<long> LargestFirstBatchBytesAsync(
        IReadOnlyList<string>? columnNames, Func<long, long, bool>? skipped, CancellationToken cancellationToken,
        long addedBytesPerRow = 0, long addedBytesPerDecodedRow = 0)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        long largest = 0;
        long groupStart = 0;
        foreach (var group in metadata.RowGroups)
        {
            long start = groupStart;
            groupStart += group.NumRows;
            if (skipped is not null && group.NumRows > 0 && skipped(start, group.NumRows))
                continue;
            long footer = PlannedBytes(group, schema, columnNames, null).Uncompressed;
            long decoded = DecodedBytes(group, schema, columnNames, null);
            // Added to the batch as sized, not to the group before it: the batch's floor at the pages it holds
            // would otherwise swallow them (#850 review). The host adds them to every row the batch holds, at most
            // BatchSize: a MaxBatchByteSize cut stops at page boundaries, so its share of the encoded bytes says
            // nothing of the rows (#850 review round 2).
            long first = ScanReservation.WithAddedBytes(
                FirstBatchBytes(decoded, footer, group, schema, columnNames, null), addedBytesPerRow,
                FirstBatchRowBound(group, null));
            largest = Math.Max(largest, ScanReservation.WithAddedBytes(first, addedBytesPerDecodedRow, group.NumRows));
        }
        return largest;
    }

    /// <summary>
    /// What <paramref name="rows"/> rows of the flat <paramref name="column"/> decode to in Arrow buffers, as
    /// <see cref="DecodedBytes"/> sizes them: <paramref name="encodedBytes"/> is the encoded size of the same rows,
    /// the floor and the size of anything not sized by type, and <paramref name="byteArrayBytes"/> the bytes of
    /// a string or binary column's values.
    /// </summary>
    private long ColumnDecodedBytes(ColumnDescriptor column, long rows, long encodedBytes, long byteArrayBytes)
    {
        var type = ArrowSchemaConverter.ToArrowField(column, _options).DataType;
        // A registered extension type (arrow.uuid over FixedSizeBinary(16)) is decoded into its storage type, so
        // that is what is sized (#845 review round 2).
        while (type is Apache.Arrow.ExtensionType extension)
            type = extension.StorageType;
        long values = type switch
        {
            Apache.Arrow.Types.FixedWidthType fixedWidth => (rows * fixedWidth.BitWidth + 7) / 8,
            Apache.Arrow.Types.StringType or Apache.Arrow.Types.BinaryType =>
                (rows + 1) * sizeof(int) + byteArrayBytes,
            Apache.Arrow.Types.LargeStringType or Apache.Arrow.Types.LargeBinaryType =>
                (rows + 1) * sizeof(long) + byteArrayBytes,
            Apache.Arrow.Types.StringViewType or Apache.Arrow.Types.BinaryViewType => rows * 16 + byteArrayBytes,
            _ => encodedBytes,
        };
        long validity = column.MaxDefinitionLevel > 0 ? (rows + 7) / 8 : 0;
        return Math.Max(values + validity, encodedBytes);
    }

    /// <summary>
    /// What a parallel read of <paramref name="plan"/> reserves for its first batch: from the pages it decodes
    /// where the read builds its page maps from OffsetIndexes (<see cref="FirstBatchFromPagesAsync"/>), else its
    /// share of the row group's decoded estimate (<see cref="FirstBatchBytes"/>); either grown by
    /// <paramref name="growth"/>. Also the OffsetIndex bytes read on the way, for the read to reuse.
    /// </summary>
    internal async ValueTask<FirstBatchEstimate> FirstBatchReservationAsync(
        RowGroupPlan plan, IReadOnlyList<string>? columnNames, DecodedGrowth growth, CancellationToken cancellationToken)
    {
        var (pages, offsetIndexes) = await FirstBatchFromPagesAsync(plan, columnNames, cancellationToken)
            .ConfigureAwait(false);
        if (pages is long bytes)
            return new FirstBatchEstimate(growth.Apply(bytes), bytes, FromPages: true, offsetIndexes);

        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var group = metadata.RowGroups[plan.Index];
        long footer = PlannedBytes(group, schema, columnNames, plan.Ranges).Uncompressed;
        long planned = DecodedBytes(group, schema, columnNames, plan.Ranges);
        return new FirstBatchEstimate(
            FirstBatchBytes(growth.Apply(planned), footer, group, schema, columnNames, plan.Ranges), planned,
            FromPages: false, offsetIndexes);
    }

    /// <summary>
    /// A first batch's reservation (<see cref="FirstBatchReservationAsync"/>), and what <see cref="DecodedGrowth"/>
    /// measures decoded sizes against: <paramref name="Baseline"/>, the estimate before growth, of the same
    /// buffers as the measurement (#847 review). From pages, that is the first batch, which holds the whole pages
    /// it touches; otherwise the whole read's <see cref="DecodedBytes"/>, measured only when it is read as one
    /// batch. Measuring a range inside a large page against the rows it kept learnt ~100x, which the page
    /// estimate, already covering the page, then multiplied.
    /// </summary>
    internal readonly record struct FirstBatchEstimate(
        long Reserved, long Baseline, bool FromPages, Dictionary<long, byte[]>? OffsetIndexes);

    /// <summary>
    /// The reservation for the first batch of a batched read of a flat row group, from the pages that batch
    /// decodes (#846), and the OffsetIndex bytes it read to find them, for the read's page maps; a null size
    /// when the read is not cut into batches by page maps, or a column has no usable OffsetIndex, and
    /// <see cref="FirstBatchBytes"/> stands.
    /// <para>A batch is a slice of whole decoded pages and keeps their buffers (<c>DecodedRows</c>), so it holds
    /// every row of each page it touches, not only its own. The footer cannot say how many that is;
    /// an OffsetIndex can. Each column is sized by type over those rows (<see cref="ColumnDecodedBytes"/>), a
    /// string column's bytes taken from the index's <c>unencoded_byte_array_data_bytes</c> where the writer
    /// recorded them (exact), else from the pages' sizes. The batch's rows are found as the read finds them
    /// (<see cref="ComputeBatchRowCount"/>, over maps built the same way).</para>
    /// <para>Only the indexes a read would fetch for its page maps are fetched, in one request, and handed on to
    /// it: the round trip moves before the first reservation and is not added.</para>
    /// </summary>
    private async ValueTask<(long? Bytes, Dictionary<long, byte[]>? OffsetIndexes)> FirstBatchFromPagesAsync(
        RowGroupPlan plan, IReadOnlyList<string>? columnNames, CancellationToken cancellationToken)
    {
        int? batchSize = _options.BatchSize;
        long? maxBytes = _options.MaxBatchByteSize;
        var ctx = await PrepareRowGroupAsync(plan.Index, columnNames, cancellationToken).ConfigureAwait(false);
        if (ctx.HasNestedColumns || ctx.Count == 0 || ctx.RowCount == 0)
            return (null, null);

        // As ReadBatchesAsync decides: a read of the whole group in one batch is sized by DecodedBytes, and one with
        // no batch limit set over a chunk too large for Arrow scans its headers rather than reading an index.
        if (maxBytes is not > 0 && HasChunkOverArrowLimit(ctx, batchSize))
            return (null, null);
        bool fitsInOneBatch = CoversWholeGroup(plan, ctx.RowCount);
        if (batchSize is > 0 && ctx.RowCount > batchSize.Value)
            fitsInOneBatch = false;
        if (maxBytes is > 0)
        {
            long footerTotal = 0;
            for (int i = 0; i < ctx.Count; i++)
                footerTotal += ctx.Chunks[i].MetaData!.TotalUncompressedSize;
            if (footerTotal > maxBytes.Value)
                fitsInOneBatch = false;
        }
        if (fitsInOneBatch)
            return (null, null);

        // Every column's index, or none is used.
        var indexRanges = new FileRange[ctx.Count];
        for (int i = 0; i < ctx.Count; i++)
        {
            var chunk = ctx.Chunks[i];
            if (chunk.OffsetIndexOffset is not { } offset || chunk.OffsetIndexLength is not { } length
                || offset < 0 || length <= 0 || offset > _fileLength - length)
            {
                return (null, null);
            }
            indexRanges[i] = new FileRange(offset, length);
        }

        var indexes = plan.OffsetIndexes is { } known
            ? new Dictionary<long, byte[]>(known)
            : new Dictionary<long, byte[]>(ctx.Count);
        var missing = new List<FileRange>();
        foreach (var range in indexRanges)
        {
            if (!indexes.ContainsKey(range.Offset) && !missing.Exists(r => r.Offset == range.Offset))
                missing.Add(range);
        }
        if (missing.Count > 0)
        {
            var read = await ReadPageIndexBytesAsync(missing, cancellationToken).ConfigureAwait(false);
            for (int k = 0; k < missing.Count; k++)
                indexes[missing[k].Offset] = read[k];
        }

        var maps = new ColumnPageMap[ctx.Count];
        var decodedIndexes = new OffsetIndex[ctx.Count];
        for (int i = 0; i < ctx.Count; i++)
        {
            OffsetIndex index;
            try
            {
                index = MetadataDecoder.DecodeOffsetIndex(indexes[indexRanges[i].Offset]);
            }
            catch (ParquetFormatException)
            {
                return (null, indexes);
            }

            var range = ctx.Ranges[i];
            if (index.PageLocations is not [var first, ..]
                || first.Offset < range.Offset || first.Offset > range.Offset + range.Length
                || PageMapBuilder.BuildLayoutFromOffsetIndex(
                        checked((int)(first.Offset - range.Offset)), range.Offset, range.Offset + range.Length,
                        index, ctx.RowCount, ctx.Columns[i], ctx.Chunks[i].MetaData!) is not { } map)
            {
                return (null, indexes);
            }

            // Where the index and the footer disagree on the first data page, the read checks the bytes before it
            // and scans the headers instead when they are not only side pages (BuildPageMapsFromOffsetIndexesAsync),
            // so the batch may not hold the pages the index lists. Telling would take a read of those bytes before
            // the reservation, a round trip after the index's; the footer estimate stands instead (#847 review).
            if (first.Offset != range.Offset && first.Offset != ctx.Chunks[i].MetaData!.DataPageOffset)
                return (null, indexes);

            maps[i] = map;
            decodedIndexes[i] = index;
        }

        int start = plan.Ranges is null ? 0 : checked((int)plan.Ranges[0].Start);
        int spanEnd = plan.Ranges is null ? ctx.RowCount : checked((int)plan.Ranges[0].End);
        int end = start + ComputeBatchRowCount(maps, start, spanEnd, batchSize, maxBytes);

        long kept = ctx.RowCount;
        if (plan.Ranges is not null)
        {
            kept = 0;
            foreach (var r in plan.Ranges)
                kept += r.End - r.Start;
        }
        double keptShare = Math.Min(1.0, (double)kept / ctx.RowCount);
        long total = 0;
        for (int i = 0; i < ctx.Count; i++)
        {
            var map = maps[i];
            int firstPage = map.FindPageForRow(start);
            int lastPage = map.FindPageForRow(end - 1);
            long rows = map.CumulativeRows[lastPage + 1] - map.CumulativeRows[firstPage];
            long encoded = 0;
            for (int p = firstPage; p <= lastPage; p++)
                encoded += map.Pages[p].UncompressedSize;

            long byteArrayBytes;
            if (decodedIndexes[i].UnencodedByteArrayDataBytes is { } unencoded
                && unencoded.Count == map.Pages.Length)
            {
                byteArrayBytes = 0;
                for (int p = firstPage; p <= lastPage; p++)
                    byteArrayBytes += unencoded[p];
            }
            else
            {
                // Without the writer's count, a page's size is only an estimate (the chunk's compression ratio over
                // its compressed size: short for a page that compresses better than the rest) and a dictionary page
                // counts indices, not strings. The column's footer size for the rows kept, as a footer-sized read
                // reserves it, stays the floor (#847 review).
                byteArrayBytes = Math.Max(encoded, (long)(ctx.Chunks[i].MetaData!.TotalUncompressedSize * keptShare));
            }
            total += ColumnDecodedBytes(ctx.Columns[i], rows, encoded, byteArrayBytes);
        }
        return (total, indexes);
    }

    /// <summary>The rows of <paramref name="rowGroup"/> that <paramref name="ranges"/> keeps; all of them for
    /// null.</summary>
    private static long KeptRows(RowGroup rowGroup, IReadOnlyList<RowRange>? ranges)
    {
        if (ranges is null)
            return rowGroup.NumRows;
        long rows = 0;
        foreach (var r in ranges)
            rows += r.End - r.Start;
        return Math.Min(rows, rowGroup.NumRows);
    }

    /// <summary>
    /// The reservation for a read's first batch, from <paramref name="groupBytes"/>, what the whole read decodes
    /// to, and <paramref name="footerBytes"/>, the footer's uncompressed size of the same read: the share of the
    /// group's rows one batch holds when the read is cut into several. Reserving the whole group for a batch a
    /// sixth its size held a wide window to a sixth of the reads the budget has room for (#829).
    /// <list type="bullet">
    /// <item><see cref="ParquetReadOptions.BatchSize"/> cuts by rows.</item>
    /// <item><see cref="ParquetReadOptions.MaxBatchByteSize"/> cuts by the pages' ENCODED uncompressed sizes, as
    /// the footer counts them, so its share is the limit over <paramref name="footerBytes"/>: a dictionary-encoded
    /// group under the limit is read in one batch however large it decodes (#845 review).</item>
    /// </list>
    /// A batch is a slice of whole decoded pages and holds their buffers, so a page larger than the batch makes the
    /// batch larger than its share of rows. How many rows a page holds is known only from an OffsetIndex
    /// (<see cref="FirstBatchFromPagesAsync"/>, used instead of this where there is one); without it, the share is never taken below <paramref name="footerBytes"/>, what a first batch reserved before (#845
    /// review): a single 100,000-row int64 page read in 1,000-row batches reserves its 800 KB, not 8 KB. A read
    /// with a nested column decodes the whole group before slicing it, so it reserves the whole group.
    /// </summary>
    internal long FirstBatchBytes(
        long groupBytes, long footerBytes, RowGroup rowGroup, SchemaDescriptor schema,
        IReadOnlyList<string>? columnNames, IReadOnlyList<RowRange>? ranges)
    {
        var (columns, _) = ResolveColumns(schema, rowGroup, columnNames);
        foreach (var column in columns)
        {
            if (column.Path.Count > 1 || column.MaxRepetitionLevel > 0)
                return groupBytes;
        }

        double share = 1.0;
        long rows = KeptRows(rowGroup, ranges);
        if (_options.BatchSize is int batchSize && batchSize > 0 && rows > batchSize)
            share = (double)batchSize / rows;
        if (_options.MaxBatchByteSize is long maxBytes && maxBytes > 0 && footerBytes > maxBytes)
            share = Math.Min(share, (double)maxBytes / footerBytes);
        long bytes = (long)(groupBytes * share);
        return Math.Max(bytes, Math.Min(footerBytes, groupBytes));
    }

    /// <summary>
    /// For each top-level column, the most leaf values a row of it holds in any row group: a column chunk's
    /// <c>num_values</c>, which counts every leaf slot (nulls and empty lists included), over its group's rows. One
    /// for a flat column; for a list or map, at least its elements a row, at any depth. For a host sizing what it adds
    /// inside nested elements (#852 review round 2). A chunk is named by the schema's leaf at its position, as the read
    /// maps it, not by its <c>path_in_schema</c>, which a footer may leave out (#852 review round 4).
    /// </summary>
    internal async ValueTask<Dictionary<string, double>> LeafValuesPerRowAsync(CancellationToken cancellationToken)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var result = new Dictionary<string, double>(StringComparer.Ordinal);
        foreach (var group in metadata.RowGroups)
        {
            if (group.NumRows <= 0)
                continue;
            for (int c = 0; c < group.Columns.Count && c < schema.Columns.Count; c++)
            {
                if (group.Columns[c].MetaData is not { } meta || schema.Columns[c].Path is not { Count: > 0 } path)
                    continue;
                double perRow = (double)meta.NumValues / group.NumRows;
                if (!result.TryGetValue(path[0], out var seen) || perRow > seen)
                    result[path[0]] = perRow;
            }
        }
        return result;
    }

    /// <summary>
    /// The most rows a read's first batch of <paramref name="rowGroup"/> can hold, for what a host adds to each:
    /// <see cref="ParquetReadOptions.BatchSize"/>, which caps every batch's rows, else every row the read keeps. A
    /// <see cref="ParquetReadOptions.MaxBatchByteSize"/> cut stops at page boundaries, so the batch's share of the
    /// encoded bytes says nothing of its rows (#850 review round 2).
    /// </summary>
    internal long FirstBatchRowBound(RowGroup rowGroup, IReadOnlyList<RowRange>? ranges)
    {
        long rows = KeptRows(rowGroup, ranges);
        return _options.BatchSize is int batchSize && batchSize > 0 ? Math.Min(batchSize, rows) : rows;
    }

    /// <summary>
    /// How much larger decoded batches have come out than their row group's estimate (<see cref="DecodedBytes"/>),
    /// across the row groups of one parallel read: the largest ratio seen, never below 1. The estimate is exact
    /// for fixed-width columns but takes a string column's bytes from the footer, which UNDERestimates
    /// dictionary-encoded strings, so without this every row group would reserve too little before its decode,
    /// not only the first. Shared by the read's producers, hence the lock.
    /// </summary>
    internal sealed class DecodedGrowth
    {
        private readonly object _gate = new();
        private double _ratio = 1.0;

        internal long Apply(long uncompressed)
        {
            double ratio;
            lock (_gate)
                ratio = _ratio;
            return (long)Math.Min(long.MaxValue / 4, uncompressed * ratio);
        }

        internal void Observe(long uncompressed, long decoded)
        {
            if (uncompressed <= 0 || decoded <= 0)
                return;
            double ratio = (double)decoded / uncompressed;
            lock (_gate)
            {
                if (ratio > _ratio)
                    _ratio = ratio;
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
    /// <param name="io">Where the column chunks are read from; the reader's file when null. A selected read's
    /// second pass reads through a file holding bytes it fetched ahead.</param>
    /// <param name="batchRows">When set, batches of exactly this many rows (fewer at a range's end) in place of
    /// <see cref="ParquetReadOptions.BatchSize"/> and <see cref="ParquetReadOptions.MaxBatchByteSize"/>, so that two
    /// reads of different columns cut the same rows into the same batches.</param>
    /// <param name="useOffsetIndex">False builds every page map by scanning page headers, never reading an
    /// OffsetIndex: for a read whose chunks are fetched whole anyway.</param>
    /// <param name="keepRows">One byte per row of the group, 1 to keep it: the only rows the batch holds. Only for a
    /// read of a whole flat group that comes back as one batch; the caller makes sure it does.</param>
    private async IAsyncEnumerable<RecordBatch> ReadBatchesAsync(
        int rowGroupIndex,
        IReadOnlyList<string>? columnNames,
        IReadOnlyList<RowRange>? ranges,
        IReadOnlyDictionary<long, byte[]>? offsetIndexes,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken,
        IRandomAccessFile? io = null,
        int? batchRows = null,
        bool useOffsetIndex = true,
        byte[]? keepRows = null,
        IReadOnlyDictionary<string, AnswerPlan>? answers = null)
    {
        var file = io ?? _file;
        int? batchSize = batchRows ?? _options.BatchSize;
        long? maxBytes = batchRows is null ? _options.MaxBatchByteSize : null;

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
        //
        // A row limit with no byte limit (a Delta table's default, #874) engages it too when a batch's share of
        // the chunk is still over the limit: the rows alone would return an array too large to build. Where the
        // row limit already brings each batch's share under it, nothing changes.
        bool implicitBudget = false;
        if (maxBytes is not > 0
            && !ctx.HasNestedColumns
            && HasChunkOverArrowLimit(ctx, batchSize))
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

        if (keepRows is not null && (!fitsInOneBatch || ctx.HasNestedColumns))
            throw new InvalidOperationException("Rows can be kept only in a flat row group read as one batch.");

        if (fitsInOneBatch)
        {
            // Row group fits in a single batch — use the optimised single-pass path.
            var buffers = await file.ReadRangesAsync(ctx.Ranges, cancellationToken)
                .ConfigureAwait(false);
            try
            {
                var results = new ColumnResult[ctx.Count];
                var wholeAnswers = keepRows is null ? DictionaryAnswersFor(ctx, answers, ctx.RowCount) : null;
                ForEachColumn(ctx.Count, i =>
                {
                    if (wholeAnswers?[i] is { } answer
                        && !AsksRowsInstead(answer,
                            () => DictionaryAnswer.FallbackRowShare(ctx.Chunks[i].MetaData!, buffers[i].Memory.Span)))
                    {
                        var result = ColumnChunkReader.ReadColumn(
                            buffers[i].Memory.Span, ctx.Columns[i],
                            ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                            validateCrc: _options.PageChecksumValidation,
                            maxPageUncompressedSize: _options.MaxPageUncompressedSize,
                            answer: answer);
                        if (result.Array is not null)
                        {
                            results[i] = result;
                            return;
                        }
                    }

                    results[i] = ColumnChunkReader.ReadColumn(
                        buffers[i].Memory.Span, ctx.Columns[i],
                        ctx.Chunks[i].MetaData!, ctx.RowCount, ctx.LeafArrowFields[i],
                        ctx.HasNestedColumns,
                        _options.PageChecksumValidation,
                        _options.FixedListFastPath,
                        _options.MaxPageUncompressedSize,
                        keepRows);
                });
                yield return AssembleRecordBatch(
                    keepRows is null ? ctx : ctx with { RowCount = results.Length > 0 ? results[0].Array.Length : 0 },
                    results, wholeAnswers);
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
                file, ctx, spans, batchSize, maxBytes, cancellationToken).ConfigureAwait(false))
            {
                yield return b;
            }
            yield break;
        }

        // Multi-batch path (flat columns): build page maps, then read only the pages needed per batch.
        //
        // Phase 1: Build each column's page map, from its OffsetIndex where it has one, otherwise by
        //          reading the whole chunk to scan its page headers, which is then kept to decode from
        //          while the chunks so read fit a budget.
        // Phase 2: Fetch the pages of as many batches as a byte budget allows in one request (a window),
        //          then for each batch decode only the pages no earlier batch has decoded, and take
        //          the batch's rows from what is decoded.
        //
        // An OffsetIndex map estimates each page's uncompressed size, which the implicit budget
        // cannot use: it exists to keep a batch under the Arrow limit, so it reads the headers.
        var (pageMaps, held) = await BuildPageMapsAsync(
                ctx, useOffsetIndex: useOffsetIndex && !implicitBudget, offsetIndexes, cancellationToken, file)
            .ConfigureAwait(false);
        var heldColumns = new bool[ctx.Count];
        for (int i = 0; i < ctx.Count; i++)
            heldColumns[i] = held[i] is not null;

        // Pages are not cut where batches are, so a page usually holds rows for more than one batch.
        // Each column keeps the decoded rows that it has not yet returned, so a page is fetched and
        // decoded by the first batch that reaches it, and at most once more (see below), rather
        // than once for every batch it overlaps (#408). That cost 8x on dictionary-encoded data,
        // whose pages hold many rows.
        var cursors = new DecodedRows[ctx.Count];
        for (int i = 0; i < ctx.Count; i++)
            cursors[i] = new DecodedRows();

        // A column answered from its dictionary (#502 phase 4b) is decoded as its indices, decided by its first
        // decode, when its dictionary is in hand: 0 undecided, 1 answered, 2 read as values.
        int spanRows = 0;
        foreach (var (start, end) in spans)
            spanRows += end - start;
        var dictionaryAnswers = keepRows is null ? DictionaryAnswersFor(ctx, answers, spanRows) : null;
        var answerModes = new byte[ctx.Count];

        // A returned column's answers, decoded beside its values, are held and returned as its values are. Made
        // before the first batch, so every skip to a range's first row moves them with the values (review round 1).
        var answerCursors = new DecodedRows?[ctx.Count];
        for (int i = 0; i < ctx.Count; i++)
        {
            if (dictionaryAnswers?[i] is { KeepsValues: true })
                answerCursors[i] = new DecodedRows();
        }

        // Where every batch falls, and which pages each decodes, is known from the page maps and the
        // decoded rows alone, so the pages of many batches are fetched in one request, a window (#622):
        // fetched batch by batch, a read returning N batches waited on N round trips one after another.
        var chunkOffsets = new long[ctx.Count];
        for (int i = 0; i < ctx.Count; i++)
            chunkOffsets[i] = ctx.Ranges[i].Offset;
        var endRows = new int[ctx.Count];
        var nextPages = new int[ctx.Count];
        Func<int, int, int> batchRowCount = (start, spanEnd) =>
            ComputeBatchRowCount(pageMaps, start, spanEnd, batchSize, maxBytes);

        int nextSpan = 0;
        int nextRow = spans[0].Start;
        BatchPageWindow? window = null;
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                for (int i = 0; i < ctx.Count; i++)
                {
                    endRows[i] = cursors[i].EndRow;
                    nextPages[i] = cursors[i].NextPage;
                }

                window?.Dispose();
                window = null;
                window = await BatchPageWindow.ReadAsync(
                        file, pageMaps, chunkOffsets, spans, nextSpan, nextRow, endRows, nextPages, batchRowCount,
                        BatchPageWindowBudgetBytes, heldColumns, cancellationToken)
                    .ConfigureAwait(false);
                if (window.Batches.Count == 0)
                    break;
                nextSpan = window.NextSpan;
                nextRow = window.NextRow;

                foreach (var (batchStartRow, batchEndRow) in window.Batches)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int actualBatchRows = batchEndRow - batchStartRow;

                    // Rows before this batch that no batch returns: the gap since the last range.
                    foreach (var cursor in cursors)
                        cursor.SkipTo(batchStartRow);
                    foreach (var cursor in answerCursors)
                        cursor?.SkipTo(batchStartRow);

                    // The pages each column still has to decode for this batch, served from the window.
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
                        var (startPage, endPage) = BatchPageWindow.PagesToDecode(
                            pageMaps[i], cursor.EndRow, cursor.NextPage, batchStartRow, batchEndRow);
                        if (startPage < 0)
                            continue;

                        startPages[i] = startPage;
                        endPages[i] = endPage;
                        var firstEntry = pageMaps[i].Pages[startPage];
                        var lastEntry = pageMaps[i].Pages[endPage];
                        long rangeStart = ctx.Ranges[i].Offset + firstEntry.Offset;
                        long rangeEnd = ctx.Ranges[i].Offset + lastEntry.Offset + lastEntry.CompressedSize;
                        dataSlots[i] = pageRanges.Count;
                        pageRanges.Add(new FileRange(rangeStart, rangeEnd - rangeStart));

                        // Side pages the map waits for come with its first pages.
                        if (pageMaps[i].PendingPrefixLength > 0)
                        {
                            prefixSlots[i] = pageRanges.Count;
                            pageRanges.Add(new FileRange(ctx.Ranges[i].Offset, pageMaps[i].PendingPrefixLength));
                        }
                    }

                    // A column whose chunk is held is decoded from it; the window holds every page the
                    // others can ask for. Anything neither holds is read here, so a planning slip costs a
                    // round trip rather than a wrong answer.
                    var pageBytes = new ReadOnlyMemory<byte>[pageRanges.Count];
                    var resolved = new bool[pageRanges.Count];
                    for (int i = 0; i < ctx.Count; i++)
                    {
                        if (held[i] is not { } chunk || dataSlots[i] < 0)
                            continue;
                        var range = pageRanges[dataSlots[i]];
                        pageBytes[dataSlots[i]] = chunk.Memory.Slice(
                            checked((int)(range.Offset - ctx.Ranges[i].Offset)), checked((int)range.Length));
                        resolved[dataSlots[i]] = true;
                    }

                    List<int>? missing = null;
                    for (int r = 0; r < pageRanges.Count; r++)
                    {
                        if (!resolved[r] && !window.TryGet(pageRanges[r], out pageBytes[r]))
                            (missing ??= []).Add(r);
                    }

                    IReadOnlyList<IMemoryOwner<byte>> pageBuffers = [];
                    if (missing is not null)
                    {
                        pageBuffers = await file.ReadRangesAsync(
                                missing.Select(r => pageRanges[r]).ToArray(), cancellationToken)
                            .ConfigureAwait(false);
                        for (int m = 0; m < missing.Count; m++)
                            pageBytes[missing[m]] = pageBuffers[m].Memory;
                    }

                    try
                    {
                        var results = new ColumnResult[ctx.Count];
                        bool[]? declined = null;
                        ForEachColumn(ctx.Count, i =>
                        {
                            var cursor = cursors[i];
                            if (dataSlots[i] >= 0)
                            {
                                if (prefixSlots[i] >= 0)
                                {
                                    pageMaps[i].CompleteSidePages(
                                        pageBytes[prefixSlots[i]].Span, ctx.Columns[i],
                                        ctx.Chunks[i].MetaData!, _options.PageChecksumValidation,
                                        _options.MaxPageUncompressedSize);
                                }

                                if (answerModes[i] == 0)
                                {
                                    answerModes[i] = dictionaryAnswers?[i] is { } answer
                                                     && !AsksRowsInstead(answer,
                                                         () => DictionaryAnswer.FallbackRowShare(ctx.Chunks[i].MetaData!, pageMaps[i]))
                                                     && answer.Prepare(pageMaps[i].Dictionary)
                                        ? (byte)1
                                        : (byte)2;
                                }

                                var buffer = pageBytes[dataSlots[i]];
                                int startPage = startPages[i];
                                ColumnResult decoded;
                                try
                                {
                                    decoded = ColumnChunkReader.ReadColumnBatchFromSlice(
                                        buffer.Span,
                                        pageMaps[i].Pages[startPage].Offset,
                                        ctx.Columns[i],
                                        ctx.Chunks[i].MetaData!,
                                        pageMaps[i],
                                        startPage, endPages[i],
                                        ctx.LeafArrowFields[i],
                                        ctx.HasNestedColumns,
                                        _options.PageChecksumValidation,
                                        _options.MaxPageUncompressedSize,
                                        answer: answerModes[i] == 1 ? dictionaryAnswers![i] : null);
                                }
                                catch (DictionaryAnswer.DeclinedException) when (answerModes[i] == 1)
                                {
                                    // A page that fell back from the dictionary could not be answered (#804). The
                                    // column is read as values from this batch on, below.
                                    lock (results)
                                        (declined ??= new bool[ctx.Count])[i] = true;
                                    return;
                                }

                                // A returned answered column holds its answers as it holds its values.
                                var answerCursor = answerModes[i] == 1 ? answerCursors[i] : null;

                                // Decoded rows that continue the held ones are appended; otherwise the
                                // pages start at or before the next row to return, and replace them.
                                if (pageMaps[i].CumulativeRows[startPage] == cursor.EndRow)
                                {
                                    cursor.Append(decoded.Array, endPages[i] + 1);
                                    answerCursor?.Append(decoded.Answers!, endPages[i] + 1);
                                }
                                else
                                {
                                    cursor.Restart(decoded.Array, pageMaps[i].CumulativeRows[startPage], endPages[i] + 1);
                                    answerCursor?.Restart(decoded.Answers!, pageMaps[i].CumulativeRows[startPage], endPages[i] + 1);
                                }
                            }

                            if (answerModes[i] == 1)
                            {
                                // The answers in place of the values, or beside them.
                                var taken = cursor.Take(actualBatchRows);
                                results[i] = answerCursors[i] is { } held && dictionaryAnswers![i]!.KeepsValues
                                    ? new ColumnResult(taken, null, null, Answers: (BooleanArray)held.Take(actualBatchRows))
                                    : new ColumnResult(taken, null, null, Answers: (BooleanArray)taken);
                            }
                            else
                            {
                                results[i] = new ColumnResult(cursor.Take(actualBatchRows), null, null);
                            }
                        });

                        // A column whose fallback page declined (#804) holds answers for rows this batch still
                        // needs, so it is decoded again as values from the page holding the batch's first row, and
                        // read as values from then on. The predicates are then asked over its live rows, which
                        // decide whether the error is raised. Earlier batches never used this page's rows: a page
                        // is decoded by the first batch that reaches it.
                        for (int i = 0; declined is not null && i < ctx.Count; i++)
                        {
                            if (!declined[i])
                                continue;

                            answerModes[i] = 2;
                            answerCursors[i]?.Dispose();
                            answerCursors[i] = null;

                            var map = pageMaps[i];
                            int firstPage = map.FindPageForRow(batchStartRow);
                            int lastPage = endPages[i];
                            ReadOnlyMemory<byte> bytes;
                            long baseOffset;
                            IMemoryOwner<byte>? refetched = null;
                            if (held[i] is { } chunk)
                            {
                                bytes = chunk.Memory;
                                baseOffset = 0;
                            }
                            else if (firstPage >= startPages[i])
                            {
                                bytes = pageBytes[dataSlots[i]];
                                baseOffset = map.Pages[startPages[i]].Offset;
                            }
                            else
                            {
                                long start = map.Pages[firstPage].Offset;
                                long end = map.Pages[lastPage].Offset + map.Pages[lastPage].CompressedSize;
                                var fetched = await file.ReadRangesAsync(
                                        [new FileRange(ctx.Ranges[i].Offset + start, end - start)], cancellationToken)
                                    .ConfigureAwait(false);
                                refetched = fetched[0];
                                bytes = refetched.Memory;
                                baseOffset = start;
                            }

                            try
                            {
                                var values = ColumnChunkReader.ReadColumnBatchFromSlice(
                                    bytes.Span, baseOffset, ctx.Columns[i], ctx.Chunks[i].MetaData!, map,
                                    firstPage, lastPage, ctx.LeafArrowFields[i], ctx.HasNestedColumns,
                                    _options.PageChecksumValidation, _options.MaxPageUncompressedSize);
                                cursors[i].Restart(values.Array, map.CumulativeRows[firstPage], lastPage + 1);
                            }
                            finally
                            {
                                refetched?.Dispose();
                            }

                            results[i] = new ColumnResult(cursors[i].Take(actualBatchRows), null, null);
                        }

                        yield return AssembleRecordBatch(
                            ctx with { RowCount = actualBatchRows }, results, dictionaryAnswers);
                    }
                    finally
                    {
                        for (int i = 0; i < pageBuffers.Count; i++)
                            pageBuffers[i].Dispose();
                    }
                }
            }
        }
        finally
        {
            window?.Dispose();
            foreach (var chunk in held)
                chunk?.Dispose();

            // The last batch is decoded, or the caller stopped early: no page of this row group is
            // decoded again, so the dictionaries go back to the pool (#644).
            ColumnPageMap.ReleaseDictionaries(pageMaps);

            // Rows decoded but not returned, when the caller stops early.
            foreach (var cursor in cursors)
                cursor.Dispose();
            foreach (var cursor in answerCursors)
                cursor?.Dispose();
        }
    }

    /// <summary>
    /// Batches a row group that contains nested columns by decoding it once and yielding row-sliced
    /// views. Batch sizing honours <paramref name="batchSize"/> and <paramref name="maxBytes"/> the
    /// same way the flat path does; only the decode strategy differs (whole chunk vs per-page). Only
    /// the rows in <paramref name="spans"/> are yielded, a batch never spanning two of them.
    /// </summary>
    private async IAsyncEnumerable<RecordBatch> ReadNestedRowGroupInBatchesAsync(
        IRandomAccessFile file,
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

        // Through the read's file, not _file: it may hold these chunks fetched ahead (#829).
        var buffers = await file.ReadRangesAsync(ctx.Ranges, cancellationToken).ConfigureAwait(false);
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

            // These maps only size the batches; ReadColumn decoded from dictionaries of its own.
            ColumnPageMap.ReleaseDictionaries(pageMaps);
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
    /// <param name="batchSize">A row limit on batches, or null: with one that splits the group, whether a batch's
    /// share of a chunk (its rows' share of the group's) is over the limit. A row limit set without a byte limit,
    /// as a Delta table's default is (#874 review), then still has an oversized chunk split by bytes, where the rows
    /// alone would leave a batch too large for one array.</param>
    private static bool HasChunkOverArrowLimit(RowGroupContext ctx, int? batchSize = null)
    {
        double share = batchSize is int rows && rows > 0 && rows < ctx.RowCount ? (double)rows / ctx.RowCount : 1.0;
        for (int i = 0; i < ctx.Count; i++)
        {
            var meta = ctx.Chunks[i].MetaData!;
            if (meta.Type == PhysicalType.ByteArray
                && meta.TotalUncompressedSize * share > Data.ByteArrayCapacity.MaxBytes)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// How many bytes of chunks read whole to scan their page headers one request may ask for, unless a
    /// single chunk is larger; and how many of them a batched read keeps to decode from, rather than
    /// fetching their pages again (#625). Tests lower it.
    /// </summary>
    internal long HeaderScanBudgetBytes { get; set; } = DefaultHeaderScanBudgetBytes;

    /// <summary>The default <see cref="HeaderScanBudgetBytes"/>.</summary>
    internal const long DefaultHeaderScanBudgetBytes = 64L * 1024 * 1024;

    /// <summary>
    /// The share of a chunk's rows on pages that fell back from the dictionary at which a column the read returns is
    /// asked row by row rather than answered from its dictionary (#804, doc/row-level-filtering.md §31). Each fallback
    /// value costs a table entry, an index and a lookup on top of the predicate. For a returned column under a cheap
    /// kernel, that outweighs what the dictionary saves once most rows have fallen back, and by then even a costly
    /// delegate gains little. A dropped column is always answered. Above 1, never. Tests and benchmarks set it.
    /// </summary>
    internal double ReturnedFallbackShareLimit { get; set; } = DefaultReturnedFallbackShareLimit;

    /// <summary>The default <see cref="ReturnedFallbackShareLimit"/>.</summary>
    internal const double DefaultReturnedFallbackShareLimit = 0.75;

    // Whether a returned column is asked row by row for the share of its chunk's rows that fell back (measured only for
    // a returned column). An unknown share keeps the dictionary.
    private bool AsksRowsInstead(DictionaryAnswer answer, Func<double?> fallbackShare) =>
        answer.KeepsValues && fallbackShare() is { } share && share >= ReturnedFallbackShareLimit;

    /// <summary>
    /// The gap between two chunks of one scan request that a coalescing reader is taken to fetch with
    /// them, and so counted against the request's budget: <see cref="CoalescingOptions.MaxGapBytes"/>'s default.
    /// </summary>
    private static readonly long s_scanGapBytes = new CoalescingOptions().MaxGapBytes;

    /// <summary>
    /// Builds the page map of each of a flat row group's columns. Where
    /// <paramref name="useOffsetIndex"/> is set and a chunk has a usable OffsetIndex, the map comes
    /// from that, so the data pages are not read to find them. Every other chunk is read whole to
    /// scan its page headers.
    /// </summary>
    /// <remarks>
    /// The chunks to scan are read together, in requests of up to <see cref="HeaderScanBudgetBytes"/>
    /// and <see cref="BatchPageWindow.MaxRangesPerWindow"/> chunks, not one request after another per
    /// column (#625). A request's bytes include the gaps between its chunks that a coalescing reader
    /// would fetch with them (unprojected columns), as its merged buffers hold those too. As long as their total stays within that budget
    /// they are also returned, by column, for the batches to decode from: their pages are already in
    /// hand, and fetching them again read every byte twice. The caller disposes them. A chunk past the
    /// budget is released once scanned, and its pages are fetched as a column with an index's are.
    /// </remarks>
    /// <param name="offsetIndexes">
    /// OffsetIndex bytes already read, by the index's file offset, as the page-index filter reads them
    /// with the ColumnIndexes it prunes by; the rest are read here.
    /// </param>
    private async ValueTask<(ColumnPageMap[] Maps, IMemoryOwner<byte>?[] Held)> BuildPageMapsAsync(
        RowGroupContext ctx, bool useOffsetIndex, IReadOnlyDictionary<long, byte[]>? offsetIndexes,
        CancellationToken cancellationToken, IRandomAccessFile? io = null)
    {
        var file = io ?? _file;
        var pageMaps = new ColumnPageMap?[ctx.Count];
        var held = new IMemoryOwner<byte>?[ctx.Count];
        long budget = HeaderScanBudgetBytes;
        long heldBytes = 0;
        try
        {
            if (useOffsetIndex)
                await BuildPageMapsFromOffsetIndexesAsync(ctx, pageMaps, offsetIndexes, cancellationToken, file).ConfigureAwait(false);

            // The chunks to scan, in file order rather than the projection's, as a coalescing reader
            // merges them: a request then spans only the gaps between its own chunks, which it counts.
            var toScan = new List<int>();
            for (int i = 0; i < ctx.Count; i++)
            {
                if (pageMaps[i] is null)
                    toScan.Add(i);
            }

            toScan.Sort((a, b) => ctx.Ranges[a].Offset.CompareTo(ctx.Ranges[b].Offset));

            int next = 0;
            while (true)
            {
                // The next request: the chunks still to scan, up to the budget and the range cap, but at
                // least one. A gap a coalescing reader would fill counts toward the budget.
                var columns = new List<int>();
                long bytes = 0;
                for (; next < toScan.Count; next++)
                {
                    var range = ctx.Ranges[toScan[next]];
                    long cost = range.Length;
                    if (columns.Count > 0)
                    {
                        var previous = ctx.Ranges[columns[^1]];
                        long gap = range.Offset - (previous.Offset + previous.Length);
                        if (gap > 0 && gap <= s_scanGapBytes)
                            cost += gap;
                        if (bytes + cost > budget || columns.Count >= BatchPageWindow.MaxRangesPerWindow)
                            break;
                    }

                    columns.Add(toScan[next]);
                    bytes += cost;
                }

                if (columns.Count == 0)
                    break;

                var buffers = await file.ReadRangesAsync(columns.ConvertAll(i => ctx.Ranges[i]), cancellationToken)
                    .ConfigureAwait(false);
                for (int k = 0; k < columns.Count; k++)
                {
                    int i = columns[k];
                    var buffer = buffers[k];
                    try
                    {
                        pageMaps[i] = PageMapBuilder.Build(
                            buffer.Memory.Span,
                            ctx.Columns[i],
                            ctx.Chunks[i].MetaData!,
                            _options.PageChecksumValidation,
                            _options.MaxPageUncompressedSize);
                    }
                    catch
                    {
                        for (int j = k; j < columns.Count; j++)
                            buffers[j].Dispose();
                        throw;
                    }

                    if (heldBytes + buffer.Memory.Length <= budget)
                    {
                        held[i] = buffer;
                        heldBytes += buffer.Memory.Length;
                    }
                    else
                    {
                        buffer.Dispose();
                    }
                }
            }
        }
        catch
        {
            foreach (var buffer in held)
                buffer?.Dispose();
            ColumnPageMap.ReleaseDictionaries(pageMaps);
            throw;
        }

        return ((ColumnPageMap[])pageMaps!, held);
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
        CancellationToken cancellationToken, IRandomAccessFile? io = null)
    {
        var file = io ?? _file;
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
            ? await file.ReadRangesAsync(prefixRanges, cancellationToken).ConfigureAwait(false)
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

        // Wrap (per batch) is handed the file's types, which a shredded variant needs to be reassembled; the batch
        // carries the types of what that produces, worked out once here.
        var batchFields = topLevelFields is null || _options.ExtensionRegistry is null
            ? topLevelFields
            : Data.VariantNestedWrapper.AssembledFields(topLevelFields);
        return new RowGroupContext(selectedColumns, selectedChunks, ranges,
            leafArrowFields, rowCount, hasNestedColumns, schemaRoot, topLevelFields, batchFields);
    }

    private RecordBatch AssembleRecordBatch(
        RowGroupContext ctx, ColumnResult[] results, DictionaryAnswer?[]? answerers = null)
    {
        if (!ctx.HasNestedColumns)
        {
            // Fast path: flat columns only
            var arrowArrays = new IArrowArray[results.Length];
            bool[]? inPlace = null;
            bool beside = false;
            for (int i = 0; i < results.Length; i++)
            {
                arrowArrays[i] = results[i].Array;
                if (results[i].Answers is not null)
                {
                    if (answerers![i]!.KeepsValues)
                        beside = true;
                    else
                        (inPlace ??= new bool[results.Length])[i] = true;
                }
            }

            var leafFields = (Field[])ctx.LeafArrowFields.Clone();
            RestoreDeclaredUnits(leafFields, arrowArrays, inPlace);

            // A column answered from its dictionary holds the predicate's answers in place of its values (#502
            // phase 4b), or, when the read returns it, its values, and the answers as one more column (phase 4d).
            if (inPlace is not null)
            {
                for (int i = 0; i < leafFields.Length; i++)
                {
                    if (inPlace[i])
                        leafFields[i] = DictionaryAnswer.AnswerField(leafFields[i].Name);
                }
            }

            if (!beside)
                return BuildRecordBatch(leafFields, arrowArrays, ctx.RowCount);

            var fields = new List<Field>(leafFields);
            var arrays = new List<IArrowArray>(arrowArrays);
            for (int i = 0; i < results.Length; i++)
            {
                if (results[i].Answers is { } answers && answerers![i]!.KeepsValues)
                {
                    fields.Add(DictionaryAnswer.AnswerField(DictionaryAnswer.AnswerColumnName(leafFields[i].Name)));
                    arrays.Add(answers);
                }
            }

            return BuildRecordBatch(fields.ToArray(), arrays.ToArray(), ctx.RowCount);
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

        // The batch's fields, not the file's: a shredded variant was reassembled above (#691).
        var topLevelFields = (Field[])ctx.BatchFields!.Clone();

        RestoreDeclaredUnits(topLevelFields, topLevelArrays);
        return BuildRecordBatch(topLevelFields, topLevelArrays, ctx.RowCount);
    }

    /// <summary>
    /// The Arrow schema the writer recorded under <c>ARROW:schema</c>, decoded once per file.
    /// Absent for a file no Arrow-aware writer produced, and for anything DuckDB wrote.
    /// </summary>
    private Apache.Arrow.Schema? DeclaredArrowSchema(Metadata.FileMetaData metadata)
    {
        var box = Volatile.Read(ref _declaredArrowSchema);
        if (box is null)
        {
            box = new StrongBox<Apache.Arrow.Schema?>(Data.ArrowSchemaMetadata.Decode(metadata.KeyValueMetadata));
            Volatile.Write(ref _declaredArrowSchema, box);
        }

        return box.Value;
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
    private void RestoreDeclaredUnits(Field[] fields, IArrowArray[] arrays, bool[]? skip = null)
    {
        // Assembly always follows a metadata read, so the footer is cached by now.
        var declared = Volatile.Read(ref _metadata) is { } metadata ? DeclaredArrowSchema(metadata) : null;
        if (declared is null)
            return;

        for (int i = 0; i < fields.Length; i++)
        {
            if (skip is not null && skip[i])
                continue;
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

    /// <summary>
    /// The columns of <paramref name="ctx"/> a selected read answers from their dictionaries (#502 phase 4b), by
    /// position: those <paramref name="answers"/> names, flat, whose chunk has dictionary-encoded pages
    /// (<see cref="DictionaryAnswer.MayAnswer"/>). Pages that fell back from the dictionary are answered from their
    /// values (#804). Null when there are none. <paramref name="rows"/> is how many rows the read decodes.
    /// </summary>
    private DictionaryAnswer?[]? DictionaryAnswersFor(
        RowGroupContext ctx, IReadOnlyDictionary<string, AnswerPlan>? answers, int rows)
    {
        if (answers is not { Count: > 0 } || ctx.HasNestedColumns)
            return null;

        DictionaryAnswer?[]? result = null;
        for (int i = 0; i < ctx.Count; i++)
        {
            var column = ctx.Columns[i];
            if (column.Path.Count == 1 && column.MaxRepetitionLevel == 0
                && answers.TryGetValue(column.Path[0], out var plan)
                && ctx.Chunks[i].MetaData is { } meta && DictionaryAnswer.MayAnswer(meta))
            {
                (result ??= new DictionaryAnswer?[ctx.Count])[i] = new DictionaryAnswer(
                    plan.Predicates, column, ctx.LeafArrowFields[i], RestoreDeclaredUnit, rows, plan.KeepValues);
            }
        }

        return result;
    }

    // One column as RestoreDeclaredUnits leaves it.
    private (Field Field, IArrowArray Array) RestoreDeclaredUnit(Field field, IArrowArray array)
    {
        var fields = new[] { field };
        var arrays = new[] { array };
        RestoreDeclaredUnits(fields, arrays);
        return (fields[0], arrays[0]);
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
        Field[]? TopLevelFields = null,
        Field[]? BatchFields = null)
    {
        public int Count => Columns.Count;
    }

    /// <summary>
    /// The leaves <paramref name="columnNames"/> select, with their chunks. For a read (<paramref name="forAssembly"/>),
    /// in the order its assembly consumes them (<see cref="NormalizeNestedSelection"/>); otherwise as named, one entry
    /// per leaf named and in that order, as <see cref="ReadPageIndexAsync"/> promises (#816 review: assembly order
    /// reordered and expanded its entries).
    /// </summary>
    private static (IReadOnlyList<ColumnDescriptor>, IReadOnlyList<ColumnChunk>) ResolveColumns(
        SchemaDescriptor schema,
        RowGroup rowGroup,
        IReadOnlyList<string>? columnNames,
        bool forAssembly = true)
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

            // First, a top-level column of that exact name: a leaf, or a group, which resolves to all its
            // descendant leaves. Before the dotted paths, because a name may hold a dot: a top-level column
            // literally named "s.a" beside a struct s with a field a is that column, not the struct's leaf,
            // whose dotted path is the same text (#718).
            foreach (var child in schema.Root.Children)
            {
                if (child.Name != name)
                    continue;
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

            if (found) continue;

            // Then a nested leaf by its dotted path.
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

            if (!found)
                throw new ArgumentException(
                    $"Column '{name}' was not found in the schema.", nameof(columnNames));
        }

        return forAssembly ? NormalizeNestedSelection(schema, rowGroup, columns, chunks) : (columns, chunks);
    }

    /// <summary>
    /// Puts the leaves a read selects in the order its assembly consumes them (#783). <see cref="NestedAssembler"/>
    /// walks each top-level column's (pruned, <see cref="PruneSchemaRoot"/>) subtree depth first, taking one leaf array
    /// per leaf, so a nested column's selected leaves must come in schema order, once each: <c>["pt.y", "pt.x"]</c>
    /// otherwise handed y's values to x, and <c>["pt.x"]</c> left the assembler looking for a y it never decoded.
    /// </summary>
    /// <remarks>
    /// <para>Top-level columns keep the order they were first named in, and each is read once, however often it or its
    /// leaves are named: the pruned root holds each top-level column once, so a flat column read twice beside a nested
    /// one handed its second copy to the nested column's first leaf (review round 1 of #815). A selection of flat
    /// columns alone is left as it is, duplicates and all, since its assembly takes each leaf as named. Within a nested
    /// column, a leaf under a list, a map, a variant or another repeated group selects
    /// the whole of the outermost such group: a map needs its key, a variant all its parts, and a list's repeated group
    /// can change meaning when its children change (a legacy two-level list). A leaf under structs alone selects just
    /// that leaf, and the struct comes back holding only the leaves selected, as Spark and arrow-rs return a nested
    /// projection.</para>
    /// </remarks>
    private static (IReadOnlyList<ColumnDescriptor>, IReadOnlyList<ColumnChunk>) NormalizeNestedSelection(
        SchemaDescriptor schema, RowGroup rowGroup, List<ColumnDescriptor> columns, List<ColumnChunk> chunks)
    {
        // Flat only when assembled flat: a bare repeated top-level column has a one-segment path but is assembled as
        // nested, against a root that holds it once (review round 2).
        if (columns.TrueForAll(c => c.Path.Count == 1 && c.MaxRepetitionLevel == 0))
            return (columns, chunks);

        var indexOf = new Dictionary<ColumnDescriptor, int>(schema.Columns.Count);
        for (int i = 0; i < schema.Columns.Count; i++)
            indexOf[schema.Columns[i]] = i;

        // Per top-level column, in the order first named: its selected leaves, in schema order, once each.
        var order = new List<string>();
        var selected = new Dictionary<string, SortedSet<int>>(StringComparer.Ordinal);
        var expanded = new HashSet<SchemaNode>(); // atomic groups already expanded: once each, not once per leaf
        foreach (var column in columns)
        {
            string top = column.Path[0];
            if (!selected.TryGetValue(top, out var leaves))
            {
                selected[top] = leaves = [];
                order.Add(top);
            }

            if (column.Path.Count > 1 && OutermostAtomicGroup(column.SchemaNode) is { } group)
            {
                if (!expanded.Add(group))
                    continue;

                var prefix = PathOf(group);
                for (int i = 0; i < schema.Columns.Count; i++)
                {
                    if (StartsWith(schema.Columns[i].Path, prefix))
                        leaves.Add(i);
                }
            }
            else
            {
                leaves.Add(indexOf[column]);
            }
        }

        var resultColumns = new List<ColumnDescriptor>(columns.Count);
        var resultChunks = new List<ColumnChunk>(columns.Count);
        foreach (var top in order)
        {
            foreach (int i in selected[top])
            {
                resultColumns.Add(schema.Columns[i]);
                resultChunks.Add(rowGroup.Columns[i]);
            }
        }

        return (resultColumns, resultChunks);
    }

    /// <summary>
    /// The outermost group above <paramref name="leaf"/> that a projection cannot cut into: a list, a map, a variant or
    /// a repeated group. Null when only structs lie above it.
    /// </summary>
    private static SchemaNode? OutermostAtomicGroup(SchemaNode leaf)
    {
        SchemaNode? outermost = null;
        for (var node = leaf.Parent; node?.Parent is not null; node = node.Parent)
        {
            if (IsAtomicGroup(node))
                outermost = node;
        }

        return outermost;
    }

    private static bool IsAtomicGroup(SchemaNode node) =>
        !node.IsLeaf
        && (node.Element.RepetitionType == FieldRepetitionType.Repeated
            || Data.ArrowSchemaConverter.IsListNode(node)
            || Data.ArrowSchemaConverter.IsMapNode(node)
            || ShreddedVariantPath.IsVariantGroup(node));

    // A node's path from the schema root, its own name last.
    private static List<string> PathOf(SchemaNode node)
    {
        var path = new List<string>();
        for (var current = node; current.Parent is not null; current = current.Parent)
            path.Add(current.Name);
        path.Reverse();
        return path;
    }

    private static bool StartsWith(IReadOnlyList<string> path, List<string> prefix)
    {
        if (path.Count < prefix.Count)
            return false;
        for (int i = 0; i < prefix.Count; i++)
        {
            if (!string.Equals(path[i], prefix[i], StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Builds a pruned schema root containing only the top-level children that have at least one descendant leaf in
    /// the selected columns, each pruned to those leaves (#783): a struct keeps only the children holding a selected
    /// leaf. A list, map, variant or repeated group is kept whole; <see cref="NormalizeNestedSelection"/> selected all
    /// of its leaves.
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

        var selected = new HashSet<SchemaNode>();
        foreach (var col in selectedColumns)
            selected.Add(col.SchemaNode);

        // Pick matching children from the full root in the requested order
        var children = new List<SchemaNode>(topLevelNames.Count);
        foreach (var name in topLevelNames)
        {
            foreach (var child in fullRoot.Children)
            {
                if (child.Name == name)
                {
                    children.Add(PruneToSelected(child, selected));
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

    /// <summary>
    /// <paramref name="node"/> with only the children that hold a selected leaf, or the node itself when nothing is cut.
    /// A copy keeps its original parent, and its kept children theirs: the assembler reads levels from the ancestors'
    /// schema elements (<c>Parent</c>), which pruning does not change.
    /// </summary>
    private static SchemaNode PruneToSelected(SchemaNode node, HashSet<SchemaNode> selected)
    {
        if (node.IsLeaf || IsAtomicGroup(node))
            return node;

        var kept = new List<SchemaNode>(node.Children.Count);
        bool cut = false;
        foreach (var child in node.Children)
        {
            if (!HoldsSelected(child, selected))
            {
                cut = true;
                continue;
            }

            var pruned = PruneToSelected(child, selected);
            cut |= !ReferenceEquals(pruned, child);
            kept.Add(pruned);
        }

        return cut
            ? new SchemaNode { Element = node.Element, Parent = node.Parent, Children = kept, AnnotationIgnored = node.AnnotationIgnored }
            : node;
    }

    private static bool HoldsSelected(SchemaNode node, HashSet<SchemaNode> selected)
    {
        if (node.IsLeaf)
            return selected.Contains(node);
        foreach (var child in node.Children)
        {
            if (HoldsSelected(child, selected))
                return true;
        }

        return false;
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
        ParquetStatisticsAccessor accessor, IReadOnlyList<string>? projection, bool[]? skipped,
        Dictionary<long, byte[]> readAhead, CancellationToken cancellationToken)
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
            if (rowGroup.NumRows <= 0 || (skipped is not null && skipped[end]))
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

    private long? _batchPageWindowBudgetBytes;

    /// <summary>
    /// How many compressed bytes one request for the pages of a row group's batches may ask for:
    /// <see cref="BatchPageWindow.ObjectStoreBudgetBytes"/> from a file that gives a concurrency hint (an object
    /// store's), else <see cref="BatchPageWindow.DefaultBudgetBytes"/>. Tests lower it to make a read span several
    /// windows.
    /// </summary>
    internal long BatchPageWindowBudgetBytes
    {
        get => _batchPageWindowBudgetBytes
            ?? (_file is IReadConcurrencyHint { PreferredConcurrentReads: >= 1 }
                ? BatchPageWindow.ObjectStoreBudgetBytes
                : BatchPageWindow.DefaultBudgetBytes);
        set => _batchPageWindowBudgetBytes = value;
    }

    /// <summary>The read-ahead for each membership source the options turn on.</summary>
    private readonly record struct Prefetches(MembershipPrefetch? Dictionary, MembershipPrefetch? BloomFilter);

    private Prefetches CreatePrefetches(
        EngineeredWood.Expressions.Predicate filter, FileMetaData metadata, SchemaDescriptor schema,
        ParquetStatisticsAccessor accessor, bool[]? skipped = null)
    {
        return new Prefetches(
            _options.FilterUseDictionaries ? Create(MembershipSource.Dictionary) : null,
            _options.FilterUseBloomFilters ? Create(MembershipSource.BloomFilter) : null);

        MembershipPrefetch Create(MembershipSource source) =>
            new(source, filter, metadata, schema, accessor, _file, _fileLength,
                _options.ColumnChunkFilePath, _options.PageChecksumValidation, MembershipPrefetchBudgetBytes,
                _options.MaxPageUncompressedSize, skipped);
    }

    /// <summary>
    /// Which row groups <paramref name="skipRows"/> skips, by index, and every group outside
    /// <paramref name="first"/> to <paramref name="end"/>; null when that is none, so the common read pays
    /// nothing. Empty groups are never asked about: they hold no rows to skip.
    /// </summary>
    private static bool[]? SkippedRowGroups(FileMetaData metadata, RowSkipPredicate? skipRows, int first = 0, int end = int.MaxValue)
    {
        int count = metadata.RowGroups.Count;
        bool[]? skipped = null;
        if (first > 0 || end < count)
        {
            skipped = new bool[count];
            for (int i = 0; i < count; i++)
                skipped[i] = i < first || i >= end;
        }
        if (skipRows is null)
            return skipped;

        long start = 0;
        for (int i = 0; i < count; i++)
        {
            long rows = metadata.RowGroups[i].NumRows;
            if (rows > 0 && !(skipped is not null && skipped[i]) && skipRows(start, rows))
                (skipped ??= new bool[count])[i] = true;
            start += rows;
        }
        return skipped;
    }

    /// <summary>A kept row group is probed for skippable runs in this many blocks.</summary>
    internal const int SkipProbeBlocks = 64;

    /// <summary>
    /// ...but in blocks of no fewer rows than this: a run shorter than a page frees no page, and cutting it
    /// only splits batches.
    /// </summary>
    internal const long MinSkipProbeBlockRows = 1024;

    /// <summary>
    /// The runs of rows, within a row group of <paramref name="rows"/> rows starting at file row
    /// <paramref name="groupStart"/>, that <paramref name="skipRows"/> says the caller needs none of, as
    /// ascending, disjoint ranges relative to the group; null when there are none worth cutting.
    /// </summary>
    /// <remarks>
    /// <para>The group is probed in <see cref="SkipProbeBlocks"/> whole blocks (a shorter remainder at the end is not
    /// probed), so a run is found only when it covers a whole block (a run of at least two blocks' rows always
    /// does, unless most of it lies in that remainder), and a read with scattered skips pays
    /// those few calls and nothing else: it needs no page index. Each run found is then widened to its true
    /// ends, by binary search within the blocks either side of it.</para>
    /// <para>Rows skipped but not cut are still returned; the caller drops them as it would without this.</para>
    /// </remarks>
    internal static List<RowRange>? SkippedRuns(RowSkipPredicate skipRows, long groupStart, long rows)
    {
        long block = Math.Max(MinSkipProbeBlockRows, (rows + SkipProbeBlocks - 1) / SkipProbeBlocks);
        if (rows <= block)
            return null;

        // Whole blocks only: a shorter last block could find a run shorter than the minimum, a cut that frees
        // no page but still costs the ranged read its page index. A run reaching into it is widened over it.
        List<RowRange>? runs = null;
        for (long start = 0; start + block <= rows; start += block)
        {
            long end = start + block;
            if (!skipRows(groupStart + start, block))
                continue;

            if (runs is { Count: > 0 } && runs[^1].End == start)
                runs[^1] = runs[^1] with { End = end };
            else
                (runs ??= []).Add(new RowRange(start, end));
        }

        if (runs is null)
            return null;

        for (int r = 0; r < runs.Count; r++)
        {
            var run = runs[r];

            // The run's first row: the lowest s, in the block before it, with [s, run.Start) all skipped.
            long lo = Math.Max(0, run.Start - block + 1), hi = run.Start;
            while (lo < hi)
            {
                long mid = lo + ((hi - lo) >> 1);
                if (skipRows(groupStart + mid, run.Start - mid))
                    hi = mid;
                else
                    lo = mid + 1;
            }

            // The run's end: the highest e, in the block after it, with [run.End, e) all skipped.
            long lo2 = run.End, hi2 = Math.Min(rows, run.End + block - 1);
            while (lo2 < hi2)
            {
                long mid = lo2 + ((hi2 - lo2 + 1) >> 1);
                if (skipRows(groupStart + run.End, mid - run.End))
                    lo2 = mid;
                else
                    hi2 = mid - 1;
            }

            runs[r] = new RowRange(lo, lo2);
        }

        return runs;
    }

    /// <summary>
    /// Whether a read of <paramref name="projection"/> can take row ranges of <paramref name="rowGroup"/> and
    /// fetch only their pages: every projected column flat, with an OffsetIndex to find its pages by. Without
    /// one, a ranged read scans the chunk's headers, reading it whole (#625), and a nested column is decoded
    /// whole anyway, so cutting rows would only cost.
    /// </summary>
    private bool CanReadRowRanges(SchemaDescriptor schema, RowGroup rowGroup, IReadOnlyList<string>? projection)
    {
        var (_, chunks) = ResolveColumns(schema, rowGroup, projection);
        return chunks.Count > 0 && ProjectedOffsetIndexRanges(schema, rowGroup, projection).Count() == chunks.Count;
    }

    /// <summary>
    /// <paramref name="ranges"/> less <paramref name="cut"/>; both ascending and disjoint, and so is the result.
    /// </summary>
    internal static List<RowRange> SubtractRanges(IReadOnlyList<RowRange> ranges, IReadOnlyList<RowRange> cut)
    {
        var result = new List<RowRange>(ranges.Count + cut.Count);
        int c = 0;
        foreach (var range in ranges)
        {
            long start = range.Start;
            while (c < cut.Count && cut[c].End <= start)
                c++;

            for (int k = c; k < cut.Count && cut[k].Start < range.End; k++)
            {
                if (cut[k].Start > start)
                    result.Add(new RowRange(start, cut[k].Start));
                start = Math.Max(start, cut[k].End);
            }

            if (start < range.End)
                result.Add(new RowRange(start, range.End));
        }

        return result;
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
        var (descriptors, chunks) = ResolveColumns(schema, metadata.RowGroups[rowGroupIndex], columnNames, forAssembly: false);

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
        int index = schema.FindLeaf(column);
        return index >= 0
            ? index
            : throw new ArgumentException($"Column '{column}' was not found in the schema.", nameof(column));
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
