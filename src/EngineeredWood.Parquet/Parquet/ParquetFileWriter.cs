// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet;

/// <summary>
/// Writes Arrow <see cref="RecordBatch"/> data to Parquet files.
/// </summary>
public sealed class ParquetFileWriter : IAsyncDisposable, IDisposable
{
    private readonly ISequentialFile _file;
    private readonly bool _ownsFile;
    private readonly ParquetWriteOptions _options;
    private readonly ParquetFileAssembler _assembler;
    private IReadOnlyList<SchemaElement>? _parquetSchema;
    private Apache.Arrow.Schema? _arrowSchema;
    private Apache.Arrow.Schema? _declaredSchema;
    private Dictionary<string, ShredSchema?>? _variantShredDecisions;
    private bool _closed;
    private bool _disposed;

    /// <summary>
    /// Set when a write to the file failed partway (its header, a row group, or the footer): the file holds part of
    /// it, so nothing more can be written to it, and it cannot be closed (#936 decision 11).
    /// </summary>
    private Exception? _fault;

    /// <summary>The row group <see cref="WriteBatchAsync"/> is filling, or null.</summary>
    private OpenRowGroup? _open;

    /// <summary>
    /// The encoded bytes per row of the last row group written, for cutting a batch that arrives before the open
    /// row group has an estimate of its own. Null before the first.
    /// </summary>
    private double? _lastBytesPerRow;

    /// <summary>
    /// The rows a batch's first slice is cut to when nothing yet says how large a row encodes: the first rows of a
    /// file. The rest of the batch is cut by the estimate those rows give.
    /// </summary>
    /// <remarks>
    /// Small, because nothing cheap bounds what a slice encodes to before it is encoded. Its Arrow size does not: a
    /// run-end encoded or narrow column can be a few bytes of Arrow and many of PLAIN pages (#938 review). A probe
    /// this size overshoots a byte limit by at most 1,024 rows; costs one more append, once a file.
    /// </remarks>
    internal const int ProbeRows = 1_024;

    /// <summary>
    /// Below this many cells (rows × columns) a batch's columns are encoded one after another rather than in
    /// parallel, where scheduling the work would cost more than it saves (#936 decision 6).
    /// </summary>
    /// <remarks>
    /// MEASURED (24 cores, the #652 lineitem shape, 16 columns, ZSTD): encoding the columns in parallel was faster
    /// than inline at every batch size tried, down to 64 rows (1,024 cells: 191 ms against 331 ms for 200,000 rows),
    /// and at 1,024 rows twice as fast. Only around 256 rows were the two within noise. So only a batch smaller than
    /// that is encoded inline.
    /// </remarks>
    internal const long InlineCells = 1 << 10;

    /// <summary>
    /// Creates a new Parquet file writer.
    /// </summary>
    /// <param name="file">The sequential file to write to.</param>
    /// <param name="ownsFile">If true, the file will be disposed when this writer is disposed.</param>
    /// <param name="options">Write options. Defaults to <see cref="ParquetWriteOptions.Default"/>.</param>
    public ParquetFileWriter(ISequentialFile file, bool ownsFile = true, ParquetWriteOptions? options = null)
    {
        _file = file;
        _ownsFile = ownsFile;
        _options = options ?? ParquetWriteOptions.Default;
        _assembler = new ParquetFileAssembler(file, _options);
    }

    /// <summary>
    /// Declares the schema to record when no row group is ever written, so that a zero-row table
    /// keeps its columns instead of producing a file whose footer declares an empty schema.
    /// </summary>
    /// <remarks>
    /// This is a footer fallback only. The moment a row group is written, the schema captured from
    /// that batch — after any variant shredding, which changes column types — wins and this
    /// declaration is ignored. It therefore cannot desynchronize the footer from the encoded data.
    /// </remarks>
    /// <param name="schema">The Arrow schema the empty file should declare.</param>
    public void DeclareSchema(Apache.Arrow.Schema schema)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(schema);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
        if (schema is null) throw new ArgumentNullException(nameof(schema));
#endif
        if (_closed)
            throw new InvalidOperationException("Writer has been closed.");

        _declaredSchema = schema;
    }

    /// <summary>
    /// Writes a row group from the given <see cref="RecordBatch"/>.
    /// The schema is inferred from the first batch; subsequent batches must have the same schema.
    /// If the batch exceeds <see cref="ParquetWriteOptions.RowGroupMaxRows"/>, it is automatically
    /// split into multiple row groups.
    /// </summary>
    /// <remarks>
    /// A row group <see cref="WriteBatchAsync"/> has open is written first, so the rows stay in the order they were
    /// given. This batch does not join it: it is cut only by <see cref="ParquetWriteOptions.RowGroupMaxRows"/>.
    /// </remarks>
    /// <exception cref="ArgumentException">
    /// The batch holds a null, at any depth, in a column that is required: a non-nullable field or a map key in the
    /// first batch, or a column the first batch made required in a later one. Or a batch after the first differs
    /// from it in column names, order or types at any depth; field metadata and list/map child names may differ.
    /// </exception>
    public async ValueTask WriteRowGroupAsync(
        RecordBatch batch,
        CancellationToken cancellationToken = default)
    {
        EnsureWritable();
        cancellationToken.ThrowIfCancellationRequested();

        // The caller's batch, rooted until every row group of it is written: CompactSlicedColumns and
        // SliceBatch can hand the encoders ZeroOffsetView views onto its buffers, and a view does not keep
        // them alive (see ZeroOffsetView). The reassignments of `batch` below would otherwise leave the
        // caller's arrays unreachable mid-write.
        var source = batch;
        try
        {
            batch = PrepareBatch(batch);
            int maxRows = _options.RowGroupMaxRows;
            if (batch.Length > maxRows)
                EnsureWholeBatchMatches(batch);
            await WriteHeaderAsync(cancellationToken).ConfigureAwait(false);
            await FlushOpenRowGroupAsync(cancellationToken).ConfigureAwait(false);

            // Auto-split large batches into multiple row groups
            if (batch.Length > maxRows)
            {
                int offset = 0;
                while (offset < batch.Length)
                {
                    int length = Math.Min(maxRows, batch.Length - offset);
                    var slice = SliceBatch(batch, offset, length);
                    await WriteSingleRowGroupAsync(slice, cancellationToken).ConfigureAwait(false);
                    offset += length;
                }
                return;
            }

            await WriteSingleRowGroupAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            GC.KeepAlive(source);
        }
    }

    /// <summary>
    /// Adds a batch's rows to the row group being filled, encoding them into pages as they arrive, and writes the
    /// row group once it is full. Only the open row group's encoded pages are held, not the batches.
    /// </summary>
    /// <remarks>
    /// <para>A row group is cut at <see cref="ParquetWriteOptions.RowGroupMaxRows"/> rows, or once it reaches
    /// <see cref="ParquetWriteOptions.RowGroupMaxBytes"/> by an estimate of its encoded size: the pages written plus
    /// the open pages and the dictionary pages at their uncompressed size. A batch that would overfill it is split:
    /// by rows exactly, and by bytes at the encoded bytes per row so far. <see cref="FlushRowGroupAsync"/> cuts one
    /// early, and <see cref="CloseAsync"/> writes the last.</para>
    /// <para>The batch is the caller's again once this returns: nothing of it is kept. The schema is captured from
    /// the first batch, as <see cref="WriteRowGroupAsync"/> does, and later batches must match it.</para>
    /// <para>If encoding fails, or the write is cancelled once the batch has rows in the open row group, the open row
    /// group is discarded, including the rows of earlier batches in it, and the writer stays usable: nothing of that
    /// row group reaches the file (rows of the batch in a row group already written stay). A batch refused before encoding (a
    /// schema mismatch, a null in a required column) leaves the open row group as it was. If writing to the file fails
    /// (a row group, or in <see cref="CloseAsync"/> the footer), the writer is unusable afterwards.</para>
    /// </remarks>
    /// <exception cref="ArgumentException">As for <see cref="WriteRowGroupAsync"/>.</exception>
    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken cancellationToken = default) =>
        await WriteBatchCoreAsync(batch, stopAfterRowGroup: false, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// <see cref="WriteBatchAsync"/>, stopping once a row group has been written: returns the rows of the batch taken,
    /// all of them unless a row group filled first. For a caller that cuts files between row groups.
    /// </summary>
    internal ValueTask<int> WriteBatchUntilRowGroupAsync(RecordBatch batch, CancellationToken cancellationToken = default) =>
        WriteBatchCoreAsync(batch, stopAfterRowGroup: true, cancellationToken);

    private async ValueTask<int> WriteBatchCoreAsync(
        RecordBatch batch, bool stopAfterRowGroup, CancellationToken cancellationToken)
    {
        EnsureWritable();
        cancellationToken.ThrowIfCancellationRequested();

        var source = batch;
        try
        {
            batch = PrepareBatch(batch);

            // An empty batch adds no rows, but its schema is captured, or checked against the captured one, as any
            // other batch's is: otherwise a file whose first batch was empty took a later batch of another shape.
            if (batch.Length == 0)
            {
                ShredAndCheck(batch);
                return 0;
            }

            // A batch that will be cut is checked whole first: checked a slice at a time, a refusal of a later slice
            // would come after earlier ones had joined the open row group, or been written.
            int firstRows = RowsToAppend(batch, batch.Length);
            if (firstRows < batch.Length)
                EnsureWholeBatchMatches(batch);

            await WriteHeaderAsync(cancellationToken).ConfigureAwait(false);

            int offset = 0;
            bool batchInOpenRowGroup = false;
            try
            {
                while (offset < batch.Length)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int rows = offset == 0 ? firstRows : RowsToAppend(batch, batch.Length - offset);
                    var slice = offset == 0 && rows == batch.Length ? batch : SliceBatch(batch, offset, rows);
                    AppendToOpenRowGroup(slice);
                    batchInOpenRowGroup = true;
                    offset += rows;

                    if (_open!.Rows >= _options.RowGroupMaxRows || _open.EstimatedBytes() >= _options.RowGroupMaxBytes)
                    {
                        batchInOpenRowGroup = false;
                        await FlushOpenRowGroupAsync(cancellationToken).ConfigureAwait(false);
                        if (stopAfterRowGroup)
                            break;
                    }
                }
            }
            catch when (batchInOpenRowGroup)
            {
                // Cancelled or failed once rows of this batch were in the open row group: they cannot be taken back
                // out, so the open row group goes, as when encoding fails (#936 decision 11). Rows of the batch in a
                // row group already written stay in the file.
                _open = null;
                throw;
            }

            return offset;
        }
        finally
        {
            GC.KeepAlive(source);
        }
    }

    /// <summary>
    /// Writes the row group <see cref="WriteBatchAsync"/> is filling now, before it is full. Does nothing when no
    /// rows are open.
    /// </summary>
    public async ValueTask FlushRowGroupAsync(CancellationToken cancellationToken = default)
    {
        EnsureWritable();
        await FlushOpenRowGroupAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The rows <see cref="WriteBatchAsync"/> has added to the open row group: not yet in the file.</summary>
    public long OpenRowGroupRows => _open?.Rows ?? 0;

    /// <summary>
    /// The estimated encoded size of the open row group, which is about what the writer holds for it: the pages
    /// written so far, and the open pages and dictionary pages at their uncompressed size.
    /// </summary>
    public long OpenRowGroupBytes => _open?.EstimatedBytes() ?? 0;

    private void EnsureWritable()
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        if (_closed)
            throw new InvalidOperationException("Writer has been closed.");
        if (_fault is not null)
            throw new InvalidOperationException("An earlier write failed partway into the file; the writer can take no more.", _fault);
    }

    /// <summary>
    /// Brings a batch into the shape the encoders read, and checks it, before anything of it is encoded. A batch
    /// refused here leaves the writer as it was.
    /// </summary>
    private RecordBatch PrepareBatch(RecordBatch batch)
    {
        // A SLICED column (Arrow's zero-copy sub-range view: Data.Offset != 0) must be compacted before
        // anything reads its buffers. The encoders below index the raw value buffer from slot 0, so a view
        // starting at row N silently writes rows 0..len instead of N..N+len — and the def levels, which come
        // from the offset-aware IsNull, stay correct, so the file is well-formed and merely holds the wrong
        // rows. Only paid when a column actually carries an offset.
        batch = CompactSlicedColumns(batch);

        // Nothing below enforces the nullable flag: a required column is encoded without definition levels, so a
        // null in it would be written as a value. Later batches get this from RowGroupSchemaCheck.EnsureMatches.
        // Checked before the declared schema and the shredding layout are captured, so a refused first batch
        // leaves the writer as it was.
        if (_arrowSchema is null)
        {
            RowGroupSchemaCheck.EnsureWritable(batch);
        }

        // Parquet has no second-precision unit, so such a column is rescaled to milliseconds here —
        // before the schema is captured, so what the footer declares and what the encoders write are
        // the same thing. The caller's original units are preserved separately, in ARROW:schema, and
        // that is what lets the reader hand them back.
        var declared = batch.Schema;
        batch = Data.TimeUnitRescaler.ToParquetUnits(batch);
        // Likewise large and view byte arrays become string and binary, the one layout the encoders read (#481).
        // It can refuse a batch, so the declared schema is kept only once it has succeeded: a refused first batch
        // must not leave its schema behind for the footer's ARROW:schema.
        batch = Data.ByteArrayLayoutNormalizer.ToOffsetLayout(batch);
        _declaredSchema ??= declared;

        // Variant shredding, if enabled, changes each shredded column's storage TYPE — so the layout
        // has to be decided before the schema is captured, and from the same batch. Decided once and
        // reused: a parquet file has one schema, and a later batch that re-inferred a different shape
        // would be encoded against this one. Applied per row group in WriteSingleRowGroupAsync, after
        // the split below, so a shredded array never goes through MaterializeSlice.
        EnsureVariantShredDecisions(batch);
        return batch;
    }

    /// <summary>
    /// Shreds a row group's (or a slice's) variants and checks it against the captured schema, capturing it from the
    /// first: after shredding, so the declared schema is the one the column writers encode.
    /// </summary>
    private RecordBatch ShredAndCheck(RecordBatch batch)
    {
        batch = ApplyVariantShredding(batch);

        if (_arrowSchema is null)
        {
            _arrowSchema = batch.Schema;
            _parquetSchema = ArrowToSchemaConverter.Convert(_arrowSchema, _options);
        }
        else
        {
            // Every later row group is encoded against the captured schema, so one of another shape would be
            // written as silently wrong data. Checked before anything of this row group is written.
            RowGroupSchemaCheck.EnsureMatches(_arrowSchema, batch);
        }

        return batch;
    }

    /// <summary>
    /// Checks a batch that will be cut into several pieces against the captured schema, whole, before any piece is
    /// written. The first batch of a file needs no more than <see cref="PrepareBatch"/> checked: it sets the schema.
    /// </summary>
    private void EnsureWholeBatchMatches(RecordBatch batch)
    {
        if (_arrowSchema is not null)
            RowGroupSchemaCheck.EnsureMatches(_arrowSchema, ApplyVariantShredding(batch));
    }

    private async ValueTask WriteSingleRowGroupAsync(
        RecordBatch batch,
        CancellationToken cancellationToken)
    {
        batch = ShredAndCheck(batch);

        // Each leaf column's chunk is encoded by its own ColumnChunkWriter: one append of this batch, then
        // finish. The unit of parallel work is a top-level column's append (#936 decision 6): its leaves run
        // in order on one thread, since a nested column is decomposed into all of them at once, and the
        // columns run in parallel.
        int arrowColumnCount = batch.ColumnCount;
        var perColumnLeaves = new ColumnChunkWriter.ColumnChunkResult[arrowColumnCount][];

        Parallel.For(0, arrowColumnCount, i =>
        {
            var writers = new ColumnChunkWriter?[1][];
            AppendColumn(batch, i, writers, 0, bloomFilterExpectedValues: null, detach: false);
            var results = new ColumnChunkWriter.ColumnChunkResult[writers[0].Length];
            for (int l = 0; l < results.Length; l++)
                results[l] = writers[0][l]!.Finish();
            perColumnLeaves[i] = results;
        });

        await WriteChunksAsync(perColumnLeaves, batch.Length, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Appends one top-level column of a batch to its leaf writers (<c>writers[slot]</c>), opening them on the first
    /// append: one for a flat column, one per leaf for a nested one.
    /// </summary>
    private void AppendColumn(
        RecordBatch batch, int column, ColumnChunkWriter?[][] writers, int slot, long? bloomFilterExpectedValues,
        bool detach)
    {
        var field = _arrowSchema!.FieldsList[column];
        var array = batch.Column(column);

        if (IsNestedType(field.DataType))
        {
            // Nested: decompose into leaf columns with def/rep levels
            var leaves = NestedLevelWriter.Decompose(array, field, batch.Length);
            var leafWriters = writers[slot] ??= new ColumnChunkWriter?[leaves.Count];
            if (leafWriters.Length != leaves.Count)
                throw new InvalidOperationException($"Column '{field.Name}' decomposed into a different number of leaves.");

            for (int l = 0; l < leaves.Count; l++)
            {
                var leaf = leaves[l];
                var writer = leafWriters[l] ??= new ColumnChunkWriter(
                    leaf.PathInSchema, leaf.PhysicalType, leaf.TypeLength,
                    leaf.MaxDefLevel, leaf.MaxRepLevel, _options, bloomFilterExpectedValues);
                writer.Append(leaf.Array, leaf.DefLevels, leaf.RepLevels, leaf.NonNullCount, leaf.LevelCount);
                if (detach)
                    writer.Detach();
            }
        }
        else
        {
            // Flat: resolve physical type from schema
            var leafWriters = writers[slot] ??= new ColumnChunkWriter?[1];
            var writer = leafWriters[0] ??= NewFlatWriter(field, bloomFilterExpectedValues);
            writer.AppendFlat(array);
            if (detach)
                writer.Detach();
        }
    }

    private ColumnChunkWriter NewFlatWriter(Field field, long? bloomFilterExpectedValues)
    {
        var element = FindLeafElement(_parquetSchema!, field.Name);
        return new ColumnChunkWriter(
            new[] { field.Name }, element.Type!.Value, element.TypeLength ?? 0,
            maxDefLevel: field.IsNullable ? 1 : 0, maxRepLevel: 0, _options, bloomFilterExpectedValues);
    }

    /// <summary>
    /// The rows of a batch to append to the open row group next: as many as fit by rows, and by bytes at the encoded
    /// bytes per row so far (or the last row group's). With no estimate at all, the first rows of a file, a probe's
    /// worth, which make one: at most <see cref="ProbeRows"/>, and fewer for rows whose Arrow size alone would fill the
    /// byte limit.
    /// </summary>
    private int RowsToAppend(RecordBatch batch, int remaining)
    {
        long openRows = _open?.Rows ?? 0;
        long take = Math.Min(remaining, Math.Max(1, _options.RowGroupMaxRows - openRows));

        long openBytes = _open?.EstimatedBytes() ?? 0;
        double? bytesPerRow = openRows > 0 ? (double)openBytes / openRows : _lastBytesPerRow;
        if (bytesPerRow is not { } perRow)
        {
            take = Math.Min(take, ProbeRows);
            long arrowBytes = EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(batch);
            if (arrowBytes > 0)
                take = WithinBytes(take, _options.RowGroupMaxBytes, (double)arrowBytes / batch.Length, roundUp: false);
        }
        else if (perRow > 0)
        {
            take = WithinBytes(take, Math.Max(0, _options.RowGroupMaxBytes - openBytes), perRow, roundUp: true);
        }

        return (int)take;
    }

    /// <summary>
    /// <paramref name="take"/>, or fewer rows if fewer fit in <paramref name="byteRoom"/> at <paramref name="bytesPerRow"/>;
    /// at least one. Compared as doubles before any cast: a byte limit near <see cref="long.MaxValue"/> (cut by rows
    /// only) would overflow the cast, and a writer that took one row per slice is #870 again.
    /// </summary>
    internal static long WithinBytes(long take, long byteRoom, double bytesPerRow, bool roundUp)
    {
        double rows = byteRoom / bytesPerRow;
        rows = roundUp ? Math.Ceiling(rows) : Math.Floor(rows);
        return rows >= take ? take : Math.Max(1, (long)rows);
    }

    /// <summary>
    /// Encodes a batch (or a slice of one) into the open row group, opening it if need be. A failure while encoding
    /// discards the open row group: its column writers have taken different parts of the batch.
    /// </summary>
    private void AppendToOpenRowGroup(RecordBatch batch)
    {
        // Refusals here (shredding, a schema mismatch) come before any column is touched: the open row group stays.
        batch = ShredAndCheck(batch);
        var open = _open ??= new OpenRowGroup(batch.ColumnCount);
        long bloomHint = _options.RowGroupMaxRows;

        try
        {
            if ((long)batch.Length * batch.ColumnCount < InlineCells)
            {
                for (int i = 0; i < batch.ColumnCount; i++)
                    AppendColumn(batch, i, open.Writers, i, bloomHint, detach: true);
            }
            else
            {
                Parallel.For(0, batch.ColumnCount, i => AppendColumn(batch, i, open.Writers, i, bloomHint, detach: true));
            }
        }
        catch
        {
            _open = null;
            throw;
        }

        open.Rows += batch.Length;
    }

    /// <summary>Finishes the open row group's column chunks and writes them to the file.</summary>
    private async ValueTask FlushOpenRowGroupAsync(CancellationToken cancellationToken)
    {
        if (_open is not { } open)
            return;
        _open = null;

        var perColumnLeaves = new ColumnChunkWriter.ColumnChunkResult[open.Writers.Length][];
        Parallel.For(0, open.Writers.Length, i =>
        {
            var writers = open.Writers[i]!;
            var results = new ColumnChunkWriter.ColumnChunkResult[writers.Length];
            for (int l = 0; l < writers.Length; l++)
                results[l] = writers[l]!.Finish();
            perColumnLeaves[i] = results;
        });

        await WriteChunksAsync(perColumnLeaves, open.Rows, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a row group's encoded chunks, in schema order. A failure here leaves part of the row group in the file,
    /// so the writer takes nothing more.
    /// </summary>
    private async ValueTask WriteChunksAsync(
        ColumnChunkWriter.ColumnChunkResult[][] perColumnLeaves, long rows, CancellationToken cancellationToken)
    {
        // Flatten results in schema order
        var allLeafResults = new List<ColumnChunkWriter.ColumnChunkResult>();
        long bytes = 0;
        foreach (var results in perColumnLeaves)
        {
            allLeafResults.AddRange(results);
            foreach (var leaf in results)
                bytes += leaf.MetaData.TotalCompressedSize;
        }

        // Streamed or written whole, the latest row group is the best measure for cutting the next batch (#938 review).
        if (rows > 0)
            _lastBytesPerRow = (double)bytes / rows;

        // Written sequentially, since each chunk's offsets depend on the one before it.
        try
        {
            await _assembler.WriteRowGroupAsync(allLeafResults, rows, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _fault = e;
            throw;
        }
    }

    /// <summary>The row group <see cref="WriteBatchAsync"/> is filling: a writer per leaf column, by top-level column.</summary>
    private sealed class OpenRowGroup(int columnCount)
    {
        public ColumnChunkWriter?[][] Writers { get; } = new ColumnChunkWriter?[columnCount][];

        public long Rows { get; set; }

        public long EstimatedBytes()
        {
            long bytes = 0;
            foreach (var leaves in Writers)
            {
                if (leaves is null)
                    continue;
                foreach (var writer in leaves)
                    bytes += writer?.EstimatedSize ?? 0;
            }
            return bytes;
        }
    }

    /// <summary>
    /// Materializes any column that is an offset-based VIEW onto a larger array, so every column's row 0 is
    /// its buffer's slot 0 — the layout the whole write path assumes.
    ///
    /// <para>Returns the batch unchanged when no column carries an offset, which is the common case: arrays
    /// built by a builder, read back by the parquet reader, or gathered by <c>ArrowCompute.Take</c> all start
    /// at 0.</para>
    ///
    /// <para>Most columns are re-based by <see cref="ZeroOffsetView"/>, which views the same value buffers from
    /// the slice's first row and rebuilds only offsets and unaligned bitmaps (#667). The rest are gathered by
    /// <c>Take</c> with the identity selection, which applies the source's offset in every one of its per-type
    /// gathers. A type it cannot gather raises <see cref="NotSupportedException"/> from there rather than
    /// being written wrong.</para>
    /// </summary>
    private static RecordBatch CompactSlicedColumns(RecordBatch batch)
    {
        bool anySliced = false;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (batch.Column(i).Data.Offset != 0)
            {
                anySliced = true;
                break;
            }
        }

        if (!anySliced)
            return batch;

        int[]? identity = null;
        var columns = new IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var column = batch.Column(i);
            if (column.Data.Offset == 0)
            {
                columns[i] = column;
                continue;
            }

            // A view at offset 0 where the type allows one, copying no values (#667); a gather otherwise.
            if (ZeroOffsetView.TrySlice(column, 0, column.Length) is { } view)
            {
                columns[i] = view;
                continue;
            }

            if (identity is null)
            {
                identity = new int[batch.Length];
                for (int r = 0; r < identity.Length; r++)
                    identity[r] = r;
            }
            columns[i] = EngineeredWood.Arrow.ArrowCompute.Take(column, identity);
        }

        return new RecordBatch(batch.Schema, columns, batch.Length);
    }

    /// <summary>
    /// Decides, once per file, which top-level variant columns are shredded and into what layout.
    /// A null entry records a column deliberately left unshredded, so the decision is not re-taken on
    /// every batch.
    /// </summary>
    private void EnsureVariantShredDecisions(RecordBatch batch)
    {
        if (_options.ShredVariants is null && _options.VariantShredSchemas is null)
            return; // shredding disabled
        if (_variantShredDecisions is not null)
            return; // already decided, from the first batch

        var decisions = new Dictionary<string, ShredSchema?>(StringComparer.Ordinal);
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (batch.Column(i) is not VariantArray variant)
                continue;

            string name = batch.Schema.FieldsList[i].Name;
            if (_options.VariantShredSchemas is not null
                && _options.VariantShredSchemas.TryGetValue(name, out var declared))
            {
                decisions[name] = declared; // an explicit layout shreds even where inference would decline
                continue;
            }

            decisions[name] = _options.ShredVariants is null
                ? null
                : VariantShredding.InferSchema(variant, _options.ShredVariants);
        }

        _variantShredDecisions = decisions;
    }

    /// <summary>
    /// Replaces each variant column that has a shred layout with its shredded form, rebuilding the
    /// batch's schema to match — the storage struct gains a <c>typed_value</c> child, so the Arrow
    /// field type changes with it.
    /// </summary>
    private RecordBatch ApplyVariantShredding(RecordBatch batch)
    {
        if (_variantShredDecisions is null || _variantShredDecisions.Count == 0)
            return batch;

        IArrowArray[]? columns = null;
        Field[]? fields = null;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (batch.Column(i) is not VariantArray variant)
                continue;

            var field = batch.Schema.FieldsList[i];
            if (!_variantShredDecisions.TryGetValue(field.Name, out var schema) || schema is null)
                continue;

            if (columns is null)
            {
                columns = new IArrowArray[batch.ColumnCount];
                fields = new Field[batch.ColumnCount];
                for (int j = 0; j < batch.ColumnCount; j++)
                {
                    columns[j] = batch.Column(j);
                    fields[j] = batch.Schema.FieldsList[j];
                }
            }

            var shredded = VariantShredding.Shred(variant, schema);
            columns[i] = shredded;
            fields![i] = new Field(field.Name, shredded.Data.DataType, field.IsNullable, field.Metadata);
        }

        return columns is null
            ? batch
            : new RecordBatch(
                new Apache.Arrow.Schema(fields!, batch.Schema.Metadata), columns, batch.Length);
    }

    /// <summary>
    /// Creates a zero-offset copy of a batch slice.
    /// Arrow's <c>Array.Slice</c> creates offset-based views, but the write path
    /// reads value buffers from index 0 — so we must materialize each slice.
    /// </summary>
    private static RecordBatch SliceBatch(RecordBatch batch, int offset, int length)
    {
        var arrays = new IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
            arrays[i] = MaterializeSlice(batch.Column(i), offset, length);
        return new RecordBatch(batch.Schema, arrays, length);
    }

    private static IArrowArray MaterializeSlice(IArrowArray array, int offset, int length)
    {
        // Roots the caller's array across the copy. The arms below read raw spans off its buffers, and a
        // span is not a GC reference to what it points at; the many returns are why this is a wrapper
        // rather than a trailing GC.KeepAlive. See doc/arrow-span-lifetime.md.
        var sliced = MaterializeSliceCore(array, offset, length);
        GC.KeepAlive(array);
        return sliced;
    }

    private static IArrowArray MaterializeSliceCore(IArrowArray array, int offset, int length)
    {
        // Most types are re-based in place, copying no values, with every child cut to the slice's rows: the
        // layout the cases below build by copying (#667). They remain for the types it declines.
        if (ZeroOffsetView.TrySlice(array, offset, length) is { } view)
            return view;

        // A NESTED column is gathered rather than sliced, and — unlike the flat cases below — it is gathered
        // even at offset zero.
        //
        // A nested column has no flat value buffer for CopyArray to slice: a struct's children are separate
        // arrays that are not sliced along with their parent, and a list-shaped column reaches its child
        // through its offsets buffer. But taking Arrow's zero-copy VIEW is not enough either, which is why
        // the offset-zero shortcut below cannot cover this. A view narrows the PARENT's row range and leaves
        // the children whole, so the first row group of a split list column still hands the leaf every row in
        // the batch while the def levels describe only this group's — measured as an IndexOutOfRangeException
        // out of StatisticsCollector. Take rebuilds the children down to exactly the selected rows, which is
        // what the write path assumes throughout. (MapArray derives from ListArray, so a map is covered by
        // the list pattern here.)
        if (array is StructArray or ListArray or LargeListArray or FixedSizeListArray)
        {
            var indices = new int[length];
            for (int i = 0; i < length; i++)
                indices[i] = offset + i;
            return EngineeredWood.Arrow.ArrowCompute.Take(array, indices);
        }

        // An EXTENSION column (VARIANT, GUID) cannot take either path below: Apache.Arrow's
        // ExtensionArray derives from object, not from Array, so the cast at the end throws
        // InvalidCastException — which is what an auto-split batch carrying a variant or guid column
        // used to do. Materialize the STORAGE and re-wrap, so the extension type survives the split.
        if (array is ExtensionArray extension)
        {
            var storage = MaterializeSliceCore(extension.Storage, offset, length);
            return extension.ExtensionType.CreateArray(storage);
        }

        // A run-end encoded column splits by RUN, not by row: the slice is a view whose children still hold
        // every run in the original, and compacting re-clips them. O(runs), so auto-splitting a constant
        // column across row groups costs a few bytes per group rather than a copy of every row in it.
        if (array is RunEndEncodedArray ree)
        {
            return EngineeredWood.Arrow.RunEndEncoding.Compact(
                (RunEndEncodedArray)ree.Slice(offset, length));
        }

        // Use Arrow's builder pattern to create a zero-offset copy of the slice
        var sliced = ((Apache.Arrow.Array)array).Slice(offset, length);
        var slicedData = sliced.Data;

        if (slicedData.Offset == 0)
            return sliced; // Already zero-offset (e.g., first slice)

        // For non-zero offset slices, create a compact copy.
        // The simplest approach: build new ArrayData with copied buffers.
        return CopyArray(slicedData, array.Data.DataType, length);
    }

    private static IArrowArray CopyArray(ArrayData data, Apache.Arrow.Types.IArrowType type, int length)
    {
        // For fixed-width types: copy value buffer, bitmap
        // For variable-width types: copy offsets + data buffer, bitmap
        int srcOffset = data.Offset;

        // The branch is decided by whether a validity bitmap EXISTS, never by the slice's null count.
        // `data` is a slice, and a slice whose parent had nulls carries an UNKNOWN (negative) count — so a
        // `nullCount > 0` test silently fails for exactly the arrays that need the copy, and the bitmap was
        // taken unshifted: row group N then read its validity from bit 0 instead of bit srcOffset, and every
        // row from the first row-group boundary onward was aligned against the wrong mask (issue #155).
        //
        // The copy walks the slice's bits anyway, so it counts the nulls on the way through rather than
        // asking Arrow to recompute them afterwards.
        ArrowBuffer newBitmap = ArrowBuffer.Empty;
        int nullCount = 0;
        if (data.Buffers.Length > 0 && data.Buffers[0].Length > 0)
        {
            var bitmapBytes = new byte[(length + 7) / 8];
            var srcBitmap = data.Buffers[0].Span;
            for (int i = 0; i < length; i++)
            {
                bool isSet = (srcBitmap[(srcOffset + i) / 8] & (1 << ((srcOffset + i) % 8))) != 0;
                if (isSet)
                    bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
                else
                    nullCount++;
            }

            // A null-free slice of a nullable column keeps no bitmap: absent validity means all-valid in
            // Arrow, and it is what the rest of the write path already sees for a column with no nulls.
            // This is reachable on its own — a column whose nulls all fall in EARLIER row groups.
            if (nullCount > 0)
                newBitmap = new ArrowBuffer(bitmapBytes);
        }

        switch (type)
        {
            case Apache.Arrow.Types.BooleanType:
            {
                var boolBytes = new byte[(length + 7) / 8];
                var srcValues = data.Buffers[1].Span;
                for (int i = 0; i < length; i++)
                {
                    bool val = (srcValues[(srcOffset + i) / 8] & (1 << ((srcOffset + i) % 8))) != 0;
                    if (val) boolBytes[i / 8] |= (byte)(1 << (i % 8));
                }
                var boolData = new ArrayData(type, length, nullCount, 0,
                    [newBitmap, new ArrowBuffer(boolBytes)]);
                return Apache.Arrow.ArrowArrayFactory.BuildArray(boolData);
            }

            case Apache.Arrow.Types.FixedSizeBinaryType fsb:
            {
                int byteWidth = fsb.ByteWidth;
                var src = data.Buffers[1].Span.Slice(srcOffset * byteWidth, length * byteWidth);
                var newData = new ArrayData(type, length, nullCount, 0,
                    [newBitmap, new ArrowBuffer(src.ToArray())]);
                return Apache.Arrow.ArrowArrayFactory.BuildArray(newData);
            }

            case Apache.Arrow.Types.StringType or Apache.Arrow.Types.BinaryType:
            {
                var srcOffsets = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
                int dataStart = srcOffsets[srcOffset];
                int dataEnd = srcOffsets[srcOffset + length];

                var newOffsets = new int[length + 1];
                for (int i = 0; i <= length; i++)
                    newOffsets[i] = srcOffsets[srcOffset + i] - dataStart;

                var srcData = data.Buffers[2].Span.Slice(dataStart, dataEnd - dataStart);
                var offsetBytes = System.Runtime.InteropServices.MemoryMarshal.AsBytes(newOffsets.AsSpan()).ToArray();
                var newData = new ArrayData(type, length, nullCount, 0,
                    [newBitmap, new ArrowBuffer(offsetBytes), new ArrowBuffer(srcData.ToArray())]);
                return Apache.Arrow.ArrowArrayFactory.BuildArray(newData);
            }

            default:
            {
                // Fixed-width numeric types (int, long, float, double, etc.)
                int byteWidth = type switch
                {
                    Apache.Arrow.Types.Int8Type or Apache.Arrow.Types.UInt8Type => 1,
                    Apache.Arrow.Types.Int16Type or Apache.Arrow.Types.UInt16Type or Apache.Arrow.Types.HalfFloatType => 2,
                    Apache.Arrow.Types.Int32Type or Apache.Arrow.Types.UInt32Type or Apache.Arrow.Types.FloatType
                        or Apache.Arrow.Types.Date32Type or Apache.Arrow.Types.Time32Type => 4,
                    Apache.Arrow.Types.Int64Type or Apache.Arrow.Types.UInt64Type or Apache.Arrow.Types.DoubleType
                        or Apache.Arrow.Types.Date64Type or Apache.Arrow.Types.Time64Type
                        or Apache.Arrow.Types.TimestampType or Apache.Arrow.Types.DurationType => 8,
                    _ => throw new NotSupportedException(
                        $"Auto-split does not support column type {type.Name}. " +
                        "Split the RecordBatch manually before calling WriteRowGroupAsync."),
                };
                var src = data.Buffers[1].Span.Slice(srcOffset * byteWidth, length * byteWidth);
                var newData = new ArrayData(type, length, nullCount, 0,
                    [newBitmap, new ArrowBuffer(src.ToArray())]);
                return Apache.Arrow.ArrowArrayFactory.BuildArray(newData);
            }
        }
    }

    /// <summary>
    /// The footer this writer wrote: its schema and every row group's column statistics. Null until
    /// <see cref="CloseAsync"/> has written it.
    /// </summary>
    public FileMetaData? WrittenMetadata => _assembler.WrittenMetadata;

    /// <summary>
    /// Finalizes the file by writing the footer and closing magic, then completes the underlying
    /// <see cref="ISequentialFile"/>, which is what publishes it.
    /// </summary>
    public async ValueTask CloseAsync(CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        if (_closed)
            return;
        if (_fault is not null)
            throw new InvalidOperationException("An earlier write failed partway into the file; it cannot be closed.", _fault);

        // The row group WriteBatchAsync was filling is the file's last. This also writes the header of a file with no
        // row groups, which is still a valid empty file.
        await WriteHeaderAsync(cancellationToken).ConfigureAwait(false);
        await FlushOpenRowGroupAsync(cancellationToken).ConfigureAwait(false);

        // The header is written before the first row group but the schema is captured during it, so a
        // row group that throws leaves this null. Falling back here keeps dispose from replacing that
        // exception with an NRE out of the footer. A caller that wrote no rows at all can supply the
        // schema through DeclareSchema so the empty file still describes its columns.
        // Coerced here too: a table with no rows never passes a batch through the write path, so
        // this is the only place a second-precision column in it would be seen.
        if (_parquetSchema is null && _declaredSchema is not null)
        {
            _parquetSchema = ArrowToSchemaConverter.Convert(
                Data.TimeUnitRescaler.ToParquetUnits(_declaredSchema), _options);
        }

        _parquetSchema ??= [new SchemaElement { Name = "schema", NumChildren = 0 }];

        // Closed only once the footer is written. One that failed, or was cancelled, partway has left bytes of it in the
        // file, which cannot be taken back: the writer is poisoned, and a second CloseAsync throws rather than
        // returning as if the file were finished (#938 review).
        try
        {
            await _assembler.WriteFooterAsync(_parquetSchema, _declaredSchema, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _fault ??= e;
            throw;
        }

        _closed = true;
    }

    /// <summary>
    /// Writes the file's header, once. A failure here, like one writing a row group or the footer, leaves the file
    /// part-written, so the writer takes nothing more.
    /// </summary>
    private async ValueTask WriteHeaderAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _assembler.WriteHeaderAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _fault ??= e;
            throw;
        }
    }

    /// <summary>
    /// Releases the writer. Without a prior <see cref="CloseAsync"/> this abandons the file: no footer is
    /// written and, when the writer owns the file, disposing it discards the write, so a write that failed
    /// part-way publishes nothing.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_ownsFile)
            await _file.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc cref="DisposeAsync"/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_ownsFile)
            _file.Dispose();
    }

    internal static bool IsNestedType(Apache.Arrow.Types.IArrowType type) =>
        type is Apache.Arrow.Types.StructType
            or Apache.Arrow.Types.ListType
            or Apache.Arrow.Types.FixedSizeListType
            or Apache.Arrow.Types.MapType
            // VariantType / any extension whose storage is itself nested.
            or Apache.Arrow.ExtensionType { StorageType: Apache.Arrow.Types.StructType }
            or Apache.Arrow.ExtensionType { StorageType: Apache.Arrow.Types.ListType }
            or Apache.Arrow.ExtensionType { StorageType: Apache.Arrow.Types.FixedSizeListType }
            or Apache.Arrow.ExtensionType { StorageType: Apache.Arrow.Types.MapType };

    /// <summary>
    /// Finds the first leaf SchemaElement matching the given top-level field name.
    /// For flat columns, this is at index 1+ in the schema element list.
    /// </summary>
    private static SchemaElement FindLeafElement(IReadOnlyList<SchemaElement> schema, string fieldName)
    {
        // Walk schema elements (index 0 is root "schema"); find matching name
        for (int i = 1; i < schema.Count; i++)
        {
            if (schema[i].Name == fieldName)
                return schema[i];
        }

        throw new InvalidOperationException(
            $"Schema element not found for field '{fieldName}'.");
    }
}
