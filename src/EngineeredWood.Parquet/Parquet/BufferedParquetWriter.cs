// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet;

/// <summary>
/// A Parquet writer that accumulates multiple <see cref="RecordBatch"/> objects into a single
/// row group before encoding. Only dictionary indices and definition levels are buffered
/// (not the full Arrow value buffers), keeping memory usage proportional to distinct values
/// rather than total rows.
/// </summary>
/// <remarks>
/// <para>
/// Call <see cref="AppendAsync"/> to feed data, and <see cref="FlushRowGroupAsync"/> to
/// write the accumulated data as a single row group. Flushing happens automatically when
/// <see cref="ParquetWriteOptions.RowGroupMaxRows"/> is reached.
/// </para>
/// <para>
/// Struct, list and map columns are split into their leaf columns as <see cref="ParquetFileWriter"/> splits them,
/// and each leaf buffers its definition and repetition levels beside its dictionary indices. A batch is never
/// split across row groups, so a row group holds whole records.
/// </para>
/// </remarks>
public sealed class BufferedParquetWriter : IAsyncDisposable, IDisposable
{
    private readonly ISequentialFile _file;
    private readonly bool _ownsFile;
    private readonly ParquetWriteOptions _options;
    private readonly ParquetFileAssembler _assembler;
    private IReadOnlyList<SchemaElement>? _parquetSchema;
    private Apache.Arrow.Schema? _arrowSchema;
    private Apache.Arrow.Schema? _declaredSchema;
    private bool _closed;
    private bool _disposed;

    // Buffered column state — one per leaf column
    private BufferedColumnState[]? _columnStates;

    // The index in _columnStates of each Arrow column's first leaf; a nested column's leaves follow it.
    private int[]? _firstLeaf;
    private int _bufferedRows;

    /// <summary>
    /// Creates a new buffered Parquet writer.
    /// </summary>
    public BufferedParquetWriter(ISequentialFile file, bool ownsFile = true, ParquetWriteOptions? options = null)
    {
        _file = file;
        _ownsFile = ownsFile;
        _options = options ?? ParquetWriteOptions.Default;
        _assembler = new ParquetFileAssembler(file, _options);
    }

    /// <summary>Number of rows currently buffered and not yet flushed.</summary>
    public int BufferedRows => _bufferedRows;

    /// <summary>
    /// Appends a <see cref="RecordBatch"/> to the buffer. Dictionary indices are accumulated;
    /// the batch's Arrow value buffers are not retained.
    /// If the accumulated rows reach <see cref="ParquetWriteOptions.RowGroupMaxRows"/>, the
    /// buffer is automatically flushed as a row group.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// The batch holds a null in a column that is required: a non-nullable field or a map key in the first batch,
    /// or a column the first batch made required in a later one. Or a batch after the first differs from it in
    /// column names, order or types; field metadata may differ.
    /// </exception>
    public async ValueTask AppendAsync(
        RecordBatch batch,
        CancellationToken cancellationToken = default)
    {

#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        if (_closed)
            throw new InvalidOperationException("Writer has been closed.");

        cancellationToken.ThrowIfCancellationRequested();

        var source = batch;
        try
        {
            // As in ParquetFileWriter: the caller's schema is kept for ARROW:schema, and the batch is
            // rescaled out of second precision before its schema is captured, so the footer declares
            // what the encoders write.
            var declared = batch.Schema;
            batch = TimeUnitRescaler.ToParquetUnits(batch);
            batch = ByteArrayLayoutNormalizer.ToOffsetLayout(batch);

            if (!_assembler.HeaderWritten)
            {
                // As in ParquetFileWriter: a null in a required column would be written as a value. Checked before
                // anything is captured or written, so a refused first batch leaves the writer as it was.
                RowGroupSchemaCheck.EnsureWritable(batch);
                _declaredSchema ??= declared;
                _arrowSchema = batch.Schema;
                _parquetSchema = ArrowToSchemaConverter.Convert(_arrowSchema, _options);
                await _assembler.WriteHeaderAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // As in ParquetFileWriter: later batches are encoded against the first one's schema.
                RowGroupSchemaCheck.EnsureMatches(_arrowSchema!, batch);
            }

            // Initialize column states on first batch
            if (_columnStates == null)
            {
                InitializeColumnStates(batch);
            }

            // Dictionary-encode each column and append indices
            AppendBatchToBuffers(batch);

            // Auto-flush if we've reached the row group limit
            while (_bufferedRows >= _options.RowGroupMaxRows)
            {
                await FlushRowGroupAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            // Normalized batches can borrow native buffers whose owner is still the original batch.
            GC.KeepAlive(source);
        }
    }

    /// <summary>
    /// Flushes all buffered rows as a single row group.
    /// </summary>
    public async ValueTask FlushRowGroupAsync(CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        if (_columnStates == null || _bufferedRows == 0)
            return;

        int numRows = _bufferedRows;
        var states = _columnStates!;

        // Encode all columns into page bytes, each column's preparation included.
        var columnResults = new ColumnChunkWriter.ColumnChunkResult[states.Length];

        try
        {
            Parallel.For(0, states.Length, i => columnResults[i] = EncodeColumn(states[i]));
        }
        catch
        {
            // The chunks already encoded will not be written: release their pages (#936 decision 2).
            foreach (var result in columnResults)
                result.Release();
            throw;
        }

        await _assembler.WriteRowGroupAsync(columnResults, numRows, cancellationToken).ConfigureAwait(false);

        // Reset buffers for next row group
        ResetBuffers();
    }

    /// <summary>One column's buffered row group, encoded: its dictionary decided, or its values rebuilt and written.</summary>
    private ColumnChunkWriter.ColumnChunkResult EncodeColumn(BufferedColumnState s)
    {
        int[]? defLevels = s.DefLevels?.ToArray(s.LevelCount);
        int[]? repLevels = s.RepLevels?.ToArray(s.LevelCount);

        if (s.PhysicalType == PhysicalType.Boolean)
        {
            using var booleans = s.BuildBooleanArray(PresenceLevels(s, defLevels));
            return WriteLeaf(s, booleans, defLevels, repLevels, _options);
        }

        if (s.Plain)
        {
            // Held as values (no dictionary, or one its first-page test declined): written in the column's own
            // encoding, as ParquetFileWriter writes a column whose dictionary it declined.
            using var values = s.BuildPlainArray(PresenceLevels(s, defLevels));
            return WriteLeaf(s, values, defLevels, repLevels, _options with
            {
                DictionaryEnabled = false,
                ColumnDictionaryEnabled = null,
            });
        }

        if (s.DictionaryCount == 0)
            return WriteValuelessColumn(s, defLevels, repLevels);

        // A dictionary over the page size limit is one ParquetFileWriter would have cut short: declined if it filled
        // before the first page decided, else kept for the pages before the cut and the rest written in the column's
        // own encoding. The column is rebuilt and written through that writer's own path, so the two writers agree;
        // this costs a second pass of hashing, only for such a column.
        bool overLimit = s.DictionaryPageLength > _options.DictionaryPageSizeLimit;

        var indices = s.Indices!.ToArray(s.NonNullCount);
        DictionaryEncoder.DictionaryResult dictionary;
        int pageLength;
        bool useNonDictionary;
        if (overLimit && s.TryBorrowPage(out byte[] page))
        {
            // Rebuilt, never written: the rebuild reads the buffered entries where they are, without the copy a
            // written page needs. Past the limit they are the values themselves, as large as the rebuilt column, so
            // a copy would have been a third instance of it at flush.
            dictionary = new DictionaryEncoder.DictionaryResult
            {
                DictionaryPageData = page,
                DictionaryCount = s.DictionaryCount,
                Indices = indices,
            };
            pageLength = s.DictionaryPageLength;
            useNonDictionary = true;
        }
        else
        {
            dictionary = new DictionaryEncoder.DictionaryResult
            {
                DictionaryPageData = s.BuildDictionaryPage(),
                DictionaryCount = s.DictionaryCount,
                Indices = indices,
            };
            pageLength = dictionary.DictionaryPageData.Length;

            // Otherwise the file writer's first-page test (DictionaryEncoder.FirstPagePays): a dictionary that does
            // not pay is declined, and the values are rebuilt and written in the column's own encoding. A column whose
            // values reached a first page was tested as they arrived, and is Plain above if it failed.
            useNonDictionary = overLimit
                || !DictionaryEncoder.FirstPagePays(
                    dictionary, s.NonNullCount, s.PhysicalType, s.TypeLength, DictionaryEncoder.FirstPageBytes);
        }

        if (!useNonDictionary)
        {
            return ColumnChunkWriter.WriteDictionaryColumnFromResult(
                dictionary, s.LevelCount, s.NonNullCount, s.PathInSchema, s.PhysicalType, s.TypeLength, s.ArrowType,
                s.MaxDefLevel, s.MaxRepLevel, defLevels, repLevels, _options);
        }

        // Reconstruct the Arrow array from dictionary + indices and encode it through the standard non-dictionary
        // path (delta, BSS, plain, etc.).
        using var array = ReconstructArrowArray(s, dictionary, pageLength, PresenceLevels(s, defLevels), s.LevelCount);
        return WriteLeaf(s, array, defLevels, repLevels, overLimit ? _options : _options with { DictionaryEnabled = false });
    }

    /// <summary>
    /// Encodes a leaf rebuilt in level order: a nested leaf with its own levels, as ParquetFileWriter writes it, and
    /// a flat column through the flat overload, which derives its levels from the array.
    /// </summary>
    private static ColumnChunkWriter.ColumnChunkResult WriteLeaf(
        BufferedColumnState state, IArrowArray array, int[]? defLevels, int[]? repLevels, ParquetWriteOptions options) =>
        state.IsNested
            ? ColumnChunkWriter.WriteColumn(
                array, state.PathInSchema, state.PhysicalType, state.TypeLength, state.MaxDefLevel, state.MaxRepLevel,
                defLevels ?? new int[state.LevelCount], repLevels, state.NonNullCount, state.LevelCount, options)
            : ColumnChunkWriter.WriteColumn(
                array, state.PathInSchema, state.PhysicalType, state.TypeLength, state.IsNullable, options);

    /// <summary>
    /// The definition levels as the reconstruction reads them: 1 where a level holds a value, 0 where it does not.
    /// A flat column's already are; a nested leaf's are 0 to its maximum, and only the maximum holds a value.
    /// </summary>
    private static int[]? PresenceLevels(BufferedColumnState state, int[]? defLevels)
    {
        if (defLevels is null || state.MaxDefLevel == 1)
            return defLevels;
        var presence = new int[defLevels.Length];
        for (int i = 0; i < presence.Length; i++)
            presence[i] = defLevels[i] == state.MaxDefLevel ? 1 : 0;
        return presence;
    }

    /// <summary>
    /// Finalizes the file: flushes any remaining buffered rows, writes the footer, and completes the
    /// underlying <see cref="ISequentialFile"/>, which is what publishes it.
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

        _closed = true;

        // Flush remaining buffered data
        if (_bufferedRows > 0)
            await FlushRowGroupAsync(cancellationToken).ConfigureAwait(false);

        if (!_assembler.HeaderWritten)
        {
            await _assembler.WriteHeaderAsync(cancellationToken).ConfigureAwait(false);
            _parquetSchema ??= [new SchemaElement { Name = "schema", NumChildren = 0 }];
        }

        await _assembler.WriteFooterAsync(_parquetSchema!, _declaredSchema, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Releases the writer. Without a prior <see cref="CloseAsync"/> this abandons the file: buffered rows
    /// are dropped, no footer is written and, when the writer owns the file, disposing it discards the
    /// write, so a write that failed part-way publishes nothing.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        DiscardBuffers();

        if (_ownsFile)
            await _file.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc cref="DisposeAsync"/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        DiscardBuffers();
        if (_ownsFile) _file.Dispose();
    }

    private void DiscardBuffers()
    {
        _columnStates = null;
        _firstLeaf = null;
        _bufferedRows = 0;
    }

    // ───── Initialization ─────

    private void InitializeColumnStates(RecordBatch batch)
    {
        var leafColumns = new List<BufferedColumnState>();
        _firstLeaf = new int[batch.ColumnCount];

        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var field = _arrowSchema!.FieldsList[c];
            _firstLeaf[c] = leafColumns.Count;

            if (ParquetFileWriter.IsNestedType(field.DataType))
            {
                // One state per leaf, described as NestedLevelWriter describes it for ParquetFileWriter (#462). The
                // first batch is decomposed again when it is appended; that is the only cost of reading the leaves'
                // types from it.
                foreach (var leaf in NestedLevelWriter.Decompose(batch.Column(c), field, batch.Length))
                {
                    leafColumns.Add(new BufferedColumnState
                    {
                        PathInSchema = leaf.PathInSchema,
                        PhysicalType = leaf.PhysicalType,
                        TypeLength = leaf.TypeLength,
                        MaxDefLevel = leaf.MaxDefLevel,
                        MaxRepLevel = leaf.MaxRepLevel,
                        ArrowType = DecimalNarrowing.EncodingType(
                            LeafValues(leaf.Array).Data.DataType, leaf.PhysicalType),
                        IsNullable = leaf.MaxDefLevel > 0,
                        IsNested = true,
                        DictionaryEnabled = _options.GetDictionaryEnabled(leaf.PathInSchema),
                        DictionaryLimit = _options.DictionaryPageSizeLimit,
                    });
                }
                continue;
            }

            var element = FindLeafElement(_parquetSchema!, field.Name);

            // Encoders dispatch on the storage type, not the ExtensionType
            // wrapper (which they don't recognise). Strip the extension here;
            // ArrowToSchemaConverter has already emitted the correct annotation
            // on the schema side.
            IArrowType encodingType = field.DataType is ExtensionType ext
                ? ext.StorageType
                : field.DataType;
            if (encodingType is RunEndEncodedType runs)
            {
                encodingType = runs.ValuesDataType;
            }

            // A promoted timestamp reaches the encoders as the twelve-byte carrier, not as int64. The
            // schema element already says FIXED_LEN_BYTE_ARRAY(12); if the encoder still dispatched on
            // TimestampType it would write eight-byte values under a twelve-byte declaration, and the
            // file would be well-formed and wrong.
            bool extendedTimestamp = _options.IsExtendedTimestampColumn([field.Name])
                && encodingType is Apache.Arrow.Types.TimestampType;
            if (extendedTimestamp)
                encodingType = new Apache.Arrow.Types.FixedSizeBinaryType(ExtendedTimestamp.ByteWidth);

            // A wide decimal written as INT32/INT64 (#504) is buffered as Decimal32/64, which the encoders below
            // already handle; each batch is narrowed to it as it arrives.
            encodingType = DecimalNarrowing.EncodingType(encodingType, element.Type!.Value);

            leafColumns.Add(new BufferedColumnState
            {
                PathInSchema = [field.Name],
                PhysicalType = element.Type!.Value,
                TypeLength = element.TypeLength ?? 0,
                MaxDefLevel = field.IsNullable ? 1 : 0,
                MaxRepLevel = 0,
                ArrowType = encodingType,
                IsNullable = field.IsNullable,
                ExtendedTimestamp = extendedTimestamp,
                DictionaryEnabled = _options.GetDictionaryEnabled([field.Name]),
                DictionaryLimit = _options.DictionaryPageSizeLimit,
            });
        }

        _columnStates = leafColumns.ToArray();
    }

    // ───── Batch accumulation ─────

    private void AppendBatchToBuffers(RecordBatch batch)
    {
        int rowCount = batch.Length;

        // Every column is brought to the form its state buffers, and refused if it cannot be (a decimal its layout
        // cannot hold, a leaf whose type changed since the first batch), before any state takes it. The columns
        // run in parallel, so a refusal found after other columns had buffered the batch would leave them a batch
        // ahead of the rest and of _bufferedRows, and the next flush would write columns that disagree on the row
        // group's row count.
        var prepared = new PreparedLeaf[batch.ColumnCount][];
        Parallel.For(0, batch.ColumnCount, c =>
        {
            var array = batch.Column(c);
            int first = _firstLeaf![c];
            if (_columnStates![first].IsNested)
            {
                var leaves = NestedLevelWriter.Decompose(array, _arrowSchema!.FieldsList[c], rowCount);
                var column = new PreparedLeaf[leaves.Count];
                for (int l = 0; l < leaves.Count; l++)
                {
                    var leaf = leaves[l];
                    column[l] = new PreparedLeaf(
                        _columnStates[first + l].Prepare(LeafValues(leaf.Array)),
                        leaf.LevelCount, leaf.DefLevels, leaf.RepLevels);
                }
                prepared[c] = column;
                return;
            }

            var state = _columnStates[first];
            array = LeafValues(array);
            if (state.ExtendedTimestamp)
                array = ExtendedTimestamp.EncodeColumn(array);
            prepared[c] = [new PreparedLeaf(state.Prepare(array), rowCount, null, null)];
        });

        Parallel.For(0, batch.ColumnCount, c =>
        {
            int first = _firstLeaf![c];
            var column = prepared[c];
            for (int l = 0; l < column.Length; l++)
            {
                var leaf = column[l];
                var state = _columnStates![first + l];
                if (state.IsNested)
                    state.AppendLevels(leaf.Array, leaf.LevelCount, leaf.DefLevels!, leaf.RepLevels);
                else
                    state.AppendArray(leaf.Array, leaf.LevelCount);
            }
        });

        _bufferedRows += rowCount;
    }

    /// <summary>One leaf of a batch as <see cref="BufferedColumnState.Prepare"/> left it, with its levels when nested.</summary>
    private readonly record struct PreparedLeaf(IArrowArray Array, int LevelCount, int[]? DefLevels, int[]? RepLevels);

    /// <summary>
    /// A leaf as the state buffers it: the storage of an extension, since the encoders dispatch on that, and a
    /// run-end encoded leaf expanded to its values. NestedLevelWriter hands a run-end encoded leaf over as runs when the
    /// batch needs no rebuild and as plain values when it does, so its layout can change from batch to batch, while a
    /// state reads every batch by the type it took from the first. The values are dictionary-encoded one by one either
    /// way, so the expansion costs only the batch's own copy.
    /// </summary>
    private static IArrowArray LeafValues(IArrowArray array)
    {
        if (array is ExtensionArray extension)
            array = extension.Storage;
        return array is RunEndEncodedArray runs ? EngineeredWood.Arrow.RunEndEncoding.Expand(runs) : array;
    }

    private void ResetBuffers()
    {
        if (_columnStates == null) return;
        foreach (var s in _columnStates)
            s.Reset();
        _bufferedRows = 0;
    }

    // ───── Array reconstruction from dictionary + indices ─────

    /// <summary>
    /// Reconstructs an Arrow array from buffered dictionary entries and indices.
    /// Used at flush time when cardinality exceeds the dictionary threshold and the
    /// column should be written with non-dictionary encoding.
    /// </summary>
    /// <param name="pageLength">
    /// The page's bytes in <see cref="DictionaryEncoder.DictionaryResult.DictionaryPageData"/>, which may be a buffer
    /// borrowed from the column's state and longer (<see cref="BufferedColumnState.TryBorrowPage"/>).
    /// </param>
    private static IArrowArray ReconstructArrowArray(
        BufferedColumnState state,
        DictionaryEncoder.DictionaryResult dictResult,
        int pageLength,
        int[]? defLevels,
        int numRows)
    {
        // Every dictionary this writer builds carries one index per row: it accumulates ROWS, appending an
        // index as each arrives. The run form comes only out of DictionaryEncoder's run-end-encoded arm,
        // which nothing here reaches — so this is an assertion, not a case left unhandled.
        int[] indices = dictResult.Indices
            ?? throw new InvalidOperationException(
                "A buffered column cannot be reconstructed from run-form dictionary indices.");

        ReadOnlySpan<byte> dictPage = dictResult.DictionaryPageData.AsSpan(0, pageLength);
        int dictCount = dictResult.DictionaryCount;

        return state.ArrowType switch
        {
            // The narrow integers' entries are INT32 (see EncodeFixed), so they are read back at that width.
            Apache.Arrow.Types.Int8Type => ReconstructFixed(
                NarrowEntries(dictPage, v => unchecked((sbyte)v)), indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.UInt8Type => ReconstructFixed(
                NarrowEntries(dictPage, v => unchecked((byte)v)), indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.Int16Type => ReconstructFixed(
                NarrowEntries(dictPage, v => unchecked((short)v)), indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.UInt16Type => ReconstructFixed(
                NarrowEntries(dictPage, v => unchecked((ushort)v)), indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.Int32Type or Apache.Arrow.Types.Date32Type or Apache.Arrow.Types.Time32Type
                => ReconstructFixed<int>(dictPage, indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.UInt32Type
                => ReconstructFixed<uint>(dictPage, indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.Int64Type or Apache.Arrow.Types.Date64Type or Apache.Arrow.Types.Time64Type
                or Apache.Arrow.Types.TimestampType or Apache.Arrow.Types.DurationType
                => ReconstructFixed<long>(dictPage, indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.UInt64Type
                => ReconstructFixed<ulong>(dictPage, indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.FloatType
                => ReconstructFixed<float>(dictPage, indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.DoubleType
                => ReconstructFixed<double>(dictPage, indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.StringType
                => ReconstructByteArray(dictPage, dictCount, indices, defLevels, numRows, state.IsNullable, isString: true),
            Apache.Arrow.Types.BinaryType
                => ReconstructByteArray(dictPage, dictCount, indices, defLevels, numRows, state.IsNullable, isString: false),
            // Decimal32/64 too: their entries are the slots DictionaryEncodeArray copied, so they rebuild unchanged.
            Apache.Arrow.Types.FixedSizeBinaryType fsb
                => ReconstructFixedLenByteArray(dictPage, indices, defLevels, numRows, state.IsNullable,
                    state.EntryWidth(fsb.ByteWidth), fsb.ByteWidth, state.ArrowType),
            // FIXED_LEN_BYTE_ARRAY(2), and its entries keep Arrow's two bytes (see EncodeFixed).
            Apache.Arrow.Types.HalfFloatType
                => ReconstructFixedLenByteArray(dictPage, indices, defLevels, numRows, state.IsNullable, 2, 2, state.ArrowType),
            _ => throw new NotSupportedException($"Cannot reconstruct array of type {state.ArrowType}"),
        };
    }

    /// <summary>An INT32 dictionary page's entries, each narrowed back to the column's Arrow type.</summary>
    private static T[] NarrowEntries<T>(ReadOnlySpan<byte> dictPage, Func<int, T> narrow)
    {
        var entries = MemoryMarshal.Cast<byte, int>(dictPage);
        var narrowed = new T[entries.Length];
        for (int i = 0; i < entries.Length; i++)
            narrowed[i] = narrow(entries[i]);
        return narrowed;
    }

    private static IArrowArray ReconstructFixed<T>(
        ReadOnlySpan<byte> dictPage, int[] indices, int[]? defLevels, int numRows,
        bool isNullable) where T : unmanaged =>
        ReconstructFixed<T>(MemoryMarshal.Cast<byte, T>(dictPage), indices, defLevels, numRows, isNullable);

    private static IArrowArray ReconstructFixed<T>(
        ReadOnlySpan<T> dictValues, int[] indices, int[]? defLevels, int numRows,
        bool isNullable) where T : unmanaged
    {
        var values = new T[numRows];
        var nullBitmap = isNullable ? new byte[(numRows + 7) / 8] : null;

        int idx = 0;
        for (int row = 0; row < numRows; row++)
        {
            if (defLevels != null && defLevels[row] == 0)
            {
                // null — leave value as default, don't set bitmap bit
            }
            else
            {
                values[row] = dictValues[indices[idx++]];
                if (nullBitmap != null)
                    nullBitmap[row / 8] |= (byte)(1 << (row % 8));
            }
        }

        int nullCount = isNullable ? numRows - indices.Length : 0;
        var valueBytes = MemoryMarshal.AsBytes(values.AsSpan()).ToArray();
        var buffers = nullBitmap != null
            ? new[] { new ArrowBuffer(nullBitmap), new ArrowBuffer(valueBytes) }
            : new[] { ArrowBuffer.Empty, new ArrowBuffer(valueBytes) };

        var arrowType = default(T) switch
        {
            sbyte => (Apache.Arrow.Types.IArrowType)Apache.Arrow.Types.Int8Type.Default,
            byte => Apache.Arrow.Types.UInt8Type.Default,
            short => Apache.Arrow.Types.Int16Type.Default,
            ushort => Apache.Arrow.Types.UInt16Type.Default,
            int => Apache.Arrow.Types.Int32Type.Default,
            uint => Apache.Arrow.Types.UInt32Type.Default,
            long => Apache.Arrow.Types.Int64Type.Default,
            ulong => Apache.Arrow.Types.UInt64Type.Default,
            float => Apache.Arrow.Types.FloatType.Default,
            double => Apache.Arrow.Types.DoubleType.Default,
            _ => throw new NotSupportedException(),
        };

        var arrayData = new ArrayData(arrowType, numRows, nullCount, 0, buffers);
        return Data.ArrowArrayFactory.BuildArray(arrayData);
    }

    private static IArrowArray ReconstructByteArray(
        ReadOnlySpan<byte> dictPage, int dictCount, int[] indices, int[]? defLevels, int numRows,
        bool isNullable, bool isString)
    {
        // Parse dictionary entries from PLAIN format: [4-byte len][data]...
        var entries = new (int offset, int length)[dictCount];
        int pos = 0;
        for (int d = 0; d < dictCount; d++)
        {
            int len = BinaryPrimitives.ReadInt32LittleEndian(dictPage.Slice(pos));
            pos += 4;
            entries[d] = (pos, len);
            pos += len;
        }

        // Build offsets + data for the Arrow array
        var offsets = new int[numRows + 1];
        int totalLen = 0;
        int idx = 0;

        // First pass: compute offsets
        for (int row = 0; row < numRows; row++)
        {
            if (defLevels != null && defLevels[row] == 0)
            {
                offsets[row + 1] = totalLen;
            }
            else
            {
                var (_, len) = entries[indices[idx++]];
                totalLen += len;
                offsets[row + 1] = totalLen;
            }
        }

        // Second pass: copy data
        var data = new byte[totalLen];
        idx = 0;
        for (int row = 0; row < numRows; row++)
        {
            if (defLevels != null && defLevels[row] == 0) continue;
            var (entryOff, entryLen) = entries[indices[idx++]];
            dictPage.Slice(entryOff, entryLen).CopyTo(data.AsSpan(offsets[row]));
        }

        var nullBitmap = isNullable ? new byte[(numRows + 7) / 8] : null;
        if (nullBitmap != null)
        {
            for (int row = 0; row < numRows; row++)
            {
                if (defLevels == null || defLevels[row] != 0)
                    nullBitmap[row / 8] |= (byte)(1 << (row % 8));
            }
        }

        int nullCount = isNullable && defLevels != null
            ? defLevels.Count(d => d == 0)
            : 0;

        var offsetBytes = MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray();
        var type = isString
            ? (Apache.Arrow.Types.IArrowType)Apache.Arrow.Types.StringType.Default
            : Apache.Arrow.Types.BinaryType.Default;

        var buffers = nullBitmap != null
            ? new[] { new ArrowBuffer(nullBitmap), new ArrowBuffer(offsetBytes), new ArrowBuffer(data) }
            : new[] { ArrowBuffer.Empty, new ArrowBuffer(offsetBytes), new ArrowBuffer(data) };

        var arrayData = new ArrayData(type, numRows, nullCount, 0, buffers);
        return Data.ArrowArrayFactory.BuildArray(arrayData);
    }

    /// <summary>
    /// A decimal written as FIXED_LEN_BYTE_ARRAY: big-endian in Parquet, little-endian in Arrow (#400).
    /// These types derive from <see cref="Apache.Arrow.Types.FixedSizeBinaryType"/>, so they take the
    /// FIXED_LEN_BYTE_ARRAY arms here unless something reverses their bytes.
    /// </summary>
    private static bool IsFlbaDecimal(Apache.Arrow.Types.IArrowType type) =>
        type is Apache.Arrow.Types.Decimal128Type or Apache.Arrow.Types.Decimal256Type;

    /// <param name="entryWidth">A dictionary entry's width: the column's FLBA width, which for a decimal is
    /// narrower than its Arrow slot (#504).</param>
    /// <param name="byteWidth">The Arrow slot width of <paramref name="arrowType"/>.</param>
    private static IArrowArray ReconstructFixedLenByteArray(
        ReadOnlySpan<byte> dictPage, int[] indices, int[]? defLevels, int numRows,
        bool isNullable, int entryWidth, int byteWidth, Apache.Arrow.Types.IArrowType arrowType)
    {
        bool decimalFlba = IsFlbaDecimal(arrowType);
        var values = new byte[numRows * byteWidth];
        var nullBitmap = isNullable ? new byte[(numRows + 7) / 8] : null;

        int idx = 0;
        for (int row = 0; row < numRows; row++)
        {
            if (defLevels != null && defLevels[row] == 0)
                continue;

            int dictIdx = indices[idx++];
            var entry = dictPage.Slice(dictIdx * entryWidth, entryWidth);

            // The page holds a decimal big-endian at the column's width, as Parquet stores it; the rebuilt array
            // goes back through ColumnChunkWriter, which expects Arrow's little-endian slot and narrows it again.
            if (decimalFlba)
                DecimalNarrowing.Widen(entry, values.AsSpan(row * byteWidth, byteWidth), sourceBigEndian: true);
            else
                entry.CopyTo(values.AsSpan(row * byteWidth));
            if (nullBitmap != null)
                nullBitmap[row / 8] |= (byte)(1 << (row % 8));
        }

        int nullCount = isNullable && defLevels != null ? defLevels.Count(d => d == 0) : 0;
        var buffers = nullBitmap != null
            ? new[] { new ArrowBuffer(nullBitmap), new ArrowBuffer(values) }
            : new[] { ArrowBuffer.Empty, new ArrowBuffer(values) };

        var arrayData = new ArrayData(arrowType, numRows, nullCount, 0, buffers);
        return Data.ArrowArrayFactory.BuildArray(arrayData);
    }

    // ───── Fallback for columns with no dictionary ─────

    /// <summary>
    /// Writes a column that accumulated no dictionary entry at all — an all-null column, since every
    /// non-null value of a supported type goes into the dictionary as it arrives.
    /// </summary>
    /// <remarks>
    /// It goes through <c>ColumnChunkWriter.WriteColumn</c> on a reconstructed all-null array
    /// rather than hand-building a <see cref="ColumnMetaData"/>: hand-building it produced a chunk that
    /// declared <c>NumValues = numRows</c> and then wrote NO PAGES, which our own reader rejects as
    /// truncated. Reconstructing costs one all-null array — the same shape the Boolean fallback and the
    /// high-cardinality fallback already pay — and in exchange the pages, the encodings and the
    /// statistics all come from the one path that gets them right.
    /// </remarks>
    private ColumnChunkWriter.ColumnChunkResult WriteValuelessColumn(
        BufferedColumnState state, int[]? defLevels, int[]? repLevels)
    {
        if (state.NonNullCount > 0)
        {
            // Non-null values that never reached the dictionary: DictionaryEncodeArray has no arm for
            // this Arrow type and silently dropped them. Refusing beats writing a file whose values are
            // gone with nothing to say so.
            throw new NotSupportedException(
                $"Column '{string.Join(".", state.PathInSchema)}' has Arrow type {state.ArrowType.Name}, " +
                "which BufferedParquetWriter cannot buffer. Write it with ParquetFileWriter instead.");
        }

        using var array = ReconstructArrowArray(
            state,
            new DictionaryEncoder.DictionaryResult
            {
                DictionaryPageData = [],
                DictionaryCount = 0,
                Indices = [],
            },
            0,
            PresenceLevels(state, defLevels),
            state.LevelCount);

        return WriteLeaf(state, array, defLevels, repLevels, _options);
    }

    private static SchemaElement FindLeafElement(IReadOnlyList<SchemaElement> schema, string fieldName)
    {
        for (int i = 1; i < schema.Count; i++)
        {
            if (schema[i].Name == fieldName)
                return schema[i];
        }
        throw new InvalidOperationException($"Schema element not found for field '{fieldName}'.");
    }

    // ───── Buffered column state ─────

    /// <summary>
    /// Accumulates dictionary-encoded data for a single column across multiple batches.
    /// Stores only the dictionary (unique values) and compact indices, not the full Arrow buffers.
    /// </summary>
    private sealed class BufferedColumnState
    {
        public required IReadOnlyList<string> PathInSchema { get; init; }
        public required PhysicalType PhysicalType { get; init; }
        public required int TypeLength { get; init; }
        public required int MaxDefLevel { get; init; }
        public required int MaxRepLevel { get; init; }
        public required Apache.Arrow.Types.IArrowType ArrowType { get; init; }
        public required bool IsNullable { get; init; }

        /// <summary>
        /// True when this column's Arrow timestamps are encoded to the twelve-byte carrier on the way in.
        /// <see cref="ArrowType"/> is then the storage type the encoders see, not the caller's type.
        /// </summary>
        public bool ExtendedTimestamp { get; init; }

        /// <summary>
        /// A leaf of a struct, list or map column. Its levels come from <see cref="NestedLevelWriter"/>, and its
        /// level count is not its row count: a list row holds a level per element, or one for an empty or null list.
        /// </summary>
        public bool IsNested { get; init; }

        /// <summary>Whether the options let this column have a dictionary.</summary>
        public bool DictionaryEnabled { get; init; } = true;

        /// <summary><see cref="ParquetWriteOptions.DictionaryPageSizeLimit"/>.</summary>
        public int DictionaryLimit { get; init; } = int.MaxValue;

        // Set once this row group's dictionary passes DictionaryLimit (after it passed its first-page test: one that
        // fails the test is moved to plain values, see Plain). From then on each value is appended as an entry of its
        // own with no lookup: the "dictionary"
        // is just the values, the hash table stops growing, and at flush the column is rebuilt and written through
        // ParquetFileWriter's path, which makes the real decision (see FlushRowGroupAsync). Deduplicating every value
        // into a dictionary that was never going to be written cost 100 ms per million random INT64s, against 7 for
        // ParquetFileWriter, and a hash table entry per distinct value.
        private bool _dictionaryFull;

        /// <summary>Whether values are appended raw rather than looked up.</summary>
        private bool Raw => _dictionaryFull;

        // Raw, the fixed-width arms still deduplicate against a small cache of recent values (the byte-keyed arms'
        // is in BytesHashTable.AppendRecent): per slot, the value's bits and its entry's number plus one.
        private const int RecentSlots = 1024;
        private ulong[]? _recentKeys;
        private int[]? _recentEntries;

        // Running dictionary: maps a value's BIT PATTERN → index. The float and double dictionaries are
        // keyed on uint/ulong rather than on the value because a dictionary entry is a set of BYTES, and
        // value equality is looser than that: IEquatable<double>.Equals(-0.0, 0.0) is true, which merged
        // the two zeros into one entry and silently rewrote whichever came second (issue #154).
        private Dictionary<int, int>? _fixedDict;
        private Dictionary<long, int>? _longDict;
        private Dictionary<uint, int>? _floatDict;
        private Dictionary<ulong, int>? _doubleDict;
        // BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY: the file writer's table, which keeps the entries as the PLAIN page.
        private DictionaryEncoder.BytesHashTable? _bytesTable;

        // Accumulated indices (compact, non-null only)
        public GrowableIntList? Indices { get; private set; }

        // Accumulated definition levels
        public GrowableIntList? DefLevels { get; private set; }

        // Accumulated repetition levels (a leaf under a list or map)
        public GrowableIntList? RepLevels { get; private set; }

        // The fixed-width arms' entries, end to end: the PLAIN dictionary page (copied out on flush). The byte-keyed
        // arms' are in _bytesTable.
        private byte[] _fixedPage = [];
        private int _dictPageSize;

        public int DictionaryCount { get; private set; }
        public int LevelCount { get; private set; }
        public int NonNullCount { get; private set; }

        /// <summary>Whether level <paramref name="level"/> holds a value: its definition level is the maximum.</summary>
        public bool IsValue(int level) => DefLevels is null || DefLevels.Get(level) == MaxDefLevel;

        /// <summary>
        /// The batch's <paramref name="array"/> as this state buffers it, or a refusal. Called for every column of
        /// a batch before any is appended, so everything that can refuse a batch belongs here and not in
        /// <see cref="AppendArray"/> or <see cref="AppendLevels"/>.
        /// </summary>
        public IArrowArray Prepare(IArrowArray array)
        {
            array = NarrowDecimal(array);

            if (ArrowType is not (BooleanType or Int8Type or UInt8Type or Int16Type or UInt16Type
                or HalfFloatType or Int32Type or UInt32Type or Date32Type or Time32Type
                or Int64Type or UInt64Type or Date64Type or Time64Type or TimestampType or DurationType
                or FloatType or DoubleType or Apache.Arrow.Types.StringType or BinaryType or FixedSizeBinaryType))
            {
                throw new NotSupportedException(
                    $"Column '{string.Join(".", PathInSchema)}' has Arrow type {ArrowType.Name}, " +
                    "which BufferedParquetWriter cannot buffer. Write it with ParquetFileWriter instead.");
            }

            // The encoders read a batch's raw buffers as ArrowType, taken from the first batch. A leaf whose layout
            // changed since would be misread, so it is refused; LeafValues keeps the known case (runs) from reaching
            // here. A flat column's type is held to the first batch's by RowGroupSchemaCheck.
            if (IsNested && array.Data.DataType.TypeId != ArrowType.TypeId)
            {
                throw new NotSupportedException(
                    $"Column '{string.Join(".", PathInSchema)}' arrived as {array.Data.DataType.Name} after an earlier " +
                    $"batch gave it as {ArrowType.Name}, which BufferedParquetWriter cannot buffer together. Write it " +
                    "with ParquetFileWriter instead.");
            }

            // A decimal FLBA is narrowed value by value as it is dictionary-encoded; checked whole here first.
            if (IsFlbaDecimal(ArrowType))
                DecimalNarrowing.EnsureFits(array, TypeLength);

            return array;
        }

        /// <summary>
        /// Appends a nested leaf from <see cref="Prepare"/>: <paramref name="array"/> holds one slot per level, in
        /// level order, as <see cref="NestedLevelWriter"/> builds it, and only the slots at the maximum definition
        /// level are values.
        /// </summary>
        public void AppendLevels(IArrowArray array, int levelCount, int[] defLevels, int[]? repLevels)
        {
            int values = levelCount;
            if (MaxDefLevel > 0)
            {
                DefLevels ??= new GrowableIntList(levelCount);
                var source = defLevels.AsSpan(0, levelCount);
                source.CopyTo(DefLevels.AppendSpan(levelCount));
                values = 0;
                int max = MaxDefLevel;
                foreach (int level in source)
                {
                    if (level == max)
                        values++;
                }
            }

            if (MaxRepLevel > 0)
            {
                RepLevels ??= new GrowableIntList(levelCount);
                repLevels.AsSpan(0, levelCount).CopyTo(RepLevels.AppendSpan(levelCount));
            }

            AppendValues(array, levelCount, values);
        }

        /// <summary>Appends a flat column from <see cref="Prepare"/>.</summary>
        public void AppendArray(IArrowArray array, int rowCount)
        {
            int batchNonNull = rowCount;
            if (IsNullable)
            {
                // The levels straight off the validity bitmap: every type buffered here keeps its nulls there.
                DefLevels ??= new GrowableIntList(rowCount);
                var levels = DefLevels.AppendSpan(rowCount);
                var data = array.Data;
                var validity = data.Buffers[0].Span;
                if (validity.IsEmpty)
                {
                    levels.Fill(1);
                }
                else
                {
                    int offset = data.Offset;
                    batchNonNull = 0;
                    for (int i = 0; i < rowCount; i++)
                    {
                        int bit = offset + i;
                        int level = (validity[bit >> 3] >> (bit & 7)) & 1;
                        levels[i] = level;
                        batchNonNull += level;
                    }
                }
            }

            AppendValues(array, rowCount, batchNonNull);
        }

        // The levels for these rowCount slots are already in DefLevels, from LevelCount on.
        private void AppendValues(IArrowArray array, int rowCount, int batchNonNull)
        {
            int srcOffset = array.Data.Offset;

            if (PhysicalType == PhysicalType.Boolean)
            {
                // Boolean can't be dictionary-encoded: its value bits are buffered as they are, a bit per level.
                AppendBooleanBits(array.Data, rowCount);
            }
            else if (Plain)
            {
                AppendPlain(array.Data, rowCount);
            }
            else if (batchNonNull > 0)
            {
                Indices ??= new GrowableIntList(batchNonNull);
                DictionaryEncodeArray(array, rowCount, srcOffset);
            }

            LevelCount += rowCount;
            NonNullCount += batchNonNull;
            GC.KeepAlive(array);

            if (!Plain && !_firstPageTested && PhysicalType != PhysicalType.Boolean)
                TakeFirstPageTest();
        }

        // ───── Booleans ─────

        // A bit per level: the value at a level that holds one, 0 at one that does not.
        private byte[] _booleanBits = [];

        private void AppendBooleanBits(ArrayData data, int levelCount)
        {
            long neededBytes = ((long)LevelCount + levelCount + 7) / 8;
            if (neededBytes > _booleanBits.Length)
                System.Array.Resize(ref _booleanBits, (int)Math.Min(
                    DictionaryEncoder.BytesHashTable.MaxArrayLength, Math.Max(neededBytes, Math.Max(64, (long)_booleanBits.Length * 2))));

            var source = data.Buffers[1].Span;
            var target = _booleanBits.AsSpan();
            int sourceOffset = data.Offset, at = LevelCount;
            bool hasLevels = DefLevels is not null;
            ReadOnlySpan<int> levels = hasLevels ? DefLevels!.Slice(LevelCount, levelCount) : default;
            int max = MaxDefLevel;
            for (int i = 0; i < levelCount; i++)
            {
                if (hasLevels && levels[i] != max)
                    continue;
                int bit = sourceOffset + i;
                if (((source[bit >> 3] >> (bit & 7)) & 1) != 0)
                {
                    int to = at + i;
                    target[to >> 3] |= (byte)(1 << (to & 7));
                }
            }
        }

        /// <summary>The buffered booleans as an array of a slot per level, null where a level holds no value.</summary>
        public Apache.Arrow.BooleanArray BuildBooleanArray(int[]? presence)
        {
            int length = LevelCount;
            var values = _booleanBits.AsSpan(0, (length + 7) / 8).ToArray();
            var (validity, nullCount) = ValidityBitmap(presence, length);
            return new Apache.Arrow.BooleanArray(new ArrowBuffer(values), validity, length, nullCount, 0);
        }

        /// <summary>
        /// A validity bitmap from presence levels (1 where a level holds a value), and its null count; an empty buffer
        /// and none for a column that cannot hold a null.
        /// </summary>
        private (ArrowBuffer Bitmap, int NullCount) ValidityBitmap(int[]? presence, int length)
        {
            if (!IsNullable)
                return (ArrowBuffer.Empty, 0);

            var bitmap = new byte[(length + 7) / 8];
            int valid = 0;
            for (int i = 0; i < length; i++)
            {
                if (presence is null || presence[i] != 0)
                {
                    bitmap[i >> 3] |= (byte)(1 << (i & 7));
                    valid++;
                }
            }

            return (new ArrowBuffer(bitmap), length - valid);
        }

        // ───── Plain values ─────

        // Declined by the first-page test (or never tried): values are held as Arrow lays them out, a slot per level,
        // with no lookup, and at flush handed to ParquetFileWriter's path as an array with no rebuild. For a fixed
        // width, _plainValues holds the slots; for BYTE_ARRAY, the values' bytes, and _plainOffsets their offsets.
        private bool _declined;
        private byte[] _plainValues = [];
        private long _plainLength;
        private byte[]? _plainOffsets;
        private int _plainLevels;

        // The first-page test: the plain bytes of the values fed while it is pending (BYTE_ARRAY; a fixed width
        // counts NonNullCount instead), and whether it has been taken.
        private long _byteArrayPlainBytes;
        private bool _firstPageTested;

        /// <summary>
        /// Whether values are held plain: the column has no dictionary, or the first-page test declined it. Such a
        /// column takes no lookups, and is written in its own encoding.
        /// </summary>
        public bool Plain => (_declined || !DictionaryEnabled) && PhysicalType != PhysicalType.Boolean;

        /// <summary>An Arrow slot's width for <see cref="ArrowType"/>; 0 for BYTE_ARRAY, whose values have offsets.</summary>
        private int SlotWidth => ArrowType switch
        {
            Int8Type or UInt8Type => 1,
            Int16Type or UInt16Type or HalfFloatType => 2,
            Int32Type or UInt32Type or Date32Type or Time32Type or FloatType => 4,
            Int64Type or UInt64Type or Date64Type or Time64Type or TimestampType or DurationType or DoubleType => 8,
            FixedSizeBinaryType fsb => fsb.ByteWidth,
            _ => 0,
        };

        private void AppendPlain(ArrayData data, int levelCount)
        {
            int width = SlotWidth;
            if (width > 0)
            {
                var source = data.Buffers[1].Span.Slice(data.Offset * width, levelCount * width);
                source.CopyTo(PlainSpace(source.Length));
            }
            else
            {
                var sourceOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span).Slice(data.Offset, levelCount + 1);
                var sourceData = data.Buffers[2].Span;

                long neededOffsets = ((long)_plainLevels + levelCount + 1) * sizeof(int);
                if (_plainOffsets is null || neededOffsets > _plainOffsets.Length)
                {
                    if (neededOffsets > DictionaryEncoder.BytesHashTable.MaxArrayLength)
                        throw new InvalidOperationException(
                            "A column's values in one row group exceed the largest array .NET can allocate; flush row groups sooner.");
                    System.Array.Resize(ref _plainOffsets, (int)Math.Min(DictionaryEncoder.BytesHashTable.MaxArrayLength,
                        Math.Max(neededOffsets, Math.Max(1024, (long)(_plainOffsets?.Length ?? 0) * 2))));
                }

                // offsets[0] is already the end of the values before (0 for the first append).
                var offsets = MemoryMarshal.Cast<byte, int>(_plainOffsets.AsSpan()).Slice(_plainLevels, levelCount + 1);

                if (!HasLargeValue(sourceOffsets))
                {
                    // The common case: the append's values end to end, copied whole, their offsets moved.
                    int start = sourceOffsets[0];
                    int length = sourceOffsets[levelCount] - start;
                    int delta = checked((int)(_plainLength - start));
                    sourceData.Slice(start, length).CopyTo(PlainSpace(length));
                    for (int i = 1; i <= levelCount; i++)
                        offsets[i] = sourceOffsets[i] + delta;
                }
                else
                {
                    AppendPlainWithLargeValues(sourceOffsets, sourceData, offsets);
                }
            }

            _plainLevels += levelCount;
        }

        // ───── Large repeated values ─────

        // A plain BYTE_ARRAY value this long is looked up in a small cache of recent large values, and a repeat is
        // held as a reference to the earlier copy rather than copied again, so a few distinct large values repeated
        // over many rows cost a few copies and a reference per row, as they did when every value was looked up.
        // Shorter values are copied without a lookup: a reference would cost as much as the value.
        private const int LargeValueBytes = 64;

        // Per slot: where in _plainValues a recent large value's copy starts, plus one, and its length.
        private int[]? _recentLargeStarts;
        private int[]? _recentLargeLengths;

        // The levels held by reference, in level order: their offsets give them no bytes of their own. Null when none.
        private List<(int Level, int Start, int Length)>? _plainReferences;

        private static bool HasLargeValue(ReadOnlySpan<int> offsets)
        {
            for (int i = 1; i < offsets.Length; i++)
            {
                if (offsets[i] - offsets[i - 1] >= LargeValueBytes)
                    return true;
            }

            return false;
        }

        private void AppendPlainWithLargeValues(ReadOnlySpan<int> sourceOffsets, ReadOnlySpan<byte> sourceData, Span<int> offsets)
        {
            _recentLargeStarts ??= new int[RecentSlots];
            _recentLargeLengths ??= new int[RecentSlots];
            int levelCount = sourceOffsets.Length - 1;
            for (int i = 0; i < levelCount; i++)
            {
                int start = sourceOffsets[i];
                int length = sourceOffsets[i + 1] - start;
                var value = sourceData.Slice(start, length);
                if (length >= LargeValueBytes)
                {
                    int slot = LargeValueSlot(value);
                    int cached = _recentLargeStarts[slot] - 1;
                    if (cached >= 0 && _recentLargeLengths[slot] == length
                        && value.SequenceEqual(_plainValues.AsSpan(cached, length)))
                    {
                        (_plainReferences ??= []).Add((_plainLevels + i, cached, length));
                        offsets[i + 1] = offsets[i];
                        continue;
                    }

                    _recentLargeStarts[slot] = checked((int)_plainLength + 1);
                    _recentLargeLengths[slot] = length;
                }

                value.CopyTo(PlainSpace(length));
                offsets[i + 1] = checked((int)_plainLength);
            }
        }

        /// <summary>A large value's cache slot, from its length and three of its words: cheap, and checked on a hit.</summary>
        private static int LargeValueSlot(ReadOnlySpan<byte> value)
        {
            ulong middle = MemoryMarshal.Read<ulong>(value.Slice(value.Length / 2));
            ulong last = MemoryMarshal.Read<ulong>(value.Slice(value.Length - 8));
            ulong key = (ulong)value.Length
                ^ MemoryMarshal.Read<ulong>(value)
                ^ ((middle << 21) | (middle >> 43))
                ^ ((last << 42) | (last >> 22));
            return (int)((key * 0x9E3779B97F4A7C15UL) >> 54); // the top 10 bits: RecentSlots = 1 << 10
        }

        /// <summary><paramref name="count"/> new bytes at the end of the plain values, for the caller to fill.</summary>
        private Span<byte> PlainSpace(int count)
        {
            long needed = _plainLength + count;
            if (needed > _plainValues.Length)
            {
                if (needed > DictionaryEncoder.BytesHashTable.MaxArrayLength)
                    throw new InvalidOperationException(
                        "A column's values in one row group exceed the largest array .NET can allocate; flush row groups sooner.");
                System.Array.Resize(ref _plainValues, (int)Math.Min(DictionaryEncoder.BytesHashTable.MaxArrayLength,
                    Math.Max(needed, Math.Max(1024, (long)_plainValues.Length * 2))));
            }

            var span = _plainValues.AsSpan((int)_plainLength, count);
            _plainLength = needed;
            return span;
        }

        /// <summary>
        /// The plain values as an array of a slot per level, of the type <see cref="ReconstructArrowArray"/> rebuilds
        /// a dictionary column as, so the two reach ParquetFileWriter's path alike. It borrows this state's buffers:
        /// encode it before the state takes another value.
        /// </summary>
        public IArrowArray BuildPlainArray(int[]? presence)
        {
            int length = LevelCount;
            var (validity, nullCount) = ValidityBitmap(presence, length);
            var values = new ArrowBuffer(_plainValues.AsMemory(0, (int)_plainLength));
            IArrowType type = ArrowType switch
            {
                Int32Type or Date32Type or Time32Type => Int32Type.Default,
                Int64Type or Date64Type or Time64Type or TimestampType or DurationType => Int64Type.Default,
                _ => ArrowType,
            };

            ArrowBuffer[] buffers;
            if (SlotWidth > 0)
                buffers = [validity, values];
            else if (_plainReferences is null)
                buffers = [validity, new ArrowBuffer(_plainOffsets is null
                    ? new byte[sizeof(int)]
                    : _plainOffsets.AsMemory(0, (length + 1) * sizeof(int))), values];
            else
                buffers = [validity, .. ResolveReferences(length)];
            return Data.ArrowArrayFactory.BuildArray(new ArrayData(type, length, nullCount, 0, buffers));
        }

        /// <summary>The offsets and values of a BYTE_ARRAY column some of whose levels are held by reference.</summary>
        private ArrowBuffer[] ResolveReferences(int length)
        {
            var references = _plainReferences!;
            long total = _plainLength;
            foreach (var reference in references)
                total += reference.Length;
            if (total > DictionaryEncoder.BytesHashTable.MaxArrayLength)
                throw new InvalidOperationException(
                    "A column's values in one row group exceed the largest array .NET can allocate; flush row groups sooner.");

            var heldOffsets = MemoryMarshal.Cast<byte, int>(_plainOffsets.AsSpan()).Slice(0, length + 1);
            var offsetBytes = new byte[(length + 1) * sizeof(int)];
            var offsets = MemoryMarshal.Cast<byte, int>(offsetBytes.AsSpan());
            var data = new byte[total];
            int written = 0, next = 0;
            for (int level = 0; level < length; level++)
            {
                ReadOnlySpan<byte> value;
                if (next < references.Count && references[next].Level == level)
                {
                    var (_, start, count) = references[next++];
                    value = _plainValues.AsSpan(start, count);
                }
                else
                {
                    value = _plainValues.AsSpan(heldOffsets[level], heldOffsets[level + 1] - heldOffsets[level]);
                }

                value.CopyTo(data.AsSpan(written));
                written += value.Length;
                offsets[level + 1] = written;
            }

            return [new ArrowBuffer(offsetBytes), new ArrowBuffer(data)];
        }

        /// <summary>
        /// Takes the first-page test once the values fed reach <see cref="DictionaryEncoder.FirstPageBytes"/> written
        /// plain, and on a decline moves the column to plain values. The test reads only those first values, so it
        /// answers here as it would at flush; a chunk that ends short of them is still tested at flush.
        /// </summary>
        private void TakeFirstPageTest()
        {
            long plainBytes = PhysicalType switch
            {
                PhysicalType.ByteArray => _byteArrayPlainBytes,
                PhysicalType.Int32 or PhysicalType.Float => 4L * NonNullCount,
                PhysicalType.Int64 or PhysicalType.Double => 8L * NonNullCount,
                _ => (long)TypeLength * NonNullCount,
            };
            if (plainBytes < DictionaryEncoder.FirstPageBytes)
                return;

            _firstPageTested = true;

            // The test reads the page only for BYTE_ARRAY entry lengths and the indices only up to the first page's
            // values, so both are lent as they stand.
            byte[] page = _bytesTable is not null ? _bytesTable.ToPage() : _fixedPage;
            var dictionary = new DictionaryEncoder.DictionaryResult
            {
                DictionaryPageData = page,
                DictionaryCount = DictionaryCount,
                Indices = Indices!.Buffer,
            };
            if (DictionaryEncoder.FirstPagePays(dictionary, NonNullCount, PhysicalType, TypeLength, DictionaryEncoder.FirstPageBytes))
                return;

            // Declined, as ParquetFileWriter declines it: whether or not the dictionary also filled first, it would
            // have declined. The values so far are rebuilt once, and the dictionary dropped.
            int pageLength;
            if (TryBorrowPage(out byte[] borrowed))
            {
                page = borrowed;
                pageLength = DictionaryPageLength;
            }
            else
            {
                page = BuildDictionaryPage();
                pageLength = page.Length;
            }

            using var rebuilt = ReconstructArrowArray(
                this,
                new DictionaryEncoder.DictionaryResult
                {
                    DictionaryPageData = page,
                    DictionaryCount = DictionaryCount,
                    Indices = Indices.ToArray(NonNullCount),
                },
                pageLength,
                PresenceLevels(this, DefLevels?.ToArray(LevelCount)),
                LevelCount);

            ResetDictionary();
            // Released, not kept for reuse: a declined column will not look anything up again this row group.
            _fixedDict = null;
            _longDict = null;
            _floatDict = null;
            _doubleDict = null;
            _fixedPage = [];
            Indices.Reset();
            _declined = true;
            AppendPlain(rebuilt.Data, LevelCount);
        }

        private void ResetDictionary()
        {
            _fixedDict?.Clear();
            _longDict?.Clear();
            _floatDict?.Clear();
            _doubleDict?.Clear();
            _bytesTable = null;
            _dictPageSize = 0;
            _dictionaryFull = false;
            if (_recentEntries is not null)
                System.Array.Clear(_recentEntries, 0, _recentEntries.Length);
            DictionaryCount = 0;
        }

        private void DictionaryEncodeArray(IArrowArray array, int rowCount, int srcOffset)
        {
            switch (ArrowType)
            {
                case Apache.Arrow.Types.Int8Type or Apache.Arrow.Types.UInt8Type:
                    EncodeFixed<byte>(MemoryMarshal.Cast<byte, byte>(
                        array.Data.Buffers[1].Span), srcOffset, rowCount);
                    break;

                case Apache.Arrow.Types.Int16Type or Apache.Arrow.Types.UInt16Type
                    or Apache.Arrow.Types.HalfFloatType:
                    EncodeFixed<short>(MemoryMarshal.Cast<byte, short>(
                        array.Data.Buffers[1].Span), srcOffset, rowCount);
                    break;

                case Apache.Arrow.Types.Int32Type or Apache.Arrow.Types.UInt32Type
                    or Apache.Arrow.Types.Date32Type or Apache.Arrow.Types.Time32Type:
                    EncodeFixedInt(MemoryMarshal.Cast<byte, int>(
                        array.Data.Buffers[1].Span), srcOffset, rowCount);
                    break;

                case Apache.Arrow.Types.Int64Type or Apache.Arrow.Types.UInt64Type
                    or Apache.Arrow.Types.Date64Type or Apache.Arrow.Types.Time64Type
                    or Apache.Arrow.Types.TimestampType or Apache.Arrow.Types.DurationType:
                    EncodeFixedLong(MemoryMarshal.Cast<byte, long>(
                        array.Data.Buffers[1].Span), srcOffset, rowCount);
                    break;

                case Apache.Arrow.Types.FloatType:
                    EncodeFixedFloat(MemoryMarshal.Cast<byte, float>(
                        array.Data.Buffers[1].Span), srcOffset, rowCount);
                    break;

                case Apache.Arrow.Types.DoubleType:
                    EncodeFixedDouble(MemoryMarshal.Cast<byte, double>(
                        array.Data.Buffers[1].Span), srcOffset, rowCount);
                    break;

                case Apache.Arrow.Types.StringType or Apache.Arrow.Types.BinaryType:
                    EncodeByteArray(array.Data, srcOffset, rowCount);
                    break;

                // Every decimal derives from FixedSizeBinaryType and lands here. A Decimal32/64 (one written as
                // INT32/INT64, #504) keeps its raw little-endian slot, which is the INT32/INT64 dictionary entry.
                case Apache.Arrow.Types.FixedSizeBinaryType fsb:
                    EncodeFixedLenByteArray(
                        array.Data.Buffers[1].Span, srcOffset, rowCount, fsb.ByteWidth, EntryWidth(fsb.ByteWidth));
                    break;
            }

            // Each arm hands a raw span off the caller's buffers to an encoder that then loops over the
            // rows, allocating. A span does not keep the array alive, so root it across the call.
            // See doc/arrow-span-lifetime.md.
            GC.KeepAlive(array);
        }

        /// <summary>
        /// Dictionary-encodes a 1- or 2-byte column: Int8/UInt8/Int16/UInt16, and HalfFloat.
        /// </summary>
        /// <remarks>
        /// The integers are written as the INT32 physical type, whose PLAIN dictionary entries are four bytes,
        /// so each entry is widened here — sign- or zero-extended as the source type says. Keeping the Arrow
        /// width made the page one or two bytes per entry, which the statistics read past and the reader
        /// refused (#441). HalfFloat is FIXED_LEN_BYTE_ARRAY(2), so its entries keep their two bytes.
        /// </remarks>
        private void EncodeFixed<T>(ReadOnlySpan<T> valueBuffer, int srcOffset, int rowCount)
            where T : unmanaged, IEquatable<T>
        {
            _fixedDict ??= new Dictionary<int, int>();
            bool asInt32 = PhysicalType == PhysicalType.Int32;
            int entrySize = asInt32 ? 4 : Marshal.SizeOf<T>();

            for (int i = 0; i < rowCount; i++)
            {
                if (!IsValue(LevelCount + i)) continue;
                T val = valueBuffer[srcOffset + i];
                // For the integers the key IS the INT32 value, so it is also what the entry holds.
                int key = asInt32 ? Int32Value(val) : Widen(val);
                bool raw = Raw;
                int idx, slot = 0;
                if (raw ? !TryRecent(unchecked((uint)key), out idx, out slot) : !_fixedDict.TryGetValue(key, out idx))
                {
                    var bytes = NewEntry(entrySize, out idx);
                    if (raw)
                        Remember(slot, unchecked((uint)key), idx);
                    else
                        _fixedDict[key] = idx;

                    if (asInt32)
                        BinaryPrimitives.WriteInt32LittleEndian(bytes, key);
                    else
#if NET8_0_OR_GREATER
                        MemoryMarshal.Write(bytes, in val);
#else
                        MemoryMarshal.Write(bytes, ref val);
#endif
                }
                Indices!.Add(idx);
            }
        }

        /// <summary>
        /// A narrow integer's value as INT32. <see cref="Widen{T}"/> cannot say: it extends by the C# type of
        /// the slot, and Int8 is read as <c>byte</c> and UInt16 as <c>short</c>, so it gets both wrong.
        /// </summary>
        private int Int32Value<T>(T val) where T : unmanaged
        {
            int slot = Widen(val);
            return ArrowType switch
            {
                Apache.Arrow.Types.Int8Type => unchecked((sbyte)slot),
                Apache.Arrow.Types.UInt8Type => unchecked((byte)slot),
                Apache.Arrow.Types.Int16Type => unchecked((short)slot),
                Apache.Arrow.Types.UInt16Type => unchecked((ushort)slot),
                _ => throw new InvalidOperationException($"{ArrowType} is not a narrow integer type."),
            };
        }

        private void EncodeFixedInt(ReadOnlySpan<int> valueBuffer, int srcOffset, int rowCount)
        {
            _fixedDict ??= new Dictionary<int, int>();

            for (int i = 0; i < rowCount; i++)
            {
                if (!IsValue(LevelCount + i)) continue;
                int val = valueBuffer[srcOffset + i];
                bool raw = Raw;
                int idx, slot = 0;
                if (raw ? !TryRecent(unchecked((uint)val), out idx, out slot) : !_fixedDict.TryGetValue(val, out idx))
                {
                    var bytes = NewEntry(4, out idx);
                    if (raw)
                        Remember(slot, unchecked((uint)val), idx);
                    else
                        _fixedDict[val] = idx;
                    BinaryPrimitives.WriteInt32LittleEndian(bytes, val);
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeFixedLong(ReadOnlySpan<long> valueBuffer, int srcOffset, int rowCount)
        {
            _longDict ??= new Dictionary<long, int>();

            for (int i = 0; i < rowCount; i++)
            {
                if (!IsValue(LevelCount + i)) continue;
                long val = valueBuffer[srcOffset + i];
                bool raw = Raw;
                int idx, slot = 0;
                if (raw ? !TryRecent(unchecked((ulong)val), out idx, out slot) : !_longDict.TryGetValue(val, out idx))
                {
                    var bytes = NewEntry(8, out idx);
                    if (raw)
                        Remember(slot, unchecked((ulong)val), idx);
                    else
                        _longDict[val] = idx;
                    BinaryPrimitives.WriteInt64LittleEndian(bytes, val);
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeFixedFloat(ReadOnlySpan<float> valueBuffer, int srcOffset, int rowCount)
        {
            _floatDict ??= new Dictionary<uint, int>();

            var bitBuffer = MemoryMarshal.Cast<float, uint>(valueBuffer);

            for (int i = 0; i < rowCount; i++)
            {
                if (!IsValue(LevelCount + i)) continue;
                uint bits = bitBuffer[srcOffset + i];
                bool raw = Raw;
                int idx, slot = 0;
                if (raw ? !TryRecent(bits, out idx, out slot) : !_floatDict.TryGetValue(bits, out idx))
                {
                    var bytes = NewEntry(4, out idx);
                    if (raw)
                        Remember(slot, bits, idx);
                    else
                        _floatDict[bits] = idx;
                    BinaryPrimitives.WriteUInt32LittleEndian(bytes, bits);
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeFixedDouble(ReadOnlySpan<double> valueBuffer, int srcOffset, int rowCount)
        {
            _doubleDict ??= new Dictionary<ulong, int>();

            var bitBuffer = MemoryMarshal.Cast<double, ulong>(valueBuffer);

            for (int i = 0; i < rowCount; i++)
            {
                if (!IsValue(LevelCount + i)) continue;
                ulong bits = bitBuffer[srcOffset + i];
                bool raw = Raw;
                int idx, slot = 0;
                if (raw ? !TryRecent(bits, out idx, out slot) : !_doubleDict.TryGetValue(bits, out idx))
                {
                    var bytes = NewEntry(8, out idx);
                    if (raw)
                        Remember(slot, bits, idx);
                    else
                        _doubleDict[bits] = idx;
                    BinaryPrimitives.WriteUInt64LittleEndian(bytes, bits);
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeByteArray(ArrayData data, int srcOffset, int rowCount)
        {
            _bytesTable ??= new DictionaryEncoder.BytesHashTable(lengthPrefixed: true);
            ReadOnlySpan<int> arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
            ReadOnlySpan<byte> arrowData = data.Buffers[2].Span;

            long plainBytes = 0;
            for (int i = 0; i < rowCount; i++)
            {
                if (!IsValue(LevelCount + i)) continue;
                int start = arrowOffsets[srcOffset + i];
                int len = arrowOffsets[srcOffset + i + 1] - start;
                ReadOnlySpan<byte> valueBytes = arrowData.Slice(start, len);
                plainBytes += 4 + len;

                Indices!.Add(AddBytes(valueBytes));
            }

            _byteArrayPlainBytes += plainBytes;

            // `data` is the only thing rooting the buffers the two spans above point into, and the loop
            // allocates on every new distinct value. See doc/arrow-span-lifetime.md.
            GC.KeepAlive(data);
        }

        /// <summary>
        /// Whether a raw value with bits <paramref name="key"/> is the one last cached in its slot, whose entry it
        /// then shares (<paramref name="index"/>). <paramref name="slot"/> is where <see cref="Remember"/> caches a new
        /// one. Bounded: repeats of a small working set cost one entry each, as they did when every value was looked
        /// up, without the lookup table growing.
        /// </summary>
        private bool TryRecent(ulong key, out int index, out int slot)
        {
            _recentKeys ??= new ulong[RecentSlots];
            _recentEntries ??= new int[RecentSlots];
            slot = (int)((key * 0x9E3779B97F4A7C15UL) >> 54); // the top 10 bits: RecentSlots = 1 << 10
            index = _recentEntries[slot] - 1;
            return index >= 0 && _recentKeys[slot] == key;
        }

        private void Remember(int slot, ulong key, int index)
        {
            _recentKeys![slot] = key;
            _recentEntries![slot] = index + 1;
        }

        /// <summary>The bytes of the dictionary page as buffered (before a decimal's entries are reversed).</summary>
        public int DictionaryPageLength => _bytesTable?.ByteCount ?? _dictPageSize;

        /// <summary>
        /// The buffered entries in place, for a column rebuilt at flush rather than written with its dictionary:
        /// the first <see cref="DictionaryPageLength"/> bytes of <paramref name="page"/>. False for a decimal, whose
        /// entries a written page reverses and the rebuild expects reversed, and for the byte-keyed arms, whose table
        /// keeps its buffer private; those take <see cref="BuildDictionaryPage"/>.
        /// </summary>
        public bool TryBorrowPage(out byte[] page)
        {
            page = _fixedPage;
            return _bytesTable is null && !IsFlbaDecimal(ArrowType);
        }

        /// <summary>
        /// A new fixed-width entry of <paramref name="size"/> bytes at the end of the page, numbered
        /// <paramref name="index"/>, for the caller to fill. Marks the dictionary full once it passes the limit.
        /// </summary>
        private Span<byte> NewEntry(int size, out int index)
        {
            // In long: doubling past 1 GiB wraps an int, and growing by one entry each time after that copied the
            // whole page per value.
            long needed = (long)_dictPageSize + size;
            if (needed > _fixedPage.Length)
            {
                if (needed > DictionaryEncoder.BytesHashTable.MaxArrayLength)
                    throw new InvalidOperationException(
                        "A column's values in one row group exceed the largest array .NET can allocate; flush row groups sooner.");
                System.Array.Resize(ref _fixedPage, (int)Math.Min(DictionaryEncoder.BytesHashTable.MaxArrayLength,
                    Math.Max(256, Math.Max((long)_fixedPage.Length * 2, needed))));
            }

            var entry = _fixedPage.AsSpan(_dictPageSize, size);
            _dictPageSize += size;
            index = DictionaryCount++;
            if (_dictPageSize > DictionaryLimit)
                _dictionaryFull = true;
            return entry;
        }

        /// <summary>
        /// A byte-keyed value's entry: looked up, and added if new, while the dictionary is under the limit; appended
        /// as an entry of its own after (see <see cref="Raw"/>).
        /// </summary>
        private int AddBytes(ReadOnlySpan<byte> valueBytes)
        {
            if (Raw)
            {
                int entry = _bytesTable!.AppendRecent(valueBytes, out bool added);
                if (added)
                    DictionaryCount++;
                return entry;
            }

            int idx = _bytesTable!.GetOrAdd(valueBytes, out bool inserted);
            if (inserted)
            {
                DictionaryCount++;
                if (_bytesTable.ByteCount > DictionaryLimit)
                    _dictionaryFull = true;
            }

            return idx;
        }

        /// <summary>
        /// A dictionary entry's width for a FIXED_LEN_BYTE_ARRAY column whose Arrow slots are
        /// <paramref name="slotWidth"/> bytes: the column's own width for a decimal, which is the narrowest its
        /// precision allows (#504), and the slot width for everything else.
        /// </summary>
        public int EntryWidth(int slotWidth) => IsFlbaDecimal(ArrowType) ? TypeLength : slotWidth;

        // A wide decimal written as INT32/INT64 arrives in Arrow's 16- or 32-byte slots; ArrowType is the
        // Decimal32/64 it is buffered as.
        private IArrowArray NarrowDecimal(IArrowArray array) =>
            ArrowType is Apache.Arrow.Types.Decimal32Type or Apache.Arrow.Types.Decimal64Type
                && DecimalNarrowing.IsWideDecimal(array.Data.DataType)
                ? DecimalNarrowing.ToInteger(array, ArrowType)
                : array;

        private void EncodeFixedLenByteArray(
            ReadOnlySpan<byte> valueBuffer, int srcOffset, int rowCount, int byteWidth, int entryWidth)
        {
            _bytesTable ??= new DictionaryEncoder.BytesHashTable(lengthPrefixed: false);

            // A decimal's entries are kept little-endian at the column's width; BuildDictionaryPage reverses them.
            byte[]? narrowed = entryWidth < byteWidth ? new byte[entryWidth] : null;

            for (int i = 0; i < rowCount; i++)
            {
                if (!IsValue(LevelCount + i)) continue;
                ReadOnlySpan<byte> valueBytes = valueBuffer.Slice((srcOffset + i) * byteWidth, byteWidth);
                if (entryWidth < byteWidth)
                {
                    DecimalNarrowing.Narrow(valueBytes, narrowed!, entryWidth, bigEndian: false);
                    valueBytes = narrowed;
                }

                Indices!.Add(AddBytes(valueBytes));
            }
        }

        public byte[] BuildDictionaryPage()
        {
            if (_bytesTable is not null)
            {
                // Already the PLAIN page. A decimal's entries are little-endian at the column's width: reverse each.
                var table = _bytesTable.ToPage();
                if (IsFlbaDecimal(ArrowType))
                {
                    for (int at = 0; at + TypeLength <= table.Length; at += TypeLength)
                        table.AsSpan(at, TypeLength).Reverse();
                }

                return table;
            }

            // The fixed-width arms' entries, which are the page as they stand.
            return _fixedPage.AsSpan(0, _dictPageSize).ToArray();
        }

        public void Reset()
        {
            ResetDictionary();
            // A page that held raw values is as large as the column was; keep one the size of a dictionary for
            // reuse, and release a larger one rather than root it for the writer's lifetime. Plain values are as
            // large as the column, so they are released too.
            if (_fixedPage.Length > DictionaryLimit)
                _fixedPage = [];
            _declined = false;
            _plainValues = [];
            _plainOffsets = null;
            _plainLength = 0;
            _plainLevels = 0;
            _plainReferences = null;
            if (_recentLargeStarts is not null)
                System.Array.Clear(_recentLargeStarts, 0, _recentLargeStarts.Length);
            _byteArrayPlainBytes = 0;
            _firstPageTested = false;
            _booleanBits.AsSpan(0, Math.Min(_booleanBits.Length, (LevelCount + 7) / 8)).Clear();
            Indices?.Reset();
            DefLevels?.Reset();
            RepLevels?.Reset();
            LevelCount = 0;
            NonNullCount = 0;
        }

        private static int Widen<T>(T val) where T : unmanaged
        {
            if (typeof(T) == typeof(byte)) return (byte)(object)val;
            if (typeof(T) == typeof(sbyte)) return (sbyte)(object)val;
            if (typeof(T) == typeof(short)) return (short)(object)val;
            if (typeof(T) == typeof(ushort)) return (ushort)(object)val;
            if (typeof(T) == typeof(int)) return (int)(object)val;
            if (typeof(T) == typeof(uint)) return unchecked((int)(uint)(object)val);
            return val.GetHashCode();
        }
    }

    /// <summary>
    /// Growable int list backed by a simple array, avoiding List&lt;int&gt; overhead.
    /// </summary>
    private sealed class GrowableIntList
    {
        private int[] _data;
        private int _count;

        public GrowableIntList(int initialCapacity)
        {
            _data = new int[Math.Max(initialCapacity, 256)];
        }

        public void Add(int value)
        {
            if (_count >= _data.Length)
                System.Array.Resize(ref _data, _data.Length * 2);
            _data[_count++] = value;
        }

        /// <summary><paramref name="count"/> new elements at the end, for the caller to fill.</summary>
        public Span<int> AppendSpan(int count)
        {
            long needed = (long)_count + count;
            if (needed > _data.Length)
            {
                if (needed > DictionaryEncoder.BytesHashTable.MaxArrayLength / sizeof(int))
                    throw new InvalidOperationException(
                        "A column's levels in one row group exceed the largest array .NET can allocate; flush row groups sooner.");
                System.Array.Resize(ref _data, (int)Math.Min(
                    DictionaryEncoder.BytesHashTable.MaxArrayLength / sizeof(int), Math.Max(needed, (long)_data.Length * 2)));
            }

            var span = _data.AsSpan(_count, count);
            _count += count;
            return span;
        }

        public ReadOnlySpan<int> Slice(int start, int count) => _data.AsSpan(start, count);

        /// <summary>The backing array, whose first elements are the list; longer than it.</summary>
        public int[] Buffer => _data;

        public int Get(int index) => _data[index];

        public int[] ToArray(int count)
        {
            var result = new int[count];
            System.Array.Copy(_data, result, count);
            return result;
        }

        public void Reset() => _count = 0;
    }
}
