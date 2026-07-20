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
/// </remarks>
public sealed class BufferedParquetWriter : IAsyncDisposable, IDisposable
{
    private static readonly byte[] Par1Magic = "PAR1"u8.ToArray();

    private readonly ISequentialFile _file;
    private readonly bool _ownsFile;
    private readonly ParquetWriteOptions _options;
    private readonly List<RowGroup> _rowGroups = new();
    private IReadOnlyList<SchemaElement>? _parquetSchema;
    private Apache.Arrow.Schema? _arrowSchema;
    private bool _headerWritten;
    private bool _closed;
    private bool _disposed;

    // Buffered column state — one per leaf column
    private BufferedColumnState[]? _columnStates;
    private int _bufferedRows;

    /// <summary>
    /// Creates a new buffered Parquet writer.
    /// </summary>
    public BufferedParquetWriter(ISequentialFile file, bool ownsFile = true, ParquetWriteOptions? options = null)
    {
        _file = file;
        _ownsFile = ownsFile;
        _options = options ?? ParquetWriteOptions.Default;
    }

    /// <summary>Number of rows currently buffered and not yet flushed.</summary>
    public int BufferedRows => _bufferedRows;

    /// <summary>
    /// Appends a <see cref="RecordBatch"/> to the buffer. Dictionary indices are accumulated;
    /// the batch's Arrow value buffers are not retained.
    /// If the accumulated rows reach <see cref="ParquetWriteOptions.RowGroupMaxRows"/>, the
    /// buffer is automatically flushed as a row group.
    /// </summary>
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

        if (!_headerWritten)
        {
            _arrowSchema = batch.Schema;
            _parquetSchema = ArrowToSchemaConverter.Convert(_arrowSchema);
            await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);
            _headerWritten = true;
        }

        // Initialize column states on first batch
        if (_columnStates == null)
            InitializeColumnStates(batch);

        // Dictionary-encode each column and append indices
        AppendBatchToBuffers(batch);

        // Auto-flush if we've reached the row group limit
        while (_bufferedRows >= _options.RowGroupMaxRows)
            await FlushRowGroupAsync(cancellationToken).ConfigureAwait(false);
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

        // Build DictionaryResult for each column, and decide encoding strategy
        var dictResults = new DictionaryEncoder.DictionaryResult?[states.Length];
        var useNonDictionary = new bool[states.Length]; // true if cardinality too high → plain encoding
        int[]?[] defLevelsPerColumn = new int[]?[states.Length];

        for (int i = 0; i < states.Length; i++)
        {
            var s = states[i];
            defLevelsPerColumn[i] = s.DefLevels?.ToArray(s.RowCount);
            if (s.DictionaryCount > 0)
            {
                // Check cardinality threshold: if too many distinct values, fall back to
                // non-dictionary encoding at flush time (reconstruct values from dict + indices)
                float cardinality = s.NonNullCount > 0
                    ? (float)s.DictionaryCount / s.NonNullCount
                    : 0;

                if (_options.DictionaryEnabled && cardinality <= DictionaryEncoder.CardinalityThreshold)
                {
                    dictResults[i] = new DictionaryEncoder.DictionaryResult
                    {
                        DictionaryPageData = s.BuildDictionaryPage(),
                        DictionaryCount = s.DictionaryCount,
                        Indices = s.Indices!.ToArray(s.NonNullCount),
                    };
                }
                else
                {
                    // Cardinality too high — will reconstruct and use non-dictionary encoding
                    useNonDictionary[i] = true;
                    dictResults[i] = new DictionaryEncoder.DictionaryResult
                    {
                        DictionaryPageData = s.BuildDictionaryPage(),
                        DictionaryCount = s.DictionaryCount,
                        Indices = s.Indices!.ToArray(s.NonNullCount),
                    };
                }
            }
        }

        // Encode all columns into page bytes
        var columnResults = new ColumnChunkWriter.ColumnChunkResult[states.Length];

        Parallel.For(0, states.Length, i =>
        {
            var s = states[i];
            if (dictResults[i] != null && !useNonDictionary[i])
            {
                // Dictionary encoding: cardinality is within threshold
                columnResults[i] = ColumnChunkWriter.WriteDictionaryColumnFromResult(
                    dictResults[i]!.Value, numRows, s.PathInSchema, s.PhysicalType,
                    s.MaxDefLevel, s.MaxRepLevel, defLevelsPerColumn[i], null, _options);
            }
            else if (dictResults[i] != null && useNonDictionary[i])
            {
                // Cardinality too high: reconstruct Arrow array from dictionary + indices
                // and encode via the standard non-dictionary path (delta, BSS, plain, etc.)
                var array = ReconstructArrowArray(s, dictResults[i]!.Value,
                    defLevelsPerColumn[i], numRows);
                var nonDictOptions = _options with { DictionaryEnabled = false };
                columnResults[i] = ColumnChunkWriter.WriteColumn(
                    array, s.PathInSchema, s.PhysicalType, s.TypeLength,
                    s.IsNullable, nonDictOptions);
            }
            else if (s.BooleanValues != null)
            {
                var boolArr = BuildBooleanArray(s, numRows);
                columnResults[i] = ColumnChunkWriter.WriteColumn(
                    boolArr, s.PathInSchema, PhysicalType.Boolean, 0, s.IsNullable, _options);
            }
            else
            {
                columnResults[i] = WriteNullOnlyColumn(s, numRows);
            }
        });

        // Write column chunks sequentially
        var columnChunks = new ColumnChunk[columnResults.Length];
        long totalByteSize = 0;
        long totalCompressedSize = 0;

        for (int i = 0; i < columnResults.Length; i++)
        {
            var result = columnResults[i];
            long chunkStart = _file.Position;

            await _file.WriteAsync(result.Data, cancellationToken).ConfigureAwait(false);

            long dataPageOffset = chunkStart + result.DictionaryPageSize;
            long? dictionaryPageOffset = result.DictionaryPageSize > 0 ? chunkStart : null;

            // Write Bloom filter block if present.
            long? bloomFilterOffset = null;
            int? bloomFilterLength = null;
            if (result.BloomFilterData != null)
            {
                bloomFilterOffset = _file.Position;
                bloomFilterLength = result.BloomFilterData.Length;
                await _file.WriteAsync(result.BloomFilterData, cancellationToken).ConfigureAwait(false);
            }

            var meta = new ColumnMetaData
            {
                Type = result.MetaData.Type,
                Encodings = result.MetaData.Encodings,
                PathInSchema = result.MetaData.PathInSchema,
                Codec = result.MetaData.Codec,
                NumValues = result.MetaData.NumValues,
                TotalUncompressedSize = result.MetaData.TotalUncompressedSize,
                TotalCompressedSize = result.MetaData.TotalCompressedSize,
                DataPageOffset = dataPageOffset,
                DictionaryPageOffset = dictionaryPageOffset,
                Statistics = result.MetaData.Statistics,
                BloomFilterOffset = bloomFilterOffset,
                BloomFilterLength = bloomFilterLength,
            };

            columnChunks[i] = new ColumnChunk { FileOffset = chunkStart, MetaData = meta };
            totalByteSize += result.MetaData.TotalUncompressedSize;
            totalCompressedSize += result.MetaData.TotalCompressedSize;
        }

        _rowGroups.Add(new RowGroup
        {
            Columns = columnChunks,
            TotalByteSize = totalByteSize,
            NumRows = numRows,
            TotalCompressedSize = totalCompressedSize,
            Ordinal = checked((short)_rowGroups.Count),
        });

        // Reset buffers for next row group
        ResetBuffers();
    }

    /// <summary>
    /// Finalizes the file: flushes any remaining buffered rows, writes the footer.
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

        if (!_headerWritten)
        {
            await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);
            _headerWritten = true;
            _parquetSchema ??= [new SchemaElement { Name = "schema", NumChildren = 0 }];
        }

        long totalRows = 0;
        foreach (var rg in _rowGroups)
            totalRows += rg.NumRows;

        var fileMetaData = new FileMetaData
        {
            Version = 2,
            Schema = _parquetSchema!,
            NumRows = totalRows,
            RowGroups = _rowGroups,
            CreatedBy = _options.CreatedBy,
            KeyValueMetadata = _options.KeyValueMetadata,
            ColumnOrders = ColumnOrderBuilder.Build(_parquetSchema!, _options.FloatColumnOrder),
        };

#pragma warning disable EWPARQUET0002 // Honoring the caller's opt-in; the experimental signal lives on the option itself.
        byte[] footerBytes = MetadataEncoder.EncodeFileMetaData(fileMetaData, writePathInSchema: !_options.OmitPathInSchema);
#pragma warning restore EWPARQUET0002
        await _file.WriteAsync(footerBytes, cancellationToken).ConfigureAwait(false);

        var footerLengthBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(footerLengthBytes, footerBytes.Length);
        await _file.WriteAsync(footerLengthBytes, cancellationToken).ConfigureAwait(false);

        await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);
        await _file.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        if (!_closed)
            await CloseAsync().ConfigureAwait(false);

        _disposed = true;

        if (_ownsFile)
            await _file.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        if (_ownsFile) _file.Dispose();
    }

    // ───── Initialization ─────

    private void InitializeColumnStates(RecordBatch batch)
    {
        var leafColumns = new List<BufferedColumnState>();

        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var field = _arrowSchema!.FieldsList[c];
            var element = FindLeafElement(_parquetSchema!, field.Name);

            // Encoders dispatch on the storage type, not the ExtensionType
            // wrapper (which they don't recognise). Strip the extension here;
            // ArrowToSchemaConverter has already emitted the correct annotation
            // on the schema side.
            IArrowType encodingType = field.DataType is ExtensionType ext
                ? ext.StorageType
                : field.DataType;

            leafColumns.Add(new BufferedColumnState
            {
                PathInSchema = [field.Name],
                PhysicalType = element.Type!.Value,
                TypeLength = element.TypeLength ?? 0,
                MaxDefLevel = field.IsNullable ? 1 : 0,
                MaxRepLevel = 0,
                ArrowType = encodingType,
                IsNullable = field.IsNullable,
            });
        }

        _columnStates = leafColumns.ToArray();
    }

    // ───── Batch accumulation ─────

    private void AppendBatchToBuffers(RecordBatch batch)
    {
        int rowCount = batch.Length;

        Parallel.For(0, _columnStates!.Length, c =>
        {
            var state = _columnStates[c];
            var array = batch.Column(c);
            // Mirror InitializeColumnStates: hand the encoder the storage
            // array so it dispatches on the Arrow storage type.
            if (array is ExtensionArray ea)
                array = ea.Storage;
            state.AppendArray(array, rowCount);
        });

        _bufferedRows += rowCount;
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
    private static IArrowArray ReconstructArrowArray(
        BufferedColumnState state,
        DictionaryEncoder.DictionaryResult dictResult,
        int[]? defLevels,
        int numRows)
    {
        int[] indices = dictResult.Indices;
        byte[] dictPage = dictResult.DictionaryPageData;
        int dictCount = dictResult.DictionaryCount;

        return state.ArrowType switch
        {
            Apache.Arrow.Types.Int8Type => ReconstructFixed<sbyte>(dictPage, indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.UInt8Type => ReconstructFixed<byte>(dictPage, indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.Int16Type => ReconstructFixed<short>(dictPage, indices, defLevels, numRows, state.IsNullable),
            Apache.Arrow.Types.UInt16Type => ReconstructFixed<ushort>(dictPage, indices, defLevels, numRows, state.IsNullable),
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
            Apache.Arrow.Types.FixedSizeBinaryType fsb
                => ReconstructFixedLenByteArray(dictPage, indices, defLevels, numRows, state.IsNullable, fsb.ByteWidth, state.ArrowType),
            _ => throw new NotSupportedException($"Cannot reconstruct array of type {state.ArrowType}"),
        };
    }

    private static IArrowArray ReconstructFixed<T>(
        byte[] dictPage, int[] indices, int[]? defLevels, int numRows,
        bool isNullable) where T : unmanaged
    {
        var dictValues = MemoryMarshal.Cast<byte, T>(dictPage.AsSpan());
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
        byte[] dictPage, int dictCount, int[] indices, int[]? defLevels, int numRows,
        bool isNullable, bool isString)
    {
        // Parse dictionary entries from PLAIN format: [4-byte len][data]...
        var entries = new (int offset, int length)[dictCount];
        int pos = 0;
        for (int d = 0; d < dictCount; d++)
        {
            int len = BinaryPrimitives.ReadInt32LittleEndian(dictPage.AsSpan(pos));
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
            dictPage.AsSpan(entryOff, entryLen).CopyTo(data.AsSpan(offsets[row]));
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

    private static IArrowArray ReconstructFixedLenByteArray(
        byte[] dictPage, int[] indices, int[]? defLevels, int numRows,
        bool isNullable, int byteWidth, Apache.Arrow.Types.IArrowType arrowType)
    {
        var values = new byte[numRows * byteWidth];
        var nullBitmap = isNullable ? new byte[(numRows + 7) / 8] : null;

        int idx = 0;
        for (int row = 0; row < numRows; row++)
        {
            if (defLevels != null && defLevels[row] == 0)
                continue;

            int dictIdx = indices[idx++];
            dictPage.AsSpan(dictIdx * byteWidth, byteWidth).CopyTo(values.AsSpan(row * byteWidth));
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

    // ───── Boolean array reconstruction ─────

    private static Apache.Arrow.BooleanArray BuildBooleanArray(
        BufferedColumnState state, int numRows)
    {
        var builder = new Apache.Arrow.BooleanArray.Builder();
        var values = state.BooleanValues!;
        var defLevels = state.DefLevels;

        for (int i = 0; i < numRows; i++)
        {
            if (defLevels != null && defLevels.Get(i) == 0)
                builder.AppendNull();
            else
                builder.Append(values.Get(i) != 0);
        }

        return builder.Build();
    }

    // ───── Fallback for non-dictionary columns ─────

    private ColumnChunkWriter.ColumnChunkResult WriteNullOnlyColumn(
        BufferedColumnState state, int numRows)
    {
        // Write a column with all nulls — this is a fallback for columns that
        // couldn't be dictionary-encoded (e.g. Boolean)
        var output = new MemoryStream(64);
        var metadata = new ColumnMetaData
        {
            Type = state.PhysicalType,
            Encodings = [Encoding.Plain, Encoding.Rle],
            PathInSchema = state.PathInSchema,
            Codec = _options.Compression,
            NumValues = numRows,
            TotalUncompressedSize = 0,
            TotalCompressedSize = 0,
            DataPageOffset = 0,
        };

        output.TryGetBuffer(out var buffer);
        return new ColumnChunkWriter.ColumnChunkResult
        {
            Data = buffer,
            MetaData = metadata,
        };
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

        // Running dictionary: maps values → index
        private Dictionary<int, int>? _fixedDict;
        private Dictionary<long, int>? _longDict;
        private Dictionary<float, int>? _floatDict;
        private Dictionary<double, int>? _doubleDict;
        private BytesDictionary? _bytesDict;

        // Accumulated indices (compact, non-null only)
        public GrowableIntList? Indices { get; private set; }

        // Accumulated definition levels
        public GrowableIntList? DefLevels { get; private set; }

        // Accumulated boolean values (for Boolean columns that can't be dictionary-encoded)
        public GrowableIntList? BooleanValues { get; private set; }

        // Dictionary page data (built lazily on flush)
        private List<byte[]>? _dictEntries;
        private int _dictPageSize;

        public int DictionaryCount { get; private set; }
        public int RowCount { get; private set; }
        public int NonNullCount { get; private set; }

        public void AppendArray(IArrowArray array, int rowCount)
        {
            int srcOffset = array.Data.Offset;

            // Build def levels
            if (IsNullable)
            {
                DefLevels ??= new GrowableIntList(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    bool isNull = array.IsNull(i);
                    DefLevels.Add(isNull ? 0 : 1);
                }
            }

            // Count non-nulls
            int batchNonNull = 0;
            if (!IsNullable)
            {
                batchNonNull = rowCount;
            }
            else
            {
                for (int i = 0; i < rowCount; i++)
                    if (!array.IsNull(i)) batchNonNull++;
            }

            // Dictionary-encode based on the Arrow type, or buffer boolean values
            if (PhysicalType == PhysicalType.Boolean)
            {
                // Boolean can't be dictionary-encoded — buffer raw values for flush-time encoding
                BooleanValues ??= new GrowableIntList(rowCount);
                var boolArr = (Apache.Arrow.BooleanArray)array;
                for (int i = 0; i < rowCount; i++)
                {
                    if (IsNullable && DefLevels!.Get(RowCount + i) == 0)
                        BooleanValues.Add(0); // placeholder for null position
                    else
                        BooleanValues.Add(boolArr.GetValue(i) == true ? 1 : 0);
                }
            }
            else if (batchNonNull > 0)
            {
                Indices ??= new GrowableIntList(batchNonNull);
                DictionaryEncodeArray(array, rowCount, srcOffset);
            }

            RowCount += rowCount;
            NonNullCount += batchNonNull;
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

                case Apache.Arrow.Types.FixedSizeBinaryType fsb:
                    EncodeFixedLenByteArray(array.Data.Buffers[1].Span, srcOffset, rowCount, fsb.ByteWidth);
                    break;
            }
        }

        // Generic fixed-width encoding via int key (handles Int8/16 widening to int)
        private void EncodeFixed<T>(ReadOnlySpan<T> valueBuffer, int srcOffset, int rowCount)
            where T : unmanaged, IEquatable<T>
        {
            _fixedDict ??= new Dictionary<int, int>();
            _dictEntries ??= new List<byte[]>();
            int elementSize = Marshal.SizeOf<T>();

            for (int i = 0; i < rowCount; i++)
            {
                if (IsNullable && DefLevels!.Get(RowCount + i) == 0) continue;
                T val = valueBuffer[srcOffset + i];
                int key = Widen(val);
                if (!_fixedDict.TryGetValue(key, out int idx))
                {
                    idx = DictionaryCount++;
                    _fixedDict[key] = idx;
                    var bytes = new byte[elementSize];

#if NET8_0_OR_GREATER
                    MemoryMarshal.Write(bytes, in val);
#else
                    MemoryMarshal.Write(bytes, ref val);
#endif
                    _dictEntries.Add(bytes);
                    _dictPageSize += elementSize;
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeFixedInt(ReadOnlySpan<int> valueBuffer, int srcOffset, int rowCount)
        {
            _fixedDict ??= new Dictionary<int, int>();
            _dictEntries ??= new List<byte[]>();

            for (int i = 0; i < rowCount; i++)
            {
                if (IsNullable && DefLevels!.Get(RowCount + i) == 0) continue;
                int val = valueBuffer[srcOffset + i];
                if (!_fixedDict.TryGetValue(val, out int idx))
                {
                    idx = DictionaryCount++;
                    _fixedDict[val] = idx;
                    var bytes = new byte[4];
                    BinaryPrimitives.WriteInt32LittleEndian(bytes, val);
                    _dictEntries.Add(bytes);
                    _dictPageSize += 4;
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeFixedLong(ReadOnlySpan<long> valueBuffer, int srcOffset, int rowCount)
        {
            _longDict ??= new Dictionary<long, int>();
            _dictEntries ??= new List<byte[]>();

            for (int i = 0; i < rowCount; i++)
            {
                if (IsNullable && DefLevels!.Get(RowCount + i) == 0) continue;
                long val = valueBuffer[srcOffset + i];
                if (!_longDict.TryGetValue(val, out int idx))
                {
                    idx = DictionaryCount++;
                    _longDict[val] = idx;
                    var bytes = new byte[8];
                    BinaryPrimitives.WriteInt64LittleEndian(bytes, val);
                    _dictEntries.Add(bytes);
                    _dictPageSize += 8;
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeFixedFloat(ReadOnlySpan<float> valueBuffer, int srcOffset, int rowCount)
        {
            _floatDict ??= new Dictionary<float, int>();
            _dictEntries ??= new List<byte[]>();

            for (int i = 0; i < rowCount; i++)
            {
                if (IsNullable && DefLevels!.Get(RowCount + i) == 0) continue;
                float val = valueBuffer[srcOffset + i];
                if (!_floatDict.TryGetValue(val, out int idx))
                {
                    idx = DictionaryCount++;
                    _floatDict[val] = idx;
                    var bytes = new byte[4];

#if NET8_0_OR_GREATER
                    MemoryMarshal.Write(bytes, in val);
#else
                    MemoryMarshal.Write(bytes, ref val);
#endif
                    _dictEntries.Add(bytes);
                    _dictPageSize += 4;
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeFixedDouble(ReadOnlySpan<double> valueBuffer, int srcOffset, int rowCount)
        {
            _doubleDict ??= new Dictionary<double, int>();
            _dictEntries ??= new List<byte[]>();

            for (int i = 0; i < rowCount; i++)
            {
                if (IsNullable && DefLevels!.Get(RowCount + i) == 0) continue;
                double val = valueBuffer[srcOffset + i];
                if (!_doubleDict.TryGetValue(val, out int idx))
                {
                    idx = DictionaryCount++;
                    _doubleDict[val] = idx;
                    var bytes = new byte[8];

#if NET8_0_OR_GREATER
                    MemoryMarshal.Write(bytes, in val);
#else
                    MemoryMarshal.Write(bytes, ref val);
#endif
                    _dictEntries.Add(bytes);
                    _dictPageSize += 8;
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeByteArray(ArrayData data, int srcOffset, int rowCount)
        {
            _bytesDict ??= new BytesDictionary();
            _dictEntries ??= new List<byte[]>();
            ReadOnlySpan<int> arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
            ReadOnlySpan<byte> arrowData = data.Buffers[2].Span;

            for (int i = 0; i < rowCount; i++)
            {
                if (IsNullable && DefLevels!.Get(RowCount + i) == 0) continue;
                int start = arrowOffsets[srcOffset + i];
                int len = arrowOffsets[srcOffset + i + 1] - start;
                ReadOnlySpan<byte> valueBytes = arrowData.Slice(start, len);

                int idx = _bytesDict.GetOrAdd(valueBytes, DictionaryCount);
                if (idx == DictionaryCount)
                {
                    DictionaryCount++;
                    _dictEntries.Add(valueBytes.ToArray());
                    _dictPageSize += 4 + len; // 4-byte length prefix for BYTE_ARRAY
                }
                Indices!.Add(idx);
            }
        }

        private void EncodeFixedLenByteArray(ReadOnlySpan<byte> valueBuffer, int srcOffset, int rowCount, int byteWidth)
        {
            _bytesDict ??= new BytesDictionary();
            _dictEntries ??= new List<byte[]>();

            for (int i = 0; i < rowCount; i++)
            {
                if (IsNullable && DefLevels!.Get(RowCount + i) == 0) continue;
                ReadOnlySpan<byte> valueBytes = valueBuffer.Slice((srcOffset + i) * byteWidth, byteWidth);

                int idx = _bytesDict.GetOrAdd(valueBytes, DictionaryCount);
                if (idx == DictionaryCount)
                {
                    DictionaryCount++;
                    _dictEntries.Add(valueBytes.ToArray());
                    _dictPageSize += byteWidth; // no length prefix for FLBA
                }
                Indices!.Add(idx);
            }
        }

        public byte[] BuildDictionaryPage()
        {
            if (_dictEntries == null || _dictEntries.Count == 0)
                return [];

            bool isByteArray = PhysicalType == PhysicalType.ByteArray ||
                (ArrowType is Apache.Arrow.Types.StringType or Apache.Arrow.Types.BinaryType);

            var page = new byte[_dictPageSize];
            int pos = 0;
            foreach (var entry in _dictEntries)
            {
                if (isByteArray)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(pos), entry.Length);
                    pos += 4;
                }
                entry.CopyTo(page.AsSpan(pos));
                pos += entry.Length;
            }
            return page;
        }

        public void Reset()
        {
            _fixedDict?.Clear();
            _longDict?.Clear();
            _floatDict?.Clear();
            _doubleDict?.Clear();
            _bytesDict = null;
            _dictEntries?.Clear();
            _dictPageSize = 0;
            DictionaryCount = 0;
            Indices?.Reset();
            DefLevels?.Reset();
            BooleanValues?.Reset();
            RowCount = 0;
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

        public int Get(int index) => _data[index];

        public int[] ToArray(int count)
        {
            var result = new int[count];
            System.Array.Copy(_data, result, count);
            return result;
        }

        public void Reset() => _count = 0;
    }

    /// <summary>
    /// Open-addressing hash table for byte sequences (same as DictionaryEncoder.BytesHashTable).
    /// </summary>
    private sealed class BytesDictionary
    {
        private int[] _hashes;
        private byte[]?[] _keys;
        private int[] _values;
        private int _mask;
        private int _count;

        public BytesDictionary(int capacity = 256)
        {
            int size = 1;
            while (size < capacity) size <<= 1;
            _hashes = new int[size];
            _keys = new byte[]?[size];
            _values = new int[size];
            _mask = size - 1;
        }

        public int GetOrAdd(ReadOnlySpan<byte> key, int nextIndex)
        {
            // Grow if >50% full
            if (_count > _mask / 2)
                Grow();

            uint h = Fnv1a(key);
            int hash = (int)(h | 1);
            int slot = hash & _mask;

            while (true)
            {
                if (_hashes[slot] == 0)
                {
                    _hashes[slot] = hash;
                    _keys[slot] = key.ToArray();
                    _values[slot] = nextIndex;
                    _count++;
                    return nextIndex;
                }

                if (_hashes[slot] == hash && key.SequenceEqual(_keys[slot]))
                    return _values[slot];

                slot = (slot + 1) & _mask;
            }
        }

        private void Grow()
        {
            int newSize = (_mask + 1) * 2;
            var newHashes = new int[newSize];
            var newKeys = new byte[]?[newSize];
            var newValues = new int[newSize];
            int newMask = newSize - 1;

            for (int i = 0; i <= _mask; i++)
            {
                if (_hashes[i] == 0) continue;
                int slot = _hashes[i] & newMask;
                while (newHashes[slot] != 0)
                    slot = (slot + 1) & newMask;
                newHashes[slot] = _hashes[i];
                newKeys[slot] = _keys[i];
                newValues[slot] = _values[i];
            }

            _hashes = newHashes;
            _keys = newKeys;
            _values = newValues;
            _mask = newMask;
        }

        private static uint Fnv1a(ReadOnlySpan<byte> data)
        {
            uint hash = 2166136261u;
            for (int i = 0; i < data.Length; i++)
            {
                hash ^= data[i];
                hash *= 16777619u;
            }
            return hash;
        }
    }
}
