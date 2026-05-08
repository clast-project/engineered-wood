// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet;

/// <summary>
/// Writes Arrow <see cref="RecordBatch"/> data to Parquet files.
/// </summary>
public sealed class ParquetFileWriter : IAsyncDisposable, IDisposable
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
    }

    /// <summary>
    /// Writes a row group from the given <see cref="RecordBatch"/>.
    /// The schema is inferred from the first batch; subsequent batches must have the same schema.
    /// If the batch exceeds <see cref="ParquetWriteOptions.RowGroupMaxRows"/>, it is automatically
    /// split into multiple row groups.
    /// </summary>
    public async ValueTask WriteRowGroupAsync(
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

        // First call: write header and convert schema
        if (!_headerWritten)
        {
            _arrowSchema = batch.Schema;
            _parquetSchema = ArrowToSchemaConverter.Convert(_arrowSchema);
            await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);
            _headerWritten = true;
        }

        // Auto-split large batches into multiple row groups
        int maxRows = _options.RowGroupMaxRows;
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

    private async ValueTask WriteSingleRowGroupAsync(
        RecordBatch batch,
        CancellationToken cancellationToken)
    {
        // Decompose all Arrow columns into leaf columns (flat columns produce 1 leaf each,
        // nested columns produce multiple leaves)
        int arrowColumnCount = batch.ColumnCount;
        var allLeafResults = new List<ColumnChunkWriter.ColumnChunkResult>();

        // Collect leaf column tasks per Arrow column
        var perColumnLeaves = new List<ColumnChunkWriter.ColumnChunkResult>[arrowColumnCount];

        Parallel.For(0, arrowColumnCount, i =>
        {
            var field = _arrowSchema!.FieldsList[i];
            var array = batch.Column(i);

            if (IsNestedType(field.DataType))
            {
                // Nested: decompose into leaf columns with def/rep levels
                var leaves = NestedLevelWriter.Decompose(array, field, batch.Length);
                var results = new List<ColumnChunkWriter.ColumnChunkResult>(leaves.Count);
                foreach (var leaf in leaves)
                {
                    results.Add(ColumnChunkWriter.WriteColumn(
                        leaf.Array,
                        leaf.PathInSchema,
                        leaf.PhysicalType,
                        leaf.TypeLength,
                        leaf.MaxDefLevel,
                        leaf.MaxRepLevel,
                        leaf.DefLevels,
                        leaf.RepLevels,
                        leaf.NonNullCount,
                        leaf.LevelCount,
                        _options));
                }
                perColumnLeaves[i] = results;
            }
            else
            {
                // Flat: resolve physical type from schema
                var element = FindLeafElement(_parquetSchema!, field.Name);
                perColumnLeaves[i] =
                [
                    ColumnChunkWriter.WriteColumn(
                        array,
                        new[] { field.Name },
                        element.Type!.Value,
                        element.TypeLength ?? 0,
                        field.IsNullable,
                        _options)
                ];
            }
        });

        // Flatten results in schema order
        foreach (var results in perColumnLeaves)
            allLeafResults.AddRange(results);

        // Write column data sequentially (to maintain file offsets)
        var columnChunks = new ColumnChunk[allLeafResults.Count];
        long totalByteSize = 0;
        long totalCompressedSize = 0;

        for (int i = 0; i < allLeafResults.Count; i++)
        {
            var result = allLeafResults[i];
            long chunkStart = _file.Position;

            await _file.WriteAsync(result.Data, cancellationToken).ConfigureAwait(false);

            // Calculate offsets: dictionary page comes first if present
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

            // Update metadata with actual file offset
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

            columnChunks[i] = new ColumnChunk
            {
                FileOffset = chunkStart,
                MetaData = meta,
            };

            totalByteSize += result.MetaData.TotalUncompressedSize;
            totalCompressedSize += result.MetaData.TotalCompressedSize;
        }

        _rowGroups.Add(new RowGroup
        {
            Columns = columnChunks,
            TotalByteSize = totalByteSize,
            NumRows = batch.Length,
            TotalCompressedSize = totalCompressedSize,
            Ordinal = checked((short)_rowGroups.Count),
        });
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
        int nullCount = data.NullCount;
        int srcOffset = data.Offset;

        ArrowBuffer newBitmap;
        if (data.Buffers.Length > 0 && data.Buffers[0].Length > 0 && nullCount > 0)
        {
            // Copy null bitmap
            var bitmapBytes = new byte[(length + 7) / 8];
            var srcBitmap = data.Buffers[0].Span;
            for (int i = 0; i < length; i++)
            {
                bool isSet = (srcBitmap[(srcOffset + i) / 8] & (1 << ((srcOffset + i) % 8))) != 0;
                if (isSet)
                    bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
            }
            newBitmap = new ArrowBuffer(bitmapBytes);
        }
        else if (nullCount == 0)
        {
            newBitmap = ArrowBuffer.Empty;
        }
        else
        {
            newBitmap = data.Buffers.Length > 0 ? data.Buffers[0] : ArrowBuffer.Empty;
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
    /// Finalizes the file by writing the footer and closing magic.
    /// Must be called before disposing.
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

        // If no row groups were written, still write a valid empty file
        if (!_headerWritten)
        {
            await _file.WriteAsync(Par1Magic, cancellationToken).ConfigureAwait(false);
            _headerWritten = true;
            _parquetSchema ??= [new SchemaElement { Name = "schema", NumChildren = 0 }];
        }

        // Calculate total rows
        long totalRows = 0;
        foreach (var rg in _rowGroups)
            totalRows += rg.NumRows;

        // Build file metadata
        var fileMetaData = new FileMetaData
        {
            Version = 2,
            Schema = _parquetSchema!,
            NumRows = totalRows,
            RowGroups = _rowGroups,
            CreatedBy = _options.CreatedBy,
            KeyValueMetadata = _options.KeyValueMetadata,
        };

        // Encode footer to Thrift
        byte[] footerBytes = MetadataEncoder.EncodeFileMetaData(fileMetaData, writePathInSchema: !_options.OmitPathInSchema);

        // Write footer
        await _file.WriteAsync(footerBytes, cancellationToken).ConfigureAwait(false);

        // Write footer length (4 bytes LE)
        var footerLengthBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(footerLengthBytes, footerBytes.Length);
        await _file.WriteAsync(footerLengthBytes, cancellationToken).ConfigureAwait(false);

        // Write trailing PAR1 magic
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

        if (_ownsFile)
            _file.Dispose();
    }

    private static bool IsNestedType(Apache.Arrow.Types.IArrowType type) =>
        type is Apache.Arrow.Types.StructType
            or Apache.Arrow.Types.ListType
            or Apache.Arrow.Types.MapType
            // VariantType / any extension whose storage is itself nested.
            or Apache.Arrow.ExtensionType { StorageType: Apache.Arrow.Types.StructType }
            or Apache.Arrow.ExtensionType { StorageType: Apache.Arrow.Types.ListType }
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
