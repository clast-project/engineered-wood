// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
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
    private static readonly byte[] Par1Magic = "PAR1"u8.ToArray();

    private readonly ISequentialFile _file;
    private readonly bool _ownsFile;
    private readonly ParquetWriteOptions _options;
    private readonly List<RowGroup> _rowGroups = new();
    private IReadOnlyList<SchemaElement>? _parquetSchema;
    private Apache.Arrow.Schema? _arrowSchema;
    private Dictionary<string, ShredSchema?>? _variantShredDecisions;
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

        // A SLICED column (Arrow's zero-copy sub-range view: Data.Offset != 0) must be compacted before
        // anything reads its buffers. The encoders below index the raw value buffer from slot 0, so a view
        // starting at row N silently writes rows 0..len instead of N..N+len — and the def levels, which come
        // from the offset-aware IsNull, stay correct, so the file is well-formed and merely holds the wrong
        // rows. Only paid when a column actually carries an offset.
        batch = CompactSlicedColumns(batch);

        // Variant shredding, if enabled, changes each shredded column's storage TYPE — so the layout
        // has to be decided before the schema is captured, and from the same batch. Decided once and
        // reused: a parquet file has one schema, and a later batch that re-inferred a different shape
        // would be encoded against this one. Applied per row group in WriteSingleRowGroupAsync, after
        // the split below, so a shredded array never goes through MaterializeSlice.
        EnsureVariantShredDecisions(batch);

        // First call: write header. The schema is captured in WriteSingleRowGroupAsync instead, once
        // shredding has been applied, so that what is written and what the footer declares agree.
        if (!_headerWritten)
        {
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
        batch = ApplyVariantShredding(batch);

        // Captured from the first row group AFTER shredding, so the declared schema is the one the
        // column writers below actually encode.
        if (_arrowSchema is null)
        {
            _arrowSchema = batch.Schema;
            _parquetSchema = ArrowToSchemaConverter.Convert(_arrowSchema);
        }

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

            // Calculate offsets: a dictionary page or an FSST symbol table page comes first if
            // present. A chunk has at most one of the two — FSST is a non-dictionary encoding.
            long dataPageOffset = chunkStart + result.DictionaryPageSize + result.SymbolTablePageSize;
            long? dictionaryPageOffset = result.DictionaryPageSize > 0 ? chunkStart : null;
            long? symbolTablePageOffset = result.SymbolTablePageSize > 0 ? chunkStart : null;

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
                SymbolTablePageOffset = symbolTablePageOffset,
                SymbolTablePageLength = result.MetaData.SymbolTablePageLength,
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
    /// Materializes any column that is an offset-based VIEW onto a larger array, so every column's row 0 is
    /// its buffer's slot 0 — the layout the whole write path assumes.
    ///
    /// <para>Returns the batch unchanged when no column carries an offset, which is the common case: arrays
    /// built by a builder, read back by the parquet reader, or gathered by <c>ArrowCompute.Take</c> all start
    /// at 0. Only a caller that sliced its own data pays for the copy.</para>
    ///
    /// <para><c>Take</c> with the identity selection is the compaction: it applies the source's offset in
    /// every one of its per-type gathers, so the result is a genuine copy of the rows the view designates.
    /// A type it cannot gather raises <see cref="NotSupportedException"/> from there rather than being
    /// written wrong.</para>
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

        var identity = new int[batch.Length];
        for (int i = 0; i < identity.Length; i++)
            identity[i] = i;

        var columns = new IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var column = batch.Column(i);
            columns[i] = column.Data.Offset == 0
                ? column
                : EngineeredWood.Arrow.ArrowCompute.Take(column, identity);
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
        }

        // The header is written before the first row group but the schema is captured during it, so a
        // row group that throws leaves this null. Falling back here keeps dispose from replacing that
        // exception with an NRE out of the footer.
        _parquetSchema ??= [new SchemaElement { Name = "schema", NumChildren = 0 }];

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
            ColumnOrders = ColumnOrderBuilder.Build(_parquetSchema!, _options.FloatColumnOrder),
        };

        // Encode footer to Thrift
#pragma warning disable EWPARQUET0002 // Honoring the caller's opt-in; the experimental signal lives on the option itself.
        byte[] footerBytes = MetadataEncoder.EncodeFileMetaData(fileMetaData, writePathInSchema: !_options.OmitPathInSchema);
#pragma warning restore EWPARQUET0002

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
