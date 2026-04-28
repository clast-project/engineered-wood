// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Lance.Encodings;
using EngineeredWood.Lance.Encodings.V20;
using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Proto;
using EngineeredWood.Lance.Proto.V2;
using EngineeredWood.Lance.Schema;
using Google.Protobuf;

namespace EngineeredWood.Lance;

/// <summary>
/// File-level Lance reader. Parses the 40-byte footer, the column and global
/// buffer offset tables, the <see cref="FileDescriptor"/> (schema + row count),
/// and per-column <see cref="ColumnMetadata"/> protobufs.
///
/// <para>Phase 1 scope: envelope only. This reader exposes the schema, row
/// count, and version; it does not yet decode page data.</para>
///
/// <para>Supported format versions: v2.0 and v2.1. Legacy v0.1 files and v2.2+
/// are rejected with a clear <see cref="LanceFormatException"/>.</para>
/// </summary>
public sealed class LanceFileReader : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Default tail-read size used to fetch the footer together with as much
    /// of the metadata region as possible in one I/O. Matches the Lance Rust
    /// reader's cloud default (64 KiB).
    /// </summary>
    internal const int DefaultTailReadSize = 64 * 1024;

    private readonly IRandomAccessFile _reader;
    private readonly bool _ownsReader;
    private readonly ColumnMetadata[] _columnMetadatas;
    private readonly FieldColumnRange[] _fieldColumnRanges;

    /// <summary>The file format version reported in the footer.</summary>
    public LanceVersion Version { get; }

    /// <summary>Total row count as reported by global buffer 0's FileDescriptor.</summary>
    public long NumberOfRows { get; }

    /// <summary>Number of columns as reported by the footer.</summary>
    public int NumberOfColumns { get; }

    /// <summary>Arrow schema derived from the Lance FileDescriptor.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>Schema-level metadata (keys/values as they appear in the proto).</summary>
    public IReadOnlyDictionary<string, byte[]> Metadata { get; }

    /// <summary>Per-column metadata blobs, in column order. Exposed for later phases.</summary>
    internal IReadOnlyList<ColumnMetadata> ColumnMetadatas => _columnMetadatas;

    /// <summary>The raw Lance proto schema. Useful for diagnostics and tests.</summary>
    internal Proto.Schema ProtoSchema { get; }

    private LanceFileReader(
        IRandomAccessFile reader,
        bool ownsReader,
        LanceVersion version,
        long numberOfRows,
        int numberOfColumns,
        Apache.Arrow.Schema schema,
        Proto.Schema protoSchema,
        IReadOnlyDictionary<string, byte[]> metadata,
        ColumnMetadata[] columnMetadatas,
        FieldColumnRange[] fieldColumnRanges)
    {
        _reader = reader;
        _ownsReader = ownsReader;
        Version = version;
        NumberOfRows = numberOfRows;
        NumberOfColumns = numberOfColumns;
        Schema = schema;
        ProtoSchema = protoSchema;
        Metadata = metadata;
        _columnMetadatas = columnMetadatas;
        _fieldColumnRanges = fieldColumnRanges;
    }

    public static async Task<LanceFileReader> OpenAsync(
        string path, CancellationToken cancellationToken = default)
    {
        var reader = new LocalRandomAccessFile(path);
        try
        {
            return await OpenAsync(reader, ownsReader: true, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    public static async Task<LanceFileReader> OpenAsync(
        IRandomAccessFile reader,
        bool ownsReader = false,
        CancellationToken cancellationToken = default)
    {
        long fileLength = await reader.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        if (fileLength < LanceFooter.Size)
            throw new LanceFormatException(
                $"File is too small to be a Lance file: {fileLength} bytes (minimum {LanceFooter.Size}).");

        int tailSize = (int)Math.Min(fileLength, DefaultTailReadSize);
        long tailOffset = fileLength - tailSize;

        using IMemoryOwner<byte> tailOwner = await reader
            .ReadAsync(new FileRange(tailOffset, tailSize), cancellationToken)
            .ConfigureAwait(false);
        ReadOnlyMemory<byte> tail = tailOwner.Memory.Slice(0, tailSize);

        // Parse the footer out of the tail. The footer is always the last 40 bytes.
        LanceFooter footer = LanceFooter.Parse(tail.Span.Slice(tailSize - LanceFooter.Size));

        // Reject legacy v0.1 explicitly — the on-disk format is entirely different.
        if (footer.Version == LanceVersion.Legacy_V0_1)
            throw new LanceFormatException(
                "This is a legacy Lance v0.1 file. Only v2.0 and v2.1 are supported.");

        if (!footer.Version.IsSupported)
            throw new LanceFormatException(
                $"Unsupported Lance file format version {footer.Version}. " +
                "Only v2.0 and v2.1 are supported.");

        ValidateFooterBounds(footer, fileLength);

        // Load the GBO table (need global buffer 0 for the FileDescriptor).
        if (footer.NumGlobalBuffers < 1)
            throw new LanceFormatException(
                "File has no global buffers; global buffer 0 (FileDescriptor) is required.");

        OffsetSizeEntry[] gboEntries = await LoadOffsetTableAsync(
            reader, tail, tailOffset,
            footer.GboTableOffset, footer.NumGlobalBuffers,
            label: "global buffer offset table",
            cancellationToken).ConfigureAwait(false);

        // Load global buffer 0 → FileDescriptor.
        OffsetSizeEntry gb0 = gboEntries[0];
        if (gb0.Size == 0)
            throw new LanceFormatException(
                "Global buffer 0 (FileDescriptor) has zero size.");
        if (gb0.End > fileLength)
            throw new LanceFormatException(
                $"Global buffer 0 extends past end of file (end={gb0.End}, file={fileLength}).");

        byte[] fileDescriptorBytes = await ReadFromTailOrFileAsync(
            reader, tail, tailOffset, gb0.Position, gb0.Size, cancellationToken)
            .ConfigureAwait(false);

        FileDescriptor fileDescriptor;
        try
        {
            fileDescriptor = FileDescriptor.Parser.ParseFrom(fileDescriptorBytes);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new LanceFormatException(
                "Failed to parse global buffer 0 as a FileDescriptor.", ex);
        }

        if (fileDescriptor.Schema is null)
            throw new LanceFormatException(
                "FileDescriptor has no schema.");

        Apache.Arrow.Schema arrowSchema = LanceSchemaConverter.ToArrowSchema(fileDescriptor.Schema);

        var metadata = new Dictionary<string, byte[]>(fileDescriptor.Schema.Metadata.Count);
        foreach (var kv in fileDescriptor.Schema.Metadata)
            metadata[kv.Key] = kv.Value.ToByteArray();

        // Load the CMO table and per-column metadatas.
        ColumnMetadata[] columnMetadatas = await LoadColumnMetadatasAsync(
            reader, tail, tailOffset, footer, cancellationToken).ConfigureAwait(false);

        long numberOfRows = checked((long)fileDescriptor.Length);

        FieldColumnRange[] fieldColumnRanges;
        int expectedColumns;
        if (footer.Version.IsV2_1)
        {
            // v2.1 puts nested structure inside per-column rep/def buffers, so
            // a struct field does not claim an extra "parent column" like it
            // does in v2.0. Phase 6 handled flat primitives; Phase 7 adds
            // single-column lists (list<primitive>) and strings/binary. Structs
            // and list-of-struct still defer — they need multi-column shared
            // rep/def logic.
            foreach (var field in arrowSchema.FieldsList)
            {
                if (field.DataType is StructType st)
                {
                    foreach (var child in st.Fields)
                        if (child.DataType is not FixedWidthType)
                            throw new NotImplementedException(
                                $"v2.1 struct child '{child.Name}' has non-primitive type {child.DataType}; only fixed-width primitives are supported in this slice.");
                }
                if (field.DataType is LargeListType)
                    throw new NotImplementedException(
                        "Reading LargeListType from v2.1 files is not yet supported.");
                if (field.DataType is FixedSizeListType fslType
                    && fslType.ValueDataType is not FixedWidthType)
                    throw new NotImplementedException(
                        "FixedSizeListType with non-primitive items is not yet supported for v2.1.");
                if (field.DataType is Apache.Arrow.Types.ListType listType
                    && listType.ValueDataType is (StructType or Apache.Arrow.Types.ListType
                        or LargeListType or FixedSizeListType))
                {
                    throw new NotImplementedException(
                        "Reading list-of-nested types from v2.1 files is not yet supported.");
                }
            }
            // v2.1 column count = number of leaf physical columns.
            // For a struct of N primitive children, that's N columns; for
            // any other primitive top-level field, 1 column.
            fieldColumnRanges = new FieldColumnRange[arrowSchema.FieldsList.Count];
            int columnCursor = 0;
            for (int i = 0; i < arrowSchema.FieldsList.Count; i++)
            {
                int leaves = arrowSchema.FieldsList[i].DataType is StructType st
                    ? st.Fields.Count
                    : 1;
                fieldColumnRanges[i] = new FieldColumnRange(columnCursor, leaves);
                columnCursor += leaves;
            }
            expectedColumns = columnCursor;
        }
        else
        {
            fieldColumnRanges = FieldColumnRange.BuildFromSchema(fileDescriptor.Schema);
            expectedColumns = 0;
            foreach (var r in fieldColumnRanges) expectedColumns += r.ColumnCount;
        }

        if (expectedColumns != footer.NumColumns)
            throw new LanceFormatException(
                $"Schema declares {expectedColumns} physical columns but footer has {footer.NumColumns}.");

        return new LanceFileReader(
            reader, ownsReader,
            footer.Version,
            numberOfRows,
            footer.NumColumns,
            arrowSchema,
            fileDescriptor.Schema,
            metadata,
            columnMetadatas,
            fieldColumnRanges);
    }

    private static void ValidateFooterBounds(LanceFooter footer, long fileLength)
    {
        long footerStart = fileLength - LanceFooter.Size;

        void Check(long offset, string label)
        {
            if (offset < 0 || offset > footerStart)
                throw new LanceFormatException(
                    $"{label} offset {offset} is outside the file body " +
                    $"[0, {footerStart}).");
        }

        Check(footer.ColumnMetaStart, "Column metadata start");
        Check(footer.CmoTableOffset, "CMO table");
        Check(footer.GboTableOffset, "GBO table");

        if (footer.NumColumns > 0 && footer.ColumnMetaStart > footer.CmoTableOffset)
            throw new LanceFormatException(
                $"Column metadata region ({footer.ColumnMetaStart}) must not start " +
                $"after the CMO table ({footer.CmoTableOffset}).");
        if (footer.CmoTableOffset > footer.GboTableOffset)
            throw new LanceFormatException(
                $"CMO table ({footer.CmoTableOffset}) must precede the GBO table " +
                $"({footer.GboTableOffset}).");
    }

    private static async Task<OffsetSizeEntry[]> LoadOffsetTableAsync(
        IRandomAccessFile reader,
        ReadOnlyMemory<byte> tail,
        long tailOffset,
        long tableOffset,
        int entryCount,
        string label,
        CancellationToken cancellationToken)
    {
        int tableBytes = checked(entryCount * OffsetSizeEntry.Bytes);
        byte[] buf = await ReadFromTailOrFileAsync(
            reader, tail, tailOffset, tableOffset, tableBytes, cancellationToken)
            .ConfigureAwait(false);
        try
        {
            return OffsetSizeEntry.ParseTable(buf, entryCount);
        }
        catch (LanceFormatException ex)
        {
            throw new LanceFormatException(
                $"Failed to parse {label}: {ex.Message}", ex);
        }
    }

    private static async Task<ColumnMetadata[]> LoadColumnMetadatasAsync(
        IRandomAccessFile reader,
        ReadOnlyMemory<byte> tail,
        long tailOffset,
        LanceFooter footer,
        CancellationToken cancellationToken)
    {
        if (footer.NumColumns == 0)
            return System.Array.Empty<ColumnMetadata>();

        OffsetSizeEntry[] cmoEntries = await LoadOffsetTableAsync(
            reader, tail, tailOffset,
            footer.CmoTableOffset, footer.NumColumns,
            label: "column metadata offset table",
            cancellationToken).ConfigureAwait(false);

        var result = new ColumnMetadata[footer.NumColumns];
        for (int i = 0; i < footer.NumColumns; i++)
        {
            OffsetSizeEntry entry = cmoEntries[i];
            byte[] bytes = await ReadFromTailOrFileAsync(
                reader, tail, tailOffset, entry.Position, entry.Size, cancellationToken)
                .ConfigureAwait(false);
            try
            {
                result[i] = ColumnMetadata.Parser.ParseFrom(bytes);
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new LanceFormatException(
                    $"Failed to parse ColumnMetadata for column {i}.", ex);
            }
        }
        return result;
    }

    /// <summary>
    /// Returns <paramref name="length"/> bytes starting at <paramref name="offset"/>.
    /// If the range lies entirely inside <paramref name="tail"/> (the optimistic
    /// tail read), it is served from memory without a fresh I/O.
    /// </summary>
    private static async Task<byte[]> ReadFromTailOrFileAsync(
        IRandomAccessFile reader,
        ReadOnlyMemory<byte> tail,
        long tailOffset,
        long offset,
        long length,
        CancellationToken cancellationToken)
    {
        if (length == 0)
            return System.Array.Empty<byte>();

        if (offset >= tailOffset && offset + length <= tailOffset + tail.Length)
        {
            int start = checked((int)(offset - tailOffset));
            int len = checked((int)length);
            return tail.Span.Slice(start, len).ToArray();
        }

        using IMemoryOwner<byte> owner = await reader
            .ReadAsync(new FileRange(offset, length), cancellationToken)
            .ConfigureAwait(false);
        return owner.Memory.Slice(0, checked((int)length)).ToArray();
    }

    /// <summary>
    /// Read the full contents of a top-level Arrow field as a single
    /// <see cref="IArrowArray"/>. For nested fields (structs, lists), this
    /// reads every physical column that makes up the field and assembles a
    /// single nested Arrow array.
    ///
    /// <para>The index is an index into <see cref="Schema"/>'s
    /// <c>FieldsList</c>, not a physical column index — for flat schemas
    /// these are identical, but a struct top-level field typically spans
    /// several physical columns.</para>
    ///
    /// <para>Supported v2.0 encodings as of Phase 5: primitive (Flat),
    /// Nullable (No/Some/All), Binary, Constant, FixedSizeBinary, Bitpacked,
    /// Dictionary, FixedSizeList, SimpleStruct (with <b>no struct-level
    /// nulls</b>, per the v2.0 proto limitation), and List / LargeList.</para>
    /// </summary>
    public async Task<IArrowArray> ReadColumnAsync(
        int fieldIndex, CancellationToken cancellationToken = default)
    {
        if (fieldIndex < 0 || fieldIndex >= Schema.FieldsList.Count)
            throw new ArgumentOutOfRangeException(
                nameof(fieldIndex),
                $"Field index {fieldIndex} is out of range [0, {Schema.FieldsList.Count}).");

        var range = _fieldColumnRanges[fieldIndex];
        var arrowField = Schema.FieldsList[fieldIndex];

        if (Version.IsV2_1)
        {
            // Phase 7b: structs of primitives are now supported via
            // ReadV21StructAsync. Other top-level fields (including primitive
            // leaves and lists) still go through the single-column path.
            if (arrowField.DataType is StructType structType)
                return await ReadV21StructAsync(structType, range, cancellationToken)
                    .ConfigureAwait(false);
            return await ReadV21SingleColumnAsync(
                range.StartColumn, arrowField.DataType, cancellationToken).ConfigureAwait(false);
        }

        // v2.0 path (including nested types handled via multi-column reads).
        var (array, consumed) = await ReadArrowFieldAsync(
            arrowField, range.StartColumn, cancellationToken).ConfigureAwait(false);

        if (consumed != range.ColumnCount)
            throw new LanceFormatException(
                $"Field {fieldIndex} ({arrowField.Name}) declared {range.ColumnCount} " +
                $"physical columns but the decoder consumed {consumed}.");
        return array;
    }

    private async Task<IArrowArray> ReadV21StructAsync(
        StructType structType, FieldColumnRange range, CancellationToken cancellationToken)
    {
        if (range.ColumnCount != structType.Fields.Count)
            throw new LanceFormatException(
                $"v2.1 struct field declared {range.ColumnCount} physical columns " +
                $"but Arrow type has {structType.Fields.Count} children.");

        // Read every child column, capturing both its Arrow array and the
        // raw def levels. The outer layer's def (struct-level nullability)
        // must be identical across siblings; we use the first child as the
        // canonical def buffer.
        var childArrays = new IArrowArray[structType.Fields.Count];
        ushort[]? canonicalDef = null;
        int length = -1;
        for (int i = 0; i < structType.Fields.Count; i++)
        {
            var child = structType.Fields[i];
            var (arr, def) = await ReadV21StructChildAsync(
                range.StartColumn + i, child.DataType, cancellationToken).ConfigureAwait(false);
            childArrays[i] = arr;
            if (length < 0) length = arr.Length;
            else if (arr.Length != length)
                throw new LanceFormatException(
                    $"Struct child '{child.Name}' has length {arr.Length} but sibling has {length}.");

            if (i == 0)
            {
                canonicalDef = def;
            }
            else if (def is null != canonicalDef is null)
            {
                throw new LanceFormatException(
                    $"Struct child '{child.Name}' def-buffer presence disagrees with sibling.");
            }
            else if (def is not null && canonicalDef is not null)
            {
                for (int k = 0; k < def.Length; k++)
                {
                    if (def[k] != canonicalDef[k])
                        throw new LanceFormatException(
                            $"Struct child '{child.Name}' def[{k}]={def[k]} disagrees with sibling def[{k}]={canonicalDef[k]} (cross-column rep/def coherence violated).");
                }
            }
        }

        // Compute outer (struct-level) validity bitmap. With 2 NULLABLE_ITEM
        // layers, def == 2 means the struct itself is null. With ALL_VALID
        // layers there is no def — struct is fully valid.
        ArrowBuffer validity = ArrowBuffer.Empty;
        int nullCount = 0;
        if (canonicalDef is not null)
        {
            const ushort StructNullDef = 2;
            int bitmapBytes = (length + 7) / 8;
            var bitmap = new byte[bitmapBytes];
            for (int k = 0; k < length; k++)
            {
                if (canonicalDef[k] == StructNullDef)
                    nullCount++;
                else
                    bitmap[k >> 3] |= (byte)(1 << (k & 7));
            }
            if (nullCount > 0)
                validity = new ArrowBuffer(bitmap);
        }

        var data = new ArrayData(
            structType, length, nullCount, offset: 0,
            new[] { validity },
            children: childArrays.Select(a => a.Data).ToArray());
        return new StructArray(data);
    }

    private async Task<(IArrowArray Array, ushort[]? DefLevels)> ReadV21StructChildAsync(
        int columnIndex, IArrowType childType, CancellationToken cancellationToken)
    {
        ColumnMetadata cm = _columnMetadatas[columnIndex];
        if (cm.Pages.Count == 0)
            return (BuildEmptyArray(childType), null);
        if (cm.Pages.Count > 1)
            throw new NotImplementedException(
                $"Multi-page struct-child reads are not yet supported (column {columnIndex}).");

        ColumnMetadata.Types.Page page = cm.Pages[0];
        var bufferOwners = await LoadPageBuffersAsync(page, cancellationToken).ConfigureAwait(false);
        try
        {
            var pageBuffers = new ReadOnlyMemory<byte>[bufferOwners.Count];
            for (int i = 0; i < bufferOwners.Count; i++)
                pageBuffers[i] = bufferOwners[i].Memory;
            var pageContext = new PageContext(pageBuffers);
            var pageLayout = EncodingUnpacker.UnpackPageLayout(page.Encoding);
            if (pageLayout.LayoutCase != Proto.Encodings.V21.PageLayout.LayoutOneofCase.MiniBlockLayout)
                throw new NotImplementedException(
                    $"Struct child column {columnIndex} uses {pageLayout.LayoutCase}; only MiniBlockLayout is supported.");
            return Encodings.V21.MiniBlockLayoutDecoder.DecodeForStructChild(
                pageLayout.MiniBlockLayout, childType, pageContext);
        }
        finally
        {
            foreach (var owner in bufferOwners) owner.Dispose();
        }
    }

    private async Task<IArrowArray> ReadV21SingleColumnAsync(
        int columnIndex, IArrowType targetType, CancellationToken cancellationToken)
    {
        ColumnMetadata cm = _columnMetadatas[columnIndex];
        if (cm.Pages.Count == 0)
            return BuildEmptyArray(targetType);
        if (cm.Pages.Count > 1)
            throw new NotImplementedException(
                $"Column {columnIndex} has {cm.Pages.Count} pages. Multi-page column reading is planned for Phase 10.");

        ColumnMetadata.Types.Page page = cm.Pages[0];
        long numRows = checked((long)page.Length);

        IReadOnlyList<IMemoryOwner<byte>> pageBufferOwners =
            await LoadPageBuffersAsync(page, cancellationToken).ConfigureAwait(false);
        try
        {
            var pageBuffers = new ReadOnlyMemory<byte>[pageBufferOwners.Count];
            for (int i = 0; i < pageBufferOwners.Count; i++)
                pageBuffers[i] = pageBufferOwners[i].Memory;

            var pageContext = new PageContext(pageBuffers);
            var pageLayout = EncodingUnpacker.UnpackPageLayout(page.Encoding);
            return Encodings.V21.PageLayoutDispatcher.Decode(
                pageLayout, numRows, targetType, pageContext);
        }
        finally
        {
            foreach (var owner in pageBufferOwners)
                owner.Dispose();
        }
    }

    private async Task<(IArrowArray Array, int Consumed)> ReadArrowFieldAsync(
        Apache.Arrow.Field field, int startColumn, CancellationToken cancellationToken)
    {
        switch (field.DataType)
        {
            case StructType st:
                return await ReadStructFieldAsync(st, startColumn, cancellationToken)
                    .ConfigureAwait(false);
            case Apache.Arrow.Types.ListType lt:
                return await ReadListFieldAsync(lt.ValueField, lt, startColumn,
                    arrowOffsetWidth: 4, cancellationToken).ConfigureAwait(false);
            case LargeListType llt:
                return await ReadListFieldAsync(llt.ValueField, llt, startColumn,
                    arrowOffsetWidth: 8, cancellationToken).ConfigureAwait(false);
            default:
                var arr = await ReadSingleColumnAsync(startColumn, field.DataType, cancellationToken)
                    .ConfigureAwait(false);
                return (arr, 1);
        }
    }

    /// <summary>
    /// Reads a v2.0 <c>SimpleStruct</c> parent column for its row count and
    /// recursively reads each child field. v2.0 cannot carry struct-level
    /// nullability (SimpleStruct has no validity bitmap), so the resulting
    /// StructArray has nullCount = 0.
    /// </summary>
    private async Task<(IArrowArray Array, int Consumed)> ReadStructFieldAsync(
        StructType structType, int startColumn, CancellationToken cancellationToken)
    {
        ColumnMetadata parentMeta = _columnMetadatas[startColumn];
        int parentLength = GetParentRowCount(parentMeta, startColumn,
            requireEncoding: Proto.Encodings.V20.ArrayEncoding.ArrayEncodingOneofCase.Struct);

        int consumed = 1;
        var childArrays = new IArrowArray[structType.Fields.Count];
        for (int i = 0; i < structType.Fields.Count; i++)
        {
            var (childArr, childCount) = await ReadArrowFieldAsync(
                structType.Fields[i], startColumn + consumed, cancellationToken)
                .ConfigureAwait(false);
            if (childArr.Length != parentLength)
                throw new LanceFormatException(
                    $"Struct child '{structType.Fields[i].Name}' has length {childArr.Length} " +
                    $"but parent struct has length {parentLength}.");
            childArrays[i] = childArr;
            consumed += childCount;
        }

        var structData = new ArrayData(
            structType,
            length: parentLength,
            nullCount: 0,
            offset: 0,
            buffers: new[] { ArrowBuffer.Empty },
            children: childArrays.Select(a => a.Data).ToArray());
        return (new StructArray(structData), consumed);
    }

    /// <summary>
    /// Reads a v2.0 <c>List</c> encoded column plus its child subtree.
    /// The list column's single buffer holds u64 offsets with the standard
    /// Lance null-offset-adjustment sentinel. The child subtree starts in
    /// the very next physical column.
    /// </summary>
    private async Task<(IArrowArray Array, int Consumed)> ReadListFieldAsync(
        Apache.Arrow.Field valueField,
        IArrowType listLikeType,
        int startColumn,
        int arrowOffsetWidth,
        CancellationToken cancellationToken)
    {
        var (arrowOffsets, bitmap, nullCount, numListRows) =
            await ReadListColumnOffsetsAsync(
                startColumn, arrowOffsetWidth, cancellationToken).ConfigureAwait(false);

        var (childArr, childConsumed) = await ReadArrowFieldAsync(
            valueField, startColumn + 1, cancellationToken).ConfigureAwait(false);

        ArrowBuffer validityBuffer = bitmap is null
            ? ArrowBuffer.Empty
            : new ArrowBuffer(bitmap);

        var data = new ArrayData(
            listLikeType,
            length: numListRows,
            nullCount: nullCount,
            offset: 0,
            buffers: new[] { validityBuffer, new ArrowBuffer(arrowOffsets) },
            children: new[] { childArr.Data });

        IArrowArray arr = listLikeType switch
        {
            Apache.Arrow.Types.ListType => new ListArray(data),
            LargeListType => new LargeListArray(data),
            _ => throw new InvalidOperationException(
                $"Unexpected list-like type {listLikeType}."),
        };
        return (arr, 1 + childConsumed);
    }

    private async Task<(byte[] ArrowOffsets, byte[]? Bitmap, int NullCount, int NumListRows)>
        ReadListColumnOffsetsAsync(
            int columnIndex, int arrowOffsetWidth, CancellationToken cancellationToken)
    {
        ColumnMetadata cm = _columnMetadatas[columnIndex];
        if (cm.Pages.Count == 0)
            return (new byte[arrowOffsetWidth], null, 0, 0);
        if (cm.Pages.Count > 1)
            throw new NotImplementedException(
                $"List column {columnIndex} has {cm.Pages.Count} pages; multi-page list reads land later.");

        ColumnMetadata.Types.Page page = cm.Pages[0];
        int numListRows = checked((int)page.Length);

        var encoding = EncodingUnpacker.UnpackArrayEncoding(page.Encoding);
        if (encoding.ArrayEncodingCase !=
            Proto.Encodings.V20.ArrayEncoding.ArrayEncodingOneofCase.List)
            throw new LanceFormatException(
                $"List column {columnIndex} has unexpected encoding " +
                $"'{encoding.ArrayEncodingCase}', expected 'List'.");

        var list = encoding.List;
        ulong nullAdjustment = list.NullOffsetAdjustment;

        IReadOnlyList<IMemoryOwner<byte>> bufferOwners =
            await LoadPageBuffersAsync(page, cancellationToken).ConfigureAwait(false);
        try
        {
            var pageBuffers = new ReadOnlyMemory<byte>[bufferOwners.Count];
            for (int i = 0; i < bufferOwners.Count; i++)
                pageBuffers[i] = bufferOwners[i].Memory;

            var context = new PageContext(pageBuffers);
            ReadOnlyMemory<byte> offsetBytes = FlatDecoder.ResolveFlatBuffer(
                list.Offsets, context, out ulong bitsPerOffset);
            if (bitsPerOffset != 64)
                throw new NotImplementedException(
                    $"List offsets with bits_per_value={bitsPerOffset} are not supported (only 64).");

            int required = 8 * numListRows;
            if (offsetBytes.Length < required)
                throw new LanceFormatException(
                    $"List offsets buffer too small: need {required} bytes, have {offsetBytes.Length}.");

            return BuildArrowOffsetsFromListDisk(
                offsetBytes.Span, numListRows, nullAdjustment, arrowOffsetWidth);
        }
        finally
        {
            foreach (var owner in bufferOwners) owner.Dispose();
        }
    }

    private static (byte[] ArrowOffsets, byte[]? Bitmap, int NullCount, int NumListRows)
        BuildArrowOffsetsFromListDisk(
            ReadOnlySpan<byte> offsetBytes,
            int numListRows,
            ulong nullAdjustment,
            int arrowOffsetWidth)
    {
        // Lance stores cumulative end-offsets (no leading zero). A row is null
        // iff its disk offset is >= null_offset_adjustment; the "real" end
        // modulo null_adjustment gives the base for the next row.
        // Null lists contribute zero items to the child column.
        var arrowOffsets = new byte[arrowOffsetWidth * (numListRows + 1)];
        var bitmap = new byte[(numListRows + 7) / 8];
        int nullCount = 0;

        ulong diskBase = 0;
        ulong arrowCumulative = 0;
        WriteArrowOffset(arrowOffsets, 0, arrowOffsetWidth, 0);

        for (int i = 0; i < numListRows; i++)
        {
            ulong diskOffset = System.Buffers.Binary.BinaryPrimitives
                .ReadUInt64LittleEndian(offsetBytes.Slice(i * 8, 8));
            bool isNull = diskOffset >= nullAdjustment;
            ulong endModulo = isNull ? diskOffset - nullAdjustment : diskOffset;
            ulong len = endModulo - diskBase;

            if (isNull)
            {
                nullCount++;
            }
            else
            {
                arrowCumulative += len;
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }

            WriteArrowOffset(arrowOffsets, i + 1, arrowOffsetWidth, arrowCumulative);
            diskBase = endModulo;
        }

        return (arrowOffsets, nullCount == 0 ? null : bitmap, nullCount, numListRows);
    }

    private static void WriteArrowOffset(byte[] buffer, int index, int width, ulong value)
    {
        switch (width)
        {
            case 4:
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                    buffer.AsSpan(index * 4, 4), checked((int)value));
                break;
            case 8:
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(
                    buffer.AsSpan(index * 8, 8), checked((long)value));
                break;
            default:
                throw new InvalidOperationException($"Unsupported offset width {width}.");
        }
    }

    private int GetParentRowCount(
        ColumnMetadata cm, int columnIndex,
        Proto.Encodings.V20.ArrayEncoding.ArrayEncodingOneofCase requireEncoding)
    {
        if (cm.Pages.Count == 0) return 0;
        if (cm.Pages.Count > 1)
            throw new NotImplementedException(
                $"Column {columnIndex} has {cm.Pages.Count} pages; multi-page reads land later.");
        var page = cm.Pages[0];
        var encoding = EncodingUnpacker.UnpackArrayEncoding(page.Encoding);
        if (encoding.ArrayEncodingCase != requireEncoding)
            throw new LanceFormatException(
                $"Column {columnIndex} has unexpected encoding " +
                $"'{encoding.ArrayEncodingCase}', expected '{requireEncoding}'.");
        return checked((int)page.Length);
    }

    /// <summary>
    /// Reads one physical column via the standard <see cref="V20ArrayEncodingDispatcher"/>
    /// path. Used for leaves, FSB, FSL, and any other single-column encoding.
    /// </summary>
    private async Task<IArrowArray> ReadSingleColumnAsync(
        int columnIndex, IArrowType targetType, CancellationToken cancellationToken)
    {
        ColumnMetadata cm = _columnMetadatas[columnIndex];
        if (cm.Pages.Count == 0)
            return BuildEmptyArray(targetType);

        if (cm.Pages.Count > 1)
            throw new NotImplementedException(
                $"Column {columnIndex} has {cm.Pages.Count} pages. Multi-page column reading is planned for Phase 10.");

        ColumnMetadata.Types.Page page = cm.Pages[0];
        long numRows = checked((long)page.Length);

        IReadOnlyList<IMemoryOwner<byte>> pageBufferOwners =
            await LoadPageBuffersAsync(page, cancellationToken).ConfigureAwait(false);
        try
        {
            var pageBuffers = new ReadOnlyMemory<byte>[pageBufferOwners.Count];
            for (int i = 0; i < pageBufferOwners.Count; i++)
                pageBuffers[i] = pageBufferOwners[i].Memory;

            var pageContext = new PageContext(pageBuffers);
            var encoding = EncodingUnpacker.UnpackArrayEncoding(page.Encoding);
            IArrayDecoder decoder = V20ArrayEncodingDispatcher.Create(encoding);
            return decoder.Decode(numRows, targetType, pageContext);
        }
        finally
        {
            foreach (var owner in pageBufferOwners)
                owner.Dispose();
        }
    }

    private async Task<IReadOnlyList<IMemoryOwner<byte>>> LoadPageBuffersAsync(
        ColumnMetadata.Types.Page page, CancellationToken cancellationToken)
    {
        int bufCount = page.BufferOffsets.Count;
        if (bufCount != page.BufferSizes.Count)
            throw new LanceFormatException(
                $"Page has {bufCount} buffer_offsets but {page.BufferSizes.Count} buffer_sizes.");

        if (bufCount == 0)
            return System.Array.Empty<IMemoryOwner<byte>>();

        var ranges = new FileRange[bufCount];
        for (int i = 0; i < bufCount; i++)
            ranges[i] = new FileRange(
                checked((long)page.BufferOffsets[i]),
                checked((long)page.BufferSizes[i]));

        return await _reader.ReadRangesAsync(ranges, cancellationToken).ConfigureAwait(false);
    }

    private static IArrowArray BuildEmptyArray(IArrowType type)
    {
        var data = new ArrayData(type, 0, 0, 0, new[] { ArrowBuffer.Empty });
        return ArrowArrayFactory.BuildArray(data);
    }

    public ValueTask DisposeAsync()
    {
        if (_ownsReader)
            return _reader.DisposeAsync();
        return default;
    }

    public void Dispose()
    {
        if (_ownsReader)
            _reader.Dispose();
    }
}
