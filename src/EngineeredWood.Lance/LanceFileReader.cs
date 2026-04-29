// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
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
            // Validates that the type tree only uses constructs we currently
            // decode for v2.1, and returns the number of leaf physical columns
            // it spans. Each primitive leaf = 1 column; struct = sum of
            // children; list<X> / FixedSizeList<X> = leaves(X). Cross-format
            // shapes like LargeList or FixedSizeList<non-primitive> still
            // throw with a precise message.
            fieldColumnRanges = new FieldColumnRange[arrowSchema.FieldsList.Count];
            int columnCursor = 0;
            for (int i = 0; i < arrowSchema.FieldsList.Count; i++)
            {
                int leaves = ValidateAndCountLeavesV21(
                    arrowSchema.FieldsList[i].DataType,
                    arrowSchema.FieldsList[i].Name);
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

    private static int LeafColumnCount(IArrowType type) => type switch
    {
        FixedWidthType => 1,
        StructType st => st.Fields.Sum(f => LeafColumnCount(f.DataType)),
        Apache.Arrow.Types.ListType lt => LeafColumnCount(lt.ValueDataType),
        FixedSizeListType fsl => LeafColumnCount(fsl.ValueDataType),
        StringType or BinaryType => 1,
        _ => 1,
    };

    private static int ValidateAndCountLeavesV21(IArrowType type, string fieldPath)
    {
        switch (type)
        {
            case FixedWidthType:
            case StringType:
            case BinaryType:
                return 1;
            case StructType st:
                {
                    int total = 0;
                    foreach (var child in st.Fields)
                        total += ValidateAndCountLeavesV21(child.DataType, $"{fieldPath}.{child.Name}");
                    return total;
                }
            case Apache.Arrow.Types.ListType lt:
                if (lt.ValueDataType is FixedSizeListType)
                    throw new NotImplementedException(
                        $"Field '{fieldPath}': list of FixedSizeList is not yet supported for v2.1.");
                return ValidateAndCountLeavesV21(lt.ValueDataType, $"{fieldPath}[]");
            case LargeListType llt:
                if (llt.ValueDataType is FixedSizeListType)
                    throw new NotImplementedException(
                        $"Field '{fieldPath}': LargeList of FixedSizeList is not yet supported for v2.1.");
                return ValidateAndCountLeavesV21(llt.ValueDataType, $"{fieldPath}[]");
            case FixedSizeListType fsl:
                if (fsl.ValueDataType is not FixedWidthType)
                    throw new NotImplementedException(
                        $"Field '{fieldPath}': FixedSizeListType with non-primitive items ({fsl.ValueDataType}) is not yet supported for v2.1.");
                return 1;
            default:
                throw new NotImplementedException(
                    $"Field '{fieldPath}': type {type} is not yet supported for v2.1.");
        }
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
            // ReadV21StructAsync. List-of-struct (one Arrow row per top-level
            // field, but multiple physical columns sharing the same rep+def
            // buffers) goes through ReadV21ListOfStructAsync. Other top-level
            // fields (primitives, list-of-primitive) still go through the
            // single-column path.
            // Nested types (struct, list, large_list) go through the recursive
            // walker — arbitrary depth, mixed-shape, list-of-struct,
            // struct-of-list all collapse into the same recursion. FSL and
            // top-level primitives stay on the single-column path.
            if (arrowField.DataType is StructType
                or Apache.Arrow.Types.ListType or LargeListType)
            {
                var (arr, _, _) = await ReadV21NestedAsync(
                    arrowField.DataType, range.StartColumn, cancellationToken).ConfigureAwait(false);
                return arr;
            }
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

    /// <summary>
    /// Per-level information captured by the recursive nested-tree walker.
    /// One entry per ancestor layer above whatever Arrow array a recursive
    /// call returns. <see cref="Validity"/> is null for ALL_VALID_*
    /// layers (no bits to track) or when no nulls were observed.
    /// <see cref="Offsets"/> is set only for list layers (length =
    /// <c>Length + 1</c>).
    /// </summary>
    private sealed class LevelInfo
    {
        public byte[]? Validity { get; init; }
        public int NullCount { get; init; }
        public int Length { get; init; }
        public int[]? Offsets { get; init; }
    }

    private static byte[] ConcatBytes(IReadOnlyList<byte[]> parts)
    {
        long total = 0;
        foreach (var p in parts) total += p.Length;
        var result = new byte[checked((int)total)];
        int pos = 0;
        foreach (var p in parts)
        {
            p.CopyTo(result, pos);
            pos += p.Length;
        }
        return result;
    }

    private static ushort[]? ConcatUshortStreams(IReadOnlyList<ushort[]?> parts)
    {
        // If any chunk has the stream, every chunk must (rep/def is uniform
        // across pages of a column). Concatenate; treat all-null as null.
        bool anyPresent = false;
        long total = 0;
        foreach (var p in parts)
        {
            if (p is not null)
            {
                anyPresent = true;
                total += p.Length;
            }
        }
        if (!anyPresent) return null;
        var result = new ushort[checked((int)total)];
        int pos = 0;
        foreach (var p in parts)
        {
            if (p is null) continue;
            System.Array.Copy(p, 0, result, pos, p.Length);
            pos += p.Length;
        }
        return result;
    }

    /// <summary>
    /// Recursive nested-tree walker. Decodes <paramref name="type"/> starting
    /// at physical column <paramref name="startColumn"/> and returns the
    /// Arrow array at this level plus per-level info for every ancestor
    /// above it (closest ancestor first). Top-level callers can ignore the
    /// ancestor array (it'll be empty); recursive callers pop its head to
    /// drive their own assembly.
    ///
    /// <para>Handles primitive leaves, arbitrary-depth struct trees, and
    /// single-list-deep nested types (the layer path may include zero or one
    /// list layer; multi-list paths like list-of-list are still rejected).
    /// FSL stays on the existing single-column path.</para>
    /// </summary>
    private async Task<(IArrowArray Array, LevelInfo[] AncestorLevels, int NumRows)>
        ReadV21NestedAsync(IArrowType type, int startColumn, CancellationToken cancellationToken)
    {
        if (type is FixedWidthType)
            return await ReadV21NestedLeafAsync(type, startColumn, cancellationToken).ConfigureAwait(false);
        if (type is StructType st)
            return await ReadV21NestedStructAsync(st, startColumn, cancellationToken).ConfigureAwait(false);
        if (type is Apache.Arrow.Types.ListType or LargeListType)
            return await ReadV21NestedListAsync(type, startColumn, cancellationToken).ConfigureAwait(false);
        throw new NotImplementedException(
            $"Recursive walker for type {type} is not yet supported.");
    }

    /// <summary>
    /// Decode a single leaf column. Walks the rep/def streams once and
    /// produces per-level validity bitmaps for every ancestor (cascading:
    /// when a layer goes null, every layer below it is also null in Arrow
    /// convention). For paths with a list ancestor, the list level also
    /// carries Arrow offsets at <c>numRows + 1</c> length.
    /// </summary>
    private async Task<(IArrowArray Array, LevelInfo[] AncestorLevels, int NumRows)>
        ReadV21NestedLeafAsync(IArrowType leafType, int columnIndex, CancellationToken cancellationToken)
    {
        ColumnMetadata cm = _columnMetadatas[columnIndex];
        if (cm.Pages.Count == 0)
            throw new LanceFormatException($"Leaf column {columnIndex} has no pages.");

        // Decode every page into (valueBytes, rep, def) and concatenate.
        // The downstream cascade walker operates on the unified streams
        // unchanged — it doesn't care about page boundaries because rep/def
        // levels carry all the structural information.
        Proto.Encodings.V21.RepDefLayer[]? layers = null;
        int valueBytesPerItem = 0;
        var perPageValues = new List<byte[]>(cm.Pages.Count);
        var perPageRep = new List<ushort[]?>(cm.Pages.Count);
        var perPageDef = new List<ushort[]?>(cm.Pages.Count);
        int visibleItems = 0;

        foreach (var page in cm.Pages)
        {
            var bufferOwners = await LoadPageBuffersAsync(page, cancellationToken).ConfigureAwait(false);
            try
            {
                var pageBuffers = new ReadOnlyMemory<byte>[bufferOwners.Count];
                for (int k = 0; k < bufferOwners.Count; k++)
                    pageBuffers[k] = bufferOwners[k].Memory;
                var pageContext = new PageContext(pageBuffers);
                var pageLayout = EncodingUnpacker.UnpackPageLayout(page.Encoding);
                if (pageLayout.LayoutCase != Proto.Encodings.V21.PageLayout.LayoutOneofCase.MiniBlockLayout)
                    throw new NotImplementedException(
                        $"Leaf column {columnIndex} uses {pageLayout.LayoutCase}; only MiniBlockLayout is supported.");

                var mb = pageLayout.MiniBlockLayout;
                var (vals, r, d, bpi, visible) = Encodings.V21.MiniBlockLayoutDecoder
                    .DecodeNestedLeafChunk(mb, leafType, pageContext);

                if (layers is null)
                {
                    layers = mb.Layers.ToArray();
                    valueBytesPerItem = bpi;
                }
                else if (bpi != valueBytesPerItem)
                {
                    throw new LanceFormatException(
                        $"Column {columnIndex}: page-level value width mismatch ({bpi} vs {valueBytesPerItem}).");
                }

                perPageValues.Add(vals);
                perPageRep.Add(r);
                perPageDef.Add(d);
                visibleItems = checked(visibleItems + visible);
            }
            finally
            {
                foreach (var owner in bufferOwners) owner.Dispose();
            }
        }

        if (layers is null)
            throw new LanceFormatException($"Leaf column {columnIndex} has no decodable pages.");

        byte[] valueBytes = ConcatBytes(perPageValues);
        ushort[]? rep = ConcatUshortStreams(perPageRep);
        ushort[]? def = ConcatUshortStreams(perPageDef);

        int n = layers.Length;
        if (n == 0)
            throw new LanceFormatException($"Leaf column {columnIndex} has zero layers.");

        // Find every list layer; we now support arbitrary nesting depth.
        // listIndices is sorted ascending = innermost first (since layers
        // are ordered leaf-out, lower index = deeper).
        var listIndicesList = new List<int>();
        for (int k = 0; k < n; k++)
            if (IsListLayerKind(layers[k]))
                listIndicesList.Add(k);
        int[] listIndices = listIndicesList.ToArray();
        int numListLayers = listIndices.Length;
        if (numListLayers > 0 && rep is null)
            throw new LanceFormatException($"Column {columnIndex}: list layer present but no rep buffer.");
        if (numListLayers == 0 && rep is not null)
            throw new LanceFormatException($"Column {columnIndex}: rep buffer present but no list layer.");

        // Compute def slot mapping. Some layers contribute 2 slots
        // (NULL_AND_EMPTY_LIST has both null and empty); innermost layers
        // get smaller def values. -1 means "this layer has no slot of that
        // kind".
        int next = 1;
        int[] layerNullDef = new int[n];
        int[] layerEmptyDef = new int[n];
        for (int k = 0; k < n; k++)
        {
            layerNullDef[k] = -1;
            layerEmptyDef[k] = -1;
            switch (layers[k])
            {
                case Proto.Encodings.V21.RepDefLayer.RepdefAllValidItem:
                case Proto.Encodings.V21.RepDefLayer.RepdefAllValidList:
                    break;
                case Proto.Encodings.V21.RepDefLayer.RepdefNullableItem:
                case Proto.Encodings.V21.RepDefLayer.RepdefNullableList:
                    layerNullDef[k] = next++;
                    break;
                case Proto.Encodings.V21.RepDefLayer.RepdefEmptyableList:
                    layerEmptyDef[k] = next++;
                    break;
                case Proto.Encodings.V21.RepDefLayer.RepdefNullAndEmptyList:
                    layerNullDef[k] = next++;
                    layerEmptyDef[k] = next++;
                    break;
                default:
                    throw new NotImplementedException(
                        $"Column {columnIndex}: unsupported layer kind '{layers[k]}'.");
            }
        }

        // Pre-pass to count rows per list layer (respecting cascade), so we
        // can size offset arrays + per-level bitmaps exactly. For pure-struct
        // (no list) every level is at visibleItems = numItems.
        //
        // Cascade rule for *counting*: skip a list-j row when the def value
        // marks a layer at or above the next list above j (= a list cascade
        // wipes out everything below it). A non-list (struct) cascade
        // BETWEEN lists doesn't skip — Arrow's struct-with-list-child has
        // child.length = struct.length, so the deeper list still has a row
        // at that position with cascaded validity. Same logic applies in
        // the walk loop below.
        int[] listRowCounts = new int[numListLayers];
        if (numListLayers > 0)
        {
            for (int i = 0; i < rep!.Length; i++)
            {
                int r = rep[i];
                if (r == 0) continue;
                int defValue = def is null ? 0 : def[i];
                int kNullLayerIdx = -1;
                if (defValue != 0)
                {
                    for (int k = 0; k < n; k++)
                    {
                        if (layerNullDef[k] == defValue || layerEmptyDef[k] == defValue) { kNullLayerIdx = k; break; }
                    }
                    if (kNullLayerIdx < 0)
                        throw new LanceFormatException(
                            $"Unexpected def value {defValue} at level {i} in column {columnIndex}.");
                }
                for (int j = numListLayers - 1; j >= 0; j--)
                {
                    int listIdx = listIndices[j];
                    int repForThis = j + 1;
                    if (r < repForThis) continue;
                    int nextListAbove = (j + 1 < numListLayers) ? listIndices[j + 1] : int.MaxValue;
                    if (kNullLayerIdx >= nextListAbove) continue;
                    listRowCounts[j]++;
                }
            }
        }

        // Each layer's array length: list layers use their own row count;
        // non-list layers inherit from the closest deeper list, falling
        // back to visibleItems when no list sits below them.
        int[] levelLengths = new int[n];
        for (int k = 0; k < n; k++)
        {
            if (IsListLayerKind(layers[k]))
            {
                int j = System.Array.IndexOf(listIndices, k);
                levelLengths[k] = listRowCounts[j];
            }
            else
            {
                int closestDeeperList = -1;
                for (int d = k - 1; d >= 0; d--)
                    if (IsListLayerKind(layers[d])) { closestDeeperList = d; break; }
                if (closestDeeperList < 0)
                    levelLengths[k] = visibleItems;
                else
                {
                    int j = System.Array.IndexOf(listIndices, closestDeeperList);
                    levelLengths[k] = listRowCounts[j];
                }
            }
        }
        int numRows = levelLengths[n - 1];

        byte[]?[] levelBitmaps = new byte[n][];
        int[] levelNullCounts = new int[n];
        for (int k = 0; k < n; k++)
        {
            // Allocate a validity bitmap only when the layer can be null.
            // Empty-only list layers (EmptyableList) don't need one — empty
            // lists are still Arrow-valid; the offsets carry the empty span.
            bool nullable = layerNullDef[k] != -1;
            if (!nullable) continue;
            levelBitmaps[k] = new byte[(levelLengths[k] + 7) / 8];
            if (levelBitmaps[k]!.Length == 0) continue;
            System.Array.Fill(levelBitmaps[k]!, (byte)0xFF);
            int trailing = levelLengths[k] & 7;
            if (trailing != 0)
                levelBitmaps[k]![^1] &= (byte)((1 << trailing) - 1);
        }

        // Per-list-layer offset arrays (length+1 entries each).
        int[]?[] listOffsets = new int[numListLayers][];
        for (int j = 0; j < numListLayers; j++)
            listOffsets[j] = new int[listRowCounts[j] + 1];

        if (numListLayers == 0)
        {
            // Pure struct path. def[i] applies to row i directly.
            if (def is not null)
            {
                for (int i = 0; i < numRows; i++)
                {
                    int defValue = def[i];
                    if (defValue == 0) continue;
                    int kNull = FindLayerForDef(defValue, layerNullDef, layerEmptyDef, columnIndex, i);
                    for (int k = 0; k <= kNull; k++)
                    {
                        if (levelBitmaps[k] is null) continue;
                        levelBitmaps[k]![i >> 3] &= (byte)~(1 << (i & 7));
                        levelNullCounts[k]++;
                    }
                }
            }
        }
        else
        {
            // Multi-list walk. rep[i] == r means: open a new boundary at the
            // r-th-deepest list layer. Lower (deeper) levels open implicitly
            // unless a cascade from above blocks them. rep == 0 is just an
            // item continuation. See project memory for full convention.
            int innerListIdx = listIndices[0];
            int[] currentListRowIdx = new int[numListLayers];
            for (int j = 0; j < numListLayers; j++) currentListRowIdx[j] = -1;
            int visibleIdx = 0;

            for (int i = 0; i < rep!.Length; i++)
            {
                int r = rep[i];
                int defValue = def is null ? 0 : def[i];
                int kNull = -1;
                bool isEmpty = false;
                if (defValue != 0)
                {
                    for (int k = 0; k < n; k++)
                    {
                        if (layerNullDef[k] == defValue) { kNull = k; break; }
                        if (layerEmptyDef[k] == defValue) { kNull = k; isEmpty = true; break; }
                    }
                    if (kNull < 0)
                        throw new LanceFormatException(
                            $"Unexpected def value {defValue} at level {i} in column {columnIndex}.");
                }

                bool consumesSlot;

                if (r == 0)
                {
                    // Continuation of the innermost list. def must be 0 or a
                    // below-innermost-list null (item null).
                    if (kNull >= innerListIdx)
                        throw new LanceFormatException(
                            $"Column {columnIndex}: rep=0 at level {i} but def value {defValue} marks a non-item level ({kNull}).");
                    consumesSlot = true;
                }
                else
                {
                    // rep >= 1: open boundaries at list levels j where
                    // rep_for_this <= r AND no LIST cascade above blocks
                    // (struct cascade between lists doesn't block — see
                    // pre-pass comment). Process outermost → innermost.
                    for (int j = numListLayers - 1; j >= 0; j--)
                    {
                        int listIdx = listIndices[j];
                        int repForThis = j + 1;
                        if (r < repForThis) continue;
                        int nextListAbove = (j + 1 < numListLayers) ? listIndices[j + 1] : int.MaxValue;
                        if (kNull >= nextListAbove) continue;

                        currentListRowIdx[j]++;
                        int rowIdx = currentListRowIdx[j];
                        // Offset target is the next-deeper level's current row
                        // count, or visibleIdx for the innermost list.
                        listOffsets[j]![rowIdx] = (j == 0) ? visibleIdx : currentListRowIdx[j - 1] + 1;

                        // Validity at this list layer. listIsNull when our own
                        // def is null (not empty), or any cascade from a layer
                        // strictly above this list (struct cascade through to
                        // here — list cascade was already filtered above).
                        bool listIsNull = ((kNull == listIdx) && !isEmpty) || (kNull > listIdx);
                        if (listIsNull && levelBitmaps[listIdx] is not null)
                        {
                            levelBitmaps[listIdx]![rowIdx >> 3] &= (byte)~(1 << (rowIdx & 7));
                            levelNullCounts[listIdx]++;
                        }

                        // Non-list layers strictly above THIS list (and below
                        // the next-shallower list, or up to the top if this
                        // is the outermost) also fire a row event here —
                        // their length tracks this list's row count via the
                        // closest-deeper-list rule.
                        int aboveStart = listIdx + 1;
                        int aboveEnd = (j + 1 < numListLayers) ? listIndices[j + 1] - 1 : n - 1;
                        for (int k = aboveStart; k <= aboveEnd; k++)
                        {
                            if (IsListLayerKind(layers[k])) continue;
                            if (levelBitmaps[k] is null) continue;
                            bool valid = (kNull == -1) || (kNull < k);
                            if (!valid)
                            {
                                levelBitmaps[k]![rowIdx >> 3] &= (byte)~(1 << (rowIdx & 7));
                                levelNullCounts[k]++;
                            }
                        }
                    }
                    // The position consumes a value slot iff (a) no list
                    // cascade above the innermost list blocked the innermost
                    // list from opening AND (b) the innermost list itself
                    // isn't null/empty. (a) means kNull < listIndices[0]
                    // (cascade from below the innermost list, ie item null —
                    // valid item slot) OR kNull == -1.
                    consumesSlot = (kNull == -1) || (kNull < innerListIdx);
                }

                if (!consumesSlot) continue;

                // Per-item processing for layers below the innermost list
                // (item leaf and any non-list layers between leaf and
                // innermost list).
                int belowEnd = numListLayers > 0 ? innerListIdx : n;
                for (int k = 0; k < belowEnd; k++)
                {
                    if (levelBitmaps[k] is null) continue;
                    bool valid = (kNull == -1) || (kNull < k);
                    if (!valid)
                    {
                        levelBitmaps[k]![visibleIdx >> 3] &= (byte)~(1 << (visibleIdx & 7));
                        levelNullCounts[k]++;
                    }
                }
                visibleIdx++;
            }

            // Finalise offsets: write the ending sentinel for each list layer.
            for (int j = 0; j < numListLayers; j++)
            {
                int finalEnd = (j == 0) ? visibleIdx : currentListRowIdx[j - 1] + 1;
                listOffsets[j]![listRowCounts[j]] = finalEnd;
            }

            if (visibleIdx != visibleItems)
                throw new LanceFormatException(
                    $"Column {columnIndex} visible-item walk produced {visibleIdx} but the page declared {visibleItems}.");
        }

        // Build the leaf array using level 0's bitmap, at level 0's length.
        byte[]? leafValidity = (levelBitmaps[0] is not null && levelNullCounts[0] > 0)
            ? levelBitmaps[0]
            : null;
        var leafArr = Encodings.V21.MiniBlockLayoutDecoder.BuildFixedWidthArray(
            leafType, levelLengths[0], valueBytes, leafValidity, levelNullCounts[0]);

        // Build ancestor LevelInfos for layers 1..n-1.
        var ancestors = new LevelInfo[n - 1];
        for (int k = 1; k < n; k++)
        {
            byte[]? validity = (levelBitmaps[k] is not null && levelNullCounts[k] > 0)
                ? levelBitmaps[k]
                : null;
            int[]? offsets = null;
            if (IsListLayerKind(layers[k]))
            {
                int j = System.Array.IndexOf(listIndices, k);
                offsets = listOffsets[j];
            }
            ancestors[k - 1] = new LevelInfo
            {
                Validity = validity,
                NullCount = levelNullCounts[k],
                Length = levelLengths[k],
                Offsets = offsets,
            };
        }

        // Return the leaf's own array length — this is the length at the
        // leaf's level, NOT the top of the path. List recursion pops the
        // list level (which uses its own Length); struct recursion uses
        // its own level's Length when popping. Each recursion level
        // returns at its own level's length.
        return (leafArr, ancestors, levelLengths[0]);
    }

    private static bool IsListLayerKind(Proto.Encodings.V21.RepDefLayer layer) => layer
        is Proto.Encodings.V21.RepDefLayer.RepdefAllValidList
        or Proto.Encodings.V21.RepDefLayer.RepdefNullableList
        or Proto.Encodings.V21.RepDefLayer.RepdefEmptyableList
        or Proto.Encodings.V21.RepDefLayer.RepdefNullAndEmptyList;

    private static int FindLayerForDef(
        int defValue, int[] layerNullDef, int[] layerEmptyDef, int columnIndex, int row)
    {
        for (int k = 0; k < layerNullDef.Length; k++)
        {
            if (layerNullDef[k] == defValue || layerEmptyDef[k] == defValue) return k;
        }
        throw new LanceFormatException(
            $"Unexpected def value {defValue} at row {row} in column {columnIndex} (no matching layer).");
    }

    /// <summary>
    /// Decode a struct of arbitrary depth. Recurses on each child, reconciles
    /// the closest ancestor (this struct's own validity from each child's
    /// view) byte-for-byte across siblings, builds the StructArray, and pops
    /// the head of the ancestor list before returning to the parent.
    /// </summary>
    private async Task<(IArrowArray Array, LevelInfo[] AncestorLevels, int NumRows)>
        ReadV21NestedStructAsync(StructType st, int startColumn, CancellationToken cancellationToken)
    {
        int childCount = st.Fields.Count;
        var childArrays = new IArrowArray[childCount];
        LevelInfo[]? canonicalAncestors = null;
        int childArrayLength = -1;

        int columnCursor = startColumn;
        for (int i = 0; i < childCount; i++)
        {
            var child = st.Fields[i];
            var (childArr, childAncestors, childOwnLength) = await ReadV21NestedAsync(
                child.DataType, columnCursor, cancellationToken).ConfigureAwait(false);
            childArrays[i] = childArr;
            columnCursor += LeafColumnCount(child.DataType);

            if (childArrayLength < 0) childArrayLength = childOwnLength;
            else if (childOwnLength != childArrayLength)
                throw new LanceFormatException(
                    $"Struct child '{child.Name}' has length {childOwnLength} but sibling has {childArrayLength}.");
            if (childAncestors.Length == 0)
                throw new LanceFormatException(
                    $"Struct child '{child.Name}' produced no ancestor levels; the leaf path " +
                    "didn't include the parent struct as an outer layer.");

            if (canonicalAncestors is null)
            {
                canonicalAncestors = childAncestors;
            }
            else
            {
                if (childAncestors.Length != canonicalAncestors.Length)
                    throw new LanceFormatException(
                        $"Struct child '{child.Name}' has {childAncestors.Length} ancestor levels " +
                        $"but sibling has {canonicalAncestors.Length}.");
                ReconcileLevels(canonicalAncestors[0], childAncestors[0], child.Name);
            }
        }

        // ancestors[0] is THIS struct's level — its Length is what the
        // StructArray takes (children's lengths must equal this), and its
        // validity becomes the struct's own validity. Pop it before passing
        // the rest of the ancestors up.
        var thisLevel = canonicalAncestors![0];
        if (childArrayLength != thisLevel.Length)
            throw new LanceFormatException(
                $"Struct '{st}' children have length {childArrayLength} but the struct level reports {thisLevel.Length}.");
        ArrowBuffer validity = (thisLevel.Validity is not null && thisLevel.NullCount > 0)
            ? new ArrowBuffer(thisLevel.Validity)
            : ArrowBuffer.Empty;
        var structArr = new StructArray(new ArrayData(
            st, thisLevel.Length, thisLevel.NullCount, 0,
            new[] { validity },
            childArrays.Select(a => a.Data).ToArray()));

        var newAncestors = new LevelInfo[canonicalAncestors.Length - 1];
        System.Array.Copy(canonicalAncestors, 1, newAncestors, 0, newAncestors.Length);
        return (structArr, newAncestors, thisLevel.Length);
    }

    /// <summary>
    /// Decode a list (or large_list) at this level. Recurses on the inner
    /// type to get a leaf-aligned Arrow array (length = visibleItems) plus
    /// the list level's <see cref="LevelInfo"/> at the head of its
    /// ancestor list. Pops the list level, builds the
    /// <see cref="ListArray"/> / <see cref="LargeListArray"/>, and returns
    /// the rest of the ancestors to the parent.
    /// </summary>
    private async Task<(IArrowArray Array, LevelInfo[] AncestorLevels, int NumRows)>
        ReadV21NestedListAsync(IArrowType listType, int startColumn, CancellationToken cancellationToken)
    {
        IArrowType innerType = listType switch
        {
            Apache.Arrow.Types.ListType lt => lt.ValueDataType,
            LargeListType llt => llt.ValueDataType,
            _ => throw new LanceFormatException(
                $"ReadV21NestedListAsync target {listType} is not a list type."),
        };
        bool isLarge = listType is LargeListType;

        var (innerArr, innerAncestors, _) = await ReadV21NestedAsync(
            innerType, startColumn, cancellationToken).ConfigureAwait(false);

        if (innerAncestors.Length == 0)
            throw new LanceFormatException(
                "List inner walker returned no ancestor levels — expected the list level at index 0.");
        var listLevel = innerAncestors[0];
        if (listLevel.Offsets is null)
            throw new LanceFormatException(
                "List inner walker returned an ancestor without offsets where the list level was expected.");

        int numRows = listLevel.Length;
        byte[] offsetsBytes;
        if (isLarge)
        {
            offsetsBytes = new byte[(numRows + 1) * sizeof(long)];
            for (int i = 0; i <= numRows; i++)
                BinaryPrimitives.WriteInt64LittleEndian(
                    offsetsBytes.AsSpan(i * 8, 8), listLevel.Offsets[i]);
        }
        else
        {
            offsetsBytes = new byte[(numRows + 1) * sizeof(int)];
            for (int i = 0; i <= numRows; i++)
                BinaryPrimitives.WriteInt32LittleEndian(
                    offsetsBytes.AsSpan(i * 4, 4), listLevel.Offsets[i]);
        }

        ArrowBuffer validity = (listLevel.Validity is not null && listLevel.NullCount > 0)
            ? new ArrowBuffer(listLevel.Validity)
            : ArrowBuffer.Empty;
        var listData = new ArrayData(
            listType, numRows, listLevel.NullCount, 0,
            new[] { validity, new ArrowBuffer(offsetsBytes) },
            children: new[] { innerArr.Data });
        IArrowArray listArr = isLarge ? new LargeListArray(listData) : new ListArray(listData);

        var newAncestors = new LevelInfo[innerAncestors.Length - 1];
        System.Array.Copy(innerAncestors, 1, newAncestors, 0, newAncestors.Length);
        return (listArr, newAncestors, numRows);
    }

    private static void ReconcileLevels(LevelInfo canonical, LevelInfo other, string childName)
    {
        if ((canonical.Validity is null) != (other.Validity is null))
            throw new LanceFormatException(
                $"Struct child '{childName}' outer-layer presence disagrees with sibling " +
                "(one column has a NULLABLE_ITEM outer layer, the other ALL_VALID_ITEM).");
        if (canonical.Length != other.Length)
            throw new LanceFormatException(
                $"Struct child '{childName}' outer-layer length {other.Length} disagrees with sibling {canonical.Length}.");
        if (canonical.Validity is not null && other.Validity is not null)
        {
            if (!other.Validity.AsSpan().SequenceEqual(canonical.Validity)
                || other.NullCount != canonical.NullCount)
                throw new LanceFormatException(
                    $"Struct child '{childName}' struct-level validity disagrees with sibling " +
                    "(cross-column rep/def coherence violated).");
        }
    }

    private async Task<IArrowArray> ReadV21SingleColumnAsync(
        int columnIndex, IArrowType targetType, CancellationToken cancellationToken)
    {
        ColumnMetadata cm = _columnMetadatas[columnIndex];
        if (cm.Pages.Count == 0)
            return BuildEmptyArray(targetType);
        if (cm.Pages.Count == 1)
            return await DecodeV21PageAsync(cm.Pages[0], targetType, cancellationToken)
                .ConfigureAwait(false);

        var perPage = new IArrowArray[cm.Pages.Count];
        for (int p = 0; p < cm.Pages.Count; p++)
            perPage[p] = await DecodeV21PageAsync(cm.Pages[p], targetType, cancellationToken)
                .ConfigureAwait(false);
        return ArrowArrayConcatenator.Concatenate(perPage);
    }

    private async Task<IArrowArray> DecodeV21PageAsync(
        ColumnMetadata.Types.Page page, IArrowType targetType,
        CancellationToken cancellationToken)
    {
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

        // Pre-pass: total list rows = sum of every page's row count, so we
        // can size the merged Arrow offsets / validity bitmap up front.
        int totalRows = 0;
        foreach (var pg in cm.Pages)
            totalRows = checked(totalRows + (int)pg.Length);

        var arrowOffsets = new byte[arrowOffsetWidth * (totalRows + 1)];
        var bitmap = new byte[(totalRows + 7) / 8];
        WriteArrowOffset(arrowOffsets, 0, arrowOffsetWidth, 0);

        int totalNullCount = 0;
        ulong arrowCumulative = 0;
        int rowCursor = 0;

        foreach (var page in cm.Pages)
        {
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

                AppendListPageOffsets(
                    offsetBytes.Span, numListRows, nullAdjustment,
                    arrowOffsets, bitmap, arrowOffsetWidth, rowCursor,
                    ref arrowCumulative, ref totalNullCount);
            }
            finally
            {
                foreach (var owner in bufferOwners) owner.Dispose();
            }
            rowCursor += numListRows;
        }

        return (arrowOffsets, totalNullCount == 0 ? null : bitmap, totalNullCount, totalRows);
    }

    private static void AppendListPageOffsets(
        ReadOnlySpan<byte> offsetBytes, int numListRows, ulong nullAdjustment,
        byte[] arrowOffsets, byte[] bitmap, int arrowOffsetWidth, int globalRowStart,
        ref ulong arrowCumulative, ref int totalNullCount)
    {
        // Within a single page, Lance stores cumulative end-offsets relative
        // to that page's diskBase=0; null sentinel uses the page's
        // null_offset_adjustment. Across pages, child rows stitch together
        // contiguously in Arrow, so arrowCumulative carries across — we
        // only reset diskBase per page.
        ulong diskBase = 0;
        for (int i = 0; i < numListRows; i++)
        {
            ulong diskOffset = System.Buffers.Binary.BinaryPrimitives
                .ReadUInt64LittleEndian(offsetBytes.Slice(i * 8, 8));
            bool isNull = diskOffset >= nullAdjustment;
            ulong endModulo = isNull ? diskOffset - nullAdjustment : diskOffset;
            ulong len = endModulo - diskBase;

            int globalRow = globalRowStart + i;
            if (isNull)
            {
                totalNullCount++;
            }
            else
            {
                arrowCumulative += len;
                bitmap[globalRow >> 3] |= (byte)(1 << (globalRow & 7));
            }
            WriteArrowOffset(arrowOffsets, globalRow + 1, arrowOffsetWidth, arrowCumulative);
            diskBase = endModulo;
        }
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
        long total = 0;
        foreach (var page in cm.Pages)
        {
            var encoding = EncodingUnpacker.UnpackArrayEncoding(page.Encoding);
            if (encoding.ArrayEncodingCase != requireEncoding)
                throw new LanceFormatException(
                    $"Column {columnIndex} has unexpected encoding " +
                    $"'{encoding.ArrayEncodingCase}', expected '{requireEncoding}'.");
            total = checked(total + (long)page.Length);
        }
        return checked((int)total);
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

        if (cm.Pages.Count == 1)
            return await DecodeV20PageAsync(cm.Pages[0], targetType, cancellationToken)
                .ConfigureAwait(false);

        var perPage = new IArrowArray[cm.Pages.Count];
        for (int p = 0; p < cm.Pages.Count; p++)
            perPage[p] = await DecodeV20PageAsync(cm.Pages[p], targetType, cancellationToken)
                .ConfigureAwait(false);
        return ArrowArrayConcatenator.Concatenate(perPage);
    }

    private async Task<IArrowArray> DecodeV20PageAsync(
        ColumnMetadata.Types.Page page, IArrowType targetType,
        CancellationToken cancellationToken)
    {
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
