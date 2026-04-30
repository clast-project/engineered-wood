// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Proto;
using EngineeredWood.Lance.Proto.Encodings.V20;
using EngineeredWood.Lance.Proto.Encodings.V21;
using EngineeredWood.Lance.Proto.V2;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace EngineeredWood.Lance;

/// <summary>
/// Writer for v2.1 Lance files. Phase 13 MVP: produces a single-file
/// (no Lance dataset wrapper, no manifest) Lance v2.1 file containing
/// one or more columns of either fixed-width primitives
/// (<see cref="sbyte"/>, <see cref="short"/>, <see cref="int"/>,
/// <see cref="long"/> and unsigned counterparts, <see cref="float"/>,
/// <see cref="double"/>) or variable-length strings/binary, nullable
/// or not, packed into a single MiniBlockLayout page per column with
/// as many chunks as needed (each chunk capped at 32 KB by the v2.1
/// chunk-meta word). Fixed-width columns use <see cref="Flat"/> value
/// compression; strings/binary use <see cref="Variable"/> with
/// Flat(u32) offsets.
///
/// <para><b>What this writer covers</b>:</para>
/// <list type="bullet">
///   <item>v2.1 file envelope: page buffers → column metadata blobs →
///   CMO table → FileDescriptor (global buffer 0) → GBO table →
///   40-byte footer + <c>"LANC"</c> magic.</item>
///   <item><see cref="MiniBlockLayout"/> with <c>num_buffers=1</c>,
///   <c>layers=[ALL_VALID_ITEM]</c> for null-free columns or
///   <c>layers=[NULLABLE_ITEM]</c> with <c>def_compression=Flat(16)</c>
///   for nullable ones; single chunk, no rep.</item>
///   <item>Schema: top-level primitive leaves with sequential field
///   ids.</item>
/// </list>
///
/// <para><b>What's deferred</b>:</para>
/// <list type="bullet">
///   <item>Booleans (bit-packed encoding, not byte-aligned).</item>
///   <item>Lists, structs, FSL.</item>
///   <item>Other value encodings (FullZip, Bitpacking, FSST,
///   Dictionary, ByteStreamSplit, ...).</item>
///   <item>Multi-page columns (Lance splits at the page boundary too,
///   typically once a page approaches its preferred size). Right now
///   we always emit exactly one page per column.</item>
///   <item>Lance dataset wrapper (manifest, fragments, _versions/).</item>
///   <item>Cloud / multipart upload — only <see cref="ISequentialFile"/>
///   to a local file or memory.</item>
/// </list>
/// </summary>
public sealed class LanceFileWriter : IAsyncDisposable
{
    private const int MiniBlockAlignment = 8;

    // pylance aligns page-buffer file positions to 64 bytes (the typical
    // local-disk minimum-read sector for lance-rs). lance-rs's reader
    // assumes this alignment when computing buffer ranges, and panics on
    // unaligned input — so we mirror it.
    private const int PageBufferAlignment = 64;

    private readonly ISequentialFile _file;
    private readonly bool _ownsFile;
    private readonly LanceVersion _version;
    private readonly List<Proto.Field> _fields = new();
    private readonly List<ColumnMetadata> _columns = new();
    private long _totalRows = -1;
    private long _finalSizeBytes = -1;
    private bool _finalized;
    private bool _disposed;

    /// <summary>
    /// Schema fields accumulated from per-column writes, in declaration
    /// order. Stable after <see cref="FinishAsync"/>; read by
    /// <c>LanceDatasetWriter</c> when building a dataset manifest.
    /// </summary>
    internal IReadOnlyList<Proto.Field> SchemaFields => _fields;

    /// <summary>
    /// Number of rows written to the file (matches the row count of every
    /// column). <c>0</c> if no columns have been added yet.
    /// </summary>
    internal long TotalRows => Math.Max(0L, _totalRows);

    /// <summary>
    /// Total file size in bytes after <see cref="FinishAsync"/>; <c>-1</c>
    /// before finish completes.
    /// </summary>
    internal long FinalSizeBytes => _finalSizeBytes;

    internal LanceVersion Version => _version;

    private LanceFileWriter(ISequentialFile file, bool ownsFile, LanceVersion version)
    {
        _file = file;
        _ownsFile = ownsFile;
        _version = version;
    }

    /// <summary>
    /// Creates a writer over a caller-provided <see cref="ISequentialFile"/>.
    /// The caller controls the file's lifetime when <paramref name="ownsFile"/>
    /// is <c>false</c>.
    /// </summary>
    public static LanceFileWriter Create(ISequentialFile file, bool ownsFile = false)
    {
        if (file is null) throw new ArgumentNullException(nameof(file));
        return new LanceFileWriter(file, ownsFile, LanceVersion.V2_1);
    }

    /// <summary>
    /// Creates a writer that emits to <paramref name="path"/> on the local
    /// filesystem, overwriting any existing file. The returned writer owns
    /// the underlying file handle.
    /// </summary>
    public static async ValueTask<LanceFileWriter> CreateAsync(
        string path, CancellationToken cancellationToken = default)
    {
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(
            System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(path))!);
        var seq = await fs.CreateAsync(
            System.IO.Path.GetFileName(path), overwrite: true, cancellationToken).ConfigureAwait(false);
        return new LanceFileWriter(seq, ownsFile: true, LanceVersion.V2_1);
    }

    private static CompressiveEncoding FlatEncoding(int bitsPerValue) =>
        new() { Flat = new Proto.Encodings.V21.Flat { BitsPerValue = (ulong)bitsPerValue } };

    /// <summary>
    /// Test-only: write a non-nullable <see cref="int"/> column whose values
    /// are split across multiple pages. Each entry of
    /// <paramref name="valuesPerPage"/> becomes one page; the column ends up
    /// with <c>valuesPerPage.Count</c> pages and a row count equal to the
    /// total. Lets tests deterministically exercise the multi-page reader
    /// path the public typed Write methods don't currently expose.
    /// </summary>
    internal Task WriteInt32ColumnPagedAsync(
        string name, IReadOnlyList<int[]> valuesPerPage,
        CancellationToken cancellationToken = default)
    {
        var pageBytes = new byte[valuesPerPage.Count][];
        for (int i = 0; i < valuesPerPage.Count; i++)
            pageBytes[i] = MemoryMarshal.AsBytes(valuesPerPage[i].AsSpan()).ToArray();
        return WriteFlatPagedColumnAsync(name, "int32", bytesPerValue: 4, pageBytes, cancellationToken);
    }

    /// <summary>
    /// Test-only: write a non-nullable string column whose values are split
    /// across multiple pages. Each <see cref="StringArray"/> in
    /// <paramref name="pagesValues"/> becomes one page.
    /// </summary>
    internal async Task WriteStringColumnPagedAsync(
        string name, IReadOnlyList<StringArray> pagesValues,
        CancellationToken cancellationToken = default)
    {
        ThrowIfFinalized();
        int totalItems = 0;
        foreach (var p in pagesValues) totalItems += p.Length;
        if (_totalRows < 0) _totalRows = totalItems;
        else if (_totalRows != totalItems)
            throw new ArgumentException(
                $"Column '{name}' has {totalItems} rows but earlier columns have {_totalRows}.",
                nameof(pagesValues));

        var valueEncoding = new CompressiveEncoding
        {
            Variable = new Proto.Encodings.V21.Variable
            {
                Offsets = new CompressiveEncoding
                {
                    Flat = new Proto.Encodings.V21.Flat { BitsPerValue = 32 },
                },
            },
        };

        var pageProtos = new List<ColumnMetadata.Types.Page>(pagesValues.Count);
        foreach (var arr in pagesValues)
        {
            int pageRows = arr.Length;
            // Re-use the same Variable chunking algorithm via a helper.
            var chunks = new List<(int, int, byte[])>();
            int idx = 0;
            while (idx < pageRows)
            {
                int items = 0;
                int dataBytes = 0;
                while (idx + items < pageRows)
                {
                    int newCount = items + 1;
                    int newDataBytes = dataBytes + arr.GetBytes(idx + items).Length;
                    int valueBufLen = (newCount + 1) * sizeof(uint) + newDataBytes;
                    int total = ChunkTotalBytes(newCount, valueBufLen, hasDef: false);
                    if (total > MaxChunkBytes) break;
                    items = newCount;
                    dataBytes = newDataBytes;
                }
                if (items == 0)
                    throw new InvalidOperationException(
                        $"Variable item at index {idx} alone exceeds chunk budget.");
                bool isLast = idx + items == pageRows;
                int finalItems = isLast ? items : Pow2Floor(items);
                byte[] valueBuf = BuildVariableChunkValueBuffer(arr, idx, finalItems);
                chunks.Add((idx, finalItems, valueBuf));
                idx += finalItems;
            }

            var page = await BuildAndWritePageAsync(
                valueEncoding, pageRows, chunks, validityBitmap: null, nullCount: 0,
                cancellationToken).ConfigureAwait(false);
            pageProtos.Add(page);
        }

        RegisterColumn(name, "string", valueEncoding, pageProtos);
    }

    /// <summary>Append a non-nullable <see cref="sbyte"/> column.</summary>
    public Task WriteInt8ColumnAsync(string name, ReadOnlySpan<sbyte> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "int8", 1, values.Length, MemoryMarshal.AsBytes(values).ToArray(), null, 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="byte"/> column.</summary>
    public Task WriteUInt8ColumnAsync(string name, ReadOnlySpan<byte> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "uint8", 1, values.Length, values.ToArray(), null, 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="short"/> column.</summary>
    public Task WriteInt16ColumnAsync(string name, ReadOnlySpan<short> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "int16", 2, values.Length, MemoryMarshal.AsBytes(values).ToArray(), null, 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="ushort"/> column.</summary>
    public Task WriteUInt16ColumnAsync(string name, ReadOnlySpan<ushort> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "uint16", 2, values.Length, MemoryMarshal.AsBytes(values).ToArray(), null, 0, cancellationToken);

    /// <summary>
    /// Append a non-nullable <see cref="int"/> column to the file.
    /// Every column added must have the same length as the first; the
    /// length becomes the file's row count.
    /// </summary>
    public Task WriteInt32ColumnAsync(string name, ReadOnlySpan<int> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "int32", 4, values.Length, MemoryMarshal.AsBytes(values).ToArray(), null, 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="uint"/> column.</summary>
    public Task WriteUInt32ColumnAsync(string name, ReadOnlySpan<uint> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "uint32", 4, values.Length, MemoryMarshal.AsBytes(values).ToArray(), null, 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="long"/> column.</summary>
    public Task WriteInt64ColumnAsync(string name, ReadOnlySpan<long> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "int64", 8, values.Length, MemoryMarshal.AsBytes(values).ToArray(), null, 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="ulong"/> column.</summary>
    public Task WriteUInt64ColumnAsync(string name, ReadOnlySpan<ulong> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "uint64", 8, values.Length, MemoryMarshal.AsBytes(values).ToArray(), null, 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="float"/> (Float32) column.</summary>
    public Task WriteFloatColumnAsync(string name, ReadOnlySpan<float> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "float", 4, values.Length, MemoryMarshal.AsBytes(values).ToArray(), null, 0, cancellationToken);

    /// <summary>Append a non-nullable <see cref="double"/> (Float64) column.</summary>
    public Task WriteDoubleColumnAsync(string name, ReadOnlySpan<double> values, CancellationToken cancellationToken = default)
        => WriteFlatPageAsync(name, "double", 8, values.Length, MemoryMarshal.AsBytes(values).ToArray(), null, 0, cancellationToken);

    /// <summary>
    /// Append an Apache Arrow column. Supported types:
    /// fixed-width primitives (Int8/16/32/64, UInt8/16/32/64, Float,
    /// Double) plus variable-length <see cref="StringArray"/> and
    /// <see cref="BinaryArray"/>. The array's null bitmap (if any) is
    /// converted to a Lance <c>NULLABLE_ITEM</c> def buffer; if the array
    /// has no nulls the page uses <c>ALL_VALID_ITEM</c>.
    /// </summary>
    public Task WriteColumnAsync(string name, IArrowArray array, CancellationToken cancellationToken = default)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));

        // StringArray inherits from BinaryArray, so test it first; the only
        // wire-level difference is the schema's logical_type string.
        if (array is StringArray sa)
            return WriteVariableColumnAsync(name, "string", sa, cancellationToken);
        if (array is BinaryArray ba)
            return WriteVariableColumnAsync(name, "binary", ba, cancellationToken);
        if (array is FixedSizeListArray fsl)
            return WriteFixedSizeListColumnAsync(name, fsl, cancellationToken);

        var (logicalType, bytesPerValue, valueBytes) = array switch
        {
            Int8Array a => ("int8", 1, MemoryMarshal.AsBytes(a.Values).ToArray()),
            UInt8Array a => ("uint8", 1, a.Values.ToArray()),
            Int16Array a => ("int16", 2, MemoryMarshal.AsBytes(a.Values).ToArray()),
            UInt16Array a => ("uint16", 2, MemoryMarshal.AsBytes(a.Values).ToArray()),
            Int32Array a => ("int32", 4, MemoryMarshal.AsBytes(a.Values).ToArray()),
            UInt32Array a => ("uint32", 4, MemoryMarshal.AsBytes(a.Values).ToArray()),
            Int64Array a => ("int64", 8, MemoryMarshal.AsBytes(a.Values).ToArray()),
            UInt64Array a => ("uint64", 8, MemoryMarshal.AsBytes(a.Values).ToArray()),
            FloatArray a => ("float", 4, MemoryMarshal.AsBytes(a.Values).ToArray()),
            DoubleArray a => ("double", 8, MemoryMarshal.AsBytes(a.Values).ToArray()),
            _ => throw new NotSupportedException(
                $"LanceFileWriter doesn't yet support Arrow array type '{array.GetType().Name}'."),
        };

        return WriteFlatPageAsync(
            name, logicalType, bytesPerValue, array.Length, valueBytes,
            ExtractValidityBitmap(array), array.NullCount, cancellationToken);
    }

    private static byte[]? ExtractValidityBitmap(IArrowArray array)
    {
        if (array.NullCount <= 0) return null;
        // Validity bitmap lives on Apache.Arrow.ArrayData.Buffers[0] for
        // every primitive/variable-length Arrow array. Spec: LSB-first,
        // bit set = valid.
        var span = array.Data.Buffers[0].Span;
        return span.IsEmpty ? null : span.ToArray();
    }

    /// <summary>
    /// Plans a multi-chunk Flat-encoded page. Thin wrapper over
    /// <see cref="WriteFixedWidthPageAsync"/>.
    /// </summary>
    private Task WriteFlatPageAsync(
        string name, string logicalType, int bytesPerValue,
        int numItems, byte[] valueBytes,
        byte[]? validityBitmap, int nullCount,
        CancellationToken cancellationToken)
        => WriteFixedWidthPageAsync(
            name, logicalType, bytesPerValue, FlatEncoding(bytesPerValue * 8),
            numItems, valueBytes, validityBitmap, nullCount, cancellationToken);

    /// <summary>
    /// Plans a multi-chunk page for any value encoding that lays out
    /// <paramref name="bytesPerItem"/>-byte items contiguously
    /// (Flat values, FSL rows, etc.). Picks the largest power-of-2 item
    /// count whose chunk fits in 32 KB, then carves the input value
    /// bytes into chunks of that size.
    /// </summary>
    private Task WriteFixedWidthPageAsync(
        string name, string logicalType, int bytesPerItem,
        CompressiveEncoding valueEncoding,
        int numItems, byte[] valueBytes,
        byte[]? validityBitmap, int nullCount,
        CancellationToken cancellationToken)
    {
        bool hasDef = nullCount > 0 && validityBitmap is not null;
        int chunkSize = LargestPow2FlatChunkSize(bytesPerItem, hasDef);

        var chunks = new List<(int itemStart, int itemCount, byte[] valueBuf)>();
        int idx = 0;
        // Fast path: if all items fit in a single chunk, emit one chunk
        // covering everything. The spec lets the LAST chunk have any item
        // count (only non-last chunks need power-of-2 sizes), so this is
        // valid even when numItems isn't a power of 2 — and lets us avoid
        // multi-chunk encoding for FSL (the reader's FSL path is single-
        // chunk only) and similar layouts.
        if (numItems > 0
            && ChunkTotalBytes(numItems, numItems * bytesPerItem, hasDef) <= MaxChunkBytes)
        {
            byte[] chunkValBytes = new byte[numItems * bytesPerItem];
            new ReadOnlySpan<byte>(valueBytes, 0, numItems * bytesPerItem)
                .CopyTo(chunkValBytes);
            chunks.Add((0, numItems, chunkValBytes));
        }
        else
        {
            while (idx < numItems)
            {
                int items = Math.Min(numItems - idx, chunkSize);
                byte[] chunkValBytes = new byte[items * bytesPerItem];
                new ReadOnlySpan<byte>(valueBytes, idx * bytesPerItem, items * bytesPerItem)
                    .CopyTo(chunkValBytes);
                chunks.Add((idx, items, chunkValBytes));
                idx += items;
            }
        }

        return WriteMultiChunkPageAsync(
            name, logicalType, valueEncoding, numItems, chunks,
            validityBitmap, nullCount, cancellationToken);
    }

    private Task WriteFixedSizeListColumnAsync(
        string name, FixedSizeListArray array, CancellationToken cancellationToken)
    {
        // v2.1 encodes FSL-of-primitive in a single column whose
        // value_compression is FixedSizeList { items_per_value, values=Flat }.
        // Each "item" from the chunker's perspective is one FSL row of
        // dim*inner_bytes wide; the row-level rep/def cascade is just
        // [ALL_VALID_ITEM] or [NULLABLE_ITEM]. Inner-element nulls would
        // require has_validity=true plus an inner-validity bitmap per
        // chunk — deferred (this is the simpler primitive-no-inner-nulls
        // path; pylance handles the inner-nulls path on its own writer
        // and our reader supports it).
        if (array.Data.DataType is not Apache.Arrow.Types.FixedSizeListType fslType)
            throw new InvalidOperationException(
                $"FixedSizeListArray has unexpected data type {array.Data.DataType}.");
        if (fslType.ValueDataType is not Apache.Arrow.Types.FixedWidthType innerFw)
            throw new NotSupportedException(
                $"LanceFileWriter only supports FixedSizeList of fixed-width primitives " +
                $"(got inner {fslType.ValueDataType}).");

        // Resolve the inner type's logical-type string so the schema
        // matches what the reader's LanceSchemaConverter expects.
        string innerLogical = innerFw switch
        {
            Apache.Arrow.Types.Int8Type => "int8",
            Apache.Arrow.Types.UInt8Type => "uint8",
            Apache.Arrow.Types.Int16Type => "int16",
            Apache.Arrow.Types.UInt16Type => "uint16",
            Apache.Arrow.Types.Int32Type => "int32",
            Apache.Arrow.Types.UInt32Type => "uint32",
            Apache.Arrow.Types.Int64Type => "int64",
            Apache.Arrow.Types.UInt64Type => "uint64",
            Apache.Arrow.Types.FloatType => "float",
            Apache.Arrow.Types.DoubleType => "double",
            _ => throw new NotSupportedException(
                $"FixedSizeList inner type {innerFw} is not yet supported by the writer."),
        };

        int dim = fslType.ListSize;
        int innerBytes = innerFw.BitWidth / 8;
        int bytesPerRow = dim * innerBytes;
        int numItems = array.Length;

        // Inner array's value buffer holds dim*numItems items concatenated
        // in row order. Apache.Arrow stores them on the child array's
        // Buffers[1] (Buffers[0] = inner validity bitmap, which we don't
        // emit for now since has_validity=false).
        var innerValuesBuf = array.Values.Data.Buffers[1];
        if (innerValuesBuf.IsEmpty)
            throw new LanceFormatException(
                "FixedSizeListArray inner values buffer is empty.");
        int totalBytes = checked(numItems * bytesPerRow);
        if (innerValuesBuf.Length < totalBytes)
            throw new LanceFormatException(
                $"FixedSizeListArray inner buffer {innerValuesBuf.Length} bytes < expected {totalBytes}.");
        var valueBytes = new byte[totalBytes];
        innerValuesBuf.Span.Slice(0, totalBytes).CopyTo(valueBytes);

        // Inner array nulls would force has_validity=true; reject for now
        // with a clear pointer (the reader-side cascade handles it).
        if (array.Values.NullCount > 0)
            throw new NotSupportedException(
                "FixedSizeListArray with inner-element nulls (has_validity=true) " +
                "is not yet supported by the writer.");

        var valueEncoding = new CompressiveEncoding
        {
            FixedSizeList = new Proto.Encodings.V21.FixedSizeList
            {
                ItemsPerValue = (ulong)dim,
                HasValidity = false,
                Values = new CompressiveEncoding
                {
                    Flat = new Proto.Encodings.V21.Flat { BitsPerValue = (ulong)(innerBytes * 8) },
                },
            },
        };
        string logicalType = $"fixed_size_list:{innerLogical}:{dim}";

        return WriteFixedWidthPageAsync(
            name, logicalType, bytesPerRow, valueEncoding,
            numItems, valueBytes,
            ExtractValidityBitmap(array), array.NullCount, cancellationToken);
    }

    private Task WriteVariableColumnAsync(
        string name, string logicalType, BinaryArray array,
        CancellationToken cancellationToken)
    {
        // Lance v2.1 Variable wire format (per chunk):
        //   value buffer = (chunkItems + 1) × u32 absolute offsets, then
        //                  concatenated data bytes
        // offsets[0] = (chunkItems + 1) * 4 (start of data section in this
        // chunk's value buffer); offsets[i+1] = offsets[i] + len(item i).
        // Multi-chunk: each chunk has its own self-contained offsets array
        // (relative within the chunk), so we just need to chunk by byte
        // budget. Non-last chunks must have power-of-2 item counts.
        int numItems = array.Length;
        bool hasDef = array.NullCount > 0;

        var chunks = new List<(int itemStart, int itemCount, byte[] valueBuf)>();
        int idx = 0;
        while (idx < numItems)
        {
            int items = 0;
            int dataBytes = 0;
            while (idx + items < numItems)
            {
                int newCount = items + 1;
                int newDataBytes = dataBytes + array.GetBytes(idx + items).Length;
                int valueBufLen = (newCount + 1) * sizeof(uint) + newDataBytes;
                int total = ChunkTotalBytes(newCount, valueBufLen, hasDef);
                if (total > MaxChunkBytes) break;
                items = newCount;
                dataBytes = newDataBytes;
            }
            if (items == 0)
                throw new InvalidOperationException(
                    $"Variable item at index {idx} alone exceeds the {MaxChunkBytes}-byte chunk limit.");
            bool isLast = idx + items == numItems;
            int finalItems = isLast ? items : Pow2Floor(items);
            byte[] valueBuf = BuildVariableChunkValueBuffer(array, idx, finalItems);
            chunks.Add((idx, finalItems, valueBuf));
            idx += finalItems;
        }

        var valueEncoding = new CompressiveEncoding
        {
            Variable = new Proto.Encodings.V21.Variable
            {
                Offsets = new CompressiveEncoding
                {
                    Flat = new Proto.Encodings.V21.Flat { BitsPerValue = 32 },
                },
                // values: leave unset = no further compression on the data bytes
            },
        };

        return WriteMultiChunkPageAsync(
            name, logicalType, valueEncoding, numItems, chunks,
            ExtractValidityBitmap(array), array.NullCount, cancellationToken);
    }

    private const int MaxChunkBytes = 4096 * MiniBlockAlignment; // 32 KiB; v2.1 chunk-meta divided_bytes is 12 bits

    private static int LargestPow2FlatChunkSize(int bytesPerValue, bool hasDef)
    {
        // Find largest 2^k such that the resulting chunk fits in MaxChunkBytes.
        for (int log2 = 15; log2 >= 1; log2--)
        {
            int items = 1 << log2;
            int valueBufLen = items * bytesPerValue;
            if (ChunkTotalBytes(items, valueBufLen, hasDef) <= MaxChunkBytes)
                return items;
        }
        return 1;
    }

    private static int ChunkTotalBytes(int items, int valueBufLen, bool hasDef)
    {
        int headerPadded = AlignUp(hasDef ? 6 : 4, MiniBlockAlignment);
        int defPadded = hasDef ? AlignUp(items * sizeof(ushort), MiniBlockAlignment) : 0;
        return headerPadded + defPadded + AlignUp(valueBufLen, MiniBlockAlignment);
    }

    private static int Pow2Floor(int n)
    {
        if (n < 1) return 1;
        int p = 1;
        while ((p << 1) <= n) p <<= 1;
        return p;
    }

    private static byte[] BuildVariableChunkValueBuffer(BinaryArray array, int itemStart, int itemCount)
    {
        int totalDataLen = 0;
        for (int i = 0; i < itemCount; i++)
            totalDataLen += array.GetBytes(itemStart + i).Length;
        int offsetsBytes = checked((itemCount + 1) * sizeof(uint));
        byte[] buf = new byte[offsetsBytes + totalDataLen];
        uint dataCursor = (uint)offsetsBytes;
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(0, sizeof(uint)), dataCursor);
        for (int i = 0; i < itemCount; i++)
        {
            var rowBytes = array.GetBytes(itemStart + i);
            rowBytes.CopyTo(buf.AsSpan((int)dataCursor));
            dataCursor += (uint)rowBytes.Length;
            BinaryPrimitives.WriteUInt32LittleEndian(
                buf.AsSpan((i + 1) * sizeof(uint), sizeof(uint)), dataCursor);
        }
        return buf;
    }

    /// <summary>
    /// Single-page wrapper around <see cref="BuildAndWritePageAsync"/> +
    /// <see cref="RegisterColumn"/> — the common path for the typed Write
    /// methods when each column is one page.
    /// </summary>
    private async Task WriteMultiChunkPageAsync(
        string name,
        string logicalType,
        CompressiveEncoding valueEncoding,
        int numItems,
        IReadOnlyList<(int itemStart, int itemCount, byte[] valueBuf)> chunks,
        byte[]? validityBitmap,
        int nullCount,
        CancellationToken cancellationToken)
    {
        ThrowIfFinalized();
        if (_totalRows < 0) _totalRows = numItems;
        else if (_totalRows != numItems)
            throw new ArgumentException(
                $"Column '{name}' has {numItems} rows but earlier columns have {_totalRows}.",
                nameof(numItems));

        var page = await BuildAndWritePageAsync(
            valueEncoding, numItems, chunks, validityBitmap, nullCount, cancellationToken)
            .ConfigureAwait(false);
        RegisterColumn(name, logicalType, valueEncoding, new[] { page });
    }

    /// <summary>
    /// Emits a multi-chunk MiniBlockLayout page for any value encoding with
    /// <c>num_buffers = 1</c>. Each entry in <paramref name="chunks"/>
    /// supplies that chunk's items range and its post-def value buffer
    /// (Flat: raw little-endian values; Variable: per-chunk
    /// <c>(items+1)</c> absolute u32 offsets followed by data bytes). The
    /// helper assembles each chunk's header + def bytes (if any) + value
    /// buffer, packs all chunks into buffer 1, and writes a u16-per-chunk
    /// metadata word into buffer 0. Non-last chunks must have power-of-2
    /// item counts (encoded as <c>log_num_values</c>); the last chunk's
    /// item count is recovered as <c>numItems - sum(prior chunks)</c>.
    ///
    /// <para>Returns a fully-populated <see cref="ColumnMetadata.Types.Page"/>
    /// the caller can attach to a column proto. Does NOT touch
    /// <see cref="_columns"/> / <see cref="_fields"/>; multi-page columns
    /// call this once per page and register the column at the end.</para>
    /// </summary>
    private async Task<ColumnMetadata.Types.Page> BuildAndWritePageAsync(
        CompressiveEncoding valueEncoding,
        int numItems,
        IReadOnlyList<(int itemStart, int itemCount, byte[] valueBuf)> chunks,
        byte[]? validityBitmap,
        int nullCount,
        CancellationToken cancellationToken)
    {
        bool hasDef = nullCount > 0 && validityBitmap is not null;
        int N = chunks.Count;
        int headerLen = hasDef ? 6 : 4;
        int headerPadded = AlignUp(headerLen, MiniBlockAlignment);

        byte[][] chunkBytes = new byte[N][];
        ushort[] metaWords = new ushort[N];
        long dataTotal = 0;

        for (int c = 0; c < N; c++)
        {
            var (itemStart, itemCount, chunkValueBuf) = chunks[c];
            int valueBufLen = chunkValueBuf.Length;
            int defByteLen = hasDef ? itemCount * sizeof(ushort) : 0;
            int defPadded = hasDef ? AlignUp(defByteLen, MiniBlockAlignment) : 0;
            int chunkTotal = AlignUp(headerPadded + defPadded + valueBufLen, MiniBlockAlignment);
            int dividedBytes = (chunkTotal / MiniBlockAlignment) - 1;
            if (dividedBytes < 0 || dividedBytes > 0xFFF)
                throw new InvalidOperationException(
                    $"Chunk {c} byte length {chunkTotal} cannot be encoded as a v2.1 mini-block word.");

            int logNumValues;
            bool isLast = c == N - 1;
            if (isLast)
            {
                // The reader spec says log_num_values is ignored for the last
                // chunk (size = items_in_page - prior_values), but pylance
                // always writes 0 there and its lance.dataset() decoder
                // appears to rely on that. Match the convention so v2.1
                // datasets we produce open via lance.dataset().
                logNumValues = 0;
            }
            else
            {
                int log2 = Log2Floor(itemCount);
                if ((1 << log2) != itemCount || log2 == 0)
                    throw new InvalidOperationException(
                        $"Non-last chunk {c} item count {itemCount} must be a power of 2 >= 2.");
                logNumValues = log2;
            }
            metaWords[c] = (ushort)((dividedBytes << 4) | (logNumValues & 0xF));

            byte[] cb = new byte[chunkTotal];
            int cursor = 0;
            // num_levels: for Flat/Variable item-shape chunks the reader
            // only needs this when there are def levels. For the FSL
            // value-compression path the reader instead asserts
            // num_levels == num_rows even without def — set it to
            // itemCount whenever the chunk is FSL-shaped (or has def).
            bool isFsl = valueEncoding.CompressionCase
                == CompressiveEncoding.CompressionOneofCase.FixedSizeList;
            BinaryPrimitives.WriteUInt16LittleEndian(
                cb.AsSpan(cursor, 2), checked((ushort)((hasDef || isFsl) ? itemCount : 0)));
            cursor += 2;
            if (hasDef)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(
                    cb.AsSpan(cursor, 2), checked((ushort)defByteLen));
                cursor += 2;
            }
            // Pylance writes valueBufSize ROUNDED UP to the 8-byte mini-block
            // alignment, not the unpadded value length. Its lance.dataset()
            // decoder reads the value buffer as a fixed-width slice and panics
            // when the declared size is not a multiple of the element width
            // (e.g. 4-byte u32 offsets for Variable encoding). Match the
            // convention so our datasets open in lance.dataset().
            int valueBufLenOnDisk = AlignUp(valueBufLen, MiniBlockAlignment);
            BinaryPrimitives.WriteUInt16LittleEndian(
                cb.AsSpan(cursor, 2), checked((ushort)valueBufLenOnDisk));

            if (hasDef)
            {
                Span<byte> defOut = cb.AsSpan(headerPadded, defByteLen);
                for (int i = 0; i < itemCount; i++)
                {
                    int globalIdx = itemStart + i;
                    bool valid = (validityBitmap![globalIdx >> 3] & (1 << (globalIdx & 7))) != 0;
                    BinaryPrimitives.WriteUInt16LittleEndian(
                        defOut.Slice(i * sizeof(ushort), sizeof(ushort)),
                        valid ? (ushort)0 : (ushort)1);
                }
            }
            chunkValueBuf.CopyTo(cb, headerPadded + defPadded);

            chunkBytes[c] = cb;
            dataTotal += chunkTotal;
        }

        // Buffer 0: one u16 chunk-meta word per chunk.
        byte[] metaBuf = new byte[checked(N * sizeof(ushort))];
        for (int c = 0; c < N; c++)
            BinaryPrimitives.WriteUInt16LittleEndian(metaBuf.AsSpan(c * sizeof(ushort), sizeof(ushort)), metaWords[c]);

        // Buffer 1: concatenated chunk bytes.
        byte[] dataBuf = new byte[checked((int)dataTotal)];
        int writePos = 0;
        for (int c = 0; c < N; c++)
        {
            chunkBytes[c].CopyTo(dataBuf, writePos);
            writePos += chunkBytes[c].Length;
        }

        // --- Write the page's two buffers, recording their offsets ---
        await PadToAlignmentAsync(PageBufferAlignment, cancellationToken).ConfigureAwait(false);
        long buf0Offset = _file.Position;
        await _file.WriteAsync(metaBuf, cancellationToken).ConfigureAwait(false);
        long buf0Size = metaBuf.Length;

        await PadToAlignmentAsync(PageBufferAlignment, cancellationToken).ConfigureAwait(false);
        long buf1Offset = _file.Position;
        await _file.WriteAsync(dataBuf, cancellationToken).ConfigureAwait(false);
        long buf1Size = dataBuf.Length;

        // --- Build the page's encoding (PageLayout → Any → DirectEncoding → Encoding) ---
        var miniBlock = new MiniBlockLayout
        {
            ValueCompression = valueEncoding,
            NumBuffers = 1,
            NumItems = (ulong)numItems,
        };
        if (hasDef)
        {
            // Reader expects Flat(16) for the def buffer per the v2.1 chunk
            // layout (each def level is a u16; 0 = valid, !=0 = null).
            miniBlock.DefCompression = new CompressiveEncoding
            {
                Flat = new Proto.Encodings.V21.Flat { BitsPerValue = 16 },
            };
            miniBlock.Layers.Add(RepDefLayer.RepdefNullableItem);
        }
        else
        {
            miniBlock.Layers.Add(RepDefLayer.RepdefAllValidItem);
        }

        var pageLayout = new PageLayout { MiniBlockLayout = miniBlock };

        var pageAny = new Any
        {
            // The reader doesn't validate this URL — it parses Any.value
            // directly as PageLayout. Use the upstream-style URL anyway so
            // a future tool that does check it sees the right thing.
            TypeUrl = "/lance.encodings21.PageLayout",
            Value = pageLayout.ToByteString(),
        };
        var pageEncoding = new Proto.V2.Encoding
        {
            Direct = new DirectEncoding { Encoding = pageAny.ToByteString() },
        };

        // --- Build the Page proto ---
        var page = new ColumnMetadata.Types.Page
        {
            Length = (ulong)numItems,
            Encoding = pageEncoding,
            Priority = 0,
        };
        page.BufferOffsets.Add((ulong)buf0Offset);
        page.BufferOffsets.Add((ulong)buf1Offset);
        page.BufferSizes.Add((ulong)buf0Size);
        page.BufferSizes.Add((ulong)buf1Size);
        return page;
    }

    private void RegisterColumn(
        string name, string logicalType, CompressiveEncoding valueEncoding,
        IEnumerable<ColumnMetadata.Types.Page> pages)
    {
        // --- Column-level encoding (lance-rs's reader unwraps this) ---
        // For non-blob columns the official writer emits ColumnEncoding {
        // column_encoding = Values(Empty) }, then wraps it in Any and
        // stores Any-bytes inside DirectEncoding.encoding. The wrapping
        // layers exactly mirror the per-page Encoding above.
        var columnEncodingProto = new ColumnEncoding { Values = new Empty() };
        var columnAny = new Any
        {
            TypeUrl = "/lance.encodings.ColumnEncoding",
            Value = columnEncodingProto.ToByteString(),
        };
        var columnEncoding = new Proto.V2.Encoding
        {
            Direct = new DirectEncoding { Encoding = columnAny.ToByteString() },
        };

        // --- Build the ColumnMetadata + accumulate the schema field ---
        var columnMeta = new ColumnMetadata { Encoding = columnEncoding };
        foreach (var page in pages) columnMeta.Pages.Add(page);
        _columns.Add(columnMeta);

        int fieldId = _fields.Count;
        // pylance fills in the deprecated v1 `encoding` and leaves `type` at
        // its proto3 default (0 = PARENT), even when writing v2.x files. The
        // Lance file reader on the dataset code path uses `encoding` as a
        // dispatch hint — strings/binary need VAR_BINARY or `lance.dataset()`
        // panics trying to interpret the offsets+data buffer as a fixed-
        // width slice. Match the convention so our datasets open in pylance.
        var fieldEncoding = valueEncoding.CompressionCase
                == CompressiveEncoding.CompressionOneofCase.Variable
            ? Proto.Encoding.VarBinary
            : Proto.Encoding.Plain;
        _fields.Add(new Proto.Field
        {
            // Type left at default (PARENT). pylance does the same; the
            // logical_type string is the real type discriminator.
            Name = name,
            Id = fieldId,
            ParentId = -1,
            LogicalType = logicalType,
            Nullable = true,
            Encoding = fieldEncoding,
        });
    }

    /// <summary>
    /// Multi-page Flat-encoded column. Each entry of
    /// <paramref name="valueChunksPerPage"/> is the raw little-endian byte
    /// buffer for one page's items. Pages are stitched into a single
    /// <see cref="ColumnMetadata"/> entry; the column's row count is the sum.
    /// </summary>
    private async Task WriteFlatPagedColumnAsync(
        string name, string logicalType, int bytesPerValue,
        IReadOnlyList<byte[]> valueChunksPerPage,
        CancellationToken cancellationToken)
    {
        ThrowIfFinalized();
        int totalItems = 0;
        foreach (var b in valueChunksPerPage) totalItems += b.Length / bytesPerValue;
        if (_totalRows < 0) _totalRows = totalItems;
        else if (_totalRows != totalItems)
            throw new ArgumentException(
                $"Column '{name}' has {totalItems} rows but earlier columns have {_totalRows}.",
                nameof(valueChunksPerPage));

        var encoding = FlatEncoding(bytesPerValue * 8);
        var pageProtos = new List<ColumnMetadata.Types.Page>(valueChunksPerPage.Count);

        foreach (byte[] valueBytes in valueChunksPerPage)
        {
            int numItems = valueBytes.Length / bytesPerValue;
            int chunkSize = LargestPow2FlatChunkSize(bytesPerValue, hasDef: false);
            var chunks = new List<(int, int, byte[])>();
            int idx = 0;
            while (idx < numItems)
            {
                int items = Math.Min(numItems - idx, chunkSize);
                byte[] chunkValBytes = new byte[items * bytesPerValue];
                new ReadOnlySpan<byte>(valueBytes, idx * bytesPerValue, items * bytesPerValue)
                    .CopyTo(chunkValBytes);
                chunks.Add((idx, items, chunkValBytes));
                idx += items;
            }

            var page = await BuildAndWritePageAsync(
                encoding, numItems, chunks, validityBitmap: null, nullCount: 0, cancellationToken)
                .ConfigureAwait(false);
            pageProtos.Add(page);
        }

        RegisterColumn(name, logicalType, encoding, pageProtos);
    }

    /// <summary>
    /// Finalises the file: writes column metadata blobs, the CMO table,
    /// the FileDescriptor (global buffer 0), the GBO table, and the
    /// 40-byte footer with <c>"LANC"</c> magic. Once called, the writer
    /// rejects further <see cref="WriteInt32ColumnAsync"/> calls.
    /// </summary>
    public async Task FinishAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfFinalized();
        if (_columns.Count == 0)
            throw new InvalidOperationException(
                "Cannot finalise a Lance file with no columns.");

        // The order below is mandatory: lance-rs's reader assumes
        // `column_meta_start >= schema_start` and computes
        // `column_meta_start - schema_start` as a u64, panicking on
        // underflow. So the FileDescriptor (global buffer 0) MUST come
        // before the column metadata blobs.

        // --- FileDescriptor (global buffer 0) ---
        // Global buffer positions are 64-byte alignment-checked by the
        // reader for v2.1 (see do_decode_gbo_table). Page writes already
        // pad to PageBufferAlignment after each buffer, so the position
        // here is normally already 64-aligned, but be defensive.
        var fileDescriptor = new FileDescriptor
        {
            Schema = new Proto.Schema(),
            Length = checked((ulong)Math.Max(0L, _totalRows)),
        };
        foreach (var f in _fields)
            fileDescriptor.Schema.Fields.Add(f);

        await PadToAlignmentAsync(PageBufferAlignment, cancellationToken).ConfigureAwait(false);
        long fdOffset = _file.Position;
        byte[] fdBytes = fileDescriptor.ToByteArray();
        await _file.WriteAsync(fdBytes, cancellationToken).ConfigureAwait(false);

        // --- Column metadata blobs (back-to-back, no padding) ---
        long columnMetaStart = _file.Position;
        var columnEntries = new OffsetSizeEntry[_columns.Count];
        for (int i = 0; i < _columns.Count; i++)
        {
            long pos = _file.Position;
            byte[] bytes = _columns[i].ToByteArray();
            await _file.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
            columnEntries[i] = new OffsetSizeEntry(pos, bytes.Length);
        }

        // --- CMO table ---
        long cmoTableOffset = _file.Position;
        {
            byte[] cmoTable = new byte[_columns.Count * OffsetSizeEntry.Bytes];
            for (int i = 0; i < _columns.Count; i++)
                columnEntries[i].WriteTo(cmoTable.AsSpan(i * OffsetSizeEntry.Bytes));
            await _file.WriteAsync(cmoTable, cancellationToken).ConfigureAwait(false);
        }

        // --- GBO table (single entry: FileDescriptor) ---
        long gboTableOffset = _file.Position;
        {
            byte[] gboTable = new byte[OffsetSizeEntry.Bytes];
            new OffsetSizeEntry(fdOffset, fdBytes.Length).WriteTo(gboTable);
            await _file.WriteAsync(gboTable, cancellationToken).ConfigureAwait(false);
        }

        // --- Footer (40 bytes) ---
        var footer = new LanceFooter(
            ColumnMetaStart: columnMetaStart,
            CmoTableOffset: cmoTableOffset,
            GboTableOffset: gboTableOffset,
            NumGlobalBuffers: 1,
            NumColumns: _columns.Count,
            Version: _version);
        byte[] footerBytes = new byte[LanceFooter.Size];
        footer.WriteTo(footerBytes);
        await _file.WriteAsync(footerBytes, cancellationToken).ConfigureAwait(false);

        await _file.FlushAsync(cancellationToken).ConfigureAwait(false);
        _finalSizeBytes = _file.Position;
        _finalized = true;
    }

    private async ValueTask PadToAlignmentAsync(int alignment, CancellationToken cancellationToken)
    {
        long pos = _file.Position;
        long aligned = (pos + (alignment - 1)) & ~(alignment - 1L);
        int padBytes = checked((int)(aligned - pos));
        if (padBytes == 0) return;
        byte[] zeros = new byte[padBytes];
        await _file.WriteAsync(zeros, cancellationToken).ConfigureAwait(false);
    }

    private static int Log2Floor(int n)
    {
        if (n <= 1) return 0;
        int log = 0;
        int v = n;
        while (v > 1) { v >>= 1; log++; }
        return log;
    }

    private static int AlignUp(int value, int alignment) =>
        (value + (alignment - 1)) & ~(alignment - 1);

    private void ThrowIfFinalized()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_finalized)
            throw new InvalidOperationException(
                "LanceFileWriter has already been finalised; no further writes are allowed.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        if (_ownsFile)
            await _file.DisposeAsync().ConfigureAwait(false);
    }
}
