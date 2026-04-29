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
    /// Plans a multi-chunk Flat-encoded page. Picks the largest power-of-2
    /// item count whose chunk fits in 32 KB, then carves the input value
    /// bytes into chunks of that size (last chunk holds the remainder).
    /// </summary>
    private Task WriteFlatPageAsync(
        string name, string logicalType, int bytesPerValue,
        int numItems, byte[] valueBytes,
        byte[]? validityBitmap, int nullCount,
        CancellationToken cancellationToken)
    {
        bool hasDef = nullCount > 0 && validityBitmap is not null;
        int chunkSize = LargestPow2FlatChunkSize(bytesPerValue, hasDef);

        var chunks = new List<(int itemStart, int itemCount, byte[] valueBuf)>();
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

        return WriteMultiChunkPageAsync(
            name, logicalType, FlatEncoding(bytesPerValue * 8), numItems, chunks,
            validityBitmap, nullCount, cancellationToken);
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
            BinaryPrimitives.WriteUInt16LittleEndian(
                cb.AsSpan(cursor, 2), checked((ushort)(hasDef ? itemCount : 0)));
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
        columnMeta.Pages.Add(page);
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
            // pylance marks top-level fields with parent_id = -1 (the
            // "no parent" sentinel). Setting parent_id = 0 would make the
            // second top-level field look like a child of the first
            // because the IsRoot check in our reader treats any
            // existing-field-id as a real reference.
            ParentId = -1,
            LogicalType = logicalType,
            // pylance writes nullable=true in the schema even when the
            // Arrow column is non-nullable; the actual presence of nulls is
            // encoded by the page's RepDefLayer (ALL_VALID_ITEM vs
            // NULLABLE_ITEM). Setting nullable=false here makes lance-rs
            // interpret the schema in a way that doesn't match the layer
            // shape we emit, so it panics on read.
            Nullable = true,
            Encoding = fieldEncoding,
        });
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
