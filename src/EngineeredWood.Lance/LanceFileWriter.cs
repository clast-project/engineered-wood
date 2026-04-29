// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
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
/// one or more non-nullable fixed-width primitive columns
/// (<see cref="sbyte"/>, <see cref="short"/>, <see cref="int"/>,
/// <see cref="long"/> and unsigned counterparts, <see cref="float"/>,
/// <see cref="double"/>) with all rows packed into a single
/// MiniBlockLayout page per column using <see cref="Flat"/> value
/// compression.
///
/// <para><b>What this writer covers</b>:</para>
/// <list type="bullet">
///   <item>v2.1 file envelope: page buffers → column metadata blobs →
///   CMO table → FileDescriptor (global buffer 0) → GBO table →
///   40-byte footer + <c>"LANC"</c> magic.</item>
///   <item><see cref="MiniBlockLayout"/> with <c>num_buffers=1</c>,
///   <c>layers=[ALL_VALID_ITEM]</c>, <c>value_compression=Flat(32)</c>,
///   single chunk, no rep, no def.</item>
///   <item>Schema: top-level <c>int32</c> leaves with sequential field
///   ids. Matches what pylance writes for primitive int32 columns.</item>
/// </list>
///
/// <para><b>What's deferred</b>:</para>
/// <list type="bullet">
///   <item>Booleans (bit-packed encoding, not byte-aligned).</item>
///   <item>Nullability (no def buffer yet).</item>
///   <item>Strings, binary, lists, structs, FSL.</item>
///   <item>Other encodings (Variable, FullZip, Bitpacking, FSST,
///   Dictionary, ByteStreamSplit, ...).</item>
///   <item>Multi-page columns (large data → page splits).</item>
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
    private bool _finalized;
    private bool _disposed;

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

    /// <summary>Append a non-nullable <see cref="sbyte"/> column.</summary>
    public Task WriteInt8ColumnAsync(string name, ReadOnlySpan<sbyte> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "int8", 8, values.Length, MemoryMarshal.AsBytes(values).ToArray(), cancellationToken);

    /// <summary>Append a non-nullable <see cref="byte"/> column.</summary>
    public Task WriteUInt8ColumnAsync(string name, ReadOnlySpan<byte> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "uint8", 8, values.Length, values.ToArray(), cancellationToken);

    /// <summary>Append a non-nullable <see cref="short"/> column.</summary>
    public Task WriteInt16ColumnAsync(string name, ReadOnlySpan<short> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "int16", 16, values.Length, MemoryMarshal.AsBytes(values).ToArray(), cancellationToken);

    /// <summary>Append a non-nullable <see cref="ushort"/> column.</summary>
    public Task WriteUInt16ColumnAsync(string name, ReadOnlySpan<ushort> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "uint16", 16, values.Length, MemoryMarshal.AsBytes(values).ToArray(), cancellationToken);

    /// <summary>
    /// Append a non-nullable <see cref="int"/> column to the file.
    /// Every column added must have the same length as the first; the
    /// length becomes the file's row count.
    /// </summary>
    public Task WriteInt32ColumnAsync(string name, ReadOnlySpan<int> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "int32", 32, values.Length, MemoryMarshal.AsBytes(values).ToArray(), cancellationToken);

    /// <summary>Append a non-nullable <see cref="uint"/> column.</summary>
    public Task WriteUInt32ColumnAsync(string name, ReadOnlySpan<uint> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "uint32", 32, values.Length, MemoryMarshal.AsBytes(values).ToArray(), cancellationToken);

    /// <summary>Append a non-nullable <see cref="long"/> column.</summary>
    public Task WriteInt64ColumnAsync(string name, ReadOnlySpan<long> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "int64", 64, values.Length, MemoryMarshal.AsBytes(values).ToArray(), cancellationToken);

    /// <summary>Append a non-nullable <see cref="ulong"/> column.</summary>
    public Task WriteUInt64ColumnAsync(string name, ReadOnlySpan<ulong> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "uint64", 64, values.Length, MemoryMarshal.AsBytes(values).ToArray(), cancellationToken);

    /// <summary>Append a non-nullable <see cref="float"/> (Float32) column.</summary>
    public Task WriteFloatColumnAsync(string name, ReadOnlySpan<float> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "float", 32, values.Length, MemoryMarshal.AsBytes(values).ToArray(), cancellationToken);

    /// <summary>Append a non-nullable <see cref="double"/> (Float64) column.</summary>
    public Task WriteDoubleColumnAsync(string name, ReadOnlySpan<double> values, CancellationToken cancellationToken = default)
        => WriteFlatPrimitiveColumnAsync(name, "double", 64, values.Length, MemoryMarshal.AsBytes(values).ToArray(), cancellationToken);

    /// <summary>
    /// Single-page MiniBlockLayout + Flat encoding for any byte-aligned
    /// fixed-width primitive. The caller serializes values to a
    /// little-endian byte buffer (one item per <paramref name="bitsPerValue"/>
    /// bits) and passes the row count + Lance logical-type string
    /// (e.g. <c>"int32"</c>, <c>"float"</c>, <c>"double"</c>).
    /// </summary>
    private async Task WriteFlatPrimitiveColumnAsync(
        string name,
        string logicalType,
        int bitsPerValue,
        int numItems,
        byte[] valueBytes,
        CancellationToken cancellationToken)
    {
        ThrowIfFinalized();
        if (_totalRows < 0) _totalRows = numItems;
        else if (_totalRows != numItems)
            throw new ArgumentException(
                $"Column '{name}' has {numItems} rows but earlier columns have {_totalRows}.",
                nameof(numItems));

        // --- Build the page's two buffers (chunk metadata + chunk data) ---
        // Chunk inner layout for ALL_VALID_ITEM, no rep, no def:
        //   u16 num_levels (=0)
        //   u16 valueBufSize
        //   pad to 8-byte alignment
        //   value bytes (length × bitsPerValue/8)
        //   pad to 8-byte alignment so divided_bytes is well-defined
        int valueByteLen = valueBytes.Length;
        const int chunkHeaderLen = 4;  // 2 (num_levels) + 2 (valueBufSize)
        int chunkHeaderPadded = AlignUp(chunkHeaderLen, MiniBlockAlignment);
        int chunkBeforePad = chunkHeaderPadded + valueByteLen;
        int chunkTotal = AlignUp(chunkBeforePad, MiniBlockAlignment);
        int dividedBytes = (chunkTotal / MiniBlockAlignment) - 1;
        if (dividedBytes < 0 || dividedBytes > 0xFFF)
            throw new InvalidOperationException(
                $"Chunk byte length {chunkTotal} cannot be encoded as a v2.1 mini-block word.");

        // Buffer 0: chunk metadata. v2.1 uses u16 words: low 4 bits =
        // log_num_values (ignored for the last chunk; here the only chunk
        // is also the last), high 12 bits = divided_bytes.
        byte[] chunkMeta = new byte[2];
        ushort word = (ushort)((dividedBytes << 4) | 0);
        BinaryPrimitives.WriteUInt16LittleEndian(chunkMeta.AsSpan(0, 2), word);

        // Buffer 1: chunk data.
        byte[] chunkData = new byte[chunkTotal];
        BinaryPrimitives.WriteUInt16LittleEndian(chunkData.AsSpan(0, 2), 0);                   // num_levels
        BinaryPrimitives.WriteUInt16LittleEndian(chunkData.AsSpan(2, 2), checked((ushort)valueByteLen));
        valueBytes.CopyTo(chunkData.AsSpan(chunkHeaderPadded));
        // trailing padding already zero

        // --- Write the buffers, recording their absolute file offsets ---
        await PadToAlignmentAsync(PageBufferAlignment, cancellationToken).ConfigureAwait(false);
        long buf0Offset = _file.Position;
        await _file.WriteAsync(chunkMeta, cancellationToken).ConfigureAwait(false);
        long buf0Size = chunkMeta.Length;

        await PadToAlignmentAsync(PageBufferAlignment, cancellationToken).ConfigureAwait(false);
        long buf1Offset = _file.Position;
        await _file.WriteAsync(chunkData, cancellationToken).ConfigureAwait(false);
        long buf1Size = chunkData.Length;

        // --- Build the page's encoding (PageLayout → Any → DirectEncoding → Encoding) ---
        var miniBlock = new MiniBlockLayout
        {
            ValueCompression = new CompressiveEncoding
            {
                Flat = new Proto.Encodings.V21.Flat { BitsPerValue = (ulong)bitsPerValue },
            },
            NumBuffers = 1,
            NumItems = (ulong)numItems,
        };
        miniBlock.Layers.Add(RepDefLayer.RepdefAllValidItem);

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
        _fields.Add(new Proto.Field
        {
            Type = Proto.Field.Types.Type.Leaf,
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
