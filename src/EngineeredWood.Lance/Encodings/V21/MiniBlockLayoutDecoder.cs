// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.Lance.Proto.Encodings.V21;

namespace EngineeredWood.Lance.Encodings.V21;

/// <summary>
/// Decodes a Lance v2.1 <see cref="MiniBlockLayout"/> page.
///
/// <para>Page-level buffer layout:</para>
/// <list type="bullet">
///   <item>
///     <term>buffer 0</term>
///     <description>Chunk metadata: packed words, one per chunk. In v2.1
///     each word is a little-endian u16 (v2.2 widens to u32 when
///     <c>has_large_chunk = true</c>).</description>
///   </item>
///   <item>
///     <term>buffer 1</term>
///     <description>Chunk data: all chunks concatenated.</description>
///   </item>
///   <item>
///     <term>buffer 2 (conditional)</term>
///     <description>Repetition index (only when
///     <c>repetition_index_depth &gt; 0</c>, i.e. list columns).</description>
///   </item>
/// </list>
///
/// <para>Each chunk metadata word packs <c>log_num_values</c> in the low 4
/// bits (the number of values is <c>1 &lt;&lt; log_num_values</c>, or
/// <c>items_in_page - prior_values</c> for the last chunk), and
/// <c>divided_bytes</c> in the high bits; the chunk byte length is
/// <c>(divided_bytes + 1) * 8</c> (<see cref="MiniBlockAlignment"/>).</para>
///
/// <para>Each chunk's internal layout (v2.1):</para>
/// <list type="number">
///   <item>u16 <c>num_levels</c> (rep/def level count — 0 when neither rep
///   nor def is present).</item>
///   <item>u16 <c>rep_size</c> (only if <c>rep_compression</c> is set).</item>
///   <item>u16 <c>def_size</c> (only if <c>def_compression</c> is set).</item>
///   <item><c>num_buffers × u16</c> value buffer sizes.</item>
///   <item>Pad to 8-byte alignment.</item>
///   <item>Rep buffer (padded to 8).</item>
///   <item>Def buffer (padded to 8).</item>
///   <item>Value buffers (each padded to 8).</item>
/// </list>
///
/// <para>Phase 6 scope: <c>num_buffers = 1</c> with
/// <see cref="CompressiveEncoding"/> = <see cref="Flat"/>. A single
/// <see cref="RepDefLayer"/> of <c>ALL_VALID_ITEM</c> or
/// <c>NULLABLE_ITEM</c>. No repetition (lists belong to Phase 7).</para>
/// </summary>
internal static class MiniBlockLayoutDecoder
{
    private const int MiniBlockAlignment = 8;

    public static IArrowArray Decode(
        MiniBlockLayout layout, IArrowType targetType, in PageContext context)
    {
        ValidateScope(layout, targetType);

        long numItems = checked((long)layout.NumItems);
        bool hasLargeChunk = layout.HasLargeChunk;
        bool hasDef = layout.DefCompression is not null;
        bool hasRep = layout.RepCompression is not null;

        ReadOnlySpan<byte> chunkMeta = context.PageBuffers[0].Span;
        ReadOnlySpan<byte> chunkData = context.PageBuffers[1].Span;
        var chunks = ParseChunkMetadata(chunkMeta, hasLargeChunk, numItems);

        // Dispatch on the shape of the output Arrow array.
        CompressiveEncoding valueEnc = layout.ValueCompression
            ?? throw new LanceFormatException("MiniBlockLayout has no value_compression.");

        // Layout-level dictionary support (low-cardinality strings/binary).
        // The dictionary buffer is an EXTRA page buffer past the chunk meta +
        // chunk data buffers. The chunks themselves carry indices into the
        // dictionary; we then materialise.
        if (layout.Dictionary is not null)
        {
            return DecodeDictionaryMiniBlock(
                chunkData, chunks, hasDef, hasLargeChunk, layout, targetType,
                context, numItems);
        }

        // Unwrap General(ZSTD, inner) — applied at the per-chunk value-buffer
        // level for MiniBlock layouts (per the encodings_v2_1.proto comment:
        // "If we apply it to mini-block data then we compress entire mini-blocks").
        // Each chunk's value bytes are a single ZSTD frame whose decompressed
        // output is then processed by the inner encoding (Flat / InlineBitpacking).
        // Pylance hasn't been observed emitting this; we accept it for
        // forward-compat with future Lance writers.
        bool generalZstd = false;
        if (valueEnc.CompressionCase == CompressiveEncoding.CompressionOneofCase.General)
        {
            valueEnc = UnwrapGeneralZstd(valueEnc.General);
            generalZstd = true;
        }

        if (targetType is Apache.Arrow.Types.ListType listType)
        {
            if (generalZstd)
                throw new NotImplementedException(
                    "MiniBlockLayout General(ZSTD) wrapping is not yet supported for list columns.");
            return DecodeListMiniBlock(
                chunkData, chunks, hasDef, hasRep, hasLargeChunk,
                layout.Layers, listType, valueEnc);
        }
        if (targetType is StringType or BinaryType)
        {
            if (generalZstd)
                throw new NotImplementedException(
                    "MiniBlockLayout General(ZSTD) wrapping is not yet supported for string/binary columns.");
            // FSST-compressed strings: build a decoder from the page-level
            // symbol table (Lance's container format), parse chunks via the
            // inner Variable encoding to get per-row compressed slices, then
            // bulk-decompress.
            if (valueEnc.CompressionCase == CompressiveEncoding.CompressionOneofCase.Fsst)
            {
                return DecodeFsstVariableMiniBlock(
                    chunkData, chunks, hasDef, hasLargeChunk,
                    valueEnc.Fsst, layout.DefCompression, targetType, numItems);
            }
            return DecodeVariableMiniBlock(
                chunkData, chunks, hasDef, hasLargeChunk,
                valueEnc, layout.DefCompression, targetType, numItems);
        }
        return DecodeFixedWidthMiniBlock(
            chunkData, chunks, hasDef, hasLargeChunk,
            valueEnc, layout.DefCompression, targetType, numItems, generalZstd);
    }

    /// <summary>
    /// Validate and unwrap a <see cref="General"/> wrapper around an inner
    /// encoding. Currently only ZSTD is supported as the wrapping scheme.
    /// </summary>
    private static CompressiveEncoding UnwrapGeneralZstd(General general)
    {
        if (general.Compression?.Scheme != CompressionScheme.CompressionAlgorithmZstd)
            throw new NotImplementedException(
                $"MiniBlockLayout General wrapping with scheme '{general.Compression?.Scheme}' " +
                "is not yet supported (only ZSTD).");
        if (general.Values is null)
            throw new LanceFormatException(
                "MiniBlockLayout General wrapper has no inner values encoding.");
        return general.Values;
    }

    private static void ValidateScope(MiniBlockLayout layout, IArrowType targetType)
    {
        if (layout.NumBuffers != 1)
            throw new NotImplementedException(
                $"MiniBlockLayout with num_buffers={layout.NumBuffers} is not yet supported.");

        // Lists have 2 layers [leaf, list]. Primitives and strings have 1 layer.
        bool isList = targetType is Apache.Arrow.Types.ListType or LargeListType;
        int expectedLayers = isList ? 2 : 1;
        if (layout.Layers.Count != expectedLayers)
            throw new NotImplementedException(
                $"MiniBlockLayout with {layout.Layers.Count} layers for target {targetType} is not yet supported.");

        if (!isList)
        {
            RepDefLayer layer = layout.Layers[0];
            if (layer != RepDefLayer.RepdefAllValidItem && layer != RepDefLayer.RepdefNullableItem)
                throw new NotImplementedException(
                    $"MiniBlockLayout RepDefLayer '{layer}' is not yet supported for {targetType}.");
        }
        else
        {
            RepDefLayer leafLayer = layout.Layers[0];
            if (leafLayer != RepDefLayer.RepdefAllValidItem && leafLayer != RepDefLayer.RepdefNullableItem)
                throw new NotImplementedException(
                    $"List leaf RepDefLayer '{leafLayer}' is not yet supported.");
            RepDefLayer listLayer = layout.Layers[1];
            if (listLayer != RepDefLayer.RepdefAllValidList
                && listLayer != RepDefLayer.RepdefNullableList
                && listLayer != RepDefLayer.RepdefEmptyableList
                && listLayer != RepDefLayer.RepdefNullAndEmptyList)
                throw new NotImplementedException(
                    $"List outer RepDefLayer '{listLayer}' is not yet supported.");
        }
    }

    private static IArrowArray DecodeFixedWidthMiniBlock(
        ReadOnlySpan<byte> chunkData, List<MiniChunk> chunks,
        bool hasDef, bool hasLargeChunk,
        CompressiveEncoding valueEnc, CompressiveEncoding? defComp,
        IArrowType targetType, long numItems, bool generalZstd = false)
    {
        int length = checked((int)numItems);

        // Phase 9: support InlineBitpacking (Fastlanes). Per-chunk bit width
        // header is read inside DecodeFixedWidthChunk.
        bool inlineBitpacking = valueEnc.CompressionCase
            == CompressiveEncoding.CompressionOneofCase.InlineBitpacking;

        int valueBytesPerItem = inlineBitpacking
            ? ResolveInlineBitpackingBytesPerValue(valueEnc.InlineBitpacking, targetType)
            : ResolveFlatBytesPerValue(valueEnc, targetType);

        byte[] valueBytes = new byte[checked(length * valueBytesPerItem)];
        byte[]? validityBitmap = hasDef ? new byte[(length + 7) / 8] : null;
        int nullCount = 0;

        // Scratch buffer for ZSTD-decompressed value bytes when General(ZSTD)
        // wraps the inner encoding. Reused across chunks; reallocated only
        // when a chunk's decompressed size exceeds the current capacity.
        byte[]? decompressScratch = generalZstd ? new byte[1024 * valueBytesPerItem] : null;

        long globalChunkByteOffset = 0;
        int valueCursor = 0;
        foreach (MiniChunk chunk in chunks)
        {
            ReadOnlySpan<byte> chunkBytes = chunkData.Slice(
                (int)globalChunkByteOffset, (int)chunk.SizeBytes);

            int itemsInChunk = (int)chunk.NumValues;
            DecodeFixedWidthChunk(
                chunkBytes, itemsInChunk, hasRep: false, hasDef, hasLargeChunk,
                valueBytesPerItem, valueBytes, valueCursor,
                validityBitmap, valueCursor, out int chunkNullCount,
                inlineBitpacking, defComp,
                generalZstd, ref decompressScratch);

            nullCount += chunkNullCount;
            valueCursor += itemsInChunk;
            globalChunkByteOffset += (long)chunk.SizeBytes;
        }

        return BuildFixedWidthArray(targetType, length, valueBytes, validityBitmap, nullCount);
    }

    private static int ResolveInlineBitpackingBytesPerValue(
        InlineBitpacking encoding, IArrowType targetType)
    {
        ulong uncompressedBits = encoding.UncompressedBitsPerValue;
        if (uncompressedBits != 8 && uncompressedBits != 16
            && uncompressedBits != 32 && uncompressedBits != 64)
            throw new NotImplementedException(
                $"InlineBitpacking uncompressed_bits_per_value={uncompressedBits} is not supported (only 8/16/32/64).");

        if (targetType is FixedWidthType fw && (ulong)fw.BitWidth != uncompressedBits)
            throw new LanceFormatException(
                $"InlineBitpacking uncompressed_bits={uncompressedBits} does not match target {targetType} ({fw.BitWidth}).");

        if (encoding.Values is not null)
            throw new NotImplementedException(
                "InlineBitpacking with BufferCompression on values is not yet supported.");

        return (int)(uncompressedBits / 8);
    }

    private static int ResolveFlatBytesPerValue(CompressiveEncoding encoding, IArrowType targetType)
    {
        if (encoding is null)
            throw new LanceFormatException(
                "MiniBlockLayout has no value_compression.");
        if (encoding.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat)
            throw new NotImplementedException(
                $"CompressiveEncoding '{encoding.CompressionCase}' is not yet supported (Phase 6 handles Flat).");

        ulong bits = encoding.Flat.BitsPerValue;
        if (encoding.Flat.Data is not null)
            throw new NotImplementedException(
                "CompressiveEncoding.Flat with BufferCompression is not yet supported.");

        if (bits % 8 != 0 || bits == 0)
            throw new NotImplementedException(
                $"CompressiveEncoding.Flat bits_per_value={bits} (non-byte-aligned) is not yet supported.");

        if (targetType is FixedWidthType fw)
        {
            if ((ulong)fw.BitWidth != bits)
                throw new LanceFormatException(
                    $"Flat bits_per_value={bits} does not match target type {targetType} width ({fw.BitWidth}).");
        }
        else
        {
            throw new NotImplementedException(
                $"MiniBlockLayout.Flat cannot yet target non-fixed-width type {targetType}.");
        }

        return (int)(bits / 8);
    }

    private readonly record struct MiniChunk(ulong NumValues, ulong SizeBytes);

    private static List<MiniChunk> ParseChunkMetadata(
        ReadOnlySpan<byte> meta, bool hasLargeChunk, long totalItems)
    {
        int wordSize = hasLargeChunk ? 4 : 2;
        if (meta.Length % wordSize != 0)
            throw new LanceFormatException(
                $"Chunk metadata buffer length {meta.Length} is not a multiple of {wordSize}.");

        int wordCount = meta.Length / wordSize;
        var chunks = new List<MiniChunk>(wordCount);

        long runningValues = 0;
        for (int i = 0; i < wordCount; i++)
        {
            uint word = hasLargeChunk
                ? BinaryPrimitives.ReadUInt32LittleEndian(meta.Slice(i * 4, 4))
                : BinaryPrimitives.ReadUInt16LittleEndian(meta.Slice(i * 2, 2));

            int logNumValues = (int)(word & 0xF);
            uint dividedBytes = word >> 4;
            ulong sizeBytes = checked((ulong)(dividedBytes + 1) * MiniBlockAlignment);

            ulong numValues;
            bool isLast = i == wordCount - 1;
            if (isLast)
            {
                numValues = (ulong)(totalItems - runningValues);
            }
            else
            {
                if (logNumValues == 0)
                    throw new LanceFormatException(
                        $"Non-last chunk {i} has log_num_values=0.");
                numValues = 1UL << logNumValues;
            }

            chunks.Add(new MiniChunk(numValues, sizeBytes));
            runningValues += (long)numValues;
        }

        if (runningValues != totalItems)
            throw new LanceFormatException(
                $"Chunks sum to {runningValues} values but layout declares num_items={totalItems}.");

        return chunks;
    }

    private static void DecodeFixedWidthChunk(
        ReadOnlySpan<byte> chunkBytes, int itemsInChunk,
        bool hasRep, bool hasDef, bool hasLargeChunk,
        int valueBytesPerItem,
        byte[] valueOutput, int valueOutputItemStart,
        byte[]? validityBitmap, int bitmapStartItem,
        out int chunkNullCount,
        bool inlineBitpacking,
        CompressiveEncoding? defComp,
        bool generalZstd,
        ref byte[]? decompressScratch)
    {
        chunkNullCount = 0;

        int headerOffset = 0;
        int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(headerOffset, 2));
        headerOffset += 2;

        int repSize = 0;
        if (hasRep)
        {
            repSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(headerOffset, 2));
            headerOffset += 2;
        }

        int defSize = 0;
        if (hasDef)
        {
            defSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(headerOffset, 2));
            headerOffset += 2;
        }

        // Phase 6 supports num_buffers = 1 → one buffer size.
        int bufSizeWidth = hasLargeChunk ? 4 : 2;
        int valueBufSize = hasLargeChunk
            ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(headerOffset, 4))
            : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(headerOffset, 2));
        headerOffset += bufSizeWidth;

        int cursor = AlignUp(headerOffset, MiniBlockAlignment);

        // Rep buffer (not present in Phase 6 scope).
        if (hasRep)
        {
            cursor += repSize;
            cursor = AlignUp(cursor, MiniBlockAlignment);
        }

        // Def buffer — supports both Flat(u16) and InlineBitpacking(16) via
        // the shared ReadDefChunkBuffer helper. When validityBitmap is null
        // (e.g. the dictionary-indices path tracks nullity separately) we
        // still parse the def buffer to advance the cursor but skip the
        // bitmap write.
        if (hasDef)
        {
            if (defComp is null)
                throw new InvalidOperationException(
                    "DecodeFixedWidthChunk called with hasDef=true but defComp=null.");
            if (validityBitmap is not null)
            {
                ushort[] defValues = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, defSize), itemsInChunk, defComp);
                for (int i = 0; i < itemsInChunk; i++)
                {
                    int globalItem = bitmapStartItem + i;
                    if (defValues[i] == 0)
                        validityBitmap[globalItem >> 3] |= (byte)(1 << (globalItem & 7));
                    else
                        chunkNullCount++;
                }
            }
            cursor += defSize;
            cursor = AlignUp(cursor, MiniBlockAlignment);
        }
        else
        {
            // ALL_VALID_ITEM: all bits are implicitly valid. No bitmap to fill.
        }

        // Value buffer. Either Flat (passthrough) or InlineBitpacking
        // (Fastlanes-decoded via Clast.FastLanes.BitPacking). When the value
        // encoding is wrapped in General(ZSTD), the on-disk value bytes are a
        // ZSTD frame; we decompress to the scratch buffer first and then run
        // the inner encoding's logic over the decompressed bytes.
        ReadOnlySpan<byte> innerValueBuf;
        int innerValueBufSize;
        if (generalZstd)
        {
            ReadOnlySpan<byte> compressed = chunkBytes.Slice(cursor, valueBufSize);
            int expectedDecompSize = inlineBitpacking
                ? valueBytesPerItem + (1024 * valueBytesPerItem) // bit-width header + max packed area
                : itemsInChunk * valueBytesPerItem;
            if (decompressScratch is null || decompressScratch.Length < expectedDecompSize)
                decompressScratch = new byte[expectedDecompSize];
            int actualDecomp = Decompressor.Decompress(
                CompressionCodec.Zstd, compressed,
                decompressScratch.AsSpan(0, expectedDecompSize));
            innerValueBuf = decompressScratch.AsSpan(0, actualDecomp);
            innerValueBufSize = actualDecomp;
        }
        else
        {
            innerValueBuf = chunkBytes.Slice(cursor, valueBufSize);
            innerValueBufSize = valueBufSize;
        }

        if (inlineBitpacking)
        {
            DecodeInlineBitpackingChunk(
                innerValueBuf, itemsInChunk, valueBytesPerItem,
                valueOutput, valueOutputItemStart);
        }
        else
        {
            int valueBytes = itemsInChunk * valueBytesPerItem;
            if (innerValueBufSize < valueBytes)
                throw new LanceFormatException(
                    $"Chunk value buffer size {innerValueBufSize} smaller than expected {valueBytes}.");

            int destOffset = valueOutputItemStart * valueBytesPerItem;
            innerValueBuf.Slice(0, valueBytes).CopyTo(valueOutput.AsSpan(destOffset, valueBytes));
        }

        _ = numLevels;
    }

    /// <summary>
    /// Decodes a single Fastlanes <c>InlineBitpacking</c> mini-block chunk.
    /// The chunk's value buffer is laid out as <c>[bit_width: T-sized LE]
    /// [packed_data: 1024 × bit_width / 8 bytes]</c>. We always unpack the
    /// full 1024-element block and then copy the live prefix (which may be
    /// less than 1024 for the final chunk).
    /// </summary>
    private static void DecodeInlineBitpackingChunk(
        ReadOnlySpan<byte> valueBuf, int itemsInChunk, int valueBytesPerItem,
        byte[] valueOutput, int valueOutputItemStart)
    {
        const int FastLanesChunk = Clast.FastLanes.BitPacking.ElementsPerChunk;
        if (itemsInChunk > FastLanesChunk)
            throw new NotImplementedException(
                $"InlineBitpacking chunk with {itemsInChunk} > {FastLanesChunk} items is not supported.");
        if (valueBuf.Length < valueBytesPerItem)
            throw new LanceFormatException(
                $"InlineBitpacking value buffer too small for the bit-width header ({valueBuf.Length} < {valueBytesPerItem}).");

        // Read bit_width header (always sizeof(T) bytes, little-endian).
        int bitWidth = valueBytesPerItem switch
        {
            1 => valueBuf[0],
            2 => BinaryPrimitives.ReadUInt16LittleEndian(valueBuf.Slice(0, 2)),
            4 => checked((int)BinaryPrimitives.ReadUInt32LittleEndian(valueBuf.Slice(0, 4))),
            8 => checked((int)BinaryPrimitives.ReadUInt64LittleEndian(valueBuf.Slice(0, 8))),
            _ => throw new NotImplementedException($"Unexpected value width {valueBytesPerItem}."),
        };
        if (bitWidth < 0 || bitWidth > valueBytesPerItem * 8)
            throw new LanceFormatException(
                $"InlineBitpacking bit_width {bitWidth} is out of range (max {valueBytesPerItem * 8}).");

        ReadOnlySpan<byte> packed = valueBuf.Slice(valueBytesPerItem);
        int destOffset = valueOutputItemStart * valueBytesPerItem;
        Span<byte> destSlice = valueOutput.AsSpan(destOffset, itemsInChunk * valueBytesPerItem);

        switch (valueBytesPerItem)
        {
            case 1:
                {
                    Span<byte> full = stackalloc byte[FastLanesChunk];
                    Clast.FastLanes.BitPacking.UnpackChunk<byte>(bitWidth, packed, full);
                    full.Slice(0, itemsInChunk).CopyTo(destSlice);
                    break;
                }
            case 2:
                {
                    Span<ushort> full = stackalloc ushort[FastLanesChunk];
                    Clast.FastLanes.BitPacking.UnpackChunk<ushort>(bitWidth, packed, full);
                    System.Runtime.InteropServices.MemoryMarshal.AsBytes(full.Slice(0, itemsInChunk))
                        .CopyTo(destSlice);
                    break;
                }
            case 4:
                {
                    // ArrayPool keeps multi-chunk columns from heap-allocating
                    // 4 KB per chunk; pool overhead (~50 ns) is well under the
                    // ~180 ns Fastlanes unpack itself.
                    int byteCount = FastLanesChunk * sizeof(uint);
                    byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(byteCount);
                    try
                    {
                        Span<uint> full = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, uint>(
                            rented.AsSpan(0, byteCount));
                        Clast.FastLanes.BitPacking.UnpackChunk<uint>(bitWidth, packed, full);
                        System.Runtime.InteropServices.MemoryMarshal.AsBytes(full.Slice(0, itemsInChunk))
                            .CopyTo(destSlice);
                    }
                    finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }
                    break;
                }
            case 8:
                {
                    int byteCount = FastLanesChunk * sizeof(ulong);
                    byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(byteCount);
                    try
                    {
                        Span<ulong> full = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(
                            rented.AsSpan(0, byteCount));
                        Clast.FastLanes.BitPacking.UnpackChunk<ulong>(bitWidth, packed, full);
                        System.Runtime.InteropServices.MemoryMarshal.AsBytes(full.Slice(0, itemsInChunk))
                            .CopyTo(destSlice);
                    }
                    finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }
                    break;
                }
        }
    }

    private static int AlignUp(int offset, int alignment)
    {
        int mod = offset % alignment;
        return mod == 0 ? offset : offset + (alignment - mod);
    }

    internal static IArrowArray BuildFixedWidthArray(
        IArrowType targetType, int length,
        byte[] valueBytes, byte[]? validityBitmap, int nullCount)
    {
        ArrowBuffer validity = validityBitmap is null
            ? ArrowBuffer.Empty
            : new ArrowBuffer(validityBitmap);

        if (targetType is BooleanType)
        {
            throw new NotImplementedException(
                "Boolean (bits_per_value=1) MiniBlock is not yet supported in Phase 6.");
        }

        var data = new ArrayData(
            targetType, length, nullCount, offset: 0,
            new[] { validity, new ArrowBuffer(valueBytes) });
        return ArrowArrayFactory.BuildArray(data);
    }

    // --- Phase 7b: Multi-leaf struct support ---

    /// <summary>
    /// Decode a leaf column belonging to a v2.1 struct. The chunk's def
    /// buffer carries layered nullability — layer[0] is the leaf's own
    /// validity, layer[1] is the struct's. Returns:
    /// <list type="bullet">
    ///   <item>An Arrow array with leaf-level validity applied (a row
    ///   counts as null when either the leaf or the parent struct is
    ///   null at that position).</item>
    ///   <item>The struct-level validity bitmap (Arrow convention: bit
    ///   set means the struct is valid). <c>null</c> when the outer
    ///   layer is <c>ALL_VALID_ITEM</c> — that is, the struct is
    ///   structurally non-nullable in this column's view.</item>
    ///   <item>The struct-level null count (always 0 when the bitmap is
    ///   null).</item>
    /// </list>
    ///
    /// <para>Supported layer combinations (the only ones pylance emits):
    /// <c>[ALL_VALID_ITEM, ALL_VALID_ITEM]</c> (no def buffer),
    /// <c>[NULLABLE_ITEM, ALL_VALID_ITEM]</c> (def∈{0,1}, 1 = leaf null),
    /// and <c>[NULLABLE_ITEM, NULLABLE_ITEM]</c> (def∈{0,1,2}, 2 =
    /// struct null cascading to the leaf). <c>[ALL_VALID_ITEM,
    /// NULLABLE_ITEM]</c> is rejected — pylance normalises this to
    /// <c>[NULLABLE_ITEM, NULLABLE_ITEM]</c> rather than emitting it.</para>
    ///
    /// <para>Other limits: value compression must be <see cref="Flat"/>,
    /// there must be exactly one mini-block chunk, no dictionary, and no
    /// repetition index.</para>
    /// </summary>
    public static (IArrowArray Array, byte[]? StructValidity, int StructNullCount) DecodeForStructChild(
        MiniBlockLayout layout, IArrowType childType, in PageContext context)
    {
        if (layout.NumBuffers != 1)
            throw new NotImplementedException(
                $"Struct-child MiniBlockLayout num_buffers={layout.NumBuffers} is not supported (must be 1).");
        if (layout.RepetitionIndexDepth != 0)
            throw new NotImplementedException(
                "Struct-child MiniBlockLayout with repetition_index_depth > 0 is not supported.");
        if (layout.Dictionary is not null)
            throw new NotImplementedException(
                "Struct-child MiniBlockLayout with a dictionary is not supported.");
        if (layout.Layers.Count != 2)
            throw new NotImplementedException(
                $"Struct-child MiniBlockLayout expects 2 layers, got {layout.Layers.Count}.");

        RepDefLayer leafLayer = layout.Layers[0];
        RepDefLayer structLayer = layout.Layers[1];
        bool leafNullable = leafLayer == RepDefLayer.RepdefNullableItem;
        bool leafAllValid = leafLayer == RepDefLayer.RepdefAllValidItem;
        bool structNullable = structLayer == RepDefLayer.RepdefNullableItem;
        bool structAllValid = structLayer == RepDefLayer.RepdefAllValidItem;
        if (!(leafNullable || leafAllValid) || !(structNullable || structAllValid))
            throw new NotImplementedException(
                $"Struct-child layers '{leafLayer}, {structLayer}' are not supported.");
        // pylance normalises [AllValid, Nullable] to [Nullable, Nullable]; reject
        // until a real fixture appears so the def-decoding path stays exercised.
        if (leafAllValid && structNullable)
            throw new NotImplementedException(
                "Struct-child layers [AllValid, Nullable] are not yet supported (no fixture available).");

        bool hasDef = layout.DefCompression is not null;
        bool needDef = leafNullable || structNullable;
        if (needDef && !hasDef)
            throw new LanceFormatException(
                "Struct-child with a NULLABLE layer requires def_compression.");
        if (!needDef && hasDef)
            throw new LanceFormatException(
                "Struct-child with all-valid layers must not have def_compression.");

        long numItems = checked((long)layout.NumItems);
        int length = checked((int)numItems);
        bool hasLargeChunk = layout.HasLargeChunk;

        ReadOnlySpan<byte> chunkMeta = context.PageBuffers[0].Span;
        ReadOnlySpan<byte> chunkData = context.PageBuffers[1].Span;
        var chunks = ParseChunkMetadata(chunkMeta, hasLargeChunk, numItems);
        if (chunks.Count != 1)
            throw new NotImplementedException(
                $"Multi-chunk struct-child reads are not yet supported (got {chunks.Count} chunks).");

        CompressiveEncoding valueEnc = layout.ValueCompression
            ?? throw new LanceFormatException("MiniBlockLayout has no value_compression.");
        int valueBytesPerItem = ResolveFlatBytesPerValue(valueEnc, childType);

        byte[] valueBytes = new byte[checked(length * valueBytesPerItem)];
        byte[]? leafValidity = leafNullable ? new byte[(length + 7) / 8] : null;
        byte[]? structValidity = structNullable ? new byte[(length + 7) / 8] : null;
        int leafNullCount = 0;
        int structNullCount = 0;

        ReadOnlySpan<byte> chunkBytes = chunkData.Slice(0, (int)chunks[0].SizeBytes);
        int itemsInChunk = (int)chunks[0].NumValues;

        int cursor = 0;
        int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2)); cursor += 2;
        _ = numLevels;
        int defSize = 0;
        if (hasDef)
        {
            defSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += 2;
        }
        int valueBufSize = hasLargeChunk
            ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
            : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
        cursor += hasLargeChunk ? 4 : 2;
        cursor = AlignUp(cursor, MiniBlockAlignment);

        if (hasDef)
        {
            int expected = itemsInChunk * sizeof(ushort);
            if (defSize != expected)
                throw new NotImplementedException(
                    $"Struct-child def buffer size {defSize} != expected {expected} (non-Flat(16) def is not supported).");

            // [N, AV]: def∈{0,1}, only the leaf layer can be null.
            // [N, N]:  def∈{0,1,2}, def=2 is the cascading struct-null.
            // In both cases leaf valid iff def == 0 (struct-null cascades to the leaf in Arrow).
            const ushort StructNullDef = 2;
            for (int i = 0; i < itemsInChunk; i++)
            {
                ushort def = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor + i * 2, 2));
                if (leafNullable)
                {
                    if (def == 0)
                        leafValidity![i >> 3] |= (byte)(1 << (i & 7));
                    else
                        leafNullCount++;
                }
                if (structNullable)
                {
                    if (def != StructNullDef)
                        structValidity![i >> 3] |= (byte)(1 << (i & 7));
                    else
                        structNullCount++;
                }
            }
            cursor += defSize;
            cursor = AlignUp(cursor, MiniBlockAlignment);
        }

        int valueBytesNeeded = itemsInChunk * valueBytesPerItem;
        if (valueBufSize < valueBytesNeeded)
            throw new LanceFormatException(
                $"Struct-child value buffer size {valueBufSize} < expected {valueBytesNeeded}.");
        chunkBytes.Slice(cursor, valueBytesNeeded).CopyTo(valueBytes);

        var arr = BuildFixedWidthArray(childType, length, valueBytes, leafValidity, leafNullCount);
        return (arr, structValidity, structNullCount);
    }

    // --- v2.1 list-of-struct: low-level chunk reader for shared rep/def + per-leaf values ---

    /// <summary>
    /// Read a single chunk of a leaf column belonging to a v2.1 nested
    /// shape (list-of-struct, struct-of-struct, struct-of-list, etc.).
    /// Returns the raw rep/def buffers and the leaf's value bytes; assembly
    /// into Arrow arrays is the caller's job, since rep/def are shared
    /// across all sibling leaves and must be reconciled there.
    ///
    /// <para>Constraints: <c>num_buffers = 1</c>, single chunk per page,
    /// no <c>dictionary</c>, value compression <see cref="Flat"/>. Layer
    /// validation (item/struct/list shape, nullability matrix, etc.) is
    /// the caller's responsibility — this method only enforces the wire
    /// invariants common to every nested shape.</para>
    /// </summary>
    public static (byte[] ValueBytes, ushort[]? Rep, ushort[]? Def, int ValueBytesPerItem, int VisibleItems)
        DecodeNestedLeafChunk(MiniBlockLayout layout, IArrowType childType, in PageContext context)
    {
        if (layout.NumBuffers != 1)
            throw new NotImplementedException(
                $"Nested-leaf MiniBlockLayout num_buffers={layout.NumBuffers} is not supported (must be 1).");
        if (layout.Dictionary is not null)
            throw new NotImplementedException(
                "Nested-leaf MiniBlockLayout with a dictionary is not supported.");

        bool hasRep = layout.RepCompression is not null;
        bool hasDef = layout.DefCompression is not null;

        long numItems = checked((long)layout.NumItems);
        bool hasLargeChunk = layout.HasLargeChunk;

        ReadOnlySpan<byte> chunkMeta = context.PageBuffers[0].Span;
        ReadOnlySpan<byte> chunkData = context.PageBuffers[1].Span;
        var chunks = ParseChunkMetadata(chunkMeta, hasLargeChunk, numItems);
        if (chunks.Count != 1)
            throw new NotImplementedException(
                $"Multi-chunk nested-leaf reads are not yet supported (got {chunks.Count} chunks).");

        CompressiveEncoding valueEnc = layout.ValueCompression
            ?? throw new LanceFormatException("MiniBlockLayout has no value_compression.");
        int valueBytesPerItem = ResolveFlatBytesPerValue(valueEnc, childType);

        ReadOnlySpan<byte> chunkBytes = chunkData.Slice(0, (int)chunks[0].SizeBytes);
        int cursor = 0;
        int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2)); cursor += 2;
        int repSize = 0;
        if (hasRep)
        {
            repSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += 2;
        }
        int defSize = 0;
        if (hasDef)
        {
            defSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += 2;
        }
        int valueBufSize = hasLargeChunk
            ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
            : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
        cursor += hasLargeChunk ? 4 : 2;
        cursor = AlignUp(cursor, MiniBlockAlignment);

        ushort[]? rep = null;
        if (hasRep)
        {
            if (repSize != numLevels * sizeof(ushort))
                throw new NotImplementedException(
                    $"Nested-leaf rep buffer size {repSize} != {numLevels * 2} (non-Flat(16) rep is not supported).");
            rep = new ushort[numLevels];
            for (int i = 0; i < numLevels; i++)
                rep[i] = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor + i * 2, 2));
            cursor += repSize;
            cursor = AlignUp(cursor, MiniBlockAlignment);
        }

        ushort[]? def = null;
        if (hasDef)
        {
            if (defSize != numLevels * sizeof(ushort))
                throw new NotImplementedException(
                    $"Nested-leaf def buffer size {defSize} != {numLevels * 2} (non-Flat(16) def is not supported).");
            def = new ushort[numLevels];
            for (int i = 0; i < numLevels; i++)
                def[i] = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor + i * 2, 2));
            cursor += defSize;
            cursor = AlignUp(cursor, MiniBlockAlignment);
        }

        // num_items in the layout counts visible items.
        int visibleItems = checked((int)numItems);
        int valueBytesNeeded = visibleItems * valueBytesPerItem;
        if (valueBufSize < valueBytesNeeded)
            throw new LanceFormatException(
                $"Nested-leaf value buffer {valueBufSize} < expected {valueBytesNeeded} for {visibleItems} visible items.");
        var valueBytes = new byte[valueBytesNeeded];
        chunkBytes.Slice(cursor, valueBytesNeeded).CopyTo(valueBytes);

        return (valueBytes, rep, def, valueBytesPerItem, visibleItems);
    }

    // --- v2.1 string-encoding gap (1/3): MiniBlockLayout-level dictionary ---

    /// <summary>
    /// Decode a MiniBlockLayout whose <see cref="MiniBlockLayout.Dictionary"/>
    /// is set: the value buffers in chunks contain indices, and an extra
    /// page buffer (the third one) carries the dictionary items.
    ///
    /// <para>pylance emits this for low-cardinality string/binary columns:
    /// indices via <see cref="InlineBitpacking"/>, dict items via
    /// <see cref="Variable"/>(Flat u32 offsets). The dictionary buffer's
    /// wire layout is an 8-byte prefix followed by
    /// <c>(num_dictionary_items + 1) × u32</c> offsets and then the raw
    /// item bytes.</para>
    /// </summary>
    private static IArrowArray DecodeDictionaryMiniBlock(
        ReadOnlySpan<byte> chunkData, List<MiniChunk> chunks,
        bool hasDef, bool hasLargeChunk, MiniBlockLayout layout,
        IArrowType targetType, in PageContext context, long numItems)
    {
        if (targetType is not (StringType or BinaryType))
            throw new NotImplementedException(
                $"MiniBlockLayout dictionary for non-string/binary type {targetType} is not yet supported.");
        if (context.PageBuffers.Count < 3)
            throw new LanceFormatException(
                $"Dictionary MiniBlockLayout expects 3 page buffers (meta + data + dict), got {context.PageBuffers.Count}.");
        if (layout.NumDictionaryItems == 0 && numItems > 0)
            throw new LanceFormatException(
                $"Dictionary MiniBlockLayout has num_dictionary_items=0 but {numItems} rows.");

        int length = checked((int)numItems);
        int dictCount = checked((int)layout.NumDictionaryItems);

        // Decode the dictionary items from buffer 2.
        ReadOnlySpan<byte> dictBuf = context.PageBuffers[2].Span;
        var dictItems = DecodeDictionaryBuffer(dictBuf, dictCount, layout.Dictionary!);

        // Decode the indices from the mini-block chunks. The value_compression
        // describes the indices layout; we treat them as fixed-width unsigned
        // integers and then pull each row's dictionary entry.
        CompressiveEncoding indicesEnc = layout.ValueCompression
            ?? throw new LanceFormatException("Dictionary MiniBlockLayout has no value_compression for indices.");

        int[] indices = DecodeDictionaryIndices(
            chunkData, chunks, hasDef, hasLargeChunk, layout, indicesEnc, length);

        // Materialise. Lance v2.1 dictionary indices are 0-based; nullability
        // is carried separately via def levels (NULLABLE_ITEM layer).
        byte[]? validityBitmap = hasDef ? new byte[(length + 7) / 8] : null;
        int nullCount = 0;
        if (hasDef)
        {
            // Re-walk chunks to read def — same logic the non-dict path uses.
            ushort[] defLevels = ReadAllDefLevels(
                chunkData, chunks, hasLargeChunk, layout.DefCompression!, length);
            for (int i = 0; i < length; i++)
            {
                if (defLevels[i] == 0)
                    validityBitmap![i >> 3] |= (byte)(1 << (i & 7));
                else
                    nullCount++;
            }
        }

        return MaterializeDictionary(
            indices, dictItems, length, targetType, validityBitmap, nullCount);
    }

    /// <summary>
    /// Parse the layout-level dictionary buffer. The wire layout pylance emits
    /// is an 8-byte prefix (the data block's offsets-buffer-size pair) followed
    /// by a <see cref="Variable"/>-style packed-offsets array and then raw item
    /// bytes. Returns parallel arrays of (start, length) into a single dense
    /// data buffer.
    /// </summary>
    private static (byte[] Data, int[] Offsets) DecodeDictionaryBuffer(
        ReadOnlySpan<byte> dictBuf, int numDictItems, CompressiveEncoding dictEnc)
    {
        if (dictEnc.CompressionCase != CompressiveEncoding.CompressionOneofCase.Variable)
            throw new NotImplementedException(
                $"MiniBlockLayout.dictionary with '{dictEnc.CompressionCase}' is not yet supported (only Variable).");
        Variable variable = dictEnc.Variable;
        if (variable.Values is not null)
            throw new NotImplementedException(
                "Dictionary Variable.values BufferCompression is not yet supported.");
        if (variable.Offsets is null
            || variable.Offsets.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat
            || variable.Offsets.Flat.BitsPerValue != 32)
            throw new NotImplementedException(
                "Dictionary Variable currently requires Flat(u32) offsets.");

        const int HeaderBytes = 8;     // 2×u32 prefix; purpose isn't semantically required, skip.
        if (dictBuf.Length < HeaderBytes + (numDictItems + 1) * 4)
            throw new LanceFormatException(
                $"Dictionary buffer too small: have {dictBuf.Length}, need at least {HeaderBytes + (numDictItems + 1) * 4}.");

        int offsetsStart = HeaderBytes;
        var rawOffsets = new int[numDictItems + 1];
        for (int i = 0; i <= numDictItems; i++)
            rawOffsets[i] = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(
                dictBuf.Slice(offsetsStart + i * 4, 4)));

        // Items' data follows the offsets array; rawOffsets are relative to
        // the start of the data section. Copy into a tight byte array.
        int dataStart = offsetsStart + (numDictItems + 1) * 4;
        int totalDataBytes = rawOffsets[numDictItems];
        if (dataStart + totalDataBytes > dictBuf.Length)
            throw new LanceFormatException(
                $"Dictionary data section extends past buffer (data_start={dataStart}, total={totalDataBytes}, buf={dictBuf.Length}).");
        var data = dictBuf.Slice(dataStart, totalDataBytes).ToArray();
        return (data, rawOffsets);
    }

    private static int[] DecodeDictionaryIndices(
        ReadOnlySpan<byte> chunkData, List<MiniChunk> chunks,
        bool hasDef, bool hasLargeChunk, MiniBlockLayout layout,
        CompressiveEncoding indicesEnc, int length)
    {
        // Re-use the existing fixed-width path: produce a UInt32Array, read
        // its int32s out. Indices are by construction smaller than the dict
        // size, so int32 is plenty.
        bool inlineBp = indicesEnc.CompressionCase
            == CompressiveEncoding.CompressionOneofCase.InlineBitpacking;
        int idxBytesPerItem = inlineBp
            ? ResolveInlineBitpackingBytesPerValue(indicesEnc.InlineBitpacking, UInt32Type.Default)
            : ResolveFlatBytesPerValue(indicesEnc, UInt32Type.Default);

        byte[] idxBytes = new byte[checked(length * idxBytesPerItem)];

        long globalChunkByteOffset = 0;
        int valueCursor = 0;
        foreach (MiniChunk chunk in chunks)
        {
            ReadOnlySpan<byte> chunkBytes = chunkData.Slice(
                (int)globalChunkByteOffset, (int)chunk.SizeBytes);
            int itemsInChunk = (int)chunk.NumValues;
            byte[]? noScratch = null;
            DecodeFixedWidthChunk(
                chunkBytes, itemsInChunk, hasRep: false, hasDef, hasLargeChunk,
                idxBytesPerItem, idxBytes, valueCursor,
                validityBitmap: null, bitmapStartItem: 0, out _,
                inlineBitpacking: inlineBp,
                defComp: layout.DefCompression,
                generalZstd: false,
                decompressScratch: ref noScratch);
            valueCursor += itemsInChunk;
            globalChunkByteOffset += (long)chunk.SizeBytes;
        }

        var indices = new int[length];
        switch (idxBytesPerItem)
        {
            case 1:
                for (int i = 0; i < length; i++) indices[i] = idxBytes[i];
                break;
            case 2:
                for (int i = 0; i < length; i++)
                    indices[i] = BinaryPrimitives.ReadUInt16LittleEndian(idxBytes.AsSpan(i * 2, 2));
                break;
            case 4:
                for (int i = 0; i < length; i++)
                    indices[i] = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(idxBytes.AsSpan(i * 4, 4)));
                break;
            default:
                throw new NotImplementedException(
                    $"Dictionary index byte width {idxBytesPerItem} not supported.");
        }
        return indices;
    }

    /// <summary>
    /// Concatenate def values across every chunk into one ushort[]. Used by
    /// the dictionary path when nullability is present.
    /// </summary>
    private static ushort[] ReadAllDefLevels(
        ReadOnlySpan<byte> chunkData, List<MiniChunk> chunks,
        bool hasLargeChunk, CompressiveEncoding defComp, int length)
    {
        var result = new ushort[length];
        long globalChunkByteOffset = 0;
        int cursorRow = 0;
        foreach (MiniChunk chunk in chunks)
        {
            ReadOnlySpan<byte> chunkBytes = chunkData.Slice(
                (int)globalChunkByteOffset, (int)chunk.SizeBytes);
            int itemsInChunk = (int)chunk.NumValues;
            int cursor = 0;
            cursor += 2; // num_levels
            int defSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += 2;
            // Skip value_buf_size (1×u16 since num_buffers=1).
            cursor += hasLargeChunk ? 4 : 2;
            cursor = AlignUp(cursor, MiniBlockAlignment);

            ushort[] defValues = ReadDefChunkBuffer(
                chunkBytes.Slice(cursor, defSize), itemsInChunk, defComp);
            defValues.CopyTo(result, cursorRow);

            cursorRow += itemsInChunk;
            globalChunkByteOffset += (long)chunk.SizeBytes;
        }
        return result;
    }

    private static IArrowArray MaterializeDictionary(
        int[] indices, (byte[] Data, int[] Offsets) dict, int length,
        IArrowType targetType, byte[]? validityBitmap, int nullCount)
    {
        // Two-pass: compute output sizes, then copy.
        var outLengths = new int[length];
        long totalOut = 0;
        for (int i = 0; i < length; i++)
        {
            // Skip null rows: they shouldn't dereference the dictionary.
            if (validityBitmap is not null && (validityBitmap[i >> 3] & (1 << (i & 7))) == 0)
                continue;
            int idx = indices[i];
            if ((uint)idx >= (uint)(dict.Offsets.Length - 1))
                throw new LanceFormatException(
                    $"Dictionary index {idx} at row {i} is out of range (dict size = {dict.Offsets.Length - 1}).");
            int len = dict.Offsets[idx + 1] - dict.Offsets[idx];
            outLengths[i] = len;
            totalOut += len;
        }

        var outOffsets = new byte[(length + 1) * sizeof(int)];
        var outData = new byte[totalOut];
        long cumulative = 0;
        BinaryPrimitives.WriteInt32LittleEndian(outOffsets.AsSpan(0, 4), 0);
        int writePos = 0;
        for (int i = 0; i < length; i++)
        {
            int len = outLengths[i];
            if (len > 0)
            {
                int idx = indices[i];
                int start = dict.Offsets[idx];
                System.Array.Copy(dict.Data, start, outData, writePos, len);
                writePos += len;
                cumulative += len;
            }
            BinaryPrimitives.WriteInt32LittleEndian(
                outOffsets.AsSpan((i + 1) * 4, 4), checked((int)cumulative));
        }

        ArrowBuffer validity = validityBitmap is null ? ArrowBuffer.Empty : new ArrowBuffer(validityBitmap);
        var data = new ArrayData(
            targetType, length, nullCount, offset: 0,
            new[] { validity, new ArrowBuffer(outOffsets), new ArrowBuffer(outData) });
        return ArrowArrayFactory.BuildArray(data);
    }

    /// <summary>
    /// Decompresses a single mini-block chunk's def buffer into <c>ushort[]</c>
    /// values. pylance picks between <c>Flat(u16)</c> and <c>InlineBitpacking
    /// (uncompressed=16)</c> for def_compression depending on entropy; this
    /// helper handles both. Returns exactly <paramref name="itemsInChunk"/>
    /// def values.
    /// </summary>
    private static ushort[] ReadDefChunkBuffer(
        ReadOnlySpan<byte> defBuf, int itemsInChunk, CompressiveEncoding defComp)
    {
        var output = new ushort[itemsInChunk];

        switch (defComp.CompressionCase)
        {
            case CompressiveEncoding.CompressionOneofCase.Flat:
                {
                    if (defComp.Flat.BitsPerValue != 16)
                        throw new NotImplementedException(
                            $"Def Flat bits_per_value={defComp.Flat.BitsPerValue} is not supported (only 16).");
                    int expected = itemsInChunk * sizeof(ushort);
                    if (defBuf.Length < expected)
                        throw new LanceFormatException(
                            $"Def Flat buffer too small: need {expected} bytes, have {defBuf.Length}.");
                    for (int i = 0; i < itemsInChunk; i++)
                        output[i] = BinaryPrimitives.ReadUInt16LittleEndian(defBuf.Slice(i * 2, 2));
                    return output;
                }

            case CompressiveEncoding.CompressionOneofCase.InlineBitpacking:
                {
                    if (defComp.InlineBitpacking.UncompressedBitsPerValue != 16)
                        throw new NotImplementedException(
                            $"Def InlineBitpacking uncompressed_bits={defComp.InlineBitpacking.UncompressedBitsPerValue} is not supported (only 16).");
                    if (defBuf.Length < sizeof(ushort))
                        throw new LanceFormatException(
                            $"Def InlineBitpacking buffer too small for header: have {defBuf.Length}.");
                    int bitWidth = BinaryPrimitives.ReadUInt16LittleEndian(defBuf.Slice(0, 2));
                    if (bitWidth < 0 || bitWidth > 16)
                        throw new LanceFormatException(
                            $"Def InlineBitpacking bit_width {bitWidth} out of range.");
                    const int FastLanesChunk = Clast.FastLanes.BitPacking.ElementsPerChunk;
                    if (itemsInChunk > FastLanesChunk)
                        throw new NotImplementedException(
                            $"Def InlineBitpacking with {itemsInChunk} > {FastLanesChunk} items is not supported.");
                    Span<ushort> full = stackalloc ushort[FastLanesChunk];
                    Clast.FastLanes.BitPacking.UnpackChunk<ushort>(
                        bitWidth, defBuf.Slice(sizeof(ushort)), full);
                    full.Slice(0, itemsInChunk).CopyTo(output);
                    return output;
                }

            case CompressiveEncoding.CompressionOneofCase.OutOfLineBitpacking:
                {
                    // OutOfLineBitpacking(uncompressed=16, values=Flat(bits_per_value=K))
                    // — like InlineBitpacking but the bit width K lives in the
                    // encoding (constant across the page), not in a per-chunk
                    // header. The buffer is fastlanes-packed for the full
                    // 1024-element block; we unpack and take the live prefix.
                    var ool = defComp.OutOfLineBitpacking;
                    if (ool.UncompressedBitsPerValue != 16)
                        throw new NotImplementedException(
                            $"Def OutOfLineBitpacking uncompressed_bits={ool.UncompressedBitsPerValue} is not supported (only 16).");
                    if (ool.Values is null
                        || ool.Values.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat)
                        throw new NotImplementedException(
                            $"Def OutOfLineBitpacking inner '{ool.Values?.CompressionCase}' is not supported (only Flat).");
                    ulong bpv = ool.Values.Flat.BitsPerValue;
                    if (bpv == 0 || bpv > 16)
                        throw new NotImplementedException(
                            $"Def OutOfLineBitpacking inner bits_per_value={bpv} is not supported (only 1..16).");
                    const int FastLanesChunk = Clast.FastLanes.BitPacking.ElementsPerChunk;
                    if (itemsInChunk > FastLanesChunk)
                        throw new NotImplementedException(
                            $"Def OutOfLineBitpacking with {itemsInChunk} > {FastLanesChunk} items is not supported.");
                    Span<ushort> full = stackalloc ushort[FastLanesChunk];
                    Clast.FastLanes.BitPacking.UnpackChunk<ushort>((int)bpv, defBuf, full);
                    full.Slice(0, itemsInChunk).CopyTo(output);
                    return output;
                }

            default:
                throw new NotImplementedException(
                    $"Def compression '{defComp.CompressionCase}' is not yet supported.");
        }
    }

    // --- Phase 7: Variable encoding (strings / binary) ---

    /// <summary>
    /// Decodes a MiniBlockLayout page whose value_compression is
    /// <c>Variable(offsets = Flat(u32), values = uncompressed)</c>. The
    /// single chunk buffer contains the packed offsets followed by value
    /// bytes — offsets point into the buffer, so <c>offsets[0]</c> is where
    /// the data payload begins.
    /// </summary>
    private static IArrowArray DecodeVariableMiniBlock(
        ReadOnlySpan<byte> chunkData, List<MiniChunk> chunks,
        bool hasDef, bool hasLargeChunk,
        CompressiveEncoding valueEnc, CompressiveEncoding? defComp,
        IArrowType targetType, long numItems)
    {
        if (valueEnc.CompressionCase != CompressiveEncoding.CompressionOneofCase.Variable)
            throw new InvalidOperationException("Expected Variable value_compression.");

        Variable variable = valueEnc.Variable;
        if (variable.Values is not null)
            throw new NotImplementedException(
                "Variable encoding with BufferCompression on values is not yet supported.");
        if (variable.Offsets is null
            || variable.Offsets.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat)
            throw new NotImplementedException(
                "Variable encoding currently requires Flat offsets.");
        ulong bitsPerOffset = variable.Offsets.Flat.BitsPerValue;
        if (bitsPerOffset != 32)
            throw new NotImplementedException(
                $"Variable offsets bits_per_value={bitsPerOffset} is not yet supported (only u32).");

        int length = checked((int)numItems);
        // Pre-size: walk every chunk, sum up per-chunk visible bytes for the
        // global data buffer, and emit Arrow offsets/validity that span all
        // chunks. Each chunk's value buffer holds its own packed
        // (items_in_chunk + 1) × u32 offsets followed by the data bytes,
        // with offsets *absolute within that chunk's value buffer* (so
        // offsets[0] is where the data section begins).

        var globalOffsets = new List<int>(length + 1) { 0 };
        var dataChunks = new List<byte[]>(chunks.Count);
        byte[]? validityBitmap = hasDef ? new byte[(length + 7) / 8] : null;
        int nullCount = 0;

        long globalChunkByteOffset = 0;
        int globalRowCursor = 0;

        foreach (MiniChunk chunk in chunks)
        {
            ReadOnlySpan<byte> chunkBytes = chunkData.Slice(
                (int)globalChunkByteOffset, (int)chunk.SizeBytes);
            int itemsInChunk = (int)chunk.NumValues;

            int cursor = 0;
            int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2)); cursor += 2;
            _ = numLevels;

            int defSize = 0;
            if (hasDef)
            {
                defSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
                cursor += 2;
            }

            int valueBufSize = hasLargeChunk
                ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
                : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += hasLargeChunk ? 4 : 2;
            cursor = AlignUp(cursor, MiniBlockAlignment);

            if (hasDef)
            {
                ushort[] defValues = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, defSize), itemsInChunk, defComp!);
                for (int i = 0; i < itemsInChunk; i++)
                {
                    int globalRow = globalRowCursor + i;
                    if (defValues[i] == 0)
                        validityBitmap![globalRow >> 3] |= (byte)(1 << (globalRow & 7));
                    else
                        nullCount++;
                }
                cursor += defSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }

            ReadOnlySpan<byte> valueBuf = chunkBytes.Slice(cursor, valueBufSize);
            int offsetsBytes = checked((itemsInChunk + 1) * 4);
            if (valueBuf.Length < offsetsBytes)
                throw new LanceFormatException(
                    $"Variable chunk value buffer too small: need {offsetsBytes} offset bytes, have {valueBuf.Length}.");

            uint baseOffset = BinaryPrimitives.ReadUInt32LittleEndian(valueBuf.Slice(0, 4));
            int globalDataBase = globalOffsets[^1];
            int chunkDataLen = 0;
            for (int i = 1; i <= itemsInChunk; i++)
            {
                uint absolute = BinaryPrimitives.ReadUInt32LittleEndian(valueBuf.Slice(i * 4, 4));
                if (absolute < baseOffset)
                    throw new LanceFormatException(
                        $"Variable offset {absolute} at index {i} is less than base {baseOffset}.");
                int rel = checked((int)(absolute - baseOffset));
                if (rel > chunkDataLen) chunkDataLen = rel;
                globalOffsets.Add(checked(globalDataBase + rel));
            }

            int dataStart = checked((int)baseOffset);
            if (dataStart + chunkDataLen > valueBuf.Length)
                throw new LanceFormatException(
                    $"Variable chunk data range [{dataStart}, {dataStart + chunkDataLen}) exceeds buffer length {valueBuf.Length}.");
            var chunkSlice = new byte[chunkDataLen];
            valueBuf.Slice(dataStart, chunkDataLen).CopyTo(chunkSlice);
            dataChunks.Add(chunkSlice);

            globalRowCursor += itemsInChunk;
            globalChunkByteOffset += (long)chunk.SizeBytes;
        }

        // Concatenate per-chunk data slices into one Arrow values buffer.
        int totalData = 0;
        foreach (var d in dataChunks) totalData = checked(totalData + d.Length);
        var arrowData = new byte[totalData];
        int writePos = 0;
        foreach (var d in dataChunks)
        {
            d.CopyTo(arrowData, writePos);
            writePos += d.Length;
        }

        var arrowOffsets = new byte[(length + 1) * sizeof(int)];
        for (int i = 0; i <= length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(arrowOffsets.AsSpan(i * 4, 4), globalOffsets[i]);

        ArrowBuffer validity = validityBitmap is null ? ArrowBuffer.Empty : new ArrowBuffer(validityBitmap);
        var data = new ArrayData(
            targetType, length, nullCount, offset: 0,
            new[] { validity, new ArrowBuffer(arrowOffsets), new ArrowBuffer(arrowData) });
        return ArrowArrayFactory.BuildArray(data);
    }

    // --- FSST-compressed strings (Lance container + cwida-derived decoder) ---

    /// <summary>
    /// Decodes a MiniBlockLayout whose value_compression is
    /// <see cref="Fsst"/>: a page-level symbol table in Lance's container
    /// format plus an inner <see cref="Variable"/> encoding describing
    /// per-row compressed-byte boundaries.
    ///
    /// <para>Lance's symbol-table layout (constant-size for FSST8):
    /// <c>[8-byte header][N × 8-byte symbol slots, left-aligned, zero-padded]
    /// [N × 1-byte per-symbol lengths]</c> where <c>N = (totalLen - 8) / 9</c>
    /// (256 for FSST8). The header isn't needed for decoding — we hand the
    /// length and slot byte arrays directly to <c>FsstDecoder.FromSymbols</c>.</para>
    ///
    /// <para>For each chunk we parse the Variable header to extract the
    /// per-row FSST-compressed slices, accumulate them with their lengths,
    /// then call <c>FsstDecoder.TryDecompressBatch</c> once at the end to
    /// fill Arrow's value buffer + offsets in one shot.</para>
    /// </summary>
    private static IArrowArray DecodeFsstVariableMiniBlock(
        ReadOnlySpan<byte> chunkData, List<MiniChunk> chunks,
        bool hasDef, bool hasLargeChunk,
        Fsst fsst, CompressiveEncoding? defComp,
        IArrowType targetType, long numItems)
    {
        if (fsst.SymbolTable is null || fsst.SymbolTable.Length == 0)
            throw new LanceFormatException("Fsst encoding has no symbol_table.");
        if (fsst.Values is null
            || fsst.Values.CompressionCase != CompressiveEncoding.CompressionOneofCase.Variable)
            throw new NotImplementedException(
                $"Fsst inner encoding '{fsst.Values?.CompressionCase}' is not yet supported (only Variable).");

        Variable variable = fsst.Values.Variable;
        if (variable.Values is not null)
            throw new NotImplementedException(
                "Fsst Variable.values BufferCompression is not yet supported.");
        if (variable.Offsets is null
            || variable.Offsets.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat
            || variable.Offsets.Flat.BitsPerValue != 32)
            throw new NotImplementedException(
                "Fsst Variable currently requires Flat(u32) offsets.");

        Clast.Fsst.FsstDecoder fsstDecoder = BuildLanceFsstDecoder(fsst.SymbolTable.Span);

        int length = checked((int)numItems);
        byte[]? validityBitmap = hasDef ? new byte[(length + 7) / 8] : null;
        int nullCount = 0;

        // Walk every chunk and accumulate per-row compressed slices into a
        // single dense buffer + parallel lengths array. Null rows contribute
        // a zero-length entry so the FSST batch decoder produces empty Arrow
        // values for them.
        var compressedAccum = new EngineeredWood.Buffers.GrowableBuffer(64 * 1024);
        var compressedLengths = new int[length];
        long globalChunkByteOffset = 0;
        int globalRowCursor = 0;

        foreach (MiniChunk chunk in chunks)
        {
            ReadOnlySpan<byte> chunkBytes = chunkData.Slice(
                (int)globalChunkByteOffset, (int)chunk.SizeBytes);
            int itemsInChunk = (int)chunk.NumValues;

            int cursor = 0;
            int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2)); cursor += 2;
            _ = numLevels;

            int defSize = 0;
            if (hasDef)
            {
                defSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
                cursor += 2;
            }

            int valueBufSize = hasLargeChunk
                ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
                : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += hasLargeChunk ? 4 : 2;
            cursor = AlignUp(cursor, MiniBlockAlignment);

            if (hasDef)
            {
                ushort[] defValues = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, defSize), itemsInChunk, defComp!);
                for (int i = 0; i < itemsInChunk; i++)
                {
                    int globalRow = globalRowCursor + i;
                    if (defValues[i] == 0)
                        validityBitmap![globalRow >> 3] |= (byte)(1 << (globalRow & 7));
                    else
                        nullCount++;
                }
                cursor += defSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }

            ReadOnlySpan<byte> valueBuf = chunkBytes.Slice(cursor, valueBufSize);
            int offsetsBytes = checked((itemsInChunk + 1) * 4);
            if (valueBuf.Length < offsetsBytes)
                throw new LanceFormatException(
                    $"Fsst Variable chunk value buffer too small: need {offsetsBytes} offset bytes, have {valueBuf.Length}.");

            uint baseOffset = BinaryPrimitives.ReadUInt32LittleEndian(valueBuf.Slice(0, 4));
            uint prevAbs = baseOffset;
            for (int i = 0; i < itemsInChunk; i++)
            {
                uint nextAbs = BinaryPrimitives.ReadUInt32LittleEndian(valueBuf.Slice((i + 1) * 4, 4));
                if (nextAbs < prevAbs)
                    throw new LanceFormatException(
                        $"Fsst Variable offsets not monotonic at chunk index {i} ({nextAbs} < {prevAbs}).");
                int rowLen = checked((int)(nextAbs - prevAbs));
                int rowStart = checked((int)(prevAbs - baseOffset)) + offsetsBytes;
                if (rowStart + rowLen > valueBuf.Length)
                    throw new LanceFormatException(
                        $"Fsst Variable row {i} extends past chunk data section.");
                compressedAccum.Write(valueBuf.Slice(rowStart, rowLen));
                compressedLengths[globalRowCursor + i] = rowLen;
                prevAbs = nextAbs;
            }

            globalRowCursor += itemsInChunk;
            globalChunkByteOffset += (long)chunk.SizeBytes;
        }

        // Bulk FSST decompress: caller-supplied destination + Arrow-style
        // prefix-sum offsets, no per-row allocations.
        ReadOnlySpan<byte> compressedAll = compressedAccum.WrittenSpan;
        int destCap = Clast.Fsst.FsstDecoder.MaxDecompressedLength(compressedAll.Length);
        byte[] destination = new byte[destCap];
        int[] destinationOffsets = new int[length + 1];

        if (!fsstDecoder.TryDecompressBatch(
                compressedAll, compressedLengths,
                destination, destinationOffsets,
                out int totalWritten))
        {
            throw new LanceFormatException(
                $"FSST batch decompress failed (compressedBytes={compressedAll.Length}, " +
                $"destCap={destCap}, rows={length}).");
        }

        byte[] arrowData;
        if (totalWritten == destination.Length)
        {
            arrowData = destination;
        }
        else
        {
            arrowData = new byte[totalWritten];
            destination.AsSpan(0, totalWritten).CopyTo(arrowData);
        }

        var arrowOffsets = new byte[(length + 1) * sizeof(int)];
        for (int i = 0; i <= length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(arrowOffsets.AsSpan(i * 4, 4), destinationOffsets[i]);

        ArrowBuffer validityBuf = validityBitmap is null
            ? ArrowBuffer.Empty
            : new ArrowBuffer(validityBitmap);
        var arrayData = new ArrayData(
            targetType, length, nullCount, offset: 0,
            new[] { validityBuf, new ArrowBuffer(arrowOffsets), new ArrowBuffer(arrowData) });
        return ArrowArrayFactory.BuildArray(arrayData);
    }

    /// <summary>
    /// Parse Lance's symbol-table container and build an
    /// <see cref="Clast.Fsst.FsstDecoder"/> via its
    /// <c>FromSymbols(lengths, packedValues)</c> bring-your-own-framing API.
    ///
    /// <para>Layout: <c>[8-byte Lance header][N × 8-byte packed symbol slots]
    /// [N × 1-byte trailing-metadata array]</c>, where N = (totalLen - 8) / 9.
    /// For FSST8 N is always 256, but we derive it to stay defensive against
    /// future variants.</para>
    ///
    /// <para>The trailing N-byte array is NOT a per-symbol byte length and
    /// also NOT a usage marker. Empirically: code 217 has trailing value 1
    /// but its slot holds the 8-char symbol "ck brown"; code 250 has trailing
    /// value 0 but its slot holds the 1-char symbol "5". The trailing bytes
    /// appear to be some FSST-internal hash/bucket metadata that doesn't map
    /// to information we need. We ignore the trailing array and derive
    /// symbol lengths purely from null-termination of the 8-byte slots
    /// (length = position of first 0x00 byte, or 8 if the slot contains no
    /// nulls). This is safe for text — Lance doesn't pick symbols with
    /// embedded nulls for text/UTF-8 data.</para>
    ///
    /// <para>Code 255 is forced to length 0 because Clast.Fsst (following
    /// cwida convention) requires it as the FSST escape code; Lance's
    /// emitted bitstreams use 0xFF as the escape byte (verified — observed
    /// in row 5's compressed bytes ahead of literal characters that aren't
    /// in any symbol).</para>
    /// </summary>
    private static Clast.Fsst.FsstDecoder BuildLanceFsstDecoder(ReadOnlySpan<byte> symbolTable)
    {
        const int LanceHeaderBytes = 8;
        const int BytesPerSymbolSlot = 8;
        const int FsstEscapeCode = 255;
        if (symbolTable.Length < LanceHeaderBytes
            || (symbolTable.Length - LanceHeaderBytes) % (BytesPerSymbolSlot + 1) != 0)
        {
            throw new LanceFormatException(
                $"Fsst symbol_table size {symbolTable.Length} is not a valid Lance FSST8 layout " +
                $"(expected 8 + N×{BytesPerSymbolSlot + 1} bytes).");
        }
        int numSymbols = (symbolTable.Length - LanceHeaderBytes) / (BytesPerSymbolSlot + 1);
        ReadOnlySpan<byte> packedValues = symbolTable.Slice(
            LanceHeaderBytes, numSymbols * BytesPerSymbolSlot);

        var lengths = new byte[numSymbols];
        for (int i = 0; i < numSymbols; i++)
        {
            ReadOnlySpan<byte> symbolBytes = packedValues.Slice(i * BytesPerSymbolSlot, BytesPerSymbolSlot);
            int len = symbolBytes.IndexOf((byte)0);
            lengths[i] = len < 0 ? (byte)BytesPerSymbolSlot : (byte)len;
        }
        if (numSymbols > FsstEscapeCode) lengths[FsstEscapeCode] = 0;

        try
        {
            return Clast.Fsst.FsstDecoder.FromSymbols(lengths, packedValues);
        }
        catch (Exception ex)
        {
            throw new LanceFormatException(
                $"Failed to build FSST decoder from Lance symbol table ({numSymbols} codes).", ex);
        }
    }

    // --- Phase 7: List of primitive ---

    /// <summary>
    /// Decodes a single-column v2.1 list of primitives. Layers are
    /// <c>[leaf, list]</c>. Rep=1 opens a new row boundary; def>0 marks an
    /// invisible placeholder (null or empty list, depending on the list
    /// layer type).
    /// </summary>
    private static IArrowArray DecodeListMiniBlock(
        ReadOnlySpan<byte> chunkData, List<MiniChunk> chunks,
        bool hasDef, bool hasRep, bool hasLargeChunk,
        Google.Protobuf.Collections.RepeatedField<RepDefLayer> layers,
        Apache.Arrow.Types.ListType listType, CompressiveEncoding valueEnc)
    {
        if (!hasRep)
            throw new LanceFormatException("List MiniBlockLayout must have rep_compression.");
        if (chunks.Count != 1)
            throw new NotImplementedException(
                "Multi-chunk list mini-block decoding is not yet supported.");
        if (valueEnc.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat)
            throw new NotImplementedException(
                $"List items with value_compression '{valueEnc.CompressionCase}' are not yet supported.");

        int valueBytesPerItem = ResolveFlatBytesPerValue(valueEnc, listType.ValueDataType);

        ReadOnlySpan<byte> chunkBytes = chunkData.Slice(0, (int)chunks[0].SizeBytes);

        int cursor = 0;
        int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2)); cursor += 2;
        int repSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2)); cursor += 2;
        int defSize = hasDef
            ? BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2))
            : 0;
        if (hasDef) cursor += 2;

        int valueBufSize = hasLargeChunk
            ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
            : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
        cursor += hasLargeChunk ? 4 : 2;
        cursor = AlignUp(cursor, MiniBlockAlignment);

        if (repSize != numLevels * sizeof(ushort))
            throw new NotImplementedException(
                $"List rep buffer size {repSize} != {numLevels * 2} (non-Flat(16) rep is not yet supported).");
        var rep = new ushort[numLevels];
        for (int i = 0; i < numLevels; i++)
            rep[i] = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor + i * 2, 2));
        cursor += repSize;
        cursor = AlignUp(cursor, MiniBlockAlignment);

        ushort[]? def = null;
        if (hasDef)
        {
            if (defSize != numLevels * sizeof(ushort))
                throw new NotImplementedException(
                    $"List def buffer size {defSize} != {numLevels * 2} (non-Flat(16) def is not yet supported).");
            def = new ushort[numLevels];
            for (int i = 0; i < numLevels; i++)
                def[i] = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor + i * 2, 2));
            cursor += defSize;
            cursor = AlignUp(cursor, MiniBlockAlignment);
        }

        // Walk rep/def to derive row boundaries, validity, and visible count.
        // Each rep=1 starts a new row; def[i] > 0 marks an invisible item
        // (null or empty list per the outer RepDefLayer).
        var arrowOffsetList = new List<int>();
        var validityList = new List<bool>();
        arrowOffsetList.Add(0);

        int visibleCount = 0;
        RepDefLayer listLayer = layers[1];
        bool listNullable = listLayer == RepDefLayer.RepdefNullableList
                            || listLayer == RepDefLayer.RepdefNullAndEmptyList;
        bool listEmptyable = listLayer == RepDefLayer.RepdefEmptyableList
                             || listLayer == RepDefLayer.RepdefNullAndEmptyList;

        for (int i = 0; i < numLevels; i++)
        {
            if (rep[i] == 1)
            {
                if (arrowOffsetList.Count > 1 || i > 0)
                    arrowOffsetList.Add(visibleCount);

                bool invisible = def is not null && def[i] > 0;
                if (invisible)
                {
                    if (listLayer == RepDefLayer.RepdefNullableList)
                    {
                        validityList.Add(false); // null list
                    }
                    else if (listLayer == RepDefLayer.RepdefEmptyableList)
                    {
                        validityList.Add(true); // empty list, valid
                    }
                    else if (listLayer == RepDefLayer.RepdefNullAndEmptyList)
                    {
                        throw new NotImplementedException(
                            "NULL_AND_EMPTY_LIST disambiguation is not yet supported.");
                    }
                    else
                    {
                        throw new LanceFormatException(
                            $"Invisible item with list layer {listLayer} is not expected.");
                    }
                    continue; // no value
                }

                validityList.Add(true);
                visibleCount++;
            }
            else
            {
                // rep=0 — continuation of current list (must be a valid item).
                if (def is not null && def[i] != 0)
                    throw new NotImplementedException(
                        "Null leaf items inside a list (def != 0 with rep == 0) are not yet supported.");
                visibleCount++;
            }
        }
        arrowOffsetList.Add(visibleCount);

        int numRows = arrowOffsetList.Count - 1;
        if (numRows != validityList.Count)
            throw new LanceFormatException(
                $"List decode mismatch: {numRows} rows vs {validityList.Count} validity entries.");

        int valuesExpected = visibleCount * valueBytesPerItem;
        if (valueBufSize < valuesExpected)
            throw new LanceFormatException(
                $"List value buffer {valueBufSize} < expected {valuesExpected} for {visibleCount} items.");
        var valueBytes = chunkBytes.Slice(cursor, valuesExpected).ToArray();

        var childData = new ArrayData(
            listType.ValueDataType, visibleCount,
            nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(valueBytes) });
        IArrowArray childArr = ArrowArrayFactory.BuildArray(childData);

        var arrowOffsets = new byte[(numRows + 1) * sizeof(int)];
        for (int i = 0; i <= numRows; i++)
            BinaryPrimitives.WriteInt32LittleEndian(
                arrowOffsets.AsSpan(i * 4, 4), arrowOffsetList[i]);

        byte[]? bitmap = null;
        int nullCount = 0;
        for (int i = 0; i < numRows; i++) if (!validityList[i]) nullCount++;
        if (nullCount > 0)
        {
            bitmap = new byte[(numRows + 7) / 8];
            for (int i = 0; i < numRows; i++)
                if (validityList[i])
                    bitmap[i >> 3] |= (byte)(1 << (i & 7));
        }

        _ = listNullable; _ = listEmptyable; // Currently checked via explicit branches above.

        ArrowBuffer validity = bitmap is null ? ArrowBuffer.Empty : new ArrowBuffer(bitmap);
        var listData = new ArrayData(
            listType, numRows, nullCount, offset: 0,
            new[] { validity, new ArrowBuffer(arrowOffsets) },
            children: new[] { childArr.Data });
        return new ListArray(listData);
    }
}
