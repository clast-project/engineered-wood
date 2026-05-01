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

        if (targetType is Apache.Arrow.Types.ListType or LargeListType)
            throw new InvalidOperationException(
                "MiniBlockLayoutDecoder.Decode should not be called directly for list types — " +
                "the recursive nested walker in LanceFileReader handles them.");
        if (targetType is FixedSizeListType fslType
            && valueEnc.CompressionCase == CompressiveEncoding.CompressionOneofCase.FixedSizeList)
        {
            if (generalZstd)
                throw new NotImplementedException(
                    "MiniBlockLayout General(ZSTD) wrapping is not yet supported for FSL columns.");
            return DecodeFslMiniBlock(
                chunkData, chunks, hasDef, hasRep, hasLargeChunk,
                layout, valueEnc.FixedSizeList, fslType, numItems);
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
        if (targetType is BooleanType)
        {
            // 1-bit Flat values, LSB-first within each byte. Decoded into an
            // Arrow bool buffer with separately-derived validity (when the
            // page has def levels).
            if (generalZstd)
                throw new NotImplementedException(
                    "MiniBlockLayout General(ZSTD) wrapping is not yet supported for Boolean columns.");
            return DecodeBoolMiniBlock(
                chunkData, chunks, hasDef, hasLargeChunk,
                valueEnc, layout.DefCompression, numItems);
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
        // FSL columns can carry an extra buffer per chunk (the inner-item
        // validity bitmap when has_validity = true). Other shapes still
        // require num_buffers = 1.
        bool isFsl = targetType is FixedSizeListType
                     && layout.ValueCompression?.CompressionCase
                        == CompressiveEncoding.CompressionOneofCase.FixedSizeList;
        ulong maxBuffers = isFsl ? 2UL : 1UL;
        if (layout.NumBuffers < 1 || layout.NumBuffers > maxBuffers)
            throw new NotImplementedException(
                $"MiniBlockLayout with num_buffers={layout.NumBuffers} is not yet supported (target {targetType}).");

        // The Decode entry point handles primitive / string / FSL targets;
        // list targets are routed through the recursive walker before
        // reaching this method.
        if (layout.Layers.Count != 1)
            throw new NotImplementedException(
                $"MiniBlockLayout with {layout.Layers.Count} layers for target {targetType} is not yet supported.");

        RepDefLayer layer = layout.Layers[0];
        if (layer != RepDefLayer.RepdefAllValidItem && layer != RepDefLayer.RepdefNullableItem)
            throw new NotImplementedException(
                $"MiniBlockLayout RepDefLayer '{layer}' is not yet supported for {targetType}.");
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

    /// <summary>
    /// Decode a MiniBlockLayout page targeting <see cref="BooleanType"/>.
    /// Each chunk's value buffer holds <c>itemsInChunk</c> 1-bit values
    /// packed LSB-first within bytes, occupying <c>ceil(items/8)</c> bytes.
    /// We assemble a single page-level bit buffer by concatenating chunk
    /// bits at the bit cursor, which keeps non-byte-aligned chunk sizes
    /// (e.g. an unusual 4-item non-last chunk) correct without alignment
    /// gaps between chunks.
    /// </summary>
    private static IArrowArray DecodeBoolMiniBlock(
        ReadOnlySpan<byte> chunkData, List<MiniChunk> chunks,
        bool hasDef, bool hasLargeChunk,
        CompressiveEncoding valueEnc, CompressiveEncoding? defComp,
        long numItems)
    {
        if (valueEnc.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat
            || valueEnc.Flat.BitsPerValue != 1)
        {
            throw new NotImplementedException(
                $"Bool MiniBlock requires Flat(bits_per_value=1); got " +
                $"{valueEnc.CompressionCase}/{valueEnc.Flat?.BitsPerValue}.");
        }

        int length = checked((int)numItems);
        byte[] valueBitmap = new byte[(length + 7) / 8];
        byte[]? validityBitmap = hasDef ? new byte[(length + 7) / 8] : null;
        int nullCount = 0;

        long globalChunkByteOffset = 0;
        int valueBitCursor = 0;

        foreach (MiniChunk chunk in chunks)
        {
            int itemsInChunk = (int)chunk.NumValues;
            ReadOnlySpan<byte> chunkBytes = chunkData.Slice(
                (int)globalChunkByteOffset, (int)chunk.SizeBytes);

            int cursor = 0;
            int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += 2;
            int defSize = 0;
            if (hasDef)
            {
                defSize = hasLargeChunk
                    ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
                    : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
                cursor += hasLargeChunk ? 4 : 2;
            }
            int valueBufSize = hasLargeChunk
                ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
                : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += hasLargeChunk ? 4 : 2;
            cursor = AlignUp(cursor, MiniBlockAlignment);

            if (hasDef)
            {
                ushort[] chunkDef = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, defSize), itemsInChunk, defComp!);
                for (int i = 0; i < itemsInChunk; i++)
                {
                    int idx = valueBitCursor + i;
                    if (chunkDef[i] != 0)
                    {
                        nullCount++;
                    }
                    else
                    {
                        validityBitmap![idx >> 3] |= (byte)(1 << (idx & 7));
                    }
                }
                cursor += defSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }

            int chunkValueBytes = (itemsInChunk + 7) / 8;
            if (valueBufSize < chunkValueBytes)
                throw new LanceFormatException(
                    $"Bool chunk value_buf_size={valueBufSize} < expected " +
                    $"{chunkValueBytes} for {itemsInChunk} bits.");

            ReadOnlySpan<byte> srcBits = chunkBytes.Slice(cursor, chunkValueBytes);
            // Fast path: the chunk starts at a byte boundary in the global
            // bit buffer AND has a multiple-of-8 item count. Then a memcpy
            // suffices. Otherwise fall back to bit-by-bit OR.
            if ((valueBitCursor & 7) == 0 && (itemsInChunk & 7) == 0)
            {
                srcBits.CopyTo(valueBitmap.AsSpan(valueBitCursor >> 3, chunkValueBytes));
            }
            else
            {
                for (int i = 0; i < itemsInChunk; i++)
                {
                    if (((srcBits[i >> 3] >> (i & 7)) & 1) != 0)
                    {
                        int idx = valueBitCursor + i;
                        valueBitmap[idx >> 3] |= (byte)(1 << (idx & 7));
                    }
                }
            }
            valueBitCursor += itemsInChunk;

            globalChunkByteOffset += (long)chunk.SizeBytes;
        }

        ArrowBuffer validity = validityBitmap is null
            ? ArrowBuffer.Empty
            : new ArrowBuffer(validityBitmap);
        var data = new ArrayData(
            BooleanType.Default, length, nullCount, 0,
            new[] { validity, new ArrowBuffer(valueBitmap) });
        return new BooleanArray(data);
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

        // FSL value-compression: each "item" is one FSL row of
        // dimension * inner-flat-bytes wide. The cascade walker treats it
        // exactly like a fixed-width primitive of that width — the bytes
        // are reassembled into a child Arrow array at the FSL level later.
        if (encoding.CompressionCase == CompressiveEncoding.CompressionOneofCase.FixedSizeList
            && targetType is FixedSizeListType fsl)
        {
            FixedSizeList fslEnc = encoding.FixedSizeList;
            if (fslEnc.Values?.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat)
                throw new NotImplementedException(
                    "FSL MiniBlock with non-Flat inner values is not yet supported.");
            if ((ulong)fsl.ListSize != fslEnc.ItemsPerValue)
                throw new LanceFormatException(
                    $"FSL dimension mismatch: schema={fsl.ListSize} vs encoding={fslEnc.ItemsPerValue}.");
            int innerBytes = ResolveFlatBytesPerValue(fslEnc.Values, fsl.ValueDataType);
            return checked((int)fslEnc.ItemsPerValue * innerBytes);
        }

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
            // Should not reach here — Decode() routes BooleanType through
            // DecodeBoolMiniBlock before this byte-aligned helper. Guard
            // anyway in case a new caller forgets the dispatch.
            throw new InvalidOperationException(
                "BooleanType reached BuildFixedWidthArray; route bool pages through DecodeBoolMiniBlock instead.");
        }

        var data = new ArrayData(
            targetType, length, nullCount, offset: 0,
            new[] { validity, new ArrowBuffer(valueBytes) });
        return ArrowArrayFactory.BuildArray(data);
    }

    // --- v2.1 nested-leaf chunk reader: shared rep/def + per-leaf values ---

    /// <summary>
    /// Read a leaf column belonging to a v2.1 nested shape (list-of-struct,
    /// struct-of-struct, struct-of-list, etc.). Returns the raw rep/def
    /// buffers and the leaf's value bytes; assembly into Arrow arrays is
    /// the caller's job, since rep/def are shared across all sibling
    /// leaves and must be reconciled there.
    ///
    /// <para>Walks every chunk in the page and concatenates their
    /// rep/def levels and value bytes end-to-end. The returned arrays
    /// have lengths equal to the page-level totals: <c>visibleItems =
    /// layout.NumItems</c>; <c>rep.Length = def.Length</c> = sum of
    /// per-chunk <c>num_levels</c> headers.</para>
    ///
    /// <para>Constraints: <c>num_buffers = 1</c>, no <c>dictionary</c>,
    /// value compression <see cref="Flat"/>. Layer validation
    /// (item/struct/list shape, nullability matrix, etc.) is the caller's
    /// responsibility — this method only enforces the wire invariants
    /// common to every nested shape.</para>
    /// </summary>
    public static (byte[] ValueBytes, ushort[]? Rep, ushort[]? Def, byte[]? InnerValidity, int InnerNullCount, int ValueBytesPerItem, int VisibleItems, int[]? VarOffsets, byte[]? VarData)
        DecodeNestedLeafChunk(MiniBlockLayout layout, IArrowType childType, in PageContext context)
    {
        if (layout.Dictionary is not null)
            throw new NotImplementedException(
                "Nested-leaf MiniBlockLayout with a dictionary is not supported.");

        CompressiveEncoding valueEnc = layout.ValueCompression
            ?? throw new LanceFormatException("MiniBlockLayout has no value_compression.");

        // Variable-width path (string/binary leaf inside a list/struct).
        // Each chunk's value buffer is (chunkVisible+1) u32 offsets +
        // concatenated data bytes (raw for Variable, FSST-compressed for
        // Fsst); we accumulate into page-level offsets[] + data buffer,
        // then return alongside rep/def for the cascade walker. valueBytes
        // is unused on this path.
        bool isVariable = valueEnc.CompressionCase == CompressiveEncoding.CompressionOneofCase.Variable;
        bool isFsst = valueEnc.CompressionCase == CompressiveEncoding.CompressionOneofCase.Fsst;
        if ((isVariable || isFsst) && childType is not (StringType or BinaryType))
            throw new NotImplementedException(
                $"Nested-leaf {valueEnc.CompressionCase} encoding is only supported for String/Binary leaves (got {childType}).");
        if (isVariable)
            return DecodeNestedLeafChunkVariable(layout, valueEnc, context);
        if (isFsst)
            return DecodeNestedLeafChunkFsst(layout, valueEnc, context);

        // FSL with has_validity=true adds an extra inner-validity bitmap
        // buffer per chunk and bumps num_buffers from 1 to 2. The chunk
        // header gains a u16 validity_size field after def_size and before
        // valueBufSize. Detect this case once up front so the per-chunk
        // loop knows what to read.
        bool fslHasValidity = false;
        int fslDim = 0;
        if (valueEnc.CompressionCase == CompressiveEncoding.CompressionOneofCase.FixedSizeList
            && childType is FixedSizeListType fslChild)
        {
            fslHasValidity = valueEnc.FixedSizeList.HasValidity;
            fslDim = checked((int)valueEnc.FixedSizeList.ItemsPerValue);
        }

        ulong expectedNumBuffers = fslHasValidity ? 2UL : 1UL;
        if (layout.NumBuffers != expectedNumBuffers)
            throw new NotImplementedException(
                $"Nested-leaf MiniBlockLayout num_buffers={layout.NumBuffers} is not supported " +
                $"(expected {expectedNumBuffers} for value_compression={valueEnc.CompressionCase}" +
                (fslHasValidity ? "/has_validity=true" : "") + ").");

        bool hasRep = layout.RepCompression is not null;
        bool hasDef = layout.DefCompression is not null;

        long numItems = checked((long)layout.NumItems);
        bool hasLargeChunk = layout.HasLargeChunk;

        ReadOnlySpan<byte> chunkMeta = context.PageBuffers[0].Span;
        ReadOnlySpan<byte> chunkData = context.PageBuffers[1].Span;
        var chunks = ParseChunkMetadata(chunkMeta, hasLargeChunk, numItems);

        // InlineBitpacking variant: the chunk's value buffer holds a
        // sizeof(T)-byte bit-width header + Fastlanes-packed values, not
        // raw little-endian values. Detect up front so the per-chunk
        // loop can route to DecodeInlineBitpackingChunk and the size of
        // the on-disk value bytes is decoupled from the unpacked size.
        bool inlineBitpacking = valueEnc.CompressionCase
            == CompressiveEncoding.CompressionOneofCase.InlineBitpacking;
        int valueBytesPerItem = inlineBitpacking
            ? ResolveInlineBitpackingBytesPerValue(valueEnc.InlineBitpacking, childType)
            : ResolveFlatBytesPerValue(valueEnc, childType);

        int visibleItems = checked((int)numItems);
        var valueBytes = new byte[visibleItems * valueBytesPerItem];

        // Inner validity bitmap (only for FSL with has_validity=true).
        // Total inner items = visibleItems * fslDim; bitmap is one bit per
        // inner item, LSB-first within each byte.
        int totalInnerItems = fslHasValidity ? checked(visibleItems * fslDim) : 0;
        byte[]? innerValidity = fslHasValidity
            ? new byte[(totalInnerItems + 7) / 8]
            : null;
        int innerValidityWritePos = 0; // measured in inner items, not bytes
        int innerNullCount = 0;

        // rep/def lengths sum across chunks; we don't know the total up
        // front (per-chunk num_levels can vary, especially for list shapes
        // where rep levels include list-level entries beyond visible items).
        // Pre-walk the chunks once to total the level counts so we can
        // allocate exact-size arrays.
        int totalLevels = 0;
        if (hasRep || hasDef)
        {
            long byteCursor = 0;
            foreach (MiniChunk c in chunks)
            {
                int n = BinaryPrimitives.ReadUInt16LittleEndian(
                    chunkData.Slice((int)byteCursor, 2));
                totalLevels = checked(totalLevels + n);
                byteCursor += (long)c.SizeBytes;
            }
        }

        ushort[]? rep = hasRep ? new ushort[totalLevels] : null;
        ushort[]? def = hasDef ? new ushort[totalLevels] : null;

        long globalChunkByteOffset = 0;
        int valueWritePos = 0;
        int levelWritePos = 0;

        foreach (MiniChunk chunk in chunks)
        {
            int chunkVisibleItems = checked((int)chunk.NumValues);
            ReadOnlySpan<byte> chunkBytes = chunkData.Slice(
                (int)globalChunkByteOffset, (int)chunk.SizeBytes);

            int cursor = 0;
            int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += 2;
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
            int validityBufSize = 0;
            if (fslHasValidity)
            {
                validityBufSize = hasLargeChunk
                    ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
                    : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
                cursor += hasLargeChunk ? 4 : 2;
            }
            int valueBufSize = hasLargeChunk
                ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
                : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += hasLargeChunk ? 4 : 2;
            cursor = AlignUp(cursor, MiniBlockAlignment);

            if (hasRep)
            {
                // Reuse the existing rep/def decompressor — it handles both
                // Flat(u16) and InlineBitpacking(16). pylance often emits
                // InlineBitpacking for rep on lists with > ~100 rows.
                ushort[] chunkRep = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, repSize), numLevels, layout.RepCompression!);
                chunkRep.AsSpan().CopyTo(rep.AsSpan(levelWritePos, numLevels));
                cursor += repSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }

            if (hasDef)
            {
                ushort[] chunkDef = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, defSize), numLevels, layout.DefCompression!);
                chunkDef.AsSpan().CopyTo(def.AsSpan(levelWritePos, numLevels));
                cursor += defSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }

            if (fslHasValidity)
            {
                int chunkInnerItems = chunkVisibleItems * fslDim;
                int chunkValidityBytesNeeded = (chunkInnerItems + 7) / 8;
                if (validityBufSize < chunkValidityBytesNeeded)
                    throw new LanceFormatException(
                        $"FSL inner-validity buffer {validityBufSize} < expected " +
                        $"{chunkValidityBytesNeeded} for {chunkInnerItems} inner items.");
                AppendBitsLsbFirst(
                    chunkBytes.Slice(cursor, chunkValidityBytesNeeded),
                    chunkInnerItems,
                    innerValidity!,
                    innerValidityWritePos,
                    out int chunkInnerNullCount);
                innerNullCount = checked(innerNullCount + chunkInnerNullCount);
                innerValidityWritePos += chunkInnerItems;
                cursor += validityBufSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }

            if (inlineBitpacking)
            {
                // Hand the chunk's packed buffer (header + packed bytes) to
                // the existing fastlanes unpack helper; it writes
                // chunkVisibleItems decompressed values into the global
                // valueBytes array starting at valueWritePos / valueBytesPerItem.
                DecodeInlineBitpackingChunk(
                    chunkBytes.Slice(cursor, valueBufSize),
                    chunkVisibleItems, valueBytesPerItem,
                    valueBytes, valueWritePos / valueBytesPerItem);
                valueWritePos += chunkVisibleItems * valueBytesPerItem;
            }
            else
            {
                int chunkValueBytes = chunkVisibleItems * valueBytesPerItem;
                if (valueBufSize < chunkValueBytes)
                    throw new LanceFormatException(
                        $"Nested-leaf chunk value buffer {valueBufSize} < expected " +
                        $"{chunkValueBytes} for {chunkVisibleItems} visible items.");
                chunkBytes.Slice(cursor, chunkValueBytes)
                    .CopyTo(valueBytes.AsSpan(valueWritePos));
                valueWritePos += chunkValueBytes;
            }

            levelWritePos += numLevels;
            globalChunkByteOffset += (long)chunk.SizeBytes;
        }

        return (valueBytes, rep, def, innerValidity, innerNullCount, valueBytesPerItem, visibleItems,
            /*VarOffsets*/ null, /*VarData*/ null);
    }

    /// <summary>
    /// Variable-width branch of <see cref="DecodeNestedLeafChunk"/>: walks
    /// every chunk in the page, parses each chunk's per-row offsets+data,
    /// and accumulates them into a single page-level (offsets, data) pair
    /// suitable for the cascade walker's variable-width leaf path.
    /// </summary>
    private static (byte[] ValueBytes, ushort[]? Rep, ushort[]? Def, byte[]? InnerValidity, int InnerNullCount, int ValueBytesPerItem, int VisibleItems, int[]? VarOffsets, byte[]? VarData)
        DecodeNestedLeafChunkVariable(MiniBlockLayout layout, CompressiveEncoding valueEnc, in PageContext context)
    {
        Variable variable = valueEnc.Variable;
        if (variable.Values is not null)
            throw new NotImplementedException(
                "Nested-leaf Variable encoding with BufferCompression on values is not yet supported.");
        if (variable.Offsets is null
            || variable.Offsets.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat
            || variable.Offsets.Flat.BitsPerValue != 32)
            throw new NotImplementedException(
                "Nested-leaf Variable encoding currently requires Flat(u32) offsets.");

        if (layout.NumBuffers != 1)
            throw new NotImplementedException(
                $"Nested-leaf Variable MiniBlockLayout num_buffers={layout.NumBuffers} is not supported.");

        bool hasRep = layout.RepCompression is not null;
        bool hasDef = layout.DefCompression is not null;
        long numItems = checked((long)layout.NumItems);
        bool hasLargeChunk = layout.HasLargeChunk;

        ReadOnlySpan<byte> chunkMeta = context.PageBuffers[0].Span;
        ReadOnlySpan<byte> chunkData = context.PageBuffers[1].Span;
        var chunks = ParseChunkMetadata(chunkMeta, hasLargeChunk, numItems);

        int visibleItems = checked((int)numItems);

        // Pre-pass to total rep/def levels (matches the fixed-width path).
        int totalLevels = 0;
        if (hasRep || hasDef)
        {
            long byteCursor = 0;
            foreach (MiniChunk c in chunks)
            {
                int n = BinaryPrimitives.ReadUInt16LittleEndian(
                    chunkData.Slice((int)byteCursor, 2));
                totalLevels = checked(totalLevels + n);
                byteCursor += (long)c.SizeBytes;
            }
        }
        ushort[]? rep = hasRep ? new ushort[totalLevels] : null;
        ushort[]? def = hasDef ? new ushort[totalLevels] : null;

        var globalOffsets = new int[visibleItems + 1];
        var dataChunks = new List<byte[]>(chunks.Count);
        long globalChunkByteOffset = 0;
        int rowCursor = 0;
        int levelWritePos = 0;

        foreach (MiniChunk chunk in chunks)
        {
            int chunkVisible = checked((int)chunk.NumValues);
            ReadOnlySpan<byte> chunkBytes = chunkData.Slice(
                (int)globalChunkByteOffset, (int)chunk.SizeBytes);

            int cursor = 0;
            int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += 2;
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

            if (hasRep)
            {
                ushort[] chunkRep = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, repSize), numLevels, layout.RepCompression!);
                chunkRep.AsSpan().CopyTo(rep.AsSpan(levelWritePos, numLevels));
                cursor += repSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }
            if (hasDef)
            {
                ushort[] chunkDef = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, defSize), numLevels, layout.DefCompression!);
                chunkDef.AsSpan().CopyTo(def.AsSpan(levelWritePos, numLevels));
                cursor += defSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }

            // Value buffer = (chunkVisible+1) × u32 offsets within this
            // chunk's value buffer + data bytes. Re-emit into page-level
            // offsets[] with the running global data cursor.
            ReadOnlySpan<byte> valueBuf = chunkBytes.Slice(cursor, valueBufSize);
            int offsetsBytes = checked((chunkVisible + 1) * 4);
            if (valueBuf.Length < offsetsBytes)
                throw new LanceFormatException(
                    $"Variable nested-leaf chunk value buffer too small: need {offsetsBytes} offset bytes, have {valueBuf.Length}.");

            uint baseOffset = BinaryPrimitives.ReadUInt32LittleEndian(valueBuf.Slice(0, 4));
            int globalDataBase = rowCursor == 0 ? 0 : globalOffsets[rowCursor];
            int chunkDataLen = 0;
            for (int i = 1; i <= chunkVisible; i++)
            {
                uint absolute = BinaryPrimitives.ReadUInt32LittleEndian(valueBuf.Slice(i * 4, 4));
                if (absolute < baseOffset)
                    throw new LanceFormatException(
                        $"Variable offset {absolute} at index {i} is less than base {baseOffset}.");
                int rel = checked((int)(absolute - baseOffset));
                if (rel > chunkDataLen) chunkDataLen = rel;
                globalOffsets[rowCursor + i] = checked(globalDataBase + rel);
            }

            int dataStart = checked((int)baseOffset);
            if (dataStart + chunkDataLen > valueBuf.Length)
                throw new LanceFormatException(
                    $"Variable chunk data range [{dataStart}, {dataStart + chunkDataLen}) exceeds buffer length {valueBuf.Length}.");
            var chunkSlice = new byte[chunkDataLen];
            valueBuf.Slice(dataStart, chunkDataLen).CopyTo(chunkSlice);
            dataChunks.Add(chunkSlice);

            rowCursor += chunkVisible;
            levelWritePos += numLevels;
            globalChunkByteOffset += (long)chunk.SizeBytes;
        }

        int totalData = 0;
        foreach (var d in dataChunks) totalData = checked(totalData + d.Length);
        var data = new byte[totalData];
        int writePos = 0;
        foreach (var d in dataChunks)
        {
            d.CopyTo(data, writePos);
            writePos += d.Length;
        }

        return (System.Array.Empty<byte>(), rep, def, /*innerValidity*/ null, /*innerNullCount*/ 0,
            /*ValueBytesPerItem*/ 0, visibleItems, globalOffsets, data);
    }

    /// <summary>
    /// FSST branch of <see cref="DecodeNestedLeafChunk"/>: the value
    /// buffer in each chunk is FSST-compressed via an inner
    /// <see cref="Variable"/>(Flat u32 offsets) layout. Walks every chunk
    /// to extract per-row compressed slices into a single accumulator,
    /// then bulk FSST-decompresses to produce the page-level
    /// (offsets, data) pair the cascade walker expects.
    /// </summary>
    private static (byte[] ValueBytes, ushort[]? Rep, ushort[]? Def, byte[]? InnerValidity, int InnerNullCount, int ValueBytesPerItem, int VisibleItems, int[]? VarOffsets, byte[]? VarData)
        DecodeNestedLeafChunkFsst(MiniBlockLayout layout, CompressiveEncoding valueEnc, in PageContext context)
    {
        Fsst fsst = valueEnc.Fsst;
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

        if (layout.NumBuffers != 1)
            throw new NotImplementedException(
                $"Nested-leaf Fsst MiniBlockLayout num_buffers={layout.NumBuffers} is not supported.");

        bool hasRep = layout.RepCompression is not null;
        bool hasDef = layout.DefCompression is not null;
        long numItems = checked((long)layout.NumItems);
        bool hasLargeChunk = layout.HasLargeChunk;

        ReadOnlySpan<byte> chunkMeta = context.PageBuffers[0].Span;
        ReadOnlySpan<byte> chunkData = context.PageBuffers[1].Span;
        var chunks = ParseChunkMetadata(chunkMeta, hasLargeChunk, numItems);

        int visibleItems = checked((int)numItems);

        int totalLevels = 0;
        if (hasRep || hasDef)
        {
            long byteCursor = 0;
            foreach (MiniChunk c in chunks)
            {
                int n = BinaryPrimitives.ReadUInt16LittleEndian(
                    chunkData.Slice((int)byteCursor, 2));
                totalLevels = checked(totalLevels + n);
                byteCursor += (long)c.SizeBytes;
            }
        }
        ushort[]? rep = hasRep ? new ushort[totalLevels] : null;
        ushort[]? def = hasDef ? new ushort[totalLevels] : null;

        Clast.Fsst.FsstDecoder fsstDecoder = BuildLanceFsstDecoder(fsst.SymbolTable.Span);
        var compressedAccum = new EngineeredWood.Buffers.GrowableBuffer(64 * 1024);
        var compressedLengths = new int[visibleItems];
        long globalChunkByteOffset = 0;
        int rowCursor = 0;
        int levelWritePos = 0;

        foreach (MiniChunk chunk in chunks)
        {
            int chunkVisible = checked((int)chunk.NumValues);
            ReadOnlySpan<byte> chunkBytes = chunkData.Slice(
                (int)globalChunkByteOffset, (int)chunk.SizeBytes);

            int cursor = 0;
            int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += 2;
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

            if (hasRep)
            {
                ushort[] chunkRep = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, repSize), numLevels, layout.RepCompression!);
                chunkRep.AsSpan().CopyTo(rep.AsSpan(levelWritePos, numLevels));
                cursor += repSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }
            if (hasDef)
            {
                ushort[] chunkDef = ReadDefChunkBuffer(
                    chunkBytes.Slice(cursor, defSize), numLevels, layout.DefCompression!);
                chunkDef.AsSpan().CopyTo(def.AsSpan(levelWritePos, numLevels));
                cursor += defSize;
                cursor = AlignUp(cursor, MiniBlockAlignment);
            }

            // Value buffer = (chunkVisible+1) × u32 offsets + FSST-compressed data.
            ReadOnlySpan<byte> valueBuf = chunkBytes.Slice(cursor, valueBufSize);
            int offsetsBytes = checked((chunkVisible + 1) * 4);
            if (valueBuf.Length < offsetsBytes)
                throw new LanceFormatException(
                    $"Fsst nested-leaf chunk value buffer too small: need {offsetsBytes} offset bytes, have {valueBuf.Length}.");

            uint baseOffset = BinaryPrimitives.ReadUInt32LittleEndian(valueBuf.Slice(0, 4));
            uint prevAbs = baseOffset;
            for (int i = 0; i < chunkVisible; i++)
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
                compressedLengths[rowCursor + i] = rowLen;
                prevAbs = nextAbs;
            }

            rowCursor += chunkVisible;
            levelWritePos += numLevels;
            globalChunkByteOffset += (long)chunk.SizeBytes;
        }

        // Bulk FSST decompress to produce the Arrow data + offsets.
        ReadOnlySpan<byte> compressedAll = compressedAccum.WrittenSpan;
        int destCap = Clast.Fsst.FsstDecoder.MaxDecompressedLength(compressedAll.Length);
        byte[] destination = new byte[destCap];
        int[] destinationOffsets = new int[visibleItems + 1];
        if (!fsstDecoder.TryDecompressBatch(
                compressedAll, compressedLengths,
                destination, destinationOffsets,
                out int totalWritten))
        {
            throw new LanceFormatException(
                $"FSST cascade batch decompress failed (compressedBytes={compressedAll.Length}, " +
                $"destCap={destCap}, rows={visibleItems}).");
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

        return (System.Array.Empty<byte>(), rep, def, /*innerValidity*/ null, /*innerNullCount*/ 0,
            /*ValueBytesPerItem*/ 0, visibleItems, destinationOffsets, arrowData);
    }

    /// <summary>
    /// Append <paramref name="numBits"/> LSB-first bits from
    /// <paramref name="src"/> into <paramref name="dest"/> starting at bit
    /// position <paramref name="destBitOffset"/>. Counts cleared bits
    /// (=null inner items) into <paramref name="zeroCount"/>.
    /// </summary>
    private static void AppendBitsLsbFirst(
        ReadOnlySpan<byte> src, int numBits,
        byte[] dest, int destBitOffset, out int zeroCount)
    {
        zeroCount = 0;
        // Slow but simple per-bit append; bitmaps for FSL inner items are
        // typically small (visibleItems * dim bits). Optimize later if a
        // benchmark warrants it.
        for (int i = 0; i < numBits; i++)
        {
            bool bit = (src[i >> 3] & (1 << (i & 7))) != 0;
            int outIdx = destBitOffset + i;
            if (bit)
                dest[outIdx >> 3] |= (byte)(1 << (outIdx & 7));
            else
                zeroCount++;
        }
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
                    // header. The buffer is fastlanes-packed for one or more
                    // 1024-element blocks. pylance writes shorter buffers
                    // when the chunk has fewer than 1024 items — we pad up
                    // to the FastLanes block minimum (1024*bpv/8 bytes) and
                    // take the live prefix from the unpacked output.
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
                    int requiredBytes = checked((int)(FastLanesChunk * bpv / 8));
                    Span<ushort> full = stackalloc ushort[FastLanesChunk];
                    if (defBuf.Length >= requiredBytes)
                    {
                        Clast.FastLanes.BitPacking.UnpackChunk<ushort>((int)bpv, defBuf, full);
                    }
                    else
                    {
                        // Pad the packed area up to the full FastLanes block.
                        // The padded suffix only affects lanes whose elements
                        // are beyond itemsInChunk, which we discard below.
                        Span<byte> padded = requiredBytes <= 256
                            ? stackalloc byte[requiredBytes]
                            : new byte[requiredBytes];
                        defBuf.CopyTo(padded);
                        // remaining bytes already zero
                        Clast.FastLanes.BitPacking.UnpackChunk<ushort>((int)bpv, padded, full);
                    }
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
    internal static Clast.Fsst.FsstDecoder BuildLanceFsstDecoder(ReadOnlySpan<byte> symbolTable)
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

    /// <summary>
    /// Decode a v2.1 <see cref="MiniBlockLayout"/> whose value compression is
    /// <see cref="FixedSizeList"/>. Single layer <c>[NULLABLE_ITEM]</c> or
    /// <c>[ALL_VALID_ITEM]</c> at the FSL-row level (so num_items = number of
    /// FSL rows). Each chunk carries either one buffer (just the flat inner
    /// values) or two (an inner-item validity bitmap + the flat values) when
    /// <c>has_validity = true</c>.
    /// </summary>
    private static IArrowArray DecodeFslMiniBlock(
        ReadOnlySpan<byte> chunkData, List<MiniChunk> chunks,
        bool hasDef, bool hasRep, bool hasLargeChunk,
        MiniBlockLayout layout, FixedSizeList fslEnc, FixedSizeListType fslType,
        long numItems)
    {
        if (hasRep)
            throw new LanceFormatException(
                "FSL MiniBlockLayout must not have rep_compression (single-layer outer).");
        if (chunks.Count != 1)
            throw new NotImplementedException(
                "Multi-chunk FSL mini-block decoding is not yet supported.");
        if (fslEnc.Values?.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat)
            throw new NotImplementedException(
                "FSL MiniBlock with non-Flat inner values is not yet supported.");
        if ((ulong)fslType.ListSize != fslEnc.ItemsPerValue)
            throw new LanceFormatException(
                $"FSL dimension mismatch: schema={fslType.ListSize} vs encoding={fslEnc.ItemsPerValue}.");

        bool hasValidity = fslEnc.HasValidity;
        ulong expectedBuffers = hasValidity ? 2UL : 1UL;
        if (layout.NumBuffers != expectedBuffers)
            throw new LanceFormatException(
                $"FSL MiniBlock num_buffers={layout.NumBuffers} != expected {expectedBuffers} for has_validity={hasValidity}.");

        int numRows = checked((int)numItems);
        int itemsPerValue = checked((int)fslEnc.ItemsPerValue);
        int totalInnerItems = checked(numRows * itemsPerValue);
        int innerBytesPerItem = ResolveFlatBytesPerValue(fslEnc.Values, fslType.ValueDataType);

        ReadOnlySpan<byte> chunkBytes = chunkData.Slice(0, (int)chunks[0].SizeBytes);
        int cursor = 0;
        int numLevels = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2)); cursor += 2;
        if (numLevels != numRows)
            throw new LanceFormatException(
                $"FSL chunk num_levels={numLevels} != num_rows={numRows} (single-chunk path).");
        int defSize = 0;
        if (hasDef)
        {
            defSize = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += 2;
        }
        int validityBufSize = 0;
        if (hasValidity)
        {
            validityBufSize = hasLargeChunk
                ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
                : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
            cursor += hasLargeChunk ? 4 : 2;
        }
        int valuesBufSize = hasLargeChunk
            ? (int)BinaryPrimitives.ReadUInt32LittleEndian(chunkBytes.Slice(cursor, 4))
            : BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor, 2));
        cursor += hasLargeChunk ? 4 : 2;
        cursor = AlignUp(cursor, MiniBlockAlignment);

        // Row-level validity from def buffer (def=0 row valid, def=1 row null).
        byte[]? rowValidity = null;
        int rowNullCount = 0;
        if (hasDef)
        {
            if (defSize != numLevels * sizeof(ushort))
                throw new NotImplementedException(
                    $"FSL def buffer size {defSize} != {numLevels * 2} (non-Flat(16) def is not supported).");
            rowValidity = new byte[(numRows + 7) / 8];
            for (int i = 0; i < numRows; i++)
            {
                ushort d = BinaryPrimitives.ReadUInt16LittleEndian(chunkBytes.Slice(cursor + i * 2, 2));
                if (d == 0) rowValidity[i >> 3] |= (byte)(1 << (i & 7));
                else rowNullCount++;
            }
            cursor += defSize;
            cursor = AlignUp(cursor, MiniBlockAlignment);
        }

        // Inner-item validity bitmap (when has_validity = true). The wire
        // bitmap is contiguous over all FSL items; we copy it as-is and let
        // the consumer index by visible-item position.
        byte[]? innerValidity = null;
        int innerNullCount = 0;
        if (hasValidity)
        {
            int expectedValidityBytes = (totalInnerItems + 7) / 8;
            if (validityBufSize < expectedValidityBytes)
                throw new LanceFormatException(
                    $"FSL inner validity buffer {validityBufSize} < expected {expectedValidityBytes} for {totalInnerItems} items.");
            innerValidity = new byte[expectedValidityBytes];
            chunkBytes.Slice(cursor, expectedValidityBytes).CopyTo(innerValidity);
            for (int i = 0; i < totalInnerItems; i++)
                if ((innerValidity[i >> 3] & (1 << (i & 7))) == 0)
                    innerNullCount++;
            cursor += validityBufSize;
            cursor = AlignUp(cursor, MiniBlockAlignment);
        }

        int valueBytesNeeded = checked(totalInnerItems * innerBytesPerItem);
        if (valuesBufSize < valueBytesNeeded)
            throw new LanceFormatException(
                $"FSL value buffer {valuesBufSize} < expected {valueBytesNeeded} for {totalInnerItems} items.");
        var valueBytes = new byte[valueBytesNeeded];
        chunkBytes.Slice(cursor, valueBytesNeeded).CopyTo(valueBytes);

        ArrowBuffer innerValidityBuf = (innerValidity is not null && innerNullCount > 0)
            ? new ArrowBuffer(innerValidity)
            : ArrowBuffer.Empty;
        var childData = new ArrayData(
            fslType.ValueDataType, totalInnerItems, innerNullCount, offset: 0,
            new[] { innerValidityBuf, new ArrowBuffer(valueBytes) });
        IArrowArray childArr = ArrowArrayFactory.BuildArray(childData);

        ArrowBuffer rowValidityBuf = (rowValidity is not null && rowNullCount > 0)
            ? new ArrowBuffer(rowValidity)
            : ArrowBuffer.Empty;
        var fslData = new ArrayData(
            fslType, numRows, rowNullCount, offset: 0,
            new[] { rowValidityBuf },
            children: new[] { childArr.Data });
        return new FixedSizeListArray(fslData);
    }
}
