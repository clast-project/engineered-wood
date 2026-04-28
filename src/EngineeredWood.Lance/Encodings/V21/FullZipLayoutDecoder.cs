// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.Lance.Proto.Encodings.V21;

namespace EngineeredWood.Lance.Encodings.V21;

/// <summary>
/// Decodes a Lance v2.1 <see cref="FullZipLayout"/> page.
///
/// <para>FullZip is the "one I/O per value" layout intended for large
/// individual values — embeddings, images, big binaries. Values are
/// concatenated in a single page buffer so each value can be addressed
/// by seeking to <c>row_index × bits_per_value / 8</c>.</para>
///
/// <para>Two paths are supported:
/// <list type="bullet">
/// <item><b>Fixed-width</b> (<see cref="FullZipLayout.BitsPerValue"/>) with
///   <see cref="Flat"/> or <see cref="FixedSizeList"/> value_compression —
///   the embedding / feature-vector case (no rep/def).</item>
/// <item><b>Variable-width</b> (<see cref="FullZipLayout.BitsPerOffset"/>)
///   with <c>General(ZSTD, Variable(Flat(u32)))</c> value_compression —
///   the very-large-string case. Each row's chunk is wrapped with a
///   12-byte framing header (u32 LE compressed_remainder + u64 LE
///   uncompressed_size) followed by a standard ZSTD frame. The nullable
///   variant prepends a 1-byte def marker (0=valid, 1=null; null rows
///   carry no further bytes).</item>
/// </list></para>
/// </summary>
internal static class FullZipLayoutDecoder
{
    public static IArrowArray Decode(
        FullZipLayout layout, IArrowType targetType, in PageContext context)
    {
        if (layout.BitsRep != 0)
            throw new NotImplementedException(
                $"FullZipLayout with bits_rep={layout.BitsRep} is not yet supported.");

        if (layout.DetailsCase == FullZipLayout.DetailsOneofCase.BitsPerOffset)
            return DecodeVariableWidth(layout, targetType, context);

        if (layout.BitsDef != 0)
            throw new NotImplementedException(
                $"FullZipLayout(fixed-width) with bits_def={layout.BitsDef} is not yet supported.");

        if (layout.Layers.Count != 1 || layout.Layers[0] != RepDefLayer.RepdefAllValidItem)
            throw new NotImplementedException(
                $"FullZipLayout(fixed-width) with layers other than [ALL_VALID_ITEM] is not yet supported " +
                $"(got {layout.Layers.Count} layers).");

        if (layout.DetailsCase != FullZipLayout.DetailsOneofCase.BitsPerValue)
            throw new LanceFormatException(
                "FullZipLayout has no bits_per_value or bits_per_offset set.");

        ulong bitsPerValue = layout.BitsPerValue;
        if (bitsPerValue == 0 || bitsPerValue % 8 != 0)
            throw new NotImplementedException(
                $"FullZipLayout bits_per_value={bitsPerValue} is not yet supported (must be non-zero, byte-aligned).");

        long bytesPerValue = checked((long)(bitsPerValue / 8));
        int numItems = checked((int)layout.NumItems);
        int numVisible = checked((int)layout.NumVisibleItems);
        if (numVisible != numItems)
            throw new NotImplementedException(
                $"FullZipLayout num_items != num_visible_items ({numItems} vs {numVisible}) is not yet supported.");

        long totalBytes = checked(numItems * bytesPerValue);
        ReadOnlySpan<byte> payload = context.PageBuffers[0].Span;
        if (payload.Length < totalBytes)
            throw new LanceFormatException(
                $"FullZip page buffer too small: need {totalBytes} bytes, have {payload.Length}.");

        CompressiveEncoding valueEnc = layout.ValueCompression
            ?? throw new LanceFormatException("FullZipLayout has no value_compression.");

        // Extract the raw values as a byte array (one-copy). In the future we
        // could slice directly from the pooled buffer.
        var valueBytes = payload.Slice(0, (int)totalBytes).ToArray();

        return BuildArray(valueBytes, numItems, bytesPerValue, valueEnc, targetType);
    }

    private static IArrowArray BuildArray(
        byte[] valueBytes, int numItems, long bytesPerValue,
        CompressiveEncoding valueEnc, IArrowType targetType)
    {
        switch (valueEnc.CompressionCase)
        {
            case CompressiveEncoding.CompressionOneofCase.Flat:
                return BuildFromFlat(valueBytes, numItems, bytesPerValue, valueEnc.Flat, targetType);

            case CompressiveEncoding.CompressionOneofCase.FixedSizeList:
                return BuildFromFixedSizeList(
                    valueBytes, numItems, bytesPerValue, valueEnc.FixedSizeList, targetType);

            default:
                throw new NotImplementedException(
                    $"FullZipLayout value_compression '{valueEnc.CompressionCase}' is not yet supported.");
        }
    }

    private static IArrowArray BuildFromFlat(
        byte[] valueBytes, int numItems, long bytesPerValue,
        Flat flat, IArrowType targetType)
    {
        if (flat.Data is not null)
            throw new NotImplementedException(
                "FullZipLayout Flat with BufferCompression is not yet supported.");
        if (flat.BitsPerValue != (ulong)(bytesPerValue * 8))
            throw new LanceFormatException(
                $"FullZip outer bits_per_value={bytesPerValue * 8} does not match inner Flat.bits_per_value={flat.BitsPerValue}.");

        if (targetType is not FixedWidthType)
            throw new LanceFormatException(
                $"FullZipLayout(Flat) cannot target non-fixed-width type {targetType}.");

        var data = new ArrayData(
            targetType, numItems, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(valueBytes) });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildFromFixedSizeList(
        byte[] valueBytes, int numItems, long bytesPerValue,
        FixedSizeList fsl, IArrowType targetType)
    {
        if (targetType is not FixedSizeListType fslType)
            throw new LanceFormatException(
                $"FullZipLayout(FixedSizeList) cannot target non-FSL type {targetType}.");

        if ((ulong)fslType.ListSize != fsl.ItemsPerValue)
            throw new LanceFormatException(
                $"FSL dimension mismatch: schema={fslType.ListSize} vs encoding={fsl.ItemsPerValue}.");

        if (fsl.HasValidity)
            throw new NotImplementedException(
                "FullZipLayout(FixedSizeList) with has_validity is not yet supported.");

        if (fsl.Values is null
            || fsl.Values.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat)
            throw new NotImplementedException(
                "FullZipLayout(FixedSizeList) with non-Flat inner values is not yet supported.");

        ulong innerBits = fsl.Values.Flat.BitsPerValue;
        ulong expectedInnerBits = (ulong)bytesPerValue * 8UL / fsl.ItemsPerValue;
        if (innerBits != expectedInnerBits)
            throw new LanceFormatException(
                $"FSL inner bits_per_value={innerBits} != expected {expectedInnerBits} based on outer bits_per_value.");

        if (fslType.ValueDataType is not FixedWidthType innerFw || (ulong)innerFw.BitWidth != innerBits)
            throw new LanceFormatException(
                $"FSL inner Arrow type {fslType.ValueDataType} does not match encoding bits={innerBits}.");

        int totalItems = checked(numItems * (int)fsl.ItemsPerValue);
        var childData = new ArrayData(
            fslType.ValueDataType, totalItems, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(valueBytes) });
        IArrowArray childArray = ArrowArrayFactory.BuildArray(childData);

        var fslData = new ArrayData(
            fslType, numItems, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty },
            children: new[] { childArray.Data });
        return new FixedSizeListArray(fslData);
    }

    // --- Variable-width FullZip (very large strings / binaries) ---

    /// <summary>
    /// Decode a <see cref="FullZipLayout"/> with <c>bits_per_offset</c> set —
    /// the variable-width path used for string / binary columns whose values
    /// are individually large enough to warrant per-value compression.
    ///
    /// <para>Wire layout: two page buffers.
    /// <list type="bullet">
    /// <item>Buffer 0: concatenated per-row payloads.</item>
    /// <item>Buffer 1: <c>(num_items + 1) × u32 LE</c> offsets into buffer 0.</item>
    /// </list></para>
    ///
    /// <para>Per-row payload structure:
    /// <list type="number">
    /// <item>Optional 1-byte def marker when <c>bits_def == 1</c>:
    ///   <c>0x00 = valid</c>, <c>0x01 = null</c> (and the row payload ends here).</item>
    /// <item>4-byte LE <c>compressed_remainder_size</c> — bytes after this u32
    ///   (i.e. 8-byte uncompressed-size + ZSTD frame).</item>
    /// <item>8-byte LE <c>uncompressed_size</c> — original value byte length.</item>
    /// <item>Standard ZSTD frame (with magic <c>28 B5 2F FD</c>).</item>
    /// </list></para>
    /// </summary>
    private static IArrowArray DecodeVariableWidth(
        FullZipLayout layout, IArrowType targetType, in PageContext context)
    {
        if (layout.BitsPerOffset != 32)
            throw new NotImplementedException(
                $"FullZipLayout(variable-width) bits_per_offset={layout.BitsPerOffset} is not yet supported (only 32 is handled).");

        if (targetType is not (StringType or BinaryType))
            throw new NotImplementedException(
                $"FullZipLayout(variable-width) targeting non-string/binary type {targetType} is not yet supported.");

        bool hasDef = layout.BitsDef != 0;
        if (hasDef)
        {
            if (layout.BitsDef != 1)
                throw new NotImplementedException(
                    $"FullZipLayout(variable-width) bits_def={layout.BitsDef} is not yet supported (only 0 or 1 are handled).");
            if (layout.Layers.Count != 1 || layout.Layers[0] != RepDefLayer.RepdefNullableItem)
                throw new NotImplementedException(
                    $"FullZipLayout(variable-width, bits_def=1) with layers other than [NULLABLE_ITEM] is not yet supported.");
        }
        else if (layout.Layers.Count != 1 || layout.Layers[0] != RepDefLayer.RepdefAllValidItem)
        {
            throw new NotImplementedException(
                $"FullZipLayout(variable-width, bits_def=0) with layers other than [ALL_VALID_ITEM] is not yet supported.");
        }

        int numItems = checked((int)layout.NumItems);
        int numVisible = checked((int)layout.NumVisibleItems);
        if (numVisible != numItems)
            throw new NotImplementedException(
                $"FullZipLayout num_items != num_visible_items ({numItems} vs {numVisible}) is not yet supported.");

        if (context.PageBuffers.Count < 2)
            throw new LanceFormatException(
                $"FullZipLayout(variable-width) requires 2 page buffers; got {context.PageBuffers.Count}.");

        ValidateGeneralZstdVariable(
            layout.ValueCompression
                ?? throw new LanceFormatException("FullZipLayout has no value_compression."));

        ReadOnlySpan<byte> valuesBuf = context.PageBuffers[0].Span;
        ReadOnlySpan<byte> offsetsBuf = context.PageBuffers[1].Span;

        int expectedOffsetBytes = (numItems + 1) * 4;
        if (offsetsBuf.Length < expectedOffsetBytes)
            throw new LanceFormatException(
                $"FullZipLayout(variable-width) offsets buffer too small: have {offsetsBuf.Length}, need {expectedOffsetBytes}.");

        // Read row offsets into buffer 0.
        var rowOffsets = new uint[numItems + 1];
        for (int i = 0; i <= numItems; i++)
            rowOffsets[i] = BinaryPrimitives.ReadUInt32LittleEndian(offsetsBuf.Slice(i * 4, 4));

        // Decode each row. Use a growable byte buffer for the concatenated
        // value bytes and parallel int[] for Arrow offsets (string/binary).
        var dataBuf = new EngineeredWood.Buffers.GrowableBuffer(64 * 1024);
        var arrowOffsets = new int[numItems + 1];
        byte[]? validity = hasDef ? new byte[(numItems + 7) / 8] : null;
        int nullCount = 0;
        byte[] decompressScratch = new byte[64 * 1024];

        for (int i = 0; i < numItems; i++)
        {
            int rowStart = checked((int)rowOffsets[i]);
            int rowEnd = checked((int)rowOffsets[i + 1]);
            int rowLen = rowEnd - rowStart;
            if (rowLen <= 0)
                throw new LanceFormatException(
                    $"FullZipLayout(variable-width) row {i} has non-positive size {rowLen}.");

            int cursor = rowStart;
            if (hasDef)
            {
                byte defByte = valuesBuf[cursor++];
                if (defByte != 0)
                {
                    // Null row — leaves Arrow offset unchanged from its predecessor
                    // (so the slice [arrowOffsets[i], arrowOffsets[i+1]) is empty).
                    nullCount++;
                    arrowOffsets[i + 1] = arrowOffsets[i];
                    continue;
                }
                validity![i >> 3] |= (byte)(1 << (i & 7));
            }

            if (rowEnd - cursor < 12)
                throw new LanceFormatException(
                    $"FullZipLayout(variable-width) row {i} too short for framing header (have {rowEnd - cursor} bytes).");
            uint compRem = BinaryPrimitives.ReadUInt32LittleEndian(valuesBuf.Slice(cursor, 4));
            ulong uncompSize = BinaryPrimitives.ReadUInt64LittleEndian(valuesBuf.Slice(cursor + 4, 8));
            cursor += 12;
            int zstdLen = checked((int)compRem) - 8;
            if (zstdLen < 0 || cursor + zstdLen > rowEnd)
                throw new LanceFormatException(
                    $"FullZipLayout(variable-width) row {i} framing header inconsistent " +
                    $"(compRem={compRem}, remaining={rowEnd - cursor}).");

            int decompLen = checked((int)uncompSize);
            if (decompressScratch.Length < decompLen)
                decompressScratch = new byte[decompLen];

            int actualDecomp = Decompressor.Decompress(
                CompressionCodec.Zstd,
                valuesBuf.Slice(cursor, zstdLen),
                decompressScratch.AsSpan(0, decompLen));
            if (actualDecomp != decompLen)
                throw new LanceFormatException(
                    $"FullZipLayout(variable-width) row {i}: ZSTD wrote {actualDecomp} bytes, expected {decompLen}.");

            dataBuf.Write(decompressScratch.AsSpan(0, decompLen));
            arrowOffsets[i + 1] = arrowOffsets[i] + decompLen;
        }

        // Materialise as Arrow String/Binary array.
        byte[] dataBytes = dataBuf.WrittenSpan.ToArray();
        var offsetBytes = new byte[(numItems + 1) * sizeof(int)];
        Buffer.BlockCopy(arrowOffsets, 0, offsetBytes, 0, offsetBytes.Length);

        var buffers = hasDef
            ? new[] { new ArrowBuffer(validity!), new ArrowBuffer(offsetBytes), new ArrowBuffer(dataBytes) }
            : new[] { ArrowBuffer.Empty, new ArrowBuffer(offsetBytes), new ArrowBuffer(dataBytes) };

        var arrayData = new ArrayData(targetType, numItems, nullCount, offset: 0, buffers);
        return ArrowArrayFactory.BuildArray(arrayData);
    }

    private static void ValidateGeneralZstdVariable(CompressiveEncoding enc)
    {
        if (enc.CompressionCase != CompressiveEncoding.CompressionOneofCase.General)
            throw new NotImplementedException(
                $"FullZipLayout(variable-width) value_compression must be 'General' (got '{enc.CompressionCase}').");
        General g = enc.General;
        if (g.Compression?.Scheme != CompressionScheme.CompressionAlgorithmZstd)
            throw new NotImplementedException(
                $"FullZipLayout(variable-width) only supports General(ZSTD); got '{g.Compression?.Scheme}'.");
        if (g.Values is null
            || g.Values.CompressionCase != CompressiveEncoding.CompressionOneofCase.Variable)
            throw new NotImplementedException(
                "FullZipLayout(variable-width) General must wrap a Variable encoding.");
        Variable v = g.Values.Variable;
        if (v.Values is not null)
            throw new NotImplementedException(
                "FullZipLayout(variable-width) Variable.values BufferCompression is not yet supported.");
        if (v.Offsets is null
            || v.Offsets.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat
            || v.Offsets.Flat.BitsPerValue != 32)
            throw new NotImplementedException(
                "FullZipLayout(variable-width) requires Variable.offsets = Flat(u32).");
    }
}
