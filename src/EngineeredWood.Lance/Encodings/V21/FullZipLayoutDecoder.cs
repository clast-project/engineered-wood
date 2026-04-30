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
    /// <summary>
    /// Walk a FullZipLayout page that lives inside a list/struct cascade and
    /// produce raw rep/def streams + per-row value bytes for the existing
    /// nested-leaf cascade walker. Each row in buffer 0 starts with a
    /// control word that encodes <c>rep</c> in its high bits and <c>def</c>
    /// in its low <c>bits_def</c> bits; visible items (those with
    /// <c>def &lt;= max_visible_def</c>) are followed by
    /// <c>bits_per_value/8</c> bytes of payload, while invisible items
    /// (e.g. null/empty list rows) carry only the control word.
    ///
    /// <para>Currently supports the fixed-width path (FSL of primitive,
    /// no inner has_validity). Variable-width FullZip with rep, plus
    /// FSL inner has_validity inside a list cascade, will throw with a
    /// clear NotImplementedException.</para>
    /// </summary>
    public static (byte[] ValueBytes, ushort[]? Rep, ushort[]? Def, byte[]? InnerValidity, int InnerNullCount, int ValueBytesPerItem, int VisibleItems, int[]? VarOffsets, byte[]? VarData)
        DecodeNestedLeafPage(
            FullZipLayout layout, IArrowType leafType, in PageContext context)
    {
        CompressiveEncoding valueEnc = layout.ValueCompression
            ?? throw new LanceFormatException("FullZipLayout has no value_compression.");

        if (layout.DetailsCase == FullZipLayout.DetailsOneofCase.BitsPerOffset)
            return DecodeNestedLeafPageVariable(layout, valueEnc, leafType, context);

        if (layout.DetailsCase != FullZipLayout.DetailsOneofCase.BitsPerValue)
            throw new LanceFormatException(
                "FullZipLayout has no bits_per_value or bits_per_offset.");

        // Compute bytes_per_value of the payload AFTER the ctrl word.
        ulong bitsPerValue = layout.BitsPerValue;
        if (bitsPerValue == 0 || bitsPerValue % 8 != 0)
            throw new NotImplementedException(
                $"FullZipLayout bits_per_value={bitsPerValue} is not yet supported (must be non-zero, byte-aligned).");
        int bytesPerValue = checked((int)(bitsPerValue / 8));

        // FSL with inner has_validity = true inside a list cascade: each
        // visible row's payload starts with ceil(dim/8) inner-validity
        // bits followed by dim*inner_bytes value bytes, exactly like the
        // no-rep FSL has_validity FullZip case. We thread the inner
        // validity bitmap through to the cascade walker.
        bool fslInnerValidity = valueEnc.CompressionCase == CompressiveEncoding.CompressionOneofCase.FixedSizeList
            && valueEnc.FixedSizeList.HasValidity;
        int fslDim = 0;
        int fslInnerByteSize = 0;
        int fslInnerValidityBytesPerRow = 0;
        if (fslInnerValidity)
        {
            if (leafType is not FixedSizeListType fslLeaf)
                throw new LanceFormatException(
                    $"FullZipLayout(FSL has_validity) cannot target non-FSL type {leafType}.");
            fslDim = checked((int)valueEnc.FixedSizeList.ItemsPerValue);
            fslInnerByteSize = ResolveFslInnerByteSize(valueEnc.FixedSizeList, fslLeaf);
            fslInnerValidityBytesPerRow = (fslDim + 7) / 8;
        }

        int numItems = checked((int)layout.NumItems);
        int numVisible = checked((int)layout.NumVisibleItems);

        int bitsRep = checked((int)layout.BitsRep);
        int bitsDef = checked((int)layout.BitsDef);
        int totalBits = bitsRep + bitsDef;
        int ctrlBytes = totalBits switch
        {
            0 => 0,
            <= 8 => 1,
            <= 16 => 2,
            <= 32 => 4,
            _ => throw new NotImplementedException(
                $"FullZipLayout ctrl word total_bits={totalBits} is not supported (max 32)."),
        };
        if (ctrlBytes == 0)
            throw new LanceFormatException(
                "FullZipLayout reached the nested-leaf path with bits_rep=bits_def=0; " +
                "this layout has no rep/def to walk.");

        ulong defMask = bitsDef == 0 ? 0UL : ((1UL << bitsDef) - 1UL);

        // max_visible_def = sum of item-level def slots across layers
        // (NULLABLE_ITEM contributes 1; ALL_VALID_ITEM contributes 0).
        // List-level layers contribute defs above this threshold and mark
        // invisible items.
        int maxVisibleDef = 0;
        foreach (var layer in layout.Layers)
        {
            if (layer == RepDefLayer.RepdefNullableItem)
                maxVisibleDef++;
        }

        ReadOnlySpan<byte> page = context.PageBuffers[0].Span;

        // Walk every item's ctrl word; visible items consume bytes_per_value
        // additional bytes of payload. When FSL has_validity is on, the
        // payload starts with ceil(dim/8) inner-validity bits before the
        // dim*inner_bytes value bytes — strip and concat into a global
        // bitmap covering numVisible * dim inner items.
        var rep = bitsRep > 0 ? new ushort[numItems] : null;
        var def = bitsDef > 0 ? new ushort[numItems] : null;
        int valueBytesPerVisibleItem = fslInnerValidity
            ? fslDim * fslInnerByteSize
            : bytesPerValue;
        var valueBytes = new byte[checked(numVisible * valueBytesPerVisibleItem)];
        byte[]? innerValidity = null;
        int innerNullCount = 0;
        if (fslInnerValidity)
            innerValidity = new byte[(checked(numVisible * fslDim) + 7) / 8];

        int byteCursor = 0;
        int valueWritePos = 0;
        int innerBitWritePos = 0;
        int visibleSeen = 0;

        for (int i = 0; i < numItems; i++)
        {
            if (byteCursor + ctrlBytes > page.Length)
                throw new LanceFormatException(
                    $"FullZipLayout page truncated at item {i} (ctrl word).");
            ulong ctrl = ctrlBytes switch
            {
                1 => page[byteCursor],
                2 => BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(byteCursor, 2)),
                4 => BinaryPrimitives.ReadUInt32LittleEndian(page.Slice(byteCursor, 4)),
                _ => throw new InvalidOperationException(),
            };
            byteCursor += ctrlBytes;
            ushort defVal = (ushort)(ctrl & defMask);
            ushort repVal = (ushort)(ctrl >> bitsDef);
            if (rep is not null) rep[i] = repVal;
            if (def is not null) def[i] = defVal;

            bool visible = defVal <= maxVisibleDef;
            if (visible)
            {
                if (byteCursor + bytesPerValue > page.Length)
                    throw new LanceFormatException(
                        $"FullZipLayout page truncated at item {i} (value).");
                if (fslInnerValidity)
                {
                    ReadOnlySpan<byte> rowInnerValidity = page.Slice(byteCursor, fslInnerValidityBytesPerRow);
                    for (int j = 0; j < fslDim; j++)
                    {
                        bool valid = (rowInnerValidity[j >> 3] & (1 << (j & 7))) != 0;
                        int outIdx = innerBitWritePos + j;
                        if (valid)
                            innerValidity![outIdx >> 3] |= (byte)(1 << (outIdx & 7));
                        else
                            innerNullCount++;
                    }
                    innerBitWritePos += fslDim;
                    byteCursor += fslInnerValidityBytesPerRow;
                    page.Slice(byteCursor, valueBytesPerVisibleItem)
                        .CopyTo(valueBytes.AsSpan(valueWritePos));
                    byteCursor += valueBytesPerVisibleItem;
                    valueWritePos += valueBytesPerVisibleItem;
                }
                else
                {
                    page.Slice(byteCursor, bytesPerValue)
                        .CopyTo(valueBytes.AsSpan(valueWritePos));
                    byteCursor += bytesPerValue;
                    valueWritePos += bytesPerValue;
                }
                visibleSeen++;
            }
        }

        if (visibleSeen != numVisible)
            throw new LanceFormatException(
                $"FullZipLayout walked {visibleSeen} visible items but layout declared {numVisible}.");

        // For FSL leaves, valueBytesPerItem is one FSL row's worth of bytes
        // (excluding inner-validity, since that's threaded out separately);
        // for primitive leaves it is the primitive width. Cascade walker
        // doesn't care which.
        return (valueBytes, rep, def, innerValidity, innerNullCount,
            valueBytesPerVisibleItem, numVisible,
            /*VarOffsets*/ null, /*VarData*/ null);
    }

    /// <summary>
    /// Variable-width branch of <see cref="DecodeNestedLeafPage"/>: each
    /// visible item's payload is a 4-byte little-endian length followed
    /// by <c>length</c> data bytes. Walks the page byte-by-byte, extracts
    /// the rep/def control words and per-visible-item payload slices, and
    /// returns them as a packed (offsets, data) pair for the cascade
    /// walker. FSST-compressed payloads aren't yet supported on this path.
    /// </summary>
    private static (byte[] ValueBytes, ushort[]? Rep, ushort[]? Def, byte[]? InnerValidity, int InnerNullCount, int ValueBytesPerItem, int VisibleItems, int[]? VarOffsets, byte[]? VarData)
        DecodeNestedLeafPageVariable(
            FullZipLayout layout, CompressiveEncoding valueEnc,
            IArrowType leafType, in PageContext context)
    {
        if (leafType is not (StringType or BinaryType))
            throw new NotImplementedException(
                $"FullZipLayout(variable-width) cascade is only supported for String/Binary leaves (got {leafType}).");
        if (valueEnc.CompressionCase == CompressiveEncoding.CompressionOneofCase.Fsst)
            throw new NotImplementedException(
                "FullZipLayout(variable-width, FSST-compressed) inside a cascade is not yet supported.");
        if (valueEnc.CompressionCase != CompressiveEncoding.CompressionOneofCase.Variable)
            throw new NotImplementedException(
                $"FullZipLayout(variable-width) value_compression '{valueEnc.CompressionCase}' is not supported.");

        ulong bitsPerOffset = layout.BitsPerOffset;
        if (bitsPerOffset != 32)
            throw new NotImplementedException(
                $"FullZipLayout(variable-width) bits_per_offset={bitsPerOffset} is not supported (only 32).");
        int bytesPerLength = checked((int)(bitsPerOffset / 8));

        int numItems = checked((int)layout.NumItems);
        int numVisible = checked((int)layout.NumVisibleItems);

        int bitsRep = checked((int)layout.BitsRep);
        int bitsDef = checked((int)layout.BitsDef);
        int totalBits = bitsRep + bitsDef;
        int ctrlBytes = totalBits switch
        {
            0 => 0,
            <= 8 => 1,
            <= 16 => 2,
            <= 32 => 4,
            _ => throw new NotImplementedException(
                $"FullZipLayout ctrl word total_bits={totalBits} is not supported (max 32)."),
        };
        ulong defMask = bitsDef == 0 ? 0UL : ((1UL << bitsDef) - 1UL);

        int maxVisibleDef = 0;
        foreach (var layer in layout.Layers)
        {
            if (layer == RepDefLayer.RepdefNullableItem)
                maxVisibleDef++;
        }

        ReadOnlySpan<byte> page = context.PageBuffers[0].Span;

        var rep = bitsRep > 0 ? new ushort[numItems] : null;
        var def = bitsDef > 0 ? new ushort[numItems] : null;
        var offsets = new int[numVisible + 1];
        // Pre-pass: compute total data size by walking ctrl + length pairs.
        // We could combine this with the actual extraction, but a two-pass
        // approach lets us allocate the data buffer at exact size and copy
        // payloads without intermediate List<byte[]> allocation.
        int byteCursor = 0;
        int visibleSeen = 0;
        long totalDataLen = 0;
        for (int i = 0; i < numItems; i++)
        {
            if (byteCursor + ctrlBytes > page.Length)
                throw new LanceFormatException(
                    $"FullZipLayout(variable-width) page truncated at item {i} (ctrl word).");
            ulong ctrl = ctrlBytes switch
            {
                1 => page[byteCursor],
                2 => BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(byteCursor, 2)),
                4 => BinaryPrimitives.ReadUInt32LittleEndian(page.Slice(byteCursor, 4)),
                _ => throw new InvalidOperationException(),
            };
            byteCursor += ctrlBytes;
            ushort defVal = (ushort)(ctrl & defMask);
            ushort repVal = (ushort)(ctrl >> bitsDef);
            if (rep is not null) rep[i] = repVal;
            if (def is not null) def[i] = defVal;

            bool visible = defVal <= maxVisibleDef;
            if (visible)
            {
                if (byteCursor + bytesPerLength > page.Length)
                    throw new LanceFormatException(
                        $"FullZipLayout(variable-width) page truncated at item {i} (length prefix).");
                uint length = BinaryPrimitives.ReadUInt32LittleEndian(page.Slice(byteCursor, 4));
                byteCursor += bytesPerLength;
                if (byteCursor + length > page.Length)
                    throw new LanceFormatException(
                        $"FullZipLayout(variable-width) page truncated at item {i} (payload, length={length}).");
                totalDataLen = checked(totalDataLen + length);
                offsets[visibleSeen + 1] = checked((int)totalDataLen);
                byteCursor += (int)length;
                visibleSeen++;
            }
        }

        if (visibleSeen != numVisible)
            throw new LanceFormatException(
                $"FullZipLayout(variable-width) walked {visibleSeen} visible items but layout declared {numVisible}.");

        // Second pass: copy payloads into a contiguous data buffer.
        byte[] data = new byte[checked((int)totalDataLen)];
        byteCursor = 0;
        int writePos = 0;
        for (int i = 0; i < numItems; i++)
        {
            byteCursor += ctrlBytes;
            ushort defVal = (ushort)(rep is null
                ? (def![i])
                : (def![i]));
            bool visible = defVal <= maxVisibleDef;
            if (visible)
            {
                uint length = BinaryPrimitives.ReadUInt32LittleEndian(page.Slice(byteCursor, 4));
                byteCursor += bytesPerLength;
                page.Slice(byteCursor, (int)length).CopyTo(data.AsSpan(writePos));
                byteCursor += (int)length;
                writePos += (int)length;
            }
        }

        return (System.Array.Empty<byte>(), rep, def, /*innerValidity*/ null,
            /*innerNullCount*/ 0, /*ValueBytesPerItem*/ 0, numVisible, offsets, data);
    }

    private static int ResolveFslInnerByteSize(FixedSizeList fslEnc, FixedSizeListType fslType)
    {
        if (fslEnc.Values?.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat)
            throw new NotImplementedException(
                "FullZipLayout(FSL has_validity) requires Flat inner values.");
        ulong innerBits = fslEnc.Values.Flat.BitsPerValue;
        if (innerBits == 0 || innerBits % 8 != 0)
            throw new NotImplementedException(
                $"FullZipLayout(FSL has_validity) inner bits_per_value={innerBits} must be byte-aligned.");
        if (fslType.ValueDataType is not FixedWidthType innerFw || (ulong)innerFw.BitWidth != innerBits)
            throw new LanceFormatException(
                $"FSL inner Arrow type {fslType.ValueDataType} does not match encoding bits={innerBits}.");
        return checked((int)(innerBits / 8));
    }

    public static IArrowArray Decode(
        FullZipLayout layout, IArrowType targetType, in PageContext context)
    {
        if (layout.BitsRep != 0)
            throw new NotImplementedException(
                $"FullZipLayout with bits_rep={layout.BitsRep} is not yet supported.");

        if (layout.DetailsCase == FullZipLayout.DetailsOneofCase.BitsPerOffset)
            return DecodeVariableWidth(layout, targetType, context);

        // Fixed-width path. Nullable rows prepend a 1-byte def marker per
        // row (0 = valid, 1 = null) when bits_def == 1; per-row total size
        // is then 1 + bits_per_value/8 bytes. Other bits_def values aren't
        // emitted by pylance and aren't yet supported.
        bool hasDef = layout.BitsDef != 0;
        if (hasDef && layout.BitsDef != 1)
            throw new NotImplementedException(
                $"FullZipLayout(fixed-width) bits_def={layout.BitsDef} is not yet supported (only 0 or 1).");

        var expectedLayer = hasDef ? RepDefLayer.RepdefNullableItem : RepDefLayer.RepdefAllValidItem;
        if (layout.Layers.Count != 1 || layout.Layers[0] != expectedLayer)
            throw new NotImplementedException(
                $"FullZipLayout(fixed-width, bits_def={layout.BitsDef}) with layers other than " +
                $"[{expectedLayer}] is not yet supported (got {layout.Layers.Count} layers).");

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

        // Per-row size on disk = (1 def byte if hasDef) + bytesPerValue.
        long bytesPerRow = (hasDef ? 1L : 0L) + bytesPerValue;
        long totalBytes = checked(numItems * bytesPerRow);
        ReadOnlySpan<byte> payload = context.PageBuffers[0].Span;
        if (payload.Length < totalBytes)
            throw new LanceFormatException(
                $"FullZip page buffer too small: need {totalBytes} bytes, have {payload.Length}.");

        CompressiveEncoding valueEnc = layout.ValueCompression
            ?? throw new LanceFormatException("FullZipLayout has no value_compression.");

        // Strip per-row def bytes (when present) into a row-validity bitmap
        // and concatenate the value payloads into one byte array.
        byte[] valueBytes;
        byte[]? rowValidity = null;
        int rowNullCount = 0;
        if (!hasDef)
        {
            valueBytes = payload.Slice(0, (int)totalBytes).ToArray();
        }
        else
        {
            valueBytes = new byte[checked(numItems * bytesPerValue)];
            rowValidity = new byte[(numItems + 7) / 8];
            for (int i = 0; i < numItems; i++)
            {
                byte def = payload[(int)(i * bytesPerRow)];
                if (def == 0)
                    rowValidity[i >> 3] |= (byte)(1 << (i & 7));
                else
                    rowNullCount++;
                payload.Slice((int)(i * bytesPerRow + 1), (int)bytesPerValue)
                    .CopyTo(valueBytes.AsSpan((int)(i * bytesPerValue)));
            }
        }

        return BuildArray(valueBytes, numItems, bytesPerValue, valueEnc, targetType,
            rowValidity, rowNullCount);
    }

    private static IArrowArray BuildArray(
        byte[] valueBytes, int numItems, long bytesPerValue,
        CompressiveEncoding valueEnc, IArrowType targetType,
        byte[]? rowValidity, int rowNullCount)
    {
        switch (valueEnc.CompressionCase)
        {
            case CompressiveEncoding.CompressionOneofCase.Flat:
                return BuildFromFlat(valueBytes, numItems, bytesPerValue, valueEnc.Flat, targetType,
                    rowValidity, rowNullCount);

            case CompressiveEncoding.CompressionOneofCase.FixedSizeList:
                return BuildFromFixedSizeList(
                    valueBytes, numItems, bytesPerValue, valueEnc.FixedSizeList, targetType,
                    rowValidity, rowNullCount);

            default:
                throw new NotImplementedException(
                    $"FullZipLayout value_compression '{valueEnc.CompressionCase}' is not yet supported.");
        }
    }

    private static IArrowArray BuildFromFlat(
        byte[] valueBytes, int numItems, long bytesPerValue,
        Flat flat, IArrowType targetType,
        byte[]? rowValidity, int rowNullCount)
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

        ArrowBuffer validity = rowValidity is null ? ArrowBuffer.Empty : new ArrowBuffer(rowValidity);
        var data = new ArrayData(
            targetType, numItems, rowNullCount, offset: 0,
            new[] { validity, new ArrowBuffer(valueBytes) });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildFromFixedSizeList(
        byte[] valueBytes, int numItems, long bytesPerValue,
        FixedSizeList fsl, IArrowType targetType,
        byte[]? rowValidity, int rowNullCount)
    {
        if (targetType is not FixedSizeListType fslType)
            throw new LanceFormatException(
                $"FullZipLayout(FixedSizeList) cannot target non-FSL type {targetType}.");

        if ((ulong)fslType.ListSize != fsl.ItemsPerValue)
            throw new LanceFormatException(
                $"FSL dimension mismatch: schema={fslType.ListSize} vs encoding={fsl.ItemsPerValue}.");

        if (fsl.Values is null
            || fsl.Values.CompressionCase != CompressiveEncoding.CompressionOneofCase.Flat)
            throw new NotImplementedException(
                "FullZipLayout(FixedSizeList) with non-Flat inner values is not yet supported.");

        ulong innerBits = fsl.Values.Flat.BitsPerValue;
        if (fslType.ValueDataType is not FixedWidthType innerFw || (ulong)innerFw.BitWidth != innerBits)
            throw new LanceFormatException(
                $"FSL inner Arrow type {fslType.ValueDataType} does not match encoding bits={innerBits}.");

        int dim = checked((int)fsl.ItemsPerValue);
        int innerByteSize = checked((int)(innerBits / 8));
        int totalItems = checked(numItems * dim);
        int valueOnlyBytes = totalItems * innerByteSize;

        // Per-row payload layout when has_validity = true:
        //   [ceil(dim/8) inner-validity bits][dim * innerByteSize value bytes]
        // When has_validity = false, the row payload is just the value bytes.
        int innerValidityRowBytes = fsl.HasValidity ? (dim + 7) / 8 : 0;
        long expectedRowBytes = (long)innerValidityRowBytes + (long)dim * innerByteSize;
        if (expectedRowBytes != bytesPerValue)
            throw new LanceFormatException(
                $"FullZip FSL row size {bytesPerValue} != expected {expectedRowBytes} " +
                $"({innerValidityRowBytes} inner-validity + {dim * innerByteSize} values).");

        byte[] flatValueBytes;
        byte[]? innerValidity = null;
        int innerNullCount = 0;
        if (!fsl.HasValidity)
        {
            flatValueBytes = valueBytes;
        }
        else
        {
            // Walk each row: split out the inner-validity bits and append
            // them to a global LSB-first bitmap; copy the value bytes
            // into a contiguous flat buffer.
            flatValueBytes = new byte[valueOnlyBytes];
            innerValidity = new byte[(totalItems + 7) / 8];
            int valueWritePos = 0;
            int bitWritePos = 0;
            for (int row = 0; row < numItems; row++)
            {
                int rowStart = row * (int)bytesPerValue;
                ReadOnlySpan<byte> rowInnerValidity = valueBytes.AsSpan(rowStart, innerValidityRowBytes);
                ReadOnlySpan<byte> rowValues = valueBytes.AsSpan(
                    rowStart + innerValidityRowBytes, dim * innerByteSize);
                for (int j = 0; j < dim; j++)
                {
                    bool valid = (rowInnerValidity[j >> 3] & (1 << (j & 7))) != 0;
                    int outIdx = bitWritePos + j;
                    if (valid)
                        innerValidity[outIdx >> 3] |= (byte)(1 << (outIdx & 7));
                    else
                        innerNullCount++;
                }
                rowValues.CopyTo(flatValueBytes.AsSpan(valueWritePos));
                valueWritePos += rowValues.Length;
                bitWritePos += dim;
            }
        }

        ArrowBuffer innerValidityBuf = (innerValidity is not null && innerNullCount > 0)
            ? new ArrowBuffer(innerValidity)
            : ArrowBuffer.Empty;
        var childData = new ArrayData(
            fslType.ValueDataType, totalItems, innerNullCount, offset: 0,
            new[] { innerValidityBuf, new ArrowBuffer(flatValueBytes) });
        IArrowArray childArray = ArrowArrayFactory.BuildArray(childData);

        ArrowBuffer rowValidityBuf = rowValidity is null ? ArrowBuffer.Empty : new ArrowBuffer(rowValidity);
        var fslData = new ArrayData(
            fslType, numItems, rowNullCount, offset: 0,
            new[] { rowValidityBuf },
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
