// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.Encodings;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Result of reading a single column chunk: the Arrow array and optionally the raw definition/repetition levels.
/// </summary>
internal readonly record struct ColumnResult(IArrowArray Array, int[]? DefinitionLevels, int[]? RepetitionLevels);

/// <summary>
/// Reads a single column chunk's pages and produces an Arrow array.
/// </summary>
internal static class ColumnChunkReader
{
    /// <summary>
    /// Reads all pages in a column chunk and returns the resulting Arrow array.
    /// </summary>
    /// <param name="data">Raw bytes of the column chunk (from file offset to end of last page).</param>
    /// <param name="column">The column descriptor (type, levels, schema info).</param>
    /// <param name="columnMeta">The column chunk metadata (codec, num_values, etc.).</param>
    /// <param name="rowCount">Number of rows in the row group.</param>
    /// <param name="arrowField">The Arrow field for this column.</param>
    /// <param name="preserveDefLevels">
    /// If true, the raw definition levels are returned in the result for struct bitmap derivation.
    /// </param>
    public static ColumnResult ReadColumn(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        int rowCount,
        Field arrowField,
        bool preserveDefLevels = false,
        bool validateCrc = false)
    {
        bool isRepeated = column.MaxRepetitionLevel > 0;
        int capacity = isRepeated ? checked((int)columnMeta.NumValues) : rowCount;
        var byteArrayOutput = arrowField.DataType switch
        {
            StringViewType or BinaryViewType => ByteArrayOutputKind.ViewType,
            LargeStringType or LargeBinaryType => ByteArrayOutputKind.LargeOffsets,
            _ => ByteArrayOutputKind.Default,
        };

        using var state = new ColumnBuildState(
            column.PhysicalType, column.MaxDefinitionLevel, column.MaxRepetitionLevel, capacity,
            byteArrayOutput);
        DictionaryDecoder? dictionary = null;

        int pos = 0;
        long valuesRead = 0;

        while (valuesRead < columnMeta.NumValues && pos < data.Length)
        {
            PageHeader pageHeader;
            int headerSize;

            try
            {
                pageHeader = PageHeaderDecoder.Decode(data.Slice(pos), out headerSize);
            }
            catch (ParquetFormatException ex)
            {
                throw new ParquetFormatException(
                    $"Column '{string.Join(".", column.Path)}': corrupted page header " +
                    $"at byte offset {pos} ({valuesRead}/{columnMeta.NumValues} values read).",
                    ex);
            }

            pos += headerSize;

            var pageData = data.Slice(pos, pageHeader.CompressedPageSize);
            pos += pageHeader.CompressedPageSize;

            // Validate CRC-32C if present and validation is enabled.
            if (validateCrc && pageHeader.Crc.HasValue)
            {
                uint expected = unchecked((uint)pageHeader.Crc.Value);
                uint actual = ComputeCrc32C(pageData);
                if (actual != expected)
                    throw new ParquetFormatException(
                        $"Page CRC-32C mismatch in column '{string.Join(".", column.Path)}': " +
                        $"expected 0x{expected:X8}, got 0x{actual:X8}.");
            }

            switch (pageHeader.Type)
            {
                case PageType.DictionaryPage:
                    dictionary = ReadDictionaryPage(pageHeader, pageData, column, columnMeta);
                    break;

                case PageType.DataPage:
                    valuesRead += ReadDataPageV1(
                        pageHeader, pageData, column, columnMeta, dictionary, state);
                    break;

                case PageType.DataPageV2:
                    valuesRead += ReadDataPageV2(
                        pageHeader, pageData, column, columnMeta, dictionary, state);
                    break;

                default:
                    // Skip index pages and unknown page types
                    break;
            }
        }

        if (valuesRead < columnMeta.NumValues)
        {
            throw new ParquetFormatException(
                $"Column '{string.Join(".", column.Path)}': expected {columnMeta.NumValues} " +
                $"values but only read {valuesRead}. The column data may be corrupted or " +
                $"truncated. To skip this column, pass a columnNames list excluding it.");
        }

        int[]? defLevels = null;
        if ((preserveDefLevels || isRepeated) && column.MaxDefinitionLevel > 0)
        {
            var src = state.DefLevelSpan;
            defLevels = new int[src.Length];
            for (int i = 0; i < src.Length; i++)
                defLevels[i] = src[i];
        }

        int[]? repLevels = null;
        if (isRepeated)
        {
            var src = state.RepLevelSpan;
            repLevels = new int[src.Length];
            for (int i = 0; i < src.Length; i++)
                repLevels[i] = src[i];
        }

        IArrowArray array;
        if (isRepeated)
        {
            int numValues = checked((int)columnMeta.NumValues);
            array = ArrowArrayBuilder.BuildDense(state, arrowField, numValues);
        }
        else
        {
            array = ArrowArrayBuilder.Build(state, arrowField, rowCount);
        }

        return new ColumnResult(array, defLevels, repLevels);
    }

    /// <summary>
    /// Decodes a contiguous range of pages from a pre-scanned <see cref="ColumnPageMap"/>
    /// and returns the resulting Arrow array. Only the pages in
    /// <c>[startPage..endPage]</c> (inclusive) are decompressed and decoded.
    /// </summary>
    /// <param name="data">Raw bytes of the column chunk.</param>
    /// <param name="column">Column descriptor (type, levels, schema info).</param>
    /// <param name="columnMeta">Column chunk metadata (codec, num_values, etc.).</param>
    /// <param name="pageMap">Pre-scanned page map for this column chunk.</param>
    /// <param name="startPage">First page index (inclusive) to decode.</param>
    /// <param name="endPage">Last page index (inclusive) to decode.</param>
    /// <param name="batchRowCount">Number of rows in the resulting batch.</param>
    /// <param name="arrowField">Arrow field for this column.</param>
    /// <param name="preserveDefLevels">
    /// If true, raw definition levels are returned for nested assembly.
    /// </param>
    public static ColumnResult ReadColumnBatch(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        ColumnPageMap pageMap,
        int startPage,
        int endPage,
        Field arrowField,
        bool preserveDefLevels = false,
        bool validateCrc = false)
    {
        bool isRepeated = column.MaxRepetitionLevel > 0;

        int pageNumValues = pageMap.CumulativeValues[endPage + 1] - pageMap.CumulativeValues[startPage];
        int pageRowCount = pageMap.CumulativeRows[endPage + 1] - pageMap.CumulativeRows[startPage];
        int capacity = isRepeated ? pageNumValues : pageRowCount;

        var byteArrayOutput = arrowField.DataType switch
        {
            StringViewType or BinaryViewType => ByteArrayOutputKind.ViewType,
            LargeStringType or LargeBinaryType => ByteArrayOutputKind.LargeOffsets,
            _ => ByteArrayOutputKind.Default,
        };

        using var state = new ColumnBuildState(
            column.PhysicalType, column.MaxDefinitionLevel, column.MaxRepetitionLevel, capacity,
            byteArrayOutput);

        for (int p = startPage; p <= endPage; p++)
        {
            var entry = pageMap.Pages[p];
            var pageData = data.Slice(entry.Offset, entry.CompressedSize);

            if (entry.Type == PageType.DataPage)
            {
                ReadDataPageV1FromEntry(entry, pageData, column, columnMeta, pageMap.Dictionary, state);
            }
            else if (entry.Type == PageType.DataPageV2)
            {
                ReadDataPageV2FromEntry(entry, pageData, column, columnMeta, pageMap.Dictionary, state);
            }
        }

        int[]? defLevels = null;
        if ((preserveDefLevels || isRepeated) && column.MaxDefinitionLevel > 0)
        {
            var src = state.DefLevelSpan;
            defLevels = new int[src.Length];
            for (int i = 0; i < src.Length; i++)
                defLevels[i] = src[i];
        }

        int[]? repLevels = null;
        if (isRepeated)
        {
            var src = state.RepLevelSpan;
            repLevels = new int[src.Length];
            for (int i = 0; i < src.Length; i++)
                repLevels[i] = src[i];
        }

        IArrowArray array;
        if (isRepeated)
        {
            array = ArrowArrayBuilder.BuildDense(state, arrowField, pageNumValues);
        }
        else
        {
            array = ArrowArrayBuilder.Build(state, arrowField, pageRowCount);
        }

        return new ColumnResult(array, defLevels, repLevels);
    }

    /// <summary>
    /// Like <see cref="ReadColumnBatch"/> but the <paramref name="data"/> buffer is a
    /// sub-range of the column chunk starting at byte offset <paramref name="dataBaseOffset"/>
    /// (the offset of the first page in the slice). Page offsets in the page map are
    /// adjusted by subtracting this base.
    /// </summary>
    public static ColumnResult ReadColumnBatchFromSlice(
        ReadOnlySpan<byte> data,
        long dataBaseOffset,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        ColumnPageMap pageMap,
        int startPage,
        int endPage,
        Field arrowField,
        bool preserveDefLevels = false,
        bool validateCrc = false)
    {
        bool isRepeated = column.MaxRepetitionLevel > 0;

        int pageNumValues = pageMap.CumulativeValues[endPage + 1] - pageMap.CumulativeValues[startPage];
        int pageRowCount = pageMap.CumulativeRows[endPage + 1] - pageMap.CumulativeRows[startPage];
        int capacity = isRepeated ? pageNumValues : pageRowCount;

        var byteArrayOutput = arrowField.DataType switch
        {
            StringViewType or BinaryViewType => ByteArrayOutputKind.ViewType,
            LargeStringType or LargeBinaryType => ByteArrayOutputKind.LargeOffsets,
            _ => ByteArrayOutputKind.Default,
        };

        using var state = new ColumnBuildState(
            column.PhysicalType, column.MaxDefinitionLevel, column.MaxRepetitionLevel, capacity,
            byteArrayOutput);

        for (int p = startPage; p <= endPage; p++)
        {
            var entry = pageMap.Pages[p];
            int localOffset = (int)(entry.Offset - dataBaseOffset);
            var pageData = data.Slice(localOffset, entry.CompressedSize);

            if (entry.Type == PageType.DataPage)
            {
                ReadDataPageV1FromEntry(entry, pageData, column, columnMeta, pageMap.Dictionary, state);
            }
            else if (entry.Type == PageType.DataPageV2)
            {
                ReadDataPageV2FromEntry(entry, pageData, column, columnMeta, pageMap.Dictionary, state);
            }
        }

        int[]? defLevels = null;
        if ((preserveDefLevels || isRepeated) && column.MaxDefinitionLevel > 0)
        {
            var src = state.DefLevelSpan;
            defLevels = new int[src.Length];
            for (int i = 0; i < src.Length; i++)
                defLevels[i] = src[i];
        }

        int[]? repLevels = null;
        if (isRepeated)
        {
            var src = state.RepLevelSpan;
            repLevels = new int[src.Length];
            for (int i = 0; i < src.Length; i++)
                repLevels[i] = src[i];
        }

        IArrowArray array;
        if (isRepeated)
        {
            array = ArrowArrayBuilder.BuildDense(state, arrowField, pageNumValues);
        }
        else
        {
            array = ArrowArrayBuilder.Build(state, arrowField, pageRowCount);
        }

        return new ColumnResult(array, defLevels, repLevels);
    }

    /// <summary>
    /// Decodes a V1 data page using metadata from a <see cref="PageMapEntry"/>.
    /// </summary>
    private static void ReadDataPageV1FromEntry(
        PageMapEntry entry,
        ReadOnlySpan<byte> compressedData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        DictionaryDecoder? dictionary,
        ColumnBuildState state)
    {
        int numValues = entry.NumValues;

        ReadOnlySpan<byte> pageData;
        byte[]? decompressedBuffer = null;

        if (columnMeta.Codec == CompressionCodec.Uncompressed)
        {
            pageData = compressedData;
        }
        else
        {
            int size = entry.UncompressedSize;
            decompressedBuffer = ArrayPool<byte>.Shared.Rent(size);
            Decompressor.Decompress(columnMeta.Codec, compressedData, decompressedBuffer);
            pageData = decompressedBuffer.AsSpan(0, size);
        }

        try
        {
            int offset = 0;
            var repEncoding = entry.RepetitionLevelEncoding;
            var defEncoding = entry.DefinitionLevelEncoding;

            if (column.MaxRepetitionLevel > 0)
            {
                var repDest = state.ReserveRepLevels(numValues);
                offset += LevelDecoder.DecodeV1(pageData.Slice(offset), column.MaxRepetitionLevel, numValues, repDest, out _, repEncoding);
            }

            int nonNullCount;
            if (column.MaxDefinitionLevel > 0)
            {
                var defDest = state.ReserveDefLevels(numValues);
                offset += LevelDecoder.DecodeV1(pageData.Slice(offset), column.MaxDefinitionLevel, numValues, defDest, out nonNullCount, defEncoding);
            }
            else
            {
                nonNullCount = numValues;
            }

            var valueData = pageData.Slice(offset);
            DecodeValues(valueData, entry.Encoding, column, dictionary, nonNullCount, state);
        }
        finally
        {
            if (decompressedBuffer != null)
                ArrayPool<byte>.Shared.Return(decompressedBuffer);
        }
    }

    /// <summary>
    /// Decodes a V2 data page using metadata from a <see cref="PageMapEntry"/>.
    /// </summary>
    private static void ReadDataPageV2FromEntry(
        PageMapEntry entry,
        ReadOnlySpan<byte> rawData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        DictionaryDecoder? dictionary,
        ColumnBuildState state)
    {
        int numValues = entry.NumValues;
        int offset = 0;

        if (column.MaxRepetitionLevel > 0)
        {
            var repDest = state.ReserveRepLevels(numValues);
            LevelDecoder.DecodeV2(
                rawData.Slice(offset, entry.RepetitionLevelsByteLength),
                column.MaxRepetitionLevel, numValues, repDest, out _);
        }
        else if (entry.RepetitionLevelsByteLength > 0)
        {
            byte[]? rentedRep = null;
            Span<byte> tempRep = numValues <= 1024
                ? stackalloc byte[numValues]
                : (rentedRep = ArrayPool<byte>.Shared.Rent(numValues)).AsSpan(0, numValues);
            try
            {
                LevelDecoder.DecodeV2(
                    rawData.Slice(offset, entry.RepetitionLevelsByteLength),
                    column.MaxRepetitionLevel, numValues, tempRep, out _);
            }
            finally
            {
                if (rentedRep is not null) ArrayPool<byte>.Shared.Return(rentedRep);
            }
        }
        offset += entry.RepetitionLevelsByteLength;

        if (column.MaxDefinitionLevel > 0)
        {
            var defDest = state.ReserveDefLevels(numValues);
            LevelDecoder.DecodeV2(
                rawData.Slice(offset, entry.DefinitionLevelsByteLength),
                column.MaxDefinitionLevel, numValues, defDest, out _);
        }
        offset += entry.DefinitionLevelsByteLength;

        int nonNullCount = numValues - entry.NumNulls;

        var valuesCompressed = rawData.Slice(offset);

        if (nonNullCount == 0 || valuesCompressed.IsEmpty)
            return;

        ReadOnlySpan<byte> valueData;
        byte[]? decompressedBuffer = null;

        if (!entry.IsCompressed || columnMeta.Codec == CompressionCodec.Uncompressed)
        {
            valueData = valuesCompressed;
        }
        else
        {
            int levelsSize = entry.RepetitionLevelsByteLength + entry.DefinitionLevelsByteLength;
            int uncompressedValuesSize = entry.UncompressedSize - levelsSize;
            decompressedBuffer = ArrayPool<byte>.Shared.Rent(uncompressedValuesSize);
            Decompressor.Decompress(columnMeta.Codec, valuesCompressed, decompressedBuffer);
            valueData = decompressedBuffer.AsSpan(0, uncompressedValuesSize);
        }

        try
        {
            DecodeValues(valueData, entry.Encoding, column, dictionary, nonNullCount, state);
        }
        finally
        {
            if (decompressedBuffer != null)
                ArrayPool<byte>.Shared.Return(decompressedBuffer);
        }
    }

    private static DictionaryDecoder ReadDictionaryPage(
        PageHeader header,
        ReadOnlySpan<byte> compressedData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta)
    {
        var dictHeader = header.DictionaryPageHeader
            ?? throw new ParquetFormatException("Dictionary page missing DictionaryPageHeader.");

        ReadOnlySpan<byte> plainData;
        byte[]? decompressedBuffer = null;

        if (columnMeta.Codec == CompressionCodec.Uncompressed)
        {
            plainData = compressedData;
        }
        else
        {
            int size = header.UncompressedPageSize;
            decompressedBuffer = ArrayPool<byte>.Shared.Rent(size);
            Decompressor.Decompress(columnMeta.Codec, compressedData, decompressedBuffer);
            plainData = decompressedBuffer.AsSpan(0, size);
        }

        try
        {
            var decoder = new DictionaryDecoder(column.PhysicalType);
            decoder.Load(plainData, dictHeader.NumValues, column.TypeLength ?? 0);
            return decoder;
        }
        finally
        {
            if (decompressedBuffer != null)
                ArrayPool<byte>.Shared.Return(decompressedBuffer);
        }
    }

    private static int ReadDataPageV1(
        PageHeader header,
        ReadOnlySpan<byte> compressedData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        DictionaryDecoder? dictionary,
        ColumnBuildState state)
    {
        var dataHeader = header.DataPageHeader
            ?? throw new ParquetFormatException("Data page missing DataPageHeader.");

        int numValues = dataHeader.NumValues;

        // V1: entire payload is compressed together
        ReadOnlySpan<byte> pageData;
        byte[]? decompressedBuffer = null;

        if (columnMeta.Codec == CompressionCodec.Uncompressed)
        {
            pageData = compressedData;
        }
        else
        {
            int size = header.UncompressedPageSize;
            decompressedBuffer = ArrayPool<byte>.Shared.Rent(size);
            Decompressor.Decompress(columnMeta.Codec, compressedData, decompressedBuffer);
            pageData = decompressedBuffer.AsSpan(0, size);
        }

        try
        {
            int offset = 0;
            var repEncoding = dataHeader.RepetitionLevelEncoding;
            var defEncoding = dataHeader.DefinitionLevelEncoding;

            // Decode repetition levels (when maxRepLevel == 0 there is no
            // encoded level data in V1 pages, so skip entirely)
            if (column.MaxRepetitionLevel > 0)
            {
                var repDest = state.ReserveRepLevels(numValues);
                offset += LevelDecoder.DecodeV1(pageData.Slice(offset), column.MaxRepetitionLevel, numValues, repDest, out _, repEncoding);
            }

            // Decode definition levels and count non-nulls in a single pass
            int nonNullCount;
            if (column.MaxDefinitionLevel > 0)
            {
                var defDest = state.ReserveDefLevels(numValues);
                offset += LevelDecoder.DecodeV1(pageData.Slice(offset), column.MaxDefinitionLevel, numValues, defDest, out nonNullCount, defEncoding);
            }
            else
            {
                nonNullCount = numValues;
            }

            // Decode values
            var valueData = pageData.Slice(offset);
            var encoding = dataHeader.Encoding;
            DecodeValues(valueData, encoding, column, dictionary, nonNullCount, state);

            return numValues;
        }
        finally
        {
            if (decompressedBuffer != null)
                ArrayPool<byte>.Shared.Return(decompressedBuffer);
        }
    }

    private static int ReadDataPageV2(
        PageHeader header,
        ReadOnlySpan<byte> rawData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        DictionaryDecoder? dictionary,
        ColumnBuildState state)
    {
        var v2Header = header.DataPageHeaderV2
            ?? throw new ParquetFormatException("Data page V2 missing DataPageHeaderV2.");

        int numValues = v2Header.NumValues;
        int offset = 0;

        // V2: repetition and definition levels are uncompressed
        if (column.MaxRepetitionLevel > 0)
        {
            var repDest = state.ReserveRepLevels(numValues);
            LevelDecoder.DecodeV2(
                rawData.Slice(offset, v2Header.RepetitionLevelsByteLength),
                column.MaxRepetitionLevel, numValues, repDest, out _);
        }
        else if (v2Header.RepetitionLevelsByteLength > 0)
        {
            byte[]? rentedRep = null;
            Span<byte> tempRep = numValues <= 1024
                ? stackalloc byte[numValues]
                : (rentedRep = ArrayPool<byte>.Shared.Rent(numValues)).AsSpan(0, numValues);
            try
            {
                LevelDecoder.DecodeV2(
                    rawData.Slice(offset, v2Header.RepetitionLevelsByteLength),
                    column.MaxRepetitionLevel, numValues, tempRep, out _);
            }
            finally
            {
                if (rentedRep is not null) ArrayPool<byte>.Shared.Return(rentedRep);
            }
        }
        offset += v2Header.RepetitionLevelsByteLength;

        // Decode definition levels; non-null count is free from the V2 page header.
        if (column.MaxDefinitionLevel > 0)
        {
            var defDest = state.ReserveDefLevels(numValues);
            LevelDecoder.DecodeV2(
                rawData.Slice(offset, v2Header.DefinitionLevelsByteLength),
                column.MaxDefinitionLevel, numValues, defDest, out _);
        }
        offset += v2Header.DefinitionLevelsByteLength;

        // V2 page headers carry NumNulls directly — no need to scan def levels again.
        int nonNullCount = numValues - v2Header.NumNulls;

        // V2: only values portion is compressed (if is_compressed, default true)
        var valuesCompressed = rawData.Slice(offset);

        // All values may be null — nothing to decompress or decode
        if (nonNullCount == 0 || valuesCompressed.IsEmpty)
            return numValues;

        ReadOnlySpan<byte> valueData;
        byte[]? decompressedBuffer = null;

        if (!v2Header.IsCompressed || columnMeta.Codec == CompressionCodec.Uncompressed)
        {
            valueData = valuesCompressed;
        }
        else
        {
            int levelsSize = v2Header.RepetitionLevelsByteLength + v2Header.DefinitionLevelsByteLength;
            int uncompressedValuesSize = header.UncompressedPageSize - levelsSize;
            decompressedBuffer = ArrayPool<byte>.Shared.Rent(uncompressedValuesSize);
            Decompressor.Decompress(columnMeta.Codec, valuesCompressed, decompressedBuffer);
            valueData = decompressedBuffer.AsSpan(0, uncompressedValuesSize);
        }

        try
        {
            var encoding = v2Header.Encoding;
            DecodeValues(valueData, encoding, column, dictionary, nonNullCount, state);

            return numValues;
        }
        finally
        {
            if (decompressedBuffer != null)
                ArrayPool<byte>.Shared.Return(decompressedBuffer);
        }
    }

    private static void DecodeValues(
        ReadOnlySpan<byte> data,
        Encoding encoding,
        ColumnDescriptor column,
        DictionaryDecoder? dictionary,
        int nonNullCount,
        ColumnBuildState state)
    {
        bool isDictEncoded = encoding == Encoding.PlainDictionary || encoding == Encoding.RleDictionary;

        if (isDictEncoded)
        {
            if (dictionary == null)
                throw new ParquetFormatException(
                    $"Dictionary-encoded data page but no dictionary page found for column '{column.DottedPath}'.");

            DecodeDictValues(data, column, dictionary, nonNullCount, state);
        }
        else if (encoding == Encoding.Plain)
        {
            DecodePlainValues(data, column, nonNullCount, state);
        }
        else if (encoding == Encoding.DeltaBinaryPacked)
        {
            DecodeDeltaBinaryPackedValues(data, column, nonNullCount, state);
        }
        else if (encoding == Encoding.DeltaLengthByteArray)
        {
            DecodeDeltaLengthByteArrayValues(data, column, nonNullCount, state);
        }
        else if (encoding == Encoding.DeltaByteArray)
        {
            DecodeDeltaByteArrayValues(data, column, nonNullCount, state);
        }
        else if (encoding == Encoding.ByteStreamSplit)
        {
            DecodeByteStreamSplitValues(data, column, nonNullCount, state);
        }
        else if (encoding == Encoding.Rle)
        {
            DecodeRleBooleanValues(data, column, nonNullCount, state);
        }
        else
        {
            throw new NotSupportedException(
                $"Encoding '{encoding}' is not supported for column '{column.DottedPath}'.");
        }
    }

    private static void DecodePlainValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        switch (column.PhysicalType)
        {
            case PhysicalType.Boolean:
            {
                Span<bool> values = count <= 1024 ? stackalloc bool[count] : new bool[count];
                PlainDecoder.DecodeBooleans(data, values, count);
                state.AddBoolValues(values.Slice(0, count));
                break;
            }
            case PhysicalType.Int32:
            {
                var dest = state.ReserveValues<int>(count);
                PlainDecoder.DecodeInt32s(data, dest, count);
                break;
            }
            case PhysicalType.Int64:
            {
                var dest = state.ReserveValues<long>(count);
                PlainDecoder.DecodeInt64s(data, dest, count);
                break;
            }
            case PhysicalType.Float:
            {
                var dest = state.ReserveValues<float>(count);
                PlainDecoder.DecodeFloats(data, dest, count);
                break;
            }
            case PhysicalType.Double:
            {
                var dest = state.ReserveValues<double>(count);
                PlainDecoder.DecodeDoubles(data, dest, count);
                break;
            }
            case PhysicalType.Int96:
            {
                var dest = state.ReserveFixedBytes(count, 12);
                PlainDecoder.DecodeInt96s(data, dest, count);
                break;
            }
            case PhysicalType.FixedLenByteArray:
            {
                int typeLength = column.TypeLength ?? throw new ParquetFormatException(
                    "FIXED_LEN_BYTE_ARRAY column missing TypeLength.");
                var dest = state.ReserveFixedBytes(count, typeLength);
                PlainDecoder.DecodeFixedLenByteArrays(data, dest, count, typeLength);
                break;
            }
            case PhysicalType.ByteArray:
            {
                if (state.IsViewMode)
                {
                    PlainDecoder.WriteViewsToState(data, count, state);
                }
                else
                {
                    var offsets = ArrayPool<int>.Shared.Rent(count + 1);
                    try
                    {
                        int totalLen = PlainDecoder.MeasureByteArrays(data, offsets, count);
                        Span<byte> dest = state.ReserveByteArrayData(totalLen);
                        PlainDecoder.CopyByteArrayData(data, offsets, dest, count);
                        state.CommitByteArrayData(offsets.AsSpan(0, count + 1), count, totalLen);
                    }
                    finally
                    {
                        ArrayPool<int>.Shared.Return(offsets);
                    }
                }
                break;
            }
            default:
                throw new NotSupportedException(
                    $"Physical type '{column.PhysicalType}' is not supported for PLAIN decoding.");
        }
    }

    private static void DecodeDeltaBinaryPackedValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        switch (column.PhysicalType)
        {
            case PhysicalType.Int32:
            {
                var dest = state.ReserveValues<int>(count);
                var decoder = new DeltaBinaryPackedDecoder(data);
                decoder.DecodeInt32s(dest);
                break;
            }
            case PhysicalType.Int64:
            {
                var dest = state.ReserveValues<long>(count);
                var decoder = new DeltaBinaryPackedDecoder(data);
                decoder.DecodeInt64s(dest);
                break;
            }
            default:
                throw new NotSupportedException(
                    $"Physical type '{column.PhysicalType}' is not supported for DELTA_BINARY_PACKED decoding.");
        }
    }

    private static void DecodeDeltaLengthByteArrayValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        if (column.PhysicalType != PhysicalType.ByteArray)
            throw new NotSupportedException(
                $"Physical type '{column.PhysicalType}' is not supported for DELTA_LENGTH_BYTE_ARRAY decoding.");

        DeltaLengthByteArrayDecoder.Decode(data, count, state);
    }

    private static void DecodeDeltaByteArrayValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        if (column.PhysicalType != PhysicalType.ByteArray &&
            column.PhysicalType != PhysicalType.FixedLenByteArray)
            throw new NotSupportedException(
                $"Physical type '{column.PhysicalType}' is not supported for DELTA_BYTE_ARRAY decoding.");

        DeltaByteArrayDecoder.Decode(data, count, state);
    }

    private static void DecodeByteStreamSplitValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        switch (column.PhysicalType)
        {
            case PhysicalType.Float:
            {
                var dest = state.ReserveValues<float>(count);
                ByteStreamSplitDecoder.DecodeFloats(data, dest, count);
                break;
            }
            case PhysicalType.Double:
            {
                var dest = state.ReserveValues<double>(count);
                ByteStreamSplitDecoder.DecodeDoubles(data, dest, count);
                break;
            }
            case PhysicalType.Int32:
            {
                var dest = state.ReserveValues<int>(count);
                ByteStreamSplitDecoder.DecodeInt32s(data, dest, count);
                break;
            }
            case PhysicalType.Int64:
            {
                var dest = state.ReserveValues<long>(count);
                ByteStreamSplitDecoder.DecodeInt64s(data, dest, count);
                break;
            }
            case PhysicalType.FixedLenByteArray:
            {
                int typeLength = column.TypeLength ?? throw new ParquetFormatException(
                    "FIXED_LEN_BYTE_ARRAY column missing TypeLength.");
                var dest = state.ReserveFixedBytes(count, typeLength);
                ByteStreamSplitDecoder.DecodeFixedLenByteArrays(data, dest, count, typeLength);
                break;
            }
            default:
                throw new NotSupportedException(
                    $"Physical type '{column.PhysicalType}' is not supported for BYTE_STREAM_SPLIT decoding.");
        }
    }

    private static void DecodeRleBooleanValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        int count,
        ColumnBuildState state)
    {
        if (column.PhysicalType != PhysicalType.Boolean)
            throw new NotSupportedException(
                $"RLE encoding for values is only supported for BOOLEAN columns, not '{column.PhysicalType}'.");

        // RLE boolean values are prefixed with a 4-byte little-endian length
        if (data.Length < 4)
            throw new ParquetFormatException("RLE boolean data too short for length prefix.");
        int rleLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(data);
        var rleData = data.Slice(4, rleLength);

        var decoder = new RleBitPackedDecoder(rleData, bitWidth: 1);
        var ints = ArrayPool<int>.Shared.Rent(count);
        try
        {
            decoder.ReadBatch(ints.AsSpan(0, count));

            Span<bool> values = count <= 1024 ? stackalloc bool[count] : new bool[count];
            for (int i = 0; i < count; i++)
                values[i] = ints[i] != 0;

            state.AddBoolValues(values.Slice(0, count));
        }
        finally
        {
            ArrayPool<int>.Shared.Return(ints);
        }
    }

    private static void DecodeDictValues(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        DictionaryDecoder dictionary,
        int count,
        ColumnBuildState state)
    {
        if (count == 0)
            return;

        // First byte is the bit width for the RLE-encoded indices
        int bitWidth = data[0];
        var rleData = data.Slice(1);
        var decoder = new RleBitPackedDecoder(rleData, bitWidth);

        var indicesArray = ArrayPool<int>.Shared.Rent(count);
        try
        {
            decoder.ReadBatch(indicesArray.AsSpan(0, count));
            ReadOnlySpan<int> indices = indicesArray.AsSpan(0, count);

            switch (column.PhysicalType)
            {
                case PhysicalType.Boolean:
                {
                    Span<bool> values = count <= 1024 ? stackalloc bool[count] : new bool[count];
                    for (int i = 0; i < count; i++)
                        values[i] = dictionary.GetBoolean(indices[i]);
                    state.AddBoolValues(values.Slice(0, count));
                    break;
                }
                case PhysicalType.Int32:
                {
                    var dest = state.ReserveValues<int>(count);
                    for (int i = 0; i < count; i++)
                        dest[i] = dictionary.GetInt32(indices[i]);
                    break;
                }
                case PhysicalType.Int64:
                {
                    var dest = state.ReserveValues<long>(count);
                    for (int i = 0; i < count; i++)
                        dest[i] = dictionary.GetInt64(indices[i]);
                    break;
                }
                case PhysicalType.Float:
                {
                    var dest = state.ReserveValues<float>(count);
                    for (int i = 0; i < count; i++)
                        dest[i] = dictionary.GetFloat(indices[i]);
                    break;
                }
                case PhysicalType.Double:
                {
                    var dest = state.ReserveValues<double>(count);
                    for (int i = 0; i < count; i++)
                        dest[i] = dictionary.GetDouble(indices[i]);
                    break;
                }
                case PhysicalType.Int96:
                case PhysicalType.FixedLenByteArray:
                {
                    int typeLength = column.PhysicalType == PhysicalType.Int96 ? 12
                        : column.TypeLength ?? throw new ParquetFormatException(
                            "FIXED_LEN_BYTE_ARRAY column missing TypeLength.");
                    var dest = state.ReserveFixedBytes(count, typeLength);
                    for (int i = 0; i < count; i++)
                    {
                        var bytes = dictionary.GetFixedBytes(indices[i]);
                        bytes.CopyTo(dest.Slice(i * typeLength, typeLength));
                    }
                    break;
                }
                case PhysicalType.ByteArray:
                {
                    var offsets = ArrayPool<int>.Shared.Rent(count + 1);
                    try
                    {
                        if (state.IsViewMode)
                        {
                            // Write views directly — no intermediate byte[] needed
                            for (int i = 0; i < count; i++)
                                state.WriteOneStringView(dictionary.GetByteArray(indices[i]));
                        }
                        else
                        {
                            // First pass: compute offsets and total size using cheap length lookups
                            int totalLen = 0;
                            offsets[0] = 0;
                            for (int i = 0; i < count; i++)
                            {
                                totalLen += dictionary.GetByteArrayLength(indices[i]);
                                offsets[i + 1] = totalLen;
                            }

                            // Second pass: copy data using precomputed offsets (single GetByteArray per value)
                            Span<byte> dest = state.ReserveByteArrayData(totalLen);
                            for (int i = 0; i < count; i++)
                            {
                                dictionary.GetByteArray(indices[i]).CopyTo(dest.Slice(offsets[i]));
                            }
                            state.CommitByteArrayData(offsets.AsSpan(0, count + 1), count, totalLen);
                        }
                    }
                    finally
                    {
                        ArrayPool<int>.Shared.Return(offsets);
                    }
                    break;
                }
                default:
                    throw new NotSupportedException(
                        $"Physical type '{column.PhysicalType}' is not supported for dictionary decoding.");
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(indicesArray);
        }
    }

    private static uint ComputeCrc32C(ReadOnlySpan<byte> data)
    {
        var crc = new System.IO.Hashing.Crc32();
        crc.Append(data);
        Span<byte> hash = stackalloc byte[4];
        crc.GetHashAndReset(hash);
        return BinaryPrimitives.ReadUInt32LittleEndian(hash);
    }
}
