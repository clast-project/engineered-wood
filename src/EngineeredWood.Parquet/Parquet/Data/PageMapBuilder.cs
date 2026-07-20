// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.Compression;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Describes a single data page within a column chunk.
/// </summary>
internal readonly record struct PageMapEntry(
    int Offset,
    int CompressedSize,
    int UncompressedSize,
    int NumValues,
    int NumNulls,
    int NumRows,
    PageType Type,
    Encoding Encoding,
    Encoding RepetitionLevelEncoding,
    Encoding DefinitionLevelEncoding,
    int RepetitionLevelsByteLength,
    int DefinitionLevelsByteLength,
    bool IsCompressed);

/// <summary>
/// Pre-scanned page layout for a single column chunk. Enables batched decoding
/// by mapping row ranges to page ranges without re-scanning headers.
/// </summary>
internal sealed class ColumnPageMap
{
    /// <summary>Decoded dictionary, or null if the column is not dictionary-encoded.</summary>
    public DictionaryDecoder? Dictionary { get; }

    /// <summary>Data pages in file order (dictionary page excluded).</summary>
    public PageMapEntry[] Pages { get; }

    /// <summary>
    /// Prefix-sum of row counts: <c>CumulativeRows[i]</c> is the total number of rows
    /// in pages <c>[0..i)</c>. Length is <c>Pages.Length + 1</c>; element 0 is always 0
    /// and the last element equals <see cref="TotalRows"/>.
    /// </summary>
    public int[] CumulativeRows { get; }

    /// <summary>
    /// Prefix-sum of value counts: <c>CumulativeValues[i]</c> is the total number of values
    /// in pages <c>[0..i)</c>. Length is <c>Pages.Length + 1</c>.
    /// </summary>
    public int[] CumulativeValues { get; }

    /// <summary>Total rows across all data pages.</summary>
    public int TotalRows { get; }

    public ColumnPageMap(
        DictionaryDecoder? dictionary,
        PageMapEntry[] pages,
        int[] cumulativeRows,
        int[] cumulativeValues,
        int totalRows)
    {
        Dictionary = dictionary;
        Pages = pages;
        CumulativeRows = cumulativeRows;
        CumulativeValues = cumulativeValues;
        TotalRows = totalRows;
    }

    /// <summary>
    /// Finds the page index that contains the given row (0-based within the row group).
    /// </summary>
    public int FindPageForRow(int row)
    {
        // Binary-search CumulativeRows for the largest i where CumulativeRows[i] <= row.
        // CumulativeRows has length Pages.Length + 1, so we search [0..Pages.Length).
        int lo = 0, hi = Pages.Length - 1;
        while (lo < hi)
        {
            int mid = lo + (hi - lo + 1) / 2;
            if (CumulativeRows[mid] <= row)
                lo = mid;
            else
                hi = mid - 1;
        }
        return lo;
    }
}

/// <summary>
/// Scans page headers in a column chunk and builds a <see cref="ColumnPageMap"/>
/// without decoding values. For V1 pages on nested (repeated) columns, the
/// repetition levels are decompressed and decoded to determine row counts.
/// </summary>
internal static class PageMapBuilder
{
    /// <summary>
    /// Scans the column chunk data and builds a page map.
    /// </summary>
    /// <param name="data">Raw bytes of the column chunk.</param>
    /// <param name="column">Column descriptor (levels, type, path).</param>
    /// <param name="columnMeta">Column chunk metadata (codec, num_values, etc.).</param>
    public static ColumnPageMap Build(
        ReadOnlySpan<byte> data,
        ColumnDescriptor column,
        ColumnMetaData columnMeta)
    {
        var pages = new List<PageMapEntry>();
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

            int pageDataOffset = pos + headerSize;
            var pageData = data.Slice(pageDataOffset, pageHeader.CompressedPageSize);
            pos = pageDataOffset + pageHeader.CompressedPageSize;

            switch (pageHeader.Type)
            {
                case PageType.DictionaryPage:
                    dictionary = DecodeDictionaryPage(pageHeader, pageData, column, columnMeta);
                    break;

                case PageType.DataPage:
                {
                    var dph = pageHeader.DataPageHeader!;
                    int numValues = dph.NumValues;
                    int numRows = DeriveRowCountV1(
                        pageHeader, pageData, column, columnMeta, dph);

                    pages.Add(new PageMapEntry(
                        Offset: pageDataOffset,
                        CompressedSize: pageHeader.CompressedPageSize,
                        UncompressedSize: pageHeader.UncompressedPageSize,
                        NumValues: numValues,
                        NumNulls: -1, // not available for V1; derived from def levels at decode time
                        NumRows: numRows,
                        Type: PageType.DataPage,
                        Encoding: dph.Encoding,
                        RepetitionLevelEncoding: dph.RepetitionLevelEncoding,
                        DefinitionLevelEncoding: dph.DefinitionLevelEncoding,
                        RepetitionLevelsByteLength: 0,
                        DefinitionLevelsByteLength: 0,
                        IsCompressed: true));

                    valuesRead += numValues;
                    break;
                }

                case PageType.DataPageV2:
                {
                    var v2h = pageHeader.DataPageHeaderV2!;

                    pages.Add(new PageMapEntry(
                        Offset: pageDataOffset,
                        CompressedSize: pageHeader.CompressedPageSize,
                        UncompressedSize: pageHeader.UncompressedPageSize,
                        NumValues: v2h.NumValues,
                        NumNulls: v2h.NumNulls,
                        NumRows: v2h.NumRows,
                        Type: PageType.DataPageV2,
                        Encoding: v2h.Encoding,
                        RepetitionLevelEncoding: Encoding.Rle,
                        DefinitionLevelEncoding: Encoding.Rle,
                        RepetitionLevelsByteLength: v2h.RepetitionLevelsByteLength,
                        DefinitionLevelsByteLength: v2h.DefinitionLevelsByteLength,
                        IsCompressed: v2h.IsCompressed));

                    valuesRead += v2h.NumValues;
                    break;
                }

                default:
                    // Skip index pages and unknown page types
                    break;
            }
        }

        var pagesArray = pages.ToArray();
        var cumulativeRows = new int[pagesArray.Length + 1];
        var cumulativeValues = new int[pagesArray.Length + 1];
        int totalRows = 0;
        int totalValues = 0;

        for (int i = 0; i < pagesArray.Length; i++)
        {
            cumulativeRows[i] = totalRows;
            cumulativeValues[i] = totalValues;
            totalRows += pagesArray[i].NumRows;
            totalValues += pagesArray[i].NumValues;
        }
        cumulativeRows[pagesArray.Length] = totalRows;
        cumulativeValues[pagesArray.Length] = totalValues;

        return new ColumnPageMap(dictionary, pagesArray, cumulativeRows, cumulativeValues, totalRows);
    }

    /// <summary>
    /// Derives the row count for a V1 data page. For flat columns (maxRepLevel == 0),
    /// numValues == numRows. For nested columns, rep levels must be decoded to count
    /// row boundaries (rep == 0).
    /// </summary>
    private static int DeriveRowCountV1(
        PageHeader header,
        ReadOnlySpan<byte> compressedData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        DataPageHeader dataHeader)
    {
        // Flat columns: values == rows
        if (column.MaxRepetitionLevel == 0)
            return dataHeader.NumValues;

        // Nested: must decompress and decode rep levels to count row starts (rep == 0).
        ReadOnlySpan<byte> pageData;
        byte[]? decompressedBuffer = null;

        try
        {
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

            int numValues = dataHeader.NumValues;
            byte[] repLevels = ArrayPool<byte>.Shared.Rent(numValues);
            try
            {
                LevelDecoder.DecodeV1(
                    pageData, column.MaxRepetitionLevel, numValues,
                    repLevels.AsSpan(0, numValues), out _,
                    dataHeader.RepetitionLevelEncoding);

                int rowCount = 0;
                for (int i = 0; i < numValues; i++)
                {
                    if (repLevels[i] == 0)
                        rowCount++;
                }
                return rowCount;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(repLevels);
            }
        }
        finally
        {
            if (decompressedBuffer != null)
                ArrayPool<byte>.Shared.Return(decompressedBuffer);
        }
    }

    private static DictionaryDecoder DecodeDictionaryPage(
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
}
