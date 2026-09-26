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

    /// <summary>Decoded FSST symbol table, or null if the column is not FSST-encoded.</summary>
    public FsstSymbolTable? SymbolTable { get; }

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

    /// <summary>
    /// Whether each entry was read from its page header. When false the map was built from the
    /// chunk's OffsetIndex (<see cref="PageMapBuilder.BuildFromOffsetIndex"/>): an entry then locates
    /// the whole page, header included, and knows its rows, but not its encodings or level lengths,
    /// and its <see cref="PageMapEntry.UncompressedSize"/> is an estimate. The decoder reads the
    /// header from the page bytes it fetches anyway (<see cref="PageMapBuilder.ResolveEntry"/>).
    /// </summary>
    public bool HeadersResolved { get; }

    public ColumnPageMap(
        DictionaryDecoder? dictionary,
        FsstSymbolTable? symbolTable,
        PageMapEntry[] pages,
        int[] cumulativeRows,
        int[] cumulativeValues,
        int totalRows,
        bool headersResolved = true)
    {
        Dictionary = dictionary;
        SymbolTable = symbolTable;
        Pages = pages;
        CumulativeRows = cumulativeRows;
        CumulativeValues = cumulativeValues;
        TotalRows = totalRows;
        HeadersResolved = headersResolved;
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
        FsstSymbolTable? symbolTable = null;

        int pos = 0;
        long valuesRead = 0;

        while (valuesRead < columnMeta.NumValues && pos < data.Length)
        {
            var pageHeader = DecodeHeader(
                data, pos, column, $"{valuesRead}/{columnMeta.NumValues} values read", out int headerSize);

            int pageDataOffset = pos + headerSize;
            var pageData = data.Slice(pageDataOffset, pageHeader.CompressedPageSize);
            pos = pageDataOffset + pageHeader.CompressedPageSize;

            switch (pageHeader.Type)
            {
                case PageType.DictionaryPage:
                    dictionary = DecodeDictionaryPage(pageHeader, pageData, column, columnMeta);
                    break;

                case PageType.SymbolTablePage:
                    symbolTable = FsstPageDecoder.ReadSymbolTablePage(pageHeader, pageData, columnMeta);
                    break;

                case PageType.DataPage:
                case PageType.DataPageV2:
                {
                    var entry = EntryFromHeader(pageHeader, pageDataOffset, pageData, column, columnMeta);
                    pages.Add(entry);
                    valuesRead += entry.NumValues;
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

        return new ColumnPageMap(
            dictionary, symbolTable, pagesArray, cumulativeRows, cumulativeValues, totalRows);
    }

    /// <summary>
    /// Builds a flat column's page map from its OffsetIndex, so that the data pages need not be
    /// read to find them. Only <paramref name="prefix"/>, the bytes before the first data page, is
    /// scanned, for a dictionary or FSST symbol-table page.
    /// </summary>
    /// <param name="prefix">The chunk's bytes from <paramref name="chunkStart"/> up to its first data page.</param>
    /// <param name="chunkStart">File offset of the chunk's first page.</param>
    /// <param name="chunkEnd">File offset one past the chunk's last byte.</param>
    /// <param name="index">The chunk's OffsetIndex.</param>
    /// <param name="rowCount">Rows in the row group.</param>
    /// <param name="column">A flat (non-repeated) column.</param>
    /// <param name="columnMeta">The chunk's metadata.</param>
    /// <returns>
    /// The map, or null when the index cannot describe this chunk's pages; the caller then scans the
    /// headers instead. The index is optional metadata, so one that cannot be right is ignored rather
    /// than refused. One that is consistent on its own but disagrees with a page's header is caught
    /// when that page is decoded (<see cref="ResolveEntry"/>).
    /// </returns>
    public static ColumnPageMap? BuildFromOffsetIndex(
        ReadOnlySpan<byte> prefix,
        long chunkStart,
        long chunkEnd,
        OffsetIndex index,
        int rowCount,
        ColumnDescriptor column,
        ColumnMetaData columnMeta)
    {
        // A repeated column's map counts values as well as rows, and the index records only rows.
        if (column.MaxRepetitionLevel > 0)
            throw new ArgumentException($"Column '{column.DottedPath}' is repeated.", nameof(column));

        var locations = index.PageLocations;
        if (!Tiles(locations, chunkStart + prefix.Length, chunkEnd, rowCount))
            return null;

        DictionaryDecoder? dictionary = null;
        FsstSymbolTable? symbolTable = null;
        int pos = 0;
        while (pos < prefix.Length)
        {
            // The prefix ends where the index puts the first data page. If that is wrong, it can end
            // inside a header, which then fails to decode: the index is wrong, not the file. A side
            // page that decodes but whose data is corrupt still throws below, as the scan would.
            PageHeader pageHeader;
            int headerSize;
            try
            {
                pageHeader = PageHeaderDecoder.Decode(prefix.Slice(pos), out headerSize);
            }
            catch (ParquetFormatException)
            {
                return null;
            }

            int pageDataOffset = pos + headerSize;
            if (pageHeader.CompressedPageSize < 0 || pageHeader.CompressedPageSize > prefix.Length - pageDataOffset)
                return null;

            var pageData = prefix.Slice(pageDataOffset, pageHeader.CompressedPageSize);
            pos = pageDataOffset + pageHeader.CompressedPageSize;

            switch (pageHeader.Type)
            {
                case PageType.DictionaryPage:
                    dictionary = DecodeDictionaryPage(pageHeader, pageData, column, columnMeta);
                    break;
                case PageType.SymbolTablePage:
                    symbolTable = FsstPageDecoder.ReadSymbolTablePage(pageHeader, pageData, columnMeta);
                    break;
                case PageType.DataPage:
                case PageType.DataPageV2:
                    // A data page the index does not list, whose rows the map would lose.
                    return null;
            }
        }

        // Only ComputeBatchRowCount's byte budget reads UncompressedSize, and that budget is
        // documented as approximate. Scale by the chunk's own ratio: both totals count page headers,
        // as a location's size does.
        double ratio = columnMeta.TotalCompressedSize > 0
            ? (double)columnMeta.TotalUncompressedSize / columnMeta.TotalCompressedSize
            : 1.0;

        var pages = new PageMapEntry[locations.Count];
        var cumulativeRows = new int[pages.Length + 1];
        for (int i = 0; i < pages.Length; i++)
        {
            var location = locations[i];
            long nextRow = i + 1 < pages.Length ? locations[i + 1].FirstRowIndex : rowCount;
            int rows = (int)(nextRow - location.FirstRowIndex);
            cumulativeRows[i] = (int)location.FirstRowIndex;

            pages[i] = new PageMapEntry(
                Offset: checked((int)(location.Offset - chunkStart)),
                CompressedSize: location.CompressedPageSize,
                UncompressedSize: (int)Math.Min(int.MaxValue, location.CompressedPageSize * ratio),
                NumValues: rows, // flat: a value slot per row
                NumNulls: -1,
                NumRows: rows,
                // The rest is in the page header, which ResolveEntry reads.
                Type: PageType.DataPage,
                Encoding: Encoding.Plain,
                RepetitionLevelEncoding: Encoding.Rle,
                DefinitionLevelEncoding: Encoding.Rle,
                RepetitionLevelsByteLength: 0,
                DefinitionLevelsByteLength: 0,
                IsCompressed: true);
        }
        cumulativeRows[pages.Length] = rowCount;

        return new ColumnPageMap(
            dictionary, symbolTable, pages, cumulativeRows, cumulativeRows, rowCount,
            headersResolved: false);
    }

    /// <summary>
    /// Whether <paramref name="locations"/> can be a chunk's data pages: at least one, the first at
    /// <paramref name="dataStart"/>, each after the one before without overlapping it, all inside
    /// the chunk, with first rows rising strictly from 0 and staying below <paramref name="rowCount"/>.
    /// </summary>
    private static bool Tiles(IReadOnlyList<PageLocation> locations, long dataStart, long chunkEnd, int rowCount)
    {
        if (locations.Count == 0 || locations[0].Offset != dataStart || locations[0].FirstRowIndex != 0)
            return false;

        for (int i = 0; i < locations.Count; i++)
        {
            var location = locations[i];
            if (location.CompressedPageSize <= 0
                || location.Offset > chunkEnd - location.CompressedPageSize
                || location.FirstRowIndex >= rowCount)
            {
                return false;
            }

            if (i > 0)
            {
                var previous = locations[i - 1];
                if (location.Offset < previous.Offset + previous.CompressedPageSize
                    || location.FirstRowIndex <= previous.FirstRowIndex)
                {
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Returns page <paramref name="page"/> of <paramref name="map"/> as its header describes it.
    /// For a map built from headers that is the entry itself. For one built from an OffsetIndex, the
    /// header is decoded from <paramref name="pageBytes"/> and checked against the index.
    /// </summary>
    /// <param name="map">The column's page map.</param>
    /// <param name="page">The page's position in <see cref="ColumnPageMap.Pages"/>.</param>
    /// <param name="pageBytes">
    /// The <see cref="PageMapEntry.CompressedSize"/> bytes at <see cref="PageMapEntry.Offset"/>: the
    /// page's data, or for an OffsetIndex map its header and data.
    /// </param>
    /// <param name="column">The column.</param>
    /// <param name="columnMeta">The chunk's metadata.</param>
    /// <param name="headerSize">How many bytes of <paramref name="pageBytes"/> precede the page's data.</param>
    /// <exception cref="ParquetFormatException">The header disagrees with the OffsetIndex.</exception>
    public static PageMapEntry ResolveEntry(
        ColumnPageMap map,
        int page,
        ReadOnlySpan<byte> pageBytes,
        ColumnDescriptor column,
        ColumnMetaData columnMeta,
        out int headerSize)
    {
        var located = map.Pages[page];
        if (map.HeadersResolved)
        {
            headerSize = 0;
            return located;
        }

        var header = DecodeHeader(pageBytes, 0, column, $"OffsetIndex page {page}", out headerSize);
        if (header.Type is not (PageType.DataPage or PageType.DataPageV2)
            || headerSize + (long)header.CompressedPageSize != located.CompressedSize)
        {
            throw new ParquetFormatException(
                $"Column '{column.DottedPath}': OffsetIndex page {page} ({located.CompressedSize} bytes at chunk " +
                $"offset {located.Offset}) does not match the page there, a {header.Type} of {headerSize} + " +
                $"{header.CompressedPageSize} bytes.");
        }

        var entry = EntryFromHeader(
            header, located.Offset + headerSize, pageBytes.Slice(headerSize), column, columnMeta);
        if (entry.NumRows != located.NumRows)
        {
            throw new ParquetFormatException(
                $"Column '{column.DottedPath}': OffsetIndex page {page} spans {located.NumRows} rows, " +
                $"but its header holds {entry.NumRows}.");
        }

        return entry;
    }

    private static PageHeader DecodeHeader(
        ReadOnlySpan<byte> data, int pos, ColumnDescriptor column, string where, out int headerSize)
    {
        try
        {
            return PageHeaderDecoder.Decode(data.Slice(pos), out headerSize);
        }
        catch (ParquetFormatException ex)
        {
            throw new ParquetFormatException(
                $"Column '{string.Join(".", column.Path)}': corrupted page header " +
                $"at byte offset {pos} ({where}).",
                ex);
        }
    }

    /// <summary>
    /// A data page's map entry, from its header. <paramref name="pageData"/> is the data after the
    /// header, read only to count the rows of a V1 page of a repeated column.
    /// </summary>
    private static PageMapEntry EntryFromHeader(
        PageHeader pageHeader,
        int pageDataOffset,
        ReadOnlySpan<byte> pageData,
        ColumnDescriptor column,
        ColumnMetaData columnMeta)
    {
        if (pageHeader.Type == PageType.DataPage)
        {
            var dph = pageHeader.DataPageHeader!;
            return new PageMapEntry(
                Offset: pageDataOffset,
                CompressedSize: pageHeader.CompressedPageSize,
                UncompressedSize: pageHeader.UncompressedPageSize,
                NumValues: dph.NumValues,
                NumNulls: -1, // not available for V1; derived from def levels at decode time
                NumRows: DeriveRowCountV1(pageHeader, pageData, column, columnMeta, dph),
                Type: PageType.DataPage,
                Encoding: dph.Encoding,
                RepetitionLevelEncoding: dph.RepetitionLevelEncoding,
                DefinitionLevelEncoding: dph.DefinitionLevelEncoding,
                RepetitionLevelsByteLength: 0,
                DefinitionLevelsByteLength: 0,
                IsCompressed: true);
        }

        var v2h = pageHeader.DataPageHeaderV2!;
        return new PageMapEntry(
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
            IsCompressed: v2h.IsCompressed);
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
