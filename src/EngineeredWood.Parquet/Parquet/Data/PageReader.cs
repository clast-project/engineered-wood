// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// One page of a column chunk, as <see cref="PageReader"/> found it.
/// </summary>
internal readonly ref struct Page
{
    /// <summary>The page's header.</summary>
    public PageHeader Header { get; }

    /// <summary>The page's bytes after its header: <see cref="PageHeader.CompressedPageSize"/> of them.</summary>
    public ReadOnlySpan<byte> Payload { get; }

    /// <summary>Where the page's header starts, relative to the bytes the reader walks.</summary>
    public int Offset { get; }

    /// <summary>How many bytes the header takes.</summary>
    public int HeaderSize { get; }

    /// <summary>
    /// The data page's position among the chunk's data pages, counting from 0: the index of its
    /// entry in the OffsetIndex's <c>page_locations</c>, and the page ordinal of the encryption AAD.
    /// -1 for a dictionary, symbol-table, index or unknown page.
    /// </summary>
    public int Ordinal { get; }

    /// <summary>Where the page's data starts, relative to the bytes the reader walks.</summary>
    public int PayloadOffset => Offset + HeaderSize;

    /// <summary>Whether this is a V1 or V2 data page.</summary>
    public bool IsDataPage => Header.Type is PageType.DataPage or PageType.DataPageV2;

    /// <summary>The values, nulls included, that a data page holds; 0 for any other page.</summary>
    public int NumValues => Header.Type switch
    {
        PageType.DataPage => Header.DataPageHeader!.NumValues,
        PageType.DataPageV2 => Header.DataPageHeaderV2!.NumValues,
        _ => 0,
    };

    public Page(PageHeader header, ReadOnlySpan<byte> payload, int offset, int headerSize, int ordinal)
    {
        Header = header;
        Payload = payload;
        Offset = offset;
        HeaderSize = headerSize;
        Ordinal = ordinal;
    }
}

/// <summary>
/// Walks the pages of a column chunk, or of any run of its pages, in file order. Every reader of
/// page headers goes through here, so that there is one place to decrypt them.
/// </summary>
/// <remarks>
/// A data page's <see cref="Page.Ordinal"/> is its position in the chunk, not a count of the pages
/// this reader has seen: a walk that starts at data page <i>k</i>, located through the OffsetIndex,
/// is told so and numbers that page <i>k</i>. Encryption binds each page to that position, so a
/// reader that skips pages must still know it.
/// </remarks>
internal ref struct PageReader
{
    private readonly ReadOnlySpan<byte> _data;
    private readonly ColumnDescriptor _column;
    private int _position;
    private int _nextOrdinal;

    /// <summary>
    /// Starts a walk at the first byte of <paramref name="data"/>.
    /// </summary>
    /// <param name="data">The pages to walk, starting at a page header.</param>
    /// <param name="column">The chunk's column, which errors name.</param>
    /// <param name="firstOrdinal">
    /// The ordinal of the first data page in <paramref name="data"/>: 0 when it starts at the
    /// chunk's first page, otherwise that page's position in the chunk.
    /// </param>
    public PageReader(ReadOnlySpan<byte> data, ColumnDescriptor column, int firstOrdinal = 0)
    {
        _data = data;
        _column = column;
        _position = 0;
        _nextOrdinal = firstOrdinal;
    }

    /// <summary>Where the next page's header starts, relative to the walked bytes.</summary>
    public readonly int Position => _position;

    /// <summary>
    /// Reads the next page.
    /// </summary>
    /// <returns>False when no bytes remain.</returns>
    /// <exception cref="ParquetFormatException">
    /// The header does not decode, declares more data than the walked bytes hold, or holds a count
    /// or size no page can have (<see cref="Validate"/>).
    /// </exception>
    public bool TryRead(out Page page)
    {
        if (_position >= _data.Length)
        {
            page = default;
            return false;
        }

        PageHeader header;
        int headerSize;
        try
        {
            header = PageHeaderDecoder.Decode(_data.Slice(_position), out headerSize);
        }
        catch (ParquetFormatException ex)
        {
            throw new ParquetFormatException(
                $"Column '{_column.DottedPath}': corrupted page header at byte offset {_position} " +
                $"(before data page {_nextOrdinal}).",
                ex);
        }

        int payloadOffset = _position + headerSize;
        if (header.CompressedPageSize < 0 || header.CompressedPageSize > _data.Length - payloadOffset)
        {
            throw new ParquetFormatException(
                $"Column '{_column.DottedPath}': the {header.Type} at byte offset {_position} declares " +
                $"{header.CompressedPageSize} bytes of data, but only {_data.Length - payloadOffset} remain. " +
                "The column data may be truncated.");
        }

        if (Validate(header, repeated: _column.MaxRepetitionLevel > 0) is { } problem)
        {
            throw new ParquetFormatException(
                $"Column '{_column.DottedPath}': the {header.Type} at byte offset {_position} is malformed: {problem}");
        }

        int ordinal = header.Type is PageType.DataPage or PageType.DataPageV2 ? _nextOrdinal++ : -1;
        page = new Page(
            header, _data.Slice(payloadOffset, header.CompressedPageSize), _position, headerSize, ordinal);
        _position = payloadOffset + header.CompressedPageSize;
        return true;
    }

    /// <summary>
    /// Checks the counts and sizes a page's decoder trusts. A header that decodes as Thrift can
    /// still hold values no page can have, and the decoders size buffers and slices from them: a
    /// negative value count reached a <c>stackalloc</c> and overflowed the stack, which no caller
    /// can catch. Only what the header alone can prove is checked here; that the data agrees
    /// with it is the decoders' business.
    /// </summary>
    /// <returns>What is wrong, or null.</returns>
    private static string? Validate(PageHeader header, bool repeated)
    {
        if (header.UncompressedPageSize < 0)
            return $"its uncompressed size is {header.UncompressedPageSize}.";

        switch (header.Type)
        {
            case PageType.DataPage:
                if (header.DataPageHeader is not { } v1)
                    return "it has no data_page_header.";
                if (v1.NumValues < 0)
                    return $"it holds {v1.NumValues} values.";
                break;

            case PageType.DataPageV2:
                if (header.DataPageHeaderV2 is not { } v2)
                    return "it has no data_page_header_v2.";
                if (v2.NumValues < 0)
                    return $"it holds {v2.NumValues} values.";
                if (v2.NumNulls < 0 || v2.NumNulls > v2.NumValues)
                    return $"it holds {v2.NumNulls} nulls among {v2.NumValues} values.";
                // Every row contributes at least one level, so at least one value slot. A flat column's
                // rows are its values, and its num_rows is not read (PageMapBuilder), so it is not
                // checked either: pyarrow, DuckDB and DataFusion all read a flat page whose num_rows is
                // wrong, and so does EW's whole-chunk read.
                if (repeated && (v2.NumRows < 0 || v2.NumRows > v2.NumValues))
                    return $"it holds {v2.NumRows} rows in {v2.NumValues} values.";
                if (v2.RepetitionLevelsByteLength < 0 || v2.DefinitionLevelsByteLength < 0)
                {
                    return $"its level lengths are {v2.RepetitionLevelsByteLength} (repetition) and " +
                        $"{v2.DefinitionLevelsByteLength} (definition) bytes.";
                }
                // The levels are stored uncompressed ahead of the values, so both sizes include them.
                long levels = (long)v2.RepetitionLevelsByteLength + v2.DefinitionLevelsByteLength;
                if (levels > header.CompressedPageSize || levels > header.UncompressedPageSize)
                {
                    return $"its levels take {levels} bytes, more than its size ({header.CompressedPageSize} " +
                        $"compressed, {header.UncompressedPageSize} uncompressed).";
                }
                // Every non-null value takes at least one byte in every encoding.
                if (v2.NumNulls < v2.NumValues
                    && (levels == header.CompressedPageSize || levels == header.UncompressedPageSize))
                {
                    return $"it holds {v2.NumValues - v2.NumNulls} non-null values but no value bytes.";
                }
                break;

            case PageType.DictionaryPage:
                if (header.DictionaryPageHeader is not { } dictionary)
                    return "it has no dictionary_page_header.";
                if (dictionary.NumValues < 0)
                    return $"it holds {dictionary.NumValues} entries.";
                break;

            case PageType.SymbolTablePage:
                if (header.SymbolTablePageHeader is null)
                    return "it has no symbol_table_page_header.";
                break;
        }

        return null;
    }
}
