// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decoded Parquet page header.
/// </summary>
internal sealed class PageHeader
{
    /// <summary>The type of this page.</summary>
    public required PageType Type { get; init; }

    /// <summary>Uncompressed page size in bytes (not including the header).</summary>
    public required int UncompressedPageSize { get; init; }

    /// <summary>Compressed page size in bytes (not including the header). Same as uncompressed if no compression.</summary>
    public required int CompressedPageSize { get; init; }

    /// <summary>Optional CRC-32C checksum of the compressed page data (excluding the page header).</summary>
    public int? Crc { get; init; }

    /// <summary>Data page header (V1). Null for non-data pages.</summary>
    public DataPageHeader? DataPageHeader { get; init; }

    /// <summary>Dictionary page header. Null for non-dictionary pages.</summary>
    public DictionaryPageHeader? DictionaryPageHeader { get; init; }

    /// <summary>Data page header V2. Null for non-V2 data pages.</summary>
    public DataPageHeaderV2? DataPageHeaderV2 { get; init; }
}

/// <summary>
/// Header for a V1 data page.
/// </summary>
internal sealed class DataPageHeader
{
    /// <summary>Number of values in this page (including nulls).</summary>
    public required int NumValues { get; init; }

    /// <summary>Encoding used for values in this page.</summary>
    public required Encoding Encoding { get; init; }

    /// <summary>Encoding used for definition levels.</summary>
    public required Encoding DefinitionLevelEncoding { get; init; }

    /// <summary>Encoding used for repetition levels.</summary>
    public required Encoding RepetitionLevelEncoding { get; init; }
}

/// <summary>
/// Header for a V2 data page.
/// </summary>
internal sealed class DataPageHeaderV2
{
    /// <summary>Number of values in this page (including nulls).</summary>
    public required int NumValues { get; init; }

    /// <summary>Number of null values in this page.</summary>
    public required int NumNulls { get; init; }

    /// <summary>Number of rows in this page.</summary>
    public required int NumRows { get; init; }

    /// <summary>Encoding used for values in this page.</summary>
    public required Encoding Encoding { get; init; }

    /// <summary>Byte length of the definition levels section.</summary>
    public required int DefinitionLevelsByteLength { get; init; }

    /// <summary>Byte length of the repetition levels section.</summary>
    public required int RepetitionLevelsByteLength { get; init; }

    /// <summary>Whether the values section is compressed. Defaults to true.</summary>
    public bool IsCompressed { get; init; } = true;
}

/// <summary>
/// Header for a dictionary page.
/// </summary>
internal sealed class DictionaryPageHeader
{
    /// <summary>Number of entries in the dictionary.</summary>
    public required int NumValues { get; init; }

    /// <summary>Encoding used for the dictionary values (always PLAIN).</summary>
    public required Encoding Encoding { get; init; }
}
