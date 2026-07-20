// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Metadata for a single column chunk.
/// </summary>
public sealed class ColumnMetaData
{
    /// <summary>Physical type of the column.</summary>
    public required PhysicalType Type { get; init; }

    /// <summary>Encodings used in this column chunk.</summary>
    public required IReadOnlyList<Encoding> Encodings { get; init; }

    /// <summary>
    /// Path components in the schema (one entry per nesting level). The Parquet spec
    /// describes this field as required, but several writers — and an in-progress spec
    /// change — omit it when the row group columns line up positionally with the file
    /// schema. <see langword="null"/> on read means the file did not include the field;
    /// when re-writing such metadata, callers should fall back to the schema-derived path.
    /// </summary>
    public IReadOnlyList<string>? PathInSchema { get; init; }

    /// <summary>Compression codec used.</summary>
    public required CompressionCodec Codec { get; init; }

    /// <summary>Total number of values (including nulls) in this column chunk.</summary>
    public required long NumValues { get; init; }

    /// <summary>Total uncompressed byte size of all pages in this column chunk.</summary>
    public required long TotalUncompressedSize { get; init; }

    /// <summary>Total compressed byte size of all pages in this column chunk.</summary>
    public required long TotalCompressedSize { get; init; }

    /// <summary>Byte offset of the first data page in the file.</summary>
    public required long DataPageOffset { get; init; }

    /// <summary>Byte offset of the index page, if any.</summary>
    public long? IndexPageOffset { get; init; }

    /// <summary>Byte offset of the dictionary page, if any.</summary>
    public long? DictionaryPageOffset { get; init; }

    /// <summary>Column chunk statistics.</summary>
    public Statistics? Statistics { get; set; }

    /// <summary>Encoding statistics per page type.</summary>
    public IReadOnlyList<KeyValue>? KeyValueMetadata { get; init; }

    /// <summary>Byte offset of the Bloom filter block in the file, if present.</summary>
    public long? BloomFilterOffset { get; init; }

    /// <summary>Length of the Bloom filter block in bytes, if present.</summary>
    public int? BloomFilterLength { get; init; }
}
