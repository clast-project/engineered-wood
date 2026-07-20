// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Compression;

/// <summary>
/// Compression codecs supported across file formats (Parquet, ORC, etc.).
/// </summary>
public enum CompressionCodec
{
    /// <summary>No compression.</summary>
    Uncompressed,

    /// <summary>Snappy block codec.</summary>
    Snappy,

    /// <summary>Gzip (deflate + framing).</summary>
    Gzip,

    /// <summary>Brotli.</summary>
    Brotli,

    /// <summary>Raw LZ4 block codec (Parquet LZ4_RAW, ORC LZ4).</summary>
    Lz4,

    /// <summary>LZ4 with Hadoop framing (Parquet legacy codec 5).</summary>
    Lz4Hadoop,

    /// <summary>Zstandard.</summary>
    Zstd,

    /// <summary>Raw deflate without gzip framing (ORC's "Zlib").</summary>
    Deflate,

    /// <summary>LZO. Defined in the Parquet spec but not implemented.</summary>
    Lzo,
}
