// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro;

/// <summary>
/// OCF compression codecs per the Avro specification.
/// </summary>
public enum AvroCodec
{
    /// <summary>No compression.</summary>
    Null,
    /// <summary>DEFLATE (RFC 1951), no zlib header.</summary>
    Deflate,
    /// <summary>Snappy with trailing 4-byte big-endian CRC32C of uncompressed data.</summary>
    Snappy,
    /// <summary>Facebook Zstandard compression.</summary>
    Zstandard,
    /// <summary>LZ4 block compression with 4-byte LE uncompressed size prefix.</summary>
    Lz4,
}
