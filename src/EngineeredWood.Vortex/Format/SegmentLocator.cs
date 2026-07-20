// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;

namespace EngineeredWood.Vortex.Format;

/// <summary>
/// Materialized form of a Vortex segment reference (a <c>PostscriptSegment</c>
/// or a <c>SegmentSpec</c>). Carries the byte range plus the codec needed to
/// decompress it.
/// </summary>
internal readonly record struct SegmentLocator(
    ulong Offset,
    uint Length,
    byte AlignmentExponent,
    CompressionCodec Codec)
{
    /// <summary>Maps Vortex's per-segment <see cref="CompressionScheme"/> to Core's codec.</summary>
    public static CompressionCodec MapScheme(CompressionScheme scheme) => scheme switch
    {
        CompressionScheme.None => CompressionCodec.Uncompressed,
        CompressionScheme.LZ4 => CompressionCodec.Lz4,
        CompressionScheme.ZLib => CompressionCodec.Deflate,
        CompressionScheme.ZStd => CompressionCodec.Zstd,
        _ => throw new VortexFormatException(
            $"Unknown Vortex CompressionScheme value {(int)scheme}."),
    };
}
