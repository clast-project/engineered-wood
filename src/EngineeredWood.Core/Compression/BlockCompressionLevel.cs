// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Compression;

/// <summary>
/// Codec-agnostic compression level used by file format writers.
/// Each codec maps these values to its native scale (e.g. zstd 1..22, gzip Fastest/Optimal).
/// Codecs without a tunable level (Snappy, raw LZ4 block, Lz4Hadoop) silently ignore this setting.
/// </summary>
public enum BlockCompressionLevel
{
    /// <summary>Optimize for compression speed at the cost of ratio.</summary>
    Fastest,

    /// <summary>Balanced compression speed and ratio. Each codec's typical default.</summary>
    Optimal,

    /// <summary>Optimize for compression ratio at the cost of speed.</summary>
    SmallestSize,
}
