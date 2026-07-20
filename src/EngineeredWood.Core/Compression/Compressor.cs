// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.IO.Compression;
using K4os.Compression.LZ4;
using Snappier;
using ZstdCompressor = ZstdSharp.Compressor;

namespace EngineeredWood.Compression;

/// <summary>
/// Dispatches compression based on the Parquet compression codec.
/// Mirror of <see cref="Decompressor"/>.
/// </summary>
internal static class Compressor
{
    [ThreadStatic]
    private static ZstdCompressor? t_zstd;

    /// <summary>
    /// Returns the maximum compressed size for a given codec and input length.
    /// Used to pre-allocate output buffers.
    /// </summary>
    public static int GetMaxCompressedLength(CompressionCodec codec, int inputLength)
    {
        return codec switch
        {
            CompressionCodec.Uncompressed => inputLength,
            CompressionCodec.Snappy => Snappy.GetMaxCompressedLength(inputLength),
            CompressionCodec.Gzip => inputLength + 64, // conservative overhead estimate
#if NET6_0_OR_GREATER
            CompressionCodec.Brotli => BrotliEncoder.GetMaxCompressedLength(inputLength),
#else
            CompressionCodec.Brotli => inputLength + 64, // conservative estimate for BrotliSharpLib
#endif
            CompressionCodec.Lz4 => LZ4Codec.MaximumOutputSize(inputLength),
            CompressionCodec.Deflate => inputLength + 64,
            CompressionCodec.Zstd => ZstdSharp.Compressor.GetCompressBound(inputLength),
            _ => throw new NotSupportedException($"Compression codec '{codec}' is not supported for writing."),
        };
    }

    /// <summary>
    /// Compresses <paramref name="source"/> into <paramref name="destination"/>.
    /// </summary>
    /// <param name="codec">The compression codec to use.</param>
    /// <param name="source">The data to compress.</param>
    /// <param name="destination">Buffer to receive compressed output (at least <see cref="GetMaxCompressedLength"/> bytes).</param>
    /// <param name="level">
    /// Optional codec-agnostic level. <see langword="null"/> preserves each codec's historical default
    /// (Gzip/Deflate Fastest, Brotli/Zstd library default, Lz4 fast).
    /// Codecs without a tunable level (Snappy, Lz4Hadoop, Uncompressed) ignore this argument.
    /// </param>
    /// <param name="customLevel">
    /// Optional explicit native level. When set, overrides <paramref name="level"/>.
    /// Honored by Zstd (1..22), Brotli (0..11), and raw Lz4 (LZ4Level enum value).
    /// Ignored by Gzip/Deflate (BCL exposes only an enum).
    /// </param>
    /// <returns>The number of bytes written to <paramref name="destination"/>.</returns>
    public static int Compress(
        CompressionCodec codec,
        ReadOnlySpan<byte> source,
        Span<byte> destination,
        BlockCompressionLevel? level = null,
        int? customLevel = null)
    {
        return codec switch
        {
            CompressionCodec.Uncompressed => CompressUncompressed(source, destination),
            CompressionCodec.Snappy => CompressSnappy(source, destination),
            CompressionCodec.Gzip => CompressGzip(source, destination, level),
            CompressionCodec.Brotli => CompressBrotli(source, destination, level, customLevel),
            CompressionCodec.Lz4 => CompressLz4(source, destination, level, customLevel),
            CompressionCodec.Deflate => CompressDeflate(source, destination, level),
            CompressionCodec.Zstd => CompressZstd(source, destination, level, customLevel),
            _ => throw new NotSupportedException($"Compression codec '{codec}' is not supported for writing."),
        };
    }

    private static int CompressUncompressed(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        source.CopyTo(destination);
        return source.Length;
    }

    private static int CompressSnappy(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        return Snappy.Compress(source, destination);
    }

    private static CompressionLevel MapBclLevel(BlockCompressionLevel level) => level switch
    {
        BlockCompressionLevel.Fastest => CompressionLevel.Fastest,
#if NET6_0_OR_GREATER
        BlockCompressionLevel.SmallestSize => CompressionLevel.SmallestSize,
#else
        BlockCompressionLevel.SmallestSize => CompressionLevel.Optimal,
#endif
        _ => CompressionLevel.Optimal,
    };

    private static int CompressGzip(ReadOnlySpan<byte> source, Span<byte> destination, BlockCompressionLevel? level)
    {
        var bclLevel = level.HasValue ? MapBclLevel(level.Value) : CompressionLevel.Fastest;
        using var outputStream = new MemoryStream();
        using (var gzip = new GZipStream(outputStream, bclLevel, leaveOpen: true))
        {
#if NET6_0_OR_GREATER
            gzip.Write(source);
#else
            byte[] array = source.ToArray();
            gzip.Write(array, 0, array.Length);
#endif
        }

        int compressedLength = checked((int)outputStream.Length);
        outputStream.GetBuffer().AsSpan(0, compressedLength).CopyTo(destination);
        return compressedLength;
    }

    private static int CompressBrotli(
        ReadOnlySpan<byte> source, Span<byte> destination,
        BlockCompressionLevel? level, int? customLevel)
    {
#if NET6_0_OR_GREATER
        if (level.HasValue || customLevel.HasValue)
        {
            int quality = customLevel ?? MapBrotliQuality(level!.Value);
            // Window size 22 is BrotliEncoder's documented default.
            if (!BrotliEncoder.TryCompress(source, destination, out int bytesWritten, quality, 22))
                throw new InvalidOperationException("Brotli compression failed.");
            return bytesWritten;
        }
        if (!BrotliEncoder.TryCompress(source, destination, out int defaultBytesWritten))
            throw new InvalidOperationException("Brotli compression failed.");
        return defaultBytesWritten;
#else
        if (level.HasValue || customLevel.HasValue)
        {
            int quality = customLevel ?? MapBrotliQuality(level!.Value);
            byte[] compressed = BrotliSharpLib.Brotli.CompressBuffer(
                source.ToArray(), 0, source.Length, quality, 22, null);
            compressed.AsSpan().CopyTo(destination);
            return compressed.Length;
        }
        byte[] defaultCompressed = BrotliSharpLib.Brotli.CompressBuffer(source.ToArray(), 0, source.Length);
        defaultCompressed.AsSpan().CopyTo(destination);
        return defaultCompressed.Length;
#endif
    }

    private static int MapBrotliQuality(BlockCompressionLevel level) => level switch
    {
        BlockCompressionLevel.Fastest => 1,
        BlockCompressionLevel.SmallestSize => 11,
        _ => 4, // Optimal — matches BrotliEncoder default quality
    };

    private static int CompressLz4(
        ReadOnlySpan<byte> source, Span<byte> destination,
        BlockCompressionLevel? level, int? customLevel)
    {
        int encoded;
        if (level.HasValue || customLevel.HasValue)
        {
            var lz4Level = customLevel.HasValue
                ? (LZ4Level)customLevel.Value
                : MapLz4Level(level!.Value);
            encoded = LZ4Codec.Encode(source, destination, lz4Level);
        }
        else
        {
            encoded = LZ4Codec.Encode(source, destination);
        }

        if (encoded < 0)
            throw new InvalidOperationException("LZ4 compression failed.");
        return encoded;
    }

    private static LZ4Level MapLz4Level(BlockCompressionLevel level) => level switch
    {
        BlockCompressionLevel.Fastest => LZ4Level.L00_FAST,
        BlockCompressionLevel.SmallestSize => LZ4Level.L12_MAX,
        _ => LZ4Level.L09_HC,
    };

    private static int CompressDeflate(ReadOnlySpan<byte> source, Span<byte> destination, BlockCompressionLevel? level)
    {
        var bclLevel = level.HasValue ? MapBclLevel(level.Value) : CompressionLevel.Fastest;
        using var outputStream = new MemoryStream();
        using (var deflate = new DeflateStream(outputStream, bclLevel, leaveOpen: true))
        {
#if NET6_0_OR_GREATER
            deflate.Write(source);
#else
            byte[] array = source.ToArray();
            deflate.Write(array, 0, array.Length);
#endif
        }

        int compressedLength = checked((int)outputStream.Length);
        outputStream.GetBuffer().AsSpan(0, compressedLength).CopyTo(destination);
        return compressedLength;
    }

    private static int CompressZstd(
        ReadOnlySpan<byte> source, Span<byte> destination,
        BlockCompressionLevel? level, int? customLevel)
    {
        var zstd = t_zstd ??= new ZstdCompressor();
        if (level.HasValue || customLevel.HasValue)
        {
            zstd.Level = customLevel ?? MapZstdLevel(level!.Value);
        }
        else if (zstd.Level != ZstdCompressor.DefaultCompressionLevel)
        {
            // Reset to library default if a previous call on this thread mutated the level.
            zstd.Level = ZstdCompressor.DefaultCompressionLevel;
        }
        return zstd.Wrap(source, destination);
    }

    private static int MapZstdLevel(BlockCompressionLevel level) => level switch
    {
        BlockCompressionLevel.Fastest => 1,
        BlockCompressionLevel.SmallestSize => 22,
        _ => 3, // Optimal — Zstd library default
    };
}
