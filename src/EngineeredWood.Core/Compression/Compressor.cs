// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if !NET6_0_OR_GREATER || NET11_0_OR_GREATER
using System.Buffers;
#endif
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
            CompressionCodec.Gzip => DeflateBound(inputLength),
#if NET6_0_OR_GREATER
            CompressionCodec.Brotli => BrotliEncoder.GetMaxCompressedLength(inputLength),
#else
            CompressionCodec.Brotli => inputLength + 64, // conservative estimate for BrotliSharpLib
#endif
            CompressionCodec.Lz4 => LZ4Codec.MaximumOutputSize(inputLength),
            CompressionCodec.Deflate => DeflateBound(inputLength),
            CompressionCodec.Zstd => ZstdSharp.Compressor.GetCompressBound(inputLength),
            _ => throw new NotSupportedException($"Compression codec '{codec}' is not supported for writing."),
        };
    }

    /// <summary>
    /// The most bytes Gzip or Deflate writes for <paramref name="inputLength"/> bytes, at any level and
    /// on any runtime.
    /// </summary>
    /// <remarks>
    /// zlib-ng, which .NET's GZipStream and DeflateStream use from .NET 9 on, writes incompressible
    /// input at Fastest as static-Huffman literals, and bytes 144 to 255 take 9 bits: up to 1/8 more,
    /// and about 5.5% for uniformly random bytes. zlib's stored blocks, on earlier runtimes, add 5
    /// bytes per 64 KB, well within that. The 64 covers the Gzip header and trailer and the final
    /// block. A bound of <c>inputLength + 64</c> was overrun from 1 KB of random bytes on .NET 10 and
    /// from about 200 KB on .NET 8 and .NET Framework.
    /// </remarks>
    private static int DeflateBound(int inputLength) =>
        checked((int)(inputLength + (inputLength + 7L) / 8 + 64));

    // An empty input's streams, written here rather than left to the runtime: zlib-ng writes nothing
    // at all for a stream with nothing written to it, and so does .NET Framework's DeflateStream. A
    // 0-byte payload declared compressed is not a valid stream (see Decompressor). Deflate: one final
    // fixed-Huffman block holding only end-of-block. Gzip: a header with no optional fields and an
    // unknown OS, that block, and a trailer of CRC-32 0 and length 0.
    private static ReadOnlySpan<byte> EmptyDeflateStream => [0x03, 0x00];

    private static ReadOnlySpan<byte> EmptyGzipStream =>
    [
        0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF,
        0x03, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

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
        if (source.IsEmpty)
        {
            EmptyGzipStream.CopyTo(destination);
            return EmptyGzipStream.Length;
        }

        return CompressZlibFormat(source, destination, level, gzip: true);
    }

    /// <summary>
    /// Runs a <see cref="GZipStream"/> or <see cref="DeflateStream"/> straight into
    /// <paramref name="destination"/>, which the caller sized by <see cref="GetMaxCompressedLength"/>.
    /// </summary>
    /// <remarks>
    /// Compressing into a growing <see cref="MemoryStream"/> and copying the result out allocated about
    /// twice the input per call: 2 MB for a 1 MiB page, on the large object heap, with Gen2
    /// collections to match. Writing into the pinned destination allocates only the streams.
    /// </remarks>
    private static unsafe int CompressZlibFormat(
        ReadOnlySpan<byte> source, Span<byte> destination, BlockCompressionLevel? level, bool gzip)
    {
        // Non-empty input always produces output, and the pointer a fixed statement gives an empty
        // span is null, which UnmanagedMemoryStream refuses.
        if (destination.IsEmpty)
            throw DestinationTooShort(innerException: null);

#if NET11_0_OR_GREATER
        return EncodeZlibFormat(source, destination, level, gzip);
#else
        var bclLevel = level.HasValue ? MapBclLevel(level.Value) : CompressionLevel.Fastest;
        fixed (byte* pointer = destination)
        {
            using var output = new UnmanagedMemoryStream(pointer, 0, destination.Length, FileAccess.Write);
            try
            {
                using Stream compressor = gzip
                    ? new GZipStream(output, bclLevel, leaveOpen: true)
                    : new DeflateStream(output, bclLevel, leaveOpen: true);
                WriteAll(compressor, source);
            }
            catch (NotSupportedException e)
            {
                // UnmanagedMemoryStream refuses to grow past its capacity. That surfaced as
                // ArgumentException when the result was copied out of a MemoryStream; keep it so.
                throw DestinationTooShort(e);
            }

            return checked((int)output.Position);
        }
#endif
    }

#if NET11_0_OR_GREATER
    [ThreadStatic]
    private static GZipEncoder? t_gzipEncoder;

    [ThreadStatic]
    private static int t_gzipQuality;

    [ThreadStatic]
    private static DeflateEncoder? t_deflateEncoder;

    [ThreadStatic]
    private static int t_deflateQuality;

    /// <summary>
    /// Gzip or Deflate through .NET 11's span-based encoders, one kept per thread and format, and
    /// reset between calls rather than built anew.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Measured against a GZipStream into the destination on .NET 11: 5-9% faster on 8 KiB pages,
    /// where a stream sets up a fresh deflate state per call and the encoder only resets one, and
    /// even at 64 KiB and 1 MiB. Nothing is allocated.
    /// </para>
    /// <para>
    /// The bytes are not GZipStream's. Both are zlib-ng at the same nominal level, but the stream
    /// configures it differently (its Fastest output differs from the first byte) and feeds it in
    /// chunks, so the encoder's output is a few bytes shorter, and equally valid Gzip or Deflate.
    /// </para>
    /// <para>
    /// One encoder per thread, rebuilt only when the level changes, because a zlib deflate state
    /// is a few hundred KB of native memory: a writer that mixes levels pays a rebuild, and a
    /// thread holds one state per format rather than one per level.
    /// </para>
    /// </remarks>
    private static int EncodeZlibFormat(
        ReadOnlySpan<byte> source, Span<byte> destination, BlockCompressionLevel? level, bool gzip)
    {
        int quality = ZlibQuality(level);
        OperationStatus status;
        int consumed, written;

        if (gzip)
        {
            var encoder = t_gzipEncoder;
            if (encoder is null || t_gzipQuality != quality)
            {
                encoder?.Dispose();
                encoder = t_gzipEncoder = new GZipEncoder(quality);
                t_gzipQuality = quality;
            }
            else
            {
                encoder.Reset(); // also clears what a call that threw left behind
            }

            status = encoder.Compress(source, destination, out consumed, out written, isFinalBlock: true);
        }
        else
        {
            var encoder = t_deflateEncoder;
            if (encoder is null || t_deflateQuality != quality)
            {
                encoder?.Dispose();
                encoder = t_deflateEncoder = new DeflateEncoder(quality);
                t_deflateQuality = quality;
            }
            else
            {
                encoder.Reset();
            }

            status = encoder.Compress(source, destination, out consumed, out written, isFinalBlock: true);
        }

        return status switch
        {
            OperationStatus.Done when consumed == source.Length => written,
            OperationStatus.DestinationTooSmall => throw DestinationTooShort(innerException: null),
            _ => throw new InvalidOperationException(
                $"{(gzip ? "Gzip" : "Deflate")} compression stopped with {status}, " +
                $"having read {consumed} of {source.Length} bytes."),
        };
    }

    /// <summary>
    /// The zlib level a <see cref="BlockCompressionLevel"/> names: what GZipStream and
    /// DeflateStream pass for the same <see cref="CompressionLevel"/>, 1, 6 (zlib's default) and 9.
    /// </summary>
    private static int ZlibQuality(BlockCompressionLevel? level) => level switch
    {
        null or BlockCompressionLevel.Fastest => 1,
        BlockCompressionLevel.SmallestSize => 9,
        _ => 6,
    };
#endif

    private static ArgumentException DestinationTooShort(Exception? innerException) =>
        new("Destination is too short for the compressed output; size it with GetMaxCompressedLength.",
            "destination", innerException);

    private static void WriteAll(Stream stream, ReadOnlySpan<byte> source)
    {
#if NET6_0_OR_GREATER
        stream.Write(source);
#else
        // Stream takes only arrays here: go through a pooled chunk rather than a copy of the whole input.
        byte[] chunk = ArrayPool<byte>.Shared.Rent(Math.Min(source.Length, WriteChunkSize));
        try
        {
            for (int offset = 0; offset < source.Length;)
            {
                int length = Math.Min(chunk.Length, source.Length - offset);
                source.Slice(offset, length).CopyTo(chunk);
                stream.Write(chunk, 0, length);
                offset += length;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunk);
        }
#endif
    }

#if !NET6_0_OR_GREATER
    private const int WriteChunkSize = 64 * 1024;
#endif

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
        if (source.IsEmpty)
        {
            EmptyDeflateStream.CopyTo(destination);
            return EmptyDeflateStream.Length;
        }

        return CompressZlibFormat(source, destination, level, gzip: false);
    }

    private static int CompressZstd(
        ReadOnlySpan<byte> source, Span<byte> destination,
        BlockCompressionLevel? level, int? customLevel)
    {
#if NET11_0_OR_GREATER
        if (source.Length >= NativeZstdMinimumLength)
        {
            int quality = customLevel
                ?? (level.HasValue ? MapZstdLevel(level.Value) : ZstdCompressor.DefaultCompressionLevel);
            if (quality >= ZstandardCompressionOptions.MinQuality && quality <= ZstandardCompressionOptions.MaxQuality)
                return EncodeZstd(source, destination, quality);
        }
#endif

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

#if NET11_0_OR_GREATER
    /// <summary>
    /// The smallest page the native Zstandard encoder takes; ZstdSharp keeps the smaller ones.
    /// </summary>
    /// <remarks>
    /// Measured on .NET 11 GA, a reused native encoder against ZstdSharp: 16-33% faster at
    /// 64 KiB and 1 MiB on compressible pages and 2-9% on poorly compressible ones, but up to 9%
    /// SLOWER at 8 KiB, where the native call's fixed cost is a larger share. The frames are
    /// byte-identical either way, so the choice is invisible to a reader. Decompression stays on
    /// ZstdSharp at every size: there the native decoder measured 7-23% slower.
    /// </remarks>
    private const int NativeZstdMinimumLength = 64 * 1024;

    [ThreadStatic]
    private static ZstandardEncoder? t_zstdNative;

    [ThreadStatic]
    private static int t_zstdNativeQuality;

    /// <summary>
    /// Zstandard through .NET 11's native encoder, one kept per thread and rebuilt only when the
    /// level changes.
    /// </summary>
    /// <remarks>
    /// Never the static <c>ZstandardEncoder.TryCompress</c>, which builds a context per call and
    /// measured 8-22% slower than ZstdSharp on small pages. The source length is set on every
    /// call so the frame records its content size, as ZstdSharp's <c>Wrap</c> does: readers size
    /// their output from it.
    /// </remarks>
    private static int EncodeZstd(ReadOnlySpan<byte> source, Span<byte> destination, int quality)
    {
        var encoder = t_zstdNative;
        if (encoder is null || t_zstdNativeQuality != quality)
        {
            encoder?.Dispose();
            encoder = t_zstdNative = new ZstandardEncoder(quality);
            t_zstdNativeQuality = quality;
        }
        else
        {
            encoder.Reset();
        }

        encoder.SetSourceLength(source.Length);
        var status = encoder.Compress(source, destination, out int consumed, out int written, isFinalBlock: true);

        return status switch
        {
            OperationStatus.Done when consumed == source.Length => written,
            OperationStatus.DestinationTooSmall => throw DestinationTooShort(innerException: null),
            _ => throw new InvalidOperationException(
                $"Zstd compression stopped with {status}, having read {consumed} of {source.Length} bytes."),
        };
    }
#endif

    private static int MapZstdLevel(BlockCompressionLevel level) => level switch
    {
        BlockCompressionLevel.Fastest => 1,
        BlockCompressionLevel.SmallestSize => 22,
        _ => 3, // Optimal — Zstd library default
    };
}
