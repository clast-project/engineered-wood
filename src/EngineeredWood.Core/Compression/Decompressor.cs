// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.IO.Compression;
using K4os.Compression.LZ4;
using Snappier;
using ZstdDecompressor = ZstdSharp.Decompressor;

namespace EngineeredWood.Compression;

/// <summary>
/// Dispatches decompression based on the Parquet compression codec.
/// </summary>
internal static class Decompressor
{
    [ThreadStatic]
    private static ZstdDecompressor? t_zstd;

    /// <summary>
    /// Decompresses <paramref name="source"/> into <paramref name="destination"/>.
    /// For <see cref="CompressionCodec.Uncompressed"/>, the source is copied directly.
    /// </summary>
    /// <returns>The number of bytes written to <paramref name="destination"/>.</returns>
    public static int Decompress(
        CompressionCodec codec,
        ReadOnlySpan<byte> source,
        Span<byte> destination)
    {
        return codec switch
        {
            CompressionCodec.Uncompressed => DecompressUncompressed(source, destination),
            CompressionCodec.Snappy => DecompressSnappy(source, destination),
            CompressionCodec.Gzip => DecompressGzip(source, destination),
            CompressionCodec.Brotli => DecompressBrotli(source, destination),
            CompressionCodec.Lz4Hadoop => DecompressHadoopLz4(source, destination),
            CompressionCodec.Zstd => DecompressZstd(source, destination),
            CompressionCodec.Lz4 => DecompressLz4Raw(source, destination),
            CompressionCodec.Deflate => DecompressDeflate(source, destination),
            _ => throw new NotSupportedException($"Compression codec '{codec}' is not supported."),
        };
    }

    private static int DecompressUncompressed(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        source.CopyTo(destination);
        return source.Length;
    }

    private static int DecompressSnappy(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        return Snappy.Decompress(source, destination);
    }

    private static int DecompressGzip(ReadOnlySpan<byte> source, Span<byte> destination)
    {
#if NET6_0_OR_GREATER
        unsafe
        {
            fixed (byte* ptr = source)
            {
                using var sourceStream = new UnmanagedMemoryStream(ptr, source.Length);
                using var gzip = new GZipStream(sourceStream, CompressionMode.Decompress);
                int totalRead = 0;
                while (totalRead < destination.Length)
                {
                    int read = gzip.Read(destination.Slice(totalRead));
                    if (read == 0) break;
                    totalRead += read;
                }
                return totalRead;
            }
        }
#else
        // .NET Framework's GZipStream stops after the first Gzip member and may
        // over-read the underlying stream. Use a MemoryStream and re-create the
        // GZipStream for each concatenated member (per RFC 1952).
        byte[] sourceArray = source.ToArray();
        var sourceStream = new MemoryStream(sourceArray);
        byte[] tempBuffer = new byte[destination.Length];
        int totalRead = 0;

        while (sourceStream.Position < sourceStream.Length && totalRead < tempBuffer.Length)
        {
            // Mark start so we can detect if GZipStream consumed anything
            long before = sourceStream.Position;
            using (var gzip = new GZipStream(sourceStream, CompressionMode.Decompress, leaveOpen: true))
            {
                while (totalRead < tempBuffer.Length)
                {
                    int read = gzip.Read(tempBuffer, totalRead, tempBuffer.Length - totalRead);
                    if (read == 0) break;
                    totalRead += read;
                }
            }
            // If GZipStream didn't advance the stream, we're stuck — break to avoid infinite loop
            if (sourceStream.Position == before)
                break;
        }

        tempBuffer.AsSpan(0, totalRead).CopyTo(destination);
        return totalRead;
#endif
    }

    private static int DecompressBrotli(ReadOnlySpan<byte> source, Span<byte> destination)
    {
#if NET6_0_OR_GREATER
        if (!BrotliDecoder.TryDecompress(source, destination, out int bytesWritten))
            throw new InvalidDataException("Brotli decompression failed.");
        return bytesWritten;
#else
        byte[] decompressed = BrotliSharpLib.Brotli.DecompressBuffer(source.ToArray(), 0, source.Length);
        decompressed.AsSpan().CopyTo(destination);
        return decompressed.Length;
#endif
    }

    private static int DecompressLz4Raw(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int decoded = LZ4Codec.Decode(source, destination);
        if (decoded < 0)
            throw new InvalidDataException("LZ4 decompression failed.");
        return decoded;
    }

    /// <summary>
    /// Decompresses data written with the deprecated LZ4 codec (value 5).
    /// Different writers used different formats: Hadoop framing, LZ4 frame format, or raw blocks.
    /// Tries each in order until one succeeds.
    /// </summary>
    private static int DecompressHadoopLz4(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (TryDecompressHadoopFramed(source, destination, out int written))
            return written;

        if (TryDecompressLz4Frame(source, destination, out written))
            return written;

        // Last fallback: treat as raw LZ4 block (some writers label raw LZ4 as Hadoop LZ4)
        return DecompressLz4Raw(source, destination);
    }

    private static bool TryDecompressHadoopFramed(
        ReadOnlySpan<byte> source, Span<byte> destination, out int bytesWritten)
    {
        bytesWritten = 0;
        if (source.Length < 8)
            return false;

        // Read first frame header — validate before committing to Hadoop framing
        uint rawDecomp = BinaryPrimitives.ReadUInt32BigEndian(source);
        uint rawComp = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(4));

        if (rawDecomp == 0 || rawComp == 0 ||
            rawDecomp > (uint)destination.Length ||
            rawComp > (uint)(source.Length - 8))
            return false;

        int srcOffset = 0;
        int dstOffset = 0;

        while (srcOffset + 8 <= source.Length)
        {
            uint decompSize = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(srcOffset));
            uint compSize = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(srcOffset + 4));
            srcOffset += 8;

            if (compSize == 0 || decompSize == 0 ||
                srcOffset + (int)compSize > source.Length ||
                dstOffset + (int)decompSize > destination.Length)
                return false;

            int decoded = LZ4Codec.Decode(
                source.Slice(srcOffset, (int)compSize),
                destination.Slice(dstOffset, (int)decompSize));

            if (decoded != (int)decompSize)
                return false; // frame decode mismatch — not valid Hadoop framing

            srcOffset += (int)compSize;
            dstOffset += (int)decompSize;
        }

        bytesWritten = dstOffset;
        return bytesWritten > 0;
    }

    /// <summary>
    /// LZ4 frame format magic number. On disk: 04 22 4D 18; as LE uint32: 0x184D2204.
    /// Used by some writers (e.g. parquet-go) with the deprecated LZ4 codec.
    /// </summary>
    private const uint Lz4FrameMagic = 0x184D2204;

    /// <summary>
    /// Attempts to decompress LZ4 frame format data (magic 0x04224D18).
    /// Format: magic (4) + FLG (1) + BD (1) + [content size (8)] + HC (1) + blocks + end mark (4) + [checksum (4)].
    /// Each block: 4-byte LE size (bit 31 = uncompressed flag) + data.
    /// </summary>
    private static bool TryDecompressLz4Frame(
        ReadOnlySpan<byte> source, Span<byte> destination, out int bytesWritten)
    {
        bytesWritten = 0;

        // Minimum: magic(4) + FLG(1) + BD(1) + HC(1) + end_mark(4) = 11
        if (source.Length < 11)
            return false;

        // Check magic number
        if (BinaryPrimitives.ReadUInt32LittleEndian(source) != Lz4FrameMagic)
            return false;

        int srcOffset = 4;
        byte flg = source[srcOffset++];

        // Version must be 01 (bits 7-6)
        if ((flg >> 6) != 1)
            return false;

        bool hasBlockChecksum = (flg & 0x10) != 0;
        bool hasContentSize = (flg & 0x08) != 0;
        bool hasContentChecksum = (flg & 0x04) != 0;

        srcOffset++; // skip BD byte
        if (hasContentSize)
            srcOffset += 8; // skip content size
        srcOffset++; // skip HC (header checksum)

        if (srcOffset >= source.Length)
            return false;

        int dstOffset = 0;

        // Read blocks until end mark (block size = 0)
        while (srcOffset + 4 <= source.Length)
        {
            uint blockHeader = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(srcOffset));
            srcOffset += 4;

            if (blockHeader == 0)
                break; // end mark

            bool isUncompressed = (blockHeader & 0x80000000) != 0;
            int blockSize = (int)(blockHeader & 0x7FFFFFFF);

            if (srcOffset + blockSize > source.Length)
                return false;

            if (isUncompressed)
            {
                if (dstOffset + blockSize > destination.Length)
                    return false;
                source.Slice(srcOffset, blockSize).CopyTo(destination.Slice(dstOffset));
                dstOffset += blockSize;
            }
            else
            {
                int decoded = LZ4Codec.Decode(
                    source.Slice(srcOffset, blockSize),
                    destination.Slice(dstOffset));
                if (decoded < 0)
                    return false;
                dstOffset += decoded;
            }

            srcOffset += blockSize;

            if (hasBlockChecksum)
                srcOffset += 4; // skip block checksum
        }

        bytesWritten = dstOffset;
        return bytesWritten > 0;
    }

    private static unsafe int DecompressDeflate(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        fixed (byte* ptr = source)
        {
            using var sourceStream = new UnmanagedMemoryStream(ptr, source.Length);
            using var deflate = new DeflateStream(sourceStream, CompressionMode.Decompress);
            int totalRead = 0;
#if NET6_0_OR_GREATER
            while (totalRead < destination.Length)
            {
                int read = deflate.Read(destination.Slice(totalRead));
                if (read == 0) break;
                totalRead += read;
            }
#else
            byte[] tempBuffer = new byte[destination.Length];
            while (totalRead < tempBuffer.Length)
            {
                int read = deflate.Read(tempBuffer, totalRead, tempBuffer.Length - totalRead);
                if (read == 0) break;
                totalRead += read;
            }
            tempBuffer.AsSpan(0, totalRead).CopyTo(destination);
#endif
            return totalRead;
        }
    }

    private static int DecompressZstd(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        var zstd = t_zstd ??= new ZstdDecompressor();
        return zstd.Unwrap(source, destination);
    }

    /// <summary>
    /// Returns the decompressed length for compressed data.
    /// For uncompressed data, returns the source length.
    /// </summary>
    public static int GetDecompressedLength(CompressionCodec codec, ReadOnlySpan<byte> source)
    {
        return codec switch
        {
            CompressionCodec.Uncompressed => source.Length,
            CompressionCodec.Snappy => Snappy.GetUncompressedLength(source),
            CompressionCodec.Zstd => checked((int)ZstdDecompressor.GetDecompressedSize(source)),
            // Gzip, Brotli, LZ4: decompressed size comes from Parquet page headers
            _ => throw new NotSupportedException(
                $"GetDecompressedLength is not supported for codec '{codec}'. " +
                "Use the uncompressed page size from the page header instead."),
        };
    }
}
