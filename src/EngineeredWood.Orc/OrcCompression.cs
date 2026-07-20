// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.Compression;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc;

/// <summary>
/// Handles ORC stream compression/decompression. ORC uses a block-based scheme
/// where each compressed block has a 3-byte header. Raw codec operations are
/// delegated to <see cref="Compressor"/> and <see cref="Decompressor"/> in Core.
/// </summary>
internal static class OrcCompression
{
    [ThreadStatic]
    private static MemoryStream? t_outputBuffer;

    /// <summary>
    /// Maps an ORC <see cref="CompressionKind"/> to the shared <see cref="CompressionCodec"/>.
    /// </summary>
    public static CompressionCodec ToCodec(CompressionKind kind) => kind switch
    {
        CompressionKind.None => CompressionCodec.Uncompressed,
        CompressionKind.Zlib => CompressionCodec.Deflate,
        CompressionKind.Snappy => CompressionCodec.Snappy,
        CompressionKind.Lz4 => CompressionCodec.Lz4,
        CompressionKind.Zstd => CompressionCodec.Zstd,
        _ => throw new NotSupportedException($"Compression kind {kind} is not supported."),
    };

    /// <summary>
    /// Decompresses an ORC compressed stream (which may contain multiple blocks).
    /// Each block has a 3-byte header:
    ///   - byte 0 bit 0: 0 = compressed, 1 = original (uncompressed)
    ///   - remaining 23 bits: length of the chunk
    /// </summary>
    public static byte[] Decompress(CompressionKind kind, ReadOnlySpan<byte> input, int compressionBlockSize)
    {
        if (kind == CompressionKind.None)
            return input.ToArray();

        var codec = ToCodec(kind);
        var output = t_outputBuffer ??= new MemoryStream();
        output.SetLength(0);
        var offset = 0;

        while (offset < input.Length)
        {
            if (offset + 3 > input.Length)
                throw new InvalidDataException("Truncated ORC compression block header.");

            int header = input[offset] | (input[offset + 1] << 8) | (input[offset + 2] << 16);
            offset += 3;

            bool isOriginal = (header & 1) == 1;
            int chunkLength = header >> 1;

            if (offset + chunkLength > input.Length)
                throw new InvalidDataException($"ORC compression block exceeds input bounds: offset={offset}, chunk={chunkLength}, total={input.Length}");

            var chunk = input.Slice(offset, chunkLength);
            offset += chunkLength;

            if (isOriginal)
            {
#if NETSTANDARD2_0
                output.Write(chunk.ToArray(), 0, chunk.Length);
#else
                output.Write(chunk);
#endif
            }
            else
            {
                var decompBuf = ArrayPool<byte>.Shared.Rent(compressionBlockSize);
                int written = Decompressor.Decompress(codec, chunk, decompBuf);
                output.Write(decompBuf, 0, written);
                ArrayPool<byte>.Shared.Return(decompBuf);
            }
        }

        return output.ToArray();
    }

    /// <summary>
    /// Decompresses like <see cref="Decompress"/>, but returns a buffer rented from
    /// <see cref="ArrayPool{T}.Shared"/>. The caller must return the buffer when done.
    /// </summary>
    public static (byte[] buffer, int length) DecompressPooled(CompressionKind kind, ReadOnlySpan<byte> input, int compressionBlockSize)
    {
        if (kind == CompressionKind.None)
        {
            var copy = ArrayPool<byte>.Shared.Rent(input.Length);
            input.CopyTo(copy);
            return (copy, input.Length);
        }

        var codec = ToCodec(kind);
        var output = t_outputBuffer ??= new MemoryStream();
        output.SetLength(0);
        var offset = 0;

        while (offset < input.Length)
        {
            if (offset + 3 > input.Length)
                throw new InvalidDataException("Truncated ORC compression block header.");

            int header = input[offset] | (input[offset + 1] << 8) | (input[offset + 2] << 16);
            offset += 3;

            bool isOriginal = (header & 1) == 1;
            int chunkLength = header >> 1;

            if (offset + chunkLength > input.Length)
                throw new InvalidDataException($"ORC compression block exceeds input bounds: offset={offset}, chunk={chunkLength}, total={input.Length}");

            var chunk = input.Slice(offset, chunkLength);
            offset += chunkLength;

            if (isOriginal)
            {
#if NETSTANDARD2_0
                output.Write(chunk.ToArray(), 0, chunk.Length);
#else
                output.Write(chunk);
#endif
            }
            else
            {
                var decompBuf = ArrayPool<byte>.Shared.Rent(compressionBlockSize);
                int written = Decompressor.Decompress(codec, chunk, decompBuf);
                output.Write(decompBuf, 0, written);
                ArrayPool<byte>.Shared.Return(decompBuf);
            }
        }

        int length = (int)output.Length;
        var pooled = ArrayPool<byte>.Shared.Rent(length);
        output.Position = 0;
        output.Read(pooled, 0, length);
        return (pooled, length);
    }

    /// <summary>
    /// Compresses data using ORC's block-based compression scheme.
    /// Each block has a 3-byte header: bit0=isOriginal, bits1-23=chunkLength.
    /// </summary>
    public static byte[] Compress(
        CompressionKind kind, ReadOnlySpan<byte> input, int compressionBlockSize,
        BlockCompressionLevel? level = null, int? customLevel = null)
    {
        if (kind == CompressionKind.None)
            return input.ToArray();

        var codec = ToCodec(kind);
        var output = t_outputBuffer ??= new MemoryStream();
        output.SetLength(0);

        int offset = 0;
        while (offset < input.Length)
        {
            int blockSize = Math.Min(compressionBlockSize, input.Length - offset);
            var block = input.Slice(offset, blockSize);
            offset += blockSize;

            int maxCompressed = Compressor.GetMaxCompressedLength(codec, blockSize);
            var compBuf = ArrayPool<byte>.Shared.Rent(maxCompressed);
            int compLen = Compressor.Compress(codec, block, compBuf, level, customLevel);

            if (compLen < blockSize)
            {
                // Write compressed block header: isOriginal=0, length=compLen
                int header = compLen << 1;
                output.WriteByte((byte)(header & 0xFF));
                output.WriteByte((byte)((header >> 8) & 0xFF));
                output.WriteByte((byte)((header >> 16) & 0xFF));
                output.Write(compBuf, 0, compLen);
            }
            else
            {
                // Write original (uncompressed) block header: isOriginal=1, length=blockSize
                int header = (blockSize << 1) | 1;
                output.WriteByte((byte)(header & 0xFF));
                output.WriteByte((byte)((header >> 8) & 0xFF));
                output.WriteByte((byte)((header >> 16) & 0xFF));
#if NETSTANDARD2_0
                output.Write(block.ToArray(), 0, block.Length);
#else
                output.Write(block);
#endif
            }

            ArrayPool<byte>.Shared.Return(compBuf);
        }

        return output.ToArray();
    }

    /// <summary>
    /// Translates an uncompressed stream offset to compressed stream positions.
    /// Appends [compressed_block_start, decompressed_offset_within_block] to positions.
    /// </summary>
    public static void TranslatePosition(
        long uncompressedOffset, ReadOnlySpan<byte> uncompressedData,
        CompressionKind kind, int compressionBlockSize, IList<ulong> positions,
        BlockCompressionLevel? level = null, int? customLevel = null)
    {
        if (kind == CompressionKind.None)
        {
            positions.Add((ulong)uncompressedOffset);
            return;
        }

        var codec = ToCodec(kind);
        long inputOffset = 0;
        long compressedOffset = 0;

        while (inputOffset < uncompressedData.Length)
        {
            int blockSize = (int)Math.Min(compressionBlockSize, uncompressedData.Length - inputOffset);
            long nextInputOffset = inputOffset + blockSize;

            if (uncompressedOffset < nextInputOffset || nextInputOffset >= uncompressedData.Length)
            {
                positions.Add((ulong)compressedOffset);
                positions.Add((ulong)(uncompressedOffset - inputOffset));
                return;
            }

            // Compute compressed size of this block — must use the same level as the actual write
            // so the row index points to the correct compressed offset.
            var block = uncompressedData.Slice((int)inputOffset, blockSize);
            int maxCompressed = Compressor.GetMaxCompressedLength(codec, blockSize);
            var compBuf = ArrayPool<byte>.Shared.Rent(maxCompressed);
            int compLen = Compressor.Compress(codec, block, compBuf, level, customLevel);
            int outputBlockSize = compLen < blockSize ? compLen : blockSize;
            ArrayPool<byte>.Shared.Return(compBuf);

            compressedOffset += 3 + outputBlockSize;
            inputOffset = nextInputOffset;
        }

        positions.Add((ulong)compressedOffset);
        positions.Add(0);
    }
}
