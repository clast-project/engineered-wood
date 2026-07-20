// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.IO.Hashing;
using EngineeredWood.Buffers;
using EngineeredWood.Compression;

namespace EngineeredWood.Avro;

/// <summary>
/// Bridges Avro compression codecs to the shared Core compressor/decompressor.
/// Handles Avro-specific framing (e.g. Snappy with trailing CRC32C).
/// </summary>
internal static class AvroCompression
{
    /// <summary>
    /// Compresses data into a caller-owned buffer, avoiding per-call allocations.
    /// The caller should call <see cref="GrowableBuffer.Reset"/> before this method.
    /// </summary>
    public static void Compress(
        AvroCodec codec, ReadOnlySpan<byte> data, GrowableBuffer output,
        BlockCompressionLevel? level = null, int? customLevel = null)
    {
        if (codec == AvroCodec.Null)
        {
            output.Write(data);
            return;
        }

        if (codec == AvroCodec.Snappy)
        {
            CompressSnappy(data, output);
            return;
        }

        if (codec == AvroCodec.Lz4)
        {
            CompressLz4(data, output, level, customLevel);
            return;
        }

        var coreCodec = ToCoreCodec(codec);
        int maxLen = Compressor.GetMaxCompressedLength(coreCodec, data.Length);
        var span = output.GetSpan(maxLen);
        int written = Compressor.Compress(coreCodec, data, span, level, customLevel);
        output.Advance(written);
    }

    /// <summary>
    /// Decompresses data into a caller-owned buffer, avoiding per-call allocations.
    /// The caller should call <see cref="GrowableBuffer.Reset"/> before this method.
    /// </summary>
    /// <remarks>
    /// Prefer the <see cref="Decompress(AvroCodec, byte[], int, int, GrowableBuffer)"/> overload
    /// when the underlying array is available — it avoids an array copy for Deflate.
    /// </remarks>
    public static void Decompress(AvroCodec codec, ReadOnlySpan<byte> data, GrowableBuffer output)
    {
        if (codec == AvroCodec.Null)
        {
            output.Write(data);
            return;
        }

        if (codec == AvroCodec.Snappy)
        {
            DecompressSnappy(data, output);
            return;
        }

        if (codec == AvroCodec.Lz4)
        {
            DecompressLz4(data, output);
            return;
        }

        if (codec == AvroCodec.Zstandard)
        {
            DecompressZstd(data, output);
            return;
        }

        // Deflate: unknown uncompressed size, use stream-based decompression.
        // Must copy to array because DeflateStream requires a Stream.
        DecompressDeflate(data.ToArray(), 0, data.Length, output);
    }

    /// <summary>
    /// Decompresses from a byte array, avoiding the span-to-array copy for Deflate.
    /// </summary>
    public static void Decompress(AvroCodec codec, byte[] data, int offset, int count, GrowableBuffer output)
    {
        if (codec == AvroCodec.Deflate)
        {
            DecompressDeflate(data, offset, count, output);
            return;
        }

        // All other codecs work on spans — delegate to the span overload.
        Decompress(codec, data.AsSpan(offset, count), output);
    }

    private static void DecompressDeflate(byte[] data, int offset, int count, GrowableBuffer output)
    {
        using var sourceStream = new MemoryStream(data, offset, count, writable: false);
        using var decompressionStream = new System.IO.Compression.DeflateStream(
            sourceStream, System.IO.Compression.CompressionMode.Decompress);
        while (true)
        {
            var span = output.GetSpan(4096);
#if NETSTANDARD2_0
            var tmp = new byte[4096];
            int read = decompressionStream.Read(tmp, 0, 4096);
            if (read == 0) break;
            tmp.AsSpan(0, read).CopyTo(span);
#else
            int read = decompressionStream.Read(span[..4096]);
            if (read == 0) break;
#endif
            output.Advance(read);
        }
    }

    private static void DecompressZstd(ReadOnlySpan<byte> data, GrowableBuffer output)
    {
        int size = checked((int)Decompressor.GetDecompressedLength(CompressionCodec.Zstd, data));
        var span = output.GetSpan(size);
        Decompressor.Decompress(CompressionCodec.Zstd, data, span[..size]);
        output.Advance(size);
    }

    /// <summary>
    /// Avro LZ4 = 4-byte LE uncompressed size + LZ4 block data.
    /// Compatible with Python lz4.block.compress(store_size=True).
    /// </summary>
    private static void CompressLz4(
        ReadOnlySpan<byte> data, GrowableBuffer output,
        BlockCompressionLevel? level, int? customLevel)
    {
        // Write 4-byte LE uncompressed size header
        var header = output.GetSpan(4);
        BinaryPrimitives.WriteInt32LittleEndian(header, data.Length);
        output.Advance(4);

        // Compress directly into the output buffer
        int maxLen = Compressor.GetMaxCompressedLength(CompressionCodec.Lz4, data.Length);
        var span = output.GetSpan(maxLen);
        int written = Compressor.Compress(CompressionCodec.Lz4, data, span, level, customLevel);
        output.Advance(written);
    }

    private static void DecompressLz4(ReadOnlySpan<byte> data, GrowableBuffer output)
    {
        if (data.Length < 4)
            throw new InvalidDataException("LZ4 data too short for size header.");

        int uncompressedSize = BinaryPrimitives.ReadInt32LittleEndian(data);
        var span = output.GetSpan(uncompressedSize);
        Decompressor.Decompress(CompressionCodec.Lz4, data[4..], span[..uncompressedSize]);
        output.Advance(uncompressedSize);
    }

    /// <summary>
    /// Avro Snappy = Snappy block + 4-byte big-endian CRC32C of uncompressed data.
    /// </summary>
    private static void CompressSnappy(ReadOnlySpan<byte> data, GrowableBuffer output)
    {
        // Compress directly into the output buffer
        int maxLen = Compressor.GetMaxCompressedLength(CompressionCodec.Snappy, data.Length);
        var span = output.GetSpan(maxLen + 4); // room for compressed + CRC
        int written = Compressor.Compress(CompressionCodec.Snappy, data, span);

        // Append CRC32C of uncompressed data
        uint crc = Crc32C(data);
        BinaryPrimitives.WriteUInt32BigEndian(span[written..], crc);
        output.Advance(written + 4);
    }

    private static void DecompressSnappy(ReadOnlySpan<byte> data, GrowableBuffer output)
    {
        // Last 4 bytes are CRC32C (big-endian) of the uncompressed data
        if (data.Length < 4)
            throw new InvalidDataException("Snappy data too short for CRC32C checksum.");

        var compressedData = data[..^4];
        uint expectedCrc = BinaryPrimitives.ReadUInt32BigEndian(data[^4..]);

        // Determine uncompressed size from Snappy header
        int uncompressedSize = checked((int)Decompressor.GetDecompressedLength(
            CompressionCodec.Snappy, compressedData));
        var span = output.GetSpan(uncompressedSize);
        Decompressor.Decompress(CompressionCodec.Snappy, compressedData, span[..uncompressedSize]);
        output.Advance(uncompressedSize);

        // Verify CRC
        uint actualCrc = Crc32C(span[..uncompressedSize]);
        if (actualCrc != expectedCrc)
            throw new InvalidDataException(
                $"Snappy CRC32C mismatch: expected 0x{expectedCrc:X8}, got 0x{actualCrc:X8}.");
    }

    private static uint Crc32C(ReadOnlySpan<byte> data)
    {
        var crc = new Crc32();
        crc.Append(data);
        Span<byte> hash = stackalloc byte[4];
        crc.GetHashAndReset(hash);
        return BinaryPrimitives.ReadUInt32LittleEndian(hash);
    }

    private static CompressionCodec ToCoreCodec(AvroCodec codec) => codec switch
    {
        AvroCodec.Deflate => CompressionCodec.Deflate,
        AvroCodec.Zstandard => CompressionCodec.Zstd,
        _ => throw new NotSupportedException($"Avro codec {codec} is not supported."),
    };

    public static string CodecName(AvroCodec codec) => codec switch
    {
        AvroCodec.Null => "null",
        AvroCodec.Deflate => "deflate",
        AvroCodec.Snappy => "snappy",
        AvroCodec.Zstandard => "zstandard",
        AvroCodec.Lz4 => "lz4",
        _ => throw new ArgumentOutOfRangeException(nameof(codec)),
    };

    public static AvroCodec ParseCodecName(string name) => name switch
    {
        "null" => AvroCodec.Null,
        "deflate" => AvroCodec.Deflate,
        "snappy" => AvroCodec.Snappy,
        "zstandard" => AvroCodec.Zstandard,
        "lz4" => AvroCodec.Lz4,
        _ => throw new NotSupportedException($"Unknown Avro codec: '{name}'"),
    };
}
