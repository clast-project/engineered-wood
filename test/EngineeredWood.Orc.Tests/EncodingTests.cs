// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Buffers;
using EngineeredWood.Orc;
using EngineeredWood.Orc.Encodings;

namespace EngineeredWood.Orc.Tests;

public class BitWriterReaderTests
{
    [Fact]
    public void BitRoundTrip_SingleBits()
    {
        var buf = new GrowableBuffer();
        var writer = new BitWriter(buf);

        writer.WriteBits(1, 1);
        writer.WriteBits(0, 1);
        writer.WriteBits(1, 1);
        writer.WriteBits(1, 1);
        writer.WriteBits(0, 1);
        writer.Flush();

        var bytes1 = buf.WrittenSpan.ToArray();
        var reader = new BitReader(new OrcByteStream(bytes1, 0, bytes1.Length));

        Assert.Equal(1, reader.ReadBits(1));
        Assert.Equal(0, reader.ReadBits(1));
        Assert.Equal(1, reader.ReadBits(1));
        Assert.Equal(1, reader.ReadBits(1));
        Assert.Equal(0, reader.ReadBits(1));
    }

    [Fact]
    public void BitRoundTrip_MultipleBitWidths()
    {
        var buf = new GrowableBuffer();
        var writer = new BitWriter(buf);

        writer.WriteBits(1, 1);
        writer.WriteBits(0xA, 4);
        writer.WriteBits(0xFF, 8);
        writer.WriteBits(0x1234, 16);
        writer.WriteBits(0x12345678, 32);
        writer.WriteBits(0x123456789ABCDEF0L, 64);
        writer.Flush();

        var bytes2 = buf.WrittenSpan.ToArray();
        var reader = new BitReader(new OrcByteStream(bytes2, 0, bytes2.Length));

        Assert.Equal(1, reader.ReadBits(1));
        Assert.Equal(0xA, reader.ReadBits(4));
        Assert.Equal(0xFF, reader.ReadBits(8));
        Assert.Equal(0x1234, reader.ReadBits(16));
        Assert.Equal(0x12345678, reader.ReadBits(32));
        Assert.Equal(0x123456789ABCDEF0L, reader.ReadBits(64));
    }

    [Fact]
    public void BitRoundTrip_PackedInts()
    {
        var buf = new GrowableBuffer();
        var writer = new BitWriter(buf);

        long[] values = [3, 7, 1, 0, 5, 2, 6, 4];
        writer.WritePackedInts(values, 3);
        writer.Flush();

        var bytes3 = buf.WrittenSpan.ToArray();
        var reader = new BitReader(new OrcByteStream(bytes3, 0, bytes3.Length));

        var result = new long[8];
        reader.ReadPackedInts(result, 3);

        Assert.Equal(values, result);
    }

    [Fact]
    public void BitRoundTrip_ZeroBits()
    {
        var buf = new GrowableBuffer();
        var writer = new BitWriter(buf);

        writer.WriteBits(0, 0);
        writer.Flush();

        var bytes4 = buf.WrittenSpan.ToArray();
        var reader = new BitReader(new OrcByteStream(bytes4, 0, bytes4.Length));

        Assert.Equal(0, reader.ReadBits(0));
    }

    [Fact]
    public void BitRoundTrip_CrossByteBoundary()
    {
        var buf = new GrowableBuffer();
        var writer = new BitWriter(buf);

        writer.WriteBits(0x1F, 5); // 5 bits: 11111
        writer.WriteBits(0x7F, 7); // 7 bits: 1111111 (crosses byte boundary)
        writer.Flush();

        var bytes5 = buf.WrittenSpan.ToArray();
        var reader = new BitReader(new OrcByteStream(bytes5, 0, bytes5.Length));

        Assert.Equal(0x1F, reader.ReadBits(5));
        Assert.Equal(0x7F, reader.ReadBits(7));
    }

    [Fact]
    public void BitRoundTrip_MaxValue64Bits()
    {
        var buf = new GrowableBuffer();
        var writer = new BitWriter(buf);

        writer.WriteBits(long.MaxValue, 64);
        writer.Flush();

        var bytes6 = buf.WrittenSpan.ToArray();
        var reader = new BitReader(new OrcByteStream(bytes6, 0, bytes6.Length));

        Assert.Equal(long.MaxValue, reader.ReadBits(64));
    }
}

public class ByteRleTests
{
    [Fact]
    public void ByteRle_RunOfIdentical()
    {
        var buf = new GrowableBuffer();
        var encoder = new ByteRleEncoder(buf);

        var input = new byte[10];
#if NET8_0_OR_GREATER
        Array.Fill(input, (byte)0xAB);
#else
        for (int i = 0; i < input.Length; i++) input[i] = 0xAB;
#endif
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new ByteRleDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new byte[10];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void ByteRle_Literals()
    {
        var buf = new GrowableBuffer();
        var encoder = new ByteRleEncoder(buf);

        byte[] input = [1, 2, 3, 4, 5, 6, 7, 8];
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new ByteRleDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new byte[8];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void ByteRle_MixedRunsAndLiterals()
    {
        var buf = new GrowableBuffer();
        var encoder = new ByteRleEncoder(buf);

        // Run of 5 As, then 4 distinct bytes, then run of 6 Bs
        byte[] input = [
            0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
            1, 2, 3, 4,
            0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB
        ];
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new ByteRleDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new byte[input.Length];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void ByteRle_SingleByte()
    {
        var buf = new GrowableBuffer();
        var encoder = new ByteRleEncoder(buf);

        byte[] input = [42];
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new ByteRleDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new byte[1];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void ByteRle_LongRun()
    {
        var buf = new GrowableBuffer();
        var encoder = new ByteRleEncoder(buf);

        var input = new byte[130];
#if NET8_0_OR_GREATER
        Array.Fill(input, (byte)0xCC);
#else
        for (int i = 0; i < input.Length; i++) input[i] = 0xCC;
#endif
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new ByteRleDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new byte[130];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void ByteRle_LongLiterals()
    {
        var buf = new GrowableBuffer();
        var encoder = new ByteRleEncoder(buf);

        var input = new byte[128];
        for (int i = 0; i < 128; i++)
            input[i] = (byte)(i % 256);
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new ByteRleDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new byte[128];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void ByteRle_Empty()
    {
        var buf = new GrowableBuffer();
        var encoder = new ByteRleEncoder(buf);

        encoder.WriteValues(ReadOnlySpan<byte>.Empty);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new ByteRleDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new byte[0];
        decoder.ReadValues(output);

        Assert.Empty(output);
    }

    [Fact]
    public void ByteRle_AllZeros()
    {
        var buf = new GrowableBuffer();
        var encoder = new ByteRleEncoder(buf);

        var input = new byte[50]; // all zeros by default
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new ByteRleDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new byte[50];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void ByteRle_ReadInSmallBatches()
    {
        var buf = new GrowableBuffer();
        var encoder = new ByteRleEncoder(buf);

        var input = new byte[20];
        for (int i = 0; i < 20; i++)
            input[i] = (byte)(i * 3);
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new ByteRleDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var allOutput = new byte[20];
        int offset = 0;
        while (offset < 20)
        {
            int count = Math.Min(3, 20 - offset);
            decoder.ReadValues(allOutput.AsSpan(offset, count));
            offset += count;
        }

        Assert.Equal(input, allOutput);
    }
}

public class BooleanEncoderDecoderTests
{
    [Fact]
    public void Boolean_AllTrue()
    {
        var buf = new GrowableBuffer();
        var encoder = new BooleanEncoder(buf);

        var input = new bool[10];
#if NET8_0_OR_GREATER
        Array.Fill(input, true);
#else
        for (int i = 0; i < input.Length; i++) input[i] = true;
#endif
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new BooleanDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new bool[10];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void Boolean_AllFalse()
    {
        var buf = new GrowableBuffer();
        var encoder = new BooleanEncoder(buf);

        var input = new bool[10]; // all false by default
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new BooleanDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new bool[10];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void Boolean_Alternating()
    {
        var buf = new GrowableBuffer();
        var encoder = new BooleanEncoder(buf);

        var input = new bool[12];
        for (int i = 0; i < input.Length; i++)
            input[i] = i % 2 == 0;
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new BooleanDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new bool[12];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void Boolean_SingleValue()
    {
        var buf = new GrowableBuffer();
        var encoder = new BooleanEncoder(buf);

        bool[] input = [true];
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new BooleanDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new bool[1];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void Boolean_SevenValues()
    {
        var buf = new GrowableBuffer();
        var encoder = new BooleanEncoder(buf);

        bool[] input = [true, false, true, true, false, false, true];
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new BooleanDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new bool[7];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void Boolean_EightValues()
    {
        var buf = new GrowableBuffer();
        var encoder = new BooleanEncoder(buf);

        bool[] input = [true, false, true, false, true, false, true, false];
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new BooleanDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new bool[8];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void Boolean_NineValues()
    {
        var buf = new GrowableBuffer();
        var encoder = new BooleanEncoder(buf);

        bool[] input = [true, true, false, true, false, false, true, true, false];
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new BooleanDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new bool[9];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }

    [Fact]
    public void Boolean_LargeCount()
    {
        var buf = new GrowableBuffer();
        var encoder = new BooleanEncoder(buf);

        var rng = new Random(42); // fixed seed for reproducibility
        var input = new bool[100];
        for (int i = 0; i < 100; i++)
            input[i] = rng.Next(2) == 1;
        encoder.WriteValues(input);
        encoder.Flush();

        var decoderBytes = buf.WrittenSpan.ToArray();
        var decoder = new BooleanDecoder(new OrcByteStream(decoderBytes, 0, decoderBytes.Length));

        var output = new bool[100];
        decoder.ReadValues(output);

        Assert.Equal(input, output);
    }
}

public class CompressionRoundTripTests
{
    [Theory]
    [InlineData(EngineeredWood.Orc.Proto.CompressionKind.Zlib, 256 * 1024)]
    [InlineData(EngineeredWood.Orc.Proto.CompressionKind.Zlib, 64)]
    [InlineData(EngineeredWood.Orc.Proto.CompressionKind.Snappy, 256 * 1024)]
    [InlineData(EngineeredWood.Orc.Proto.CompressionKind.Snappy, 128)]
    [InlineData(EngineeredWood.Orc.Proto.CompressionKind.Lz4, 256 * 1024)]
    [InlineData(EngineeredWood.Orc.Proto.CompressionKind.Lz4, 100)]
    [InlineData(EngineeredWood.Orc.Proto.CompressionKind.Zstd, 256 * 1024)]
    [InlineData(EngineeredWood.Orc.Proto.CompressionKind.Zstd, 200)]
    public void CompressDecompress_RoundTrip(EngineeredWood.Orc.Proto.CompressionKind kind, int blockSize)
    {
        // Create test data that's larger than the block size
        var original = new byte[1024];
        var rng = new Random(42);
        rng.NextBytes(original);

        var compressed = OrcCompression.Compress(kind, original, blockSize);
        var decompressed = OrcCompression.Decompress(kind, compressed, blockSize);

        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void CompressDecompress_None_PassThrough()
    {
        var original = new byte[] { 1, 2, 3, 4, 5 };
        var compressed = OrcCompression.Compress(EngineeredWood.Orc.Proto.CompressionKind.None, original, 256);
        Assert.Equal(original, compressed);

        var decompressed = OrcCompression.Decompress(EngineeredWood.Orc.Proto.CompressionKind.None, compressed, 256);
        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void CompressDecompress_SmallBlockSize_MultipleBlocks()
    {
        // Force multiple compression blocks with a tiny block size
        var original = new byte[500];
        for (int i = 0; i < 500; i++)
            original[i] = (byte)(i % 256);

        var compressed = OrcCompression.Compress(EngineeredWood.Orc.Proto.CompressionKind.Zlib, original, 50);

        // Should have multiple blocks (500 / 50 = 10 blocks)
        // Each block has a 3-byte header, so compressed data should have at least 10 * 3 = 30 header bytes
        Assert.True(compressed.Length >= 30, "Expected multiple compression block headers");

        var decompressed = OrcCompression.Decompress(EngineeredWood.Orc.Proto.CompressionKind.Zlib, compressed, 50);
        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void CompressDecompress_EmptyInput()
    {
        var empty = System.Array.Empty<byte>();
        var compressed = OrcCompression.Compress(EngineeredWood.Orc.Proto.CompressionKind.Zlib, empty, 256);
        var decompressed = OrcCompression.Decompress(EngineeredWood.Orc.Proto.CompressionKind.Zlib, compressed, 256);
        Assert.Empty(decompressed);
    }

    [Fact]
    public void Decompress_TruncatedHeader_Throws()
    {
        // Less than 3 bytes — can't read a block header
        var data = new byte[] { 0x01, 0x02 };
        Assert.Throws<InvalidDataException>(() =>
            OrcCompression.Decompress(EngineeredWood.Orc.Proto.CompressionKind.Zlib, data, 256));
    }

    [Fact]
    public void Decompress_ChunkExceedsInput_Throws()
    {
        // Header claims 100 bytes of data, but only 2 are present
        // header: isOriginal=1, length=100 → (100<<1)|1 = 201
        int header = (100 << 1) | 1;
        var data = new byte[]
        {
            (byte)(header & 0xFF), (byte)((header >> 8) & 0xFF), (byte)((header >> 16) & 0xFF),
            0x00, 0x00 // only 2 bytes instead of 100
        };
        Assert.Throws<InvalidDataException>(() =>
            OrcCompression.Decompress(EngineeredWood.Orc.Proto.CompressionKind.Zlib, data, 256));
    }

    [Fact]
    public void Decompress_OriginalBlock_PassesThrough()
    {
        // An "original" (uncompressed) block: isOriginal=1, length=5
        var payload = new byte[] { 10, 20, 30, 40, 50 };
        int header = (5 << 1) | 1;
        var data = new byte[3 + 5];
        data[0] = (byte)(header & 0xFF);
        data[1] = (byte)((header >> 8) & 0xFF);
        data[2] = (byte)((header >> 16) & 0xFF);
        payload.CopyTo(data, 3);

        var result = OrcCompression.Decompress(EngineeredWood.Orc.Proto.CompressionKind.Zlib, data, 256);
        Assert.Equal(payload, result);
    }

    [Fact]
    public void Decompress_MultipleBlocks_Concatenated()
    {
        // Compress with block size 10, input is 25 bytes → 3 blocks
        var original = new byte[25];
        for (int i = 0; i < 25; i++)
            original[i] = (byte)i;

        var compressed = OrcCompression.Compress(EngineeredWood.Orc.Proto.CompressionKind.Zlib, original, 10);
        var decompressed = OrcCompression.Decompress(EngineeredWood.Orc.Proto.CompressionKind.Zlib, compressed, 10);
        Assert.Equal(original, decompressed);
    }
}
