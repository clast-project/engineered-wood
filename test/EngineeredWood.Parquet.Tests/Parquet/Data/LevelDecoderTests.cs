// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class LevelDecoderTests
{
    [Fact]
    public void DecodeV1_MaxLevelZero_ReturnsZeros()
    {
        byte[] data = [0xFF]; // irrelevant
        var levels = new byte[4];
        int consumed = LevelDecoder.DecodeV1(data, maxLevel: 0, valueCount: 4, levels, out int matchCount);
        Assert.Equal(0, consumed);
        Assert.Equal([0, 0, 0, 0], levels);
        Assert.Equal(4, matchCount); // all values "match" maxLevel 0 when non-nullable
    }

    [Fact]
    public void DecodeV1_RleEncoded()
    {
        // 4-byte length prefix + RLE data
        // RLE: header = (4 << 1) | 0 = 8, value = 1 (bit width 1, 1 byte)
        byte[] rleData = [8, 1];
        byte[] data = new byte[4 + rleData.Length];
#if NET8_0_OR_GREATER
        BitConverter.TryWriteBytes(data.AsSpan(0), rleData.Length);
#else
        Buffer.BlockCopy(BitConverter.GetBytes(rleData.Length), 0, data, 0, 4);
#endif
        rleData.CopyTo(data.AsSpan(4));

        var levels = new byte[4];
        int consumed = LevelDecoder.DecodeV1(data, maxLevel: 1, valueCount: 4, levels, out int matchCount);

        Assert.Equal(4 + rleData.Length, consumed);
        Assert.Equal([1, 1, 1, 1], levels);
        Assert.Equal(4, matchCount);
    }

    [Fact]
    public void DecodeV2_MaxLevelZero_ReturnsZeros()
    {
        byte[] data = [];
        var levels = new byte[3];
        LevelDecoder.DecodeV2(data, maxLevel: 0, valueCount: 3, levels, out int matchCount);
        Assert.Equal([0, 0, 0], levels);
        Assert.Equal(3, matchCount);
    }

    [Fact]
    public void DecodeV2_RleEncoded()
    {
        // Raw RLE: 3 copies of value 1, bitWidth=1
        // Header = (3 << 1) | 0 = 6, value = 1
        byte[] data = [6, 1];
        var levels = new byte[3];
        LevelDecoder.DecodeV2(data, maxLevel: 1, valueCount: 3, levels, out int matchCount);
        Assert.Equal([1, 1, 1], levels);
        Assert.Equal(3, matchCount);
    }

    [Fact]
    public void DecodeV1_BitPacked_1BitWidth()
    {
        // MSB-packed: values [1, 0, 1, 0, 1, 1, 0, 0] with bitWidth=1
        // Packed MSB-first: 10101100 = 0xAC
        byte[] data = [0xAC];
        var levels = new byte[8];
        int consumed = LevelDecoder.DecodeV1(data, maxLevel: 1, valueCount: 8, levels, out int matchCount,
            Encoding.BitPacked);
        Assert.Equal(1, consumed);
        Assert.Equal([1, 0, 1, 0, 1, 1, 0, 0], levels);
        Assert.Equal(4, matchCount); // four 1s
    }

    [Fact]
    public void DecodeV1_BitPacked_2BitWidth()
    {
        // MSB-packed: values [0, 1, 2, 3] with bitWidth=2
        // Bit stream: 00 01 10 11 → 00011011 = 0x1B
        byte[] data = [0x1B];
        var levels = new byte[4];
        int consumed = LevelDecoder.DecodeV1(data, maxLevel: 3, valueCount: 4, levels, out int matchCount,
            Encoding.BitPacked);
        Assert.Equal(1, consumed);
        Assert.Equal([0, 1, 2, 3], levels);
        Assert.Equal(1, matchCount); // one value == maxLevel (3)
    }

    [Fact]
    public void DecodeV1_BitPacked_3BitWidth_CrossesByteBoundary()
    {
        // MSB-packed: values [1, 2, 5] with bitWidth=3
        // Bit stream: 001 010 101 (pad) → 00101010 1(0000000) → 0x2A, 0x80
        byte[] data = [0x2A, 0x80];
        var levels = new byte[3];
        int consumed = LevelDecoder.DecodeV1(data, maxLevel: 7, valueCount: 3, levels, out int matchCount,
            Encoding.BitPacked);
        Assert.Equal(2, consumed); // ceil(3*3/8) = 2
        Assert.Equal([1, 2, 5], levels);
        Assert.Equal(0, matchCount); // none equal maxLevel (7)
    }

    [Fact]
    public void DecodeV1_BitPacked_MaxLevelZero_ReturnsZeros()
    {
        byte[] data = [0xFF]; // irrelevant — should not be read
        var levels = new byte[4];
        int consumed = LevelDecoder.DecodeV1(data, maxLevel: 0, valueCount: 4, levels, out int matchCount,
            Encoding.BitPacked);
        Assert.Equal(0, consumed);
        Assert.Equal([0, 0, 0, 0], levels);
        Assert.Equal(4, matchCount);
    }

    [Fact]
    public void GetBitWidth_ReturnsCorrectValues()
    {
        Assert.Equal(0, LevelDecoder.GetBitWidth(0));
        Assert.Equal(1, LevelDecoder.GetBitWidth(1));
        Assert.Equal(2, LevelDecoder.GetBitWidth(2));
        Assert.Equal(2, LevelDecoder.GetBitWidth(3));
        Assert.Equal(3, LevelDecoder.GetBitWidth(4));
        Assert.Equal(3, LevelDecoder.GetBitWidth(7));
        Assert.Equal(4, LevelDecoder.GetBitWidth(8));
    }
}
