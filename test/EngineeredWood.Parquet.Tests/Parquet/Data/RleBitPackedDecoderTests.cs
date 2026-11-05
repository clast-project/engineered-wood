// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class RleBitPackedDecoderTests
{
    [Fact]
    public void RleRun_DecodesRepeatedValue()
    {
        // RLE header: (3 << 1) | 0 = 6, meaning 3 repeated values
        // Value: 5 encoded in 1 byte (bit width = 3)
        byte[] data = [6, 5];
        var decoder = new RleBitPackedDecoder(data, bitWidth: 3);
        var result = new int[3];
        decoder.ReadBatch(result);
        Assert.Equal([5, 5, 5], result);
    }

    /// <summary>
    /// An RLE value is stored in whole bytes, so a byte can hold more than the declared width. Such a
    /// value is refused: a dictionary trusts an index's width to keep it inside its entries, and its
    /// rented arrays reach past them, so index 3 of a three-entry dictionary read whatever was left
    /// there (#688).
    /// </summary>
    [Theory]
    [InlineData(1, new byte[] { 6, 3 })]
    [InlineData(3, new byte[] { 6, 8 })]
    [InlineData(9, new byte[] { 6, 0x00, 0x02 })]
    [InlineData(31, new byte[] { 6, 0x00, 0x00, 0x00, 0x80 })]
    public void RleRun_ValueWiderThanItsBitWidth_IsRefused(int bitWidth, byte[] data)
    {
        var ex = Assert.Throws<ParquetFormatException>(() => new RleBitPackedDecoder(data, bitWidth).ReadBatch(new int[3]));
        Assert.Contains($"bit width of {bitWidth}", ex.Message);
    }

    [Theory]
    [InlineData(1, new byte[] { 6, 1 }, 1)]
    [InlineData(3, new byte[] { 6, 7 }, 7)]
    [InlineData(9, new byte[] { 6, 0xFF, 0x01 }, 511)]
    [InlineData(32, new byte[] { 6, 0xFF, 0xFF, 0xFF, 0xFF }, -1)]
    public void RleRun_WidestValueOfItsBitWidth_Decodes(int bitWidth, byte[] data, int expected)
    {
        var decoder = new RleBitPackedDecoder(data, bitWidth);
        var result = new int[3];
        decoder.ReadBatch(result);
        Assert.Equal([expected, expected, expected], result);
    }

    [Fact]
    public void BitPackedGroup_DecodesEightValues()
    {
        // Bit-packed header: (1 << 1) | 1 = 3, meaning 1 group of 8 values
        // Bit width = 1: values 1,0,1,0,1,0,1,0
        // Packed as: 0b01010101 = 0x55
        byte[] data = [3, 0x55];
        var decoder = new RleBitPackedDecoder(data, bitWidth: 1);
        var result = new int[8];
        decoder.ReadBatch(result);
        Assert.Equal([1, 0, 1, 0, 1, 0, 1, 0], result);
    }

    [Fact]
    public void MixedRleAndBitPacked()
    {
        // RLE: 2 copies of value 3 (bitWidth=2, so 1 byte value)
        // Header: (2 << 1) | 0 = 4
        // Then bit-packed: 1 group of 8, bitWidth=2, 2 bytes
        // Values: 0,1,2,3,0,1,2,3 = 0b11_10_01_00 0b11_10_01_00 = 0xE4 0xE4
        // Header: (1 << 1) | 1 = 3
        byte[] data = [4, 3, 3, 0xE4, 0xE4];
        var decoder = new RleBitPackedDecoder(data, bitWidth: 2);
        var result = new int[10];
        decoder.ReadBatch(result);
        Assert.Equal([3, 3, 0, 1, 2, 3, 0, 1, 2, 3], result);
    }

    [Fact]
    public void ZeroBitWidth_AllZeros()
    {
        // RLE header: (5 << 1) | 0 = 10, value is 0 bytes wide (byteWidth = 0)
        byte[] data = [10];
        var decoder = new RleBitPackedDecoder(data, bitWidth: 0);
        var result = new int[5];
        decoder.ReadBatch(result);
        Assert.Equal([0, 0, 0, 0, 0], result);
    }

    [Fact]
    public void ReadNext_ReturnsOneAtATime()
    {
        // RLE: 3 copies of value 7, bitWidth=3
        byte[] data = [6, 7];
        var decoder = new RleBitPackedDecoder(data, bitWidth: 3);
        Assert.Equal(7, decoder.ReadNext());
        Assert.Equal(7, decoder.ReadNext());
        Assert.Equal(7, decoder.ReadNext());
    }
}
