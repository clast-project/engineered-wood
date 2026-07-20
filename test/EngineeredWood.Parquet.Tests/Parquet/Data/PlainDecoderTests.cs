// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class PlainDecoderTests
{
    [Fact]
    public void DecodeBooleans_BitPacked()
    {
        // 0b00001011 → true, true, false, true, false, false, false, false
        byte[] data = [0x0B];
        var result = new bool[4];
        PlainDecoder.DecodeBooleans(data, result, 4);
        Assert.Equal([true, true, false, true], result);
    }

    [Fact]
    public void DecodeInt32s_LittleEndian()
    {
        var data = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(0), 42);
        BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(4), -1);
        var result = new int[2];
        PlainDecoder.DecodeInt32s(data, result, 2);
        Assert.Equal(42, result[0]);
        Assert.Equal(-1, result[1]);
    }

    [Fact]
    public void DecodeInt64s_LittleEndian()
    {
        var data = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(data.AsSpan(0), long.MaxValue);
        BinaryPrimitives.WriteInt64LittleEndian(data.AsSpan(8), -42L);
        var result = new long[2];
        PlainDecoder.DecodeInt64s(data, result, 2);
        Assert.Equal(long.MaxValue, result[0]);
        Assert.Equal(-42L, result[1]);
    }

    [Fact]
    public void DecodeFloats()
    {
        var data = new byte[8];
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteSingleLittleEndian(data.AsSpan(0), 3.14f);
        BinaryPrimitives.WriteSingleLittleEndian(data.AsSpan(4), -0.5f);
#else
        Buffer.BlockCopy(BitConverter.GetBytes(3.14f), 0, data, 0, 4);
        Buffer.BlockCopy(BitConverter.GetBytes(-0.5f), 0, data, 4, 4);
#endif
        var result = new float[2];
        PlainDecoder.DecodeFloats(data, result, 2);
        Assert.Equal(3.14f, result[0]);
        Assert.Equal(-0.5f, result[1]);
    }

    [Fact]
    public void DecodeDoubles()
    {
        var data = new byte[16];
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteDoubleLittleEndian(data.AsSpan(0), 2.718);
        BinaryPrimitives.WriteDoubleLittleEndian(data.AsSpan(8), -1.0);
#else
        Buffer.BlockCopy(BitConverter.GetBytes(2.718), 0, data, 0, 8);
        Buffer.BlockCopy(BitConverter.GetBytes(-1.0), 0, data, 8, 8);
#endif
        var result = new double[2];
        PlainDecoder.DecodeDoubles(data, result, 2);
        Assert.Equal(2.718, result[0]);
        Assert.Equal(-1.0, result[1]);
    }

    [Fact]
    public void DecodeByteArrays_LengthPrefixed()
    {
        // Two byte arrays: "hi" (2 bytes) and "world" (5 bytes)
        var data = new byte[4 + 2 + 4 + 5];
        BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(0), 2);
        data[4] = (byte)'h';
        data[5] = (byte)'i';
        BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(6), 5);
        "world"u8.CopyTo(data.AsSpan(10));

        var offsets = new int[3];
        int consumed = PlainDecoder.DecodeByteArrays(data, offsets, out byte[] values, 2);

        Assert.Equal(data.Length, consumed);
        Assert.Equal(0, offsets[0]);
        Assert.Equal(2, offsets[1]);
        Assert.Equal(7, offsets[2]);
        Assert.Equal("hi"u8.ToArray(), new ArraySegment<byte>(values, 0, 2).ToArray());
        Assert.Equal("world"u8.ToArray(), new ArraySegment<byte>(values, 2, 5).ToArray());
    }

    [Fact]
    public void DecodeFixedLenByteArrays()
    {
        byte[] data = [1, 2, 3, 4, 5, 6];
        var result = new byte[6];
        PlainDecoder.DecodeFixedLenByteArrays(data, result, 2, 3);
        Assert.Equal([1, 2, 3, 4, 5, 6], result);
    }
}
