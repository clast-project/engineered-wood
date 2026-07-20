// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Writer.Encodings;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Encodes a primitive array → parses the resulting segment with the reader's
/// <see cref="SerializedArray.Parse"/>, and verifies the FlatBuffer + buffer
/// payload look exactly as the decoder expects.
/// </summary>
public class PrimitiveArrayEncoderTests
{
    [Fact]
    public void Int32_Encode_ParsesAsExpected()
    {
        var arr = new Int32Array.Builder().AppendRange(new[] { 1, 2, -3, 1_000_000 }).Build();
        var segment = PrimitiveArrayEncoder.Encode(arr, primitiveEncodingIdx: 5, boolEncodingIdx: 99);

        var serialized = SerializedArray.Parse(segment);
        var msg = serialized.Message;
        Assert.Equal(1, msg.BufferCount);
        var bufDesc = msg.Buffer(0);
        Assert.Equal(0, bufDesc.Padding);
        Assert.Equal(2, bufDesc.AlignmentExponent);
        Assert.Equal(BufferCompression.None, bufDesc.Compression);
        Assert.Equal(16u, bufDesc.Length);

        var node = msg.Root;
        Assert.Equal((ushort)5, node.EncodingIndex);
        Assert.Equal(0, node.ChildCount);
        Assert.Equal(1, node.BufferRefCount);
        Assert.Equal((ushort)0, node.BufferRef(0));

        var bytes = serialized.BufferBytes(0);
        Assert.Equal(16, bytes.Length);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(0, 4)));
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(4, 4)));
        Assert.Equal(-3, BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(8, 4)));
        Assert.Equal(1_000_000, BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(12, 4)));
    }

    [Fact]
    public void Double_Encode_ParsesAsExpected()
    {
        var arr = new DoubleArray.Builder().AppendRange(new[] { 3.14, 2.71, -1.0 }).Build();
        var segment = PrimitiveArrayEncoder.Encode(arr, primitiveEncodingIdx: 11, boolEncodingIdx: 99);

        var serialized = SerializedArray.Parse(segment);
        var bufDesc = serialized.Message.Buffer(0);
        Assert.Equal(3, bufDesc.AlignmentExponent);
        Assert.Equal(24u, bufDesc.Length);

        var bytes = serialized.BufferBytes(0);
        Assert.Equal(3.14, BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(0, 8))));
        Assert.Equal(2.71, BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(8, 8))));
        Assert.Equal(-1.0, BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(16, 8))));
    }
}
