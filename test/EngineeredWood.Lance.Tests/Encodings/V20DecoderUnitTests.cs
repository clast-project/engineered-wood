// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Encodings;
using EngineeredWood.Lance.Encodings.V20;
using EngineeredWood.Lance.Proto.Encodings.V20;
using Google.Protobuf;

namespace EngineeredWood.Lance.Tests.Encodings;

/// <summary>
/// Unit tests for v2.0 decoders that target code paths the pylance
/// cross-validation tests don't exercise (<see cref="ConstantDecoder"/> and
/// <see cref="FixedSizeBinaryDecoder"/>).
/// </summary>
public class V20DecoderUnitTests
{
    [Fact]
    public void Constant_Int32()
    {
        var encoding = new Constant { Value = ByteString.CopyFrom(BitConverter.GetBytes(42)) };
        var decoder = new ConstantDecoder(encoding);
        var ctx = new PageContext(System.Array.Empty<ReadOnlyMemory<byte>>());

        var arr = (Int32Array)decoder.Decode(5, Int32Type.Default, ctx);
        Assert.Equal(5, arr.Length);
        Assert.Equal(0, arr.NullCount);
        for (int i = 0; i < 5; i++)
            Assert.Equal(42, arr.GetValue(i));
    }

    [Fact]
    public void Constant_Int64_NegativeValue()
    {
        var encoding = new Constant { Value = ByteString.CopyFrom(BitConverter.GetBytes(-7L)) };
        var decoder = new ConstantDecoder(encoding);
        var ctx = new PageContext(System.Array.Empty<ReadOnlyMemory<byte>>());

        var arr = (Int64Array)decoder.Decode(3, Int64Type.Default, ctx);
        Assert.Equal(new long?[] { -7, -7, -7 }, arr.ToArray());
    }

    [Fact]
    public void Constant_Bool_True()
    {
        var encoding = new Constant { Value = ByteString.CopyFrom(new byte[] { 1 }) };
        var decoder = new ConstantDecoder(encoding);
        var ctx = new PageContext(System.Array.Empty<ReadOnlyMemory<byte>>());

        var arr = (BooleanArray)decoder.Decode(10, BooleanType.Default, ctx);
        Assert.Equal(10, arr.Length);
        for (int i = 0; i < 10; i++)
            Assert.True(arr.GetValue(i));
    }

    [Fact]
    public void Constant_Bool_False()
    {
        var encoding = new Constant { Value = ByteString.CopyFrom(new byte[] { 0 }) };
        var decoder = new ConstantDecoder(encoding);
        var ctx = new PageContext(System.Array.Empty<ReadOnlyMemory<byte>>());

        var arr = (BooleanArray)decoder.Decode(10, BooleanType.Default, ctx);
        for (int i = 0; i < 10; i++)
            Assert.False(arr.GetValue(i));
    }

    [Fact]
    public void Constant_MismatchedWidth_Throws()
    {
        var encoding = new Constant { Value = ByteString.CopyFrom(new byte[] { 1, 2, 3 }) };
        var decoder = new ConstantDecoder(encoding);
        var ctx = new PageContext(System.Array.Empty<ReadOnlyMemory<byte>>());

        Assert.Throws<LanceFormatException>(
            () => decoder.Decode(1, Int32Type.Default, ctx));
    }

    [Fact]
    public void FixedSizeBinary_RoundTripsThroughFlatInner()
    {
        // Payload: 3 × 4 bytes = 12 bytes.
        var payload = "AAAABBBBCCCC"u8.ToArray();
        var pageBuffers = new[] { (ReadOnlyMemory<byte>)payload };
        var ctx = new PageContext(pageBuffers);

        var encoding = new FixedSizeBinary
        {
            ByteWidth = 4,
            Bytes = new ArrayEncoding
            {
                Flat = new Flat
                {
                    BitsPerValue = 8,
                    Buffer = new Proto.Encodings.V20.Buffer
                    {
                        BufferIndex = 0,
                        BufferType = Proto.Encodings.V20.Buffer.Types.BufferType.Page,
                    },
                },
            },
        };

        var decoder = new FixedSizeBinaryDecoder(encoding);
        var arr = (FixedSizeBinaryArray)decoder.Decode(3, new FixedSizeBinaryType(4), ctx);
        Assert.Equal(3, arr.Length);
        Assert.Equal("AAAA"u8.ToArray(), arr.GetBytes(0).ToArray());
        Assert.Equal("BBBB"u8.ToArray(), arr.GetBytes(1).ToArray());
        Assert.Equal("CCCC"u8.ToArray(), arr.GetBytes(2).ToArray());
    }

    [Fact]
    public void FixedSizeBinary_WidthMismatch_Throws()
    {
        var encoding = new FixedSizeBinary
        {
            ByteWidth = 4,
            Bytes = new ArrayEncoding { Flat = new Flat { BitsPerValue = 8 } },
        };
        var decoder = new FixedSizeBinaryDecoder(encoding);

        Assert.Throws<LanceFormatException>(
            () => decoder.Decode(1, new FixedSizeBinaryType(8),
                new PageContext(System.Array.Empty<ReadOnlyMemory<byte>>())));
    }
}
