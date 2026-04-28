// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Encodings;
using EngineeredWood.Lance.Encodings.V20;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Tests.Encodings;

public class DictionaryDecoderTests
{
    [Fact]
    public void Int32_Dictionary_Materializes()
    {
        // Lance 1-based indices: 0 = null, N ≥ 1 = items[N-1].
        // indices = [1, 2, 1, 2, 3] → items[0], items[1], items[0], items[1], items[2]
        // items   = [10, 20, 30]
        byte[] indicesBuf = { 1, 2, 1, 2, 3 };

        byte[] itemsBuf = new byte[3 * 4];
        BinaryPrimitives.WriteInt32LittleEndian(itemsBuf.AsSpan(0, 4), 10);
        BinaryPrimitives.WriteInt32LittleEndian(itemsBuf.AsSpan(4, 4), 20);
        BinaryPrimitives.WriteInt32LittleEndian(itemsBuf.AsSpan(8, 4), 30);

        var encoding = new Dictionary
        {
            Indices = FlatInPage(0, bitsPerValue: 8),
            Items = FlatInPage(1, bitsPerValue: 32),
            NumDictionaryItems = 3,
        };
        var decoder = new DictionaryDecoder(encoding);
        var ctx = new PageContext(new[]
        {
            (ReadOnlyMemory<byte>)indicesBuf,
            (ReadOnlyMemory<byte>)itemsBuf,
        });

        var arr = (Int32Array)decoder.Decode(5, Int32Type.Default, ctx);
        Assert.Equal(new int?[] { 10, 20, 10, 20, 30 }, arr.ToArray());
    }

    [Fact]
    public void Int32_Dictionary_WithNulls()
    {
        // indices = [1, 0, 2, 0, 1] → [items[0], null, items[1], null, items[0]]
        byte[] indicesBuf = { 1, 0, 2, 0, 1 };
        byte[] itemsBuf = new byte[2 * 4];
        BinaryPrimitives.WriteInt32LittleEndian(itemsBuf.AsSpan(0, 4), 42);
        BinaryPrimitives.WriteInt32LittleEndian(itemsBuf.AsSpan(4, 4), 99);

        var encoding = new Dictionary
        {
            Indices = FlatInPage(0, bitsPerValue: 8),
            Items = FlatInPage(1, bitsPerValue: 32),
            NumDictionaryItems = 2,
        };
        var decoder = new DictionaryDecoder(encoding);
        var ctx = new PageContext(new[]
        {
            (ReadOnlyMemory<byte>)indicesBuf,
            (ReadOnlyMemory<byte>)itemsBuf,
        });

        var arr = (Int32Array)decoder.Decode(5, Int32Type.Default, ctx);
        Assert.Equal(2, arr.NullCount);
        Assert.Equal(42, arr.GetValue(0));
        Assert.Null(arr.GetValue(1));
        Assert.Equal(99, arr.GetValue(2));
        Assert.Null(arr.GetValue(3));
        Assert.Equal(42, arr.GetValue(4));
    }

    [Fact]
    public void Boolean_Dictionary_Materializes()
    {
        // 1-based: indices = [1, 2, 1, 1, 2] → items[0], items[1], items[0], items[0], items[1]
        // items   = [true, false] packed as a 2-bit bitmap (0b01)
        byte[] indicesBuf = { 1, 2, 1, 1, 2 };
        byte[] itemsBuf = { 0b01 }; // bit 0 = true, bit 1 = false

        var encoding = new Dictionary
        {
            Indices = FlatInPage(0, bitsPerValue: 8),
            Items = FlatInPage(1, bitsPerValue: 1),
            NumDictionaryItems = 2,
        };
        var decoder = new DictionaryDecoder(encoding);
        var ctx = new PageContext(new[]
        {
            (ReadOnlyMemory<byte>)indicesBuf,
            (ReadOnlyMemory<byte>)itemsBuf,
        });

        var arr = (BooleanArray)decoder.Decode(5, BooleanType.Default, ctx);
        Assert.Equal(5, arr.Length);
        Assert.True(arr.GetValue(0));
        Assert.False(arr.GetValue(1));
        Assert.True(arr.GetValue(2));
        Assert.True(arr.GetValue(3));
        Assert.False(arr.GetValue(4));
    }

    [Fact]
    public void String_Dictionary_Materializes()
    {
        // indices = [1, 2, 1] → ["foo", "bar", "foo"]
        // items Binary: indices=[3, 6], bytes="foobar", null_adjustment=7
        byte[] indicesBuf = { 1, 2, 1 };

        byte[] itemOffsetsBuf = new byte[2 * 8];
        BinaryPrimitives.WriteUInt64LittleEndian(itemOffsetsBuf.AsSpan(0, 8), 3);
        BinaryPrimitives.WriteUInt64LittleEndian(itemOffsetsBuf.AsSpan(8, 8), 6);
        byte[] itemBytesBuf = System.Text.Encoding.UTF8.GetBytes("foobar");

        var itemsEncoding = new ArrayEncoding
        {
            Binary = new Binary
            {
                Indices = new ArrayEncoding
                {
                    Nullable = new Proto.Encodings.V20.Nullable
                    {
                        NoNulls = new Proto.Encodings.V20.Nullable.Types.NoNull
                        {
                            Values = FlatInPage(1, bitsPerValue: 64),
                        },
                    },
                },
                Bytes = FlatInPage(2, bitsPerValue: 8),
                NullAdjustment = 7,
            },
        };

        var encoding = new Dictionary
        {
            Indices = FlatInPage(0, bitsPerValue: 8),
            Items = itemsEncoding,
            NumDictionaryItems = 2,
        };
        var decoder = new DictionaryDecoder(encoding);
        var ctx = new PageContext(new[]
        {
            (ReadOnlyMemory<byte>)indicesBuf,
            (ReadOnlyMemory<byte>)itemOffsetsBuf,
            (ReadOnlyMemory<byte>)itemBytesBuf,
        });

        var arr = (StringArray)decoder.Decode(3, StringType.Default, ctx);
        Assert.Equal("foo", arr.GetString(0));
        Assert.Equal("bar", arr.GetString(1));
        Assert.Equal("foo", arr.GetString(2));
    }

    [Fact]
    public void OutOfRange_Index_Throws()
    {
        // indices = [1, 5] but items only has 3 entries (valid 1-based indices are 1..3)
        byte[] indicesBuf = { 1, 5 };
        byte[] itemsBuf = new byte[3 * 4];

        var encoding = new Dictionary
        {
            Indices = FlatInPage(0, bitsPerValue: 8),
            Items = FlatInPage(1, bitsPerValue: 32),
            NumDictionaryItems = 3,
        };
        var decoder = new DictionaryDecoder(encoding);
        var ctx = new PageContext(new[]
        {
            (ReadOnlyMemory<byte>)indicesBuf,
            (ReadOnlyMemory<byte>)itemsBuf,
        });

        Assert.Throws<LanceFormatException>(
            () => decoder.Decode(2, Int32Type.Default, ctx));
    }

    [Fact]
    public void Zero_Dict_Items_With_Rows_Throws()
    {
        var encoding = new Dictionary
        {
            Indices = FlatInPage(0, bitsPerValue: 8),
            Items = FlatInPage(1, bitsPerValue: 32),
            NumDictionaryItems = 0,
        };
        var decoder = new DictionaryDecoder(encoding);
        var ctx = new PageContext(new[]
        {
            (ReadOnlyMemory<byte>)new byte[1],
            (ReadOnlyMemory<byte>)new byte[0],
        });

        Assert.Throws<LanceFormatException>(
            () => decoder.Decode(1, Int32Type.Default, ctx));
    }

    private static ArrayEncoding FlatInPage(uint pageBufferIndex, int bitsPerValue) =>
        new ArrayEncoding
        {
            Flat = new Flat
            {
                BitsPerValue = (ulong)bitsPerValue,
                Buffer = new Proto.Encodings.V20.Buffer
                {
                    BufferIndex = pageBufferIndex,
                    BufferType = Proto.Encodings.V20.Buffer.Types.BufferType.Page,
                },
            },
        };
}
