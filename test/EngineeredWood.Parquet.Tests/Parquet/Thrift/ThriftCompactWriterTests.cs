// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Tests.Parquet.Thrift;

public class ThriftCompactWriterTests
{
    [Fact]
    public void WriteByte_SingleByte()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteByte(0x42);
        Assert.Equal([0x42], writer.ToArray());
    }

    [Fact]
    public void WriteVarint_SingleByte()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteVarint(42);
        Assert.Equal([0x2A], writer.ToArray());
    }

    [Fact]
    public void WriteVarint_MultiByte()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteVarint(300);
        Assert.Equal([0xAC, 0x02], writer.ToArray());
    }

    [Fact]
    public void WriteVarint_LargeValue()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteVarint(624485);
        Assert.Equal([0xE5, 0x8E, 0x26], writer.ToArray());
    }

    [Theory]
    [InlineData(0, new byte[] { 0x00 })]
    [InlineData(-1, new byte[] { 0x01 })]
    [InlineData(1, new byte[] { 0x02 })]
    [InlineData(-2, new byte[] { 0x03 })]
    [InlineData(2147483647, new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0x0F })]
    [InlineData(-2147483648, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x0F })]
    public void WriteZigZagInt32_RoundTrips(int value, byte[] expectedBytes)
    {
        var writer = new ThriftCompactWriter();
        writer.WriteZigZagInt32(value);
        Assert.Equal(expectedBytes, writer.ToArray());

        // Verify round-trip with reader
        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(value, reader.ReadZigZagInt32());
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(1L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void WriteZigZagInt64_RoundTrips(long value)
    {
        var writer = new ThriftCompactWriter();
        writer.WriteZigZagInt64(value);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(value, reader.ReadZigZagInt64());
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)-1)]
    [InlineData((short)1)]
    [InlineData(short.MaxValue)]
    [InlineData(short.MinValue)]
    public void WriteI16_RoundTrips(short value)
    {
        var writer = new ThriftCompactWriter();
        writer.WriteI16(value);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(value, reader.ReadI16());
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(-1.5)]
    [InlineData(3.14159265358979)]
    [InlineData(double.MaxValue)]
    [InlineData(double.MinValue)]
    public void WriteDouble_RoundTrips(double value)
    {
        var writer = new ThriftCompactWriter();
        writer.WriteDouble(value);
        Assert.Equal(8, writer.Length);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(value, reader.ReadDouble());
    }

    [Fact]
    public void WriteBinary_RoundTrips()
    {
        byte[] data = [0x01, 0x02, 0x03, 0x04, 0x05];
        var writer = new ThriftCompactWriter();
        writer.WriteBinary(data);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(data, reader.ReadBinary().ToArray());
    }

    [Fact]
    public void WriteBinary_Empty()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteBinary(ReadOnlySpan<byte>.Empty);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Empty(reader.ReadBinary().ToArray());
    }

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("Hello, 世界! 🌍")]
    public void WriteString_RoundTrips(string value)
    {
        var writer = new ThriftCompactWriter();
        writer.WriteString(value);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(value, reader.ReadString());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void WriteBool_RoundTrips(bool value)
    {
        var writer = new ThriftCompactWriter();
        writer.WriteBool(value);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        Assert.Equal(value, reader.ReadBool());
    }

    [Fact]
    public void WriteFieldHeader_ShortForm_DeltaEncoded()
    {
        var writer = new ThriftCompactWriter();
        // Field 1 (delta = 1), type I32 (5)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        // Expected: high nibble = delta (1), low nibble = type (5) → 0x15
        Assert.Equal([0x15], writer.ToArray());
    }

    [Fact]
    public void WriteFieldHeader_ConsecutiveFields_DeltaEncoded()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(42);
        writer.WriteFieldHeader(ThriftType.Binary, 2);
        writer.WriteString("test");

        // Both fields should be delta-encoded (short form)
        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (type1, fid1) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.I32, type1);
        Assert.Equal(1, fid1);
        Assert.Equal(42, reader.ReadZigZagInt32());

        var (type2, fid2) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Binary, type2);
        Assert.Equal(2, fid2);
        Assert.Equal("test", reader.ReadString());
    }

    [Fact]
    public void WriteFieldHeader_LargeGap_LongForm()
    {
        var writer = new ThriftCompactWriter();
        // Field 20 from 0 → delta = 20, which is > 15, so long form
        writer.WriteFieldHeader(ThriftType.I32, 20);
        writer.WriteZigZagInt32(99);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (type, fid) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.I32, type);
        Assert.Equal(20, fid);
        Assert.Equal(99, reader.ReadZigZagInt32());
    }

    [Fact]
    public void WriteBoolField_EncodesInTypeNibble()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteBoolField(1, true);
        writer.WriteBoolField(2, false);

        var reader = new ThriftCompactReader(writer.WrittenSpan);

        var (type1, fid1) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.BooleanTrue, type1);
        Assert.Equal(1, fid1);
        Assert.True(reader.ReadBool());

        var (type2, fid2) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.BooleanFalse, type2);
        Assert.Equal(2, fid2);
        Assert.False(reader.ReadBool());
    }

    [Fact]
    public void WriteListHeader_SmallList()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteListHeader(ThriftType.I32, 3);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (elemType, count) = reader.ReadListHeader();
        Assert.Equal(ThriftType.I32, elemType);
        Assert.Equal(3, count);
    }

    [Fact]
    public void WriteListHeader_LargeList()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteListHeader(ThriftType.Binary, 100);

        var reader = new ThriftCompactReader(writer.WrittenSpan);
        var (elemType, count) = reader.ReadListHeader();
        Assert.Equal(ThriftType.Binary, elemType);
        Assert.Equal(100, count);
    }

    [Fact]
    public void WriteStructStop_WritesZero()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteStructStop();
        Assert.Equal([0x00], writer.ToArray());
    }

    [Fact]
    public void PushPopStruct_ResetsFieldIdContext()
    {
        var writer = new ThriftCompactWriter();

        // Outer struct: field 3
        writer.WriteFieldHeader(ThriftType.I32, 3);
        writer.WriteZigZagInt32(1);

        // Enter nested struct at field 4
        writer.WriteFieldHeader(ThriftType.Struct, 4);
        writer.PushStruct();

        // Inner struct: field 1 (delta from 0)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(2);
        writer.WriteStructStop();
        writer.PopStruct();

        // Back to outer: field 5 (delta from 4)
        writer.WriteFieldHeader(ThriftType.I32, 5);
        writer.WriteZigZagInt32(3);

        // Verify via reader
        var reader = new ThriftCompactReader(writer.WrittenSpan);

        var (t1, f1) = reader.ReadFieldHeader();
        Assert.Equal(3, f1);
        Assert.Equal(1, reader.ReadZigZagInt32());

        var (t2, f2) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Struct, t2);
        Assert.Equal(4, f2);

        reader.PushStruct();
        var (t3, f3) = reader.ReadFieldHeader();
        Assert.Equal(1, f3);
        Assert.Equal(2, reader.ReadZigZagInt32());
        var (t4, _) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Stop, t4);
        reader.PopStruct();

        var (t5, f5) = reader.ReadFieldHeader();
        Assert.Equal(5, f5);
        Assert.Equal(3, reader.ReadZigZagInt32());
    }

    [Fact]
    public void Reset_ClearsState()
    {
        var writer = new ThriftCompactWriter();
        writer.WriteZigZagInt32(42);
        Assert.True(writer.Length > 0);

        writer.Reset();
        Assert.Equal(0, writer.Length);
    }

    [Fact]
    public void EnsureCapacity_GrowsBuffer()
    {
        var writer = new ThriftCompactWriter(initialCapacity: 4);
        // Write more than 4 bytes to force growth
        for (int i = 0; i < 100; i++)
            writer.WriteByte((byte)(i & 0xFF));

        Assert.Equal(100, writer.Length);
        for (int i = 0; i < 100; i++)
            Assert.Equal((byte)(i & 0xFF), writer.WrittenSpan[i]);
    }
}
