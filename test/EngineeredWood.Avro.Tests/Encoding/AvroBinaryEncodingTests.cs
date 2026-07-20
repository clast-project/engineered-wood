// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Avro.Encoding;
using EngineeredWood.Buffers;

namespace EngineeredWood.Avro.Tests.Encoding;

public class AvroBinaryEncodingTests
{
    [Fact]
    public void RoundTrip_Boolean()
    {
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteBoolean(true);
        writer.WriteBoolean(false);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.True(reader.ReadBoolean());
        Assert.False(reader.ReadBoolean());
        Assert.True(reader.IsEmpty);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(42)]
    [InlineData(-42)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void RoundTrip_Int(int value)
    {
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteInt(value);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.Equal(value, reader.ReadInt());
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    [InlineData(1234567890123L)]
    public void RoundTrip_Long(long value)
    {
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteLong(value);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.Equal(value, reader.ReadLong());
    }

    [Theory]
    [InlineData(0f)]
    [InlineData(1.5f)]
    [InlineData(-3.14f)]
    [InlineData(float.MaxValue)]
    [InlineData(float.MinValue)]
    [InlineData(float.NaN)]
    [InlineData(float.PositiveInfinity)]
    public void RoundTrip_Float(float value)
    {
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteFloat(value);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.Equal(value, reader.ReadFloat());
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(1.5)]
    [InlineData(-3.14159265358979)]
    [InlineData(double.MaxValue)]
    [InlineData(double.MinValue)]
    public void RoundTrip_Double(double value)
    {
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteDouble(value);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.Equal(value, reader.ReadDouble());
    }

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("hello world 🌍")]
    public void RoundTrip_String(string value)
    {
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteString(value);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.Equal(value, reader.ReadString());
    }

    [Fact]
    public void RoundTrip_Bytes()
    {
        byte[] value = [0, 1, 2, 255, 128];

        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteBytes(value);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.Equal(value, reader.ReadBytes().ToArray());
    }

    [Fact]
    public void RoundTrip_EmptyBytes()
    {
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteBytes([]);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.Empty(reader.ReadBytes().ToArray());
    }

    [Fact]
    public void RoundTrip_MultipleValues()
    {
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);

        writer.WriteInt(42);
        writer.WriteString("test");
        writer.WriteDouble(3.14);
        writer.WriteBoolean(true);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.Equal(42, reader.ReadInt());
        Assert.Equal("test", reader.ReadString());
        Assert.Equal(3.14, reader.ReadDouble());
        Assert.True(reader.ReadBoolean());
        Assert.True(reader.IsEmpty);
    }

    [Fact]
    public void RoundTrip_UnionIndex()
    {
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteUnionIndex(0);
        writer.WriteUnionIndex(1);

        var reader = new AvroBinaryReader(buf.WrittenSpan);
        Assert.Equal(0, reader.ReadUnionIndex());
        Assert.Equal(1, reader.ReadUnionIndex());
    }

    [Fact]
    public void Int_ZigzagEncoding_MatchesAvroSpec()
    {
        // Avro spec: int 0 encodes as 0x00
        var buf = new GrowableBuffer();
        var writer = new AvroBinaryWriter(buf);
        writer.WriteInt(0);
        Assert.Equal(new byte[] { 0x00 }, buf.WrittenSpan.ToArray());

        buf.Reset();
        writer.WriteInt(-1);
        Assert.Equal(new byte[] { 0x01 }, buf.WrittenSpan.ToArray());

        buf.Reset();
        writer.WriteInt(1);
        Assert.Equal(new byte[] { 0x02 }, buf.WrittenSpan.ToArray());

        buf.Reset();
        writer.WriteInt(-2);
        Assert.Equal(new byte[] { 0x03 }, buf.WrittenSpan.ToArray());

        buf.Reset();
        writer.WriteInt(2);
        Assert.Equal(new byte[] { 0x04 }, buf.WrittenSpan.ToArray());
    }
}
