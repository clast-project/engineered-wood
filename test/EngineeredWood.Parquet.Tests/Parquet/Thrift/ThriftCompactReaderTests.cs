// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Tests.Parquet.Thrift;

public class ThriftCompactReaderTests
{
    [Fact]
    public void ReadByte_ReturnsSingleByte()
    {
        var reader = new ThriftCompactReader([0x42]);
        Assert.Equal(0x42, reader.ReadByte());
    }

    [Fact]
    public void ReadByte_ThrowsOnEmpty()
    {
        var reader = new ThriftCompactReader([]);
        try
        {
            reader.ReadByte();
            Assert.Fail("Expected ParquetFormatException");
        }
        catch (ParquetFormatException) { }
    }

    [Fact]
    public void ReadVarint_SingleByte()
    {
        // 0x2A = 42, high bit clear → single byte varint
        var reader = new ThriftCompactReader([0x2A]);
        Assert.Equal(42UL, reader.ReadVarint());
    }

    [Fact]
    public void ReadVarint_MultiByte()
    {
        // 300 = 0x12C → varint bytes: 0xAC 0x02
        var reader = new ThriftCompactReader([0xAC, 0x02]);
        Assert.Equal(300UL, reader.ReadVarint());
    }

    [Fact]
    public void ReadVarint_LargeValue()
    {
        // 624485 → varint: 0xE5 0x8E 0x26
        var reader = new ThriftCompactReader([0xE5, 0x8E, 0x26]);
        Assert.Equal(624485UL, reader.ReadVarint());
    }

    [Fact]
    public void ReadZigZagInt32_PositiveValues()
    {
        // zigzag(0) = 0 → varint 0x00
        var reader = new ThriftCompactReader([0x00]);
        Assert.Equal(0, reader.ReadZigZagInt32());
    }

    [Fact]
    public void ReadZigZagInt32_NegativeOne()
    {
        // zigzag(-1) = 1 → varint 0x01
        var reader = new ThriftCompactReader([0x01]);
        Assert.Equal(-1, reader.ReadZigZagInt32());
    }

    [Fact]
    public void ReadZigZagInt32_PositiveOne()
    {
        // zigzag(1) = 2 → varint 0x02
        var reader = new ThriftCompactReader([0x02]);
        Assert.Equal(1, reader.ReadZigZagInt32());
    }

    [Fact]
    public void ReadZigZagInt32_NegativeTwo()
    {
        // zigzag(-2) = 3 → varint 0x03
        var reader = new ThriftCompactReader([0x03]);
        Assert.Equal(-2, reader.ReadZigZagInt32());
    }

    [Fact]
    public void ReadZigZagInt64_LargePositive()
    {
        // zigzag(150) = 300 → varint 0xAC 0x02
        var reader = new ThriftCompactReader([0xAC, 0x02]);
        Assert.Equal(150L, reader.ReadZigZagInt64());
    }

    [Fact]
    public void ReadZigZagInt64_LargeNegative()
    {
        // zigzag(-150) = 299 → varint 0xAB 0x02
        var reader = new ThriftCompactReader([0xAB, 0x02]);
        Assert.Equal(-150L, reader.ReadZigZagInt64());
    }

    [Fact]
    public void ReadI16_Simple()
    {
        // zigzag(42) = 84 → varint 0x54
        var reader = new ThriftCompactReader([0x54]);
        Assert.Equal((short)42, reader.ReadI16());
    }

    [Fact]
    public void ReadDouble_LittleEndian()
    {
        // 3.14 as little-endian IEEE 754 double
        byte[] data = new byte[8];
#if NET8_0_OR_GREATER
        BitConverter.TryWriteBytes(data, 3.14);
#else
        Buffer.BlockCopy(BitConverter.GetBytes(3.14), 0, data, 0, 8);
#endif
        var reader = new ThriftCompactReader(data);
        Assert.Equal(3.14, reader.ReadDouble());
    }

    [Fact]
    public void ReadBinary_ReturnsCorrectSlice()
    {
        // Length 3 (varint 0x03) followed by bytes [0xAA, 0xBB, 0xCC]
        var reader = new ThriftCompactReader([0x03, 0xAA, 0xBB, 0xCC]);
        var result = reader.ReadBinary();
        Assert.Equal(3, result.Length);
        Assert.Equal(0xAA, result[0]);
        Assert.Equal(0xBB, result[1]);
        Assert.Equal(0xCC, result[2]);
    }

    [Fact]
    public void ReadString_Utf8()
    {
        // "Hi" = [0x48, 0x69], length 2
        var reader = new ThriftCompactReader([0x02, 0x48, 0x69]);
        Assert.Equal("Hi", reader.ReadString());
    }

    [Fact]
    public void ReadFieldHeader_ShortForm()
    {
        // Short form: high nibble = delta (1-15), low nibble = type
        // Delta 1, type I32 (5) → byte 0x15
        var reader = new ThriftCompactReader([0x15]);
        var (type, fieldId) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.I32, type);
        Assert.Equal(1, fieldId);
    }

    [Fact]
    public void ReadFieldHeader_ShortFormSequential()
    {
        // Field 1 (delta=1, I32), Field 2 (delta=1, Binary)
        var reader = new ThriftCompactReader([0x15, 0x18]);
        var (type1, id1) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.I32, type1);
        Assert.Equal(1, id1);

        var (type2, id2) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Binary, type2);
        Assert.Equal(2, id2);
    }

    [Fact]
    public void ReadFieldHeader_LongForm()
    {
        // Long form: delta=0, type in low nibble, then zigzag i16 field ID
        // Type I64 (6), field ID 100 → zigzag(100)=200 → varint 0xC8 0x01
        var reader = new ThriftCompactReader([0x06, 0xC8, 0x01]);
        var (type, fieldId) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.I64, type);
        Assert.Equal(100, fieldId);
    }

    [Fact]
    public void ReadFieldHeader_BooleanTrue()
    {
        // Delta 1, BooleanTrue (1) → 0x11
        var reader = new ThriftCompactReader([0x11]);
        var (type, fieldId) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.BooleanTrue, type);
        Assert.Equal(1, fieldId);
        Assert.True(reader.ReadBool());
    }

    [Fact]
    public void ReadFieldHeader_BooleanFalse()
    {
        // Delta 1, BooleanFalse (2) → 0x12
        var reader = new ThriftCompactReader([0x12]);
        var (type, fieldId) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.BooleanFalse, type);
        Assert.Equal(1, fieldId);
        Assert.False(reader.ReadBool());
    }

    [Fact]
    public void ReadFieldHeader_Stop()
    {
        var reader = new ThriftCompactReader([0x00]);
        var (type, _) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Stop, type);
    }

    [Fact]
    public void ReadListHeader_SmallList()
    {
        // Count 3, element type I32 (5) → high nibble=3, low nibble=5 → 0x35
        var reader = new ThriftCompactReader([0x35]);
        var (elemType, count) = reader.ReadListHeader();
        Assert.Equal(ThriftType.I32, elemType);
        Assert.Equal(3, count);
    }

    [Fact]
    public void ReadListHeader_LargeList()
    {
        // Count >= 15: high nibble = 0xF, low nibble = type, then varint count
        // 20 elements of Binary (8) → 0xF8, varint 0x14
        var reader = new ThriftCompactReader([0xF8, 0x14]);
        var (elemType, count) = reader.ReadListHeader();
        Assert.Equal(ThriftType.Binary, elemType);
        Assert.Equal(20, count);
    }

    [Fact]
    public void ReadMapHeader_EmptyMap()
    {
        // Count = 0 as varint → 0x00
        var reader = new ThriftCompactReader([0x00]);
        var (_, _, count) = reader.ReadMapHeader();
        Assert.Equal(0, count);
    }

    [Fact]
    public void ReadMapHeader_NonEmpty()
    {
        // Count = 2 (varint 0x02), key=Binary(8), value=I32(5) → types byte 0x85
        var reader = new ThriftCompactReader([0x02, 0x85]);
        var (keyType, valueType, count) = reader.ReadMapHeader();
        Assert.Equal(ThriftType.Binary, keyType);
        Assert.Equal(ThriftType.I32, valueType);
        Assert.Equal(2, count);
    }

    [Fact]
    public void PushPopStruct_RestoresFieldId()
    {
        // Simulate: read field 5 in outer struct, enter nested struct, read field 1, exit
        // Outer field 5: delta=5, I32 → 0x55
        // Nested struct begins (PushStruct), field 1: delta=1, Byte → 0x13, value byte, stop
        // Pop restores to field ID 5, next field 6: delta=1, I32 → 0x15
        var reader = new ThriftCompactReader([
            0x55, 0x00,     // field 5 I32, value zigzag(0)
            0x1C,           // field 6 Struct
            0x13, 0xFF,     // nested field 1 Byte, value 0xFF
            0x00,           // nested stop
            0x15, 0x00,     // field 7 I32, value zigzag(0)
        ]);

        // Read outer field 5
        var (t1, id1) = reader.ReadFieldHeader();
        Assert.Equal(5, id1);
        Assert.Equal(ThriftType.I32, t1);
        reader.ReadZigZagInt32();

        // Read outer field 6 (Struct)
        var (t2, id2) = reader.ReadFieldHeader();
        Assert.Equal(6, id2);
        Assert.Equal(ThriftType.Struct, t2);

        // Enter nested struct
        reader.PushStruct();

        // Read nested field 1
        var (t3, id3) = reader.ReadFieldHeader();
        Assert.Equal(1, id3);
        Assert.Equal(ThriftType.Byte, t3);
        reader.ReadByte();

        // Nested stop
        var (t4, _) = reader.ReadFieldHeader();
        Assert.Equal(ThriftType.Stop, t4);

        // Exit nested struct
        reader.PopStruct();

        // Read outer field 7 (delta 1 from field 6 = 7)
        var (t5, id5) = reader.ReadFieldHeader();
        Assert.Equal(7, id5);
        Assert.Equal(ThriftType.I32, t5);
    }

    [Fact]
    public void Skip_ScalarTypes()
    {
        // Byte: 1 byte
        var reader = new ThriftCompactReader([0x42]);
        reader.Skip(ThriftType.Byte);
        Assert.Equal(1, reader.Position);

        // I32: varint 0xAC 0x02 (2 bytes)
        reader = new ThriftCompactReader([0xAC, 0x02]);
        reader.Skip(ThriftType.I32);
        Assert.Equal(2, reader.Position);

        // Double: 8 bytes
        reader = new ThriftCompactReader(new byte[8]);
        reader.Skip(ThriftType.Double);
        Assert.Equal(8, reader.Position);

        // Binary: length 2 + 2 data bytes
        reader = new ThriftCompactReader([0x02, 0xAA, 0xBB]);
        reader.Skip(ThriftType.Binary);
        Assert.Equal(3, reader.Position);
    }

    [Fact]
    public void Skip_BooleanTypes()
    {
        // BooleanTrue and BooleanFalse are encoded in the header; skip consumes nothing.
        var reader = new ThriftCompactReader([]);
        reader.Skip(ThriftType.BooleanTrue);
        Assert.Equal(0, reader.Position);

        reader = new ThriftCompactReader([]);
        reader.Skip(ThriftType.BooleanFalse);
        Assert.Equal(0, reader.Position);
    }

    [Fact]
    public void Skip_List()
    {
        // List of 2 x I32: header 0x25, then two zigzag varints
        // zigzag(1)=2 → 0x02, zigzag(2)=4 → 0x04
        var reader = new ThriftCompactReader([0x25, 0x02, 0x04]);
        reader.Skip(ThriftType.List);
        Assert.Equal(3, reader.Position);
    }

    [Fact]
    public void Skip_Struct()
    {
        // Struct with field 1 (Byte) value 0xFF, then Stop
        var reader = new ThriftCompactReader([0x13, 0xFF, 0x00]);
        reader.Skip(ThriftType.Struct);
        Assert.Equal(3, reader.Position);
    }

    [Fact]
    public void Skip_Map()
    {
        // Empty map: count=0
        var reader = new ThriftCompactReader([0x00]);
        reader.Skip(ThriftType.Map);
        Assert.Equal(1, reader.Position);
    }

    [Fact]
    public void Skip_NestedStruct()
    {
        // Outer struct: field 1 Struct → inner struct: field 1 Byte 0xAA, stop → stop
        var reader = new ThriftCompactReader([
            0x1C,           // field 1 Struct
            0x13, 0xAA,     // inner field 1 Byte, value
            0x00,           // inner stop
            0x00,           // outer stop
        ]);
        reader.Skip(ThriftType.Struct);
        Assert.Equal(5, reader.Position);
    }

    [Fact]
    public void Position_TracksCorrectly()
    {
        var reader = new ThriftCompactReader([0x01, 0x02, 0x03]);
        Assert.Equal(0, reader.Position);
        reader.ReadByte();
        Assert.Equal(1, reader.Position);
        reader.ReadByte();
        Assert.Equal(2, reader.Position);
    }

    [Fact]
    public void ReadDouble_ThrowsOnInsufficientData()
    {
        var reader = new ThriftCompactReader([0x01, 0x02]);
        try
        {
            reader.ReadDouble();
            Assert.Fail("Expected ParquetFormatException");
        }
        catch (ParquetFormatException) { }
    }

    [Fact]
    public void ReadBinary_ThrowsOnInsufficientData()
    {
        // Length says 5 but only 2 bytes available
        var reader = new ThriftCompactReader([0x05, 0xAA, 0xBB]);
        try
        {
            reader.ReadBinary();
            Assert.Fail("Expected ParquetFormatException");
        }
        catch (ParquetFormatException) { }
    }

    [Fact]
    public void PopStruct_ThrowsOnUnderflow()
    {
        var reader = new ThriftCompactReader([]);
        try
        {
            reader.PopStruct();
            Assert.Fail("Expected ParquetFormatException");
        }
        catch (ParquetFormatException) { }
    }
}
