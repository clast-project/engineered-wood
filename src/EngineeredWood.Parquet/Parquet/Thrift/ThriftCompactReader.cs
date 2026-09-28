// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Encodings;

namespace EngineeredWood.Parquet.Thrift;

/// <summary>
/// Zero-allocation Thrift Compact Protocol decoder over a <see cref="ReadOnlySpan{T}"/>.
/// </summary>
internal ref struct ThriftCompactReader
{
    private readonly ReadOnlySpan<byte> _data;
    private int _position;
    private short _lastFieldId;

    // Inline stack for nested struct field IDs. Parquet nesting is shallow (max ~6 levels).
    private const int MaxNesting = 8;
    private short _stack0, _stack1, _stack2, _stack3;
    private short _stack4, _stack5, _stack6, _stack7;
    private int _stackDepth;

    // Pending boolean value from field header (compact protocol encodes bool in type nibble).
    private bool _hasPendingBool;
    private bool _pendingBoolValue;

    public ThriftCompactReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        _position = 0;
        _lastFieldId = 0;
        _stackDepth = 0;
        _hasPendingBool = false;
        _pendingBoolValue = false;
        _stack0 = _stack1 = _stack2 = _stack3 = 0;
        _stack4 = _stack5 = _stack6 = _stack7 = 0;
    }

    /// <summary>Current read position within the data span.</summary>
    public int Position => _position;

    /// <summary>Bytes left to read. Every list element occupies at least one, which bounds a list's count.</summary>
    public int Remaining => _data.Length - _position;

    public byte ReadByte()
    {
        if (_position >= _data.Length)
            throw new ParquetFormatException("Unexpected end of Thrift data.");
        return _data[_position++];
    }

    /// <summary>Reads an unsigned variable-length integer (ULEB128).</summary>
    /// <remarks>
    /// Bounded here rather than through <see cref="Varint.ReadUnsigned"/>, which checks nothing: a
    /// varint whose last byte still has its continuation bit set would otherwise run off the end of
    /// the span as an <see cref="IndexOutOfRangeException"/>.
    /// </remarks>
    public ulong ReadVarint()
    {
        ulong result = 0;
        for (int shift = 0; shift < 64; shift += 7)
        {
            if (_position >= _data.Length)
                throw new ParquetFormatException("Unexpected end of Thrift data reading a varint.");
            byte b = _data[_position++];
            // The tenth byte carries only bit 63; more would be shifted out and lost.
            if (shift == 63 && (b & 0x7E) != 0)
                throw new ParquetFormatException("Thrift varint overflows 64 bits.");
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
        }

        throw new ParquetFormatException("Thrift varint is longer than 10 bytes.");
    }

    /// <summary>Reads a varint that must fit a non-negative <see cref="int"/>: a length or a count.</summary>
    private int ReadVarintInt32()
    {
        ulong value = ReadVarint();
        if (value > int.MaxValue)
            throw new ParquetFormatException($"Thrift length or count {value} is out of range.");
        return (int)value;
    }

    /// <summary>Reads a zigzag-encoded 32-bit integer.</summary>
    public int ReadZigZagInt32()
    {
        long value = ReadZigZagInt64();
        if (value is < int.MinValue or > int.MaxValue)
            throw new ParquetFormatException($"Thrift i32 value {value} is out of range.");
        return (int)value;
    }

    /// <summary>Reads a zigzag-encoded 64-bit integer.</summary>
    public long ReadZigZagInt64() => Varint.ZigzagDecode(unchecked((long)ReadVarint()));

    /// <summary>Reads a 16-bit integer (zigzag encoded in compact protocol).</summary>
    public short ReadI16()
    {
        return (short)ReadZigZagInt32();
    }

    /// <summary>Reads a 64-bit IEEE double (8 bytes little-endian).</summary>
    public double ReadDouble()
    {
        if (_position + 8 > _data.Length)
            throw new ParquetFormatException("Unexpected end of Thrift data reading double.");
#if NET8_0_OR_GREATER
        double value = BinaryPrimitives.ReadDoubleLittleEndian(_data.Slice(_position));
#else
        double value = BitConverter.ToDouble(_data.Slice(_position).ToArray(), 0);
#endif
        _position += 8;
        return value;
    }

    /// <summary>Reads a binary field (length-prefixed byte sequence).</summary>
    public ReadOnlySpan<byte> ReadBinary()
    {
        int length = ReadVarintInt32();
        if (length > _data.Length - _position)
            throw new ParquetFormatException("Invalid binary length in Thrift data.");
        var span = _data.Slice(_position, length);
        _position += length;
        return span;
    }

    /// <summary>Reads a UTF-8 string field.</summary>
    public string ReadString()
    {
        var bytes = ReadBinary();
#if NET8_0_OR_GREATER
        return System.Text.Encoding.UTF8.GetString(bytes);
#else
        return System.Text.Encoding.UTF8.GetString(bytes.ToArray());
#endif
    }

    /// <summary>Reads a boolean value. If the bool was encoded in the field header, returns that cached value.</summary>
    public bool ReadBool()
    {
        if (_hasPendingBool)
        {
            _hasPendingBool = false;
            return _pendingBoolValue;
        }
        return ReadByte() == 1;
    }

    /// <summary>
    /// Reads the next field header. Returns the wire type and field ID.
    /// Returns <see cref="ThriftType.Stop"/> when the struct is complete.
    /// </summary>
    public (ThriftType Type, short FieldId) ReadFieldHeader()
    {
        byte header = ReadByte();
        if (header == 0)
            return (ThriftType.Stop, 0);

        var type = (ThriftType)(header & 0x0F);
        int delta = (header >> 4) & 0x0F;

        short fieldId;
        if (delta != 0)
        {
            // Short form: delta encoded from previous field ID.
            fieldId = (short)(_lastFieldId + delta);
        }
        else
        {
            // Long form: field ID follows as zigzag i16.
            fieldId = ReadI16();
        }

        _lastFieldId = fieldId;

        // In compact protocol, boolean values are encoded directly in the type nibble.
        if (type == ThriftType.BooleanTrue)
        {
            _hasPendingBool = true;
            _pendingBoolValue = true;
        }
        else if (type == ThriftType.BooleanFalse)
        {
            _hasPendingBool = true;
            _pendingBoolValue = false;
        }

        return (type, fieldId);
    }

    /// <summary>Reads a list header, returning element type and count.</summary>
    public (ThriftType ElementType, int Count) ReadListHeader()
    {
        byte header = ReadByte();
        int count = (header >> 4) & 0x0F;
        ThriftType elementType;

        if (count == 15)
        {
            // Large list: count follows as varint.
            count = ReadVarintInt32();
            elementType = (ThriftType)(header & 0x0F);
        }
        else
        {
            elementType = (ThriftType)(header & 0x0F);
        }

        return (elementType, count);
    }

    /// <summary>
    /// Reads a list header whose count is about to size an allocation or a loop. Every element, a
    /// bool included, takes at least a byte, so a count larger than the bytes left cannot be real.
    /// </summary>
    public (ThriftType ElementType, int Count) ReadBoundedListHeader()
    {
        var (elementType, count) = ReadListHeader();
        if (count > Remaining)
            throw new ParquetFormatException($"Thrift list claims {count} elements in {Remaining} bytes.");
        return (elementType, count);
    }

    /// <summary>Reads a map header, returning key type, value type, and count.</summary>
    public (ThriftType KeyType, ThriftType ValueType, int Count) ReadMapHeader()
    {
        int count = ReadVarintInt32();
        if (count == 0)
            return (ThriftType.Stop, ThriftType.Stop, 0);

        byte types = ReadByte();
        var keyType = (ThriftType)((types >> 4) & 0x0F);
        var valueType = (ThriftType)(types & 0x0F);
        return (keyType, valueType, count);
    }

    /// <summary>Saves the current field ID context before descending into a nested struct.</summary>
    public void PushStruct()
    {
        if (_stackDepth >= MaxNesting)
            throw new ParquetFormatException("Thrift struct nesting too deep.");

        SetStack(_stackDepth, _lastFieldId);
        _stackDepth++;
        _lastFieldId = 0;
    }

    /// <summary>Restores the field ID context after returning from a nested struct.</summary>
    public void PopStruct()
    {
        if (_stackDepth <= 0)
            throw new ParquetFormatException("Thrift struct stack underflow.");

        _stackDepth--;
        _lastFieldId = GetStack(_stackDepth);
    }

    /// <summary>Skips a field value of the given type, recursively for containers.</summary>
    public void Skip(ThriftType type)
    {
        switch (type)
        {
            case ThriftType.BooleanTrue:
            case ThriftType.BooleanFalse:
                // Boolean value is already encoded in the field header; nothing to skip.
                // But if there was a pending bool that hasn't been consumed, clear it.
                _hasPendingBool = false;
                break;

            case ThriftType.Byte:
                ReadByte();
                break;

            case ThriftType.I16:
            case ThriftType.I32:
                ReadVarint(); // zigzag-encoded, variable length
                break;

            case ThriftType.I64:
                ReadVarint();
                break;

            case ThriftType.Double:
                if (Remaining < 8)
                    throw new ParquetFormatException("Unexpected end of Thrift data skipping a double.");
                _position += 8;
                break;

            case ThriftType.Binary:
                ReadBinary();
                break;

            case ThriftType.List:
            case ThriftType.Set:
                var (elemType, count) = ReadBoundedListHeader();
                for (int i = 0; i < count; i++)
                    SkipElement(elemType);
                break;

            case ThriftType.Map:
                var (keyType, valueType, mapCount) = ReadMapHeader();
                // A key and a value take at least a byte each; a larger count would spin, not read.
                if (mapCount > Remaining / 2)
                    throw new ParquetFormatException($"Thrift map claims {mapCount} entries in {Remaining} bytes.");
                for (int i = 0; i < mapCount; i++)
                {
                    SkipElement(keyType);
                    SkipElement(valueType);
                }
                break;

            case ThriftType.Struct:
                PushStruct();
                while (true)
                {
                    var (fieldType, _) = ReadFieldHeader();
                    if (fieldType == ThriftType.Stop)
                        break;
                    Skip(fieldType);
                }
                PopStruct();
                break;

            default:
                throw new ParquetFormatException($"Cannot skip unknown Thrift type {type}.");
        }
    }

    /// <summary>
    /// Skips one element of a list, set or map. A bool there is a byte of its own, unlike a bool
    /// field, whose value is in the field header that <see cref="Skip"/> assumes.
    /// </summary>
    private void SkipElement(ThriftType type)
    {
        if (type is ThriftType.BooleanTrue or ThriftType.BooleanFalse)
            ReadByte();
        else
            Skip(type);
    }

    private readonly short GetStack(int index) => index switch
    {
        0 => _stack0,
        1 => _stack1,
        2 => _stack2,
        3 => _stack3,
        4 => _stack4,
        5 => _stack5,
        6 => _stack6,
        7 => _stack7,
        _ => throw new ParquetFormatException("Thrift struct nesting too deep."),
    };

    private void SetStack(int index, short value)
    {
        switch (index)
        {
            case 0: _stack0 = value; break;
            case 1: _stack1 = value; break;
            case 2: _stack2 = value; break;
            case 3: _stack3 = value; break;
            case 4: _stack4 = value; break;
            case 5: _stack5 = value; break;
            case 6: _stack6 = value; break;
            case 7: _stack7 = value; break;
            default: throw new ParquetFormatException("Thrift struct nesting too deep.");
        }
    }
}
