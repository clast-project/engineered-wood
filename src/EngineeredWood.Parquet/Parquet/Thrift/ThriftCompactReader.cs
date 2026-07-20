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

    public byte ReadByte()
    {
        if (_position >= _data.Length)
            throw new ParquetFormatException("Unexpected end of Thrift data.");
        return _data[_position++];
    }

    /// <summary>Reads an unsigned variable-length integer (ULEB128).</summary>
    public ulong ReadVarint()
    {
        if (_position >= _data.Length)
            throw new ParquetFormatException("Unexpected end of Thrift data.");
        return unchecked((ulong)Varint.ReadUnsigned(_data, ref _position));
    }

    /// <summary>Reads a zigzag-encoded 32-bit integer.</summary>
    public int ReadZigZagInt32()
    {
        if (_position >= _data.Length)
            throw new ParquetFormatException("Unexpected end of Thrift data.");
        return checked((int)Varint.ReadSigned(_data, ref _position));
    }

    /// <summary>Reads a zigzag-encoded 64-bit integer.</summary>
    public long ReadZigZagInt64()
    {
        if (_position >= _data.Length)
            throw new ParquetFormatException("Unexpected end of Thrift data.");
        return Varint.ReadSigned(_data, ref _position);
    }

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
        int length = checked((int)ReadVarint());
        if (length < 0 || _position + length > _data.Length)
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
            count = checked((int)ReadVarint());
            elementType = (ThriftType)(header & 0x0F);
        }
        else
        {
            elementType = (ThriftType)(header & 0x0F);
        }

        return (elementType, count);
    }

    /// <summary>Reads a map header, returning key type, value type, and count.</summary>
    public (ThriftType KeyType, ThriftType ValueType, int Count) ReadMapHeader()
    {
        int count = checked((int)ReadVarint());
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
                _position++;
                break;

            case ThriftType.I16:
            case ThriftType.I32:
                ReadVarint(); // zigzag-encoded, variable length
                break;

            case ThriftType.I64:
                ReadVarint();
                break;

            case ThriftType.Double:
                _position += 8;
                break;

            case ThriftType.Binary:
                ReadBinary();
                break;

            case ThriftType.List:
            case ThriftType.Set:
                var (elemType, count) = ReadListHeader();
                for (int i = 0; i < count; i++)
                    Skip(elemType);
                break;

            case ThriftType.Map:
                var (keyType, valueType, mapCount) = ReadMapHeader();
                for (int i = 0; i < mapCount; i++)
                {
                    Skip(keyType);
                    Skip(valueType);
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
