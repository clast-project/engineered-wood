// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using EngineeredWood.Encodings;

namespace EngineeredWood.Parquet.Thrift;

/// <summary>
/// Thrift Compact Protocol encoder that writes to a growable byte buffer.
/// Mirror of <see cref="ThriftCompactReader"/>.
/// </summary>
internal sealed class ThriftCompactWriter
{
    private byte[] _buffer;
    private int _position;
    private short _lastFieldId;

    // Inline stack for nested struct field IDs.
    private const int MaxNesting = 8;
    private readonly short[] _stack = new short[MaxNesting];
    private int _stackDepth;

    public ThriftCompactWriter(int initialCapacity = 1024)
    {
        _buffer = new byte[initialCapacity];
    }

    /// <summary>Number of bytes written so far.</summary>
    public int Length => _position;

    /// <summary>Returns the written bytes as a span.</summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _position);

    /// <summary>Returns the written bytes as a new array.</summary>
    public byte[] ToArray() => _buffer.AsSpan(0, _position).ToArray();

    /// <summary>Resets the writer for reuse.</summary>
    public void Reset()
    {
        _position = 0;
        _lastFieldId = 0;
        _stackDepth = 0;
    }

    private void EnsureCapacity(int additionalBytes)
    {
        int required = _position + additionalBytes;
        if (required <= _buffer.Length)
            return;

        int newSize = Math.Max(_buffer.Length * 2, required);
        var newBuffer = new byte[newSize];
        _buffer.AsSpan(0, _position).CopyTo(newBuffer);
        _buffer = newBuffer;
    }

    /// <summary>Writes a single byte.</summary>
    public void WriteByte(byte value)
    {
        EnsureCapacity(1);
        _buffer[_position++] = value;
    }

    /// <summary>Writes an unsigned variable-length integer (ULEB128).</summary>
    public void WriteVarint(ulong value)
    {
        EnsureCapacity(10); // max varint size
        _position += Varint.WriteUnsigned(_buffer.AsSpan(_position), value);
    }

    /// <summary>Writes a zigzag-encoded 32-bit integer.</summary>
    public void WriteZigZagInt32(int value)
    {
        EnsureCapacity(10);
        _position += Varint.WriteSigned(_buffer.AsSpan(_position), value);
    }

    /// <summary>Writes a zigzag-encoded 64-bit integer.</summary>
    public void WriteZigZagInt64(long value)
    {
        EnsureCapacity(10);
        _position += Varint.WriteSigned(_buffer.AsSpan(_position), value);
    }

    /// <summary>Writes a 16-bit integer (zigzag encoded in compact protocol).</summary>
    public void WriteI16(short value) => WriteZigZagInt32(value);

    /// <summary>Writes a 64-bit IEEE double (8 bytes little-endian).</summary>
    public void WriteDouble(double value)
    {
        EnsureCapacity(8);
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteDoubleLittleEndian(_buffer.AsSpan(_position), value);
#else
        byte[] tmp = BitConverter.GetBytes(value);
        tmp.CopyTo(_buffer, _position);
#endif
        _position += 8;
    }

    /// <summary>Writes a binary field (length-prefixed byte sequence).</summary>
    public void WriteBinary(ReadOnlySpan<byte> value)
    {
        WriteVarint((ulong)value.Length);
        EnsureCapacity(value.Length);
        value.CopyTo(_buffer.AsSpan(_position));
        _position += value.Length;
    }

    /// <summary>Writes a UTF-8 string field.</summary>
    public void WriteString(string value)
    {
        int byteCount = System.Text.Encoding.UTF8.GetByteCount(value);
        WriteVarint((ulong)byteCount);
        EnsureCapacity(byteCount);
#if NET8_0_OR_GREATER
        System.Text.Encoding.UTF8.GetBytes(value, _buffer.AsSpan(_position));
#else
        System.Text.Encoding.UTF8.GetBytes(value, 0, value.Length, _buffer, _position);
#endif
        _position += byteCount;
    }

    /// <summary>Writes a boolean value (standalone, not in a field header).</summary>
    public void WriteBool(bool value)
    {
        WriteByte(value ? (byte)1 : (byte)0);
    }

    /// <summary>
    /// Writes a field header. For boolean fields, the value is encoded
    /// directly in the type nibble (compact protocol optimization).
    /// </summary>
    public void WriteFieldHeader(ThriftType type, short fieldId)
    {
        int delta = fieldId - _lastFieldId;

        if (delta > 0 && delta <= 15)
        {
            // Short form: delta encoded in high nibble
            WriteByte((byte)((delta << 4) | (byte)type));
        }
        else
        {
            // Long form: type byte followed by zigzag field ID
            WriteByte((byte)type);
            WriteI16(fieldId);
        }

        _lastFieldId = fieldId;
    }

    /// <summary>
    /// Writes a boolean field with the value encoded in the type nibble.
    /// </summary>
    public void WriteBoolField(short fieldId, bool value)
    {
        var type = value ? ThriftType.BooleanTrue : ThriftType.BooleanFalse;
        WriteFieldHeader(type, fieldId);
    }

    /// <summary>Writes a struct stop marker (0x00).</summary>
    public void WriteStructStop()
    {
        WriteByte(0);
    }

    /// <summary>Writes a list header.</summary>
    public void WriteListHeader(ThriftType elementType, int count)
    {
        if (count < 15)
        {
            // Small list: count in high nibble
            WriteByte((byte)((count << 4) | (byte)elementType));
        }
        else
        {
            // Large list: 0xF in high nibble, count follows as varint
            WriteByte((byte)(0xF0 | (byte)elementType));
            WriteVarint((ulong)count);
        }
    }

    /// <summary>Saves the current field ID context before descending into a nested struct.</summary>
    public void PushStruct()
    {
        if (_stackDepth >= MaxNesting)
            throw new InvalidOperationException("Thrift struct nesting too deep.");

        _stack[_stackDepth++] = _lastFieldId;
        _lastFieldId = 0;
    }

    /// <summary>Restores the field ID context after returning from a nested struct.</summary>
    public void PopStruct()
    {
        if (_stackDepth <= 0)
            throw new InvalidOperationException("Thrift struct stack underflow.");

        _lastFieldId = _stack[--_stackDepth];
    }
}
