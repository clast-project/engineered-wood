// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using EngineeredWood.Buffers;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Encoding;

/// <summary>
/// Avro binary encoder that writes to a <see cref="GrowableBuffer"/>.
/// Uses the Avro binary encoding specification.
/// </summary>
internal sealed class AvroBinaryWriter
{
    private readonly GrowableBuffer _buffer;

    public AvroBinaryWriter(GrowableBuffer buffer)
    {
        _buffer = buffer;
    }

    public int Length => _buffer.Length;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteNull() { } // Avro null is zero bytes

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBoolean(bool value) => _buffer.WriteByte(value ? (byte)1 : (byte)0);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt(int value) => WriteLong(value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteLong(long value)
    {
        var span = _buffer.GetSpan(10);
        _buffer.Advance(Varint.WriteSigned(span, value));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteFloat(float value)
    {
        var span = _buffer.GetSpan(4);
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteSingleLittleEndian(span, value);
#else
        MemoryMarshal.Write(span, ref value);
#endif
        _buffer.Advance(4);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteDouble(double value)
    {
        var span = _buffer.GetSpan(8);
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteDoubleLittleEndian(span, value);
#else
        MemoryMarshal.Write(span, ref value);
#endif
        _buffer.Advance(8);
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteLong(value.Length);
        _buffer.Write(value);
    }

    public void WriteString(string value)
    {
#if NET8_0_OR_GREATER
        // Single-pass fast path for strings whose UTF-8 length equals char length
        // (pure ASCII). This covers the vast majority of real-world Avro string data
        // and avoids the separate GetByteCount traversal.
        int maxBytes = System.Text.Encoding.UTF8.GetMaxByteCount(value.Length);
        int varintMax = Varint.MaxBytesForValue(maxBytes);
        var span = _buffer.GetSpan(varintMax + maxBytes);
        int byteCount = System.Text.Encoding.UTF8.GetBytes(value, span[varintMax..]);
        int varintLen = Varint.WriteSigned(span, byteCount);
        if (varintLen < varintMax)
            span.Slice(varintMax, byteCount).CopyTo(span[varintLen..]);
        _buffer.Advance(varintLen + byteCount);
#else
        int byteCount = System.Text.Encoding.UTF8.GetByteCount(value);
        WriteLong(byteCount);
        var span = _buffer.GetSpan(byteCount);
#if NETSTANDARD2_0
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        bytes.AsSpan().CopyTo(span);
#else
        System.Text.Encoding.UTF8.GetBytes(value, span);
#endif
        _buffer.Advance(byteCount);
#endif
    }

    public void WriteString(ReadOnlySpan<byte> utf8Bytes)
    {
        WriteLong(utf8Bytes.Length);
        _buffer.Write(utf8Bytes);
    }

    public void WriteFixed(ReadOnlySpan<byte> value)
    {
        _buffer.Write(value);
    }

    /// <summary>Writes a union branch index (0-based).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteUnionIndex(int index) => WriteLong(index);

    /// <summary>Returns the underlying buffer's written span.</summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.WrittenSpan;

    /// <summary>Returns the underlying buffer's written memory.</summary>
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.WrittenMemory;

    public void Reset() => _buffer.Reset();
}
