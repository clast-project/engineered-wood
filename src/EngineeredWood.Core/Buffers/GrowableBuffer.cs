// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace EngineeredWood.Buffers;

/// <summary>
/// A growable byte buffer for building encoded data without the overhead of
/// <see cref="MemoryStream"/>. All write operations are non-virtual and inlineable.
/// </summary>
internal sealed class GrowableBuffer
{
    private byte[] _buffer;
    private int _position;

    public GrowableBuffer(int initialCapacity = 256)
    {
        _buffer = new byte[initialCapacity];
    }

    /// <summary>Number of bytes written.</summary>
    public int Length => _position;

    /// <summary>Returns the written bytes as a span (no copy).</summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _position);

    /// <summary>Returns the written bytes as memory (no copy). Suitable for async I/O.</summary>
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _position);

    /// <summary>Writes a single byte.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteByte(byte value)
    {
        if (_position >= _buffer.Length)
            Grow(1);
        _buffer[_position++] = value;
    }

    /// <summary>Writes a span of bytes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySpan<byte> data)
    {
        EnsureCapacity(data.Length);
        data.CopyTo(_buffer.AsSpan(_position));
        _position += data.Length;
    }

    /// <summary>Writes a segment of a byte array.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(byte[] data, int offset, int count)
    {
        EnsureCapacity(count);
        data.AsSpan(offset, count).CopyTo(_buffer.AsSpan(_position));
        _position += count;
    }

    /// <summary>
    /// Returns a writable span of at least <paramref name="sizeHint"/> bytes.
    /// Caller must follow with <see cref="Advance"/> after writing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<byte> GetSpan(int sizeHint)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_position);
    }

    /// <summary>Advances the position after a direct span write.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(int count) => _position += count;

    /// <summary>Resets the buffer for reuse without releasing memory.</summary>
    public void Reset() => _position = 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EnsureCapacity(int additionalBytes)
    {
        if (_position + additionalBytes <= _buffer.Length)
            return;
        Grow(additionalBytes);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(int additionalBytes)
    {
        int required = _position + additionalBytes;
        int newSize = Math.Max(_buffer.Length * 2, required);
        var newBuffer = new byte[newSize];
        _buffer.AsSpan(0, _position).CopyTo(newBuffer);
        _buffer = newBuffer;
    }
}
