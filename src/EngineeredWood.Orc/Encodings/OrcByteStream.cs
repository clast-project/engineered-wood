// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace EngineeredWood.Orc.Encodings;

/// <summary>
/// A lightweight, non-virtual byte reader over an array segment.
/// Replaces <see cref="MemoryStream"/> in ORC decoders for better inlining
/// and zero object-allocation overhead.
/// </summary>
internal sealed class OrcByteStream
{
    private readonly byte[] _array;
    private int _position;
    private readonly int _end;

    public OrcByteStream(byte[] array, int offset, int length)
    {
        _array = array;
        _position = offset;
        _end = offset + length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadByte()
    {
        if (_position >= _end) return -1;
        return _array[_position++];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReadExactly(Span<byte> buffer)
    {
        var src = _array.AsSpan(_position, buffer.Length);
        src.CopyTo(buffer);
        _position += buffer.Length;
    }

    /// <summary>
    /// Returns a read-only span of the next <paramref name="length"/> bytes
    /// directly from the backing array, advancing the position.
    /// Zero-copy alternative to ReadExactly when no mutation is needed.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> ReadSpan(int length)
    {
        var span = _array.AsSpan(_position, length);
        _position += length;
        return span;
    }
}
