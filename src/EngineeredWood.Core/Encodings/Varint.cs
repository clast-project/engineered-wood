// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Runtime.CompilerServices;

namespace EngineeredWood.Encodings;

/// <summary>
/// Shared varint and zigzag encoding/decoding primitives used by both Parquet and ORC.
/// </summary>
internal static class Varint
{
    /// <summary>
    /// Reads an unsigned variable-length integer (LEB128) from <paramref name="data"/>
    /// starting at <paramref name="pos"/>, advancing <paramref name="pos"/> past the varint.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadUnsigned(ReadOnlySpan<byte> data, ref int pos)
    {
        long result = 0;
        int shift = 0;
        while (true)
        {
            byte b = data[pos++];
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
            shift += 7;
        }
    }

    /// <summary>
    /// Reads a signed zigzag-encoded variable-length integer from <paramref name="data"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadSigned(ReadOnlySpan<byte> data, ref int pos)
    {
        long unsigned = ReadUnsigned(data, ref pos);
        return ZigzagDecode(unsigned);
    }

    /// <summary>
    /// Writes an unsigned variable-length integer (LEB128) to <paramref name="dest"/>.
    /// </summary>
    /// <returns>The number of bytes written.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteUnsigned(Span<byte> dest, ulong value)
    {
        int i = 0;
        while (value > 0x7F)
        {
            dest[i++] = (byte)(value | 0x80);
            value >>= 7;
        }
        dest[i++] = (byte)value;
        return i;
    }

    /// <summary>
    /// Writes a signed zigzag-encoded variable-length integer to <paramref name="dest"/>.
    /// </summary>
    /// <returns>The number of bytes written.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteSigned(Span<byte> dest, long value)
    {
        return WriteUnsigned(dest, ZigzagEncode(value));
    }

    /// <summary>
    /// Writes an unsigned variable-length integer to a <see cref="Stream"/>.
    /// </summary>
    public static void WriteUnsigned(Stream output, ulong value)
    {
        while (value > 0x7F)
        {
            output.WriteByte((byte)(0x80 | (value & 0x7F)));
            value >>= 7;
        }
        output.WriteByte((byte)value);
    }

    /// <summary>
    /// Writes a signed zigzag-encoded variable-length integer to a <see cref="Stream"/>.
    /// </summary>
    public static void WriteSigned(Stream output, long value)
    {
        WriteUnsigned(output, ZigzagEncode(value));
    }

    /// <summary>Zigzag-decodes a long value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ZigzagDecode(long value)
    {
        return (long)((ulong)value >> 1) ^ -(value & 1);
    }

    /// <summary>Zigzag-encodes a long value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ZigzagEncode(long value)
    {
        return unchecked((ulong)((value << 1) ^ (value >> 63)));
    }

    /// <summary>
    /// Returns the number of bytes required to encode a signed value as a zigzag varint.
    /// </summary>
    public static int SignedSize(long value)
    {
        ulong v = ZigzagEncode(value);
        if (v == 0) return 1;
        return (64 - BitOperations.LeadingZeroCount(v) + 6) / 7;
    }

    /// <summary>
    /// Returns the maximum varint bytes needed for any signed value up to <paramref name="maxValue"/>.
    /// Useful for reserving buffer space before the actual value is known.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int MaxBytesForValue(long maxValue) => SignedSize(maxValue);
}
