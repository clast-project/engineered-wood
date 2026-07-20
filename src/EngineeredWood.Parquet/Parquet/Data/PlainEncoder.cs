// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes values in the PLAIN encoding for each Parquet physical type.
/// Mirror of <see cref="PlainDecoder"/>.
/// </summary>
internal static class PlainEncoder
{
    /// <summary>
    /// Encodes boolean values as bit-packed bytes (1 bit per value, LSB first).
    /// </summary>
    /// <returns>Number of bytes written.</returns>
    public static int EncodeBooleans(ReadOnlySpan<byte> validityBitmap, int offset, int count, Span<byte> dest)
    {
        int byteCount = (count + 7) / 8;
        dest.Slice(0, byteCount).Clear();

        for (int i = 0; i < count; i++)
        {
            int srcIdx = offset + i;
            bool value = (validityBitmap[srcIdx / 8] & (1 << (srcIdx % 8))) != 0;
            if (value)
                dest[i / 8] |= (byte)(1 << (i % 8));
        }

        return byteCount;
    }

    /// <summary>
    /// Encodes boolean values from a bool span as bit-packed bytes.
    /// </summary>
    public static int EncodeBooleans(ReadOnlySpan<bool> values, Span<byte> dest)
    {
        int byteCount = (values.Length + 7) / 8;
        dest.Slice(0, byteCount).Clear();

        for (int i = 0; i < values.Length; i++)
        {
            if (values[i])
                dest[i / 8] |= (byte)(1 << (i % 8));
        }

        return byteCount;
    }

    /// <summary>
    /// Encodes Int32 values. On little-endian systems this is a direct memory copy.
    /// </summary>
    public static int EncodeInt32s(ReadOnlySpan<int> values, Span<byte> dest)
    {
        var bytes = MemoryMarshal.AsBytes(values);
        bytes.CopyTo(dest);
        return bytes.Length;
    }

    /// <summary>
    /// Encodes Int64 values. On little-endian systems this is a direct memory copy.
    /// </summary>
    public static int EncodeInt64s(ReadOnlySpan<long> values, Span<byte> dest)
    {
        var bytes = MemoryMarshal.AsBytes(values);
        bytes.CopyTo(dest);
        return bytes.Length;
    }

    /// <summary>
    /// Encodes Float values.
    /// </summary>
    public static int EncodeFloats(ReadOnlySpan<float> values, Span<byte> dest)
    {
        var bytes = MemoryMarshal.AsBytes(values);
        bytes.CopyTo(dest);
        return bytes.Length;
    }

    /// <summary>
    /// Encodes Double values.
    /// </summary>
    public static int EncodeDoubles(ReadOnlySpan<double> values, Span<byte> dest)
    {
        var bytes = MemoryMarshal.AsBytes(values);
        bytes.CopyTo(dest);
        return bytes.Length;
    }

    /// <summary>
    /// Encodes FIXED_LEN_BYTE_ARRAY values (concatenated, no length prefix).
    /// </summary>
    public static int EncodeFixedLenByteArrays(ReadOnlySpan<byte> values, Span<byte> dest)
    {
        values.CopyTo(dest);
        return values.Length;
    }

    /// <summary>
    /// Encodes BYTE_ARRAY values with 4-byte LE length prefix per value.
    /// </summary>
    /// <returns>Number of bytes written.</returns>
    public static int EncodeByteArrays(ReadOnlySpan<int> offsets, ReadOnlySpan<byte> data, int count, Span<byte> dest)
    {
        int pos = 0;
        for (int i = 0; i < count; i++)
        {
            int start = offsets[i];
            int length = offsets[i + 1] - start;
            BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(pos), length);
            pos += 4;
            data.Slice(start, length).CopyTo(dest.Slice(pos));
            pos += length;
        }
        return pos;
    }

    /// <summary>
    /// Returns the PLAIN-encoded byte size for a given physical type and value count.
    /// For BYTE_ARRAY, use <see cref="MeasureByteArrays"/> instead.
    /// </summary>
    public static int GetEncodedSize(PhysicalType type, int valueCount, int typeLength = 0)
    {
        return type switch
        {
            PhysicalType.Boolean => (valueCount + 7) / 8,
            PhysicalType.Int32 or PhysicalType.Float => valueCount * 4,
            PhysicalType.Int64 or PhysicalType.Double => valueCount * 8,
            PhysicalType.Int96 => valueCount * 12,
            PhysicalType.FixedLenByteArray => valueCount * typeLength,
            PhysicalType.ByteArray => throw new InvalidOperationException(
                "Use MeasureByteArrays for BYTE_ARRAY columns."),
            _ => throw new NotSupportedException($"Unsupported physical type: {type}"),
        };
    }

    /// <summary>
    /// Returns the total PLAIN-encoded byte size for BYTE_ARRAY values:
    /// 4 bytes per length prefix + sum of all value lengths.
    /// </summary>
    public static int MeasureByteArrays(ReadOnlySpan<int> offsets, int count)
    {
        int dataBytes = offsets[count] - offsets[0];
        return count * 4 + dataBytes;
    }
}
