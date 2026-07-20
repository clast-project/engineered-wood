// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes PLAIN-encoded values for each Parquet physical type.
/// </summary>
internal static class PlainDecoder
{
    /// <summary>
    /// Decodes <paramref name="count"/> boolean values from the PLAIN encoding.
    /// Booleans are bit-packed: 1 bit per value, LSB first.
    /// </summary>
    public static void DecodeBooleans(ReadOnlySpan<byte> data, Span<bool> destination, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int byteIndex = i / 8;
            int bitIndex = i % 8;
            if (byteIndex >= data.Length)
                throw new ParquetFormatException("Unexpected end of PLAIN boolean data.");
            destination[i] = ((data[byteIndex] >> bitIndex) & 1) == 1;
        }
    }

    /// <summary>
    /// Decodes <paramref name="count"/> Int32 values from the PLAIN encoding.
    /// </summary>
    public static void DecodeInt32s(ReadOnlySpan<byte> data, Span<int> destination, int count)
    {
        var source = MemoryMarshal.Cast<byte, int>(data);
        if (source.Length < count)
            throw new ParquetFormatException("Unexpected end of PLAIN Int32 data.");
        source.Slice(0, count).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> Int64 values from the PLAIN encoding.
    /// </summary>
    public static void DecodeInt64s(ReadOnlySpan<byte> data, Span<long> destination, int count)
    {
        var source = MemoryMarshal.Cast<byte, long>(data);
        if (source.Length < count)
            throw new ParquetFormatException("Unexpected end of PLAIN Int64 data.");
        source.Slice(0, count).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> Float values from the PLAIN encoding.
    /// </summary>
    public static void DecodeFloats(ReadOnlySpan<byte> data, Span<float> destination, int count)
    {
        var source = MemoryMarshal.Cast<byte, float>(data);
        if (source.Length < count)
            throw new ParquetFormatException("Unexpected end of PLAIN Float data.");
        source.Slice(0, count).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> Double values from the PLAIN encoding.
    /// </summary>
    public static void DecodeDoubles(ReadOnlySpan<byte> data, Span<double> destination, int count)
    {
        var source = MemoryMarshal.Cast<byte, double>(data);
        if (source.Length < count)
            throw new ParquetFormatException("Unexpected end of PLAIN Double data.");
        source.Slice(0, count).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> INT96 values from the PLAIN encoding.
    /// Each value is 12 bytes.
    /// </summary>
    public static void DecodeInt96s(ReadOnlySpan<byte> data, Span<byte> destination, int count)
    {
        int totalBytes = count * 12;
        if (data.Length < totalBytes)
            throw new ParquetFormatException("Unexpected end of PLAIN Int96 data.");
        data.Slice(0, totalBytes).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> FIXED_LEN_BYTE_ARRAY values from the PLAIN encoding.
    /// </summary>
    public static void DecodeFixedLenByteArrays(
        ReadOnlySpan<byte> data, Span<byte> destination, int count, int typeLength)
    {
        int totalBytes = count * typeLength;
        if (data.Length < totalBytes)
            throw new ParquetFormatException("Unexpected end of PLAIN FixedLenByteArray data.");
        data.Slice(0, totalBytes).CopyTo(destination);
    }

    /// <summary>
    /// Decodes <paramref name="count"/> BYTE_ARRAY values from the PLAIN encoding.
    /// Each value is prefixed with a 4-byte little-endian length.
    /// Returns offsets and data as separate arrays for use with Arrow binary/string arrays.
    /// </summary>
    /// <returns>The number of bytes consumed from the source data.</returns>
    public static int DecodeByteArrays(
        ReadOnlySpan<byte> data,
        Span<int> offsets,
        out byte[] values,
        int count)
    {
        // Single pass: scan lengths to compute total size, then bulk-copy.
        // Lengths array avoids re-reading each 4-byte header in a second pass.
        // Cap stack buffer at 256 ints (1 KB); rent from the pool for larger batches.
        int[]? rentedLengths = null;
        Span<int> lengths = count <= 256
            ? stackalloc int[count]
            : (rentedLengths = ArrayPool<int>.Shared.Rent(count)).AsSpan(0, count);
        try
        {
            int pos = 0;
            int totalDataSize = 0;
            for (int i = 0; i < count; i++)
            {
                if (pos + 4 > data.Length)
                    throw new ParquetFormatException("Unexpected end of PLAIN ByteArray data.");
                int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos));
                lengths[i] = len;
                pos += 4 + len;
                totalDataSize += len;
            }

            values = new byte[totalDataSize];

            // Build offsets and copy data in one pass using cached lengths
            int dataPos = 0;
            int srcPos = 0;
            offsets[0] = 0;
            for (int i = 0; i < count; i++)
            {
                int len = lengths[i];
                srcPos += 4;
                data.Slice(srcPos, len).CopyTo(values.AsSpan(dataPos));
                srcPos += len;
                dataPos += len;
                offsets[i + 1] = dataPos;
            }

            return pos;
        }
        finally
        {
            if (rentedLengths is not null) ArrayPool<int>.Shared.Return(rentedLengths);
        }
    }

    /// <summary>
    /// First-pass scan of <paramref name="count"/> PLAIN-encoded BYTE_ARRAY values.
    /// Fills <paramref name="offsets"/> with cumulative output lengths (offsets[0]=0,
    /// offsets[i+1]=offsets[i]+len_i) and returns the total output data size.
    /// Use <see cref="CopyByteArrayData"/> as the second pass to write data into a
    /// pre-allocated buffer.
    /// </summary>
    public static int MeasureByteArrays(ReadOnlySpan<byte> data, Span<int> offsets, int count)
    {
        int srcPos = 0;
        int totalDataSize = 0;
        offsets[0] = 0;
        for (int i = 0; i < count; i++)
        {
            if (srcPos + 4 > data.Length)
                throw new ParquetFormatException("Unexpected end of PLAIN ByteArray data.");
            int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(srcPos));
            srcPos += 4 + len;
            totalDataSize += len;
            offsets[i + 1] = totalDataSize;
        }
        return totalDataSize;
    }

    /// <summary>
    /// Second-pass copy for PLAIN-encoded BYTE_ARRAY values. Uses the offsets filled by
    /// <see cref="MeasureByteArrays"/> to copy each value's bytes into <paramref name="dest"/>.
    /// Source position for value i is derived as <c>4*(i+1) + offsets[i]</c>.
    /// </summary>
    public static void CopyByteArrayData(
        ReadOnlySpan<byte> data,
        ReadOnlySpan<int> offsets,
        Span<byte> dest,
        int count)
    {
        for (int i = 0; i < count; i++)
        {
            int destOffset = offsets[i];
            int len = offsets[i + 1] - destOffset;
            // Source layout: [H0 4B][D0][H1 4B][D1]... → Di starts at 4*(i+1) + offsets[i]
            data.Slice(4 * (i + 1) + destOffset, len).CopyTo(dest.Slice(destOffset));
        }
    }

    /// <summary>
    /// Batch view writer for PLAIN-encoded BYTE_ARRAY values.
    /// Pre-reserves overflow space then writes all views in a tight loop.
    /// </summary>
    public static void WriteViewsToState(ReadOnlySpan<byte> data, int count, ColumnBuildState state)
    {
        state.WritePlainByteArrayViews(data, count);
    }
}
