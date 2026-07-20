// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes values using the BYTE_STREAM_SPLIT encoding.
/// Mirror of <see cref="ByteStreamSplitDecoder"/>.
/// </summary>
/// <remarks>
/// For N values of W-byte width, produces W interleaved streams of N bytes each:
/// stream 0 has byte 0 of every value, stream 1 has byte 1, etc.
/// This layout groups similar bytes together for better compression.
/// </remarks>
internal static class ByteStreamSplitEncoder
{
    public static byte[] EncodeFloats(ReadOnlySpan<float> values)
        => Split(MemoryMarshal.AsBytes(values), values.Length, sizeof(float));

    public static byte[] EncodeDoubles(ReadOnlySpan<double> values)
        => Split(MemoryMarshal.AsBytes(values), values.Length, sizeof(double));

    public static byte[] EncodeInt32s(ReadOnlySpan<int> values)
        => Split(MemoryMarshal.AsBytes(values), values.Length, sizeof(int));

    public static byte[] EncodeInt64s(ReadOnlySpan<long> values)
        => Split(MemoryMarshal.AsBytes(values), values.Length, sizeof(long));

    /// <summary>Encodes floats into a pre-allocated destination buffer.</summary>
    public static void EncodeFloatsTo(ReadOnlySpan<float> values, byte[] dest)
        => SplitTo(MemoryMarshal.AsBytes(values), values.Length, sizeof(float), dest);

    /// <summary>Encodes doubles into a pre-allocated destination buffer.</summary>
    public static void EncodeDoublesTo(ReadOnlySpan<double> values, byte[] dest)
        => SplitTo(MemoryMarshal.AsBytes(values), values.Length, sizeof(double), dest);

    /// <summary>Encodes floats from Arrow buffer, skipping nulls, into a pre-allocated destination.</summary>
    public static void EncodeFloatsTo(
        ReadOnlySpan<float> valueBuffer, int offset, int numValues, int nonNullCount,
        int[] defLevels, byte[] dest)
        => SplitSparseTo<float>(valueBuffer, offset, numValues, nonNullCount, defLevels, sizeof(float), dest);

    /// <summary>Encodes doubles from Arrow buffer, skipping nulls, into a pre-allocated destination.</summary>
    public static void EncodeDoublesTo(
        ReadOnlySpan<double> valueBuffer, int offset, int numValues, int nonNullCount,
        int[] defLevels, byte[] dest)
        => SplitSparseTo<double>(valueBuffer, offset, numValues, nonNullCount, defLevels, sizeof(double), dest);

    private static byte[] Split(ReadOnlySpan<byte> source, int count, int width)
    {
        var result = new byte[count * width];
        SplitTo(source, count, width, result);
        return result;
    }

    private static void SplitTo(ReadOnlySpan<byte> source, int count, int width, byte[] dest)
    {
        for (int stream = 0; stream < width; stream++)
        {
            int streamOffset = stream * count;
            for (int i = 0; i < count; i++)
                dest[streamOffset + i] = source[i * width + stream];
        }
    }

    private static void SplitSparseTo<T>(
        ReadOnlySpan<T> valueBuffer, int offset, int numValues, int nonNullCount,
        int[] defLevels, int width, byte[] dest) where T : unmanaged
    {
        int idx = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[offset + i] == 0) continue;

            ReadOnlySpan<byte> valueBytes = MemoryMarshal.AsBytes(
                valueBuffer.Slice(offset + i, 1));
            for (int stream = 0; stream < width; stream++)
                dest[stream * nonNullCount + idx] = valueBytes[stream];
            idx++;
        }
    }
}
