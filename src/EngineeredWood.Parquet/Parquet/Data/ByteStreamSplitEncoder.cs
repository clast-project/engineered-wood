// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Runtime.InteropServices;
using EngineeredWood.Encodings;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes values using the BYTE_STREAM_SPLIT encoding.
/// Mirror of the decode side, <c>ColumnChunkReader.DecodeByteStreamSplitValues</c>.
/// </summary>
/// <remarks>
/// For N values of W-byte width, produces W interleaved streams of N bytes each:
/// stream 0 has byte 0 of every value, stream 1 has byte 1, etc.
/// This layout groups similar bytes together for better compression.
/// The transform itself, with its SIMD kernels, is <see cref="ByteStreamSplitter"/> in Core.
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
        => ByteStreamSplitter.Split(MemoryMarshal.AsBytes(values), dest, values.Length, sizeof(float));

    /// <summary>Encodes doubles into a pre-allocated destination buffer.</summary>
    public static void EncodeDoublesTo(ReadOnlySpan<double> values, byte[] dest)
        => ByteStreamSplitter.Split(MemoryMarshal.AsBytes(values), dest, values.Length, sizeof(double));

    /// <summary>Encodes floats from Arrow buffer, skipping nulls, into a pre-allocated destination.</summary>
    public static void EncodeFloatsTo(
        ReadOnlySpan<float> valueBuffer, int offset, int numValues, int nonNullCount,
        int[] defLevels, byte[] dest)
        => SplitSparseTo(valueBuffer, offset, numValues, nonNullCount, defLevels, sizeof(float), dest);

    /// <summary>Encodes doubles from Arrow buffer, skipping nulls, into a pre-allocated destination.</summary>
    public static void EncodeDoublesTo(
        ReadOnlySpan<double> valueBuffer, int offset, int numValues, int nonNullCount,
        int[] defLevels, byte[] dest)
        => SplitSparseTo(valueBuffer, offset, numValues, nonNullCount, defLevels, sizeof(double), dest);

    private static byte[] Split(ReadOnlySpan<byte> source, int count, int width)
    {
        var result = new byte[count * width];
        ByteStreamSplitter.Split(source, result, count, width);
        return result;
    }

    /// <summary>
    /// Gathers the non-null values into a pooled buffer, then splits that densely. Copying whole values
    /// and running the SIMD kernel beats scattering each value's bytes across the streams one at a time.
    /// </summary>
    private static void SplitSparseTo<T>(
        ReadOnlySpan<T> valueBuffer, int offset, int numValues, int nonNullCount,
        int[] defLevels, int width, byte[] dest) where T : unmanaged
    {
        // One spare slot: the loop writes every value and only advances past the non-null ones, so a null
        // after the last non-null value is written at index nonNullCount and then ignored.
        T[] rented = ArrayPool<T>.Shared.Rent(nonNullCount + 1);
        try
        {
            Span<T> compact = rented.AsSpan(0, nonNullCount + 1);
            ReadOnlySpan<T> values = valueBuffer.Slice(offset, numValues);
            ReadOnlySpan<int> levels = defLevels.AsSpan(offset, numValues);
            int idx = 0;

            // Branchless: with nulls scattered at random, an if here mispredicts often enough to cost
            // more than the split itself.
            for (int i = 0; i < values.Length; i++)
            {
                compact[idx] = values[i];
                idx += levels[i] != 0 ? 1 : 0;
            }

            ByteStreamSplitter.Split(MemoryMarshal.AsBytes(compact.Slice(0, nonNullCount)), dest, nonNullCount, width);
        }
        finally
        {
            ArrayPool<T>.Shared.Return(rented);
        }
    }
}
