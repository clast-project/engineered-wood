// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Benchmarks ALP encoding and decoding at the encoder/decoder level (no file I/O), across the
/// frame-of-reference bit widths that dominate real data.
/// </summary>
/// <remarks>
/// <para>Widths are taken from the CWI ALP corpus, weighted by value: 0 is 16% of values and takes
/// the <c>Fill</c> path rather than unpacking anything; 11 and 16 are the two commonest non-zero
/// widths; 24 and 39 cover the middle; 56 is the widest that corpus produces. Nothing there reaches
/// 58, where a value can straddle two 64-bit words and the decoder falls back to unpacking one value
/// at a time.</para>
/// <para>DOUBLE only. The FLOAT path shares the unpack kernel and differs just in the inverse
/// transform, which the DOUBLE numbers already characterise.</para>
/// <para><b>This measures steady state, and cannot see the failure it would most be wanted for.</b>
/// BenchmarkDotNet warms every case by design. The staged unpacker ran at a third the speed of the
/// code it replaced until its methods had been called a few thousand times — a regression that a
/// warmed benchmark reports as a clean win. Guarding that needs a short-lived process measuring the
/// first calls, which is not what this is.</para>
/// </remarks>
[MemoryDiagnoser]
public class AlpBenchmarks
{
    /// <summary>One canonical vector's worth of values times 128 — a full default-size data page.</summary>
    private const int ValueCount = 131_072;

    private double[] _values = null!;
    private byte[] _page = null!;
    private double[] _decoded = null!;

    [Params(0, 11, 16, 24, 39, 56)]
    public int BitWidth { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _values = GenerateForBitWidth(BitWidth, ValueCount);
        _page = AlpEncoder.EncodeDoubles(_values);
        _decoded = new double[ValueCount];

        // A benchmark labelled with a width it did not actually produce is worse than no benchmark,
        // so check what the encoder chose rather than trusting the generator. Every vector, not just
        // the first: a page holds 128 of them and each picks its own (exponent, factor) and its own
        // width from its own range, so checking one and assuming the rest would be checking the only
        // vector the generator pins the extremes into.
        ValidateEveryVectorBitWidth(_page, BitWidth);

        AlpDecoder.DecodeDoubles(_page, _decoded, ValueCount);
        for (int i = 0; i < ValueCount; i++)
        {
            if (BitConverter.DoubleToInt64Bits(_decoded[i]) != BitConverter.DoubleToInt64Bits(_values[i]))
                throw new InvalidOperationException($"round trip failed at index {i}");
        }
    }

    [Benchmark(Description = "Encode")]
    public int Encode() => AlpEncoder.EncodeDoubles(_values).Length;

    [Benchmark(Description = "Decode")]
    public double Decode()
    {
        AlpDecoder.DecodeDoubles(_page, _decoded, ValueCount);
        return _decoded[^1];
    }

    /// <summary>
    /// Values whose frame-of-reference deltas span exactly <paramref name="bitWidth"/> bits.
    /// </summary>
    /// <remarks>
    /// Two decimal places up to the point where the scaled integer stops being exactly representable
    /// as a double, which is the shape ALP is for; whole numbers above that, where any decimal form
    /// would round-trip through exceptions instead and stop measuring the width asked for.
    /// </remarks>
    private static double[] GenerateForBitWidth(int bitWidth, int count)
    {
        var values = new double[count];

        if (bitWidth == 0)
        {
            // One repeated value: every delta is zero, so the decoder fills from the frame of
            // reference and never unpacks.
            for (int i = 0; i < count; i++)
                values[i] = 12.34;
            return values;
        }

        var random = new Random(42);
        bool decimalLike = bitWidth <= 45;

        // Past 53 bits an integer is no longer exactly representable as a double, so values are
        // quantized to the spacing of doubles at that magnitude. Without this the top of the range
        // rounds UP to the next power of two and the vector comes out one bit wider than asked for
        // — which is exactly what the check in GlobalSetup caught.
        long unit = 1L << Math.Max(0, bitWidth - 53);
        long top = ((1L << bitWidth) - 1) / unit * unit;

        for (int i = 0; i < count; i++)
        {
            long k = NextValueOfWidth(random, bitWidth) / unit * unit;
            values[i] = decimalLike ? k / 100.0 : k;
        }

        // Pin the extremes so the frame-of-reference range spans the full width every time rather
        // than whatever the sample happened to reach.
        values[0] = 0.0;
        values[1] = decimalLike ? top / 100.0 : top;
        return values;
    }

    /// <summary>A random value in the low <paramref name="bits"/> bits, built 30 at a time.</summary>
    private static long NextValueOfWidth(Random random, int bits)
    {
        long value = 0;
        for (int taken = 0; taken < bits; taken += 30)
            value = (value << 30) | (uint)random.Next(1 << 30);
        return value & ((1L << bits) - 1);
    }

    private static void ValidateEveryVectorBitWidth(byte[] page, int expected)
    {
        const int PageHeaderSize = 7;
        const int AlpInfoSize = 4;
        const int DoubleForInfoSize = 9;

        int vectorSize = 1 << page[2];
        int numElements = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(3, 4));
        int numVectors = (numElements + vectorSize - 1) / vectorSize;

        for (int v = 0; v < numVectors; v++)
        {
            int vector = PageHeaderSize +
                (int)BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(PageHeaderSize + v * 4, 4));
            int width = page[vector + AlpInfoSize + DoubleForInfoSize - 1];

            if (width != expected)
            {
                throw new InvalidOperationException(
                    $"aimed for bit width {expected} but vector {v} of {numVectors} came out at {width}");
            }
        }
    }
}
