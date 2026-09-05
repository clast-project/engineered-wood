// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The bulk group-of-eight path in <see cref="RleBitPackedDecoder"/>. Its correctness turns on three
/// things the surrounding tests only reach incidentally: that it declines when the run is not sitting
/// on a group boundary, that it declines near the end of the buffer where its trailing read would
/// overrun, and that it produces the same values as the per-value loop at every bit width.
/// </summary>
public class RleBitPackedBulkTests
{
    [Fact]
    public void ReadBatch_EveryBitWidthAndLength_RoundTrips()
    {
        ulong state = 20260825;

        for (int bitWidth = 1; bitWidth <= 32; bitWidth++)
        {
            foreach (int count in new[] { 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 1000, 1024, 4096 })
            {
                var values = RandomValues(ref state, count, bitWidth);
                var encoded = Encode(values, bitWidth);

                var actual = new int[count];
                var decoder = new RleBitPackedDecoder(encoded, bitWidth);
                decoder.ReadBatch(actual);

                Assert.True(
                    actual.AsSpan().SequenceEqual(values),
                    $"bitWidth={bitWidth} count={count}");
            }
        }
    }

    [Fact]
    public void ReadBatch_InChunksThatStraddleGroups_RoundTrips()
    {
        // Two cases, and the chunk sizes name which is which. The odd ones are coprime with eight,
        // so a call ends part-way through a group and the next one has to decline the bulk path and
        // hand back to the per-value loop; eight and sixteen land on a group boundary, where it has
        // to stay engaged across the call instead. Getting either wrong corrupts silently.
        ulong state = 777;

        foreach (int chunk in new[] { 1, 3, 5, 7, 11, 13, 8, 16 })
        {
            for (int bitWidth = 1; bitWidth <= 32; bitWidth += 3)
            {
                const int Count = 2000;
                var values = RandomValues(ref state, Count, bitWidth);
                var encoded = Encode(values, bitWidth);

                var actual = new int[Count];
                var decoder = new RleBitPackedDecoder(encoded, bitWidth);
                for (int offset = 0; offset < Count; offset += chunk)
                {
                    int take = Math.Min(chunk, Count - offset);
                    decoder.ReadBatch(actual.AsSpan(offset, take));
                }

                Assert.True(
                    actual.AsSpan().SequenceEqual(values),
                    $"chunk={chunk} bitWidth={bitWidth}");
            }
        }
    }

    [Fact]
    public void ReadBatch_MixedRleAndBitPackedRuns_RoundTrips()
    {
        // Long stretches of one value become RLE runs, so the decoder alternates between the two
        // modes and the bulk path has to pick up correctly after each RLE run.
        ulong state = 31337;

        for (int bitWidth = 2; bitWidth <= 24; bitWidth += 2)
        {
            var values = new List<int>();
            int mask = (1 << bitWidth) - 1;
            for (int block = 0; block < 40; block++)
            {
                if (block % 2 == 0)
                {
                    int repeated = (int)(NextBits(ref state) & (ulong)mask);
                    values.AddRange(Enumerable.Repeat(repeated, 50 + (block * 3)));
                }
                else
                {
                    for (int i = 0; i < 37 + block; i++)
                        values.Add((int)(NextBits(ref state) & (ulong)mask));
                }
            }

            var expected = values.ToArray();
            var encoded = Encode(expected, bitWidth);

            var actual = new int[expected.Length];
            var decoder = new RleBitPackedDecoder(encoded, bitWidth);
            decoder.ReadBatch(actual);

            Assert.True(actual.AsSpan().SequenceEqual(expected), $"bitWidth={bitWidth}");
        }
    }

    [Fact]
    public void ReadBatch_CountingOverload_AgreesWithASeparatePass()
    {
        // The counting overloads unpack in bulk and count afterwards rather than comparing inside
        // the kernel, so the count has to be checked independently of the values.
        ulong state = 4242;

        for (int bitWidth = 1; bitWidth <= 16; bitWidth++)
        {
            const int Count = 3000;
            var values = RandomValues(ref state, Count, bitWidth);
            var encoded = Encode(values, bitWidth);
            int matchValue = values[Count / 2];

            var actual = new int[Count];
            var decoder = new RleBitPackedDecoder(encoded, bitWidth);
            decoder.ReadBatch(actual, matchValue, out int matchCount);

            Assert.True(actual.AsSpan().SequenceEqual(values), $"bitWidth={bitWidth}: values");
            Assert.Equal(values.Count(v => v == matchValue), matchCount);
        }
    }

    [Fact]
    public void ReadBatch_ByteOverload_MatchesTheIntOverload()
    {
        ulong state = 99;

        for (int bitWidth = 1; bitWidth <= 8; bitWidth++)
        {
            const int Count = 2500;
            var values = RandomValues(ref state, Count, bitWidth);
            var encoded = Encode(values, bitWidth);

            var asBytes = new byte[Count];
            var byteDecoder = new RleBitPackedDecoder(encoded, bitWidth);
            byteDecoder.ReadBatch(asBytes);

            for (int i = 0; i < Count; i++)
                Assert.True(asBytes[i] == values[i], $"bitWidth={bitWidth} index={i}");
        }
    }

    private static int[] RandomValues(ref ulong state, int count, int bitWidth)
    {
        ulong mask = bitWidth == 32 ? uint.MaxValue : (1UL << bitWidth) - 1UL;
        var values = new int[count];
        for (int i = 0; i < count; i++)
            values[i] = (int)(NextBits(ref state) & mask);
        return values;
    }

    private static byte[] Encode(int[] values, int bitWidth)
    {
        var encoder = new RleBitPackedEncoder(bitWidth, (values.Length * bitWidth / 8) + 1024);
        encoder.Encode(values);
        return encoder.ToArray();
    }

    /// <summary>SplitMix64: a bit source that behaves identically on every target framework.</summary>
    private static ulong NextBits(ref ulong state)
    {
        state = unchecked(state + 0x9E3779B97F4A7C15UL);
        ulong z = state;
        z = unchecked((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL);
        z = unchecked((z ^ (z >> 27)) * 0x94D049BB133111EBUL);
        return z ^ (z >> 31);
    }
}
