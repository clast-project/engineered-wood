// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="ByteArrayBounds"/> against comparing every value byte by byte, positions and all (ties keep
/// the first). The shapes aim at what the 8-byte key could get wrong: values ending inside it next to
/// values holding zero bytes there, empty values, long shared prefixes, ties, and values at the very end
/// of the data.
/// </summary>
public class ByteArrayBoundsTests
{
    public static TheoryData<int> Shapes()
    {
        var data = new TheoryData<int>();
        for (int shape = 0; shape < 6; shape++)
            data.Add(shape);
        return data;
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void MatchesByteComparison(int shape)
    {
        var random = new Random(shape);
        foreach (int count in new[] { 1, 2, 3, 10, 100, 2000 })
        {
            for (int round = 0; round < 30; round++)
            {
                var values = Enumerable.Range(0, count).Select(_ => Value(random, shape)).ToArray();
                var (offsets, data) = Pack(values);
                AssertMatches(offsets, data, null, 0, count);
                AssertMatches(offsets, data, Levels(random, count), 0, count);
                if (count > 2)
                    AssertMatches(offsets, data, null, 1, count - 1);
            }
        }
    }

    [Fact]
    public void ZeroBytesAgainstAShorterValue()
    {
        // "a" < "a\0" < "a\0\0" < "a\x01": the key zero-fills "a", so keys tie and bytes must decide.
        byte[][] values = [[0x61, 0x00], [0x61, 0x01], [0x61], [0x61, 0x00, 0x00], [0x61, 0x00]];
        var (offsets, data) = Pack(values);
        Assert.Equal((2, 1), ByteArrayBounds.Find(offsets, data, null, 0, values.Length));
    }

    [Fact]
    public void EmptyValueIsTheLeast()
    {
        byte[][] values = [[0x10], [], [0x00], []];
        var (offsets, data) = Pack(values);
        Assert.Equal((1, 0), ByteArrayBounds.Find(offsets, data, null, 0, values.Length));
    }

    [Fact]
    public void AllNull_HasNoBounds()
    {
        var (offsets, data) = Pack([[1], [2], [3]]);
        Assert.Equal((-1, -1), ByteArrayBounds.Find(offsets, data, new int[3], 0, 3));
        Assert.Equal((-1, -1), ByteArrayBounds.Find(offsets, data, null, 2, 2));
    }

    private static void AssertMatches(int[] offsets, byte[] data, int[]? levels, int start, int end)
    {
        int minIdx = -1, maxIdx = -1;
        for (int i = start; i < end; i++)
        {
            if (levels != null && levels[i] == 0)
                continue;
            if (minIdx < 0)
            {
                minIdx = maxIdx = i;
                continue;
            }

            var value = Slice(offsets, data, i);
            if (value.SequenceCompareTo(Slice(offsets, data, minIdx)) < 0) minIdx = i;
            if (value.SequenceCompareTo(Slice(offsets, data, maxIdx)) > 0) maxIdx = i;
        }

        Assert.Equal((minIdx, maxIdx), ByteArrayBounds.Find(offsets, data, levels, start, end));
    }

    private static ReadOnlySpan<byte> Slice(int[] offsets, byte[] data, int i) =>
        data.AsSpan(offsets[i], offsets[i + 1] - offsets[i]);

    /// <summary>
    /// One value of a shape: 0 random bytes of length 0-20; 1 a small alphabet (0x00, 0x01, 0xFF) of
    /// length 0-12, so prefixes, zero bytes and ties abound; 2 a shared 30-byte prefix and a short random
    /// tail; 3 a shared prefix of random length 0-20 and a tail from the small alphabet; 4 lengths 7-9
    /// around the key's width; 5 few distinct values.
    /// </summary>
    private static byte[] Value(Random random, int shape)
    {
        byte[] Bytes(int n, bool small)
        {
            var b = new byte[n];
            if (small)
            {
                for (int i = 0; i < n; i++)
                    b[i] = (byte)(random.Next(3) switch { 0 => 0x00, 1 => 0x01, _ => 0xFF });
            }
            else
            {
                random.NextBytes(b);
            }

            return b;
        }

        byte[] prefix = "https://example.com/catalog/i/"u8.ToArray();
        return shape switch
        {
            0 => Bytes(random.Next(21), small: false),
            1 => Bytes(random.Next(13), small: true),
            2 => [.. prefix, .. Bytes(random.Next(10), small: false)],
            3 => [.. prefix.AsSpan(0, random.Next(21)).ToArray(), .. Bytes(random.Next(10), small: true)],
            4 => Bytes(7 + random.Next(3), small: true),
            _ => new[] { "b"u8.ToArray(), "a"u8.ToArray(), "ab"u8.ToArray() }[random.Next(3)],
        };
    }

    /// <summary>
    /// The values end to end, nothing after the last, so its key is read from the end of the data.
    /// </summary>
    private static (int[] Offsets, byte[] Data) Pack(byte[][] values)
    {
        var offsets = new int[values.Length + 1];
        for (int i = 0; i < values.Length; i++)
            offsets[i + 1] = offsets[i] + values[i].Length;
        var data = new byte[offsets[values.Length]];
        for (int i = 0; i < values.Length; i++)
            values[i].CopyTo(data, offsets[i]);
        return (offsets, data);
    }

    private static int[] Levels(Random random, int count)
    {
        double share = random.Next(4) switch { 0 => 1.0, 1 => 0.9, _ => random.NextDouble() };
        var levels = new int[count];
        for (int i = 0; i < count; i++)
            levels[i] = random.NextDouble() < share ? 0 : 1;
        return levels;
    }
}
