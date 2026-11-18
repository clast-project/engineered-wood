// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="FixedLenBounds"/> against comparing the bytes: unsigned lexicographic, or for a signed
/// big-endian integer the first byte signed and the rest unsigned. Positions must match, ties
/// keeping the first.
/// </summary>
public class FixedLenBoundsTests
{
    public static TheoryData<int, bool> LengthsAndOrders()
    {
        var data = new TheoryData<int, bool>();
        for (int length = 1; length <= FixedLenBounds.MaxLength; length++)
        {
            data.Add(length, false);
            data.Add(length, true);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(LengthsAndOrders))]
    public void RandomBytes_MatchByteComparison(int length, bool signed)
    {
        var random = new Random(length * 2 + (signed ? 1 : 0));
        foreach (int count in new[] { 1, 2, 3, 17, 200 })
        {
            for (int round = 0; round < 20; round++)
            {
                byte[] buffer = Values(random, count, length, ShapeOf(round));
                AssertMatches(buffer, length, null, signed, 0, count);
                AssertMatches(buffer, length, Levels(random, count), signed, 0, count);
                if (count > 2)
                    AssertMatches(buffer, length, null, signed, 1, count - 1);
            }
        }
    }

    [Theory]
    [MemberData(nameof(LengthsAndOrders))]
    public void Extremes_AtEveryPosition(int length, bool signed)
    {
        // Each extreme alone among copies of a middling value: the least and greatest signed (0x80 00..,
        // 0x7F FF..), and unsigned (00.., FF..), which are -1 and 0 signed.
        byte[][] extremes =
        [
            [0x80, .. new byte[length - 1]],
            [0x7F, .. Enumerable.Repeat((byte)0xFF, length - 1)],
            new byte[length],
            Enumerable.Repeat((byte)0xFF, length).ToArray(),
        ];
        byte[] middle = Enumerable.Range(0, length).Select(i => (byte)(0x40 + i)).ToArray();

        for (int at = 0; at < 20; at++)
        {
            foreach (var extreme in extremes)
            {
                var buffer = new byte[20 * length];
                for (int i = 0; i < 20; i++)
                    (i == at ? extreme : middle).CopyTo(buffer, i * length);
                AssertMatches(buffer, length, null, signed, 0, 20);
            }
        }
    }

    [Fact]
    public void Ties_KeepTheFirstPosition()
    {
        var buffer = new byte[6 * 4];
        new byte[] { 1, 2, 3, 4 }.CopyTo(buffer, 0);
        new byte[] { 0, 0, 0, 1 }.CopyTo(buffer, 4);
        new byte[] { 9, 9, 9, 9 }.CopyTo(buffer, 8);
        new byte[] { 0, 0, 0, 1 }.CopyTo(buffer, 12);
        new byte[] { 9, 9, 9, 9 }.CopyTo(buffer, 16);
        new byte[] { 1, 2, 3, 4 }.CopyTo(buffer, 20);

        Assert.Equal((1, 2), FixedLenBounds.Find(buffer, 4, null, signed: false, 0, 6));
    }

    [Fact]
    public void AllNull_HasNoBounds()
    {
        Assert.Equal((-1, -1), FixedLenBounds.Find(new byte[160], 16, new int[10], signed: true, 0, 10));
        Assert.Equal((-1, -1), FixedLenBounds.Find(new byte[160], 16, null, signed: true, 5, 5));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(17)]
    public void LengthOutsideOneToSixteen_IsRefused(int length) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => FixedLenBounds.Find(new byte[64], length, null, false, 0, 1));

    private static void AssertMatches(byte[] buffer, int length, int[]? levels, bool signed, int start, int end)
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

            if (Compare(buffer, length, i, minIdx, signed) < 0) minIdx = i;
            if (Compare(buffer, length, i, maxIdx, signed) > 0) maxIdx = i;
        }

        Assert.Equal((minIdx, maxIdx), FixedLenBounds.Find(buffer, length, levels, signed, start, end));
    }

    private static int Compare(byte[] buffer, int length, int a, int b, bool signed)
    {
        var left = buffer.AsSpan(a * length, length);
        var right = buffer.AsSpan(b * length, length);
        if (signed)
        {
            int bySign = ((sbyte)left[0]).CompareTo((sbyte)right[0]);
            if (bySign != 0)
                return bySign;
            return left.Slice(1).SequenceCompareTo(right.Slice(1));
        }

        return left.SequenceCompareTo(right);
    }

    private enum Shape { Random, SignPadded, FewDistinct }

    private static Shape ShapeOf(int round) => (Shape)(round % 3);

    /// <summary>
    /// Values of one shape: random bytes; decimal-like, small numbers of either sign padded with 0x00 or
    /// 0xFF, so most comparisons run past the first word; or a few distinct values, so ties are common.
    /// </summary>
    private static byte[] Values(Random random, int count, int length, Shape shape)
    {
        var buffer = new byte[count * length];
        random.NextBytes(buffer);
        if (shape == Shape.SignPadded)
        {
            for (int i = 0; i < count; i++)
            {
                byte pad = random.Next(2) == 0 ? (byte)0x00 : (byte)0xFF;
                int keep = random.Next(1, 4);
                for (int b = 0; b < length - keep; b++)
                    buffer[i * length + b] = pad;
            }
        }
        else if (shape == Shape.FewDistinct)
        {
            for (int i = 0; i < count; i++)
                Array.Copy(buffer, random.Next(Math.Min(3, count)) * length, buffer, i * length, length);
        }

        return buffer;
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
