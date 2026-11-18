// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="IntegerBounds"/> against the plain answer (LINQ over the present values, ordered signed
/// or unsigned), at every length across the vector width and its tail, with and without nulls.
/// </summary>
public class IntegerBoundsTests
{
    public static TheoryData<bool, bool> Orders() => new()
    {
        { false, false }, { false, true }, { true, false }, { true, true }, // (unsigned, nullable)
    };

    [Theory]
    [MemberData(nameof(Orders))]
    public void Int32_EveryLength_MatchesLinq(bool unsigned, bool nullable)
    {
        var random = new Random(unsigned ? 1 : 2);
        for (int length = 0; length <= 80; length++)
        {
            for (int round = 0; round < 20; round++)
                AssertInt32(Int32s(random, length), Levels(random, length, nullable), unsigned);
        }

        AssertInt32(Int32s(random, 10_000), Levels(random, 10_000, nullable), unsigned);
    }

    [Theory]
    [MemberData(nameof(Orders))]
    public void Int64_EveryLength_MatchesLinq(bool unsigned, bool nullable)
    {
        var random = new Random(unsigned ? 3 : 4);
        for (int length = 0; length <= 80; length++)
        {
            for (int round = 0; round < 20; round++)
                AssertInt64(Int64s(random, length), Levels(random, length, nullable), unsigned);
        }

        AssertInt64(Int64s(random, 10_000), Levels(random, 10_000, nullable), unsigned);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Extremes_OrderBySign(bool unsigned)
    {
        // Signed: MinValue is least and MaxValue greatest. Unsigned: 0 is least and -1 (all ones)
        // greatest. Each extreme sits once among 100 middling values, at every position.
        for (int at = 0; at < 100; at++)
        {
            foreach (int extreme in new[] { int.MinValue, int.MaxValue, 0, -1 })
            {
                var values = Enumerable.Repeat(12345, 100).ToArray();
                values[at] = extreme;
                AssertInt32(values, [], unsigned);
            }

            foreach (long extreme in new[] { long.MinValue, long.MaxValue, 0L, -1L })
            {
                var values = Enumerable.Repeat(1_234_567_890_123L, 100).ToArray();
                values[at] = extreme;
                AssertInt64(values, [], unsigned);
            }
        }
    }

    [Fact]
    public void NullsAroundTheOnlyPresentValue()
    {
        // One present value at each position, every other value an extreme that must not count.
        for (int at = 0; at < 70; at++)
        {
            var ints = Enumerable.Repeat(int.MinValue, 70).ToArray();
            var longs = Enumerable.Repeat(long.MaxValue, 70).ToArray();
            ints[at] = 7;
            longs[at] = 7;
            var levels = new int[70];
            levels[at] = 1;

            Assert.True(IntegerBounds.TryInt32(ints, levels, false, out int min, out int max));
            Assert.Equal((7, 7), (min, max));
            Assert.True(IntegerBounds.TryInt64(longs, levels, false, out long lmin, out long lmax));
            Assert.Equal((7L, 7L), (lmin, lmax));
        }
    }

    [Fact]
    public void AllNull_HasNoBounds()
    {
        Assert.False(IntegerBounds.TryInt32(new int[50], new int[50], false, out _, out _));
        Assert.False(IntegerBounds.TryInt64(new long[50], new int[50], true, out _, out _));
        Assert.False(IntegerBounds.TryInt32([], [], false, out _, out _));
    }

    [Fact]
    public void FewerLevelsThanValues_IsRefused()
    {
        Assert.Throws<ArgumentException>(() => IntegerBounds.TryInt32(new int[40], new int[39], false, out _, out _));
        Assert.Throws<ArgumentException>(() => IntegerBounds.TryInt64(new long[40], new int[39], false, out _, out _));
    }

    private static void AssertInt32(int[] values, int[] levels, bool unsigned)
    {
        var present = values.Where((_, i) => levels.Length == 0 || levels[i] != 0).ToArray();
        bool found = IntegerBounds.TryInt32(values, levels, unsigned, out int min, out int max);
        Assert.Equal(present.Length > 0, found);
        if (!found)
            return;

        if (unsigned)
            Assert.Equal((present.Min(v => (uint)v), present.Max(v => (uint)v)), ((uint)min, (uint)max));
        else
            Assert.Equal((present.Min(), present.Max()), (min, max));
    }

    private static void AssertInt64(long[] values, int[] levels, bool unsigned)
    {
        var present = values.Where((_, i) => levels.Length == 0 || levels[i] != 0).ToArray();
        bool found = IntegerBounds.TryInt64(values, levels, unsigned, out long min, out long max);
        Assert.Equal(present.Length > 0, found);
        if (!found)
            return;

        if (unsigned)
            Assert.Equal((present.Min(v => (ulong)v), present.Max(v => (ulong)v)), ((ulong)min, (ulong)max));
        else
            Assert.Equal((present.Min(), present.Max()), (min, max));
    }

    /// <summary>Values spread over the whole range, both signs, so the orders disagree.</summary>
    private static int[] Int32s(Random random, int length)
    {
        var bytes = new byte[length * 4];
        random.NextBytes(bytes);
        var values = new int[length];
        Buffer.BlockCopy(bytes, 0, values, 0, bytes.Length);
        return values;
    }

    private static long[] Int64s(Random random, int length)
    {
        var bytes = new byte[length * 8];
        random.NextBytes(bytes);
        var values = new long[length];
        Buffer.BlockCopy(bytes, 0, values, 0, bytes.Length);
        return values;
    }

    /// <summary>
    /// No levels, or levels with a null share drawn per stream from none to all, so some streams start
    /// or end with a run of nulls and some are all null.
    /// </summary>
    private static int[] Levels(Random random, int length, bool nullable)
    {
        if (!nullable)
            return [];

        double share = random.Next(5) switch { 0 => 0.0, 1 => 1.0, 2 => 0.9, _ => random.NextDouble() };
        var levels = new int[length];
        for (int i = 0; i < length; i++)
            levels[i] = random.NextDouble() < share ? 0 : 1 + random.Next(3); // any non-zero level is present
        return levels;
    }
}
