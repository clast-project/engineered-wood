// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="FloatBounds"/> against the scan it replaces: numeric order, NaN skipped and counted, -0
/// taken over +0 for the least and +0 over -0 for the greatest. Compared bit for bit, over values drawn
/// heavily from the special cases, at every length across the vector width and its tail.
/// </summary>
public class FloatBoundsTests
{
    private static readonly int[] SpecialFloats =
    [
        0x0000_0000, unchecked((int)0x8000_0000),             // +0, -0
        0x7F80_0000, unchecked((int)0xFF80_0000),             // +inf, -inf
        0x7FC0_0000, unchecked((int)0xFFC0_0000),             // quiet NaN, negative quiet NaN
        0x7F80_0001, unchecked((int)0xFF80_0001), 0x7FFF_FFFF, unchecked((int)0xFFFF_FFFF), // signaling, extreme NaNs
        0x0000_0001, unchecked((int)0x8000_0001),             // smallest subnormals
        0x7F7F_FFFF, unchecked((int)0xFF7F_FFFF),             // +-MaxValue
        0x3F80_0000, unchecked((int)0xBF80_0000),             // +-1
    ];

    private static readonly long[] SpecialDoubles =
    [
        0, long.MinValue,
        0x7FF0_0000_0000_0000, unchecked((long)0xFFF0_0000_0000_0000),
        0x7FF8_0000_0000_0000, unchecked((long)0xFFF8_0000_0000_0000),
        0x7FF0_0000_0000_0001, unchecked((long)0xFFF0_0000_0000_0001), long.MaxValue, -1,
        1, unchecked((long)0x8000_0000_0000_0001),
        0x7FEF_FFFF_FFFF_FFFF, unchecked((long)0xFFEF_FFFF_FFFF_FFFF),
        0x3FF0_0000_0000_0000, unchecked((long)0xBFF0_0000_0000_0000),
    ];

    public static TheoryData<int> Mixes() => new() { 0, 1, 2, 3 };

    [Theory]
    [MemberData(nameof(Mixes))]
    public void Float_EveryLength_MatchesTheScan(int mix)
    {
        var random = new Random(mix);
        for (int length = 0; length <= 80; length++)
        {
            for (int round = 0; round < 30; round++)
                AssertFloat(Floats(random, length, mix));
        }

        AssertFloat(Floats(random, 5000, mix));
    }

    [Theory]
    [MemberData(nameof(Mixes))]
    public void Double_EveryLength_MatchesTheScan(int mix)
    {
        var random = new Random(10 + mix);
        for (int length = 0; length <= 80; length++)
        {
            for (int round = 0; round < 30; round++)
                AssertDouble(Doubles(random, length, mix));
        }

        AssertDouble(Doubles(random, 5000, mix));
    }

    [Fact]
    public void SignedZeros_LeastIsNegative_GreatestIsPositive()
    {
        foreach (int length in new[] { 2, 9, 33 })
        {
            var floats = new float[length];
            floats[length / 2] = -0f;
            Assert.True(FloatBounds.TryFloat(floats, out int min, out int max, out _));
            Assert.Equal(unchecked((int)0x8000_0000), min);
            Assert.Equal(0, max);

            var doubles = new double[length];
            doubles[length - 1] = -0d;
            Assert.True(FloatBounds.TryDouble(doubles, out long dmin, out long dmax, out _));
            Assert.Equal(long.MinValue, dmin);
            Assert.Equal(0L, dmax);
        }
    }

    [Fact]
    public void AllNaN_OrEmpty_LeavesTheBoundsToTheCaller()
    {
        Assert.False(FloatBounds.TryFloat(Enumerable.Repeat(float.NaN, 40).ToArray(), out _, out _, out long count));
        Assert.Equal(40, count);
        Assert.False(FloatBounds.TryDouble(Enumerable.Repeat(double.NaN, 40).ToArray(), out _, out _, out count));
        Assert.Equal(40, count);
        Assert.False(FloatBounds.TryFloat([], out _, out _, out count));
        Assert.Equal(0, count);
    }

    private static void AssertFloat(float[] values)
    {
        var (found, min, max, nans) = ScanFloat(values);
        Assert.Equal(found, FloatBounds.TryFloat(values, out int minBits, out int maxBits, out long count));
        Assert.Equal(nans, count);
        if (found)
        {
            Assert.Equal(BitConverter.ToInt32(BitConverter.GetBytes(min), 0), minBits);
            Assert.Equal(BitConverter.ToInt32(BitConverter.GetBytes(max), 0), maxBits);
        }
    }

    private static void AssertDouble(double[] values)
    {
        var (found, min, max, nans) = ScanDouble(values);
        Assert.Equal(found, FloatBounds.TryDouble(values, out long minBits, out long maxBits, out long count));
        Assert.Equal(nans, count);
        if (found)
        {
            Assert.Equal(BitConverter.DoubleToInt64Bits(min), minBits);
            Assert.Equal(BitConverter.DoubleToInt64Bits(max), maxBits);
        }
    }

    /// <summary>The scan as StatisticsCollector wrote it before FloatBounds.</summary>
    private static (bool Found, float Min, float Max, long NaNs) ScanFloat(float[] values)
    {
        bool first = true;
        float min = 0, max = 0;
        long nans = 0;
        foreach (float v in values)
        {
            if (float.IsNaN(v)) { nans++; continue; }
            if (first) { min = max = v; first = false; continue; }
            if (v < min || (v == 0f && min == 0f && IsNegative(v))) min = v;
            if (v > max || (v == 0f && max == 0f && !IsNegative(v))) max = v;
        }

        return (!first, min, max, nans);
    }

    private static (bool Found, double Min, double Max, long NaNs) ScanDouble(double[] values)
    {
        bool first = true;
        double min = 0, max = 0;
        long nans = 0;
        foreach (double v in values)
        {
            if (double.IsNaN(v)) { nans++; continue; }
            if (first) { min = max = v; first = false; continue; }
            if (v < min || (v == 0d && min == 0d && BitConverter.DoubleToInt64Bits(v) < 0)) min = v;
            if (v > max || (v == 0d && max == 0d && BitConverter.DoubleToInt64Bits(v) >= 0)) max = v;
        }

        return (!first, min, max, nans);
    }

    private static bool IsNegative(float v) => BitConverter.ToInt32(BitConverter.GetBytes(v), 0) < 0;

    /// <summary>
    /// Values of one mix: 0 random bits (a few NaNs); 1 mostly special values; 2 small numbers around zero
    /// with both zeros and NaNs; 3 mostly NaN, sometimes all.
    /// </summary>
    private static float[] Floats(Random random, int length, int mix)
    {
        var values = new float[length];
        var bytes = new byte[4];
        for (int i = 0; i < length; i++)
        {
            random.NextBytes(bytes);
            int bits = BitConverter.ToInt32(bytes, 0);
            bits = mix switch
            {
                0 => bits,
                1 => random.Next(4) == 0 ? bits : SpecialFloats[random.Next(SpecialFloats.Length)],
                2 => random.Next(5) switch
                {
                    0 => 0,
                    1 => unchecked((int)0x8000_0000),
                    2 => 0x7FC0_0000,
                    _ => BitConverter.ToInt32(BitConverter.GetBytes((float)(random.NextDouble() - 0.5)), 0),
                },
                _ => random.Next(10) == 0 ? SpecialFloats[random.Next(SpecialFloats.Length)] : 0x7FC0_0001,
            };
            values[i] = BitConverter.ToSingle(BitConverter.GetBytes(bits), 0);
        }

        return values;
    }

    private static double[] Doubles(Random random, int length, int mix)
    {
        var values = new double[length];
        var bytes = new byte[8];
        for (int i = 0; i < length; i++)
        {
            random.NextBytes(bytes);
            long bits = BitConverter.ToInt64(bytes, 0);
            bits = mix switch
            {
                0 => bits,
                1 => random.Next(4) == 0 ? bits : SpecialDoubles[random.Next(SpecialDoubles.Length)],
                2 => random.Next(5) switch
                {
                    0 => 0,
                    1 => long.MinValue,
                    2 => 0x7FF8_0000_0000_0000,
                    _ => BitConverter.DoubleToInt64Bits(random.NextDouble() - 0.5),
                },
                _ => random.Next(10) == 0 ? SpecialDoubles[random.Next(SpecialDoubles.Length)] : 0x7FF8_0000_0000_0001,
            };
            values[i] = BitConverter.Int64BitsToDouble(bits);
        }

        return values;
    }
}
