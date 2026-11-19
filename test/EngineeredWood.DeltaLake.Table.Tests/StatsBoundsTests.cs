// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.Stats;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="StatsBounds"/> against reading every value through <c>IsNull</c> / <c>GetValue</c>, in Delta's order
/// (NaN the greatest). The arrays are sliced at an offset, run either side of the 4,096-row chunk the validity is
/// expanded in, and hold nulls at random, in whole chunks, or everywhere.
/// </summary>
public class StatsBoundsTests
{
    public static TheoryData<int, int> Layouts()
    {
        // (length, null pattern): 0 none, 1 random 14%, 2 the first chunk and a run in the middle all null,
        // 3 every value null.
        var data = new TheoryData<int, int>();
        foreach (int length in new[] { 1, 2, 31, 4095, 4096, 4097, 9001 })
        {
            for (int nulls = 0; nulls < 4; nulls++)
                data.Add(length, nulls);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void Int64_MatchesPerValue(int length, int nulls)
    {
        var random = new Random(length * 4 + nulls);
        var builder = new Int64Array.Builder();
        foreach (bool present in Pattern(random, length + 8, nulls))
        {
            if (present) builder.Append(NextInt64(random)); else builder.AppendNull();
        }

        var array = (Int64Array)Slice(builder.Build(), length);
        var present64 = Present(array, i => array.GetValue(i)!.Value);
        Assert.Equal(present64.Count > 0, StatsBounds.TryInt64(array, out long min, out long max));
        if (present64.Count > 0)
            Assert.Equal((present64.Min(), present64.Max()), (min, max));
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void Int32_MatchesPerValue(int length, int nulls)
    {
        var random = new Random(100 + length * 4 + nulls);
        var builder = new Int32Array.Builder();
        foreach (bool present in Pattern(random, length + 8, nulls))
        {
            if (present) builder.Append(random.Next(int.MinValue, int.MaxValue)); else builder.AppendNull();
        }

        var array = (Int32Array)Slice(builder.Build(), length);
        var values = Present(array, i => array.GetValue(i)!.Value);
        Assert.Equal(values.Count > 0, StatsBounds.TryInt32(array, out int min, out int max));
        if (values.Count > 0)
            Assert.Equal((values.Min(), values.Max()), (min, max));
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void Double_MatchesDeltaOrder(int length, int nulls)
    {
        var random = new Random(200 + length * 4 + nulls);
        double[] specials = [double.NaN, 0.0, -0.0, double.PositiveInfinity, double.NegativeInfinity, double.MaxValue, -double.Epsilon];
        var builder = new DoubleArray.Builder();
        foreach (bool present in Pattern(random, length + 8, nulls))
        {
            if (!present) builder.AppendNull();
            else builder.Append(random.Next(8) == 0 ? specials[random.Next(specials.Length)] : random.NextDouble() - 0.5);
        }

        var array = (DoubleArray)Slice(builder.Build(), length);
        var values = Present(array, i => array.GetValue(i)!.Value);
        Assert.Equal(values.Count > 0, StatsBounds.TryDouble(array, out double min, out double max));
        if (values.Count == 0)
            return;

        var (expectedMin, expectedMax) = DeltaBounds(values);
        Assert.Equal(BitConverter.DoubleToInt64Bits(expectedMin), BitConverter.DoubleToInt64Bits(min));
        Assert.Equal(BitConverter.DoubleToInt64Bits(expectedMax), BitConverter.DoubleToInt64Bits(max));
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void Float_MatchesDeltaOrder(int length, int nulls)
    {
        var random = new Random(300 + length * 4 + nulls);
        float[] specials = [float.NaN, 0f, -0f, float.PositiveInfinity, float.NegativeInfinity, float.MaxValue];
        var builder = new FloatArray.Builder();
        foreach (bool present in Pattern(random, length + 8, nulls))
        {
            if (!present) builder.AppendNull();
            else builder.Append(random.Next(8) == 0 ? specials[random.Next(specials.Length)] : (float)(random.NextDouble() - 0.5));
        }

        var array = (FloatArray)Slice(builder.Build(), length);
        var values = Present(array, i => (double)array.GetValue(i)!.Value);
        Assert.Equal(values.Count > 0, StatsBounds.TryFloat(array, out float min, out float max));
        if (values.Count == 0)
            return;

        var (expectedMin, expectedMax) = DeltaBounds(values);
        Assert.Equal(BitConverter.DoubleToInt64Bits(expectedMin), BitConverter.DoubleToInt64Bits(min));
        Assert.Equal(BitConverter.DoubleToInt64Bits(expectedMax), BitConverter.DoubleToInt64Bits(max));
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void String_MatchesPerValue(int length, int nulls)
    {
        var random = new Random(400 + length * 4 + nulls);
        string[] pool = ["", "a", "a\0", "ab", "b", "https://x/1", "https://x/2", "zz"];
        var builder = new StringArray.Builder();
        foreach (bool present in Pattern(random, length + 8, nulls))
        {
            if (!present) builder.AppendNull();
            else builder.Append(random.Next(3) == 0 ? pool[random.Next(pool.Length)] : RandomText(random));
        }

        var array = (StringArray)Slice(builder.Build(), length);
        int minIdx = -1, maxIdx = -1;
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            if (minIdx < 0) { minIdx = maxIdx = i; continue; }
            if (array.GetBytes(i).SequenceCompareTo(array.GetBytes(minIdx)) < 0) minIdx = i;
            if (array.GetBytes(i).SequenceCompareTo(array.GetBytes(maxIdx)) > 0) maxIdx = i;
        }

        Assert.Equal((minIdx, maxIdx), StatsBounds.FindString(array));
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void Decimal128_MatchesPerValue(int length, int nulls)
    {
        var random = new Random(500 + length * 4 + nulls);
        var builder = new Decimal128Array.Builder(new Decimal128Type(38, 2));
        foreach (bool present in Pattern(random, length + 8, nulls))
        {
            if (!present) builder.AppendNull();
            else builder.Append(decimal.Round(random.Next(4) switch
            {
                0 => decimal.MaxValue / 1000m,
                1 => -decimal.MaxValue / 1000m,
                2 => random.Next(-3, 4) / 100m, // small numbers either side of zero: high words all sign
                _ => (decimal)NextInt64(random) / 100m,
            }, 2)); // the column's scale
        }

        var array = (Decimal128Array)Slice(builder.Build(), length);
        int minIdx = -1, maxIdx = -1;
        BigInteger min = 0, max = 0;
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            var v = new BigInteger(array.ValueBuffer.Span.Slice((array.Data.Offset + i) * 16, 16).ToArray());
            if (minIdx < 0) { minIdx = maxIdx = i; min = max = v; continue; }
            if (v < min) { minIdx = i; min = v; }
            if (v > max) { maxIdx = i; max = v; }
        }

        Assert.Equal((minIdx, maxIdx), StatsBounds.FindDecimal128(array));
    }

    [Fact]
    public void Double_BothZeros_LeastIsNegativeGreatestPositive()
    {
        var array = new DoubleArray.Builder().Append(0.0).Append(-0.0).Append(0.0).Build();
        Assert.True(StatsBounds.TryDouble(array, out double min, out double max));
        Assert.Equal(BitConverter.DoubleToInt64Bits(-0.0), BitConverter.DoubleToInt64Bits(min));
        Assert.Equal(BitConverter.DoubleToInt64Bits(0.0), BitConverter.DoubleToInt64Bits(max));
    }

    /// <summary>
    /// Delta's bounds: NaN is the greatest (so the least is NaN only when every value is), and of two zeros the
    /// least is -0 and the greatest +0.
    /// </summary>
    private static (double Min, double Max) DeltaBounds(List<double> values)
    {
        var numbers = values.Where(v => !double.IsNaN(v)).ToList();
        if (numbers.Count == 0)
            return (double.NaN, double.NaN);

        double min = numbers.Min(), max = numbers.Max();
        if (min == 0 && numbers.Any(v => v == 0 && IsNegative(v))) min = -0.0;
        if (max == 0 && numbers.Any(v => v == 0 && !IsNegative(v))) max = 0.0;
        if (min == 0 && !numbers.Any(v => v == 0 && IsNegative(v))) min = 0.0;
        if (max == 0 && !numbers.Any(v => v == 0 && !IsNegative(v))) max = -0.0;
        return (min, values.Count > numbers.Count ? double.NaN : max);
    }

    private static List<T> Present<T>(IArrowArray array, Func<int, T> get)
    {
        var values = new List<T>();
        for (int i = 0; i < array.Length; i++)
        {
            if (!array.IsNull(i))
                values.Add(get(i));
        }

        return values;
    }

    /// <summary>The middle <paramref name="length"/> values of an array 8 longer: an offset of 3.</summary>
    private static IArrowArray Slice(IArrowArray array, int length) => ArrowArrayFactory.Slice(array, 3, length);

    private static IEnumerable<bool> Pattern(Random random, int count, int nulls)
    {
        for (int i = 0; i < count; i++)
        {
            yield return nulls switch
            {
                0 => true,
                1 => random.Next(7) != 0,
                // Offset 3, so the slice's first chunk is rows 3..4098 of the array.
                2 => !(i < 4099 || (i >= 6000 && i < 7000)),
                _ => false,
            };
        }
    }

    private static long NextInt64(Random random)
    {
        var bytes = new byte[8];
        random.NextBytes(bytes);
        return BitConverter.ToInt64(bytes, 0);
    }

    private static bool IsNegative(double v) => BitConverter.DoubleToInt64Bits(v) < 0;

    private static string RandomText(Random random)
    {
        var chars = new char[random.Next(0, 20)];
        for (int i = 0; i < chars.Length; i++)
            chars[i] = (char)('a' + random.Next(26));
        return new string(chars);
    }
}
