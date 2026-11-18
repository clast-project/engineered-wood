// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Tests.Parquet.Data;

/// <summary>
/// The page and chunk bounds of INT32, INT64, FLOAT and DOUBLE columns with nulls, against a reference
/// written from the definition of each order. The collector replaces each null with a value already
/// present (integers) or gathers the present values before scanning (floating point), so these pin
/// what that has to keep:
/// <list type="bullet">
/// <item>a null slot holding NaN, an infinity or an extreme integer must not reach a bound or the NaN count;</item>
/// <item>-0.0 orders below +0.0, and NaN is skipped, or bounds the page under total order when it is all NaN;</item>
/// <item>unsigned order for INT32/INT64.</item>
/// </list>
/// Ranges start part way into the levels, as a page after the first does.
/// </summary>
public class StatisticsNullPatternTests
{
    private const int Rows = 300;

    public static TheoryData<double, bool> FloatingCases()
    {
        var data = new TheoryData<double, bool>();
        foreach (double share in new[] { 0.0, 0.15, 0.5, 0.95, 1.0 })
        foreach (bool totalOrder in new[] { false, true })
            data.Add(share, totalOrder);
        return data;
    }

    public static TheoryData<double, bool> IntegerCases()
    {
        var data = new TheoryData<double, bool>();
        foreach (double share in new[] { 0.0, 0.15, 0.5, 0.95, 1.0 })
        foreach (bool unsigned in new[] { false, true })
            data.Add(share, unsigned);
        return data;
    }

    [Theory]
    [MemberData(nameof(FloatingCases))]
    public void Double_BoundsMatchReference(double nullShare, bool totalOrder)
    {
        for (int seed = 0; seed < 20; seed++)
        {
            var random = new Random(seed);
            var levels = Levels(random, nullShare);
            var bits = new long[Rows];
            for (int i = 0; i < Rows; i++)
            {
                // A null slot holds anything a producer might leave there, NaN included.
                bits[i] = levels[i] == 0 && random.Next(2) == 0
                    ? BitConverter.DoubleToInt64Bits(random.Next(3) switch { 0 => double.NaN, 1 => double.NegativeInfinity, _ => 1e300 })
                    : BitConverter.DoubleToInt64Bits(DoubleValue(random));
            }

            var array = Array(DoubleType.Default, bits.SelectMany(BitConverter.GetBytes).ToArray());
            foreach (var (start, count) in Ranges(random))
            {
                var actual = StatisticsCollector.ComputePageBounds(
                    array, PhysicalType.Double, 0, levels, start, count, totalOrder, StatisticsOrder.Default);
                var present = Present(levels, start, count).Select(i => bits[i]).ToList();
                var (min, max, nanCount) = FloatingReference(present, totalOrder, 64);
                AssertBounds(min, max, nanCount, actual, $"seed {seed} [{start}, +{count})");
            }

            AssertChunk(array, PhysicalType.Double, levels, totalOrder, StatisticsOrder.Default,
                FloatingReference(Present(levels, 0, Rows).Select(i => bits[i]).ToList(), totalOrder, 64));
        }
    }

    [Theory]
    [MemberData(nameof(FloatingCases))]
    public void Float_BoundsMatchReference(double nullShare, bool totalOrder)
    {
        for (int seed = 0; seed < 20; seed++)
        {
            var random = new Random(seed + 100);
            var levels = Levels(random, nullShare);
            var bits = new int[Rows];
            for (int i = 0; i < Rows; i++)
            {
                bits[i] = levels[i] == 0 && random.Next(2) == 0
                    ? BitConverter.ToInt32(BitConverter.GetBytes(random.Next(3) switch { 0 => float.NaN, 1 => float.PositiveInfinity, _ => -3e38f }), 0)
                    : BitConverter.ToInt32(BitConverter.GetBytes((float)DoubleValue(random)), 0);
            }

            var array = Array(FloatType.Default, bits.SelectMany(BitConverter.GetBytes).ToArray());
            foreach (var (start, count) in Ranges(random))
            {
                var actual = StatisticsCollector.ComputePageBounds(
                    array, PhysicalType.Float, 0, levels, start, count, totalOrder, StatisticsOrder.Default);
                var present = Present(levels, start, count).Select(i => (long)bits[i]).ToList();
                var (min, max, nanCount) = FloatingReference(present, totalOrder, 32);
                AssertBounds(min, max, nanCount, actual, $"seed {seed} [{start}, +{count})");
            }

            AssertChunk(array, PhysicalType.Float, levels, totalOrder, StatisticsOrder.Default,
                FloatingReference(Present(levels, 0, Rows).Select(i => (long)bits[i]).ToList(), totalOrder, 32));
        }
    }

    [Theory]
    [MemberData(nameof(IntegerCases))]
    public void Int64_BoundsMatchReference(double nullShare, bool unsigned)
    {
        var order = unsigned ? StatisticsOrder.Unsigned : StatisticsOrder.Default;
        for (int seed = 0; seed < 20; seed++)
        {
            var random = new Random(seed + 200);
            var levels = Levels(random, nullShare);
            var values = new long[Rows];
            for (int i = 0; i < Rows; i++)
            {
                values[i] = levels[i] == 0 && random.Next(2) == 0
                    ? (random.Next(2) == 0 ? long.MinValue : long.MaxValue)
                    : (long)random.Next(-50, 50) << 40;
            }

            var array = Array(Int64Type.Default, values.SelectMany(BitConverter.GetBytes).ToArray());
            Comparison<long> compare = unsigned
                ? (a, b) => unchecked((ulong)a).CompareTo(unchecked((ulong)b))
                : (a, b) => a.CompareTo(b);
            foreach (var (start, count) in Ranges(random))
            {
                var actual = StatisticsCollector.ComputePageBounds(
                    array, PhysicalType.Int64, 0, levels, start, count, false, order);
                var present = Present(levels, start, count).Select(i => values[i]).ToList();
                var (min, max) = IntegerReference(present, compare);
                AssertBounds(min.HasValue ? BitConverter.GetBytes(min.Value) : null,
                    max.HasValue ? BitConverter.GetBytes(max.Value) : null, 0, actual, $"seed {seed} [{start}, +{count})");
            }

            var (cmin, cmax) = IntegerReference(Present(levels, 0, Rows).Select(i => values[i]).ToList(), compare);
            AssertChunk(array, PhysicalType.Int64, levels, false, order,
                (cmin.HasValue ? BitConverter.GetBytes(cmin.Value) : null, cmax.HasValue ? BitConverter.GetBytes(cmax.Value) : null, 0));
        }
    }

    [Theory]
    [MemberData(nameof(IntegerCases))]
    public void Int32_BoundsMatchReference(double nullShare, bool unsigned)
    {
        var order = unsigned ? StatisticsOrder.Unsigned : StatisticsOrder.Default;
        for (int seed = 0; seed < 20; seed++)
        {
            var random = new Random(seed + 300);
            var levels = Levels(random, nullShare);
            var values = new int[Rows];
            for (int i = 0; i < Rows; i++)
            {
                values[i] = levels[i] == 0 && random.Next(2) == 0
                    ? (random.Next(2) == 0 ? int.MinValue : int.MaxValue)
                    : random.Next(-50, 50) << 20;
            }

            var array = Array(Int32Type.Default, values.SelectMany(BitConverter.GetBytes).ToArray());
            Comparison<int> compare = unsigned
                ? (a, b) => unchecked((uint)a).CompareTo(unchecked((uint)b))
                : (a, b) => a.CompareTo(b);
            foreach (var (start, count) in Ranges(random))
            {
                var actual = StatisticsCollector.ComputePageBounds(
                    array, PhysicalType.Int32, 0, levels, start, count, false, order);
                var present = Present(levels, start, count).Select(i => values[i]).ToList();
                var (min, max) = IntegerReference(present, compare);
                AssertBounds(min.HasValue ? BitConverter.GetBytes(min.Value) : null,
                    max.HasValue ? BitConverter.GetBytes(max.Value) : null, 0, actual, $"seed {seed} [{start}, +{count})");
            }
        }
    }

    private static int[] Levels(Random random, double nullShare)
    {
        var levels = new int[Rows];
        for (int i = 0; i < Rows; i++)
            levels[i] = random.NextDouble() < nullShare ? 0 : 1;
        return levels;
    }

    private static double DoubleValue(Random random) => random.Next(8) switch
    {
        0 => double.NaN,
        1 => 0.0,
        2 => -0.0,
        3 => double.PositiveInfinity,
        4 => double.NegativeInfinity,
        _ => random.Next(-20, 20) / 4.0,
    };

    /// <summary>A whole range, a page at an offset, single values and an empty tail.</summary>
    private static IEnumerable<(int Start, int Count)> Ranges(Random random)
    {
        yield return (0, Rows);
        yield return (37, 150);
        yield return (Rows - 1, 1);
        for (int i = 0; i < 5; i++)
        {
            int start = random.Next(Rows);
            yield return (start, random.Next(1, Rows - start + 1));
        }
    }

    private static IEnumerable<int> Present(int[] levels, int start, int count) =>
        Enumerable.Range(start, count).Where(i => levels[i] != 0);

    /// <summary>
    /// Non-NaN bounds by IEEE 754 total-order key (numeric order with -0 below +0). An all-NaN set has
    /// NaN bounds under total order and none otherwise. Bits are little-endian, of the given width.
    /// </summary>
    private static (byte[]? Min, byte[]? Max, long NanCount) FloatingReference(List<long> bits, bool totalOrder, int width)
    {
        bool IsNaN(long b) => width == 64
            ? double.IsNaN(BitConverter.Int64BitsToDouble(b))
            : float.IsNaN(BitConverter.ToSingle(BitConverter.GetBytes((int)b), 0));
        long Key(long b) => width == 64
            ? b ^ ((b >> 63) & 0x7FFF_FFFF_FFFF_FFFF)
            : (int)b ^ (((int)b >> 31) & 0x7FFF_FFFF);
        byte[] Bytes(long b) => width == 64 ? BitConverter.GetBytes(b) : BitConverter.GetBytes((int)b);

        long nanCount = bits.Count(IsNaN);
        var numbers = bits.Where(b => !IsNaN(b)).ToList();
        if (numbers.Count > 0)
            return (Bytes(numbers.OrderBy(Key).First()), Bytes(numbers.OrderBy(Key).Last()), nanCount);
        if (totalOrder && nanCount > 0)
        {
            var nans = bits.Where(IsNaN).ToList();
            return (Bytes(nans.OrderBy(Key).First()), Bytes(nans.OrderBy(Key).Last()), nanCount);
        }

        return (null, null, nanCount);
    }

    private static (T? Min, T? Max) IntegerReference<T>(List<T> values, Comparison<T> compare)
        where T : struct
    {
        if (values.Count == 0)
            return (null, null);
        var sorted = values.ToList();
        sorted.Sort(compare);
        return (sorted[0], sorted[^1]);
    }

    private static void AssertBounds(byte[]? min, byte[]? max, long nanCount, PageBounds actual, string where)
    {
        Assert.True(Same(min, actual.Min), $"min, {where}");
        Assert.True(Same(max, actual.Max), $"max, {where}");
        Assert.True(nanCount == actual.NanCount, $"NaN count {actual.NanCount}, expected {nanCount}, {where}");
    }

    private static void AssertChunk(
        IArrowArray array, PhysicalType type, int[] levels, bool totalOrder, StatisticsOrder order,
        (byte[]? Min, byte[]? Max, long NanCount) expected)
    {
        int nonNull = levels.Count(l => l != 0);
        var stats = StatisticsCollector.Compute(array, type, 0, levels, nonNull, Rows, totalOrder, order);
        Assert.Equal(Rows - nonNull, stats.NullCount);
        if (nonNull == 0)
            return;
        Assert.True(Same(expected.Min, stats.MinValue), "chunk min");
        Assert.True(Same(expected.Max, stats.MaxValue), "chunk max");
        if (type is PhysicalType.Float or PhysicalType.Double)
            Assert.Equal(expected.NanCount, stats.NanCount);
    }

    private static bool Same(byte[]? a, byte[]? b) =>
        a is null ? b is null : b is not null && a.AsSpan().SequenceEqual(b);

    private static IArrowArray Array(IArrowType type, byte[] values) =>
        Apache.Arrow.ArrowArrayFactory.BuildArray(new ArrayData(type, Rows, nullCount: 0, offset: 0,
            [ArrowBuffer.Empty, new ArrowBuffer(values)]));
}
