// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Expressions;
using EngineeredWood.Vortex.Layouts;
using EngineeredWood.Vortex.Tests.TestData;
using Pred = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Reads fixtures that exercise what vortex 0.86 writes differently from 0.70: the
/// <c>vortex.zoned</c> zone-map layout, <c>fastlanes.delta</c> over signed integers, and a
/// <c>vortex.sequence</c> with a u64 base and an i64 step. The generators are
/// <c>write_zoned_mixed</c>, <c>write_delta_signed_2k</c> and <c>write_sequence_u64_desc</c>
/// in <c>Rust/src/main.rs</c>.
/// </summary>
public class VortexZonedFixtureTests
{
    private const string Zoned = "zoned_mixed_20000rows.vortex";
    private const int Rows = 20_000;
    private const int ZoneLen = 8192;

    [Fact]
    public async Task ReadsSignedDeltaColumn()
    {
        var expected = new int[2048];
        ulong x = 0x5EED_0086UL;
        int acc = -1_000;
        for (int i = 0; i < expected.Length; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            acc = unchecked(acc + (int)(x % 7) - 2);
            expected[i] = i == 1500 ? int.MinValue + 1 : acc;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("delta_signed_2048rows.vortex"));

        var column = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
        Assert.Equal(expected, column.Values.ToArray());
    }

    [Fact]
    public async Task ReadsDescendingUInt64Sequence()
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("sequence_u64_desc_64rows.vortex"));

        var column = Assert.IsType<UInt64Array>(await reader.ReadColumnAsync(0));
        var expected = Enumerable.Range(0, 64).Select(i => ulong.MaxValue - 5 - 3UL * (ulong)i);
        Assert.Equal(expected, column.Values.ToArray());
    }

    [Fact]
    public async Task EveryColumnIsWrappedInAZonedLayout()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));

        Assert.Equal(VortexLayoutEncodings.Struct, reader.RootLayout.EncodingId);
        Assert.All(reader.RootLayout.Children,
            c => Assert.Equal(VortexLayoutEncodings.Zoned, c.EncodingId));
    }

    [Fact]
    public async Task ReadsEveryZonedColumn()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));
        Assert.Equal(Rows, reader.NumberOfRows);

        var id = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(0));
        Assert.Equal(Enumerable.Range(0, Rows).Select(i => (long)i), id.Values.ToArray());

        var val = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(1));
        Assert.Equal(ExpectedVal(), val.Values.ToArray());

        var f = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(2));
        for (int i = 0; i < Rows; i++)
        {
            if (IsNanRow(i)) Assert.True(double.IsNaN(f.GetValue(i)!.Value), $"row {i}");
            else Assert.Equal(i * 0.5, f.GetValue(i));
        }

        var s = Assert.IsType<StringArray>(await reader.ReadColumnAsync(3));
        var tag = Assert.IsType<StringArray>(await reader.ReadColumnAsync(4));
        var n = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(5));
        for (int i = 0; i < Rows; i++)
        {
            Assert.Equal(LongString(i), s.GetString(i));
            Assert.Equal($"k{i / ZoneLen}-{i % 7}", tag.GetString(i));
            Assert.Equal(i % 3 == 0 ? null : i * 3, n.GetValue(i));
        }
    }

    [Fact]
    public async Task StreamsColumnsChunkedDifferently()
    {
        // The writer chunks the long-string column separately from the others,
        // so batches have to be cut at the union of the columns' chunk ends.
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));
        Assert.NotEqual(reader.ColumnPlans[0].ChunkCount, reader.ColumnPlans[3].ChunkCount);

        long next = 0;
        await foreach (var batch in reader.ReadAllAsync(new[] { 0, 3 }))
        {
            var id = (Int64Array)batch.Column(0);
            var s = (StringArray)batch.Column(1);
            for (int i = 0; i < batch.Length; i++, next++)
            {
                Assert.Equal(next, id.GetValue(i));
                Assert.Equal(LongString((int)next), s.GetString(i));
            }
        }
        Assert.Equal(Rows, next);
    }

    [Fact]
    public async Task RowRangeAcrossColumnsChunkedDifferently()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));

        long next = 9_000;
        await foreach (var batch in reader.ReadAllAsync(rowOffset: 9_000, rowCount: 5_000))
        {
            var id = (Int64Array)batch.Column(0);
            var s = (StringArray)batch.Column(3);
            for (int i = 0; i < batch.Length; i++, next++)
            {
                Assert.Equal(next, id.GetValue(i));
                Assert.Equal(LongString((int)next), s.GetString(i));
            }
        }
        Assert.Equal(14_000, next);
    }

    [Fact]
    public async Task ExactZoneStats()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));

        var val = await reader.GetZoneStatsAsync(1);
        Assert.NotNull(val);
        Assert.Equal(ZoneLen, val!.ZoneLen);
        Assert.Equal(3, val.ZoneCount);
        Assert.Equal(new[] { Stat.Max, Stat.Min, Stat.NullCount }, val.PresentStats);
        Assert.Null(val.MinIsTruncated);
        Assert.Null(val.MaxIsTruncated);
        var expected = ExpectedVal();
        var min = Assert.IsType<Int32Array>(val.Min);
        var max = Assert.IsType<Int32Array>(val.Max);
        for (int z = 0; z < 3; z++)
        {
            var zone = expected.Skip(z * ZoneLen).Take(ZoneLen).ToArray();
            Assert.Equal(zone.Min(), min.GetValue(z));
            Assert.Equal(zone.Max(), max.GetValue(z));
            Assert.Equal(0UL, val.NullCount!.GetValue(z));
        }

        var tag = await reader.GetZoneStatsAsync(4);
        Assert.NotNull(tag);
        var tagMin = Assert.IsType<StringArray>(tag!.Min);
        var tagMax = Assert.IsType<StringArray>(tag.Max);
        for (int z = 0; z < 3; z++)
        {
            Assert.Equal($"k{z}-0", tagMin.GetString(z));
            Assert.Equal($"k{z}-6", tagMax.GetString(z));
            // Short strings are stored whole, so the bounded min is exact.
            Assert.False(tag.MinIsTruncated!.GetValue(z));
            // A bounded max never proves itself exact.
            Assert.True(tag.MaxIsTruncated!.GetValue(z));
        }
    }

    [Fact]
    public async Task TruncatedStringZoneStatsAreBounds()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));

        var s = await reader.GetZoneStatsAsync(3);
        Assert.NotNull(s);
        var min = Assert.IsType<StringArray>(s!.Min);
        var max = Assert.IsType<StringArray>(s.Max);
        for (int z = 0; z < 3; z++)
        {
            var first = LongString(z * ZoneLen);
            var last = LongString(Math.Min((z + 1) * ZoneLen, Rows) - 1);
            Assert.True(min.GetString(z).Length <= 64);
            Assert.True(max.GetString(z).Length <= 64);
            Assert.True(string.CompareOrdinal(min.GetString(z), first) <= 0);
            Assert.True(string.CompareOrdinal(max.GetString(z), last) >= 0);
            Assert.True(s.MinIsTruncated!.GetValue(z));
            Assert.True(s.MaxIsTruncated!.GetValue(z));
        }
    }

    [Fact]
    public async Task NanAndNullCountZoneStats()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));

        var f = await reader.GetZoneStatsAsync(2);
        Assert.NotNull(f);
        Assert.Equal(new[] { Stat.Max, Stat.Min, Stat.NullCount, Stat.NaNCount }, f!.PresentStats);
        Assert.Equal(new ulong[] { 0, 8, 0 }, f.NaNCount!.Values.ToArray());
        // Min and max skip NaN.
        Assert.Equal(ZoneLen * 0.5, Assert.IsType<DoubleArray>(f.Min).GetValue(1));

        var n = await reader.GetZoneStatsAsync(5);
        Assert.NotNull(n);
        Assert.Equal(new ulong[] { 2731, 2731, 1205 }, n!.NullCount!.Values.ToArray());
    }

    [Theory]
    [InlineData("val", 1600, Rows - 2 * ZoneLen)]
    [InlineData("val", 2600, 0)]
    // Zone 1's finite max is below 9000, but its NaN count keeps it: NaN
    // sorts above every number, so those rows may match any `>`.
    [InlineData("f", 9000.0, Rows - ZoneLen)]
    [InlineData("f", 10_000.0, ZoneLen)]
    [InlineData("s", "019999-z", 0)]
    [InlineData("s", "016384", Rows - 2 * ZoneLen)]
    [InlineData("s", "000000", Rows)]
    [InlineData("tag", "k1-6", Rows - 2 * ZoneLen)]
    public async Task GreaterThanPrunesZones(string column, object literal, int expectedRows)
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));
        Assert.Equal(expectedRows, await CountRows(reader, Pred.GreaterThan(column, Literal(literal))));
    }

    [Theory]
    [InlineData("val", 500, ZoneLen)]
    [InlineData("val", 0, 0)]
    [InlineData("s", "000000", 0)]
    [InlineData("s", "008192", ZoneLen)]
    [InlineData("tag", "k0-0", 0)]
    public async Task LessThanPrunesZones(string column, object literal, int expectedRows)
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));
        Assert.Equal(expectedRows, await CountRows(reader, Pred.LessThan(column, Literal(literal))));
    }

    [Fact]
    public async Task EqualityOnTruncatedStringsKeepsTheZoneThatMayHoldIt()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Zoned));

        var hit = Pred.Equal("s", LiteralValue.Of(LongString(10_000)));
        Assert.Equal(ZoneLen, await CountRows(reader, hit));

        var miss = Pred.Equal("s", LiteralValue.Of("zzz"));
        Assert.Equal(0, await CountRows(reader, miss));
    }

    [Theory]
    [InlineData(Zoned, "val")]
    // No zone maps at all: nothing to evaluate per zone, but a predicate that
    // is false regardless of the data still reads nothing.
    [InlineData("sequence_u64_desc_64rows.vortex", "a")]
    public async Task PredicateFalseWithoutStatsReadsNothing(string fixture, string column)
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(fixture));

        Assert.Equal(0, await CountRows(reader, Pred.False));
        Assert.Equal(0, await CountRows(reader,
            Pred.And(Pred.False, Pred.GreaterThan(column, LiteralValue.Of(0)))));
        Assert.Equal(0, await CountRowsInRange(reader, Pred.False));
    }

    private static async Task<int> CountRowsInRange(VortexFileReader reader, Predicate predicate)
    {
        int rows = 0;
        await foreach (var batch in reader.ReadAllAsync(0, long.MaxValue, predicate: predicate))
            rows += batch.Length;
        return rows;
    }

    private static async Task<int> CountRows(VortexFileReader reader, Predicate predicate)
    {
        int rows = 0;
        await foreach (var batch in reader.ReadAllAsync(predicate))
            rows += batch.Length;
        return rows;
    }

    private static LiteralValue Literal(object value) => value switch
    {
        int i => LiteralValue.Of(i),
        double d => LiteralValue.Of(d),
        string s => LiteralValue.Of(s),
        _ => throw new ArgumentException($"unsupported literal {value}"),
    };

    private static bool IsNanRow(int i) => i / ZoneLen == 1 && i % 1000 == 0;

    private static string LongString(int i) => i.ToString("D6") + "-" + new string('x', 70);

    private static int[] ExpectedVal()
    {
        var vals = new int[Rows];
        ulong x = 0x2026_0916UL;
        for (int i = 0; i < Rows; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            vals[i] = (i / ZoneLen) * 1000 + (int)((x >> 33) % 500);
        }
        return vals;
    }
}
