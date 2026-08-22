// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0004 // These tests exist to exercise the experimental carrier.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Statistics for the extended-precision timestamp carrier, in both directions.
///
/// The carrier is the one FIXED_LEN_BYTE_ARRAY whose bytes are NOT ordered lexicographically. It is
/// little-endian two's complement, so its most significant byte is last and -1 encodes as all-0xFF —
/// which a lexicographic comparison ranks above every positive value. DECIMAL avoids the problem by
/// being rewritten to big-endian before statistics run; this cannot, because little-endian is what the
/// spec says goes on the wire.
///
/// A min/max computed with the wrong comparator is not a cosmetic defect: it is a wrong bound in the
/// footer, and a wrong bound is a wrong prune.
/// </summary>
public class ExtendedTimestampStatisticsTests
{
    private static byte[] Encode(long value)
    {
        var bytes = new byte[ExtendedTimestamp.ByteWidth];
        ExtendedTimestamp.Write((Int128)value, bytes);
        return bytes;
    }

    private static FixedSizeBinaryArray Carrier(params long[] values)
    {
        var packed = new byte[values.Length * ExtendedTimestamp.ByteWidth];
        for (int i = 0; i < values.Length; i++)
        {
            Encode(values[i]).CopyTo(packed, i * ExtendedTimestamp.ByteWidth);
        }

        var validity = new byte[(values.Length + 7) / 8];
        for (int i = 0; i < values.Length; i++)
        {
            validity[i / 8] |= (byte)(1 << (i % 8));
        }

        return new FixedSizeBinaryArray(new ArrayData(
            new FixedSizeBinaryType(ExtendedTimestamp.ByteWidth), values.Length, 0, 0,
            [new ArrowBuffer(validity), new ArrowBuffer(packed)]));
    }

    private static (long Min, long Max) CollectBounds(params long[] values)
    {
        var stats = StatisticsCollector.Compute(
            Carrier(values), PhysicalType.FixedLenByteArray, ExtendedTimestamp.ByteWidth,
            defLevels: null, nonNullCount: values.Length, rowCount: values.Length,
            floatingPointTotalOrder: false, extendedTimestamp: true);

        return ((long)ExtendedTimestamp.Read(stats.MinValue!), (long)ExtendedTimestamp.Read(stats.MaxValue!));
    }

    [Fact]
    public void NegativeValuesSortBelowPositiveOnes()
    {
        // The case a lexicographic comparator gets exactly backwards: -1 is all-0xFF.
        var (min, max) = CollectBounds(1, -1, 0);

        Assert.Equal(-1, min);
        Assert.Equal(1, max);
    }

    [Fact]
    public void OrderingHoldsAcrossTheLowWordBoundary()
    {
        // 2^63 has bit 63 set, so a comparator that read the low eight bytes SIGNED would rank it below
        // every small positive value.
        var (min, max) = CollectBounds(1, long.MaxValue, 256);

        Assert.Equal(1, min);
        Assert.Equal(long.MaxValue, max);
    }

    [Fact]
    public void TheMostSignificantByteIsTheLastOne()
    {
        // 256 encodes as 00 01 00... — LSB first. A comparator walking bytes from the front sees 0x00
        // against 0x01 and ranks 256 below 1.
        var (min, max) = CollectBounds(1, 256);

        Assert.Equal(1, min);
        Assert.Equal(256, max);
    }

    [Fact]
    public void PreEpochValuesOrderAmongThemselves()
    {
        var (min, max) = CollectBounds(-1, -256, -1_000_000);

        Assert.Equal(-1_000_000, min);
        Assert.Equal(-1, max);
    }

    [Fact]
    public void TheLexicographicComparatorReallyWouldDisagree()
    {
        // Keeps the test above honest: it is only meaningful because the default comparator is wrong here.
        var stats = StatisticsCollector.Compute(
            Carrier(1, -1, 0), PhysicalType.FixedLenByteArray, ExtendedTimestamp.ByteWidth,
            defLevels: null, nonNullCount: 3, rowCount: 3,
            floatingPointTotalOrder: false, extendedTimestamp: false);

        // Unsigned lexicographic puts all-0xFF on top, so -1 comes back as the MAXIMUM.
        Assert.Equal(-1, (long)ExtendedTimestamp.Read(stats.MaxValue!));
    }

    // ── Read side, against the upstream conformance fixture ──

    private const string Fixture = "flba12_timestamp.parquet";

    private static readonly DateTimeOffset Year0001 = new(1, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset Year9999 = new(9999, 12, 31, 23, 59, 59, TimeSpan.Zero);

    [Theory]
    [InlineData("timestamp_millis")]
    [InlineData("timestamp_micros")]
    [InlineData("timestamp_nanos")]
    public async Task FixtureBoundsDecodeToTheExtremeRows(string column)
    {
        var path = TestData.GetPath(Fixture);
        if (!File.Exists(path)) return; // fixture lands with apache/parquet-testing#123

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var accessor = new ParquetStatisticsAccessor(await reader.GetSchemaAsync());

        var min = accessor.GetMinValue(metadata.RowGroups[0], column);
        var max = accessor.GetMaxValue(metadata.RowGroups[0], column);

        // The fixture's own documented min and max: year 0001 and year 9999. Both are far outside int64
        // nanoseconds, so a reader that could only narrow to int64 would have no bounds to offer at all.
        Assert.NotNull(min);
        Assert.NotNull(max);
        Assert.Equal(Year0001, min!.Value.AsDateTimeOffset);
        Assert.Equal(Year9999, max!.Value.AsDateTimeOffset);
    }

    [Theory]
    [InlineData("timestamp_millis")]
    [InlineData("timestamp_micros")]
    [InlineData("timestamp_nanos")]
    public async Task FixtureBoundsContainEveryValueInTheColumn(string column)
    {
        var path = TestData.GetPath(Fixture);
        if (!File.Exists(path)) return;

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var accessor = new ParquetStatisticsAccessor(await reader.GetSchemaAsync());

        var min = accessor.GetMinValue(metadata.RowGroups[0], column)!.Value.AsDateTimeOffset;
        var max = accessor.GetMaxValue(metadata.RowGroups[0], column)!.Value.AsDateTimeOffset;

        // Read the column itself and check the footer is not lying about it — the property that makes a
        // bound safe to prune on.
        var batch = await reader.ReadRowGroupAsync(0, [column]);
        var values = Assert.IsType<TimestampArray>(batch.Column(0));
        for (int i = 0; i < values.Length; i++)
        {
            var value = values.GetTimestamp(i)!.Value;
            Assert.True(value >= min, $"row {i} is below the min bound");
            Assert.True(value <= max, $"row {i} is above the max bound");
        }
    }
}
