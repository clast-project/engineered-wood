// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.Partitioning;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Splitting a batch by partition value gathers rows out of every NON-partition column, so those
/// columns must come through byte-identical. These cover the types whose .NET surface representation
/// is narrower than their Arrow storage — a gather that round-trips through a typed array builder
/// loses data on them, silently, on the write path.
/// </summary>
public class PartitionTakeFidelityTests
{
    private static RecordBatch BatchWith(Field dataField, IArrowArray dataColumn, string[] regions)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, false))
            .Field(dataField)
            .Build();

        var regionBuilder = new StringArray.Builder();
        foreach (string r in regions)
            regionBuilder.Append(r);

        return new RecordBatch(
            schema, [regionBuilder.Build(), dataColumn], regions.Length);
    }

    [Theory]
    [InlineData(TimeUnit.Microsecond)]
    [InlineData(TimeUnit.Millisecond)]
    public void SplitByPartition_PreservesTimestampValuesExactly(TimeUnit unit)
    {
        var tsType = new TimestampType(unit, (string?)null);

        // Deliberately not round multiples of the next coarser unit: 123 microseconds cannot survive a
        // trip through a millisecond-resolution DateTimeOffset, and a millisecond value read as if it
        // were microseconds comes back 1000x too small.
        long[] raw = unit == TimeUnit.Microsecond
            ? [1_700_000_000_000_123L, 1_700_000_000_000_456L, 1_700_000_000_000_789L]
            : [1_700_000_000_123L, 1_700_000_000_456L, 1_700_000_000_789L];

        var values = new ArrowBuffer.Builder<long>(raw.Length);
        foreach (long v in raw) values.Append(v);
        var tsArray = new TimestampArray(new ArrayData(
            tsType, raw.Length, nullCount: 0, offset: 0,
            [ArrowBuffer.Empty, values.Build()]));

        var batch = BatchWith(
            new Field("ts", tsType, false), tsArray, ["us", "eu", "us"]);

        var groups = PartitionUtils.SplitByPartition(batch, ["region"]);

        // Rows 0 and 2 land in "us", row 1 in "eu"; the partition column itself is stripped.
        var byRegion = groups.ToDictionary(g => g.PartitionValues["region"], g => g.Data);
        Assert.Equal(2, byRegion.Count);

        var us = Assert.IsType<TimestampArray>(byRegion["us"].Column("ts"));
        var eu = Assert.IsType<TimestampArray>(byRegion["eu"].Column("ts"));

        // The gathered column must keep its own unit, not be re-tagged to whatever a builder defaulted to.
        Assert.Equal(unit, ((TimestampType)us.Data.DataType).Unit);

        Assert.Equal(raw[0], us.GetValue(0));
        Assert.Equal(raw[2], us.GetValue(1));
        Assert.Equal(raw[1], eu.GetValue(0));
    }

    [Fact]
    public void SplitByPartition_PreservesDecimalsWiderThanSystemDecimal()
    {
        // 38 digits — outside System.Decimal's ~28-29 digit range, so any gather that materialises the
        // value as a decimal on the way through either throws or rounds.
        var decType = new Decimal128Type(precision: 38, scale: 0);
        var big = System.Numerics.BigInteger.Parse("12345678901234567890123456789012345678");

        var bytes = new byte[3 * 16];
        for (int row = 0; row < 3; row++)
        {
            var v = big - row; // three distinct, all too wide for System.Decimal
            // ToByteArray is little-endian two's complement; for a positive value it may carry one
            // extra 0x00 sign byte past the 16 we want, so take only the low 16.
            var le = v.ToByteArray();
            le.AsSpan(0, Math.Min(le.Length, 16)).CopyTo(bytes.AsSpan(row * 16, 16));
        }

        var decArray = new Decimal128Array(new ArrayData(
            decType, 3, nullCount: 0, offset: 0,
            [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));

        var batch = BatchWith(
            new Field("amount", decType, false), decArray, ["us", "eu", "us"]);

        var groups = PartitionUtils.SplitByPartition(batch, ["region"]);
        var byRegion = groups.ToDictionary(g => g.PartitionValues["region"], g => g.Data);

        var us = Assert.IsType<Decimal128Array>(byRegion["us"].Column("amount"));
        var usType = (Decimal128Type)us.Data.DataType;
        Assert.Equal(38, usType.Precision);
        Assert.Equal(0, usType.Scale);

        // Compare raw storage bytes: rows 0 and 2 of the source, in order.
        Assert.Equal(bytes.AsSpan(0, 16).ToArray(), us.GetBytes(0).ToArray());
        Assert.Equal(bytes.AsSpan(32, 16).ToArray(), us.GetBytes(1).ToArray());
    }

    [Fact]
    public void SplitByPartition_PreservesLargeStringType()
    {
        var offsets = new ArrowBuffer.Builder<long>(4);
        offsets.Append(0).Append(5).Append(10).Append(15);
        var chars = System.Text.Encoding.UTF8.GetBytes("alphabravocharl");

        var large = new LargeStringArray(new ArrayData(
            LargeStringType.Default, 3, nullCount: 0, offset: 0,
            [ArrowBuffer.Empty, offsets.Build(), new ArrowBuffer(chars)]));

        var batch = BatchWith(
            new Field("name", LargeStringType.Default, false), large, ["us", "eu", "us"]);

        var groups = PartitionUtils.SplitByPartition(batch, ["region"]);
        var byRegion = groups.ToDictionary(g => g.PartitionValues["region"], g => g.Data);

        // A LargeString column must not be narrowed to String — that contradicts the declared schema.
        var us = Assert.IsType<LargeStringArray>(byRegion["us"].Column("name"));
        Assert.Equal("alpha", us.GetString(0));
        Assert.Equal("charl", us.GetString(1));
    }

    [Fact]
    public void SplitByPartition_PreservesNullsAndUnsignedValues()
    {
        var values = new ArrowBuffer.Builder<uint>(4);
        values.Append(uint.MaxValue).Append(0).Append(7).Append(uint.MaxValue - 1);

        // Rows 1 and 3 are null.
        var validity = new ArrowBuffer.BitmapBuilder(4);
        validity.Append(true).Append(false).Append(true).Append(false);

        var arr = new UInt32Array(new ArrayData(
            UInt32Type.Default, 4, nullCount: 2, offset: 0,
            [validity.Build(), values.Build()]));

        var batch = BatchWith(
            new Field("n", UInt32Type.Default, true), arr, ["us", "us", "eu", "eu"]);

        var groups = PartitionUtils.SplitByPartition(batch, ["region"]);
        var byRegion = groups.ToDictionary(g => g.PartitionValues["region"], g => g.Data);

        var us = Assert.IsType<UInt32Array>(byRegion["us"].Column("n"));
        Assert.Equal(uint.MaxValue, us.GetValue(0));
        Assert.Null(us.GetValue(1));
        Assert.Equal(1, us.NullCount);

        var eu = Assert.IsType<UInt32Array>(byRegion["eu"].Column("n"));
        Assert.Equal(7u, eu.GetValue(0));
        Assert.Null(eu.GetValue(1));
        Assert.Equal(1, eu.NullCount);
    }
}
