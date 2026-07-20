// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// Type coverage for the row filter behind copy-on-write DELETE/UPDATE. Types it could not handle used to be
/// returned UNFILTERED — a column of the wrong length silently mispaired with its neighbours in the rewritten
/// file. Fixed-width types are now filtered generically by slicing raw value bytes (preserving exact type
/// parameters), and a genuinely unsupported type throws instead of corrupting the rewrite.
/// </summary>
public class DeletionVectorFilterTypeCoverageTests
{
    // Row 1 of 3 is deleted throughout, so a column returned unfiltered would have length 3, not 2.
    private static readonly HashSet<long> Deleted = [1];

    private static RecordBatch Filter(Field field, IArrowArray column)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(field).Build();
        var batch = new RecordBatch(schema, [column], 3);
        return DeletionVectorFilter.Filter(batch, Deleted, batchStartRow: 0);
    }

    [Fact]
    public void Filter_UInt32_KeepsTypeAndValues()
    {
        var col = new UInt32Array.Builder().Append(1u).Append(2u).Append(3u).Build();
        var result = Filter(new Field("v", UInt32Type.Default, true), col);

        Assert.Equal(2, result.Length);
        var v = (UInt32Array)result.Column(0);
        Assert.Equal(1u, v.GetValue(0));
        Assert.Equal(3u, v.GetValue(1));
    }

    // Timestamp carries a unit + timezone; a per-type builder would be easy to get wrong here.
    [Fact]
    public void Filter_Timestamp_PreservesUnitAndTimezone()
    {
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");
        var col = new TimestampArray.Builder(type)
            .Append(DateTimeOffset.FromUnixTimeSeconds(100))
            .Append(DateTimeOffset.FromUnixTimeSeconds(200))
            .Append(DateTimeOffset.FromUnixTimeSeconds(300))
            .Build();

        var result = Filter(new Field("ts", type, true), col);

        Assert.Equal(2, result.Length);
        var resultType = (TimestampType)result.Schema.FieldsList[0].DataType;
        Assert.Equal(TimeUnit.Microsecond, resultType.Unit);
        Assert.Equal("UTC", resultType.Timezone);

        var v = (TimestampArray)result.Column(0);
        Assert.Equal(100L, v.GetTimestamp(0)!.Value.ToUnixTimeSeconds());
        Assert.Equal(300L, v.GetTimestamp(1)!.Value.ToUnixTimeSeconds());
    }

    // Decimal128 derives from FixedSizeBinaryType — precision/scale must survive, and the raw-bytes path
    // sidesteps System.Decimal's 28-digit cap.
    [Fact]
    public void Filter_Decimal128_PreservesPrecisionAndScale()
    {
        var type = new Decimal128Type(20, 4);
        var builder = new Decimal128Array.Builder(type);
        builder.Append(1.5m).Append(2.5m).Append(3.5m);
        var result = Filter(new Field("d", type, true), builder.Build());

        Assert.Equal(2, result.Length);
        var resultType = (Decimal128Type)result.Schema.FieldsList[0].DataType;
        Assert.Equal(20, resultType.Precision);
        Assert.Equal(4, resultType.Scale);

        var v = (Decimal128Array)result.Column(0);
        Assert.Equal(1.5m, v.GetValue(0));
        Assert.Equal(3.5m, v.GetValue(1));
    }

    [Fact]
    public void Filter_Date32AndTime64_AreFiltered()
    {
        var date = new Date32Array.Builder()
            .Append(new DateTime(2020, 1, 1)).Append(new DateTime(2021, 1, 1))
            .Append(new DateTime(2022, 1, 1)).Build();
        var dateResult = Filter(new Field("d", Date32Type.Default, true), date);
        Assert.Equal(2, dateResult.Length);
        Assert.Equal(new DateTime(2022, 1, 1), ((Date32Array)dateResult.Column(0)).GetDateTime(1));

        var timeType = new Time64Type(TimeUnit.Microsecond);
        var time = new Time64Array.Builder(timeType).Append(1L).Append(2L).Append(3L).Build();
        var timeResult = Filter(new Field("t", timeType, true), time);
        Assert.Equal(2, timeResult.Length);
        Assert.Equal(3L, ((Time64Array)timeResult.Column(0)).GetValue(1));
    }

    [Fact]
    public void Filter_FixedWidth_PreservesNulls()
    {
        var col = new UInt64Array.Builder().Append(1ul).AppendNull().AppendNull().Build();
        // Delete row 1 (a null); row 2 (also null) survives.
        var result = Filter(new Field("v", UInt64Type.Default, true), col);

        Assert.Equal(2, result.Length);
        var v = (UInt64Array)result.Column(0);
        Assert.False(v.IsNull(0));
        Assert.Equal(1ul, v.GetValue(0));
        Assert.True(v.IsNull(1));
    }

    // Struct children are aligned with parent rows but do NOT carry the parent's logical offset — the
    // recursive take has to index children at parentOffset + row.
    [Fact]
    public void Filter_Struct_FiltersChildrenAndRebuildsValidity()
    {
        var type = new StructType(
        [
            new Field("a", Int64Type.Default, true),
            new Field("b", Int32Type.Default, true),
        ]);
        var a = new Int64Array.Builder().Append(10).Append(20).Append(30).Build();
        var b = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var col = new StructArray(type, 3, [a, b], ArrowBuffer.Empty);

        var result = Filter(new Field("s", type, true), col);

        Assert.Equal(2, result.Length);
        var s = (StructArray)result.Column(0);
        Assert.Equal(2, s.Length);
        Assert.Equal(10L, ((Int64Array)s.Fields[0]).GetValue(0));
        Assert.Equal(30L, ((Int64Array)s.Fields[0]).GetValue(1));
        Assert.Equal(1, ((Int32Array)s.Fields[1]).GetValue(0));
        Assert.Equal(3, ((Int32Array)s.Fields[1]).GetValue(1));
    }

    // The important part: an unsliceable type must THROW rather than come back unfiltered at the wrong length.
    [Fact]
    public void Filter_UnsupportedType_Throws()
    {
        var valueField = new Field("item", Int64Type.Default, true);
        var type = new ListType(valueField);
        var builder = new ListArray.Builder(valueField);
        var values = (Int64Array.Builder)builder.ValueBuilder;
        builder.Append(); values.Append(1);
        builder.Append(); values.Append(2);
        builder.Append(); values.Append(3);
        var col = builder.Build();

        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("l", type, true)).Build();
        var batch = new RecordBatch(schema, [col], 3);

        Assert.Throws<NotSupportedException>(() =>
            DeletionVectorFilter.Filter(batch, Deleted, batchStartRow: 0));
    }
}
