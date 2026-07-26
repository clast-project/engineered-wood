// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.TypeWidening;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>ValueWidener.WidenArray</c> decides WHICH widenings Delta permits; <c>ArrowCompute.Widen</c> performs
/// them. These cover the decision, so the pair list cannot quietly gain a narrowing or lose a legal pair — the
/// kernel's own correctness is covered in <c>EngineeredWood.Core.Tests</c>.
/// </summary>
public class TypeWideningPolicyTests
{
    private static IArrowArray Int8Of(params sbyte[] values)
    {
        var b = new ArrowBuffer.Builder<sbyte>(values.Length);
        foreach (sbyte v in values) b.Append(v);
        return new Int8Array(new ArrayData(
            Int8Type.Default, values.Length, 0, 0, [ArrowBuffer.Empty, b.Build()]));
    }

    public static TheoryData<string, IArrowType> LegalWidenings() => new()
    {
        { "int8->int16", Int16Type.Default },
        { "int8->int32", Int32Type.Default },
        { "int8->int64", Int64Type.Default },
        { "int8->double", DoubleType.Default },
    };

    [Theory]
    [MemberData(nameof(LegalWidenings))]
    public void LegalWidening_ConvertsAndKeepsValues(string label, IArrowType target)
    {
        Assert.NotNull(label);

        var result = ValueWidener.WidenArray(Int8Of(-128, 0, 127), target);

        Assert.Equal(target.TypeId, result.Data.DataType.TypeId);
        Assert.Equal(3, result.Length);
    }

    [Fact]
    public void IntegerLadder_AndFloatToDouble_AllRoute()
    {
        var i16 = new Int16Array(new ArrayData(
            Int16Type.Default, 1, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer.Builder<short>().Append(-5).Build()]));
        var i32 = new Int32Array(new ArrayData(
            Int32Type.Default, 1, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer.Builder<int>().Append(-5).Build()]));
        var f = new FloatArray(new ArrayData(
            FloatType.Default, 1, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer.Builder<float>().Append(1.5f).Build()]));

        Assert.Equal(-5, Assert.IsType<Int32Array>(
            ValueWidener.WidenArray(i16, Int32Type.Default)).GetValue(0));
        Assert.Equal(-5L, Assert.IsType<Int64Array>(
            ValueWidener.WidenArray(i16, Int64Type.Default)).GetValue(0));
        Assert.Equal(-5d, Assert.IsType<DoubleArray>(
            ValueWidener.WidenArray(i16, DoubleType.Default)).GetValue(0));
        Assert.Equal(-5L, Assert.IsType<Int64Array>(
            ValueWidener.WidenArray(i32, Int64Type.Default)).GetValue(0));
        Assert.Equal(-5d, Assert.IsType<DoubleArray>(
            ValueWidener.WidenArray(i32, DoubleType.Default)).GetValue(0));
        Assert.Equal(1.5d, Assert.IsType<DoubleArray>(
            ValueWidener.WidenArray(f, DoubleType.Default)).GetValue(0));
    }

    private static Date32Array Date32Of(int days)
    {
        var b = new ArrowBuffer.Builder<int>(1);
        b.Append(days);
        return new Date32Array(new ArrayData(
            Date32Type.Default, 1, 0, 0, [ArrowBuffer.Empty, b.Build()]));
    }

    [Fact]
    public void Date32ToTimestampNtz_IsWidened()
    {
        int days = (int)(new DateTime(2023, 11, 14) - new DateTime(1970, 1, 1)).TotalDays;

        var result = Assert.IsType<TimestampArray>(ValueWidener.WidenArray(
            Date32Of(days), new TimestampType(TimeUnit.Microsecond, (string?)null)));

        Assert.Equal(days * 86_400_000_000L, result.GetValue(0));
    }

    [Fact]
    public void Date32ToZonedTimestamp_IsNotWidened()
    {
        // Delta only permits date -> timestamp_ntz. A zoned target would mean reinterpreting a naive
        // calendar date as an absolute instant, so the arm is guarded on Timezone being null and the
        // column must pass through untouched rather than be converted.
        var source = Date32Of(19_675);

        var result = ValueWidener.WidenArray(source, new TimestampType(TimeUnit.Microsecond, "UTC"));

        Assert.Same(source, result);
    }

    [Theory]
    [InlineData("int64->int32")]
    [InlineData("double->float")]
    [InlineData("int32->int16")]
    public void Narrowing_IsNotPerformed(string which)
    {
        // A narrowing must never be routed to the kernel: it would truncate for some values and not others.
        // WidenArray's `_ => source` fallback means "no widening applies", so the column passes through.
        var (source, target) = which switch
        {
            "int64->int32" => ((IArrowArray)new Int64Array(new ArrayData(
                Int64Type.Default, 1, 0, 0,
                [ArrowBuffer.Empty, new ArrowBuffer.Builder<long>().Append(1L).Build()])),
                (IArrowType)Int32Type.Default),
            "double->float" => (new DoubleArray(new ArrayData(
                DoubleType.Default, 1, 0, 0,
                [ArrowBuffer.Empty, new ArrowBuffer.Builder<double>().Append(1d).Build()])),
                FloatType.Default),
            "int32->int16" => (new Int32Array(new ArrayData(
                Int32Type.Default, 1, 0, 0,
                [ArrowBuffer.Empty, new ArrowBuffer.Builder<int>().Append(1).Build()])),
                Int16Type.Default),
            _ => throw new ArgumentOutOfRangeException(nameof(which)),
        };

        Assert.Same(source, ValueWidener.WidenArray(source, target));
    }

    [Fact]
    public void Widening_CarriesNullsThrough()
    {
        var values = new ArrowBuffer.Builder<sbyte>(3);
        values.Append(1).Append(2).Append(3);
        var validity = new ArrowBuffer.BitmapBuilder(3);
        validity.Append(true).Append(false).Append(true);
        var source = new Int8Array(new ArrayData(
            Int8Type.Default, 3, nullCount: 1, offset: 0, [validity.Build(), values.Build()]));

        var result = Assert.IsType<Int64Array>(ValueWidener.WidenArray(source, Int64Type.Default));

        Assert.Equal(1, result.NullCount);
        Assert.Equal(1L, result.GetValue(0));
        Assert.Null(result.GetValue(1));
        Assert.Equal(3L, result.GetValue(2));
    }
}
