// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// <c>ArrowCompute.Widen</c> converts a fixed-width column to a wider type by copying value slots. Every pair
/// it accepts is exactly representable in the target, so the interesting failures are not about values but
/// about the VALIDITY buffer: widening never changes which rows are null, so the source's bitmap can be shared
/// by reference — but only while the source starts at physical slot 0, because a bitmap is indexed by physical
/// slot. Sharing it across a non-zero offset reads every row's null flag from the wrong bit, silently, and only
/// for sliced arrays.
/// </summary>
public class WidenTests
{
    // ── Values, per pair ──

    [Fact]
    public void Int8ToWiderIntegers_CopiesValuesIncludingNegatives()
    {
        // sbyte.MinValue is the case a widening that forgot to sign-extend would get wrong.
        sbyte[] values = [0, 1, -1, sbyte.MaxValue, sbyte.MinValue];
        var source = RawArrays.Fixed(Int8Type.Default, values);

        var i16 = Assert.IsType<Int16Array>(ArrowCompute.Widen(source, Int16Type.Default));
        var i32 = Assert.IsType<Int32Array>(ArrowCompute.Widen(source, Int32Type.Default));
        var i64 = Assert.IsType<Int64Array>(ArrowCompute.Widen(source, Int64Type.Default));

        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(values[i], i16.GetValue(i));
            Assert.Equal(values[i], i32.GetValue(i));
            Assert.Equal(values[i], i64.GetValue(i));
        }
    }

    [Fact]
    public void Int16ToWiderIntegers_CopiesValues()
    {
        short[] values = [0, -1, short.MaxValue, short.MinValue];
        var source = RawArrays.Fixed(Int16Type.Default, values);

        var i32 = Assert.IsType<Int32Array>(ArrowCompute.Widen(source, Int32Type.Default));
        var i64 = Assert.IsType<Int64Array>(ArrowCompute.Widen(source, Int64Type.Default));

        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(values[i], i32.GetValue(i));
            Assert.Equal(values[i], i64.GetValue(i));
        }
    }

    [Fact]
    public void Int32ToInt64_CopiesValuesIncludingTheExtremes()
    {
        int[] values = [0, -1, int.MaxValue, int.MinValue, 12345];
        var source = RawArrays.Fixed(Int32Type.Default, values);

        var result = Assert.IsType<Int64Array>(ArrowCompute.Widen(source, Int64Type.Default));

        Assert.Equal(values.Length, result.Length);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], result.GetValue(i));
    }

    [Fact]
    public void IntegersToDouble_CopiesValues()
    {
        var i8 = RawArrays.Fixed(Int8Type.Default, new sbyte[] { -128, 0, 127 });
        var i16 = RawArrays.Fixed(Int16Type.Default, new short[] { -32768, 0, 32767 });
        var i32 = RawArrays.Fixed(Int32Type.Default, new[] { int.MinValue, 0, int.MaxValue });

        var d8 = Assert.IsType<DoubleArray>(ArrowCompute.Widen(i8, DoubleType.Default));
        var d16 = Assert.IsType<DoubleArray>(ArrowCompute.Widen(i16, DoubleType.Default));
        var d32 = Assert.IsType<DoubleArray>(ArrowCompute.Widen(i32, DoubleType.Default));

        Assert.Equal(-128d, d8.GetValue(0));
        Assert.Equal(127d, d8.GetValue(2));
        Assert.Equal(-32768d, d16.GetValue(0));
        Assert.Equal(32767d, d16.GetValue(2));
        Assert.Equal((double)int.MinValue, d32.GetValue(0));
        Assert.Equal((double)int.MaxValue, d32.GetValue(2));
    }

    [Fact]
    public void FloatToDouble_WidensExactly()
    {
        // 0.1f is not exactly 0.1, and widening must reproduce the FLOAT's value rather than re-parsing a
        // decimal literal — (double)0.1f differs from 0.1d.
        float[] values = [0.1f, -1.5f, float.MaxValue, float.Epsilon];
        var source = RawArrays.Fixed(FloatType.Default, values);

        var result = Assert.IsType<DoubleArray>(ArrowCompute.Widen(source, DoubleType.Default));

        for (int i = 0; i < values.Length; i++)
            Assert.Equal((double)values[i], result.GetValue(i));

        Assert.NotEqual(0.1d, result.GetValue(0));
    }

    [Theory]
    [InlineData(float.NaN)]
    [InlineData(float.PositiveInfinity)]
    [InlineData(float.NegativeInfinity)]
    public void FloatToDouble_CarriesNonFiniteValues(float value)
    {
        var source = RawArrays.Fixed(FloatType.Default, new[] { value });

        var result = Assert.IsType<DoubleArray>(ArrowCompute.Widen(source, DoubleType.Default));

        Assert.Equal((double)value, result.GetValue(0));
    }

    // ── Date32 → Timestamp ──

    [Theory]
    [InlineData(TimeUnit.Second, 86_400L)]
    [InlineData(TimeUnit.Millisecond, 86_400_000L)]
    [InlineData(TimeUnit.Microsecond, 86_400_000_000L)]
    [InlineData(TimeUnit.Nanosecond, 86_400_000_000_000L)]
    public void Date32ToTimestamp_MultipliesByTheUnitsDayLength(TimeUnit unit, long perDay)
    {
        // 19675 days = 2023-11-14, plus the epoch itself and a pre-epoch date.
        int[] days = [0, 19_675, -1];
        var source = RawArrays.Fixed(Date32Type.Default, days);
        var target = new TimestampType(unit, (string?)null);

        var result = Assert.IsType<TimestampArray>(ArrowCompute.Widen(source, target));

        // The type must be the instance handed in, so unit and timezone cannot be re-tagged.
        Assert.Same(target, result.Data.DataType);
        for (int i = 0; i < days.Length; i++)
            Assert.Equal(days[i] * perDay, result.GetValue(i));
    }

    [Fact]
    public void Date32ToTimestamp_AgreesWithTheDateItRepresents()
    {
        int days = (int)(new DateTime(2023, 11, 14) - new DateTime(1970, 1, 1)).TotalDays;
        var source = RawArrays.Fixed(Date32Type.Default, new[] { days });

        var result = Assert.IsType<TimestampArray>(
            ArrowCompute.Widen(source, new TimestampType(TimeUnit.Microsecond, "UTC")));

        Assert.Equal(
            new DateTimeOffset(2023, 11, 14, 0, 0, 0, TimeSpan.Zero), result.GetTimestamp(0));
    }

    [Fact]
    public void Date32ToTimestamp_OverflowThrowsRatherThanWrapping()
    {
        // Int64 nanoseconds run out in 2262; a date past that must fail rather than wrap to a bogus instant.
        var source = RawArrays.Fixed(Date32Type.Default, new[] { 200_000_000 });

        Assert.Throws<OverflowException>(
            () => ArrowCompute.Widen(source, new TimestampType(TimeUnit.Nanosecond, (string?)null)));
    }

    // ── Validity ──

    [Fact]
    public void NoNulls_ProducesNoValidityBufferAtAll()
    {
        var source = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3 });

        var result = ArrowCompute.Widen(source, Int64Type.Default);

        // An absent bitmap is what lets Arrow skip the per-element validity check downstream.
        Assert.Equal(0, result.Data.NullCount);
        Assert.Equal(0, result.Data.Buffers[0].Length);
    }

    [Fact]
    public void Nulls_AreCarriedThroughUnchanged()
    {
        int[] values = [10, 20, 30, 40, 50];
        bool[] valid = [true, false, true, false, true];
        var source = RawArrays.Fixed(Int32Type.Default, values, valid);

        var result = Assert.IsType<Int64Array>(ArrowCompute.Widen(source, Int64Type.Default));

        Assert.Equal(2, result.NullCount);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(!valid[i], result.IsNull(i));
            if (valid[i])
                Assert.Equal(values[i], result.GetValue(i));
        }
    }

    [Fact]
    public void NoNullsAtOffsetZero_SharesTheSourceBitmapByReference()
    {
        int[] values = [1, 2, 3, 4];
        bool[] valid = [true, false, true, true];
        var source = RawArrays.Fixed(Int32Type.Default, values, valid);

        var result = ArrowCompute.Widen(source, Int64Type.Default);

        // Widening cannot change which rows are null, so at offset 0 the bitmap is reused rather than rebuilt.
        // ArrowBuffer is a readonly struct over ReadOnlyMemory, which is what makes that safe.
        Assert.True(
            result.Data.Buffers[0].Memory.Equals(source.Data.Buffers[0].Memory),
            "the source's validity bitmap should be shared, not rebuilt, at offset 0");
    }

    /// <summary>
    /// The case the whole <c>AlignedValidity</c> split exists for. A validity bitmap is indexed by PHYSICAL
    /// slot, so an array sliced to a non-zero offset cannot have its bitmap reused once the values have been
    /// re-based to slot 0 — every row's null flag would come from the wrong bit. Modelled on tier 1's
    /// <c>Offset_ReadsCorrectValidityBits</c>.
    /// </summary>
    [Fact]
    public void Offset_ReadsCorrectValidityBits()
    {
        // Physical rows 0-6; the array starts at logical row 0 == physical slot 3.
        int[] physical = [100, 101, 102, 103, 104, 105, 106];
        bool[] valid = [true, true, true, false, true, false, true];
        var source = RawArrays.Fixed(Int32Type.Default, physical, valid, offset: 3);

        Assert.Equal(4, source.Length); // physical slots 3,4,5,6

        var result = Assert.IsType<Int64Array>(ArrowCompute.Widen(source, Int64Type.Default));

        Assert.Equal(4, result.Length);
        Assert.Equal(0, result.Data.Offset);
        Assert.Equal(2, result.NullCount);

        // Logical row i corresponds to physical slot 3 + i. Sharing the bitmap unshifted would have reported
        // rows 0-3 as valid/valid/valid/invalid — the flags of physical slots 0-3.
        Assert.True(result.IsNull(0));   // physical 3
        Assert.False(result.IsNull(1));  // physical 4
        Assert.True(result.IsNull(2));   // physical 5
        Assert.False(result.IsNull(3));  // physical 6

        Assert.Equal(104, result.GetValue(1));
        Assert.Equal(106, result.GetValue(3));
    }

    [Fact]
    public void Offset_WithoutNullsStillReadsTheRightValues()
    {
        int[] physical = [10, 11, 12, 13, 14];
        var source = RawArrays.Fixed(Int32Type.Default, physical, offset: 2);

        var result = Assert.IsType<Int64Array>(ArrowCompute.Widen(source, Int64Type.Default));

        Assert.Equal(3, result.Length);
        Assert.Equal(12, result.GetValue(0));
        Assert.Equal(13, result.GetValue(1));
        Assert.Equal(14, result.GetValue(2));
    }

    [Fact]
    public void UnknownNullCount_IsCountedRatherThanTrusted()
    {
        // A declared null count of -1 means "not computed". Passing it straight through would leave the
        // result claiming an impossible count.
        int[] values = [1, 2, 3, 4];
        bool[] valid = [true, false, false, true];
        var source = RawArrays.Fixed(Int32Type.Default, values, valid, nullCountOverride: -1);

        var result = Assert.IsType<Int64Array>(ArrowCompute.Widen(source, Int64Type.Default));

        Assert.Equal(2, result.Data.NullCount);
        Assert.True(result.IsNull(1));
        Assert.True(result.IsNull(2));
        Assert.False(result.IsNull(0));
        Assert.Equal(4, result.GetValue(3));
    }

    [Fact]
    public void Offset_Date32ToTimestampAlsoRebasesCorrectly()
    {
        int[] physical = [0, 1, 19_675, 19_676];
        bool[] valid = [true, true, false, true];
        var source = RawArrays.Fixed(Date32Type.Default, physical, valid, offset: 2);

        var result = Assert.IsType<TimestampArray>(
            ArrowCompute.Widen(source, new TimestampType(TimeUnit.Microsecond, (string?)null)));

        Assert.Equal(2, result.Length);
        Assert.True(result.IsNull(0));
        Assert.False(result.IsNull(1));
        Assert.Equal(19_676L * 86_400_000_000L, result.GetValue(1));
    }

    [Fact]
    public void ZeroLength_IsWellFormed()
    {
        var source = RawArrays.Fixed(Int32Type.Default, System.Array.Empty<int>());

        var result = ArrowCompute.Widen(source, Int64Type.Default);

        Assert.Equal(0, result.Length);
        Assert.Equal(0, result.Data.NullCount);
    }

    // ── Refusals ──

    [Theory]
    [InlineData("narrowing")]
    [InlineData("unrelated")]
    [InlineData("identity")]
    public void UnsupportedPair_Throws(string which)
    {
        var (source, target) = which switch
        {
            // A narrowing must not be silently accepted — it would truncate for some values only.
            "narrowing" => (RawArrays.Fixed(Int64Type.Default, new[] { 1L }), (IArrowType)Int32Type.Default),
            "unrelated" => (RawArrays.Fixed(Int32Type.Default, new[] { 1 }), FloatType.Default),
            // Identity is the caller's job to short-circuit; this kernel only widens.
            "identity" => (RawArrays.Fixed(Int32Type.Default, new[] { 1 }), Int32Type.Default),
            _ => throw new ArgumentOutOfRangeException(nameof(which)),
        };

        var ex = Assert.Throws<NotSupportedException>(() => ArrowCompute.Widen(source, target));
        Assert.Contains("ArrowCompute", ex.Message, StringComparison.Ordinal);
    }
}
