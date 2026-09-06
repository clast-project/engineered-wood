// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;

namespace EngineeredWood.Expressions.Tests;

public class LiteralValueTests
{
    [Fact]
    public void Null_IsNull()
    {
        var v = LiteralValue.Null;
        Assert.True(v.IsNull);
        Assert.Equal(LiteralValue.Kind.Null, v.Type);
    }

    [Fact]
    public void Default_IsNull()
    {
        LiteralValue v = default;
        Assert.True(v.IsNull);
    }

    [Fact]
    public void Boolean_RoundTrips()
    {
        LiteralValue v = true;
        Assert.Equal(LiteralValue.Kind.Boolean, v.Type);
        Assert.True(v.AsBoolean);
    }

    [Fact]
    public void Int32_RoundTrips()
    {
        LiteralValue v = 42;
        Assert.Equal(LiteralValue.Kind.Int32, v.Type);
        Assert.Equal(42, v.AsInt32);
    }

    [Fact]
    public void Int64_RoundTrips()
    {
        LiteralValue v = 42L;
        Assert.Equal(LiteralValue.Kind.Int64, v.Type);
        Assert.Equal(42L, v.AsInt64);
    }

    [Fact]
    public void Double_RoundTrips()
    {
        LiteralValue v = 3.14;
        Assert.Equal(LiteralValue.Kind.Double, v.Type);
        Assert.Equal(3.14, v.AsDouble);
    }

    [Fact]
    public void String_RoundTrips()
    {
        LiteralValue v = "hello";
        Assert.Equal(LiteralValue.Kind.String, v.Type);
        Assert.Equal("hello", v.AsString);
    }

    [Fact]
    public void Binary_RoundTrips()
    {
        var bytes = new byte[] { 1, 2, 3, 4 };
        LiteralValue v = bytes;
        Assert.Equal(LiteralValue.Kind.Binary, v.Type);
        Assert.Equal(bytes, v.AsBinary);
    }

    [Fact]
    public void Decimal_RoundTrips()
    {
        LiteralValue v = 12.34m;
        Assert.Equal(LiteralValue.Kind.Decimal, v.Type);
        Assert.Equal(12.34m, v.AsDecimal);
    }

    [Fact]
    public void Guid_RoundTrips()
    {
        var g = Guid.NewGuid();
        LiteralValue v = g;
        Assert.Equal(LiteralValue.Kind.Guid, v.Type);
        Assert.Equal(g, v.AsGuid);
    }

    [Fact]
    public void DateTimeOffset_RoundTrips()
    {
        var dto = new DateTimeOffset(2024, 1, 15, 12, 30, 0, TimeSpan.FromHours(-5));
        LiteralValue v = dto;
        Assert.Equal(LiteralValue.Kind.DateTimeOffset, v.Type);
        Assert.Equal(dto, v.AsDateTimeOffset);
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void DateOnly_RoundTrips()
    {
        var d = new DateOnly(2024, 1, 15);
        LiteralValue v = d;
        Assert.Equal(LiteralValue.Kind.DateOnly, v.Type);
        Assert.Equal(d, v.AsDateOnly);
    }

    [Fact]
    public void TimeOnly_RoundTrips()
    {
        var t = new TimeOnly(12, 30, 0);
        LiteralValue v = t;
        Assert.Equal(LiteralValue.Kind.TimeOnly, v.Type);
        Assert.Equal(t, v.AsTimeOnly);
    }
#endif

    [Fact]
    public void HighPrecisionDecimal_RoundTrips()
    {
        // Decimal128 with precision exceeding System.decimal: 38 digits
        var unscaled = BigInteger.Parse("12345678901234567890123456789012345678");
        var v = LiteralValue.HighPrecisionDecimalOf(unscaled, 5);

        Assert.Equal(LiteralValue.Kind.HighPrecisionDecimal, v.Type);
        var (back, scale) = v.AsHighPrecisionDecimal;
        Assert.Equal(unscaled, back);
        Assert.Equal(5, scale);
    }

    [Fact]
    public void InvalidAccess_Throws()
    {
        LiteralValue v = 42;
        Assert.Throws<InvalidOperationException>(() => v.AsString);
    }

    // ── Equality ──

    [Fact]
    public void Equals_SameKind_SameValue()
    {
        Assert.Equal((LiteralValue)42, (LiteralValue)42);
        Assert.Equal((LiteralValue)"x", (LiteralValue)"x");
        Assert.Equal(LiteralValue.Of(new byte[] { 1, 2 }), LiteralValue.Of(new byte[] { 1, 2 }));
    }

    [Fact]
    public void Equals_DifferentKind_DifferentValue()
    {
        Assert.NotEqual((LiteralValue)42, (LiteralValue)43);
        Assert.NotEqual((LiteralValue)"a", (LiteralValue)"b");
    }

    /// <summary>
    /// Equality is representation-based, so two kinds are never equal -- while the SQL
    /// comparison still promotes across them. See <see cref="LiteralValue.Equals(LiteralValue)"/>
    /// for why the two cannot be one method (#206).
    /// </summary>
    [Fact]
    public void Equals_CrossTypeNumeric_IsFalse_WhileCompareToStillMatches()
    {
        Assert.NotEqual((LiteralValue)42, (LiteralValue)42L);
        Assert.NotEqual((LiteralValue)1, (LiteralValue)1.0d);
        Assert.NotEqual((LiteralValue)1.5f, (LiteralValue)1.5d);
        Assert.NotEqual(LiteralValue.Of(1m), LiteralValue.HighPrecisionDecimalOf(1, 0));

        // The SQL answer is unchanged. It is what the evaluators ask for, through
        // ArrowRowEvaluator.ValueEqual and StatisticsEvaluator.ConstantCompare.
        Assert.Equal(0, ((LiteralValue)42).CompareTo((LiteralValue)42L));
        Assert.Equal(0, ((LiteralValue)1).CompareTo((LiteralValue)1.0d));
        Assert.Equal(0, ((LiteralValue)1.5f).CompareTo((LiteralValue)1.5d));
        Assert.Equal(0, LiteralValue.Of(1m).CompareTo(LiteralValue.HighPrecisionDecimalOf(1, 0)));
    }

    [Fact]
    public void Null_EqualsNull()
    {
        Assert.Equal(LiteralValue.Null, LiteralValue.Null);
    }

    // ── Comparison ──

    [Fact]
    public void Compare_Int32_SameType()
    {
        Assert.True(((LiteralValue)1).CompareTo((LiteralValue)2) < 0);
        Assert.True(((LiteralValue)2).CompareTo((LiteralValue)1) > 0);
        Assert.Equal(0, ((LiteralValue)1).CompareTo((LiteralValue)1));
    }

    [Fact]
    public void Compare_String_Ordinal()
    {
        Assert.True(((LiteralValue)"a").CompareTo((LiteralValue)"b") < 0);
        Assert.True(((LiteralValue)"B").CompareTo((LiteralValue)"a") < 0); // ordinal: uppercase before lowercase
    }

    [Fact]
    public void Compare_CrossType_IntLong()
    {
        Assert.True(((LiteralValue)1).CompareTo((LiteralValue)2L) < 0);
        Assert.True(((LiteralValue)2L).CompareTo((LiteralValue)1) > 0);
        Assert.Equal(0, ((LiteralValue)42).CompareTo((LiteralValue)42L));
    }

    [Fact]
    public void Compare_CrossType_FloatDouble()
    {
        Assert.True(((LiteralValue)1.0f).CompareTo((LiteralValue)2.0) < 0);
        Assert.Equal(0, ((LiteralValue)1.5f).CompareTo((LiteralValue)1.5));
    }

    [Fact]
    public void Compare_CrossType_IntDouble()
    {
        Assert.True(((LiteralValue)1).CompareTo((LiteralValue)1.5) < 0);
    }

    [Fact]
    public void Compare_Null_SortsFirst()
    {
        Assert.True(LiteralValue.Null.CompareTo((LiteralValue)1) < 0);
        Assert.True(((LiteralValue)1).CompareTo(LiteralValue.Null) > 0);
        Assert.Equal(0, LiteralValue.Null.CompareTo(LiteralValue.Null));
    }

    [Fact]
    public void Compare_IncompatibleTypes_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
            ((LiteralValue)"x").CompareTo((LiteralValue)42));
    }

    [Fact]
    public void Compare_Binary_Lexicographic()
    {
        var a = LiteralValue.Of(new byte[] { 1, 2, 3 });
        var b = LiteralValue.Of(new byte[] { 1, 2, 4 });
        var c = LiteralValue.Of(new byte[] { 1, 2 });

        Assert.True(a.CompareTo(b) < 0);
        Assert.True(b.CompareTo(a) > 0);
        Assert.True(c.CompareTo(a) < 0); // shorter prefix sorts first
    }

    [Fact]
    public void Compare_HighPrecisionDecimal_SameScale()
    {
        var a = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("12345"), 2);
        var b = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("67890"), 2);
        Assert.True(a.CompareTo(b) < 0);
    }

    [Fact]
    public void Compare_HighPrecisionDecimal_DifferentScale()
    {
        // 1.23 (123 * 10^-2) vs 1.230 (1230 * 10^-3) — should compare equal
        var a = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("123"), 2);
        var b = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("1230"), 3);
        Assert.Equal(0, a.CompareTo(b));
    }

    // ── Cross-type: decimal / high-precision decimal / integer ──

    [Fact]
    public void Compare_Decimal_Vs_HighPrecisionDecimal()
    {
        // A plain decimal literal against a high-precision decimal column value (10^30, scale 0).
        var small = LiteralValue.Of(100m);
        var huge = LiteralValue.HighPrecisionDecimalOf(BigInteger.Pow(10, 30), 0);
        Assert.True(small.CompareTo(huge) < 0);
        Assert.True(huge.CompareTo(small) > 0);
    }

    [Fact]
    public void Compare_Decimal_Vs_HighPrecisionDecimal_Equal()
    {
        // 12.34 as System.Decimal vs the same value as unscaled 1234 * 10^-2.
        Assert.Equal(0, LiteralValue.Of(12.34m).CompareTo(
            LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("1234"), 2)));
    }

    [Fact]
    public void Compare_Integer_Vs_Decimal_Exact()
    {
        // 10 (int) vs 5.00 / 15.00 (decimal) — exact, not via lossy double.
        Assert.True(((LiteralValue)10).CompareTo(LiteralValue.Of(5.00m)) > 0);
        Assert.True(((LiteralValue)10).CompareTo(LiteralValue.Of(15.00m)) < 0);
        Assert.Equal(0, ((LiteralValue)10).CompareTo(LiteralValue.Of(10m)));
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void Compare_DateOnly_Vs_DateTimeOffset_AsUtcMidnight()
    {
        var date = LiteralValue.Of(new DateOnly(2021, 6, 1));
        var midnight = LiteralValue.Of(new DateTimeOffset(2021, 6, 1, 0, 0, 0, TimeSpan.Zero));
        var later = LiteralValue.Of(new DateTimeOffset(2021, 6, 1, 12, 0, 0, TimeSpan.Zero));
        Assert.Equal(0, date.CompareTo(midnight));
        Assert.True(date.CompareTo(later) < 0);
    }
#endif

    // ── Operators ──

    [Fact]
    public void Operators_LessThanGreaterThan()
    {
        Assert.True((LiteralValue)1 < (LiteralValue)2);
        Assert.True((LiteralValue)2 > (LiteralValue)1);
        Assert.True((LiteralValue)1 <= (LiteralValue)1);
        Assert.True((LiteralValue)1 >= (LiteralValue)1);
    }

    // ── Hash ──

    [Fact]
    public void Hash_EqualValues_EqualHashes()
    {
        Assert.Equal(((LiteralValue)42).GetHashCode(), ((LiteralValue)42).GetHashCode());
        Assert.Equal(((LiteralValue)"x").GetHashCode(), ((LiteralValue)"x").GetHashCode());
        Assert.Equal(
            LiteralValue.Of(new byte[] { 1, 2 }).GetHashCode(),
            LiteralValue.Of(new byte[] { 1, 2 }).GetHashCode());
    }

    [Fact]
    public void ToObject_BoxesValue()
    {
        Assert.Equal((object)42, ((LiteralValue)42).ToObject());
        Assert.Equal((object)"x", ((LiteralValue)"x").ToObject());
        Assert.Null(LiteralValue.Null.ToObject());
    }

    // ── Decimal against floating point (#171) ──

    [Fact]
    public void HighPrecisionDecimal_ComparesAgainstDouble_ThroughDouble()
    {
        // 2^53+1, which no double holds. Spark compares the pair through double and calls them
        // equal; measured, and pinned end-to-end in ArrowRowEvaluatorTests.
        var d = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("9007199254740993"), 0);
        Assert.Equal(0, d.CompareTo(LiteralValue.Of(9007199254740992d)));

        // The same pair against an INTEGER stays exact, so it is NOT equal.
        Assert.NotEqual(0, d.CompareTo(LiteralValue.Of(9007199254740992L)));
    }

    [Fact]
    public void HighPrecisionDecimal_ToDouble_RoundsRatherThanTruncating()
    {
        // (double)BigInteger truncates: 10^30 becomes 9.999999999999999e29, one ulp below the
        // 1e30 Spark produces. The conversion formats and parses once instead.
        var d = LiteralValue.HighPrecisionDecimalOf(BigInteger.Pow(10, 30), 0);
        Assert.Equal(0, d.CompareTo(LiteralValue.Of(1e30d)));
    }

    [Fact]
    public void HighPrecisionDecimal_WithNegativeScale_ComparesRatherThanThrowing()
    {
        // A negative scale means the value is unscaled * 10^|scale|. The exponent carries its own
        // sign, so building it as "E-" + scale would render "123E--2" and throw. Scale is normally
        // >= 0, but HighPrecisionDecimalOf is public and the format accessors pass through
        // whatever a file's metadata claims.
        var d = LiteralValue.HighPrecisionDecimalOf(new BigInteger(123), -2);   // 12300
        Assert.Equal(0, d.CompareTo(LiteralValue.Of(12300d)));
        Assert.True(d.CompareTo(LiteralValue.Of(12299d)) > 0);
    }

    [Theory]
    // A scale extreme enough to push the value outside double's range. Since .NET Core 3.0
    // parsing saturates; on .NET Framework the same input is an error, and a comparison may not
    // throw -- callers turn only InvalidOperationException into a SQL null. Both targets saturate
    // now, so these answers are the same everywhere.
    [InlineData(1, -30000, double.PositiveInfinity)]
    [InlineData(-1, -30000, double.NegativeInfinity)]
    [InlineData(1, 30000, 0d)]
    [InlineData(-1, 30000, 0d)]
    [InlineData(0, -30000, 0d)]
    public void HighPrecisionDecimal_WithAnOutOfRangeScale_SaturatesRatherThanThrowing(
        int unscaled, int scale, double expected)
    {
        var d = LiteralValue.HighPrecisionDecimalOf(new BigInteger(unscaled), scale);

        // Comparing against the saturated value is how the conversion is observable from outside.
        Assert.Equal(0, d.CompareTo(LiteralValue.Of(expected)));
    }

    [Theory]
    // A mantissa large enough to convert to Infinity on its own, paired with a scale large enough
    // to underflow a power of ten to zero. Computing the saturation as
    // (double)unscaled * Math.Pow(10, -scale) yields Infinity * 0, which is NaN -- and since #204
    // NaN sorts ABOVE every value, so the answer would not merely be wrong, it would compare
    // greater than everything. 10^1000 at scale 400 is 10^600, genuinely past double's ceiling.
    [InlineData(1000, 400, false)]
    [InlineData(1000, 400, true)]
    public void HighPrecisionDecimal_WithAHugeMantissaAndScale_SaturatesToInfinityNotNaN(
        int mantissaDigits, int scale, bool negative)
    {
        var unscaled = BigInteger.Pow(10, mantissaDigits);
        if (negative) unscaled = -unscaled;

        var d = LiteralValue.HighPrecisionDecimalOf(unscaled, scale);
        var expected = negative ? double.NegativeInfinity : double.PositiveInfinity;

        Assert.Equal(0, d.CompareTo(LiteralValue.Of(expected)));

        // Stated separately because NaN would satisfy neither, and would fail loudly here rather
        // than quietly ordering itself at the top.
        Assert.NotEqual(0, d.CompareTo(LiteralValue.Of(double.NaN)));
    }

    // ── NaN sits at the top of the order (#204) ──

    [Theory]
    // Measured against Spark 4.0: -Infinity < finite < +Infinity < NaN, confirmed by sort_array
    // returning [-inf, 1.0, inf, nan]. .NET puts NaN at the bottom, so all of these were inverted.
    [InlineData(double.NegativeInfinity, double.NaN, -1)]
    [InlineData(double.PositiveInfinity, double.NaN, -1)]
    [InlineData(1.0, double.NaN, -1)]
    [InlineData(double.NaN, 1.0, 1)]
    [InlineData(double.NaN, double.NegativeInfinity, 1)]
    // NaN equals itself, which .NET already agreed with and Spark confirms (`NaN = NaN` is true).
    [InlineData(double.NaN, double.NaN, 0)]
    // The rest of the order is untouched.
    [InlineData(double.NegativeInfinity, 1.0, -1)]
    [InlineData(double.PositiveInfinity, 1.0, 1)]
    // -0.0 equals 0.0 in Spark, and double.CompareTo already compares by value rather than by
    // total order, so this holds before and after. Pinned because the two conventions differ here.
    [InlineData(-0.0, 0.0, 0)]
    [InlineData(0.0, -0.0, 0)]
    public void Double_OrdersNaNAboveEveryValue(double a, double b, int expected)
    {
        Assert.Equal(expected, Math.Sign(LiteralValue.Of(a).CompareTo(LiteralValue.Of(b))));
    }

    [Theory]
    // The SAME-KIND float arm, which the cross-width test below does not reach: both operands are
    // Kind.Float, so this is the only thing that exercises it. Review feedback on #213 -- the
    // earlier version asserted only nanF vs nanF, which was already zero before the change.
    [InlineData(1.0f, float.NaN, -1)]
    [InlineData(float.NaN, 1.0f, 1)]
    [InlineData(float.PositiveInfinity, float.NaN, -1)]
    [InlineData(float.NegativeInfinity, float.NaN, -1)]
    [InlineData(float.NaN, float.NaN, 0)]
    [InlineData(1.0f, 2.0f, -1)]
    public void Float_OrdersNaNAboveEveryValue(float a, float b, int expected)
    {
        Assert.Equal(expected, Math.Sign(LiteralValue.Of(a).CompareTo(LiteralValue.Of(b))));
    }

#if NET6_0_OR_GREATER
    [Theory]
    // The Half arm, which nothing exercised at all.
    [InlineData(1.0, double.NaN, -1)]
    [InlineData(double.NaN, 1.0, 1)]
    [InlineData(double.NaN, double.NaN, 0)]
    [InlineData(1.0, 2.0, -1)]
    public void Half_OrdersNaNAboveEveryValue(double a, double b, int expected)
    {
        // Built from double because [InlineData] cannot carry a Half.
        var ha = LiteralValue.Of(double.IsNaN(a) ? Half.NaN : (Half)a);
        var hb = LiteralValue.Of(double.IsNaN(b) ? Half.NaN : (Half)b);

        Assert.Equal(expected, Math.Sign(ha.CompareTo(hb)));
    }
#endif

    [Fact]
    public void Float_AndCrossWidth_OrderNaNTheSameWay()
    {
        // The same rule has to reach the float arm and the cross-type arm, or a float NaN and a
        // double NaN would disagree with each other. Measured: Spark's `CAST('NaN' AS FLOAT) =
        // CAST('NaN' AS DOUBLE)` is TRUE and `CAST('NaN' AS FLOAT) < CAST('Infinity' AS DOUBLE)`
        // is FALSE.
        var nanF = LiteralValue.Of(float.NaN);
        var nanD = LiteralValue.Of(double.NaN);
        var infD = LiteralValue.Of(double.PositiveInfinity);

        Assert.Equal(0, nanF.CompareTo(nanF));
        Assert.Equal(0, nanF.CompareTo(nanD));          // cross-width, through the double branch
        Assert.True(nanF.CompareTo(infD) > 0);
        Assert.True(LiteralValue.Of(1.0f).CompareTo(nanD) < 0);
    }
}
