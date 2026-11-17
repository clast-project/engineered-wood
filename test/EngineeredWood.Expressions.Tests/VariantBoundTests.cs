// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// <see cref="VariantBound.ToLiteral"/>: a variant path's bound converts to <c>variant_get</c>'s target type
/// only exactly, and never across kinds Spark would cast between (#682, shared with Delta's #677).
/// </summary>
public class VariantBoundTests
{
    [Theory]
    [InlineData("tinyint", 127L, true)]
    [InlineData("tinyint", 128L, false)]
    [InlineData("smallint", -32768L, true)]
    [InlineData("smallint", 32768L, false)]
    [InlineData("int", 2147483647L, true)]
    [InlineData("int", 2147483648L, false)]
    [InlineData("bigint", long.MinValue, true)]
    public void Integer_ConvertsToAnIntegerTypeItFits(string target, long value, bool converts)
    {
        var literal = VariantBound.Integer(VariantBoundKind.Int64, value).ToLiteral(target);
        Assert.Equal(converts, literal is not null);
        if (literal is { } l)
            Assert.Equal(value, target == "bigint" ? l.AsInt64 : l.AsInt32);
    }

    [Theory]
    [InlineData(12345L, 2, "decimal(10,2)", "123.45")]
    [InlineData(12345L, 2, "decimal(10,4)", "123.4500")]   // padding the scale is exact
    [InlineData(12345L, 2, "decimal(10,1)", null)]         // dropping a digit would round
    [InlineData(12345L, 2, "decimal(4,2)", null)]          // too many digits for the precision
    [InlineData(-99999L, 0, "decimal(5,0)", "-99999")]
    public void Decimal_ConvertsToADecimalOfAtLeastItsScaleThatItFits(
        long unscaled, int scale, string target, string? expected)
    {
        var literal = VariantBound.Decimal(new BigInteger(unscaled), scale).ToLiteral(target);
        Assert.Equal(expected, literal?.AsDecimal.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    [Fact]
    public void Integer_ConvertsToADecimal()
    {
        Assert.Equal(7.00m, VariantBound.Integer(VariantBoundKind.Int32, 7).ToLiteral("decimal(3,2)")!.Value.AsDecimal);
        Assert.Null(VariantBound.Integer(VariantBoundKind.Int32, 70).ToLiteral("decimal(3,2)"));
    }

    [Fact]
    public void Floats_WidenToDouble_AndNotTheOtherWay()
    {
        Assert.Equal(1.5, VariantBound.Float(1.5f).ToLiteral("double")!.Value.AsDouble);
        Assert.Equal(1.5f, VariantBound.Float(1.5f).ToLiteral("float")!.Value.AsFloat);
        Assert.Null(VariantBound.Double(1.5).ToLiteral("float"));
        Assert.Null(VariantBound.Double(1.5).ToLiteral("decimal(10,2)"));
        Assert.Null(VariantBound.Integer(VariantBoundKind.Int32, 1).ToLiteral("double"));
    }

    [Fact]
    public void OtherKinds_ConvertOnlyToTheirOwnType()
    {
        // Spark converts "7" to 7, but a string bound's order is not an integer's.
        Assert.Null(VariantBound.String("7").ToLiteral("int"));
        Assert.Equal("7", VariantBound.String("7").ToLiteral("string")!.Value.AsString);
        Assert.Null(VariantBound.Integer(VariantBoundKind.Int8, 1).ToLiteral("string"));
        Assert.Null(VariantBound.Integer(VariantBoundKind.Int8, 1).ToLiteral("boolean"));
        Assert.True(VariantBound.Boolean(true).ToLiteral("boolean")!.Value.AsBoolean);

        Assert.Equal(
            new DateTimeOffset(2026, 11, 8, 0, 0, 0, TimeSpan.Zero),
            VariantBound.Date(20765).ToLiteral("date")!.Value.AsDateTimeOffset);
        Assert.Null(VariantBound.Date(20765).ToLiteral("timestamp"));

        Assert.NotNull(VariantBound.Timestamp(0).ToLiteral("timestamp"));
        Assert.Null(VariantBound.Timestamp(0).ToLiteral("timestamp_ntz"));
        Assert.Null(VariantBound.TimestampNtz(0).ToLiteral("timestamp"));
        Assert.Null(VariantBound.Timestamp(0).ToLiteral("variant"));
        Assert.Null(VariantBound.Timestamp(0).ToLiteral("binary"));
    }

    [Fact]
    public void Temporal_OutsideDateTimeRange_GivesNoBound()
    {
        Assert.Null(VariantBound.Timestamp(long.MaxValue).ToLiteral("timestamp"));
        Assert.Null(VariantBound.Date(int.MaxValue).ToLiteral("date"));
    }

    // ───── TryInvert: the leaf value a probe asks for (#772) ─────

    private static VariantBound? Invert(VariantBoundKind kind, string target, LiteralValue literal, int scale = 0)
    {
        Assert.True(VariantBound.TryInvert(kind, scale, target, literal, out var value),
            $"{kind} as {target} = {literal} declined");
        // The inverse and ToLiteral are one rule: what the inverse finds converts back to the literal exactly.
        if (value is { } found)
            Assert.Equal(0, found.ToLiteral(target)!.Value.CompareTo(literal));
        return value;
    }

    private static void Declines(VariantBoundKind kind, string target, LiteralValue literal, int scale = 0) =>
        Assert.False(VariantBound.TryInvert(kind, scale, target, literal, out _), $"{kind} as {target} = {literal}");

    [Theory]
    [InlineData(VariantBoundKind.Int64, "bigint")]
    [InlineData(VariantBoundKind.Int64, "int")]
    [InlineData(VariantBoundKind.Int8, "int")]
    [InlineData(VariantBoundKind.Int32, "tinyint")]
    public void Invert_IntegerLeaf_IntegerTarget_IsTheLiteral(VariantBoundKind kind, string target)
    {
        Assert.Equal(7L, Invert(kind, target, LiteralValue.Of(7))!.Value.AsInteger);
        Assert.Equal(7L, Invert(kind, target, LiteralValue.Of(7L))!.Value.AsInteger);
        Assert.Equal(7L, Invert(kind, target, LiteralValue.Of(7.00m))!.Value.AsInteger);
    }

    [Fact]
    public void Invert_IntegerLeaf_ValueTheLeafCannotHold_MatchesNone()
    {
        // An int8 leaf holds nothing of 300, though an int result can be 300.
        Assert.Null(Invert(VariantBoundKind.Int8, "int", LiteralValue.Of(300)));
    }

    [Fact]
    public void Invert_LiteralNotExactlyOfTheTarget_Declines()
    {
        Declines(VariantBoundKind.Int64, "int", LiteralValue.Of(7.5m));
        Declines(VariantBoundKind.Int64, "tinyint", LiteralValue.Of(1000));
        Declines(VariantBoundKind.Int64, "int", LiteralValue.Of(7.0)); // a double is not an exact number here
        Declines(VariantBoundKind.Int64, "bigint", LiteralValue.Of("7"));
        Declines(VariantBoundKind.Int64, "bigint", LiteralValue.Null);
    }

    /// <summary>
    /// Pairs Spark converts loosely, measured against Spark 4.0 with <c>try_variant_get</c>: a string "7" is int 7, a
    /// double 1.5 and a decimal 1.5 are int 1, a decimal 1.25 is decimal(5,1) 1.3, true is int 1, an int is double
    /// 7.0, a date is a timestamp, a double 0.1 is a float. "No leaf value matches" would be wrong for each.
    /// </summary>
    [Fact]
    public void Invert_PairSparkConvertsLoosely_Declines()
    {
        Declines(VariantBoundKind.String, "int", LiteralValue.Of(7));
        Declines(VariantBoundKind.Double, "int", LiteralValue.Of(1));
        Declines(VariantBoundKind.Decimal, "int", LiteralValue.Of(1), scale: 1);
        Declines(VariantBoundKind.Decimal, "decimal(5,1)", LiteralValue.Of(1.3m), scale: 2);
        Declines(VariantBoundKind.Boolean, "int", LiteralValue.Of(1));
        Declines(VariantBoundKind.Int32, "double", LiteralValue.Of(7.0));
        Declines(VariantBoundKind.Date, "timestamp", LiteralValue.Of(new DateTimeOffset(2024, 1, 2, 0, 0, 0, TimeSpan.Zero)));
        Declines(VariantBoundKind.Double, "float", LiteralValue.Of(0.1f));
        Declines(VariantBoundKind.Int64, "string", LiteralValue.Of("7"));
    }

    [Theory]
    // The issue's example: decimal(10,2) 1.50 against a DECIMAL(9,1) leaf is 15 at scale 1.
    [InlineData("1.50", 1, "decimal(10,2)", "15")]
    [InlineData("1.5", 1, "decimal(10,2)", "15")]
    [InlineData("1.55", 1, "decimal(10,2)", null)]  // needs more scale than the leaf has
    [InlineData("-3", 0, "decimal(10,2)", "-3")]
    [InlineData("1.5", 1, "decimal(10,1)", "15")]
    public void Invert_DecimalLeaf_DecimalTarget(string literal, int leafScale, string target, string? unscaled)
    {
        var value = Invert(VariantBoundKind.Decimal, target,
            LiteralValue.Of(decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture)), leafScale);
        Assert.Equal(unscaled, value?.AsDecimal.Unscaled.ToString(System.Globalization.CultureInfo.InvariantCulture));
        if (value is { } v)
            Assert.Equal(leafScale, v.AsDecimal.Scale);
    }

    [Fact]
    public void Invert_IntegerLeaf_DecimalTarget()
    {
        Assert.Equal(7L, Invert(VariantBoundKind.Int32, "decimal(5,2)", LiteralValue.Of(7.00m))!.Value.AsInteger);
        Assert.Null(Invert(VariantBoundKind.Int32, "decimal(5,2)", LiteralValue.Of(7.25m)));
        // Past the declared precision the literal is no decimal(5,2) at all.
        Declines(VariantBoundKind.Int32, "decimal(5,2)", LiteralValue.Of(1000));
        // A high-precision literal that is exact in the type.
        Assert.Equal(7L, Invert(VariantBoundKind.Int64, "decimal(38,2)",
            LiteralValue.HighPrecisionDecimalOf(new BigInteger(700), 2))!.Value.AsInteger);
    }

    [Fact]
    public void Invert_FloatLeaf_DoubleTarget_OnlyADoubleThatIsAFloat()
    {
        // Spark 4.0 widens a float exactly: float 0.1 reads as double 0.10000000149011612, not 0.1.
        Assert.Null(Invert(VariantBoundKind.Float, "double", LiteralValue.Of(0.1)));
        Assert.Equal(0.1f, (float)Invert(VariantBoundKind.Float, "double", LiteralValue.Of((double)0.1f))!.Value.AsReal);
        Assert.Equal(1.5, Invert(VariantBoundKind.Float, "double", LiteralValue.Of(1.5))!.Value.AsReal);
        Assert.Equal(1.5, Invert(VariantBoundKind.Float, "float", LiteralValue.Of(1.5f))!.Value.AsReal);
        Assert.Equal(7.0, Invert(VariantBoundKind.Double, "double", LiteralValue.Of(7))!.Value.AsReal);
        Assert.True(VariantBound.TryInvert(VariantBoundKind.Float, 0, "double", LiteralValue.Of(double.NaN), out var nan));
        Assert.True(double.IsNaN(nan!.Value.AsReal));
    }

    [Fact]
    public void Invert_OwnTypes()
    {
        Assert.Equal("x", Invert(VariantBoundKind.String, "string", LiteralValue.Of("x"))!.Value.AsString);
        Assert.Equal(1L, Invert(VariantBoundKind.Boolean, "boolean", LiteralValue.Of(true))!.Value.AsInteger);
        Assert.Equal(20765L, Invert(VariantBoundKind.Date, "date",
            LiteralValue.Of(new DateTimeOffset(2026, 11, 8, 0, 0, 0, TimeSpan.Zero)))!.Value.AsInteger);
        Declines(VariantBoundKind.Date, "date", LiteralValue.Of(new DateTimeOffset(2026, 11, 8, 1, 0, 0, TimeSpan.Zero)));
        Assert.Equal(1_500_000L, Invert(VariantBoundKind.Timestamp, "timestamp",
            LiteralValue.Of(new DateTimeOffset(1970, 1, 1, 0, 0, 1, 500, TimeSpan.Zero)))!.Value.AsInteger);
        Declines(VariantBoundKind.TimestampNtz, "timestamp",
            LiteralValue.Of(new DateTimeOffset(1970, 1, 1, 0, 0, 1, TimeSpan.Zero)));
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void Invert_DateLeaf_DateOnlyLiteral()
    {
        Assert.Equal(20765L, Invert(VariantBoundKind.Date, "date", LiteralValue.Of(new DateOnly(2026, 11, 8)))!.Value.AsInteger);
    }
#endif

    /// <summary>
    /// Spark compares a decimal at the two sides' least common type. Where that rounds either side the inverse
    /// declines (#901 review): <c>decimal(38,38)</c> against <c>0</c> is compared at <c>decimal(38,37)</c>.
    /// </summary>
    [Fact]
    public void Invert_ComparisonThatRounds_Declines()
    {
        Declines(VariantBoundKind.Decimal, "decimal(38,38)", LiteralValue.Of(0), scale: 38);
        Declines(VariantBoundKind.Decimal, "decimal(38,30)", LiteralValue.Of(123456789m), scale: 30);
        // A literal with more scale than fits beside the result's integral digits is the one rounded.
        Declines(VariantBoundKind.Int64, "bigint", LiteralValue.HighPrecisionDecimalOf(new BigInteger(7) * BigInteger.Pow(10, 19), 19));
        // One digit and 29 places fit; an IN member types an int at its full 10 digits, which does not.
        Assert.NotNull(Invert(VariantBoundKind.Decimal, "decimal(30,29)", LiteralValue.Of(1), scale: 29));
        Assert.True(VariantBound.TryInvert(VariantBoundKind.Decimal, 29, "decimal(30,29)", LiteralValue.Of(1), out _,
            setMembership: false));
        Assert.False(VariantBound.TryInvert(VariantBoundKind.Decimal, 29, "decimal(30,29)", LiteralValue.Of(1), out _,
            setMembership: true));
        // Two integrals are never unified as decimals.
        Assert.NotNull(Invert(VariantBoundKind.Int64, "bigint", LiteralValue.Of(long.MaxValue)));
    }

    /// <summary>
    /// A literal no Spark type holds is declined before its digits are used: a 50-million-place scale, or an unscaled
    /// value of 200,000 digits, would otherwise cost a huge power of ten or a quadratic digit count (#901 review).
    /// </summary>
    [Fact]
    public void Invert_EnormousLiteral_DeclinesAtOnce()
    {
        var watch = System.Diagnostics.Stopwatch.StartNew();
        Declines(VariantBoundKind.Int64, "bigint", LiteralValue.HighPrecisionDecimalOf(BigInteger.One, 50_000_000));
        Declines(VariantBoundKind.Int64, "bigint", LiteralValue.HighPrecisionDecimalOf(BigInteger.Zero, 50_000_000));
        Declines(VariantBoundKind.Decimal, "decimal(38,2)", LiteralValue.HighPrecisionDecimalOf(BigInteger.One, 50_000_000), scale: 2);
        // A 200,000-digit unscaled value: counting its digits one division at a time is quadratic.
        var huge = BigInteger.Pow(10, 200_000);
        watch.Restart();
        // TryInvert directly: Declines would format the literal into its message, which is slow itself.
        Assert.False(VariantBound.TryInvert(VariantBoundKind.Int64, 0, "bigint", LiteralValue.HighPrecisionDecimalOf(huge, 0), out _));
        Assert.False(VariantBound.TryInvert(VariantBoundKind.Decimal, 2, "decimal(38,2)",
            LiteralValue.HighPrecisionDecimalOf(huge, 2), out _));
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(1), $"took {watch.Elapsed}");
    }
}
