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
}
