// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Numerics;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <see cref="DecimalText"/> refuses text describing a value no Delta decimal can hold BEFORE building it.
/// Before #760, <c>1e50000000</c> took 81 s and 910 MB to build a value every caller then discarded.
/// </summary>
public class DecimalTextTests
{
    [Theory]
    [InlineData("1e2147483647")]
    [InlineData("1e50000000")]
    [InlineData("-1e999999999")]
    [InlineData("1e-2147483648")]
    [InlineData("1e-50000000")]
    [InlineData("1e39")]
    [InlineData("1234567890123456789012345678901234567890")] // 40 integer digits
    [InlineData("1.5e38")] // 39 integer digits
    [InlineData("0.000000000000000000000000000000000000001")] // a non-zero 39th decimal
    // 38 integer digits plus a fraction: 39 digits of precision, which no decimal(38,s) holds.
    [InlineData("12345678901234567890123456789012345678.1")]
    [InlineData("-12345678901234567890123456789012345678.10")]
    [InlineData("1.23456789012345678901234567890123456789")]
    [InlineData("1234567890123456789.01234567890123456789e1")]
    public void OutOfRangeValueIsRefusedAtOnce(string text)
    {
        var watch = Stopwatch.StartNew();
        Assert.False(DecimalText.TryParse(text, out _, out _));
        Assert.True(watch.ElapsedMilliseconds < 1000, $"took {watch.ElapsedMilliseconds} ms");
    }

    [Theory]
    [InlineData("12345678901234567890123456789012345678", "12345678901234567890123456789012345678", 0)]
    [InlineData("-12345678901234567890123456789012345678", "-12345678901234567890123456789012345678", 0)]
    [InlineData("1.5e37", "15000000000000000000000000000000000000", 0)]
    [InlineData("9.9e37", "99000000000000000000000000000000000000", 0)]
    [InlineData("1.23e-4", "123", 6)]
    [InlineData("0.00000000000000000000000000000000000001", "1", 38)]
    [InlineData("1.50", "150", 2)]
    [InlineData("1e0", "1", 0)]
    [InlineData("+7", "7", 0)]
    // Leading zeros are not significant digits.
    [InlineData("000000000000000000000000000000000000000001e37", "10000000000000000000000000000000000000", 0)]
    // Trailing zeros past scale 38 are dropped: the value is unchanged.
    [InlineData("1.0000000000000000000000000000000000000000", "100000000000000000000000000000000000000", 38)]
    [InlineData("100e-40", "1", 38)]
    // Trailing fractional zeros don't count toward precision: a narrower column's rescale drops them.
    [InlineData("12345678901234567890123456789012345678.000", "12345678901234567890123456789012345678000", 3)]
    [InlineData("1.234567890123456789012345678901234567", "1234567890123456789012345678901234567", 36)]
    public void InRangeValueParsesExactly(string text, string unscaled, int scale)
    {
        Assert.True(DecimalText.TryParse(text, out var actual, out int actualScale));
        Assert.Equal(BigInteger.Parse(unscaled), actual);
        Assert.Equal(scale, actualScale);
    }

    [Theory]
    [InlineData("0e2147483647", 0)]
    [InlineData("-0e-2147483648", 38)]
    [InlineData("0.00", 2)]
    public void ZeroParsesAtAnyExponent(string text, int scale)
    {
        Assert.True(DecimalText.TryParse(text, out var actual, out int actualScale));
        Assert.True(actual.IsZero);
        Assert.Equal(scale, actualScale);
    }

    [Theory]
    [InlineData("")]
    [InlineData("1e")]
    [InlineData("1e1.5")]
    [InlineData("1x")]
    [InlineData(".")]
    [InlineData("-")]
    public void MalformedTextIsRefused(string text) =>
        Assert.False(DecimalText.TryParse(text, out _, out _));

    [Theory]
    [InlineData(0, int.MaxValue)]
    [InlineData(int.MaxValue, 0)]
    [InlineData(0, 39)]
    [InlineData(-1, 2)]
    public void RescaleRefusesScalesNoDecimalHas(int from, int to) =>
        Assert.False(DecimalText.TryRescale(BigInteger.One, from, to, out _));

    [Fact]
    public void RescaleStillWidensAndTrimsZeros()
    {
        Assert.True(DecimalText.TryRescale(15, 1, 38, out var widened));
        Assert.Equal(BigInteger.Parse("15" + new string('0', 37)), widened);

        Assert.True(DecimalText.TryRescale(150, 2, 1, out var trimmed));
        Assert.Equal(new BigInteger(15), trimmed);

        Assert.False(DecimalText.TryRescale(151, 2, 1, out _));
    }
}
