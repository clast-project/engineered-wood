// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using EngineeredWood.Iceberg.Values;

namespace EngineeredWood.Iceberg.Tests.Values;

/// <summary>
/// The exact text-to-binary rounding the netstandard2.0 build reads floats and doubles with. Checked on
/// every target: against fixed answers everywhere, and against .NET Core's correctly-rounded parser where
/// there is one.
/// </summary>
public class ExactFloatTextTests
{
    private static double Double(string text)
    {
        Assert.True(ExactFloatText.TryParseRational(text, out bool negative, out var num, out var den));
        long bits = ExactFloatText.RoundToBinary(num, den, 53, -1022, 1023);
        return BitConverter.Int64BitsToDouble(bits | (negative ? long.MinValue : 0));
    }

    private static float Single(string text)
    {
        Assert.True(ExactFloatText.TryParseRational(text, out bool negative, out var num, out var den));
        int bits = (int)ExactFloatText.RoundToBinary(num, den, 24, -126, 127);
        return BitConverter.ToSingle(BitConverter.GetBytes(bits | (negative ? int.MinValue : 0)), 0);
    }

    [Theory]
    [InlineData("0.0", 0x0000000000000000L)]
    [InlineData("-0.0", unchecked((long)0x8000000000000000UL))]
    [InlineData("1.0", 0x3FF0000000000000L)]
    [InlineData("0.1", 0x3FB999999999999AL)]
    [InlineData("4.9E-324", 0x0000000000000001L)]
    [InlineData("2.4703282292062328E-324", 0x0000000000000001L)] // just over half the smallest subnormal
    [InlineData("2.4703282292062327E-324", 0x0000000000000000L)] // just under
    [InlineData("1e-400", 0x0000000000000000L)]
    [InlineData("2.2250738585072014E-308", 0x0010000000000000L)] // smallest normal
    [InlineData("2.225073858507201E-308", 0x000FFFFFFFFFFFFFL)]  // largest subnormal
    [InlineData("1.7976931348623157E308", 0x7FEFFFFFFFFFFFFFL)]
    [InlineData("1.7976931348623159E308", 0x7FF0000000000000L)]  // rounds past the largest finite
    [InlineData("1e400", 0x7FF0000000000000L)]
    [InlineData("9007199254740993", 0x4340000000000000L)]          // 2^53 + 1, a tie, to even
    [InlineData("9007199254740995", 0x4340000000000002L)]          // 2^53 + 3, a tie, to even (up)
    [InlineData("3.465494185217035E-271", 0x07C76F2BB76635D8L)]    // .NET Framework reads ...D9
    public void Doubles(string text, long bits) =>
        Assert.Equal(bits, BitConverter.DoubleToInt64Bits(Double(text)));

    [Theory]
    [InlineData("1.0", 0x3F800000)]
    [InlineData("0.1", 0x3DCCCCCD)]
    [InlineData("1.4E-45", 0x00000001)]
    [InlineData("3.4028235E38", 0x7F7FFFFF)]
    [InlineData("3.4028235677973366E38", 0x7F7FFFFF)] // just under the half-way point to 2^128
    [InlineData("3.4028236E38", 0x7F800000)]          // past it: an infinity
    [InlineData("3.5E38", 0x7F800000)]
    [InlineData("1.00000005960464477539062", 0x3F800000)]   // just under the tie: down
    [InlineData("1.00000005960464477539063", 0x3F800001)]   // just over: up
    [InlineData("1.000000059604644775390625", 0x3F800000)]  // the tie itself: to even
    public void Singles(string text, int bits) =>
        Assert.Equal(bits, BitConverter.ToInt32(BitConverter.GetBytes(Single(text)), 0));

#if NET
    /// <summary>The platform parse is correctly rounded on .NET Core, so the two must agree everywhere.</summary>
    [Fact]
    public void AgreesWithTheNetCoreParser()
    {
        var random = new Random(586);
        var bytes = new byte[8];
        for (int i = 0; i < 20_000; i++)
        {
            random.NextBytes(bytes);
            double d = BitConverter.ToDouble(bytes, 0);
            if (double.IsNaN(d) || double.IsInfinity(d))
                continue;
            foreach (string format in new[] { "R", "G15", "G16", "E3" })
            {
                string text = d.ToString(format, CultureInfo.InvariantCulture);
                Assert.Equal(
                    BitConverter.DoubleToInt64Bits(double.Parse(text, CultureInfo.InvariantCulture)),
                    BitConverter.DoubleToInt64Bits(Double(text)));
            }

            float f = BitConverter.ToSingle(bytes, 0);
            if (float.IsNaN(f) || float.IsInfinity(f))
                continue;
            foreach (string format in new[] { "R", "G7", "G12" })
            {
                string text = f.ToString(format, CultureInfo.InvariantCulture);
                Assert.Equal(float.Parse(text, CultureInfo.InvariantCulture), Single(text));
            }
        }
    }
#endif
}
