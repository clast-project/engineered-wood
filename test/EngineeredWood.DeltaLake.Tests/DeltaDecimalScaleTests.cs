// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// A decimal bound carries the column's DECLARED scale, not the one its text happened to be
/// written with.
/// </summary>
/// <remarks>
/// <para>
/// #323, found in review. The scale on a <see cref="LiteralValue"/> is read downstream as a fact
/// about the column's TYPE — <c>StatisticsEvaluator</c> uses it to decide whether Spark would
/// round the comparison, and so whether a file may be skipped. Everywhere else it comes from a
/// declared type: Parquet takes it from the <c>DecimalType</c> logical annotation and Vortex from
/// the Arrow <c>Decimal128Type</c>. Only this decoder took it from the formatting, which is a fact
/// about the writer.
/// </para>
/// <para>
/// It moves in both directions. JSON does not keep a trailing zero, so a <c>decimal(10,2)</c>
/// bound of 1.00 arrives as <c>1</c> and used to carry scale 0; a writer that pads instead sends
/// <c>1.00</c> for a <c>decimal(38,0)</c> and used to carry scale 2. Measured before the fix: both.
/// </para>
/// </remarks>
public class DeltaDecimalScaleTests
{
    [Theory]
    [InlineData("1", "decimal(10,2)", 2)]          // JSON dropped the trailing zeros
    [InlineData("1.00", "decimal(38,0)", 0)]       // the writer padded past the declared scale
    [InlineData("0.99", "decimal(10,2)", 2)]
    [InlineData("1.5", "decimal(38,30)", 30)]
    [InlineData("-2.25", "decimal(9,4)", 4)]
    public void APartitionDecimalTakesTheDeclaredScale(string text, string typeName, int expected) =>
        Assert.Equal(expected, ScaleOf(DeltaLiteralDecoder.FromPartitionString(text, typeName)));

    /// <summary>Rescaling moves the digits with the point, so the VALUE is untouched.</summary>
    [Theory]
    [InlineData("1", "decimal(10,2)", "1")]
    [InlineData("1.00", "decimal(38,0)", "1")]
    [InlineData("0.99", "decimal(10,4)", "0.99")]
    public void RescalingDoesNotMoveTheValue(string text, string typeName, string same)
    {
        var rescaled = DeltaLiteralDecoder.FromPartitionString(text, typeName)!.Value;
        var plain = DeltaLiteralDecoder.FromPartitionString(same, "decimal(38,18)")!.Value;

        Assert.Equal(0, rescaled.CompareTo(plain));
    }

    /// <summary>
    /// A statistic with more decimals than its column declares is refused, not rounded.
    /// </summary>
    /// <remarks>
    /// Such a value contradicts its own type and cannot have come from the column, so there is no
    /// scale to move it to without moving the bound. Null reaches the evaluator as Unknown, which
    /// skips no data — the one safe answer.
    /// </remarks>
    [Theory]
    [InlineData("1.25", "decimal(10,1)")]
    [InlineData("0.001", "decimal(38,2)")]
    public void AStatisticDeeperThanItsColumnIsRefused(string text, string typeName) =>
        Assert.Null(DeltaLiteralDecoder.FromPartitionString(text, typeName));

    private static int ScaleOf(LiteralValue? value)
    {
        var v = value!.Value;

        return v.Type == LiteralValue.Kind.Decimal
            ? (decimal.GetBits(v.AsDecimal)[3] >> 16) & 0xFF
            : v.AsHighPrecisionDecimal.Scale;
    }
}
