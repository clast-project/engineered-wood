// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Text.Json;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// Decoding Delta stats/partition values into <see cref="LiteralValue"/>. The focus is decimals: a
/// bound must be decoded EXACTLY, because the pruner uses it to prove a file cannot match — a value
/// rounded to System.Decimal's ~28-29 significant digits could shift a min/max bound and wrongly skip a
/// file (silent data loss).
/// </summary>
public class DeltaLiteralDecoderTests
{
    // A standalone JsonElement for a JSON literal (cloned so it outlives the document).
    private static JsonElement Json(string literal)
    {
        using var doc = JsonDocument.Parse(literal);
        return doc.RootElement.Clone();
    }

    [Fact]
    public void Decimal_InRange_UsesSystemDecimal()
    {
        var lit = DeltaLiteralDecoder.FromJson(Json("12.34"), "decimal(12,2)");
        Assert.NotNull(lit);
        Assert.Equal(LiteralValue.Kind.Decimal, lit!.Value.Type);
        Assert.Equal(LiteralValue.Of(12.34m), lit.Value);
    }

    [Fact]
    public void Decimal_BeyondSystemDecimalMagnitude_DecodedExactly()
    {
        // 10^31 + 1 — a 32-digit integer that overflows System.Decimal's range.
        var big = BigInteger.Pow(10, 31) + 1;
        var lit = DeltaLiteralDecoder.FromJson(Json(big.ToString()), "decimal(38,0)");
        Assert.NotNull(lit);
        Assert.Equal(0, lit!.Value.CompareTo(LiteralValue.HighPrecisionDecimalOf(big, 0)));
    }

    [Fact]
    public void Decimal_HighScale_MoreThan28Digits_NotRounded()
    {
        // 1.000000000000000000000000000001 = (10^30 + 1) / 10^30, 31 significant digits. Both
        // JsonElement.TryGetDecimal and decimal.TryParse would round this to exactly 1.0 — the decoder
        // must not.
        var lit = DeltaLiteralDecoder.FromJson(
            Json("1.000000000000000000000000000001"), "decimal(38,30)");
        Assert.NotNull(lit);

        var expected = LiteralValue.HighPrecisionDecimalOf(BigInteger.Pow(10, 30) + 1, 30);
        Assert.Equal(0, lit!.Value.CompareTo(expected));

        // The property a prune depends on: this bound is strictly greater than 1, so a "> 1" predicate
        // must not treat the file as empty.
        Assert.True(lit.Value.CompareTo(LiteralValue.Of(1m)) > 0);
    }

    [Fact]
    public void Decimal_Negative_HighScale_NotRounded()
    {
        var lit = DeltaLiteralDecoder.FromJson(
            Json("-1.000000000000000000000000000001"), "decimal(38,30)");
        Assert.NotNull(lit);

        var expected = LiteralValue.HighPrecisionDecimalOf(-(BigInteger.Pow(10, 30) + 1), 30);
        Assert.Equal(0, lit!.Value.CompareTo(expected));
        Assert.True(lit.Value.CompareTo(LiteralValue.Of(-1m)) < 0);
    }

    [Fact]
    public void Decimal_ScientificNotation_Decoded()
    {
        var lit = DeltaLiteralDecoder.FromJson(Json("1.5e3"), "decimal(10,0)"); // 1500
        Assert.NotNull(lit);
        Assert.Equal(LiteralValue.Of(1500m), lit!.Value);
    }

    [Fact]
    public void Decimal_FromPartitionString_HighScale_NotRounded()
    {
        var lit = DeltaLiteralDecoder.FromPartitionString(
            "1.000000000000000000000000000001", "decimal(38,30)");
        Assert.NotNull(lit);
        Assert.Equal(0, lit!.Value.CompareTo(
            LiteralValue.HighPrecisionDecimalOf(BigInteger.Pow(10, 30) + 1, 30)));
    }

    [Fact]
    public void Date_DecodesIsoString()
    {
        // Regression guard: the shared decoder still reads dates (used by the same pruning path).
        var lit = DeltaLiteralDecoder.FromJson(Json("\"2021-06-01\""), "date");
        Assert.NotNull(lit);
        Assert.Equal(
            LiteralValue.Of(new DateTimeOffset(2021, 6, 1, 0, 0, 0, TimeSpan.Zero)), lit!.Value);
    }
}
