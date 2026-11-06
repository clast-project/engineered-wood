// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using EngineeredWood.Expressions.Sql;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Expressions.Tests;

public sealed class SparkSqlRendererTests
{
    private static string Render(Expression expression) => SparkSqlRenderer.Render(expression);

    private static Expression Parse(string sql) => SparkSqlParser.ParseExpression(sql);

    // ── Round trips ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Every expression Spark accepted in the corpus that the parser reads: rendering its tree and parsing the text
    /// gives the same tree back, or (for the forms the renderer's remarks list) text that renders the same again.
    /// </summary>
    [Fact]
    public void EveryParsedCorpusExpressionRoundTrips()
    {
        var failures = new List<string>();
        int checkedCount = 0;
        foreach (string sql in SparkCorpus.ParsableExpressions().Distinct())
        {
            Expression tree;
            try
            {
                tree = Parse(sql);
            }
            catch (SparkSqlParseException)
            {
                continue; // a construct this parser refuses; nothing to render
            }

            checkedCount++;
            if (!SparkSqlRenderer.TryRender(tree, out string? rendered))
            {
                // The one refusal the corpus can reach: a special value ('now', current_timestamp()) folds to the
                // clock, whose ticks are finer than Spark's microseconds.
                string reason = Assert.Throws<NotSupportedException>(() => Render(tree)).Message;
                if (!reason.Contains("microseconds"))
                    failures.Add($"{sql}\n    not renderable: {reason}");
                continue;
            }

            Expression reparsed;
            try
            {
                reparsed = Parse(rendered);
            }
            catch (SparkSqlParseException e)
            {
                failures.Add($"{sql}\n    rendered {rendered}\n    which does not parse: {e.Message}");
                continue;
            }

            if (!reparsed.Equals(tree) && Render(reparsed) != rendered)
                failures.Add($"{sql}\n    rendered {rendered}\n    reparsed {reparsed}\n    original {tree}");
        }

        Assert.True(checkedCount > 500, $"only {checkedCount} corpus expressions were checked");
        Assert.True(failures.Count == 0, string.Join("\n", failures));
    }

    public static TheoryData<Predicate> Predicates() => new()
    {
        Ex.Equal("id", 2L),
        Ex.Equal("value", "b'q"),
        Ex.Equal("value", "a\\b"),
        Ex.Equal("value", "line\nbreak"),
        Ex.Equal("value", "\\%_"),
        Ex.In("id", LiteralValue.Of(1L), LiteralValue.Of(3L)),
        Ex.NotIn("id", LiteralValue.Of(1), LiteralValue.Of(-3)),
        Ex.And(Ex.GreaterThanOrEqual("v", 3), Ex.LessThan("v", 10)),
        Ex.Or(Ex.IsNull("value"), Ex.Not(Ex.GreaterThan("id", 1L))),
        new AndPredicate([Ex.Equal("a", 1), new AndPredicate([Ex.Equal("b", 2), Ex.Equal("c", 3)])]),
        Ex.Equal("amt", 1.50m),
        Ex.Equal("amt", -0.001m),
        Ex.Equal("amt", LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("12345678901234567890123456789012345678"), 38)),
        Ex.Equal("amt", LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("-12345678901234567890123456789012345678"), 2)),
        Ex.GreaterThan("dbl", 1.5),
        Ex.GreaterThan("dbl", 1e300),
        Ex.GreaterThan("dbl", -2.5e-300),
        Ex.GreaterThan("f", 0.1f),
        Ex.LessThan("f", float.MaxValue),
        Ex.GreaterThan("f", -float.MaxValue),
        Ex.LessThan("dbl", double.MaxValue),
        Ex.GreaterThan("dbl", 9.337299911372906e-309),
        Ex.GreaterThan("f", float.Epsilon),
        Ex.Equal("id", long.MinValue),
        Ex.Equal("i", int.MinValue),
        Ex.Equal("bin", new byte[] { 0x00, 0xAB, 0xFF }),
        Ex.Equal("ts", new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero)),
        Ex.Equal("ts", new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(1234560)),
        Ex.NullSafeEqual("flag", true),
        Ex.Equal("value", LiteralValue.Null),
        Ex.IsNotNull("value"),
        Ex.Equal(Ex.Call("lower", Ex.Ref("value")), Ex.Lit("x")),
        Ex.Equal(Ex.Call("+", Ex.Ref("i"), Ex.Lit(1)), Ex.Lit(3)),
        Ex.Equal(Ex.Call("negative", Ex.Ref("i")), Ex.Lit(-3)),
        Ex.Equal(Ex.Call("cast", Ex.Ref("value"), Ex.Lit("DECIMAL(10,2)")), Ex.Lit(1.5m)),
        Ex.Equal(Ex.VariantGet("v", "$.a.b", "int"), Ex.Lit(1)),
        Ex.Equal(new UnboundReference("a.b"), Ex.Lit(1)),
        Ex.Equal(UnboundReference.FromParts(["s", "a b"]), Ex.Lit(2)),
        Ex.Equal(UnboundReference.FromParts(["s", "c`d"]), Ex.Lit("y")),
        Ex.Equal(new UnboundReference("true"), Ex.Lit(true)),
        Ex.Equal(new UnboundReference("1st"), Ex.Lit(1)),
        Ex.Equal(new UnboundReference("naïve"), Ex.Lit(1)),
        Ex.True,
        Ex.Equal(Ex.Call("case", Ex.GreaterThan("i", 0), Ex.Lit("pos"), Ex.Lit("other")), Ex.Lit("pos")),
    };

    [Theory]
    [MemberData(nameof(Predicates))]
    public void ARenderedPredicateParsesBackToItself(Predicate predicate)
    {
        string sql = Render(predicate);
        Assert.Equal(predicate, SparkSqlParser.ParsePredicate(sql));
    }

    // ── The text, against Spark's own ──────────────────────────────────────────────────────────

    /// <summary>The text Spark's <c>Expression.sql</c> gives for the same condition, where Spark recorded one
    /// (<c>INSERT … REPLACE WHERE</c> records <c>((id &gt;= 1L) AND (value = 'a'))</c>).</summary>
    [Fact]
    public void TheTextFollowsSparksSql()
    {
        Assert.Equal("((id >= 1L) AND (value = 'a'))",
            Render(Ex.And(Ex.GreaterThanOrEqual("id", 1L), Ex.Equal("value", "a"))));
        Assert.Equal("(id IN (1L, 3L))", Render(Ex.In("id", LiteralValue.Of(1L), LiteralValue.Of(3L))));
        Assert.Equal("(amt > 1.50BD)", Render(Ex.GreaterThan("amt", 1.50m)));
        Assert.Equal("(value IS NULL)", Render(Ex.IsNull("value")));
        Assert.Equal("(NOT (id > 1L))", Render(Ex.Not(Ex.GreaterThan("id", 1L))));
        Assert.Equal("(bin = X'02')", Render(Ex.Equal("bin", new byte[] { 2 })));
        Assert.Equal("-0.0D", Render(Ex.Lit(-0.0)));
        Assert.Equal("0.0F", Render(Ex.Lit(0f)));
        Assert.Equal("CAST('NaN' AS DOUBLE)", Render(Ex.Lit(double.NaN)));
        Assert.Equal("CAST('-Infinity' AS FLOAT)", Render(Ex.Lit(float.NegativeInfinity)));
    }

    [Fact]
    public void AStringIsEscapedForSparksUnescaping()
    {
        Assert.Equal(@"(value = 'it\'s')", Render(Ex.Equal("value", "it's")));
        Assert.Equal(@"(value = 'a\\b')", Render(Ex.Equal("value", @"a\b")));
        Assert.Equal(@"(value = '\\%')", Render(Ex.Equal("value", @"\%")));
    }

    [Fact]
    public void AnIdentifierIsQuotedOnlyWhenItMustBe()
    {
        Assert.Equal("(value = 1)", Render(Ex.Equal(Ex.Ref("value"), Ex.Lit(1))));
        Assert.Equal("(`a.b` = 1)", Render(Ex.Equal(new UnboundReference("a.b"), Ex.Lit(1))));
        Assert.Equal("(s.`a b` = 1)", Render(Ex.Equal(UnboundReference.FromParts(["s", "a b"]), Ex.Lit(1))));
        Assert.Equal("(`x``y` = 1)", Render(Ex.Equal(new UnboundReference("x`y"), Ex.Lit(1))));
        Assert.Equal("(`NULL` = 1)", Render(Ex.Equal(new UnboundReference("NULL"), Ex.Lit(1))));
    }

    [Fact]
    public void ATimestampNamesItsInstant()
    {
        var instant = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
        Assert.Equal("(ts = TIMESTAMP '2026-01-02 03:04:05Z')", Render(Ex.Equal("ts", instant)));
        Assert.Equal("(ts = TIMESTAMP '2026-01-02 03:04:05.12Z')",
            Render(Ex.Equal("ts", instant.AddTicks(1_200_000))));
        // In UTC: the parser keeps the instant, not the offset.
        var offset = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.FromMinutes(330));
        string sql = Render(Ex.Equal("ts", offset));
        Assert.Equal("(ts = TIMESTAMP '2026-01-01 21:34:05Z')", sql);
        var reparsed = (ComparisonPredicate)SparkSqlParser.ParsePredicate(sql);
        Assert.Equal(offset, ((LiteralExpression)reparsed.Right).Value.AsDateTimeOffset);
    }

    [Fact]
    public void ADateRendersAsADateLiteralEitherWay()
    {
        var parsed = Parse("d = DATE '2026-01-02'");
        Assert.Equal("(d = DATE '2026-01-02')", Render(parsed));
#if NET6_0_OR_GREATER
        Assert.Equal("(d = DATE '2026-01-02')", Render(Ex.Equal("d", new DateOnly(2026, 1, 2))));
#endif
    }

    [Fact]
    public void StartsWithRendersAsSparksFunction()
    {
        Assert.Equal("startswith(value, 'b')", Render(Ex.StartsWith("value", "b")));
        Assert.Equal("(NOT startswith(value, 'b'))", Render(Ex.NotStartsWith("value", "b")));
        Assert.Equal("isnan(dbl)", Render(Ex.IsNaN(Ex.Ref("dbl"))));
    }

    /// <summary>Spark's <c>isnan(NULL)</c> is false, so <c>NOT isnan(x)</c> would be true for a NULL where
    /// <c>x IS NOT NaN</c> is NULL; the CASE keeps the NULL.</summary>
    [Fact]
    public void IsNotNaNKeepsANullOperandNull()
    {
        string sql = Render(Ex.IsNotNaN(Ex.Ref("dbl")));
        Assert.Equal("CASE WHEN (dbl IS NULL) THEN NULL ELSE (NOT isnan(dbl)) END", sql);
        // The parser reads isnan(dbl) under NOT as isnan(dbl) = TRUE, the same condition, so the text is stable from
        // the second rendering on.
        string again = Render(Parse(sql));
        Assert.Equal(again, Render(Parse(again)));
    }

    /// <summary>A decimal is at most 38 digits, its scale among them; a negative scale's zeros count too.</summary>
    [Fact]
    public void ADecimalWithinSparksPrecisionRenders_AndOneBeyondIsRefused()
    {
        Assert.Equal("0." + new string('0', 37) + "1BD", Render(Ex.Lit(LiteralValue.HighPrecisionDecimalOf(1, 38))));
        Assert.Equal("1" + new string('0', 37) + "BD", Render(Ex.Lit(LiteralValue.HighPrecisionDecimalOf(1, -37))));

        Assert.False(SparkSqlRenderer.TryRender(Ex.Lit(LiteralValue.HighPrecisionDecimalOf(1, 39)), out _));
        Assert.False(SparkSqlRenderer.TryRender(Ex.Lit(LiteralValue.HighPrecisionDecimalOf(1, -38)), out _));
        Assert.False(SparkSqlRenderer.TryRender(
            Ex.Lit(LiteralValue.HighPrecisionDecimalOf(BigInteger.Pow(10, 38), 0)), out _));
        // Refused, not thrown: negating int.MinValue overflows.
        Assert.False(SparkSqlRenderer.TryRender(Ex.Lit(LiteralValue.HighPrecisionDecimalOf(1, int.MinValue)), out _));
        Assert.False(SparkSqlRenderer.TryRender(Ex.Lit(LiteralValue.HighPrecisionDecimalOf(1, int.MaxValue)), out _));
    }

    // ── What has no Spark SQL ──────────────────────────────────────────────────────────────────

    public static TheoryData<Expression> Unrenderable() => new()
    {
        Ex.Equal("id", 1u),
        Ex.Equal("id", 1ul),
        Ex.Equal("id", Guid.Empty),
        Ex.Equal(new BoundReference(3, "id"), Ex.Lit(1)),
        Ex.Equal("ts", new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(1)),
        Ex.Equal(Ex.Call("no such", Ex.Ref("a")), Ex.Lit(1)),
        Ex.Equal(Ex.Call("cast", Ex.Ref("a"), Ex.Ref("b")), Ex.Lit(1)),
        Ex.Equal(new UnboundReference(""), Ex.Lit(1)),
        Ex.And(Ex.Equal("a", 1), Ex.Equal("id", 1u)),
#if NET6_0_OR_GREATER
        Ex.Equal("t", new TimeOnly(1, 2)),
        Ex.Equal("h", (Half)1),
#endif
    };

    [Theory]
    [MemberData(nameof(Unrenderable))]
    public void ATreeWithNoSparkSqlIsRefusedNotMisspelled(Expression expression)
    {
        Assert.False(SparkSqlRenderer.TryRender(expression, out string? sql));
        Assert.Null(sql);
        Assert.Throws<NotSupportedException>(() => SparkSqlRenderer.Render(expression));
    }
}
