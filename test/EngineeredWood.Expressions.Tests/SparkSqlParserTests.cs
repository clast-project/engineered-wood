// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Tests;

public sealed class SparkSqlParserTests
{
    private static Expression Parse(string sql) => SparkSqlParser.ParseExpression(sql);

    // ── Precedence, checked against Spark rather than against intuition ────────────────────

    /// <summary>
    /// Spark renders a parsed expression fully parenthesised, so <c>1 + 2 * 3</c> comes back as
    /// <c>(1 + (2 * 3))</c>. Parsing both and comparing the trees tests precedence and
    /// associativity without needing a renderer of our own — if we bind operators differently
    /// from Spark, the two trees disagree.
    /// </summary>
    [Theory]
    [InlineData("arithmetic-precedence")]
    [InlineData("logical-precedence")]
    [InlineData("comparison")]
    public void ParsingSparksOwnRenderingGivesTheSameTree(string group)
    {
        var mismatches = new List<string>();

        foreach (var entry in SparkCorpus.Group(group).EnumerateArray())
        {
            var sql = entry.Expression();
            var rendered = entry.GetProperty("parse").GetProperty("sql").GetString()!;

            var fromSource = Normalize(Parse(sql));
            var fromRendering = Normalize(Parse(rendered));

            if (fromSource != fromRendering)
                mismatches.Add($"{sql}\n    ours:  {fromSource}\n    spark: {rendered} -> {fromRendering}");
        }

        Assert.Empty(mismatches);
    }

    // ── Lowerings that have no node of their own ──────────────────────────────────────────

    [Fact]
    public void ABooleanExpressionInPredicatePositionBecomesEqualsTrue()
    {
        // Exact under three-valued logic: `bl` and `bl = TRUE` are true, false and null on the
        // same inputs, so a null still fails a CHECK constraint.
        Assert.Equal(
            new ComparisonPredicate(new UnboundReference("bl"), ComparisonOperator.Equal, True),
            SparkSqlParser.ParsePredicate("bl"));
    }

    [Fact]
    public void IsTrueIsNullSafeEqualityAndNotEqualsTrue()
    {
        // The distinction is the whole point of the form: a null operand makes `IS TRUE` false,
        // while `= TRUE` stays null. Collapsing them would silently change which rows pass.
        Assert.Equal(
            new ComparisonPredicate(new UnboundReference("bl"), ComparisonOperator.NullSafeEqual, True),
            Parse("bl IS TRUE"));

        Assert.Equal(
            new NotPredicate(
                new ComparisonPredicate(new UnboundReference("bl"), ComparisonOperator.NullSafeEqual, True)),
            Parse("bl IS NOT TRUE"));
    }

    [Theory]
    [InlineData("a IS NULL", UnaryOperator.IsNull)]
    [InlineData("a IS NOT NULL", UnaryOperator.IsNotNull)]
    public void IsNullBecomesAUnaryPredicate(string sql, UnaryOperator expected)
    {
        Assert.Equal(new UnaryPredicate(new UnboundReference("a"), expected), Parse(sql));
    }

    [Fact]
    public void BetweenExpandsToTheTwoComparisonsItMeans()
    {
        var parsed = Assert.IsType<AndPredicate>(Parse("a BETWEEN 1 AND 10"));

        Assert.Collection(parsed.Children,
            low => Assert.Equal(ComparisonOperator.GreaterThanOrEqual, ((ComparisonPredicate)low).Op),
            high => Assert.Equal(ComparisonOperator.LessThanOrEqual, ((ComparisonPredicate)high).Op));
    }

    [Fact]
    public void BetweenDoesNotSurrenderItsAndToTheSurroundingJunction()
    {
        // `a BETWEEN 1 AND 10 AND b > 0` has two ANDs meaning different things. The inner one
        // belongs to BETWEEN; only the outer one is a junction.
        var parsed = Assert.IsType<AndPredicate>(Parse("a BETWEEN 1 AND 10 AND b > 0"));

        Assert.Equal(2, parsed.Children.Count);
        Assert.IsType<AndPredicate>(parsed.Children[0]);
        Assert.IsType<ComparisonPredicate>(parsed.Children[1]);
    }

    [Fact]
    public void AnAllLiteralInListBecomesASetPredicate()
    {
        var parsed = Assert.IsType<SetPredicate>(Parse("a IN (1, 2, 3)"));
        Assert.Equal(SetOperator.In, parsed.Op);
        Assert.Equal(3, parsed.Values.Count);

        Assert.Equal(SetOperator.NotIn, Assert.IsType<SetPredicate>(Parse("a NOT IN (1, 2)")).Op);
    }

    [Fact]
    public void AnInListHoldingExpressionsExpandsToADisjunction()
    {
        // SetPredicate carries LiteralValues, so `a IN (b, 5)` cannot use it. Expanding to
        // equality alternatives is SQL's own definition of IN and keeps the null behaviour.
        var parsed = Assert.IsType<OrPredicate>(Parse("a IN (b, 5)"));

        Assert.Equal(2, parsed.Children.Count);
        Assert.All(parsed.Children,
            child => Assert.Equal(ComparisonOperator.Equal, ((ComparisonPredicate)child).Op));
    }

    [Fact]
    public void ArithmeticAndCastsBecomeFunctionCalls()
    {
        Assert.Equal(
            new FunctionCall("+", new Expression[] { new UnboundReference("a"), new UnboundReference("b") }),
            Parse("a + b"));

        var cast = Assert.IsType<FunctionCall>(Parse("CAST(ts AS DATE)"));
        Assert.Equal("cast", cast.Name);
        Assert.Equal("DATE", ((LiteralExpression)cast.Arguments[1]).Value.AsString);

        var parameterized = Assert.IsType<FunctionCall>(Parse("CAST(g AS DECIMAL(10,2))"));
        Assert.Equal("DECIMAL(10,2)", ((LiteralExpression)parameterized.Arguments[1]).Value.AsString);

        Assert.Equal("cast", Assert.IsType<FunctionCall>(Parse("a::bigint")).Name);
    }

    [Fact]
    public void CaseKeepsItsShapeWithAnOddArgumentCountMeaningElseIsPresent()
    {
        Assert.Equal(2, Assert.IsType<FunctionCall>(Parse("CASE WHEN a > 0 THEN 1 END")).Arguments.Count);
        Assert.Equal(3, Assert.IsType<FunctionCall>(Parse("CASE WHEN a > 0 THEN 1 ELSE 0 END")).Arguments.Count);

        // `CASE x WHEN 1 THEN …` compares against the operand.
        var compared = Assert.IsType<FunctionCall>(Parse("CASE a WHEN 1 THEN 'one' ELSE 'many' END"));
        Assert.IsType<ComparisonPredicate>(compared.Arguments[0]);
    }

    [Fact]
    public void DottedNamesStayOneReferenceBecauseThatIsHowDeltaWritesThem()
    {
        Assert.Equal(new UnboundReference("nested.arr"), Parse("nested.arr"));
        Assert.Equal(new UnboundReference("weird name"), Parse("`weird name`"));
    }

    // ── Literal typing, every rule measured from Spark ─────────────────────────────────────

    [Theory]
    [InlineData("1", LiteralValue.Kind.Int32)]
    [InlineData("1000000000000", LiteralValue.Kind.Int64)]
    [InlineData("1.5", LiteralValue.Kind.Decimal)]       // fractional is DECIMAL, not double
    [InlineData(".5", LiteralValue.Kind.Decimal)]
    [InlineData("1e3", LiteralValue.Kind.Double)]        // only an exponent makes it double
    [InlineData("1.5e-2", LiteralValue.Kind.Double)]
    [InlineData("1L", LiteralValue.Kind.Int64)]
    [InlineData("1D", LiteralValue.Kind.Double)]
    [InlineData("1F", LiteralValue.Kind.Float)]
    [InlineData("1BD", LiteralValue.Kind.Decimal)]
    [InlineData("'abc'", LiteralValue.Kind.String)]
    [InlineData("true", LiteralValue.Kind.Boolean)]
    [InlineData("NULL", LiteralValue.Kind.Null)]
    [InlineData("X'ABCD'", LiteralValue.Kind.Binary)]
    [InlineData("DATE'2026-08-11'", LiteralValue.Kind.DateTimeOffset)]
    public void LiteralsTakeSparksType(string sql, LiteralValue.Kind expected)
    {
        Assert.Equal(expected, Assert.IsType<LiteralExpression>(Parse(sql)).Value.Type);
    }

    [Theory]
    [InlineData("'it''s'", "it's")]
    [InlineData(@"'a\'b'", "a'b")]
    [InlineData(@"'100\%'", @"100\%")]   // an unrecognised escape keeps its backslash
    [InlineData("''", "")]
    public void StringsAreUnescapedTheWaySparkUnescapesThem(string sql, string expected)
    {
        Assert.Equal(expected, Assert.IsType<LiteralExpression>(Parse(sql)).Value.AsString);
    }

    [Fact]
    public void BinaryLiteralsDecodeToBytes()
    {
        Assert.Equal(new byte[] { 0xAB, 0xCD },
            Assert.IsType<LiteralExpression>(Parse("X'ABCD'")).Value.AsBinary);
    }

    [Theory]
    [InlineData("X'A BC'")]   // NumberStyles.HexNumber would read "A " as 0x0A
    [InlineData("X'AB C '")]
    [InlineData("X'ZZ'")]
    [InlineData("X'ABC'")]    // odd length
    public void BinaryLiteralsRejectAnythingThatIsNotPurelyHexDigits(string sql)
    {
        Assert.Throws<SparkSqlParseException>(() => Parse(sql));
    }

    // ── Refusals, which must be clean rather than absent ───────────────────────────────────

    [Theory]
    [InlineData("1Y", "suffix")]
    [InlineData("INTERVAL 1 DAY", "INTERVAL")]
    [InlineData("rank() OVER (ORDER BY a)", "window")]
    [InlineData("count(*)", "'*'")]
    [InlineData("a +", "unexpected end")]
    [InlineData("((a)", "expected ')'")]
    [InlineData("a > > 0", "cannot start an expression")]
    [InlineData("", "unexpected end")]
    [InlineData("a > 0 garbage", "trailing input")]
    public void UnsupportedAndMalformedInputFailsWithAQuotableReason(string sql, string expectedFragment)
    {
        var ex = Assert.Throws<SparkSqlParseException>(() => Parse(sql));
        Assert.Contains(expectedFragment, ex.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(sql, ex.Expression);
    }

    // ── The corpus ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void NothingInTheCorpusEscapesAsAnUnexpectedExceptionType()
    {
        // The contract a caller relies on: an expression we cannot read fails as a
        // SparkSqlParseException, so the write is refused with an explanation. An
        // IndexOutOfRange or NullReference escaping here would be a crash on table metadata.
        foreach (var entry in SparkCorpus.Entries())
        {
            var sql = entry.Expression();

            try
            {
                Parse(sql);
            }
            catch (SparkSqlParseException)
            {
                // Expected for anything outside the supported grammar.
            }
            catch (Exception ex)
            {
                Assert.Fail($"'{sql}' threw {ex.GetType().Name}: {ex.Message}");
            }
        }
    }

    [Theory]
    [InlineData("arithmetic-precedence")]
    [InlineData("logical-precedence")]
    [InlineData("comparison")]
    [InlineData("is-predicates")]
    [InlineData("set-and-pattern")]
    [InlineData("case-and-conditional")]
    [InlineData("cast")]
    [InlineData("null-semantics")]
    [InlineData("coercion")]
    public void EveryExpressionInASupportedGroupParses(string group)
    {
        var failures = new List<string>();

        foreach (var entry in SparkCorpus.Group(group).EnumerateArray())
        {
            var sql = entry.Expression();
            if (!entry.GetProperty("parse").GetProperty("ok").GetBoolean())
                continue;

            try
            {
                Parse(sql);
            }
            catch (SparkSqlParseException ex)
            {
                failures.Add($"{sql}  ->  {ex.Reason}");
            }
        }

        Assert.Empty(failures);
    }

    /// <summary>
    /// Pins exactly which corpus expressions this parser refuses, and why.
    /// </summary>
    /// <remarks>
    /// 190 of the 197 expressions Spark accepts also parse here. The seven that do not are
    /// listed rather than counted, so the set is a decision on the record instead of a number
    /// nobody can check: adding a construct should shorten this list deliberately, and losing
    /// one should fail loudly. Four are constructs this tree has no shape for, and three are
    /// exactly what Delta itself rejects with DELTA_UNSUPPORTED_EXPRESSION_CHECK_CONSTRAINT.
    /// </remarks>
    [Fact]
    public void TheCorpusExpressionsWeRefuseAreExactlyThese()
    {
        var expected = new[]
        {
            "INTERVAL 1 DAY",           // LiteralValue has no interval kind
            "1Y",                       // no 8-bit integer kind; widening would change coercion
            "1S",                       // no 16-bit integer kind
            "a > (SELECT 1)",           // subquery — Delta refuses these too
            "*",                        // not an expression
            "a IN (SELECT 1)",          // subquery
            "rank() OVER (ORDER BY a)", // window function — Delta refuses these too
        };

        var refused = new List<string>();
        foreach (var entry in SparkCorpus.Entries())
        {
            var sql = entry.Expression();
            if (!entry.GetProperty("parse").GetProperty("ok").GetBoolean())
                continue;

            try
            {
                Parse(sql);
            }
            catch (SparkSqlParseException)
            {
                refused.Add(sql);
            }
        }

        Assert.Equal(expected.OrderBy(x => x, StringComparer.Ordinal),
            refused.OrderBy(x => x, StringComparer.Ordinal));
    }

    [Fact]
    public void ASubqueryIsRefusedForBeingASubqueryRatherThanForAStrayParenthesis()
    {
        // The reason a caller sees decides whether they can act on it.
        Assert.Contains("subqueries",
            Assert.Throws<SparkSqlParseException>(() => Parse("a > (SELECT 1)")).Reason);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────────────────

    private static LiteralExpression True => new(LiteralValue.Of(true));

    /// <summary>
    /// Flattens nested same-operator junctions, so <c>And(And(a, b), c)</c> and
    /// <c>And(a, b, c)</c> compare equal.
    /// </summary>
    /// <remarks>
    /// The parser folds `a AND b AND c` into one n-ary node while Spark renders it left-nested.
    /// Both are correct — AND is associative — and the difference is not what a precedence test
    /// is asking about, so it is normalised away rather than allowed to mask real disagreements
    /// about which operator binds tighter. <c>And</c> and <c>Or</c> stay distinct, so a genuine
    /// precedence error between them still fails.
    /// </remarks>
    private static Expression Normalize(Expression expression)
    {
        switch (expression)
        {
            // Spark has no `<>` operator internally: it renders `a <> b` as `(NOT (a = b))`.
            // This tree does have one, so the two spellings are folded together. They agree on
            // nulls — both are null when either side is — so nothing is lost by treating them as
            // the same, and the fold makes the comparison test cover `<>` rather than skip it.
            case NotPredicate { Child: ComparisonPredicate { Op: ComparisonOperator.Equal } inner }:
                return new ComparisonPredicate(
                    Normalize(inner.Left), ComparisonOperator.NotEqual, Normalize(inner.Right));

            case AndPredicate and:
                return new AndPredicate(FlattenJunction<AndPredicate>(and.Children));
            case OrPredicate or:
                return new OrPredicate(FlattenJunction<OrPredicate>(or.Children));
            case NotPredicate not:
                return new NotPredicate((Predicate)Normalize(not.Child));
            case ComparisonPredicate comparison:
                return new ComparisonPredicate(
                    Normalize(comparison.Left), comparison.Op, Normalize(comparison.Right));
            case UnaryPredicate unary:
                return new UnaryPredicate(Normalize(unary.Operand), unary.Op);
            case FunctionCall call:
                return new FunctionCall(call.Name, call.Arguments.Select(Normalize).ToList());
            default:
                return expression;
        }
    }

    private static List<Predicate> FlattenJunction<T>(IReadOnlyList<Predicate> children)
        where T : Predicate
    {
        var flattened = new List<Predicate>();

        foreach (var child in children)
        {
            var normalized = (Predicate)Normalize(child);

            if (normalized is T)
            {
                var nested = normalized switch
                {
                    AndPredicate and => and.Children,
                    OrPredicate or => or.Children,
                    _ => null,
                };

                if (nested is not null)
                {
                    flattened.AddRange(nested);
                    continue;
                }
            }

            flattened.Add(normalized);
        }

        return flattened;
    }
}
