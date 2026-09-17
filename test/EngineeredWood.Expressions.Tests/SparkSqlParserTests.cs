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
    public void AnInListHoldingExpressionsKeepsItsShape()
    {
        // It used to expand to a disjunction of equalities, on the grounds that SQL defines IN
        // that way. True of the three-valued logic and false of the TYPES: Spark resolves one
        // type over the operand and the whole list, so `a IN ('01')` is false under the legacy
        // dialect where `a = '01'` is true, and a disjunction cannot express that. #261.
        var parsed = Assert.IsType<SetPredicate>(Parse("a IN (b, 5)"));

        Assert.Equal(SetOperator.In, parsed.Op);
        Assert.Equal(new Expression[] { new UnboundReference("b"), new LiteralExpression(5) },
            parsed.Values);

        // ...and a list of expressions is not a list of literals, which is what pruning asks.
        Assert.False(parsed.TryGetLiteralValues(out _));
        Assert.True(Assert.IsType<SetPredicate>(Parse("a IN (1, 2)")).TryGetLiteralValues(out _));
    }

    [Fact]
    public void ANegatedInListKeepsItsShapeToo()
    {
        var parsed = Assert.IsType<SetPredicate>(Parse("a NOT IN (b, 5)"));
        Assert.Equal(SetOperator.NotIn, parsed.Op);
        Assert.Equal(2, parsed.Values.Count);
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
    public void LiteralsTakeSparksType(string sql, LiteralValue.Kind expected)
    {
        Assert.Equal(expected, Assert.IsType<LiteralExpression>(Parse(sql)).Value.Type);
    }

    /// <summary>
    /// A double literal reads the same bits on every target framework.
    /// </summary>
    /// <remarks>
    /// <para>
    /// #350, and the half of it that is not a cast. Only an exponent makes a numeric literal a
    /// DOUBLE in Spark — <c>1.5</c> is a decimal and <c>1.5e0</c> is not — so an exponent-bearing
    /// literal goes through the same parser a column's text does, and carried the same defect:
    /// .NET Framework's is not correctly rounded and reads about 1% of ordinary fifteen- and
    /// sixteen-digit numbers as the double NEXT DOOR.
    /// </para>
    /// <para>
    /// The expectations are the bits <c>Double.parseDouble</c> produces on JDK 21, so they are
    /// Spark's answers. <b>Only a run on net472 can fail these</b>, and the fix for the cast half
    /// did not reach here — the literal parser lives in another assembly and had to be routed
    /// separately.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("49.0793458194787E0", 4632104121170255391L)]
    [InlineData("1.20823154016232E-221", 1301953993310182709L)]
    [InlineData("2.17221101871326E+109", 6242692313457557362L)]
    [InlineData("5.596579825486209E-132", 2643550992869693082L)]
    [InlineData("3.465494185217035E-271", 560538912182121944L)]
    public void ADoubleLiteralReadsTheSameBitsOnEveryTargetFramework(string sql, long expected)
    {
        var value = Assert.IsType<LiteralExpression>(Parse(sql)).Value;

        Assert.Equal(LiteralValue.Kind.Double, value.Type);
        Assert.Equal(expected, BitConverter.DoubleToInt64Bits(value.AsDouble));
    }

    /// <summary>
    /// Spark's escape table, every row of it measured — #179.
    /// </summary>
    /// <remarks>
    /// The corpus asserts these too, through <c>SparkEvaluationCorpusTests</c>. They are repeated
    /// here because this is where a reader looks for the table, and because a theory row names
    /// the rule where a corpus entry only records an answer. Three rows contradict the obvious
    /// reading and are called out where they appear.
    /// </remarks>
    [Theory]
    // The rule this replaces: a doubled quote is TWO literals, joined into one string.
    [InlineData("'it''s'", "its")]
    [InlineData("''", "")]
    [InlineData("''''", "")]
    [InlineData("'a'''", "a")]

    // Adjacent literals concatenate over TOKENS, so whitespace, comments and the quote style
    // between the pieces make no difference.
    [InlineData("'a' 'b'", "ab")]
    [InlineData("'a'  'b'", "ab")]
    [InlineData("'a' 'b' 'c'", "abc")]
    [InlineData("'a' /* c */ 'b'", "ab")]
    [InlineData("'a' \"b\"", "ab")]
    [InlineData("\"a\"\"b\"", "ab")]

    // Each piece is unescaped BEFORE the join, so an escape cannot span two literals. Joining
    // first would answer "A" and "101" here.
    [InlineData(@"'\u00' '41'", "u0041")]
    [InlineData(@"'\1' '01'", "101")]

    // The escapes that are in the table.
    [InlineData(@"'a\'b'", "a'b")]
    [InlineData(@"'a\""b'", "a\"b")]
    [InlineData(@"'a\\b'", @"a\b")]
    [InlineData(@"'a\nb'", "a\nb")]
    [InlineData(@"'a\tb'", "a\tb")]
    [InlineData(@"'a\rb'", "a\rb")]
    [InlineData(@"'a\bb'", "a\bb")]
    [InlineData(@"'\0'", "\0")]
    [InlineData(@"'a\Zb'", "a\u001Ab")]   // \Z is in the table though C has no such escape

    // ...and the ones that are not. An unrecognised escape DROPS its backslash, except for the
    // two LIKE wildcards, which keep it.
    [InlineData(@"'a\fb'", "afb")]        // \f is NOT a form feed: it is not in the table at all
    [InlineData(@"'a\qb'", "aqb")]
    [InlineData(@"'a\vb'", "avb")]
    [InlineData(@"'a\ b'", "a b")]
    [InlineData(@"'a\`b'", "a`b")]
    [InlineData(@"'100\%'", @"100\%")]
    [InlineData(@"'a\_b'", @"a\_b")]

    // Width decides the numeric escapes, and the octal one stops at \177 rather than \377.
    [InlineData(@"'\101'", "A")]
    [InlineData(@"'\177'", "\u007F")]
    [InlineData(@"'\200'", "200")]        // first digit above 1: not an octal escape at all
    [InlineData(@"'\377'", "377")]
    [InlineData(@"'\7'", "7")]            // one digit is not an octal escape
    [InlineData(@"'\1011'", "A1")]
    [InlineData(@"'\u0041'", "A")]
    [InlineData(@"'\u004a'", "J")]        // hex digits are either case
    [InlineData(@"'\u00411'", "A1")]
    [InlineData(@"'\u12'", "u12")]        // too few digits: not an escape

    // The same shortfall, but landing at the END of the literal rather than mid-string — the
    // boundary Unquote's explicit bound exists for, now that it scans the original quoted text
    // instead of an unquoted copy. Coverage of that edge rather than a new measurement: the rule
    // is the one the two rows above measured.
    [InlineData(@"'\u004'", "u004")]
    [InlineData(@"'\10'", "10")]
    [InlineData(@"'\U0000004'", "U0000004")]
    [InlineData(@"'\x41'", "x41")]        // C's hex escape is not Spark's
    [InlineData(@"'\U00000041'", "A")]
    [InlineData(@"'\U0001F600'", "\U0001F600")]
    public void StringsAreUnescapedTheWaySparkUnescapesThem(string sql, string expected)
    {
        Assert.Equal(expected, Assert.IsType<LiteralExpression>(Parse(sql)).Value.AsString);
    }

    [Fact]
    public void AnOutOfRangeCodePointDecomposesRatherThanBeingRefused()
    {
        // char.ConvertFromUtf32 would throw for both of these. Spark applies Java's surrogate
        // arithmetic with no range check and answers, so this does too — measured, and the
        // corpus cannot assert it because the value reaches the fixture as '?' once the unpaired
        // surrogates are encoded.
        Assert.Equal(
            new[] { '\uDC00', '\uDC00' },
            Assert.IsType<LiteralExpression>(Parse(@"'\U00110000'")).Value.AsString.ToCharArray());

        Assert.Equal(
            new[] { '\uD7BF', '\uDFFF' },
            Assert.IsType<LiteralExpression>(Parse(@"'\UFFFFFFFF'")).Value.AsString.ToCharArray());
    }

    /// <summary>
    /// A DATE literal is lowered to a cast, where a TIMESTAMP literal stays a literal.
    /// </summary>
    /// <remarks>
    /// The value cannot carry the difference: <c>SparkLiteral.Typed</c> makes both the same
    /// <see cref="DateTimeOffset"/>, deliberately, so a literal and a column value compare on one
    /// footing. Saying it in the TREE instead is the same kind of lowering as BETWEEN becoming
    /// two comparisons, and it hands the work to the DATE cast, which is already measured. #254.
    /// </remarks>
    [Fact]
    public void ADateLiteralIsLoweredToACast()
    {
        var lowered = Assert.IsType<FunctionCall>(Parse("DATE'2026-08-11'"));

        Assert.Equal("cast", lowered.Name);
        Assert.Equal("DATE", ((LiteralExpression)lowered.Arguments[1]).Value.AsString);

        var instant = Assert.IsType<LiteralExpression>(lowered.Arguments[0]).Value;
        Assert.Equal(LiteralValue.Kind.DateTimeOffset, instant.Type);
        Assert.Equal(
            new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero), instant.AsDateTimeOffset);

        // A TIMESTAMP literal has nothing to disambiguate and stays one.
        Assert.Equal(
            LiteralValue.Kind.DateTimeOffset,
            Assert.IsType<LiteralExpression>(Parse("TIMESTAMP'2026-08-11 12:30:00'")).Value.Type);
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

    // ── Wide decimal literals (#173) ───────────────────────────────────────────────────────

    /// <summary>
    /// A literal wider than <see cref="decimal"/> keeps every digit.
    /// </summary>
    /// <remarks>
    /// #173 was filed as "cannot parse", and that was half of it. `decimal.TryParse` REFUSES a
    /// literal too large for the type and silently ROUNDS one that is merely too precise, so
    /// <c>0.12345678901234567890123456789012345678</c> came back as
    /// <c>0.1234567890123456789012345679</c> with the parse reporting success — a wrong value
    /// rather than a refusal. That row is the one that matters most here.
    /// </remarks>
    [Theory]
    // No negative row: a leading '-' is a NEGATION node rather than part of the literal, so it
    // does not reach SparkLiteral at all. Its evaluation is asserted in the Arrow tests.
    [InlineData("12345678901234567890123456789012345678", "12345678901234567890123456789012345678", 0)]
    [InlineData("1234567890123456789012345678901234.5678", "12345678901234567890123456789012345678", 4)]
    [InlineData("0.12345678901234567890123456789012345678", "12345678901234567890123456789012345678", 38)]
    [InlineData("79228162514264337593543950336", "79228162514264337593543950336", 0)]
    public void AWideDecimalLiteralKeepsEveryDigit(string sql, string unscaled, int scale)
    {
        var value = Assert.IsType<LiteralExpression>(Parse(sql)).Value;

        Assert.Equal(LiteralValue.Kind.HighPrecisionDecimal, value.Type);
        Assert.Equal(unscaled, value.AsHighPrecisionDecimal.UnscaledValue.ToString());
        Assert.Equal(scale, value.AsHighPrecisionDecimal.Scale);
    }

    /// <summary>
    /// A literal <see cref="decimal"/> can hold EXACTLY stays an ordinary decimal.
    /// </summary>
    /// <remarks>
    /// The boundary is the type's own domain — a scale it can carry and an unscaled value inside
    /// its 96 bits — and not whether <c>TryParse</c> happens to succeed. 79228162514264337593543950335
    /// is the largest it holds; one more is the row above.
    /// </remarks>
    [Theory]
    [InlineData("1.5")]
    [InlineData("1.50")]
    [InlineData("79228162514264337593543950335")]
    [InlineData("12345678901234567890123456789")]
    public void ALiteralDecimalCanHoldExactlyStaysADecimal(string sql) =>
        Assert.Equal(
            LiteralValue.Kind.Decimal, Assert.IsType<LiteralExpression>(Parse(sql)).Value.Type);

    [Fact]
    public void AnIntegralLiteralTooWideForBigintBecomesADecimal()
    {
        // Spark's ladder is int, then bigint, then decimal — it does not stop at bigint and
        // refuse. 20 digits is past long and far inside decimal.
        Assert.Equal(
            LiteralValue.Kind.Decimal,
            Assert.IsType<LiteralExpression>(Parse("12345678901234567890")).Value.Type);

        Assert.Equal(
            LiteralValue.Kind.Int64,
            Assert.IsType<LiteralExpression>(Parse("1000000000000")).Value.Type);
    }

    /// <summary>
    /// A literal wider than any Spark decimal is refused, and width counts the SCALE.
    /// </summary>
    /// <remarks>
    /// A Spark decimal requires 0 &lt;= scale &lt;= precision &lt;= 38, so the last two rows are
    /// too wide on their scale alone — one significant digit each. Checking only the digit count
    /// accepted <c>1e-45BD</c> as a scale-45 decimal that no Spark type can hold. Measured:
    /// <c>1e-38BD</c> is a <c>decimal(38,38)</c> and <c>1e-39BD</c> is refused by Spark's own
    /// parser, which is the stage this refuses at.
    /// </remarks>
    [Theory]
    [InlineData("123456789012345678901234567890123456789")]
    [InlineData("0.123456789012345678901234567890123456789")]
    [InlineData("12345678901234567890123456789012345.6789")]
    [InlineData("0.000000000000000000000000000000000000001")]
    [InlineData("1e-39BD")]
    [InlineData("1e-45BD")]
    public void ALiteralWiderThanAnyDecimalIsRefusedWithAQuotableReason(string sql)
    {
        var thrown = Assert.Throws<SparkSqlParseException>(() => Parse(sql));
        Assert.Contains("38", thrown.Reason, StringComparison.Ordinal);
        Assert.Equal(sql, thrown.Expression);
    }

    [Theory]
    [InlineData("0.00000000000000000000000000000000000001", 38)]
    [InlineData("1e-38BD", 38)]
    [InlineData("0.0000000000000000000000000000000000001", 37)]
    public void AScaleOfExactlyThirtyEightIsStillAccepted(string sql, int scale)
    {
        // The accepting side of the same boundary, so the check cannot be tightened by accident.
        var value = Assert.IsType<LiteralExpression>(Parse(sql)).Value;

        Assert.Equal(LiteralValue.Kind.HighPrecisionDecimal, value.Type);
        Assert.Equal(scale, value.AsHighPrecisionDecimal.Scale);
        Assert.Equal("1", value.AsHighPrecisionDecimal.UnscaledValue.ToString());
    }

    [Fact]
    public void TheBdSuffixTakesTheSameWidthAsAnyOtherDecimalLiteral()
    {
        var value = Assert.IsType<LiteralExpression>(
            Parse("12345678901234567890123456789012345678BD")).Value;

        Assert.Equal(LiteralValue.Kind.HighPrecisionDecimal, value.Type);
        Assert.Equal(
            "12345678901234567890123456789012345678",
            value.AsHighPrecisionDecimal.UnscaledValue.ToString());
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
    /// The expressions Spark accepts and this parser does not are listed rather than counted, so
    /// the set is a decision on the record instead of a number nobody can check: adding a
    /// construct should shorten this list deliberately, and losing one should fail loudly.
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

            // An ALIAS. Spark's expression parser reads `'a' x` as `'a' AS x`, which is a
            // select-list shape with no meaning in a CHECK constraint — and Spark itself refuses
            // it the moment the expression is evaluated. Arrived with #179's string-literals
            // group, where it was asked in order to find where `stringLit+` stops.
            "'a' x",

            // Raw string literals, where no escape applies at all: R'a\n' is a backslash and an
            // n. Spark reads them, and they even take part in the adjacent-literal concatenation
            // (R'it' 's' is "its"), but the tokenizer has no notion of a prefixed literal — `R`
            // scans as an identifier. Its own change, and not part of #179.
            @"R'a\nb'",
            @"r'a\nb'",
            "R'it''s'",
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

    /// <summary>
    /// The other direction: every corpus expression Spark's PARSER refuses is refused here too.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The counterpart of <see cref="TheCorpusExpressionsWeRefuseAreExactlyThese"/>, and the gate
    /// that was missing. That test pins what Spark accepts and we do not, which is a scope
    /// decision; nothing pinned what Spark REJECTS and we accept, which is always a defect --
    /// accepting a constraint Spark will not parse writes table metadata that no Spark session
    /// can read back. #287 lived in that gap: `1e400` is INVALID_NUMERIC_LITERAL_RANGE there and
    /// was an infinity here.
    /// </para>
    /// <para>
    /// The exception list is EMPTY and is expected to stay that way. It exists so that a
    /// deliberate divergence has somewhere to be written down rather than being discovered as a
    /// mystery, in the same spelling as the list above -- but unlike that one, an entry here
    /// needs an argument, because the refusing side is Spark.
    /// </para>
    /// </remarks>
    [Fact]
    public void EveryCorpusExpressionSparkRefusesIsRefusedHereToo()
    {
        var allowed = Array.Empty<string>();

        var accepted = new List<string>();
        foreach (var entry in SparkCorpus.Entries())
        {
            var sql = entry.Expression();
            if (entry.GetProperty("parse").GetProperty("ok").GetBoolean())
                continue;

            try
            {
                Parse(sql);
                accepted.Add(sql);
            }
            catch (SparkSqlParseException)
            {
                // Refused, which is the point.
            }
        }

        Assert.Equal(allowed.OrderBy(x => x, StringComparer.Ordinal),
            accepted.Distinct().OrderBy(x => x, StringComparer.Ordinal));
    }

    // -- The range of a numeric literal, #287 ------------------------------------------------

    /// <summary>
    /// A floating-point literal outside the type's range is refused, as Spark's parser refuses
    /// it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The check is on the literal's EXACT decimal text, which is what the last rows of each
    /// width are for: `1.79769313486231575e308` and `3.4028234663852887e38F` both round to a
    /// finite value, so an implementation that asked whether the parse overflowed would accept
    /// both. Spark refuses them, comparing against the bound as a decimal.
    /// </para>
    /// <para>
    /// It also removes a per-runtime divergence. `double.TryParse` answers an infinity on .NET
    /// Core and FALSE on .NET Framework, so before this the same literal was an infinity on
    /// net10.0 and a refusal on net472 -- the netstandard2.0 build disagreeing with itself
    /// depending on which runtime loaded it, as in #282.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("1e400")]
    [InlineData("1e400D")]
    [InlineData("1e309")]
    [InlineData("1.8e308")]
    [InlineData("1.7976931348623158e308")]
    [InlineData("1.7976931348623159e308")]
    [InlineData("1.79769313486231575e308")]
    [InlineData("1e39F")]
    [InlineData("1e400F")]
    [InlineData("3.5e38F")]
    [InlineData("3.4028235e38F")]
    [InlineData("3.4028234663852887e38F")]
    public void AFloatingLiteralOutsideTheTypeRangeIsRefused(string sql)
    {
        var thrown = Assert.Throws<SparkSqlParseException>(() => Parse(sql));
        Assert.Contains("out of range", thrown.Reason, StringComparison.Ordinal);
        Assert.Equal(sql, thrown.Expression);
    }

    /// <summary>The largest literal of each width, which must still be accepted.</summary>
    /// <remarks>
    /// `3.4028234663852886e38F` is the row that says the float bound is not the one Java prints:
    /// `Float.toString(Float.MaxValue)` is `3.4028235E38`, which Spark refuses, while this -- the
    /// same value widened to a double -- it accepts. Both were measured.
    /// </remarks>
    [Theory]
    [InlineData("1.7976931348623157e308")]
    [InlineData("17976931348623157e292")]
    [InlineData("1e308")]
    [InlineData("3.4028234663852886e38F")]
    [InlineData("3.4e38F")]
    [InlineData("1e38F")]
    public void TheLargestLiteralOfEachWidthIsAccepted(string sql) =>
        Assert.IsType<LiteralExpression>(Parse(sql));

    /// <summary>
    /// Underflow is not a range error, which is the half of #287 that does not reproduce.
    /// </summary>
    /// <remarks>
    /// The issue reports `1e-400` as the same gap at the other end. Measured, Spark accepts it
    /// and answers 0.0: the range it checks is [-MaxValue, MaxValue], and a value too small to
    /// represent sits well inside that. `-1e-400` is a negative zero there, which is #282's rule
    /// and not this one.
    /// </remarks>
    [Theory]
    [InlineData("1e-400")]
    [InlineData("1e-400D")]
    [InlineData("1e-325")]
    [InlineData("1e-324")]
    [InlineData("1e-46F")]
    // A zero mantissa is in range at every exponent, which is what stops a check written on the
    // exponent alone.
    [InlineData("0e400")]
    [InlineData("0e-400")]
    [InlineData("0.0e400")]
    [InlineData("000e400")]
    public void ALiteralThatUnderflowsIsAcceptedRatherThanRefused(string sql) =>
        Assert.IsType<LiteralExpression>(Parse(sql));

    /// <summary>
    /// An exponent no decimal scale can carry is refused before the range is compared at all.
    /// </summary>
    /// <remarks>
    /// `0e2147483648` is the row that pins the ORDER: its mantissa is zero, so it is in range by
    /// the rule above and only a check running first can refuse it. `1e-2147483648` refuses
    /// although the exponent fits an int, because it is the negation of the scale that overflows
    /// -- which is why the bound is -int.MaxValue and not int.MinValue. All measured; Spark
    /// answers these with a plain ParseException rather than INVALID_NUMERIC_LITERAL_RANGE, and
    /// both are one refusal here.
    /// </remarks>
    [Theory]
    [InlineData("1e2147483648")]
    [InlineData("1e99999999999")]
    [InlineData("1e-99999999999")]
    [InlineData("1e-2147483648")]
    [InlineData("1e-2147483649")]
    [InlineData("0e2147483648")]
    // The decimal path has the same guard, and had the same wrong wording before it was split.
    [InlineData("0e2147483648BD")]
    [InlineData("1e99999999999BD")]
    public void AnExponentTooWideForAScaleIsRefused(string sql)
    {
        var thrown = Assert.Throws<SparkSqlParseException>(() => Parse(sql));

        // NOT the range reason, which would be FALSE for `0e2147483648`: that literal is
        // numerically zero and in the range of every type there is. Raised in review of #287,
        // and it is a distinction Spark draws too -- INVALID_NUMERIC_LITERAL_RANGE names the
        // min and max it compared against, while these come back as a plain ParseException.
        Assert.Contains("exponent", thrown.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("out of range", thrown.Reason, StringComparison.Ordinal);
        Assert.Equal(sql, thrown.Expression);
    }

    /// <summary>
    /// The largest exponent that DOES fit is refused by the RANGE check, with the range reason.
    /// </summary>
    /// <remarks>
    /// The pair of tests is what says the two refusals are told apart rather than merged. It is
    /// also the row that needs the normalisation to count in a long: `1e2147483647` normalises to
    /// an exponent of 2147483648, which an int cannot hold.
    /// </remarks>
    [Fact]
    public void TheLargestExponentThatFitsIsRefusedForItsValueInstead()
    {
        var thrown = Assert.Throws<SparkSqlParseException>(() => Parse("1e2147483647"));

        Assert.Contains("out of range", thrown.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("exponent", thrown.Reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// The neighbouring rules, which are NOT this one and must not have moved.
    /// </summary>
    /// <remarks>
    /// A decimal literal is bounded by its PRECISION -- 39 digits is too many however small the
    /// value, a different error class in Spark -- and an integral literal has no upper bound at
    /// all, because Spark's ladder does not stop at bigint but widens to a decimal. #173.
    /// </remarks>
    [Fact]
    public void ADecimalLiteralIsBoundedByPrecisionAndAnIntegralLiteralNotAtAll()
    {
        Assert.Contains("38", Assert.Throws<SparkSqlParseException>(() => Parse("1e38BD")).Reason,
            StringComparison.Ordinal);
        Assert.Contains("38", Assert.Throws<SparkSqlParseException>(() => Parse("1e400BD")).Reason,
            StringComparison.Ordinal);

        // A 38-digit unscaled value is past System.Decimal's 96 bits, so the accepted literal
        // one below the precision bound lands on the high-precision kind rather than on
        // System.Decimal. Both are decimals; which one is a storage question and not a range one.
        Assert.Equal(LiteralValue.Kind.HighPrecisionDecimal,
            Assert.IsType<LiteralExpression>(Parse("1e37BD")).Value.Type);
        Assert.Equal(LiteralValue.Kind.Decimal,
            Assert.IsType<LiteralExpression>(Parse("9223372036854775808")).Value.Type);
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

    /// <summary>
    /// A leading <c>+</c> becomes a call, exactly as a leading <c>-</c> does.
    /// </summary>
    /// <remarks>
    /// <para>
    /// It used to be DISCARDED, which is right for every numeric operand and wrong for the two
    /// that are not: Spark's <c>UnaryPositive</c> casts a string to <c>double</c> and types a bare
    /// <c>NULL</c> as one, so <c>+'1'</c> is 1.0 and <c>+'abc'</c> refuses where the bare string
    /// did neither. #313, #340.
    /// </para>
    /// <para>
    /// Pinned here as well as by the <c>unary-operators</c> corpus group because it is a property
    /// of the TREE: Spark's own parse names the node <c>UnaryPositive</c> and renders it
    /// <c>(+ a)</c>, so a tree that dropped the operator would be a different tree even where it
    /// happens to evaluate alike.
    /// </para>
    /// </remarks>
    [Fact]
    public void UnaryPlusBecomesACallLikeUnaryMinus()
    {
        Assert.Equal(
            new FunctionCall("positive", new Expression[] { new UnboundReference("a") }),
            Parse("+a"));

        Assert.Equal(
            new FunctionCall("negative", new Expression[] { new UnboundReference("a") }),
            Parse("-a"));

        // Nested, in both orders, because each operator has a type rule and the inner one decides
        // what the outer one is applied to.
        Assert.Equal(
            new FunctionCall(
                "negative",
                new Expression[]
                {
                    new FunctionCall("positive", new Expression[] { new UnboundReference("a") }),
                }),
            Parse("-+a"));

        Assert.Equal(
            new FunctionCall(
                "positive",
                new Expression[]
                {
                    new FunctionCall("positive", new Expression[] { new UnboundReference("a") }),
                }),
            Parse("+ +a"));

        // BINARY plus is untouched: the change is confined to the prefix position.
        Assert.Equal(
            new FunctionCall(
                "+", new Expression[] { new UnboundReference("a"), new UnboundReference("b") }),
            Parse("a + b"));
    }
}
