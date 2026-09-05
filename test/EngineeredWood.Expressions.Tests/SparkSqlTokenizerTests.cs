// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Tests;

public sealed class SparkSqlTokenizerTests
{
    private static List<Token> Scan(string sql) => SparkSqlTokenizer.Tokenize(sql);

    /// <summary>The tokens of <paramref name="sql"/> without the trailing end-of-input marker.</summary>
    private static List<Token> Significant(string sql)
    {
        var tokens = Scan(sql);
        tokens.RemoveAt(tokens.Count - 1);
        return tokens;
    }

    /// <summary>The one token <paramref name="sql"/> is expected to produce.</summary>
    private static Token Only(string sql) => Assert.Single(Significant(sql));

    private static TokenKind[] Kinds(string sql) =>
        Significant(sql).Select(t => t.Kind).ToArray();

    private static string[] Texts(string sql) =>
        Significant(sql).Select(t => t.Text(sql).ToString()).ToArray();

    // ---- Operators, where longest-match decides correctness -------------------------------

    // TokenKind is internal, so the expected kind travels as its name — xunit only discovers
    // public methods, and a public signature cannot mention an internal type.
    [Theory]
    [InlineData("<=>", nameof(TokenKind.NullSafeEqual))]
    [InlineData("<=", nameof(TokenKind.LessThanOrEqual))]
    [InlineData("<", nameof(TokenKind.LessThan))]
    [InlineData("<>", nameof(TokenKind.NotEqual))]
    [InlineData("!=", nameof(TokenKind.NotEqual))]
    [InlineData(">=", nameof(TokenKind.GreaterThanOrEqual))]
    [InlineData(">", nameof(TokenKind.GreaterThan))]
    [InlineData("=", nameof(TokenKind.Equal))]
    [InlineData("==", nameof(TokenKind.Equal))]
    [InlineData("||", nameof(TokenKind.Concat))]
    [InlineData("::", nameof(TokenKind.ColonColon))]
    public void OperatorsTakeTheLongestMatch(string sql, string expected)
    {
        var token = Only(sql);
        Assert.Equal(expected, token.Kind.ToString());
        Assert.Equal(sql.Length, token.Length);
    }

    [Fact]
    public void NullSafeEqualIsNotReadAsLessThanOrEqualFollowedByGreaterThan()
    {
        // `<=>` is what generated-column validation is specified in terms of, so losing it to a
        // shorter match would break the feature rather than merely mis-tokenize.
        Assert.Equal(
            new[] { TokenKind.Identifier, TokenKind.NullSafeEqual, TokenKind.Identifier },
            Kinds("a <=> b"));
    }

    // ---- Numbers --------------------------------------------------------------------------

    [Theory]
    [InlineData("1")]
    [InlineData("1.5")]
    [InlineData(".5")]
    [InlineData("1.")]
    [InlineData("1e3")]
    [InlineData("1.5e-2")]
    [InlineData("1.5E+2")]
    [InlineData("1000000000000")]
    [InlineData("1Y")]
    [InlineData("1S")]
    [InlineData("1L")]
    [InlineData("1F")]
    [InlineData("1D")]
    [InlineData("1BD")]
    [InlineData("1.5bd")]
    public void NumericLiteralsScanAsOneToken(string sql)
    {
        var token = Only(sql);
        Assert.Equal(TokenKind.Number, token.Kind);
        Assert.Equal(sql, token.Text(sql).ToString());
    }

    [Fact]
    public void LeadingDotBelongsToTheNumber()
    {
        // No field name can start with a digit, so `.5` needs no context to disambiguate.
        Assert.Equal(new[] { TokenKind.Number }, Kinds(".5"));
        Assert.Equal(new[] { TokenKind.Identifier, TokenKind.Dot, TokenKind.Identifier }, Kinds("nested.arr"));
    }

    [Fact]
    public void ASuffixIsOnlyASuffixWhenNothingIdentifierLikeFollows()
    {
        // Consuming trailing letters greedily would swallow the operator here and produce one
        // nonsense number token instead of three tokens.
        Assert.Equal(new[] { "1", "and", "b" }, Texts("1and b"));
        Assert.Equal(new[] { "1", "day" }, Texts("1 day"));
    }

    [Fact]
    public void AnExponentNeedsDigitsToCount()
    {
        Assert.Equal(new[] { "1", "e" }, Texts("1e"));
        Assert.Equal(new[] { "1e3" }, Texts("1e3"));
    }

    // ---- Strings and quoted identifiers ---------------------------------------------------

    [Theory]
    [InlineData("'abc'")]
    [InlineData("''")]
    [InlineData("\"abc\"")]
    [InlineData(@"'a\'b'")]
    [InlineData(@"'100\%'")]
    [InlineData("'a, b (c)'")]
    public void StringLiteralsScanAsOneTokenIncludingTheirQuotes(string sql)
    {
        var token = Only(sql);
        Assert.Equal(TokenKind.String, token.Kind);
        Assert.Equal(sql, token.Text(sql).ToString());
    }

    /// <summary>
    /// A doubled quote ends one string literal and starts another — #179.
    /// </summary>
    /// <remarks>
    /// This is the tokenizer half of the fix, and the reason it belongs here rather than in the
    /// parser: Spark's <c>STRING_LITERAL</c> stops at the first unescaped quote, so <c>'it''s'</c>
    /// is two tokens and the parser's job is to join them into <c>its</c>. Reading <c>''</c> as
    /// an escaped quote made it one token holding <c>it's</c>, which no amount of later work
    /// could correct.
    /// </remarks>
    [Theory]
    [InlineData("'it''s'", "'it'", "'s'")]
    [InlineData("''''", "''", "''")]
    [InlineData("'a'''", "'a'", "''")]
    [InlineData("\"a\"\"b\"", "\"a\"", "\"b\"")]
    public void ADoubledQuoteSplitsAStringIntoTwoTokens(string sql, string first, string second)
    {
        var tokens = Significant(sql);

        Assert.Equal(2, tokens.Count);
        Assert.All(tokens, t => Assert.Equal(TokenKind.String, t.Kind));
        Assert.Equal(first, tokens[0].Text(sql).ToString());
        Assert.Equal(second, tokens[1].Text(sql).ToString());
    }

    [Fact]
    public void QuotedIdentifiersKeepTheirSpellingAndNeverBecomeKeywords()
    {
        var tokens = Scan("`weird name`");
        Assert.Equal(TokenKind.QuotedIdentifier, tokens[0].Kind);
        Assert.Equal("weird name", tokens[0].IdentifierName("`weird name`"));

        // A quoted identifier keeps the doubling rule that a string literal loses: it is the only
        // escape a backquoted identifier has, and Spark gives it no backslash escape at all. The
        // two kinds moved apart in #179 and this is the side that did not move.
        const string doubled = "`a``b`";
        Assert.Equal("a`b", Only(doubled).IdentifierName(doubled));
    }

    [Fact]
    public void ABackslashDoesNotEscapeInsideAQuotedIdentifier()
    {
        // A backslash is not an escape here, so `a\` is COMPLETE: the final backquote closes the
        // identifier instead of being escaped by the backslash before it. Inside a STRING literal
        // the same spelling means the opposite: the backslash escapes the quote, the literal runs
        // on, and the scan reaches the end unterminated. Sharing one rule between the two kinds
        // is what #179 undid, and this is the side that had to keep doubling instead.
        const string sql = @"`a\`";
        var token = Only(sql);

        Assert.Equal(TokenKind.QuotedIdentifier, token.Kind);
        Assert.Equal(@"`a\`", token.Text(sql).ToString());
        Assert.Equal(@"a\", token.IdentifierName(sql));
    }

    [Fact]
    public void CaseIsPreservedBecauseDeltaStoresConstraintsAsWritten()
    {
        // Delta persists the token stream re-joined with spaces and does not upper-case, so
        // `and` arrives lowercase; the parser matches keywords case-insensitively instead.
        Assert.Equal(new[] { "a", ">", "0", "and", "b", "IS", "NOT", "NULL" },
            Texts("a > 0 and b IS NOT NULL"));
    }

    // ---- Shapes Delta actually stores ------------------------------------------------------

    [Fact]
    public void TheTokenSpacedFormDeltaPersistsScansIdentically()
    {
        // delta-spark stores `SUBSTRING(s, 1, 2) = 'ab'` as the token stream re-joined with
        // single spaces. Both spellings must produce the same tokens.
        Assert.Equal(Texts("SUBSTRING(s, 1, 2) = 'ab'"), Texts("SUBSTRING ( s , 1 , 2 ) = 'ab'"));
        Assert.Equal(Texts("nested.arr[1] < 5"), Texts("nested . arr [ 1 ] < 5"));
    }

    // ---- Trivia ----------------------------------------------------------------------------

    [Fact]
    public void WhitespaceAndCommentsAreSkipped()
    {
        Assert.Equal(new[] { "a", ">", "0" }, Texts("a -- trailing\n > /* inline */ 0"));
        Assert.Equal(new[] { "a", "-", "b" }, Texts("a - b"));
    }

    // ---- Failures --------------------------------------------------------------------------

    [Theory]
    [InlineData("'unterminated", "unterminated string literal", 0)]
    [InlineData("`unterminated", "unterminated quoted identifier", 0)]
    [InlineData("a /* unterminated", "unterminated block comment", 2)]
    [InlineData("a @ b", "unexpected character '@'", 2)]
    [InlineData("a | b", "unexpected '|'", 2)]
    [InlineData("a ! b", "unexpected '!'", 2)]
    public void MalformedInputFailsWithAQuotablePositionedReason(string sql, string reason, int position)
    {
        var ex = Assert.Throws<SparkSqlParseException>(() => Scan(sql));
        Assert.Equal(reason, ex.Reason);
        Assert.Equal(position, ex.Position);
        Assert.Equal(sql, ex.Expression);
        Assert.Contains(sql, ex.Message);
    }

    // ---- The corpus ------------------------------------------------------------------------

    [Fact]
    public void EveryExpressionSparkAcceptsAlsoScans()
    {
        // 190+ real expressions. Scanning is strictly weaker than parsing, so anything Spark
        // parsed must at minimum tokenize; a failure here is a hole in the token set.
        var failures = new List<string>();

        foreach (var sql in SparkCorpus.ParsableExpressions())
        {
            try
            {
                var tokens = SparkSqlTokenizer.Tokenize(sql);
                Assert.Equal(TokenKind.EndOfInput, tokens[^1].Kind);
            }
            catch (SparkSqlParseException ex)
            {
                failures.Add($"{sql}  ->  {ex.Reason} at {ex.Position}");
            }
        }

        Assert.Empty(failures);
    }

    [Fact]
    public void MalformedCorpusEntriesStillScan()
    {
        // `a +`, `((a)`, `a > > 0` and `` are parse errors, not lexical ones. Keeping them
        // scannable is what proves the two layers are actually separate.
        foreach (var entry in SparkCorpus.Group("malformed").EnumerateArray())
            SparkSqlTokenizer.Tokenize(entry.Expression());
    }

    [Fact]
    public void TokensCoverTheSourceInOrderWithoutOverlapping()
    {
        // The corpus carries no comments, so the three appended cases are the only ones that
        // exercise a gap holding anything other than whitespace.
        var cases = SparkCorpus.ParsableExpressions().Concat(new[]
        {
            "a -- dropped? no: comment body\n > 0",
            "a /* a - b * c / d */ > 0",
            "a/*x*/+/*y*/b",
        });

        foreach (var sql in cases)
        {
            var previousEnd = 0;

            foreach (var token in SparkSqlTokenizer.Tokenize(sql))
            {
                Assert.True(token.Start >= previousEnd,
                    $"token at {token.Start} overlaps the previous one in: {sql}");
                Assert.True(token.Start + token.Length <= sql.Length,
                    $"token at {token.Start} runs past the end of: {sql}");

                AssertGapIsOnlyTrivia(sql, previousEnd, token.Start);
                previousEnd = token.Start + token.Length;
            }
        }
    }


    /// <summary>
    /// Asserts that everything the tokenizer skipped between two tokens really was trivia.
    /// </summary>
    /// <remarks>
    /// This re-walks the gap rather than accepting a set of characters. Whitelisting <c>-</c>,
    /// <c>/</c> and <c>*</c> — so that comment markers pass — would whitelist exactly the
    /// operator characters whose loss this invariant exists to catch, and would still reject the
    /// arbitrary text inside a comment body. Recognising the two comment forms is the only way
    /// to be both.
    /// </remarks>
    private static void AssertGapIsOnlyTrivia(string sql, int start, int end)
    {
        var i = start;

        while (i < end)
        {
            if (char.IsWhiteSpace(sql[i]))
            {
                i++;
            }
            else if (sql[i] == '-' && i + 1 < end && sql[i + 1] == '-')
            {
                while (i < end && sql[i] != '\n')
                    i++;
            }
            else if (sql[i] == '/' && i + 1 < end && sql[i + 1] == '*')
            {
                var closed = false;
                for (var j = i + 2; j + 1 < end; j++)
                {
                    if (sql[j] == '*' && sql[j + 1] == '/')
                    {
                        i = j + 2;
                        closed = true;
                        break;
                    }
                }

                Assert.True(closed, $"unterminated block comment skipped at {i} in: {sql}");
            }
            else
            {
                Assert.Fail($"character '{sql[i]}' at {i} was dropped from: {sql}");
            }
        }
    }
}
