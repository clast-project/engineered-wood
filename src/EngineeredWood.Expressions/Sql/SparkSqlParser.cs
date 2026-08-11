// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Sql;

/// <summary>
/// Parses Spark SQL expression text — a Delta CHECK constraint, invariant, or generation
/// expression — into an <see cref="Expression"/> tree.
/// </summary>
/// <remarks>
/// <para>
/// Hand-written recursive descent with precedence climbing. See "The Spark SQL parser" in
/// <c>doc/predicate-pushdown-design.md</c> for why this is not a generated grammar, and
/// <see cref="SparkSqlTokenizer"/> for the scanner beneath it.
/// </para>
/// <para>
/// <b>What it does not do.</b> It resolves no columns — every reference comes out as an
/// <see cref="UnboundReference"/> for <see cref="ExpressionBinder"/> to bind against a schema —
/// and it evaluates nothing. It also does not reject aggregates, window functions or
/// subqueries on principle: Spark's own expression parser accepts all three and Delta rejects
/// them afterwards with <c>DELTA_UNSUPPORTED_EXPRESSION_CHECK_CONSTRAINT</c>, so refusing them
/// here would be a validation decision wearing a grammar's clothes. They fail only because this
/// tree cannot represent them, and they fail with a quotable reason.
/// </para>
/// <para>
/// <b>Precedence</b>, tightest first, as measured from Spark rather than assumed: postfix
/// (<c>.</c>, <c>[]</c>, <c>::</c>), unary <c>-</c>/<c>+</c>, <c>* / %</c>, <c>+ - ||</c>,
/// comparison and the <c>IS</c>/<c>IN</c>/<c>BETWEEN</c>/<c>LIKE</c> suffixes, <c>NOT</c>,
/// <c>AND</c>, <c>OR</c>. The corpus pins the two that are easy to get wrong:
/// <c>1 + 2 * 3</c> is <c>(1 + (2 * 3))</c>, and <c>NOT a &gt; 0 AND b &gt; 0</c> is
/// <c>((NOT (a &gt; 0)) AND (b &gt; 0))</c> — <c>NOT</c> binds looser than comparison but
/// tighter than <c>AND</c>.
/// </para>
/// </remarks>
public static class SparkSqlParser
{
    /// <summary>Parses <paramref name="sql"/> as a value-producing expression.</summary>
    /// <exception cref="SparkSqlParseException">
    /// The text is malformed, or uses a construct this parser does not support.
    /// </exception>
    public static Expression ParseExpression(string sql)
    {
        if (sql is null)
            throw new ArgumentNullException(nameof(sql));

        return new Impl(sql).ParseAll();
    }

    /// <summary>
    /// Parses <paramref name="sql"/> as a boolean predicate — a CHECK constraint or invariant.
    /// </summary>
    /// <remarks>
    /// A boolean-valued expression that is not already a <see cref="Predicate"/> — a boolean
    /// column, or a call like <c>isnotnull(x)</c> — becomes <c>expr = TRUE</c>. That is exact
    /// under three-valued logic rather than a convenience: both forms are true, false and null
    /// on exactly the same inputs, so a null result still fails the constraint. Note this is
    /// deliberately <em>not</em> what <c>IS TRUE</c> means; see the <c>IS</c> handling below.
    /// </remarks>
    /// <exception cref="SparkSqlParseException">
    /// The text is malformed, or uses a construct this parser does not support.
    /// </exception>
    public static Predicate ParsePredicate(string sql) => Impl.AsPredicate(ParseExpression(sql));

    private sealed class Impl
    {
        private readonly string _sql;
        private readonly List<Token> _tokens;
        private int _index;

        internal Impl(string sql)
        {
            _sql = sql;
            _tokens = SparkSqlTokenizer.Tokenize(sql);
        }

        // ── Entry ──────────────────────────────────────────────────────────────────────

        internal Expression ParseAll()
        {
            var expression = ParseOr();

            if (Current.Kind != TokenKind.EndOfInput)
                throw Fail($"unexpected trailing input '{TextOf(Current)}'");

            return expression;
        }

        /// <summary>Coerces a value-producing expression into predicate position.</summary>
        internal static Predicate AsPredicate(Expression expression) => expression switch
        {
            Predicate predicate => predicate,
            LiteralExpression literal when literal.Value.Type == LiteralValue.Kind.Boolean
                => literal.Value.AsBoolean ? new TruePredicate() : new FalsePredicate(),
            _ => new ComparisonPredicate(expression, ComparisonOperator.Equal, Lit(true)),
        };

        // ── OR / AND / NOT ─────────────────────────────────────────────────────────────

        private Expression ParseOr()
        {
            var left = ParseAnd();
            if (!IsKeyword("OR"))
                return left;

            // Flattened rather than nested: the tree's junctions are n-ary, and `a OR b OR c`
            // renders as one node instead of two.
            var children = new List<Predicate> { AsPredicate(left) };
            while (TakeKeyword("OR"))
                children.Add(AsPredicate(ParseAnd()));

            return new OrPredicate(children);
        }

        private Expression ParseAnd()
        {
            var left = ParseNot();
            if (!IsKeyword("AND"))
                return left;

            var children = new List<Predicate> { AsPredicate(left) };
            while (TakeKeyword("AND"))
                children.Add(AsPredicate(ParseNot()));

            return new AndPredicate(children);
        }

        private Expression ParseNot()
        {
            if (!TakeKeyword("NOT"))
                return ParseComparison();

            return new NotPredicate(AsPredicate(ParseNot()));
        }

        // ── Comparison and its suffixes ────────────────────────────────────────────────

        private Expression ParseComparison()
        {
            var left = ParseAdditive();

            var op = Current.Kind switch
            {
                TokenKind.Equal => ComparisonOperator.Equal,
                TokenKind.NotEqual => ComparisonOperator.NotEqual,
                TokenKind.LessThan => ComparisonOperator.LessThan,
                TokenKind.LessThanOrEqual => ComparisonOperator.LessThanOrEqual,
                TokenKind.GreaterThan => ComparisonOperator.GreaterThan,
                TokenKind.GreaterThanOrEqual => ComparisonOperator.GreaterThanOrEqual,
                TokenKind.NullSafeEqual => ComparisonOperator.NullSafeEqual,
                _ => (ComparisonOperator?)null,
            } is { } comparison ? comparison : default;

            if (IsComparisonToken(Current.Kind))
            {
                Advance();
                return new ComparisonPredicate(left, op, ParseAdditive());
            }

            if (IsKeyword("IS"))
                return ParseIsSuffix(left);

            // `NOT` here belongs to the suffix (`a NOT IN (…)`), not to a fresh predicate.
            var negated = false;
            var savedIndex = _index;
            if (TakeKeyword("NOT"))
            {
                negated = true;
                if (!IsKeyword("IN") && !IsKeyword("BETWEEN") && !IsKeyword("LIKE")
                    && !IsKeyword("ILIKE") && !IsKeyword("RLIKE"))
                {
                    _index = savedIndex;
                    return left;
                }
            }

            if (TakeKeyword("IN"))
                return ParseInSuffix(left, negated);

            if (TakeKeyword("BETWEEN"))
                return ParseBetweenSuffix(left, negated);

            if (IsKeyword("LIKE") || IsKeyword("ILIKE") || IsKeyword("RLIKE"))
                return ParseLikeSuffix(left, negated);

            return left;
        }

        private static bool IsComparisonToken(TokenKind kind) => kind is
            TokenKind.Equal or TokenKind.NotEqual or TokenKind.LessThan or TokenKind.LessThanOrEqual
            or TokenKind.GreaterThan or TokenKind.GreaterThanOrEqual or TokenKind.NullSafeEqual;

        /// <summary>Parses <c>IS [NOT] NULL | TRUE | FALSE</c>.</summary>
        /// <remarks>
        /// <c>IS TRUE</c> becomes <c>x &lt;=&gt; TRUE</c>, not <c>x = TRUE</c>. The distinction is
        /// the whole point of the form: a null operand makes <c>IS TRUE</c> <em>false</em>, while
        /// <c>= TRUE</c> stays null. Null-safe equality has exactly the wanted behaviour.
        /// </remarks>
        private Expression ParseIsSuffix(Expression left)
        {
            ExpectKeyword("IS");
            var negated = TakeKeyword("NOT");

            if (TakeKeyword("NULL"))
                return new UnaryPredicate(left, negated ? UnaryOperator.IsNotNull : UnaryOperator.IsNull);

            bool wanted;
            if (TakeKeyword("TRUE"))
                wanted = true;
            else if (TakeKeyword("FALSE"))
                wanted = false;
            else
                throw Fail($"expected NULL, TRUE or FALSE after IS, found '{TextOf(Current)}'");

            Predicate test = new ComparisonPredicate(left, ComparisonOperator.NullSafeEqual, Lit(wanted));
            return negated ? new NotPredicate(test) : test;
        }

        /// <summary>Parses <c>[NOT] IN (…)</c>.</summary>
        /// <remarks>
        /// An all-literal list becomes a <see cref="SetPredicate"/>, which stats pruning can use.
        /// A list containing expressions cannot — <see cref="SetPredicate"/> holds
        /// <see cref="LiteralValue"/>s — so it expands to the disjunction of equalities that SQL
        /// defines <c>IN</c> to mean, which carries the same three-valued behaviour.
        /// </remarks>
        private Expression ParseInSuffix(Expression left, bool negated)
        {
            Expect(TokenKind.OpenParen, "(");

            var items = new List<Expression>();
            if (Current.Kind != TokenKind.CloseParen)
            {
                do
                {
                    items.Add(ParseOr());
                }
                while (TakeToken(TokenKind.Comma));
            }

            Expect(TokenKind.CloseParen, ")");

            if (items.Count == 0)
                throw Fail("IN requires at least one value");

            if (items.TrueForAll(item => item is LiteralExpression))
            {
                var values = new List<LiteralValue>(items.Count);
                foreach (var item in items)
                    values.Add(((LiteralExpression)item).Value);

                return new SetPredicate(left, values, negated ? SetOperator.NotIn : SetOperator.In);
            }

            var alternatives = new List<Predicate>(items.Count);
            foreach (var item in items)
                alternatives.Add(new ComparisonPredicate(left, ComparisonOperator.Equal, item));

            Predicate any = alternatives.Count == 1 ? alternatives[0] : new OrPredicate(alternatives);
            return negated ? new NotPredicate(any) : any;
        }

        /// <summary>
        /// Parses <c>[NOT] BETWEEN low AND high</c>, expanding to the two comparisons it means.
        /// </summary>
        private Expression ParseBetweenSuffix(Expression left, bool negated)
        {
            // Parsed below AND so this consumes BETWEEN's own AND rather than the junction's.
            var low = ParseAdditive();
            ExpectKeyword("AND");
            var high = ParseAdditive();

            Predicate range = new AndPredicate(new Predicate[]
            {
                new ComparisonPredicate(left, ComparisonOperator.GreaterThanOrEqual, low),
                new ComparisonPredicate(left, ComparisonOperator.LessThanOrEqual, high),
            });

            return negated ? new NotPredicate(range) : range;
        }

        /// <summary>Parses <c>[NOT] LIKE | ILIKE | RLIKE pattern</c>.</summary>
        /// <remarks>
        /// Pattern matching has no node in this tree, so it becomes a call the function registry
        /// resolves. It is deliberately not folded into <see cref="ComparisonOperator.StartsWith"/>
        /// even when the pattern looks like <c>'prefix%'</c>: that is only sound after accounting
        /// for escapes and for wildcards elsewhere in the pattern, and getting it wrong would
        /// silently change which rows a constraint accepts.
        /// </remarks>
        private Expression ParseLikeSuffix(Expression left, bool negated)
        {
            var name = TextOf(Current).ToLowerInvariant();
            Advance();

            var pattern = ParseAdditive();
            Predicate match = AsPredicate(new FunctionCall(name, new[] { left, pattern }));
            return negated ? new NotPredicate(match) : match;
        }

        // ── Arithmetic ─────────────────────────────────────────────────────────────────

        private Expression ParseAdditive()
        {
            var left = ParseMultiplicative();

            while (true)
            {
                var name = Current.Kind switch
                {
                    TokenKind.Plus => "+",
                    TokenKind.Minus => "-",
                    TokenKind.Concat => "||",
                    _ => null,
                };

                if (name is null)
                    return left;

                Advance();
                left = new FunctionCall(name, new[] { left, ParseMultiplicative() });
            }
        }

        private Expression ParseMultiplicative()
        {
            var left = ParseUnary();

            while (true)
            {
                var name = Current.Kind switch
                {
                    TokenKind.Star => "*",
                    TokenKind.Slash => "/",
                    TokenKind.Percent => "%",
                    _ => null,
                };

                if (name is null)
                    return left;

                Advance();
                left = new FunctionCall(name, new[] { left, ParseUnary() });
            }
        }

        private Expression ParseUnary()
        {
            if (TakeToken(TokenKind.Plus))
                return ParseUnary();

            if (TakeToken(TokenKind.Minus))
                return new FunctionCall("negative", new[] { ParseUnary() });

            return ParsePostfix();
        }

        // ── Postfix: field access, subscript, cast shorthand ───────────────────────────

        private Expression ParsePostfix()
        {
            var expression = ParsePrimary();

            while (true)
            {
                if (TakeToken(TokenKind.Dot))
                {
                    // Nested field access joins the path, so `nested.arr` binds as one name the
                    // way Delta writes it rather than as a call on `nested`.
                    var field = ExpectIdentifier("a field name after '.'");
                    if (expression is UnboundReference reference)
                        expression = new UnboundReference(reference.Name + "." + field);
                    else
                        expression = new FunctionCall("getfield", new[] { expression, Lit(field) });
                }
                else if (TakeToken(TokenKind.OpenBracket))
                {
                    var index = ParseOr();
                    Expect(TokenKind.CloseBracket, "]");
                    expression = new FunctionCall("getitem", new[] { expression, index });
                }
                else if (TakeToken(TokenKind.ColonColon))
                {
                    expression = new FunctionCall("cast", new[] { expression, Lit(ParseTypeName()) });
                }
                else
                {
                    return expression;
                }
            }
        }

        // ── Primaries ──────────────────────────────────────────────────────────────────

        private Expression ParsePrimary()
        {
            var token = Current;

            switch (token.Kind)
            {
                case TokenKind.Number:
                    Advance();
                    return new LiteralExpression(SparkLiteral.Number(TextOf(token), _sql, token.Start));

                case TokenKind.String:
                    Advance();
                    return new LiteralExpression(SparkLiteral.String(TextOf(token)));

                case TokenKind.QuotedIdentifier:
                    Advance();
                    return new UnboundReference(token.IdentifierName(_sql));

                case TokenKind.OpenParen:
                    Advance();
                    var grouped = ParseOr();
                    Expect(TokenKind.CloseParen, ")");
                    return grouped;

                case TokenKind.Identifier:
                    return ParseIdentifierPrimary();

                case TokenKind.EndOfInput:
                    throw Fail("unexpected end of expression");

                default:
                    throw Fail($"'{TextOf(token)}' cannot start an expression");
            }
        }

        private Expression ParseIdentifierPrimary()
        {
            var token = Current;
            var word = TextOf(token);

            if (Matches(word, "CASE"))
                return ParseCase();

            if (Matches(word, "CAST") || Matches(word, "TRY_CAST"))
                return ParseCast(word.ToLowerInvariant());

            if (Matches(word, "NULL"))
            {
                Advance();
                return new LiteralExpression(LiteralValue.Null);
            }

            if (Matches(word, "TRUE") || Matches(word, "FALSE"))
            {
                Advance();
                return Lit(Matches(word, "TRUE"));
            }

            if (Matches(word, "INTERVAL"))
                throw Fail("INTERVAL literals are not supported");

            // Caught here only to say so plainly. Without this the failure surfaces as a
            // mismatched parenthesis several tokens later, which tells a caller nothing about
            // why their constraint was refused.
            if (Matches(word, "SELECT"))
                throw Fail("subqueries are not supported");

            // A typed literal is a keyword abutting a string: DATE '…', TIMESTAMP '…', X'…'.
            if (Peek(1).Kind == TokenKind.String
                && (Matches(word, "DATE") || Matches(word, "TIMESTAMP") || Matches(word, "X")))
            {
                Advance();
                var text = TextOf(Current);
                var start = Current.Start;
                Advance();
                return new LiteralExpression(SparkLiteral.Typed(word, text, _sql, start));
            }

            Advance();

            if (Current.Kind != TokenKind.OpenParen)
                return new UnboundReference(word);

            Advance();
            var arguments = new List<Expression>();
            if (Current.Kind != TokenKind.CloseParen)
            {
                do
                {
                    // `count(*)` and friends: the star is not an expression this tree can hold.
                    if (Current.Kind == TokenKind.Star)
                        throw Fail($"'*' is not supported as an argument to {word}");

                    arguments.Add(ParseOr());
                }
                while (TakeToken(TokenKind.Comma));
            }

            Expect(TokenKind.CloseParen, ")");

            if (IsKeyword("OVER"))
                throw Fail("window functions are not supported");

            return new FunctionCall(word.ToLowerInvariant(), arguments);
        }

        /// <summary>
        /// Parses <c>CASE [operand] WHEN … THEN … [ELSE …] END</c>.
        /// </summary>
        /// <remarks>
        /// Emitted as <c>case(when1, then1, …, whenN, thenN[, else])</c> — an odd argument count
        /// means the trailing ELSE is present. The shape is kept rather than rewritten into
        /// nested <c>if</c> calls so the tree still resembles what was written, which matters if
        /// an expression ever has to be rendered back to SQL.
        /// </remarks>
        private Expression ParseCase()
        {
            ExpectKeyword("CASE");

            // `CASE x WHEN 1 THEN …` compares against x; `CASE WHEN c THEN …` tests c directly.
            Expression? operand = IsKeyword("WHEN") ? null : ParseOr();

            var arguments = new List<Expression>();
            while (TakeKeyword("WHEN"))
            {
                var condition = ParseOr();
                if (operand is not null)
                    condition = new ComparisonPredicate(operand, ComparisonOperator.Equal, condition);

                ExpectKeyword("THEN");
                arguments.Add(condition);
                arguments.Add(ParseOr());
            }

            if (arguments.Count == 0)
                throw Fail("CASE requires at least one WHEN branch");

            if (TakeKeyword("ELSE"))
                arguments.Add(ParseOr());

            ExpectKeyword("END");
            return new FunctionCall("case", arguments);
        }

        private Expression ParseCast(string name)
        {
            Advance();
            Expect(TokenKind.OpenParen, "(");
            var operand = ParseOr();
            ExpectKeyword("AS");
            var type = ParseTypeName();
            Expect(TokenKind.CloseParen, ")");

            return new FunctionCall(name, new[] { operand, Lit(type) });
        }

        /// <summary>
        /// Reads a type name, keeping any parenthesised parameters: <c>DECIMAL(10,2)</c>.
        /// </summary>
        private string ParseTypeName()
        {
            var name = ExpectIdentifier("a type name");

            if (Current.Kind != TokenKind.OpenParen)
                return name;

            var start = Current.Start;
            Advance();
            while (Current.Kind is TokenKind.Number or TokenKind.Comma)
                Advance();

            if (Current.Kind != TokenKind.CloseParen)
                throw Fail($"unsupported type parameters in '{name}'");

            var end = Current.Start + Current.Length;
            Advance();
            return name + _sql.Substring(start, end - start);
        }

        // ── Token plumbing ─────────────────────────────────────────────────────────────

        private Token Current => _tokens[_index];

        private Token Peek(int offset) =>
            _tokens[Math.Min(_index + offset, _tokens.Count - 1)];

        private void Advance()
        {
            if (_index < _tokens.Count - 1)
                _index++;
        }

        private string TextOf(Token token) => _sql.Substring(token.Start, token.Length);

        private static bool Matches(string word, string keyword) =>
            string.Equals(word, keyword, StringComparison.OrdinalIgnoreCase);

        // Keywords are matched here rather than recognised by the tokenizer, because Spark makes
        // most of them non-reserved — `value` and `year` are legal column names — and because
        // Delta stores constraints with their original casing, so `and` arrives lowercase.
        //
        // Compared over the token's span rather than a substring of it. Every precedence level
        // probes several keywords per operand, so materialising a string for each probe would
        // allocate steadily through a parse — and would give up the reason Token stores a range
        // into the source instead of its own copy.
        private bool IsKeyword(string keyword) =>
            Current.Kind == TokenKind.Identifier
            && Current.Text(_sql).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase);

        private bool TakeKeyword(string keyword)
        {
            if (!IsKeyword(keyword))
                return false;

            Advance();
            return true;
        }

        private void ExpectKeyword(string keyword)
        {
            if (!TakeKeyword(keyword))
                throw Fail($"expected {keyword}, found '{TextOf(Current)}'");
        }

        private bool TakeToken(TokenKind kind)
        {
            if (Current.Kind != kind)
                return false;

            Advance();
            return true;
        }

        private void Expect(TokenKind kind, string what)
        {
            if (!TakeToken(kind))
                throw Fail($"expected '{what}', found '{TextOf(Current)}'");
        }

        private string ExpectIdentifier(string what)
        {
            var token = Current;
            if (token.Kind is not (TokenKind.Identifier or TokenKind.QuotedIdentifier))
                throw Fail($"expected {what}, found '{TextOf(token)}'");

            Advance();
            return token.IdentifierName(_sql);
        }

        private static LiteralExpression Lit(bool value) => new(LiteralValue.Of(value));

        private static LiteralExpression Lit(string value) => new(LiteralValue.Of(value));

        private SparkSqlParseException Fail(string message) =>
            new(message, _sql, Current.Start);
    }
}
