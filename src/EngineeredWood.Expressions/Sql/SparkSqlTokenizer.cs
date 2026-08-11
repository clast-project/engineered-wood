// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Sql;

/// <summary>
/// Scans a Spark SQL expression into <see cref="Token"/>s.
/// </summary>
/// <remarks>
/// <para>
/// Hand-written, with no dependency beyond the BCL — see "The Spark SQL parser" in
/// <c>doc/predicate-pushdown-design.md</c> for why this is not a generated grammar.
/// </para>
/// <para>
/// The scanner recognises shapes, never meaning. It does not know that <c>AND</c> is an
/// operator, that <c>DATE</c> may precede a string to form a typed literal, or that <c>1.5</c>
/// is a decimal while <c>1e3</c> is a double. Everything it produces is a category and a range;
/// the parser and its lowering step supply the rest. That split is what lets the same token
/// stream serve constructs the parser has not learned yet.
/// </para>
/// <para>
/// Two input shapes are worth knowing about because Delta produces them. Constraint text is
/// stored as the token stream re-joined with single spaces, so <c>SUBSTRING(s, 1, 2) = 'ab'</c>
/// comes back as <c>SUBSTRING ( s , 1 , 2 ) = 'ab'</c> — harmless here, since whitespace is not
/// significant. And casing is preserved, so <c>and</c> arrives lowercase; this scanner never
/// case-folds, and the parser compares keywords case-insensitively instead.
/// </para>
/// </remarks>
internal static class SparkSqlTokenizer
{
    /// <summary>
    /// Scans <paramref name="expression"/> to completion, ending with
    /// <see cref="TokenKind.EndOfInput"/>.
    /// </summary>
    /// <exception cref="SparkSqlParseException">
    /// A string, quoted identifier, or block comment is unterminated, or a character appears that
    /// cannot begin a token.
    /// </exception>
    public static List<Token> Tokenize(string expression)
    {
        if (expression is null)
            throw new ArgumentNullException(nameof(expression));

        var tokens = new List<Token>();
        var position = 0;

        while (true)
        {
            position = SkipTrivia(expression, position);
            if (position >= expression.Length)
                break;

            tokens.Add(ScanToken(expression, ref position));
        }

        tokens.Add(new Token(TokenKind.EndOfInput, expression.Length, 0));
        return tokens;
    }

    /// <summary>Advances past whitespace and comments, which carry no meaning between tokens.</summary>
    private static int SkipTrivia(string source, int position)
    {
        while (position < source.Length)
        {
            var c = source[position];

            if (char.IsWhiteSpace(c))
            {
                position++;
            }
            else if (c == '-' && position + 1 < source.Length && source[position + 1] == '-')
            {
                while (position < source.Length && source[position] != '\n')
                    position++;
            }
            else if (c == '/' && position + 1 < source.Length && source[position + 1] == '*')
            {
                var start = position;
                position += 2;
                while (true)
                {
                    if (position + 1 >= source.Length)
                        throw Fail("unterminated block comment", source, start);
                    if (source[position] == '*' && source[position + 1] == '/')
                    {
                        position += 2;
                        break;
                    }
                    position++;
                }
            }
            else
            {
                break;
            }
        }

        return position;
    }

    private static Token ScanToken(string source, ref int position)
    {
        var start = position;
        var c = source[position];

        // A dot introducing a digit belongs to the number: `.5` is a literal, and no field name
        // can start with a digit, so there is nothing to disambiguate against.
        if (IsDigit(c) || (c == '.' && position + 1 < source.Length && IsDigit(source[position + 1])))
            return ScanNumber(source, ref position);

        if (IsIdentifierStart(c))
            return ScanIdentifier(source, ref position);

        switch (c)
        {
            case '\'':
            case '"':
                return ScanQuoted(source, ref position, c, TokenKind.String, "string literal");

            case '`':
                return ScanQuoted(source, ref position, c, TokenKind.QuotedIdentifier, "quoted identifier");

            case '(': position++; return new Token(TokenKind.OpenParen, start, 1);
            case ')': position++; return new Token(TokenKind.CloseParen, start, 1);
            case '[': position++; return new Token(TokenKind.OpenBracket, start, 1);
            case ']': position++; return new Token(TokenKind.CloseBracket, start, 1);
            case ',': position++; return new Token(TokenKind.Comma, start, 1);
            case '.': position++; return new Token(TokenKind.Dot, start, 1);
            case '+': position++; return new Token(TokenKind.Plus, start, 1);
            case '-': position++; return new Token(TokenKind.Minus, start, 1);
            case '*': position++; return new Token(TokenKind.Star, start, 1);
            case '/': position++; return new Token(TokenKind.Slash, start, 1);
            case '%': position++; return new Token(TokenKind.Percent, start, 1);

            case ':':
                if (Peek(source, position + 1) == ':')
                {
                    position += 2;
                    return new Token(TokenKind.ColonColon, start, 2);
                }
                throw Fail("unexpected ':'", source, start);

            case '|':
                if (Peek(source, position + 1) == '|')
                {
                    position += 2;
                    return new Token(TokenKind.Concat, start, 2);
                }
                throw Fail("unexpected '|'", source, start);

            case '=':
                // `==` is accepted as a synonym for `=`, as Spark accepts it.
                position += Peek(source, position + 1) == '=' ? 2 : 1;
                return new Token(TokenKind.Equal, start, position - start);

            case '!':
                if (Peek(source, position + 1) == '=')
                {
                    position += 2;
                    return new Token(TokenKind.NotEqual, start, 2);
                }
                throw Fail("unexpected '!'", source, start);

            case '<':
                // Longest match first: `<=>` before `<=`, or null-safe equality is lost.
                if (Peek(source, position + 1) == '=' && Peek(source, position + 2) == '>')
                {
                    position += 3;
                    return new Token(TokenKind.NullSafeEqual, start, 3);
                }
                if (Peek(source, position + 1) == '=')
                {
                    position += 2;
                    return new Token(TokenKind.LessThanOrEqual, start, 2);
                }
                if (Peek(source, position + 1) == '>')
                {
                    position += 2;
                    return new Token(TokenKind.NotEqual, start, 2);
                }
                position++;
                return new Token(TokenKind.LessThan, start, 1);

            case '>':
                if (Peek(source, position + 1) == '=')
                {
                    position += 2;
                    return new Token(TokenKind.GreaterThanOrEqual, start, 2);
                }
                position++;
                return new Token(TokenKind.GreaterThan, start, 1);

            default:
                throw Fail($"unexpected character '{c}'", source, start);
        }
    }

    /// <summary>
    /// Scans a numeric literal: digits with an optional fraction, exponent, and type suffix.
    /// </summary>
    /// <remarks>
    /// The suffix set is closed (<c>Y S L F D BD</c>, case-insensitive) rather than "consume any
    /// trailing letters". Greedy consumption would swallow the operator in <c>a&gt;1and b&gt;2</c>,
    /// and Delta's token-spaced storage form is not the only shape this has to read.
    /// </remarks>
    private static Token ScanNumber(string source, ref int position)
    {
        var start = position;

        while (position < source.Length && IsDigit(source[position]))
            position++;

        if (position < source.Length && source[position] == '.')
        {
            position++;
            while (position < source.Length && IsDigit(source[position]))
                position++;
        }

        // An exponent only counts if digits actually follow, so `1e` is a number then an
        // identifier rather than a malformed literal.
        if (position < source.Length && (source[position] == 'e' || source[position] == 'E'))
        {
            var afterExponent = position + 1;
            if (afterExponent < source.Length && (source[afterExponent] == '+' || source[afterExponent] == '-'))
                afterExponent++;

            if (afterExponent < source.Length && IsDigit(source[afterExponent]))
            {
                position = afterExponent;
                while (position < source.Length && IsDigit(source[position]))
                    position++;
            }
        }

        position += SuffixLength(source, position);
        return new Token(TokenKind.Number, start, position - start);
    }

    /// <summary>Length of the type suffix at <paramref name="position"/>, or zero.</summary>
    private static int SuffixLength(string source, int position)
    {
        if (position >= source.Length)
            return 0;

        // `BD` before `D`: longest match, or `1BD` reads as `1B` plus a stray `D`.
        if (position + 1 < source.Length
            && (source[position] == 'b' || source[position] == 'B')
            && (source[position + 1] == 'd' || source[position + 1] == 'D')
            && !IsIdentifierPart(Peek(source, position + 2)))
        {
            return 2;
        }

        switch (source[position])
        {
            case 'y' or 'Y' or 's' or 'S' or 'l' or 'L' or 'f' or 'F' or 'd' or 'D':
                return IsIdentifierPart(Peek(source, position + 1)) ? 0 : 1;
            default:
                return 0;
        }
    }

    private static Token ScanIdentifier(string source, ref int position)
    {
        var start = position;
        while (position < source.Length && IsIdentifierPart(source[position]))
            position++;

        return new Token(TokenKind.Identifier, start, position - start);
    }

    /// <summary>
    /// Scans text delimited by <paramref name="quote"/>, where the delimiter is escaped by
    /// doubling it and a backslash escapes whatever follows.
    /// </summary>
    /// <remarks>
    /// Both escapes matter to where the token ends, which is why they are handled here rather
    /// than left to lowering: <c>'it''s'</c> is one token, not two, and <c>'a\'b'</c> does not
    /// end at its middle quote. Unescaping is still lowering's job — this keeps the text as
    /// written.
    /// </remarks>
    private static Token ScanQuoted(string source, ref int position, char quote, TokenKind kind, string what)
    {
        var start = position;
        position++;

        while (true)
        {
            if (position >= source.Length)
                throw Fail($"unterminated {what}", source, start);

            var c = source[position];

            if (c == '\\' && kind == TokenKind.String && position + 1 < source.Length)
            {
                position += 2;
                continue;
            }

            if (c == quote)
            {
                if (Peek(source, position + 1) == quote)
                {
                    position += 2;
                    continue;
                }

                position++;
                return new Token(kind, start, position - start);
            }

            position++;
        }
    }

    private static char Peek(string source, int position) =>
        position < source.Length ? source[position] : '\0';

    // Explicitly ASCII rather than char.IsDigit, which accepts every Unicode decimal digit and
    // would admit numerals no numeric parser downstream can read.
    private static bool IsDigit(char c) => c >= '0' && c <= '9';

    private static bool IsIdentifierStart(char c) =>
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';

    private static bool IsIdentifierPart(char c) => IsIdentifierStart(c) || IsDigit(c);

    private static SparkSqlParseException Fail(string message, string source, int position) =>
        new SparkSqlParseException(message, source, position);
}
