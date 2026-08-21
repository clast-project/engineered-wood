// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Sql;

/// <summary>
/// One lexical token, stored as a range over the source rather than a copied string.
/// </summary>
/// <remarks>
/// The tokenizer does not interpret what it reads: a <see cref="TokenKind.String"/> keeps its
/// quotes and escapes, and a <see cref="TokenKind.Number"/> keeps its exponent and type suffix.
/// Turning <c>'it''s'</c> into <c>it's</c>, or deciding that <c>1.5</c> is a
/// <c>decimal(2,1)</c> while <c>1e3</c> is a <c>double</c>, is lowering — it needs Spark's
/// typing rules, and it belongs with them rather than in the scanner.
/// </remarks>
internal readonly struct Token
{
    internal Token(TokenKind kind, int start, int length)
    {
        Kind = kind;
        Start = start;
        Length = length;
    }

    /// <summary>The token's lexical category.</summary>
    public TokenKind Kind { get; }

    /// <summary>Index of the token's first character in the source.</summary>
    public int Start { get; }

    /// <summary>Length of the token in characters. Zero for <see cref="TokenKind.EndOfInput"/>.</summary>
    public int Length { get; }

    /// <summary>The token's text, as written.</summary>
    public ReadOnlySpan<char> Text(string source) => source.AsSpan(Start, Length);

    /// <summary>
    /// The identifier this token names, with backticks removed and doubled backticks collapsed.
    /// </summary>
    /// <remarks>
    /// Case is preserved. Spark resolves column names case-insensitively by default, but that is
    /// the binder's decision to make against a schema, not something to discard here.
    /// </remarks>
    public string IdentifierName(string source)
    {
        if (Kind == TokenKind.Identifier)
            return source.Substring(Start, Length);

        if (Kind != TokenKind.QuotedIdentifier)
            throw new InvalidOperationException($"token is {Kind}, not an identifier");

        // Strip the surrounding backticks, then collapse `` to a single backtick.
        var inner = source.Substring(Start + 1, Length - 2);
        return inner.IndexOf('`') >= 0 ? inner.Replace("``", "`") : inner;
    }

    public override string ToString() => Length == 0 ? Kind.ToString() : $"{Kind}@{Start}+{Length}";
}
