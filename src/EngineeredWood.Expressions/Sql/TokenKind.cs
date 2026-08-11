// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Sql;

/// <summary>
/// The lexical categories of a Spark SQL expression.
/// </summary>
/// <remarks>
/// There is deliberately no <c>Keyword</c> member. Spark's grammar makes most keywords
/// non-reserved, so <c>value</c>, <c>year</c> and <c>date</c> are all legal column names, and a
/// tokenizer that decided keyword-ness on its own would reject tables it has no business
/// rejecting. Keywords are therefore <see cref="Identifier"/> tokens that the parser matches
/// case-insensitively in the positions where they are meaningful — which also handles Delta
/// storing constraints with their original casing, so <c>and</c> arrives lowercase.
/// </remarks>
internal enum TokenKind
{
    /// <summary>A bare identifier, including anything the parser may treat as a keyword.</summary>
    Identifier,

    /// <summary>A backtick-quoted identifier. Never a keyword, whatever it spells.</summary>
    QuotedIdentifier,

    /// <summary>A numeric literal, with any exponent and type suffix included in its text.</summary>
    Number,

    /// <summary>A single- or double-quoted string literal, quotes included in its text.</summary>
    String,

    /// <summary><c>(</c></summary>
    OpenParen,

    /// <summary><c>)</c></summary>
    CloseParen,

    /// <summary><c>[</c></summary>
    OpenBracket,

    /// <summary><c>]</c></summary>
    CloseBracket,

    /// <summary><c>,</c></summary>
    Comma,

    /// <summary><c>.</c> — field access. A dot introducing a number is part of the number.</summary>
    Dot,

    /// <summary><c>::</c> — Spark's cast shorthand, as in <c>a::bigint</c>.</summary>
    ColonColon,

    /// <summary><c>+</c></summary>
    Plus,

    /// <summary><c>-</c></summary>
    Minus,

    /// <summary><c>*</c></summary>
    Star,

    /// <summary><c>/</c></summary>
    Slash,

    /// <summary><c>%</c></summary>
    Percent,

    /// <summary><c>||</c> — string concatenation.</summary>
    Concat,

    /// <summary><c>=</c> or <c>==</c>.</summary>
    Equal,

    /// <summary><c>&lt;&gt;</c> or <c>!=</c>. Delta stores whichever was written, so both occur.</summary>
    NotEqual,

    /// <summary><c>&lt;=&gt;</c> — null-safe equality, which generated-column validation depends on.</summary>
    NullSafeEqual,

    /// <summary><c>&lt;</c></summary>
    LessThan,

    /// <summary><c>&lt;=</c></summary>
    LessThanOrEqual,

    /// <summary><c>&gt;</c></summary>
    GreaterThan,

    /// <summary><c>&gt;=</c></summary>
    GreaterThanOrEqual,

    /// <summary>Input is exhausted. Always the final token.</summary>
    EndOfInput,
}
