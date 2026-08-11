// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text;

namespace EngineeredWood.Expressions.Sql;

/// <summary>
/// Turns literal token text into a typed <see cref="LiteralValue"/>.
/// </summary>
/// <remarks>
/// This is the lowering the tokenizer deliberately does not do. It needs Spark's typing rules,
/// and the rules are not guessable — every one below was measured into
/// <c>Fixtures/spark-expression-corpus.json</c> rather than assumed.
/// </remarks>
internal static class SparkLiteral
{
    /// <summary>
    /// Types a numeric literal the way Spark does.
    /// </summary>
    /// <remarks>
    /// The rule that surprises: a fractional literal is a DECIMAL, not a double. Spark types
    /// <c>1.5</c> as <c>decimal(2,1)</c>, <c>.5</c> as <c>decimal(1,1)</c> and <c>1.</c> as
    /// <c>decimal(1,0)</c> — only an exponent makes it a double, so <c>1e3</c> is
    /// <c>double</c>. Integers take the narrowest of int then bigint, so <c>1</c> is an
    /// <c>int</c> while <c>1000000000000</c> is a <c>bigint</c>.
    /// </remarks>
    public static LiteralValue Number(string text, string sql, int position)
    {
        var digits = text;
        var suffix = string.Empty;

        if (digits.Length >= 2 && digits.EndsWith("BD", StringComparison.OrdinalIgnoreCase))
        {
            suffix = "BD";
            digits = digits.Substring(0, digits.Length - 2);
        }
        else if (digits.Length >= 2 && IsSuffixLetter(digits[digits.Length - 1]))
        {
            suffix = digits.Substring(digits.Length - 1).ToUpperInvariant();
            digits = digits.Substring(0, digits.Length - 1);
        }

        switch (suffix)
        {
            case "L":
                return LiteralValue.Of(ParseLong(digits, sql, position));
            case "F":
                return LiteralValue.Of(ParseFloat(digits, sql, position));
            case "D":
                return LiteralValue.Of(ParseDouble(digits, sql, position));
            case "BD":
                return LiteralValue.Of(ParseDecimal(digits, sql, position));

            // LiteralValue has no 8- or 16-bit integer kind, and silently widening to int would
            // change how the value coerces and overflows. Refusing is the honest answer.
            case "Y":
            case "S":
                throw new SparkSqlParseException(
                    $"the '{suffix}' literal suffix has no representation in this expression tree",
                    sql, position);
        }

        var hasExponent = digits.IndexOf('e') >= 0 || digits.IndexOf('E') >= 0;
        if (hasExponent)
            return LiteralValue.Of(ParseDouble(digits, sql, position));

        if (digits.IndexOf('.') >= 0)
            return LiteralValue.Of(ParseDecimal(digits, sql, position));

        if (int.TryParse(digits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var asInt))
            return LiteralValue.Of(asInt);

        return LiteralValue.Of(ParseLong(digits, sql, position));
    }

    /// <summary>
    /// Unquotes a string literal, resolving the doubled delimiter and backslash escapes.
    /// </summary>
    /// <remarks>
    /// An unrecognised escape keeps its backslash — <c>'100\%'</c> stays <c>100\%</c> — matching
    /// Spark, where that behaviour is what makes backslashes usable in LIKE patterns.
    /// </remarks>
    public static LiteralValue String(string text)
    {
        var quote = text[0];
        var inner = text.Substring(1, text.Length - 2);
        var builder = new StringBuilder(inner.Length);

        for (var i = 0; i < inner.Length; i++)
        {
            var c = inner[i];

            if (c == quote && i + 1 < inner.Length && inner[i + 1] == quote)
            {
                builder.Append(quote);
                i++;
                continue;
            }

            if (c == '\\' && i + 1 < inner.Length)
            {
                var escaped = inner[++i];
                switch (escaped)
                {
                    case 'n': builder.Append('\n'); break;
                    case 't': builder.Append('\t'); break;
                    case 'r': builder.Append('\r'); break;
                    case 'b': builder.Append('\b'); break;
                    case '0': builder.Append('\0'); break;
                    case '\\': builder.Append('\\'); break;
                    case '\'': builder.Append('\''); break;
                    case '"': builder.Append('"'); break;
                    default:
                        builder.Append('\\').Append(escaped);
                        break;
                }

                continue;
            }

            builder.Append(c);
        }

        return LiteralValue.Of(builder.ToString());
    }

    /// <summary>
    /// Builds a typed literal — <c>DATE '…'</c>, <c>TIMESTAMP '…'</c>, or <c>X'…'</c>.
    /// </summary>
    /// <remarks>
    /// Both date and timestamp become a <see cref="DateTimeOffset"/>, a date at UTC midnight.
    /// That is how this library already surfaces date columns, so a literal and a column value
    /// compare on the same footing, and unlike <c>DateOnly</c> it exists on every target
    /// framework — a literal must not change type between net472 and net10.0.
    ///
    /// Reading a timestamp without an offset as UTC is the same policy choice recorded for the
    /// function registry: Spark resolves it against the session timezone, EngineeredWood has no
    /// session, and UTC is what the pinned configuration uses.
    /// </remarks>
    public static LiteralValue Typed(string keyword, string quoted, string sql, int position)
    {
        var text = String(quoted).AsString;

        if (keyword.Equals("X", StringComparison.OrdinalIgnoreCase))
            return LiteralValue.Of(ParseHex(text, sql, position));

        var styles = DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;
        if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, styles, out var instant))
            return LiteralValue.Of(instant);

        throw new SparkSqlParseException(
            $"'{text}' is not a valid {keyword.ToUpperInvariant()} literal", sql, position);
    }

    private static bool IsSuffixLetter(char c) =>
        c is 'y' or 'Y' or 's' or 'S' or 'l' or 'L' or 'f' or 'F' or 'd' or 'D';

    private static byte[] ParseHex(string text, string sql, int position)
    {
        if (text.Length % 2 != 0)
            throw new SparkSqlParseException(
                "a binary literal needs an even number of hex digits", sql, position);

        var bytes = new byte[text.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
        {
            if (!byte.TryParse(text.Substring(i * 2, 2), NumberStyles.HexNumber,
                    CultureInfo.InvariantCulture, out bytes[i]))
            {
                throw new SparkSqlParseException(
                    $"'{text}' is not a valid binary literal", sql, position);
            }
        }

        return bytes;
    }

    private static long ParseLong(string text, string sql, int position) =>
        long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw Overflow(text, "an integer", sql, position);

    private static decimal ParseDecimal(string text, string sql, int position) =>
        decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw Overflow(text, "a decimal", sql, position);

    private static double ParseDouble(string text, string sql, int position) =>
        double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw Overflow(text, "a double", sql, position);

    private static float ParseFloat(string text, string sql, int position) =>
        float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw Overflow(text, "a float", sql, position);

    private static SparkSqlParseException Overflow(string text, string what, string sql, int position) =>
        new($"'{text}' is out of range for {what}", sql, position);
}
