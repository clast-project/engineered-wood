// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
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
                return Decimal(digits, sql, position);

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
            return Decimal(digits, sql, position);

        if (int.TryParse(digits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var asInt))
            return LiteralValue.Of(asInt);

        if (long.TryParse(digits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var asLong))
            return LiteralValue.Of(asLong);

        // Spark's ladder does not stop at bigint: an integral literal too wide for one becomes a
        // DECIMAL, which is why a 38-digit literal is a decimal(38,0) rather than an error. #173.
        return Decimal(digits, sql, position);
    }

    /// <summary>
    /// Unquotes one string literal, resolving Spark's backslash escapes.
    /// </summary>
    /// <remarks>
    /// ONE literal, not a run of them. A doubled quote never appears inside a string token at
    /// all — it closes one literal and opens the next — so joining the run belongs to
    /// <see cref="SparkSqlParser"/>, which joins these results rather than the raw text. See
    /// #179 and the <c>string-literals</c> group of the corpus.
    /// </remarks>
    public static LiteralValue String(string text) => LiteralValue.Of(Unquote(text));

    /// <summary>
    /// The text of one string literal token, without its quotes and with its escapes resolved.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark's table, and almost none of it was guessable. Every rule below is an answer in the
    /// <c>string-literals</c> group of <c>Fixtures/spark-expression-corpus.json</c>, and three
    /// of them contradict the obvious reading:
    /// </para>
    /// <list type="bullet">
    /// <item><description>
    ///   <b><c>\f</c> is not a form feed.</b> It is not in the table at all, so it is the letter
    ///   <c>f</c> and <c>'a\fb'</c> is <c>afb</c>. Meanwhile <c>\Z</c>, which C does not have,
    ///   IS in the table and is U+001A.
    /// </description></item>
    /// <item><description>
    ///   <b>An unrecognised escape DROPS its backslash</b> — <c>'a\qb'</c> is <c>aqb</c> —
    ///   except for <c>\%</c> and <c>\_</c>, which keep it so a backslash stays usable in a LIKE
    ///   pattern. The rule this replaces kept the backslash for every unrecognised escape: right
    ///   for the one case it had been checked against, wrong for the rest.
    /// </description></item>
    /// <item><description>
    ///   <b>The octal escape stops at <c>\177</c>, not <c>\377</c>.</b> <c>'\101'</c> is
    ///   <c>A</c> while <c>'\200'</c> is the text <c>200</c>, so the first digit must be 0 or 1
    ///   and only ASCII is reachable. One octal digit is not an escape either: <c>'\7'</c> is
    ///   <c>7</c>, and <c>'\0'</c> is U+0000 only because the table has a <c>0</c> row.
    /// </description></item>
    /// </list>
    /// <para>
    /// Width decides the rest. <c>\u</c> takes exactly four hex digits and <c>\U</c> exactly
    /// eight, in either case; short of that they are not escapes at all, so <c>'\u12'</c> is
    /// <c>u12</c> and <c>'\u00411'</c> is <c>A1</c>.
    /// </para>
    /// </remarks>
    public static string Unquote(string text)
    {
        // The index of the closing quote, and the exclusive bound for everything below. Working
        // against the ORIGINAL text rather than an unquoted copy is what keeps the escape path to
        // a single allocation — the StringBuilder's result.
        var end = text.Length - 1;

        // Nothing to resolve is the common case: it is every literal in a generated constraint,
        // and there the substring is the answer rather than a working copy.
        if (text.IndexOf('\\', 1, end - 1) < 0)
            return text.Substring(1, end - 1);

        var builder = new StringBuilder(end - 1);

        for (var i = 1; i < end; i++)
        {
            var c = text[i];

            // A trailing backslash cannot reach here from the tokenizer — it would have escaped
            // the closing quote and the scan would have run on to an unterminated literal — but
            // this method is reachable from outside it, so it must not read past the end.
            if (c != '\\' || i + 1 >= end)
            {
                builder.Append(c);
                continue;
            }

            var next = text[i + 1];

            if (next == 'u' && TryHex(text, i + 2, 4, end, out var unit))
            {
                builder.Append((char)unit);
                i += 5;
                continue;
            }

            if (next == 'U' && TryHex(text, i + 2, 8, end, out var point))
            {
                AppendCodePoint(builder, point);
                i += 9;
                continue;
            }

            if ((next == '0' || next == '1')
                && IsOctal(text, i + 2, end) && IsOctal(text, i + 3, end))
            {
                builder.Append((char)(
                    ((next - '0') << 6) | ((text[i + 2] - '0') << 3) | (text[i + 3] - '0')));
                i += 3;
                continue;
            }

            switch (next)
            {
                case '0': builder.Append('\0'); break;
                case 'b': builder.Append('\b'); break;
                case 'n': builder.Append('\n'); break;
                case 'r': builder.Append('\r'); break;
                case 't': builder.Append('\t'); break;
                case 'Z': builder.Append('\u001A'); break;
                case '\'': builder.Append('\''); break;
                case '"': builder.Append('"'); break;
                case '\\': builder.Append('\\'); break;

                // The two LIKE wildcards keep their backslash, and they are the only characters
                // that do — it is what lets '100\%' survive as a pattern.
                case '%': builder.Append("\\%"); break;
                case '_': builder.Append("\\_"); break;

                default: builder.Append(next); break;
            }

            i++;
        }

        return builder.ToString();
    }

    /// <summary>
    /// Appends a <c>\U</c> code point the way Spark's own arithmetic does.
    /// </summary>
    /// <remarks>
    /// Deliberately not <c>char.ConvertFromUtf32</c>, which refuses anything above U+10FFFF or
    /// inside the surrogate range. Spark applies Java's surrogate formulas with no range check at
    /// all, and the two measurements that pin it are the ones a range check would have refused:
    /// <c>'\U00110000'</c> and <c>'\UFFFFFFFF'</c> are both ANSWERS, the second of them
    /// U+D7BF followed by an unpaired low surrogate.
    /// <para>
    /// The BMP test is an UNSIGNED shift, which is what keeps those two apart from
    /// <c>'\U00000041'</c>: eight hex digits overflow a signed 32-bit accumulator, so
    /// <c>\UFFFFFFFF</c> arrives here as -1, and a signed <c>&lt; 0x10000</c> test would take the
    /// BMP branch for it and answer one character where Spark answers two.
    /// </para>
    /// </remarks>
    private static void AppendCodePoint(StringBuilder builder, int point)
    {
        if ((uint)point >> 16 == 0)
        {
            builder.Append((char)point);
            return;
        }

        // Java's Character.highSurrogate/lowSurrogate, including their arithmetic shift: -1 >> 10
        // is -1, which is what puts 0xD7BF at the front of the \UFFFFFFFF answer.
        builder.Append((char)((point >> 10) + 0xD7C0));
        builder.Append((char)((point & 0x3FF) + 0xDC00));
    }

    /// <summary>
    /// Reads exactly <paramref name="count"/> hex digits before <paramref name="end"/>, or
    /// reports that they are not there.
    /// </summary>
    /// <remarks>
    /// <paramref name="end"/> is the literal's closing quote rather than the string's length,
    /// because the caller scans the original quoted text: without it, <c>'\u0041'</c> would be
    /// free to read its own closing quote as a digit position.
    /// <para>
    /// Overflow is deliberate rather than guarded: eight digits do not fit a signed int, and the
    /// wrapped value is exactly what <see cref="AppendCodePoint"/> needs. Digits are either case,
    /// measured — <c>'\u004a'</c> is <c>J</c>.
    /// </para>
    /// </remarks>
    private static bool TryHex(string text, int start, int count, int end, out int value)
    {
        value = 0;

        if (start + count > end)
            return false;

        for (var i = start; i < start + count; i++)
        {
            var digit = HexDigit(text[i]);
            if (digit < 0)
                return false;

            value = unchecked((value << 4) | digit);
        }

        return true;
    }

    private static bool IsOctal(string text, int index, int end) =>
        index < end && text[index] >= '0' && text[index] <= '7';

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

    /// <summary>Decodes the hex digits of an <c>X'…'</c> literal.</summary>
    /// <remarks>
    /// Digits are converted directly rather than through <c>byte.TryParse</c> with
    /// <c>NumberStyles.HexNumber</c>. That style implies <c>AllowLeadingWhite</c> and
    /// <c>AllowTrailingWhite</c>, so it reads the pair <c>"A "</c> as <c>0x0A</c> and would let
    /// <c>X'A BC'</c> decode instead of being refused. It also avoids a two-character substring
    /// per byte, which is what drew attention to the behaviour.
    /// </remarks>
    private static byte[] ParseHex(string text, string sql, int position)
    {
        if (text.Length % 2 != 0)
            throw new SparkSqlParseException(
                "a binary literal needs an even number of hex digits", sql, position);

        var bytes = new byte[text.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
        {
            var high = HexDigit(text[i * 2]);
            var low = HexDigit(text[(i * 2) + 1]);

            if (high < 0 || low < 0)
                throw new SparkSqlParseException(
                    $"'{text}' is not a valid binary literal", sql, position);

            bytes[i] = (byte)((high << 4) | low);
        }

        return bytes;
    }

    /// <summary>The value of one hex digit, or -1 if the character is not one.</summary>
    private static int HexDigit(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _ => -1,
    };

    private static long ParseLong(string text, string sql, int position) =>
        long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw Overflow(text, "an integer", sql, position);

    /// <summary>
    /// Parses a decimal literal exactly, at whatever width Spark allows one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Not <c>decimal.TryParse</c>, and not because it refuses.</b> It refuses a literal too
    /// LARGE for <see cref="decimal"/> — and it silently ROUNDS one that is merely too precise,
    /// reporting success. Measured:
    /// <c>0.12345678901234567890123456789012345678</c> comes back as
    /// <c>0.1234567890123456789012345679</c> and <c>TryParse</c> returns true, so the literal was
    /// quietly wrong rather than refused. Keying the fallback off a failed parse would have left
    /// that half of #173 in place.
    /// </para>
    /// <para>
    /// So the digits decide. The text is split into an unscaled integer and a scale — which is
    /// what the rest of the pipeline speaks anyway, since #131 put arithmetic, casts, unification
    /// and equality on exactly that pair — and <see cref="decimal"/> is used only where it is
    /// EXACT: a scale it can carry, and an unscaled value inside its 96 bits.
    /// </para>
    /// </remarks>
    private static LiteralValue Decimal(string text, string sql, int position)
    {
        var (unscaled, scale) = SplitDecimal(text, sql, position);

        if (Precision(unscaled) > MaxPrecision)
            throw new SparkSqlParseException(
                $"'{text}' has more than {MaxPrecision} digits, which is wider than any decimal",
                sql, position);

        // System.Decimal holds a scale up to 28 and an unscaled value inside 96 bits. Inside that
        // it is exact and is the kind the rest of the library expects for an ordinary literal;
        // outside it, the high-precision kind carries the same pair losslessly.
        return scale <= 28 && BigInteger.Abs(unscaled) <= MaxDecimalUnscaled
            ? LiteralValue.Of(ToDecimal(unscaled, scale))
            : LiteralValue.HighPrecisionDecimalOf(unscaled, scale);
    }

    /// <summary>Spark's widest decimal, and so the widest literal one can be written as.</summary>
    private const int MaxPrecision = 38;

    /// <summary>2^96 - 1, the largest unscaled value <see cref="decimal"/> carries.</summary>
    private static readonly BigInteger MaxDecimalUnscaled =
        BigInteger.Parse("79228162514264337593543950335", CultureInfo.InvariantCulture);

    /// <summary>
    /// Splits literal text into the unscaled integer and scale that denote it exactly.
    /// </summary>
    /// <remarks>
    /// An exponent is handled because it can reach here: the <c>BD</c> suffix is stripped before
    /// the exponent check above, so <c>1e3BD</c> arrives as <c>1e3</c> asking to be a decimal.
    /// A negative resulting scale is folded into the integer rather than kept, because a decimal
    /// has no negative scale — <c>1e3BD</c> is 1000 at scale 0.
    /// </remarks>
    private static (BigInteger Unscaled, int Scale) SplitDecimal(string text, string sql, int position)
    {
        var exponent = 0;
        var e = text.IndexOf('E');
        if (e < 0) e = text.IndexOf('e');

        if (e >= 0)
        {
            if (!int.TryParse(
                    text.Substring(e + 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out exponent))
            {
                throw Overflow(text, "a decimal", sql, position);
            }

            text = text.Substring(0, e);
        }

        var dot = text.IndexOf('.');
        if (dot >= 0)
        {
            exponent -= text.Length - dot - 1;
            text = text.Remove(dot, 1);
        }

        // The tokenizer has already established that what is left is digits, possibly none of
        // them — `.5` and `1.` are both literals Spark accepts.
        var unscaled = text.Length == 0
            ? BigInteger.Zero
            : BigInteger.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture);

        if (exponent >= 0)
            return (unscaled * BigInteger.Pow(Ten, exponent), 0);

        return (unscaled, -exponent);
    }

    private static readonly BigInteger Ten = new(10);

    /// <summary>The number of digits in an unscaled value, which is a decimal's precision.</summary>
    private static int Precision(BigInteger unscaled)
    {
        var digits = 0;
        var magnitude = BigInteger.Abs(unscaled);

        do
        {
            digits++;
            magnitude /= Ten;
        }
        while (!magnitude.IsZero);

        return digits;
    }

    /// <summary>
    /// Builds a <see cref="decimal"/> from a pair the caller has already checked it can hold.
    /// </summary>
    private static decimal ToDecimal(BigInteger unscaled, int scale)
    {
        var magnitude = BigInteger.Abs(unscaled);
        var low = (int)(uint)(magnitude & uint.MaxValue);
        var mid = (int)(uint)((magnitude >> 32) & uint.MaxValue);
        var high = (int)(uint)((magnitude >> 64) & uint.MaxValue);

        return new decimal(low, mid, high, unscaled.Sign < 0, (byte)scale);
    }

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
