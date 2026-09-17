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
    /// <para>
    /// <paramref name="negative"/> is a minus sign the parser folded into the literal, and it is
    /// applied BEFORE the ladder, because Spark types the signed text: <c>-2147483648</c> is an
    /// <c>int</c> and <c>-9223372036854775808</c> a <c>bigint</c>, though neither magnitude fits
    /// the type its negation does. #303. A floating-point literal is the exception: it is negated
    /// after parsing, so the range check keeps reading an unsigned mantissa, and a zero keeps
    /// its sign -- <c>-1e-400</c> is <c>-0.0</c> to Spark. #282.
    /// </para>
    /// </remarks>
    public static LiteralValue Number(string text, bool negative, string sql, int position)
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

        var signed = negative ? "-" + digits : digits;

        switch (suffix)
        {
            case "L":
                return LiteralValue.Of(ParseLong(signed, sql, position));
            case "F":
                var asFloat = ParseFloat(digits, sql, position);
                return LiteralValue.Of(negative ? -asFloat : asFloat);
            case "D":
                return LiteralValue.Of(Double(digits, negative, sql, position));
            case "BD":
                return Decimal(signed, sql, position);

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
            return LiteralValue.Of(Double(digits, negative, sql, position));

        if (digits.IndexOf('.') >= 0)
            return Decimal(signed, sql, position);

        if (int.TryParse(signed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var asInt))
            return LiteralValue.Of(asInt);

        if (long.TryParse(signed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var asLong))
            return LiteralValue.Of(asLong);

        // Spark's ladder does not stop at bigint: an integral literal too wide for one becomes a
        // DECIMAL, which is why a 38-digit literal is a decimal(38,0) rather than an error. #173.
        return Decimal(signed, sql, position);
    }

    private static double Double(string digits, bool negative, string sql, int position)
    {
        var value = ParseDouble(digits, sql, position);
        return negative ? -value : value;
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

        // Precision counts the SCALE as well as the digits, because a Spark decimal requires
        // 0 <= scale <= precision <= 38: a value with a single significant digit is still too wide
        // if its scale is. Measured — `1e-38BD` is a decimal(38,38) and `1e-39BD` is refused,
        // by Spark's PARSER rather than at analysis, which is where this refuses too. Checking
        // only the digit count let `1e-45BD` through as a scale-45 decimal no Spark type holds.
        var precision = Math.Max(Precision(unscaled), scale);

        if (precision > MaxPrecision)
            throw new SparkSqlParseException(
                $"'{text}' needs a precision of {precision}, and no decimal is wider than {MaxPrecision}",
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
                throw ExponentOverflow(text, sql, position);
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
        // them — `.5` and `1.` are both literals Spark accepts — behind the sign the parser may
        // have folded in, which the parse reads.
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

    /// <summary>
    /// Reads a double LITERAL, which is the same question the cast asks of a column's text.
    /// </summary>
    /// <remarks>
    /// Through <see cref="SparkDoubleText"/> rather than <c>double.TryParse</c>, because .NET
    /// Framework's parser is not correctly rounded and reads about 1% of ordinary fifteen- and
    /// sixteen-digit numbers as the double next door. A literal is not exempt from that:
    /// <c>SELECT 49.0793458194787E0</c> is a DOUBLE in Spark, and it materialized different bits
    /// per runtime exactly as <c>CAST('49.0793458194787' AS DOUBLE)</c> did. #350.
    /// <para>
    /// The overflow half of that type does not reach here — <see cref="RefuseOutOfRange"/> has
    /// already refused a literal past a double's range on every framework, which is #287, so a
    /// parse that fails at this point failed for some other reason and is still an error.
    /// </para>
    /// </remarks>
    private static double ParseDouble(string text, string sql, int position)
    {
        RefuseOutOfRange(text, MaxDoubleDigits, MaxDoubleExponent, "a double", sql, position);

        return SparkDoubleText.TryParse(text, out var value)
            ? value
            : throw Overflow(text, "a double", sql, position);
    }

    /// <summary>
    /// Reads a float literal, rounded once from its text.
    /// </summary>
    /// <remarks>
    /// Through <see cref="SparkDoubleText.TryParseSingle(string, out float)"/>. A float's SHORTEST
    /// form is nine digits, and .NET Framework reads every such form right -- measured over
    /// 100,000 random floats at six to nine digits -- which is why #350 left this alone. A literal
    /// is not limited to the shortest form, though: beside a rounding tie, net472's parse was
    /// wrong on a third of 8,400 longer spellings where .NET Core's was right on all of them.
    /// #372.
    /// </remarks>
    private static float ParseFloat(string text, string sql, int position)
    {
        RefuseOutOfRange(text, MaxFloatDigits, MaxFloatExponent, "a float", sql, position);

        return SparkDoubleText.TryParseSingle(text, out var value)
            ? value
            : throw Overflow(text, "a float", sql, position);
    }

    // -- THE RANGE OF A FLOATING-POINT LITERAL ----------------------------------------------

    /// <summary>
    /// The largest magnitude a <c>double</c> literal may spell, as <c>0.&lt;digits&gt; x 10^exp</c>.
    /// </summary>
    /// <remarks>
    /// Spark states the bound as <c>1.7976931348623157E+308</c> in its own error text, which is
    /// <see cref="double.MaxValue"/> written in its shortest round-tripping form rather than the
    /// exact 309-digit integer that value really is. Taking the same spelling is what makes the
    /// boundary agree, because the comparison is against the literal EXACTLY: measured,
    /// <c>1.79769313486231575e308</c> is refused although it rounds to
    /// <see cref="double.MaxValue"/>. #287.
    /// </remarks>
    private const string MaxDoubleDigits = "17976931348623157";

    private const int MaxDoubleExponent = 309;

    /// <summary>The same bound for a <c>float</c>, which Spark states as a widened double.</summary>
    /// <remarks>
    /// <c>3.4028234663852886E+38</c>, not the <c>3.4028235E38</c> that Java prints for
    /// <see cref="float.MaxValue"/> -- so <c>3.4028235e38F</c>, the ordinary spelling of the
    /// largest float, is REFUSED while <c>3.4028234663852886e38F</c> is accepted. Measured, and
    /// it is the row that says the bound is not "whatever survives the cast to float".
    /// </remarks>
    private const string MaxFloatDigits = "34028234663852886";

    private const int MaxFloatExponent = 39;

    /// <summary>
    /// Refuses a floating-point literal whose exact value lies outside the type range, the way
    /// Spark PARSER does.
    /// </summary>
    /// <remarks>
    /// <para>
    /// #287. <c>1e400</c> is <c>INVALID_NUMERIC_LITERAL_RANGE</c> in Spark, refused before any
    /// data is touched; we produced an infinity. The check is on the literal exact decimal TEXT
    /// rather than on what the parse produces, which is the half that is easy to miss:
    /// <c>3.4028234663852887e38F</c> and <c>1.79769313486231575e308</c> both round to a finite
    /// value and are both refused.
    /// </para>
    /// <para>
    /// <b>Only the magnitude is bounded, and only from above.</b> The issue reports
    /// <c>1e-400</c> as the same gap at the other end and it is not one: Spark compares against
    /// <c>[-MaxValue, MaxValue]</c>, and a value that underflows sits well inside that. Measured,
    /// <c>1e-400</c> is <c>0.0D</c>, <c>1e-325</c> is <c>0.0D</c>, and <c>-1e-400</c> is
    /// <c>-0.0D</c> -- Spark folds the sign into the literal there, which is why that row belongs
    /// to #282 rather than here.
    /// </para>
    /// <para>
    /// A zero mantissa is in range at every exponent: <c>0e400</c> is <c>0.0D</c>. An exponent
    /// that Java BigDecimal cannot carry as a scale is refused ahead of the comparison, which is
    /// why <c>0e2147483648</c> refuses where <c>0e400</c> does not -- and why the lower bound is
    /// <c>-int.MaxValue</c> rather than <c>int.MinValue</c>, since it is the NEGATION that
    /// overflows there and <c>1e-2147483648</c> is measured refusing.
    /// </para>
    /// </remarks>
    private static void RefuseOutOfRange(
        string text, string boundDigits, int boundExponent, string what, string sql, int position)
    {
        var (digits, exponent) = Normalize(text, what, sql, position);

        if (digits.Length == 0)
            return;

        if (exponent > boundExponent
            || (exponent == boundExponent && IsGreater(digits, boundDigits)))
        {
            throw Overflow(text, what, sql, position);
        }
    }

    /// <summary>
    /// Significant digits and the base-10 exponent of the point: the value is
    /// <c>0.&lt;digits&gt; x 10^exponent</c>, and a zero has no digits at all.
    /// </summary>
    /// <remarks>
    /// The exponent is a <see cref="long"/>, and that is not decoration. <c>1e2147483647</c> is a
    /// legal token whose normalised exponent is 2147483648, so accumulating it in an
    /// <see cref="int"/> would wrap to <see cref="int.MinValue"/> and read the largest literal
    /// expressible as the smallest -- accepting exactly what this exists to refuse.
    /// </remarks>
    private static (string Digits, long Exponent) Normalize(
        string text, string what, string sql, int position)
    {
        long exponent = 0;
        var e = text.IndexOf('E');
        if (e < 0) e = text.IndexOf('e');

        if (e >= 0)
        {
            if (!long.TryParse(
                    text.Substring(e + 1), NumberStyles.Integer, CultureInfo.InvariantCulture,
                    out exponent)
                || exponent > int.MaxValue
                || exponent < -int.MaxValue)
            {
                throw ExponentOverflow(text, sql, position);
            }

            text = text.Substring(0, e);
        }

        var dot = text.IndexOf('.');
        if (dot >= 0)
        {
            exponent -= text.Length - dot - 1;
            text = text.Remove(dot, 1);
        }

        var digits = text.TrimStart('0');
        exponent += digits.Length;

        return (digits.TrimEnd('0'), exponent);
    }

    /// <summary>Whether one normalised digit string is greater than another, padding with zeros.</summary>
    /// <remarks>
    /// Both sides have had their leading and trailing zeros removed, so neither starts with one
    /// and a longer string is the greater once the shared prefix matches.
    /// </remarks>
    private static bool IsGreater(string digits, string bound)
    {
        var length = Math.Max(digits.Length, bound.Length);

        for (var i = 0; i < length; i++)
        {
            var left = i < digits.Length ? digits[i] : '0';
            var right = i < bound.Length ? bound[i] : '0';

            if (left != right)
                return left > right;
        }

        return false;
    }

    private static SparkSqlParseException Overflow(string text, string what, string sql, int position) =>
        new($"'{text}' is out of range for {what}", sql, position);

    /// <summary>
    /// An exponent no decimal scale can carry, which is a different refusal from a value out of
    /// range.
    /// </summary>
    /// <remarks>
    /// Its own reason because the range one would be FALSE here, and visibly so: the literal that
    /// reaches this most clearly is <c>0e2147483648</c>, which is numerically zero and in the
    /// range of every type there is. It is refused for the spelling of its exponent alone. Raised
    /// in review of #287.
    /// <para>
    /// The distinction is the oracle's too. Spark answers an out-of-range literal with
    /// <c>INVALID_NUMERIC_LITERAL_RANGE</c>, naming the min and max it compared against, and
    /// answers these with a plain <c>ParseException</c> out of <c>BigDecimal</c> instead --
    /// measured on 4.0.3 for <c>1e2147483648</c>, <c>1e99999999999</c>, <c>1e-99999999999</c>,
    /// <c>1e-2147483648</c> and <c>0e2147483648</c>. A caller quoting the reason into a refused
    /// write should be able to tell a value that is too big from one nothing can spell.
    /// </para>
    /// <para>
    /// Reached from the decimal path as well, where the wording was wrong in the same way and for
    /// the same inputs -- <c>0e2147483648BD</c> is not out of range for a decimal either.
    /// </para>
    /// </remarks>
    private static SparkSqlParseException ExponentOverflow(string text, string sql, int position) =>
        new($"'{text}' has an exponent no decimal scale can carry", sql, position);
}
