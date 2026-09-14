// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// A <c>date_format</c> pattern, compiled from Java's pattern language and rendered here.
/// </summary>
/// <remarks>
/// <b>The pattern is NOT handed to .NET's formatter.</b> It used to be, on the grounds that the
/// two languages agree for the fields Delta expressions use — <c>y M d H m s</c> — and they do
/// agree for a run of two or more of those letters and for the punctuation between them. They
/// agree for nothing else, and what a pass-through did with the rest was not a near miss but a
/// different string, because .NET's custom-format language has three constructs Java's does not
/// and one rule that changes the meaning of the whole pattern. Measured on Spark 4.0.3 / JDK 17
/// with <c>spark.sql.session.timeZone=UTC</c>, against <c>2026-08-11 12:30:45</c>:
/// <list type="bullet">
/// <item><description>
/// <b>An empty pattern.</b> Spark formats no fields and answers <c>''</c>; .NET reads an empty
/// format string as a request for the GENERAL format and answers
/// <c>'08/11/2026 12:30:00 +00:00'</c> — a whole timestamp where Spark produced nothing. #284.
/// </description></item>
/// <item><description>
/// <b>A pattern of exactly one character.</b> .NET reads a one-character format string as a
/// STANDARD specifier rather than a custom one, so the single most natural way to ask for a field
/// is the one spelling that cannot mean the field: <c>'d'</c> is Spark's day-of-month <c>11</c>
/// and .NET's short date <c>08/11/2026</c>, <c>'s'</c> is Spark's second <c>45</c> and .NET's
/// sortable <c>2026-08-11T12:30:45</c>, <c>'M'</c> is Spark's <c>8</c> and .NET's
/// <c>August 11</c>, and <c>'H'</c> is Spark's <c>12</c> and no .NET standard specifier at all —
/// it threw <see cref="FormatException"/> out of the evaluator.
/// </description></item>
/// <item><description>
/// <b>A single <c>y</c>.</b> Java's count-1 year is the year in as many digits as it needs, so
/// <c>'y'</c> is <c>2026</c> and year 999 is <c>999</c>. .NET's <c>y</c> is the last TWO digits.
/// There is no .NET spelling that agrees at every year: <c>yyyy</c> matches 2026 and pads year
/// 999 to <c>0999</c>, which is what Java's <c>yyyy</c> does and not what its <c>y</c> does. This
/// is the one divergence a translation could not have closed, and the reason this renders the
/// fields itself rather than rewriting the pattern into .NET's language.
/// </description></item>
/// <item><description>
/// <b><c>\</c>, <c>%</c> and <c>"</c>.</b> Each is a literal to Java and a construct to .NET —
/// the escape, the single-custom-specifier prefix, and the other literal delimiter. So
/// <c>'\d'</c> is <c>\11</c> to Spark and the letter <c>d</c> to .NET, <c>'%d'</c> is <c>%11</c>
/// to Spark and <c>11</c> to .NET, and <c>'"yy"'</c> is <c>"26"</c> to Spark and <c>yy</c> to
/// .NET. A lone trailing <c>\</c> is a backslash to Spark and a <see cref="FormatException"/> to
/// .NET.
/// </description></item>
/// </list>
/// <para>
/// The refusals are as deliberate as the answers. Spark supports many more pattern letters than
/// this does — <c>D E a h S G q L Z z X</c> all answer — and every one of them means something
/// else in .NET, so an unimplemented letter is refused rather than reinterpreted. Spark ALSO
/// refuses: a run too long for its letter (<c>ddd</c>, <c>HHH</c>, <c>mmm</c>, <c>sss</c> and
/// <c>MMMMM</c> are all <c>SparkUpgradeException</c>, not a wider field), the reserved characters
/// <c>#</c>, <c>{</c> and <c>}</c>, and a pattern ending inside a quoted literal. Refusing those
/// keeps the property that matters: where Spark refuses, so do we, and where Spark answers we
/// answer the same string.
/// </para>
/// </remarks>
internal sealed class SparkDatePattern
{
    /// <summary>
    /// Spark formats with <c>Locale.US</c>, so the month names are English on every machine.
    /// </summary>
    /// <remarks>
    /// Copied out once rather than read per row: <see cref="DateTimeFormatInfo.MonthNames"/> and
    /// its abbreviated twin hand back a fresh array on every get, so reading them inside the loop
    /// would allocate a thirteen-element array per formatted value.
    /// </remarks>
    private static readonly string[] MonthNames =
        CultureInfo.InvariantCulture.DateTimeFormat.MonthNames;

    private static readonly string[] AbbreviatedMonthNames =
        CultureInfo.InvariantCulture.DateTimeFormat.AbbreviatedMonthNames;

    private readonly Token[] _tokens;

    private SparkDatePattern(Token[] tokens) => _tokens = tokens;

    /// <summary>
    /// Parses <paramref name="pattern"/>, refusing anything not measured to agree with Spark.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The pattern uses a field this does not implement, or one Spark itself rejects.
    /// </exception>
    public static SparkDatePattern Compile(string pattern)
    {
        var tokens = new List<Token>();
        var literal = new StringBuilder();
        var index = 0;

        while (index < pattern.Length)
        {
            var c = pattern[index];

            if (c == '\'')
            {
                AppendQuoted(pattern, ref index, literal);
                continue;
            }

            if (!IsPatternLetter(c))
            {
                // Java reserves these for future use and throws on them; Spark passes the throw
                // through. `[` and `]` it does NOT throw on -- they open and close an optional
                // section, and `date_format(ts, '[yyyy]')` answers 2026 -- but an optional section
                // is a parse-side construct with no counterpart here, so it is refused with them.
                if (c is '#' or '{' or '}' or '[' or ']')
                {
                    throw new NotSupportedException(
                        $"date_format pattern character '{c}' is not supported; " +
                        "Java reserves #, { and }, and an optional section [ ] has no meaning " +
                        "when formatting");
                }

                literal.Append(c);
                index++;
                continue;
            }

            Flush(tokens, literal);

            var count = 1;
            while (index + count < pattern.Length && pattern[index + count] == c)
                count++;

            tokens.Add(Field(c, count));
            index += count;
        }

        Flush(tokens, literal);
        return new SparkDatePattern(tokens.ToArray());
    }

    /// <summary>Renders <paramref name="value"/>, already converted to the session zone.</summary>
    public string Format(DateTimeOffset value)
    {
        var text = new StringBuilder();

        foreach (var token in _tokens)
        {
            if (token.Field == '\0')
            {
                text.Append(token.Text);
                continue;
            }

            AppendField(text, token.Field, token.Count, value);
        }

        return text.ToString();
    }

    private static bool IsPatternLetter(char c) => c is >= 'a' and <= 'z' or >= 'A' and <= 'Z';

    private static void Flush(List<Token> tokens, StringBuilder literal)
    {
        if (literal.Length == 0)
            return;

        tokens.Add(new Token('\0', 0, literal.ToString()));
        literal.Clear();
    }

    /// <summary>
    /// Consumes a <c>'…'</c> section, which is Java's only way to write a literal.
    /// </summary>
    /// <remarks>
    /// Java's own parse, reproduced rather than approximated, because its two special cases are
    /// both reachable from SQL and neither is what a reader would guess. An EMPTY section is not
    /// an empty literal but a literal apostrophe — measured, <c>date_format(ts, '\'\'')</c> is
    /// <c>'</c> — and a doubled quote INSIDE a section is one apostrophe rather than the end of
    /// the section followed by the start of another.
    /// </remarks>
    private static void AppendQuoted(string pattern, ref int index, StringBuilder literal)
    {
        var start = index;
        index++;

        for (; index < pattern.Length; index++)
        {
            if (pattern[index] != '\'')
                continue;

            if (index + 1 < pattern.Length && pattern[index + 1] == '\'')
                index++;   // an escaped apostrophe, not the end of the section
            else
                break;
        }

        if (index >= pattern.Length)
        {
            throw new NotSupportedException(
                $"date_format pattern ends with an incomplete string literal: {pattern}");
        }

        var text = pattern.Substring(start + 1, index - start - 1);
        literal.Append(text.Length == 0 ? "'" : text.Replace("''", "'"));
        index++;
    }

    private static Token Field(char letter, int count)
    {
        // The maximum run per letter is Spark's, not Java's. Java widens a field to the count it
        // is given, so `ddd` would be a three-digit day -- but Spark refuses every one of these
        // with SparkUpgradeException.DATETIME_PATTERN_RECOGNITION, because the meaning changed
        // when it moved to java.time in 3.0. Measured on 4.0.3: ddd, dddd, HHH, mmm, sss and
        // MMMMM all refuse, while MMM and MMMM answer `Aug` and `August`.
        var maximum = letter switch
        {
            // 19 is Java's own ceiling on a numeric field -- `appendValue(field, count, 19, …)`,
            // past which it throws "Too many pattern letters" -- rather than a measured Spark
            // answer. It is here so a pattern of a thousand `y` refuses instead of rendering a
            // thousand characters; no year needs more than four.
            'y' => 19,
            'M' => 4,
            'd' or 'H' or 'm' or 's' => 2,
            _ => throw new NotSupportedException(
                $"date_format pattern letter '{letter}' is not supported; " +
                "only y, M, d, H, m and s are implemented"),
        };

        if (count > maximum)
        {
            throw new NotSupportedException(
                $"date_format pattern '{new string(letter, count)}' is not supported; " +
                $"Spark refuses a run of more than {maximum} '{letter}'");
        }

        return new Token(letter, count, string.Empty);
    }

    private static void AppendField(StringBuilder text, char field, int count, DateTimeOffset value)
    {
        switch (field)
        {
            case 'y':
                AppendYear(text, value.Year, count);
                return;
            case 'M':
                AppendMonth(text, value.Month, count);
                return;
            case 'd':
                AppendPadded(text, value.Day, count);
                return;
            case 'H':
                AppendPadded(text, value.Hour, count);
                return;
            case 'm':
                AppendPadded(text, value.Minute, count);
                return;
            default:
                AppendPadded(text, value.Second, count);
                return;
        }
    }

    /// <summary>
    /// Java's three year rules, which do not reduce to "pad to the count".
    /// </summary>
    /// <remarks>
    /// Count 1 is the year in its own width, count 2 is the last two digits, and count 3 or more
    /// is zero-padded to the count. Measured across the boundary that tells them apart, year 999:
    /// <c>y</c> is <c>999</c>, <c>yy</c> is <c>99</c>, <c>yyy</c> is <c>999</c>, <c>yyyy</c> is
    /// <c>0999</c> and <c>yyyyy</c> is <c>00999</c>.
    /// <para>
    /// Java prefixes a <c>+</c> when the year needs more digits than a count of four or more asks
    /// for. Unreachable here: <see cref="DateTimeOffset"/> stops at year 9999, so the year never
    /// exceeds a four-digit field.
    /// </para>
    /// </remarks>
    private static void AppendYear(StringBuilder text, int year, int count)
    {
        if (count == 2)
        {
            AppendPadded(text, year % 100, 2);
            return;
        }

        AppendPadded(text, year, count);
    }

    private static void AppendMonth(StringBuilder text, int month, int count)
    {
        switch (count)
        {
            case 3:
                text.Append(AbbreviatedMonthNames[month - 1]);
                return;
            case 4:
                text.Append(MonthNames[month - 1]);
                return;
            default:
                AppendPadded(text, month, count);
                return;
        }
    }

    private static void AppendPadded(StringBuilder text, int value, int width)
    {
        var digits = value.ToString(CultureInfo.InvariantCulture);
        for (var i = digits.Length; i < width; i++)
            text.Append('0');

        text.Append(digits);
    }

    private readonly struct Token
    {
        public Token(char field, int count, string text)
        {
            Field = field;
            Count = count;
            Text = text;
        }

        /// <summary>The pattern letter, or <c>'\0'</c> for a literal run.</summary>
        public char Field { get; }

        public int Count { get; }

        public string Text { get; }
    }
}
