// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// The five words Spark reads as a date or a timestamp — <c>epoch</c>, <c>today</c>,
/// <c>yesterday</c>, <c>tomorrow</c>, <c>now</c> — over a constant and nowhere else.
/// </summary>
/// <remarks>
/// <para>
/// This is deliberately not part of <see cref="SparkTemporalText"/>'s grammar. Spark reads the
/// words in exactly two places: <c>AstBuilder</c>'s typed literal, and
/// <c>SpecialDatetimeValues</c> — an optimizer rule that rewrites a cast to DATE or TIMESTAMP
/// whose operand is <c>foldable</c> into a literal. <c>stringToDate</c> itself does not know
/// them, so the same word arriving in a row is refused. In both dialects:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <c>CAST('epoch' AS DATE)</c> is 1970-01-01, and <c>CAST(s AS DATE)</c> over a column holding
/// <c>'epoch'</c> is <c>CAST_INVALID_INPUT</c> under ANSI and null without it.
/// </description></item>
/// <item><description>
/// Foldable, not literal: <c>CAST(concat('epo','ch') AS DATE)</c> and
/// <c>CAST(upper('epoch') AS DATE)</c> answer 1970-01-01 — the rule calls <c>e.eval()</c> itself
/// rather than waiting for constant folding — while <c>CAST(concat(s,'') AS DATE)</c> and
/// <c>CAST(CASE WHEN a &gt; 0 THEN 'epoch' ELSE 'epoch' END AS DATE)</c> are refused, though
/// each is <c>'epoch'</c> in every row.
/// </description></item>
/// <item><description>
/// The optimizer rule runs before either cast does, so both dialects fold the word and differ
/// only in how they refuse a word outside the vocabulary. <c>try_cast('epoch' AS DATE)</c> is
/// 1970-01-01, not null.
/// </description></item>
/// </list>
/// <para>
/// The word is not simply matched: see <see cref="ExtractSpecialValue"/>. Four of the five
/// depend on the clock, so a generated column defined as <c>CAST('today' AS DATE)</c> names a
/// different day on every write — in Spark too.
/// </para>
/// </remarks>
internal static class SparkSpecialDatetimeValues
{
    /// <summary>The Unix epoch, which is what the word <c>epoch</c> names in either target.</summary>
    private static DateTimeOffset Epoch => new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// Reads <paramref name="text"/> as one of the special words naming a DATE, as UTC midnight
    /// of the day it names.
    /// </summary>
    /// <remarks>
    /// Spark's <c>convertSpecialDate</c>, where <c>now</c> is today, since a date has no time to
    /// carry the current instant.
    /// </remarks>
    public static bool TryReadDate(ReadOnlySpan<char> text, out DateTimeOffset value)
    {
        switch (ExtractSpecialValue(text))
        {
            case Word.Epoch: value = Epoch; return true;
            case Word.Now or Word.Today: value = Today(); return true;
            case Word.Tomorrow: value = Today().AddDays(1); return true;
            case Word.Yesterday: value = Today().AddDays(-1); return true;
            default: value = default; return false;
        }
    }

    /// <summary>
    /// Reads <paramref name="text"/> as one of the special words naming a TIMESTAMP, as an
    /// instant.
    /// </summary>
    /// <remarks>
    /// Spark's <c>convertSpecialTimestamp</c>. <c>now</c> is the current instant here and today's
    /// date in <see cref="TryReadDate"/> — the one word the two conversions disagree about.
    /// </remarks>
    public static bool TryReadTimestamp(ReadOnlySpan<char> text, out DateTimeOffset value)
    {
        switch (ExtractSpecialValue(text))
        {
            case Word.Epoch: value = Epoch; return true;
            case Word.Now: value = DateTimeOffset.UtcNow; return true;
            case Word.Today: value = Today(); return true;
            case Word.Tomorrow: value = Today().AddDays(1); return true;
            case Word.Yesterday: value = Today().AddDays(-1); return true;
            default: value = default; return false;
        }
    }

    /// <summary>Midnight of the current day in the session zone, as an instant.</summary>
    /// <remarks>
    /// Spark's <c>today(zoneId)</c> is <c>Instant.now().atZone(zoneId).with(MIDNIGHT)</c>, so the
    /// day is the one the session zone is having and the instant is that day's midnight there.
    /// With <see cref="SparkTemporalText.SessionTimeZone"/> fixed at UTC the two spellings
    /// coincide; writing it out keeps this right if #133 makes the zone configurable.
    /// </remarks>
    private static DateTimeOffset Today()
    {
        var zone = SparkTemporalText.SessionTimeZone;
        var midnight = TimeZoneInfo.ConvertTime(DateTimeOffset.UtcNow, zone).DateTime.Date;
        return new DateTimeOffset(midnight, zone.GetUtcOffset(midnight));
    }

    private enum Word
    {
        None = 0,
        Epoch,
        Today,
        Yesterday,
        Tomorrow,
        Now,
    }

    /// <summary>
    /// Spark's <c>extractSpecialValue</c>: the word at the head of the trimmed text, when what
    /// follows it is a timezone the word admits.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The regex is <c>(\p{Alpha}+)\p{Blank}*(.*)</c> against the whole string, guarded by
    /// <c>isValid</c>. Four properties of it do not follow from the description:
    /// </para>
    /// <list type="number">
    /// <item><description>
    /// The alpha run is greedy and the match must cover the string, so <c>'epochUTC'</c> yields
    /// the word <c>epochutc</c> and is refused rather than backtracking to <c>epoch</c>.
    /// </description></item>
    /// <item><description>
    /// <c>\p{Alpha}</c> and <c>\p{Blank}</c> are ASCII in Java without
    /// <c>UNICODE_CHARACTER_CLASS</c> — letters, and space or tab. The separate
    /// <c>input(0).isLetter</c> guard is Unicode-aware but cannot admit anything the regex then
    /// refuses, so the ASCII test below stands for both.
    /// </description></item>
    /// <item><description>
    /// <c>.</c> does not match a line terminator, so a newline anywhere past the word fails the
    /// match even when the zone would resolve. One at either end is already trimmed away.
    /// </description></item>
    /// <item><description>
    /// A tail must resolve as a timezone (<c>'epoch UTC'</c> is 1970-01-01, <c>'epoch extra'</c>
    /// is refused), and <c>now</c> refuses one at all. The zone is never used:
    /// <c>'today America/Los_Angeles'</c> is today in the session zone.
    /// </description></item>
    /// </list>
    /// <para>
    /// One divergence: the tail is checked with <see cref="SparkTemporalText.IsResolvableZone"/>,
    /// which refuses a region id because the tz database is not the same on every target
    /// framework. So <c>CAST('epoch America/Los_Angeles' AS DATE)</c> is refused here and is
    /// 1970-01-01 to Spark.
    /// </para>
    /// </remarks>
    private static Word ExtractSpecialValue(ReadOnlySpan<char> text)
    {
        // Java's `String.trim`, which is `SparkText.TrimBounds`' set exactly.
        var (start, end) = SparkText.TrimBounds(text);

        // `input.length < 3 || !input(0).isLetter`, before the regex is reached at all.
        if (end - start < 3 || !IsAsciiLetter(text[start]))
            return Word.None;

        var wordEnd = start;
        while (wordEnd < end && IsAsciiLetter(text[wordEnd])) wordEnd++;

        var word = Match(text.Slice(start, wordEnd - start));
        if (word == Word.None)
            return Word.None;

        var zoneStart = wordEnd;
        while (zoneStart < end && (text[zoneStart] == ' ' || text[zoneStart] == '\t')) zoneStart++;

        var zone = text.Slice(zoneStart, end - zoneStart);

        // `(.*)` cannot span a line terminator, so the match fails before `isValid` is asked.
        for (var i = 0; i < zone.Length; i++)
        {
            if (IsLineTerminator(zone[i]))
                return Word.None;
        }

        if (zone.Length == 0)
            return word;

        return word != Word.Now && SparkTemporalText.IsResolvableZone(zone) ? word : Word.None;
    }

    /// <summary>The vocabulary, matched case-insensitively — Spark lowercases under Locale.US.</summary>
    private static Word Match(ReadOnlySpan<char> word)
    {
        if (Same(word, "epoch")) return Word.Epoch;
        if (Same(word, "today")) return Word.Today;
        if (Same(word, "yesterday")) return Word.Yesterday;
        if (Same(word, "tomorrow")) return Word.Tomorrow;
        if (Same(word, "now")) return Word.Now;
        return Word.None;
    }

    /// <summary>
    /// ASCII case-insensitive equality, which is what <c>toLowerCase(Locale.US)</c> amounts to
    /// over a run of ASCII letters.
    /// </summary>
    private static bool Same(ReadOnlySpan<char> word, string other)
    {
        if (word.Length != other.Length)
            return false;

        for (var i = 0; i < word.Length; i++)
        {
            var c = word[i];
            if (c >= 'A' && c <= 'Z') c = (char)(c + ('a' - 'A'));
            if (c != other[i])
                return false;
        }

        return true;
    }

    private static bool IsAsciiLetter(char c) => (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');

    /// <summary>The characters Java's <c>.</c> refuses without <c>DOTALL</c>.</summary>
    private static bool IsLineTerminator(char c) =>
        c == '\n' || c == '\r' || c == (char)0x85 || c == (char)0x2028 || c == (char)0x2029;
}
