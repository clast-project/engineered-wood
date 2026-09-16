// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Text rules the Spark kernels share, where .NET's own would answer differently.
/// </summary>
internal static class SparkText
{
    /// <summary>
    /// The bounds of <paramref name="text"/> with the whitespace SPARK trims removed from both
    /// ends, which is not the set <see cref="string.Trim()"/> removes.
    /// </summary>
    /// <remarks>
    /// Spark trims with <c>UTF8String.trimAll</c>, which removes leading and trailing BYTES of
    /// 0x20 or below and nothing else. <see cref="string.Trim()"/> removes Unicode whitespace.
    /// <b>Neither set contains the other</b>, so a <c>Trim</c> in a cast is wrong in both
    /// directions rather than merely lenient — measured on 4.0.3, in both dialects:
    /// <list type="bullet">
    /// <item><description>
    /// U+001F is not .NET whitespace and Spark trims it, so a string led by one still reads as
    /// its word: <c>CAST(U+001F + 'true' AS BOOLEAN)</c> is true, and the same prefix on
    /// <c>'1'</c> casts to 1.
    /// </description></item>
    /// <item><description>
    /// U+00A0 IS .NET whitespace and Spark does not trim it, so a string led by one is refused:
    /// <c>CAST(U+00A0 + 'true' AS BOOLEAN)</c> is <c>CAST_INVALID_INPUT</c> under ANSI and
    /// null without it.
    /// </description></item>
    /// </list>
    /// <para>
    /// Testing the CHAR against 0x20 is the same test Spark makes on bytes: every char above
    /// 0x7F encodes as UTF-8 bytes of 0x80 or above, so none of them can look like whitespace to
    /// it, and every char at or below 0x20 encodes as the single byte with its own value.
    /// </para>
    /// <para>
    /// Bounds rather than a trimmed span, because <see cref="SparkDecimalText"/> reads the text
    /// by index and a span would cost it a second pass to recover them. It is the caller that
    /// knows which it wants.
    /// </para>
    /// <para>
    /// <b>Every string CAST reads its text through here</b>, which is #316: the numeric parses
    /// share it through <see cref="SparkArrays.CastInput"/>, and the integral, decimal, temporal
    /// and boolean rules each reach it from there.
    /// <para>
    /// <b>The <c>trim</c>/<c>ltrim</c>/<c>rtrim</c> FUNCTIONS do not, and must not</b> — they
    /// take <see cref="TrimSpaces"/>, which is a third rule again. Three rules for one word, and
    /// the wrong one is wrong in a different direction each time.
    /// </para>
    /// </para>
    /// </remarks>
    public static (int Start, int End) TrimBounds(ReadOnlySpan<char> text)
    {
        var start = 0;
        var end = text.Length;

        while (start < end && text[start] <= ' ') start++;
        while (end > start && text[end - 1] <= ' ') end--;

        return (start, end);
    }

    /// <summary><see cref="TrimBounds"/> applied, for a caller that wants the text.</summary>
    public static ReadOnlySpan<char> Trim(ReadOnlySpan<char> text)
    {
        var (start, end) = TrimBounds(text);
        return text.Slice(start, end - start);
    }

    /// <summary>
    /// Spark's trim FUNCTION rule: the SPACE removed from both ends, and nothing else.
    /// </summary>
    /// <remarks>
    /// <b>A third rule, and the one the shared name makes hardest to guess.</b> Measured on
    /// 4.0.3: <c>trim(' x ')</c> is "x" while <c>trim('\tx\t')</c> comes back unchanged, and so do
    /// the newline and no-break-space forms. So it is neither <see cref="TrimBounds"/>, which
    /// removes every byte at or below 0x20, nor <see cref="string.Trim()"/>, which removes Unicode
    /// whitespace — each of those is wrong here in a different direction. #316.
    /// <para>
    /// Hand-rolled rather than <c>string.Trim(' ')</c> because <b>that overload does not exist on
    /// netstandard2.0</b>: the call binds to <c>Trim(params char[])</c> there and allocates a
    /// one-element array for EVERY ROW, since these run per row through
    /// <c>SparkFunctions.MapString</c>. All three return the original instance when there is
    /// nothing to remove, which <c>Trim</c> also does.
    /// </para>
    /// </remarks>
    public static string TrimSpaces(string text)
    {
        var start = 0;
        var end = text.Length;

        while (start < end && text[start] == ' ') start++;
        while (end > start && text[end - 1] == ' ') end--;

        return start == 0 && end == text.Length ? text : text.Substring(start, end - start);
    }

    /// <summary>The leading half of <see cref="TrimSpaces"/>, for <c>ltrim</c>.</summary>
    /// <inheritdoc cref="TrimSpaces" path="/remarks"/>
    public static string TrimLeadingSpaces(string text)
    {
        var start = 0;
        while (start < text.Length && text[start] == ' ') start++;

        return start == 0 ? text : text.Substring(start);
    }

    /// <summary>The trailing half of <see cref="TrimSpaces"/>, for <c>rtrim</c>.</summary>
    /// <inheritdoc cref="TrimSpaces" path="/remarks"/>
    public static string TrimTrailingSpaces(string text)
    {
        var end = text.Length;
        while (end > 0 && text[end - 1] == ' ') end--;

        return end == text.Length ? text : text.Substring(0, end);
    }

    /// <summary>The same, for a caller that needs a string because .NET's parse takes one.</summary>
    /// <remarks>
    /// Returns <paramref name="text"/> itself when there is nothing to trim, which
    /// <see cref="string.Trim()"/> also does. This runs once per row of every string cast.
    /// </remarks>
    public static string Trim(string text)
    {
        var (start, end) = TrimBounds(text.AsSpan());
        return start == 0 && end == text.Length ? text : text.Substring(start, end - start);
    }

    /// <summary>
    /// Trailing zeros removed, which is what a rendered number does with the digits it does not
    /// count.
    /// </summary>
    /// <remarks>
    /// <b><c>TrimEnd('0')</c> is not the same call on every target.</b> netstandard2.0 has no
    /// single-character overload, so it binds there to <c>TrimEnd(params char[])</c> and allocates
    /// a one-element array on every call — and every caller of this is per-row: the fraction of a
    /// rendered timestamp, and the digits of a rendered float. Scanning by hand costs nothing and
    /// costs the same everywhere, which is the same reason <see cref="TrimTrailingSpaces"/> above
    /// is written out rather than handed to <c>TrimEnd(' ')</c>.
    /// <para>
    /// Matches <c>TrimEnd('0')</c> exactly, down to returning an empty string for digits that are
    /// all zero, and returns <paramref name="text"/> itself when there is nothing to drop — which
    /// is the common case for a float at full width.
    /// </para>
    /// </remarks>
    public static string TrimTrailingZeros(string text)
    {
        var end = text.Length;
        while (end > 0 && text[end - 1] == '0') end--;

        return end == text.Length ? text : text.Substring(0, end);
    }
}
