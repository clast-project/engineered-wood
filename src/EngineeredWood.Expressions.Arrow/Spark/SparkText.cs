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
    /// <b>The string casts that still call <see cref="string.Trim()"/> have the gap this exists
    /// to close</b>, and each needs its own measured rows before it moves: #316.
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
}
