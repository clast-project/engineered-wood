// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Ordinal string comparison in CODE POINT order, which is the same order as a
/// byte-wise comparison of the UTF-8 encoding.
/// </summary>
/// <remarks>
/// Every columnar format this library reads specifies its string min/max sort order over
/// UTF-8 bytes — Parquet (<c>UTF8</c> logical type: unsigned byte-wise), Delta, Iceberg and
/// Vortex all agree. .NET's <see cref="string.CompareOrdinal(string, string)"/> compares
/// UTF-16 code UNITS instead, and the two disagree: surrogates (U+D800..U+DFFF) encode
/// supplementary characters at U+10000 and above, yet as raw code units they sort BELOW
/// U+E000..U+FFFF. U+FFFD, for instance, compares greater than an emoji as code units but
/// less than it over UTF-8 bytes.
///
/// Comparing in code-unit order against bounds written in byte order yields wrong bounds
/// comparisons and therefore wrong data skipping, so every statistics-driven string
/// comparison must go through here.
///
/// Lone (unpaired) surrogates are invalid UTF-16 and have no UTF-8 encoding; a writer
/// transcoding them substitutes U+FFFD, so no total order over UTF-16 can model them
/// faithfully. They are ordered here above U+E000..U+FFFF, matching where their code units
/// sit. Stats writers are responsible for never emitting them — see
/// <c>StatsCollector.TruncateMinString</c>/<c>TruncateMaxString</c>.
/// </remarks>
internal static class StringOrdering
{
    private const int SurrogateStart = 0xD800;   // first high surrogate
    private const int AfterSurrogate = 0xE000;   // first code unit past the surrogate block

    // Shifts applied by Fold. U+E000..U+FFFF move down into the vacated surrogate range,
    // and surrogates move up above them, so numeric order becomes code point order.
    private const int NonSurrogateShift = 0x800;    // E000..FFFF -> D800..F7FF
    private const int SurrogateShift = 0x2000;      // D800..DFFF -> F800..FFFF

    /// <summary>
    /// Compares two strings in code point order. Null sorts before any non-null string,
    /// matching <see cref="string.CompareOrdinal(string, string)"/>.
    /// </summary>
    public static int Compare(string? a, string? b)
    {
        if (ReferenceEquals(a, b)) return 0;
        if (a is null) return -1;
        if (b is null) return 1;

        int min = Math.Min(a.Length, b.Length);
        for (int i = 0; i < min; i++)
        {
            char ca = a[i], cb = b[i];
            if (ca == cb)
                continue;
            // Fast path: below the surrogate block, code-unit order already IS code point order.
            if (ca < SurrogateStart && cb < SurrogateStart)
                return ca < cb ? -1 : 1;
            int fa = Fold(ca), fb = Fold(cb);
            return fa < fb ? -1 : 1;
        }

        return a.Length.CompareTo(b.Length);
    }

    /// <summary>
    /// Maps a UTF-16 code unit into a space whose numeric order equals code point order.
    /// Comparing only the FIRST differing code unit is sufficient: order is preserved within
    /// each block, and two supplementary characters differ at whichever surrogate half differs
    /// first — high halves order by the code point's upper bits, low halves by its lower bits.
    /// </summary>
    private static int Fold(char c) =>
        c >= AfterSurrogate ? c - NonSurrogateShift :
        c >= SurrogateStart ? c + SurrogateShift :
        c;
}
