// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// Differential tests pinning <see cref="StringOrdering"/> against the order it claims to
/// implement: a byte-wise comparison of the UTF-8 encoding. The fold in StringOrdering is a
/// constant-shift trick, so rather than re-deriving it these tests check it against an
/// independent, obviously-correct implementation over a wide corpus of code points.
/// </summary>
public class StringOrderingTests
{
    /// <summary>The order every format actually specifies: unsigned byte-wise over UTF-8.</summary>
    private static int Utf8Compare(string a, string b)
    {
        byte[] x = Encoding.UTF8.GetBytes(a), y = Encoding.UTF8.GetBytes(b);
        int n = Math.Min(x.Length, y.Length);
        for (int i = 0; i < n; i++)
            if (x[i] != y[i]) return x[i].CompareTo(y[i]);
        return x.Length.CompareTo(y.Length);
    }

    /// <summary>
    /// Code points chosen to straddle every boundary the fold cares about: the end of the
    /// low BMP, both edges of the surrogate block, the top of the BMP, and the start/end of
    /// the supplementary range.
    /// </summary>
    private static readonly int[] Probes =
    [
        0x0000, 0x0041, 0x007F, 0x0080, 0x07FF, 0x0800, 0x1000,
        0xD7FE, 0xD7FF,           // last code points below the surrogate block
        0xE000, 0xE001,           // first code points above it
        0xFFFC, 0xFFFD, 0xFFFE, 0xFFFF,       // top of the BMP (U+FFFD is the substitution char)
        0x10000, 0x10001, 0x103FF,            // first supplementary code points
        0x1F600, 0x2FFFF, 0x100000, 0x10FFFF, // emoji, plane boundaries, last code point
    ];

    /// <summary>
    /// Every BMP code point (surrogates excluded — they are not independently encodable)
    /// compared against each probe. This is the sweep that catches the U+E000..U+FFFF vs
    /// supplementary inversion that plain CompareOrdinal gets wrong.
    /// </summary>
    [Fact]
    public void AgreesWithUtf8ByteOrder_AcrossEntireBmp()
    {
        var mismatches = new List<string>();

        for (int cp = 0; cp <= 0xFFFF; cp++)
        {
            if (cp is >= 0xD800 and <= 0xDFFF)
                continue;
            string s = char.ConvertFromUtf32(cp);

            foreach (int probeCp in Probes)
            {
                string p = char.ConvertFromUtf32(probeCp);
                int actual = Math.Sign(StringOrdering.Compare(s, p));
                int expected = Math.Sign(Utf8Compare(s, p));
                if (actual != expected && mismatches.Count < 20)
                    mismatches.Add($"U+{cp:X4} vs U+{probeCp:X4}: got {actual}, expected {expected}");
            }
        }

        Assert.Empty(mismatches);
    }

    /// <summary>
    /// The supplementary range, sampled systematically. Both operands are surrogate pairs here,
    /// so this exercises the case where the first DIFFERING code unit is a low surrogate.
    /// </summary>
    [Fact]
    public void AgreesWithUtf8ByteOrder_AcrossSupplementaryPlanes()
    {
        var mismatches = new List<string>();

        for (int cp = 0x10000; cp <= 0x10FFFF; cp += 0x37)
        {
            string s = char.ConvertFromUtf32(cp);

            foreach (int probeCp in Probes)
            {
                string p = char.ConvertFromUtf32(probeCp);
                int actual = Math.Sign(StringOrdering.Compare(s, p));
                int expected = Math.Sign(Utf8Compare(s, p));
                if (actual != expected && mismatches.Count < 20)
                    mismatches.Add($"U+{cp:X5} vs U+{probeCp:X5}: got {actual}, expected {expected}");
            }
        }

        Assert.Empty(mismatches);
    }

    /// <summary>
    /// Sorting a mixed corpus must produce byte-identical results either way — a stronger
    /// statement than pairwise agreement, since it also pins transitivity.
    /// </summary>
    [Fact]
    public void SortOrder_MatchesUtf8ByteOrder()
    {
        var corpus = new List<string>();
        foreach (int cp in Probes)
        {
            string c = char.ConvertFromUtf32(cp);
            corpus.Add(c);
            corpus.Add("prefix" + c);
            corpus.Add("prefix" + c + "suffix");
            corpus.Add(c + c);
        }

        var byFold = corpus.OrderBy(s => s, Comparer<string>.Create(StringOrdering.Compare)).ToList();
        var byUtf8 = corpus.OrderBy(s => s, Comparer<string>.Create(Utf8Compare)).ToList();

        Assert.Equal(byUtf8, byFold);
    }

    /// <summary>
    /// The concrete inversions that motivated this type. Each of these is a case where
    /// string.CompareOrdinal returns the WRONG sign for stats comparison.
    /// </summary>
    [Theory]
    [InlineData(0xFFFD, 0x1F600)] // replacement character vs emoji
    [InlineData(0xE000, 0x1F600)] // first private-use char vs emoji
    [InlineData(0xFFFF, 0x10000)] // last BMP code point vs first supplementary
    [InlineData(0xE000, 0x10FFFF)]
    public void SupplementaryCharacters_SortAboveHighBmp(int lowCp, int highCp)
    {
        string low = char.ConvertFromUtf32(lowCp);
        string high = char.ConvertFromUtf32(highCp);

        Assert.True(StringOrdering.Compare(low, high) < 0);
        Assert.True(Utf8Compare(low, high) < 0);
        // Guard the premise: plain ordinal comparison disagrees, which is the whole point.
        Assert.True(string.CompareOrdinal(low, high) > 0);
    }

    [Fact]
    public void SupplementaryCharacters_OrderAmongThemselves()
    {
        // U+10000 and U+103FF share a high surrogate (D800) and differ only in the low half.
        string a = char.ConvertFromUtf32(0x10000);
        string b = char.ConvertFromUtf32(0x103FF);
        Assert.Equal(a[0], b[0]);

        Assert.True(StringOrdering.Compare(a, b) < 0);
        Assert.True(Utf8Compare(a, b) < 0);
    }

    [Fact]
    public void ShorterPrefix_SortsFirst()
    {
        Assert.True(StringOrdering.Compare("ab", "abc") < 0);
        Assert.True(StringOrdering.Compare("abc", "ab") > 0);
        Assert.Equal(0, StringOrdering.Compare("abc", "abc"));
    }

    [Fact]
    public void Nulls_SortBeforeAnyString()
    {
        Assert.Equal(0, StringOrdering.Compare(null, null));
        Assert.True(StringOrdering.Compare(null, "") < 0);
        Assert.True(StringOrdering.Compare("", null) > 0);
    }

    /// <summary>Confirms the comparator is actually reached through the public LiteralValue API.</summary>
    [Fact]
    public void LiteralValue_UsesCodePointOrder()
    {
        LiteralValue replacement = LiteralValue.Of(char.ConvertFromUtf32(0xFFFD));
        LiteralValue emoji = LiteralValue.Of(char.ConvertFromUtf32(0x1F600));

        Assert.True(replacement.CompareTo(emoji) < 0);
        Assert.True(replacement < emoji);
    }
}
