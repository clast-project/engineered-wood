// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Page-index bound truncation. A truncated minimum must still be at most the value, a truncated
/// maximum at least the value, and a truncated UTF-8 bound must still be valid UTF-8.
/// </summary>
public class PageIndexTruncationTests
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, throwOnInvalidBytes: true);

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    [Theory]
    [InlineData(63)]
    [InlineData(64)]
    public void AtOrUnderTheLimit_IsKept(int length)
    {
        byte[] value = Utf8(new string('x', length));

        Assert.Same(value, PageIndexTruncation.TruncateMin(value, 64, utf8: true));
        Assert.Same(value, PageIndexTruncation.TruncateMax(value, 64, utf8: true));
    }

    [Fact]
    public void OneOverTheLimit_IsShortened()
    {
        byte[] value = Utf8(new string('x', 64) + "y");

        Assert.Equal(Utf8(new string('x', 64)), PageIndexTruncation.TruncateMin(value, 64, utf8: true));
        Assert.Equal(Utf8(new string('x', 63) + "y"), PageIndexTruncation.TruncateMax(value, 64, utf8: true));
    }

    [Fact]
    public void Binary_MaxIncrementsWithCarry()
    {
        byte[] value = [0x10, 0x20, 0xFF, 0xFF, 0x01];

        Assert.Equal(new byte[] { 0x10, 0x20, 0xFF }, PageIndexTruncation.TruncateMin(value, 3, utf8: false));
        // 0x20 0xFF 0xFF cannot take a carry in place: the 0xFFs are dropped and 0x20 becomes 0x21.
        Assert.Equal(new byte[] { 0x10, 0x21 }, PageIndexTruncation.TruncateMax(value, 4, utf8: false));
    }

    [Fact]
    public void Binary_AllFF_KeepsTheFullMax()
    {
        byte[] value = [0xFF, 0xFF, 0xFF, 0x00];

        Assert.Same(value, PageIndexTruncation.TruncateMax(value, 3, utf8: false));
        Assert.Equal(new byte[] { 0xFF, 0xFF, 0xFF }, PageIndexTruncation.TruncateMin(value, 3, utf8: false));
    }

    [Fact]
    public void Utf8_CutFallsInsideACodePoint_MovesBackToItsStart()
    {
        // 63 ASCII bytes, then 'é' (2 bytes) straddling the 64-byte limit.
        string text = new string('a', 63) + "é" + "zzz";
        byte[] value = Utf8(text);

        byte[] min = PageIndexTruncation.TruncateMin(value, 64, utf8: true);
        byte[] max = PageIndexTruncation.TruncateMax(value, 64, utf8: true);

        Assert.Equal(Utf8(new string('a', 63)), min);
        // The last code point that fits is the 63rd 'a'; incremented, it becomes 'b'.
        Assert.Equal(Utf8(new string('a', 62) + "b"), max);
        AssertBounds(value, min, max);
    }

    [Fact]
    public void Utf8_IncrementIsByCodePoint_NotByByte()
    {
        // 'é' is C3 A9; a byte increment would give C3 AA ('ê') too, but 'ÿ' (C3 BF) would wrap into
        // an invalid continuation byte. The code-point increment gives U+0100 (C4 80).
        byte[] value = Utf8("ÿÿÿ");

        byte[] max = PageIndexTruncation.TruncateMax(value, 5, utf8: true);

        Assert.Equal(Utf8("ÿĀ"), max);
        AssertBounds(value, PageIndexTruncation.TruncateMin(value, 5, utf8: true), max);
    }

    [Fact]
    public void Utf8_IncrementThatWouldOutgrowTheLimit_CarriesToThePreviousCodePoint()
    {
        // U+007F is one byte; U+0080 needs two, which does not fit at position 3 of a 4-byte limit.
        byte[] value = Utf8("ab\u007F\u007Fzz");

        byte[] max = PageIndexTruncation.TruncateMax(value, 4, utf8: true);

        Assert.True(max.Length <= 4);
        AssertBounds(value, PageIndexTruncation.TruncateMin(value, 4, utf8: true), max);
    }

    [Fact]
    public void Utf8_SkipsTheSurrogateRange()
    {
        byte[] value = Utf8("퟿퟿x");

        byte[] max = PageIndexTruncation.TruncateMax(value, 6, utf8: true);

        Assert.Equal(Utf8("퟿"), max);
    }

    [Fact]
    public void Utf8_HighestCodePoint_CarriesToThePreviousOne()
    {
        string top = char.ConvertFromUtf32(0x10FFFF);
        byte[] value = Utf8("a" + top + top);

        byte[] max = PageIndexTruncation.TruncateMax(value, 5, utf8: true);

        Assert.Equal(Utf8("b"), max);
    }

    [Fact]
    public void Utf8_NothingToIncrement_KeepsTheFullMax()
    {
        string top = char.ConvertFromUtf32(0x10FFFF);
        byte[] value = Utf8(top + top);

        Assert.Same(value, PageIndexTruncation.TruncateMax(value, 5, utf8: true));
    }

    [Fact]
    public void InvalidUtf8_IsTruncatedAsBinary()
    {
        byte[] value = [(byte)'a', 0xC3, 0xC3, (byte)'b', (byte)'c'];

        Assert.Equal(new byte[] { (byte)'a', 0xC3, 0xC3 }, PageIndexTruncation.TruncateMin(value, 3, utf8: true));
        Assert.Equal(new byte[] { (byte)'a', 0xC3, 0xC4 }, PageIndexTruncation.TruncateMax(value, 3, utf8: true));
    }

    [Fact]
    public void RandomStrings_AlwaysBoundAndStayValid()
    {
        var rng = new Random(64);
        string[] alphabet = ["a", "z", "é", "ÿ", "߿", "ࠀ", "퟿", "", "￿",
            char.ConvertFromUtf32(0x10000), char.ConvertFromUtf32(0x10FFFF), "\u007F"];
        for (int i = 0; i < 2000; i++)
        {
            var sb = new StringBuilder();
            int length = rng.Next(1, 30);
            for (int j = 0; j < length; j++)
                sb.Append(alphabet[rng.Next(alphabet.Length)]);
            byte[] value = Utf8(sb.ToString());
            int limit = rng.Next(1, 20);

            byte[] min = PageIndexTruncation.TruncateMin(value, limit, utf8: true);
            byte[] max = PageIndexTruncation.TruncateMax(value, limit, utf8: true);

            Assert.True(min.Length <= limit);
            Assert.True(max.Length <= limit || ReferenceEquals(max, value));
            AssertBounds(value, min, max);
        }
    }

    private static void AssertBounds(byte[] value, byte[] min, byte[] max)
    {
        Assert.True(min.AsSpan().SequenceCompareTo(value) <= 0, "min above the value");
        Assert.True(max.AsSpan().SequenceCompareTo(value) >= 0, "max below the value");
        StrictUtf8.GetString(min);
        StrictUtf8.GetString(max);
    }
}
