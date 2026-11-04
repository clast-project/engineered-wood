// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.Stats;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// String min/max is found over UTF-8 bytes and only the winners are decoded (#661). These pin that the result
/// is exactly the code point order bounds, and that ill-formed UTF-8 still gets bounds that hold for the decoded
/// values.
/// </summary>
public class StringStatsTests
{
    [Fact]
    public void SupplementaryCharacter_SortsAbove_E000ToFFFF()
    {
        // UTF-16 code-unit order would put the emoji (a surrogate pair) BELOW U+E000 and U+FFFF.
        var (min, max) = Bounds(Strings("￿", "\U0001F600", "", "z"));
        Assert.Equal("z", min);
        Assert.Equal("\U0001F600", max);
    }

    [Fact]
    public void RandomMixedStrings_MatchCodePointOrder()
    {
        var random = new Random(661);
        string[] alphabet = ["a", "Z", "é", "߿", "ࠀ", "퟿", "", "�", "￿",
            "\U00010000", "\U0001F600", "\U0010FFFF"];

        for (int round = 0; round < 200; round++)
        {
            int count = random.Next(1, 30);
            var values = new string?[count];
            for (int i = 0; i < count; i++)
            {
                if (random.Next(6) == 0) continue;   // null
                var sb = new StringBuilder();
                int length = random.Next(0, 6);
                for (int k = 0; k < length; k++) sb.Append(alphabet[random.Next(alphabet.Length)]);
                values[i] = sb.ToString();
            }

            var present = values.Where(v => v is not null).Select(v => v!).ToList();
            var (min, max) = Bounds(Strings(values));
            if (present.Count == 0)
            {
                Assert.Null(min);
                Assert.Null(max);
                continue;
            }

            present.Sort(CompareCodePoints);
            Assert.Equal(present[0], min);
            Assert.Equal(present[present.Count - 1], max);

            // LargeString takes the same route.
            var large = new LargeStringArray.Builder();
            foreach (var v in values) { if (v is null) large.AppendNull(); else large.Append(v); }
            var (lmin, lmax) = Bounds(large.Build(), LargeStringType.Default);
            Assert.Equal(present[0], lmin);
            Assert.Equal(present[present.Count - 1], lmax);
        }
    }

    [Fact]
    public void SlicedArray_BoundsOnlyTheSlice()
    {
        var full = Strings("\U0001F600", "m", "c", "x", "a");
        var slice = (StringArray)full.Slice(1, 3);   // "m", "c", "x"
        var (min, max) = Bounds(slice);
        Assert.Equal("c", min);
        Assert.Equal("x", max);
    }

    [Fact]
    public void AllNull_HasNoBounds()
    {
        var (min, max) = Bounds(Strings(null, null));
        Assert.Null(min);
        Assert.Null(max);
    }

    [Fact]
    public void IllFormedCell_ThatLosesOnBytes_StillBoundedWhenDecoded()
    {
        // Byte max is "é" (C3 A9 > 80), but 0x80 decodes to U+FFFD, which sorts above "é". The bound has to
        // cover the decoded value even though the bad cell wins neither comparison on bytes.
        var array = RawStrings([0x61], [0xC3, 0xA9], [0x80]);
        var (min, max) = Bounds(array);
        Assert.Equal("a", min);
        Assert.Equal("�", max);
    }

    [Fact]
    public void IllFormedWinner_DecodedBoundsHold()
    {
        // On bytes, 0xFF beats the emoji; decoded it is U+FFFD, which the emoji beats.
        var array = RawStrings([0xFF], Encoding.UTF8.GetBytes("\U0001F600"), [0x62]);
        var (min, max) = Bounds(array);
        Assert.Equal("b", min);
        Assert.Equal("\U0001F600", max);
    }

    [Fact]
    public void CharacterSplitAcrossCells_IsIllFormed()
    {
        // The value buffer "aé" (61 C3 A9) is well-formed as a whole, but the cells are [61 C3] and [A9]: each
        // half of "é" decodes to U+FFFD. The one-pass validation has to catch the boundary inside the character.
        // "中" (E4 B8 AD) wins on bytes, but the halves decode to U+FFFD, which sorts above it.
        var array = RawStrings([0xE4, 0xB8, 0xAD], [0x61, 0xC3], [0xA9]);
        var (min, max) = Bounds(array);
        Assert.Equal("a�", min);
        Assert.Equal("�", max);
    }

    [Fact]
    public void ScalarValidator_MatchesUnicodeDefinition()
    {
        var strict = new UTF8Encoding(false, throwOnInvalidBytes: true);
        bool Expected(byte[] bytes)
        {
            try { strict.GetCharCount(bytes); return true; }
            catch (DecoderFallbackException) { return false; }
        }

        // Every 1- and 2-byte sequence, and every 3-byte sequence led by E0..EF / F0..F4 lead + 2 bytes.
        for (int a = 0; a < 256; a++)
        {
            Check([(byte)a]);
            for (int b = 0; b < 256; b++)
            {
                Check([(byte)a, (byte)b]);
                if (a >= 0xE0 && a <= 0xF4)
                    for (int c = 0x70; c < 0xD0; c++) Check([(byte)a, (byte)b, (byte)c]);
            }
        }

        var random = new Random(661);
        for (int round = 0; round < 50_000; round++)
        {
            var bytes = new byte[random.Next(1, 9)];
            for (int i = 0; i < bytes.Length; i++)
                bytes[i] = random.Next(3) == 0 ? (byte)random.Next(0x80, 0xC0) : (byte)random.Next(256);
            Check(bytes);
        }

        // Boundary code points round-trip as valid.
        foreach (var s in new[] { "\u007F", "\u0080", "߿", "ࠀ", "퟿", "", "￿",
            "\U00010000", "\U0010FFFF" })
            Assert.True(StatsCollector.IsWellFormedUtf8Scalar(Encoding.UTF8.GetBytes(s)), s);

        void Check(byte[] bytes) =>
            Assert.True(Expected(bytes) == StatsCollector.IsWellFormedUtf8Scalar(bytes), BitConverter.ToString(bytes));
    }

    private static StringArray Strings(params string?[] values)
    {
        var builder = new StringArray.Builder();
        foreach (var v in values) { if (v is null) builder.AppendNull(); else builder.Append(v); }
        return builder.Build();
    }

    private static StringArray RawStrings(params byte[][] cells)
    {
        var offsets = new ArrowBuffer.Builder<int>();
        var data = new List<byte>();
        offsets.Append(0);
        foreach (var cell in cells)
        {
            data.AddRange(cell);
            offsets.Append(data.Count);
        }
        return new StringArray(cells.Length, offsets.Build(), new ArrowBuffer(data.ToArray()), ArrowBuffer.Empty);
    }

    private static (string? Min, string? Max) Bounds(IArrowArray array, IArrowType? type = null)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", type ?? StringType.Default, true))
            .Build();
        string? stats = StatsCollector.Collect(new RecordBatch(schema, [array], array.Length));
        Assert.NotNull(stats);
        var root = JsonDocument.Parse(stats).RootElement;
        return (Get(root, "minValues"), Get(root, "maxValues"));

        static string? Get(JsonElement root, string key) =>
            root.TryGetProperty(key, out var bounds) && bounds.TryGetProperty("s", out var v) ? v.GetString() : null;
    }

    // Code point order, computed independently of UTF-8 bytes.
    private static int CompareCodePoints(string a, string b)
    {
        int i = 0, j = 0;
        while (i < a.Length && j < b.Length)
        {
            int ca = char.ConvertToUtf32(a, i), cb = char.ConvertToUtf32(b, j);
            if (ca != cb) return ca.CompareTo(cb);
            i += char.IsSurrogatePair(a, i) ? 2 : 1;
            j += char.IsSurrogatePair(b, j) ? 2 : 1;
        }
        return (a.Length - i).CompareTo(b.Length - j);
    }
}
