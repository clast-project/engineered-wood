// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using Apache.Arrow;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Tests.TestData;
using EngineeredWood.Vortex.Tests.TestHelpers;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Reads <c>vortex.zstd</c> arrays. The fixtures come from <c>write_zstd_single_frame</c>,
/// <c>write_zstd_framed</c> and <c>write_zstd_compact</c> in <c>Rust/src/main.rs</c>.
/// </summary>
public class VortexZstdTests
{
    private const string SingleFrame = "zstd_string_64rows.vortex";
    private const string Framed = "zstd_framed_2000rows.vortex";
    private const string Compact = "zstd_compact_20000rows.vortex";

    [Fact]
    public async Task ReadsSingleFrameStrings()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(SingleFrame));

        var s = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
        AssertStrings(Enumerable.Range(0, 64).Select(i => i % 5 == 0 ? null : Row(i)).ToArray(), s);
    }

    [Fact]
    public async Task ReadsFramedColumnsSharingADictionary()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Framed));
        var rows = Enumerable.Range(0, 2000).ToArray();

        var s = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
        AssertStrings(rows.Select(i => i % 5 == 0 ? null : Row(i)).ToArray(), s);

        var b = Assert.IsType<BinaryArray>(await reader.ReadColumnAsync(1));
        Assert.Equal(0, b.NullCount);
        foreach (var i in rows)
            Assert.Equal(Encoding.UTF8.GetBytes(Row(i)).Concat(new byte[] { 0xff }), b.GetBytes(i).ToArray());

        var n = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(2));
        Assert.Equal(rows.Count(i => i % 7 == 0), n.NullCount);
        Assert.Equal(
            rows.Select(i => i % 7 == 0 ? (long?)null : (long)i * i - 1000),
            rows.Select(i => n.GetValue(i)));
    }

    /// <summary>
    /// Frames hold 100 valid values each in the framed fixture: `s` has 1600 (16 frames), `b`
    /// 2000 (20) and `n` 1714 (18). The single-frame fixture has one frame and, with too few
    /// samples to train one, no dictionary.
    /// </summary>
    [Theory]
    [InlineData(SingleFrame, false, new[] { 1 })]
    [InlineData(Framed, true, new[] { 16, 18, 20 })]
    public async Task FixtureHasTheIntendedFramesAndDictionary(string fixture, bool dictionary, int[] frameCounts)
    {
        var nodes = (await FixtureArrayNodes.ReadAsync(TestDataPath.Resolve(fixture)))
            .Where(n => n.Encoding == VortexArrayEncodings.Zstd)
            .ToList();

        var frames = new List<int>();
        foreach (var node in nodes)
        {
            var (dictionarySize, frameCount) = Metadata(node.Metadata);
            Assert.Equal(dictionary, dictionarySize > 0);
            Assert.Equal(frameCount + (dictionary ? 1 : 0), node.BufferCount);
            frames.Add(frameCount);
        }
        Assert.Equal(frameCounts, frames.OrderBy(f => f));
    }

    [Fact]
    public async Task CompactWriterChoosesZstdForBinary()
    {
        var roots = await FixtureArrayNodes.ReadColumnRootsAsync(TestDataPath.Resolve(Compact), 1);

        Assert.NotEmpty(roots);
        Assert.All(roots, r => Assert.Equal(VortexArrayEncodings.Zstd, r.Encoding));
    }

    [Fact]
    public async Task ReadsZstdChosenByTheCompactWriter()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Compact));

        var s = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
        AssertStrings(Enumerable.Range(0, 20_000).Select(CompactRow).ToArray(), s);

        var b = Assert.IsType<BinaryArray>(await reader.ReadColumnAsync(1));
        Assert.Equal(Enumerable.Range(0, 20_000).Count(i => i % 13 == 0), b.NullCount);
        for (int i = 0; i < 20_000; i++)
        {
            if (CompactRow(i) is { } row)
                Assert.Equal(Encoding.UTF8.GetBytes(row), b.GetBytes(i).ToArray());
            else
                Assert.True(b.IsNull(i), $"row {i}");
        }
    }

    private static readonly string[] Words =
        { "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "zstd", "frames", "façade", "日本" };

    /// <summary>Port of <c>zstd_row</c> in the fixture generator.</summary>
    private static string Row(int i)
    {
        if (i % 17 == 3)
            return "";
        int n = 3 + i * 7 % 9;
        return string.Join(" ", Enumerable.Range(0, n).Select(k => Words[(i * 31 + k * 11) % Words.Length]));
    }

    /// <summary>Row <paramref name="i"/> of <c>write_zstd_compact</c>, as text or bytes.</summary>
    private static string? CompactRow(int i) =>
        i % 13 == 0 ? null : $"{Row(i)} / {Row(i / 3)} / {Row(i / 7)} / {Row(i / 11)} / {Row(i / 19)}";

    private static void AssertStrings(string?[] expected, StringArray actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected.Count(e => e is null), actual.NullCount);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], actual.IsNull(i) ? null : actual.GetString(i));
    }

    /// <summary>The dictionary size and frame count from <c>ZstdMetadata</c>.</summary>
    private static (ulong DictionarySize, int Frames) Metadata(byte[] metadata)
    {
        ulong dictionarySize = 0;
        int frames = 0, pos = 0;
        var span = metadata.AsSpan();
        while (pos < span.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(span, ref pos);
            if (tag == (1 << 3))
            {
                dictionarySize = (ulong)Varint.ReadUnsigned(span, ref pos);
            }
            else if (tag == ((2 << 3) | 2))
            {
                frames++;
                int length = (int)Varint.ReadUnsigned(span, ref pos);
                pos += length;
            }
            else
            {
                throw new InvalidOperationException($"unexpected ZstdMetadata tag {tag}");
            }
        }
        return (dictionarySize, frames);
    }
}
