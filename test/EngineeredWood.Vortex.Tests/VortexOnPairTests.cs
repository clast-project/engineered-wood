// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Encodings;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Tests.TestData;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Reads <c>vortex.onpair</c> string arrays (edition <c>core2026.08.1</c>). The fixtures come from
/// <c>write_onpair_string</c> and <c>write_onpair_default</c> in <c>Rust/src/main.rs</c>.
/// </summary>
public class VortexOnPairTests
{
    private const string HandBuilt = "onpair_string_64rows.vortex";
    private const string Sliced = "onpair_sliced_40rows.vortex";
    private const string Default = "onpair_default_20000rows.vortex";

    [Fact]
    public async Task ReadsHandBuiltOnPair()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(HandBuilt));

        var s = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
        AssertRows(Enumerable.Range(0, 64).Select(Row).ToArray(), s);
    }

    [Fact]
    public async Task ReadsSlicedOnPairFromItsFirstCodeOffset()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Sliced));

        var s = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
        AssertRows(Enumerable.Range(10, 40).Select(Row).ToArray(), s);
    }

    [Fact]
    public async Task SliceKeepsTheWholeCodeStream()
    {
        // What makes the sliced fixture a test of the code window: its codes child is the
        // unsliced array's, so rows 10..50 start partway into it.
        var full = await OnPairNodes(HandBuilt);
        var sliced = await OnPairNodes(Sliced);

        Assert.Single(full);
        Assert.Single(sliced);
        Assert.Equal(full[0].CodesLen, sliced[0].CodesLen);
    }

    [Fact]
    public async Task ReadsOnPairChosenByTheDefaultWriter()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Default));

        var s = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
        AssertRows(Enumerable.Range(0, 20_000).Select(DefaultRow).ToArray(), s);
    }

    [Fact]
    public async Task DefaultWriterCompressesOnPairChildrenAcrossChunks()
    {
        var nodes = await OnPairNodes(Default);

        Assert.True(nodes.Count > 1, $"expected OnPair in several chunks, found {nodes.Count}");
        Assert.Contains(nodes, n => n.ChildEncodings.Any(e => e != VortexArrayEncodings.Primitive
                                                             && e != VortexArrayEncodings.Bool));
    }

    [Fact]
    public async Task StreamsDefaultWrittenOnPairInOrder()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Default));

        int row = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            var s = (StringArray)batch.Column(0);
            for (int i = 0; i < batch.Length; i++, row++)
            {
                Assert.Equal(DefaultRow(row), s.IsNull(i) ? null : s.GetString(i));
            }
        }
        Assert.Equal(20_000, row);
    }

    /// <summary>Port of <c>onpair_row</c> in the fixture generator.</summary>
    private static string? Row(int i) => (i % 9) switch
    {
        0 => null,
        1 => "",
        2 => $"https://example.com/products/{i * 7}",
        3 => $"https://example.com/profile/{i}",
        4 => $"café-{i % 5}-naïve-日本語",
        5 => "https://example.org/about",
        6 => $"user-{i:D4}@example.com",
        7 => new string('x', i % 40),
        _ => $"https://example.com/products/{i}?ref=home",
    };

    private static readonly string[] Pages =
    {
        "https://example.com/products/widget",
        "https://example.com/products/gadget",
        "https://example.org/about",
        "https://example.net/blog/2026/09/post",
        "mailto:someone@example.com",
    };

    /// <summary>Port of <c>onpair_default_row</c> in the fixture generator.</summary>
    private static string? DefaultRow(int i) =>
        i % 13 == 0 ? null : $"{Pages[i % 5]}?id={(long)i * 7919 % 10007}";

    private static void AssertRows(string?[] expected, StringArray actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected.Count(e => e is null), actual.NullCount);
        for (int i = 0; i < expected.Length; i++)
        {
            if (expected[i] is null)
                Assert.True(actual.IsNull(i), $"row {i} should be null");
            else
                Assert.Equal(expected[i], actual.GetString(i));
        }
    }

    private sealed record OnPairNode(ulong CodesLen, IReadOnlyList<string> ChildEncodings);

    /// <summary>Every <c>vortex.onpair</c> array node in the file's segments.</summary>
    private static async Task<List<OnPairNode>> OnPairNodes(string fixture)
    {
        var path = TestDataPath.Resolve(fixture);
        await using var reader = await VortexFileReader.OpenAsync(path);
        using var file = new LocalRandomAccessFile(path);

        var found = new List<OnPairNode>();
        foreach (var locator in reader.SegmentSpecs)
        {
            using var owner = await file.ReadAsync(
                new FileRange(checked((long)locator.Offset), checked((int)locator.Length)));
            var serialized = SerializedArray.Parse(owner.Memory.Span);
            Collect(serialized.Message.Root, reader.ArraySpecs, found);
        }
        return found;
    }

    private static void Collect(ArrayNode node, IReadOnlyList<string> specs, List<OnPairNode> found)
    {
        if (specs[node.EncodingIndex] == VortexArrayEncodings.OnPair)
        {
            var children = new List<string>();
            for (int i = 0; i < node.ChildCount; i++)
                children.Add(specs[node.Child(i).EncodingIndex]);
            var meta = node.Metadata;
            found.Add(new OnPairNode(CodesLen(meta.Length == 0 ? default : meta.RawBytes(meta.Length)), children));
        }
        for (int i = 0; i < node.ChildCount; i++)
            Collect(node.Child(i), specs, found);
    }

    /// <summary>Field 4 of <c>OnPairMetadata</c>.</summary>
    private static ulong CodesLen(ReadOnlySpan<byte> metadata)
    {
        int pos = 0;
        while (pos < metadata.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(metadata, ref pos);
            if ((tag & 7) != 0)
                throw new InvalidOperationException("OnPairMetadata has only varint fields.");
            var value = (ulong)Varint.ReadUnsigned(metadata, ref pos);
            if (tag >> 3 == 4)
                return value;
        }
        return 0;
    }
}
