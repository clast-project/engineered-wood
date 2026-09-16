// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Encodings;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Tests.TestData;
using EngineeredWood.Vortex.Tests.TestHelpers;

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

    // Corrupt copies of the hand-built fixture: each must fail as a format error, not as an
    // index or allocation failure, however the bad value would have been used.

    [Fact]
    public async Task RejectsDecreasingInteriorRowOffset()
    {
        var bytes = await PatchChild(HandBuilt, child: 2, (buf, width) =>
            Write(buf, width, 10, Read(buf, width, 11) + 1));
        await AssertRejected(bytes, "vortex.onpair");
    }

    [Fact]
    public async Task RejectsInteriorRowOffsetPastTheCodes()
    {
        var bytes = await PatchChild(HandBuilt, child: 2, (buf, width) =>
            Write(buf, width, 5, 1_000_000));
        await AssertRejected(bytes, "codes_offsets");
    }

    [Fact]
    public async Task RejectsCodeOutsideTheDictionary()
    {
        var bytes = await PatchChild(HandBuilt, child: 1, (buf, width) =>
            Write(buf, width, 0, (1L << (8 * width)) - 1));
        await AssertRejected(bytes, "out of range");
    }

    [Fact]
    public async Task RejectsEmptyDictionaryToken()
    {
        var bytes = await PatchChild(HandBuilt, child: 0, (buf, width) =>
            Write(buf, width, 1, 0));
        await AssertRejected(bytes, "tokens are 1 to 16 bytes");
    }

    private static async Task AssertRejected(byte[] file, string messagePart)
    {
        using var stream = new ByteArrayRandomAccessFile(file);
        await using var reader = await VortexFileReader.OpenAsync(stream);
        var ex = await Assert.ThrowsAsync<VortexFormatException>(async () => await reader.ReadColumnAsync(0));
        Assert.Contains(messagePart, ex.Message);
    }

    /// <summary>
    /// The fixture's bytes with <paramref name="patch"/> applied to the data buffer of the given
    /// child of its OnPair array (the root of segment 0). The patch gets the buffer and its
    /// element width, derived from the child's value count.
    /// </summary>
    private static async Task<byte[]> PatchChild(string fixture, int child, Action<byte[], int> patch)
    {
        var path = TestDataPath.Resolve(fixture);
        var bytes = File.ReadAllBytes(path);
        long segmentOffset, segmentLength;
        await using (var reader = await VortexFileReader.OpenAsync(path))
        {
            segmentOffset = checked((long)reader.SegmentSpecs[0].Offset);
            segmentLength = checked((long)reader.SegmentSpecs[0].Length);
        }
        var node = (await OnPairNodes(fixture))[0];
        var (offset, length) = LocateChildBuffer(bytes, (int)segmentOffset, (int)segmentLength, child);
        long count = child switch
        {
            0 => (long)node.DictSize + 1,
            1 => (long)node.CodesLen,
            _ => 65,
        };
        int width = checked((int)(length / count));
        var buffer = bytes.AsSpan(offset, length).ToArray();
        patch(buffer, width);
        buffer.CopyTo(bytes, offset);
        return bytes;
    }

    private static (int Offset, int Length) LocateChildBuffer(
        byte[] file, int segmentOffset, int segmentLength, int child)
    {
        var serialized = SerializedArray.Parse(file.AsSpan(segmentOffset, segmentLength));
        var bufferRef = serialized.Message.Root.Child(child).BufferRef(0);
        return (segmentOffset + serialized.BufferOffset(bufferRef), serialized.BufferBytes(bufferRef).Length);
    }

    private static long Read(byte[] buf, int width, int index)
    {
        long value = 0;
        for (int b = width - 1; b >= 0; b--)
            value = (value << 8) | buf[index * width + b];
        return value;
    }

    private static void Write(byte[] buf, int width, int index, long value)
    {
        for (int b = 0; b < width; b++)
            buf[index * width + b] = (byte)(value >> (8 * b));
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

    private sealed record OnPairNode(ulong CodesLen, ulong DictSize, IReadOnlyList<string> ChildEncodings);

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
            var raw = meta.Length == 0 ? default : meta.RawBytes(meta.Length);
            found.Add(new OnPairNode(MetadataField(raw, 4), MetadataField(raw, 3), children));
        }
        for (int i = 0; i < node.ChildCount; i++)
            Collect(node.Child(i), specs, found);
    }

    /// <summary>A varint field of <c>OnPairMetadata</c> (3 = dict_size, 4 = codes_len).</summary>
    private static ulong MetadataField(ReadOnlySpan<byte> metadata, int field)
    {
        int pos = 0;
        while (pos < metadata.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(metadata, ref pos);
            if ((tag & 7) != 0)
                throw new InvalidOperationException("OnPairMetadata has only varint fields.");
            var value = (ulong)Varint.ReadUnsigned(metadata, ref pos);
            if (tag >> 3 == (ulong)field)
                return value;
        }
        return 0;
    }
}
