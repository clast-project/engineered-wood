// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Layouts;

namespace EngineeredWood.Vortex.Tests.TestHelpers;

/// <summary>An array node in a file, by encoding id, with its children's encoding ids.</summary>
internal sealed record ArrayNodeInfo(string Encoding, IReadOnlyList<string> Children);

/// <summary>
/// Lists every array node in a Vortex file's segments, so a test can confirm the writer used
/// the encoding the test means to exercise rather than trusting the fixture's name.
/// </summary>
internal static class FixtureArrayNodes
{
    public static async Task<List<ArrayNodeInfo>> ReadAsync(string path)
    {
        await using var reader = await VortexFileReader.OpenAsync(path);
        using var file = new LocalRandomAccessFile(path);

        var nodes = new List<ArrayNodeInfo>();
        foreach (var locator in reader.SegmentSpecs)
        {
            using var owner = await file.ReadAsync(
                new FileRange(checked((long)locator.Offset), checked((int)locator.Length)));
            Collect(SerializedArray.Parse(owner.Memory.Span).Message.Root, reader.ArraySpecs, nodes);
        }
        return nodes;
    }

    /// <summary>
    /// The root array node of each data segment of column <paramref name="field"/>, in chunk
    /// order (a dict layout contributes its values' segments, then its codes').
    /// </summary>
    public static async Task<List<ArrayNodeInfo>> ReadColumnRootsAsync(string path, int field)
    {
        await using var reader = await VortexFileReader.OpenAsync(path);
        using var file = new LocalRandomAccessFile(path);

        var roots = new List<ArrayNodeInfo>();
        foreach (var segmentRef in SegmentRefs(reader.ColumnPlans[field]))
        {
            var locator = reader.SegmentSpecs[(int)segmentRef];
            using var owner = await file.ReadAsync(
                new FileRange(checked((long)locator.Offset), checked((int)locator.Length)));
            var root = SerializedArray.Parse(owner.Memory.Span).Message.Root;
            var children = new string[root.ChildCount];
            for (int i = 0; i < children.Length; i++)
                children[i] = reader.ArraySpecs[root.Child(i).EncodingIndex];
            roots.Add(new ArrayNodeInfo(reader.ArraySpecs[root.EncodingIndex], children));
        }
        return roots;
    }

    private static IEnumerable<uint> SegmentRefs(ColumnPlan plan) => plan switch
    {
        FlatColumnPlan flat => flat.Chunks.Select(c => c.SegmentRef),
        DictColumnPlan dict => SegmentRefs(dict.Values).Concat(SegmentRefs(dict.Codes)),
        _ => throw new NotSupportedException($"No segment listing for {plan.GetType().Name}."),
    };

    private static void Collect(ArrayNode node, IReadOnlyList<string> specs, List<ArrayNodeInfo> nodes)
    {
        var children = new string[node.ChildCount];
        for (int i = 0; i < children.Length; i++)
            children[i] = specs[node.Child(i).EncodingIndex];
        nodes.Add(new ArrayNodeInfo(specs[node.EncodingIndex], children));
        for (int i = 0; i < node.ChildCount; i++)
            Collect(node.Child(i), specs, nodes);
    }
}
