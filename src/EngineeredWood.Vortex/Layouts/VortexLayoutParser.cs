// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// Builds a managed <see cref="VortexLayout"/> tree from the root
/// <see cref="Format.Layout"/> FlatBuffer plus the layout-specs registry from
/// the file footer. Resolves <c>encoding</c> indices to their registered ids
/// and recursively materializes children, metadata bytes, and segment refs.
/// </summary>
internal static class VortexLayoutParser
{
    public static VortexLayout Parse(
        ReadOnlySpan<byte> layoutBytes,
        IReadOnlyList<string> layoutSpecs)
    {
        var root = Format.Layout.ReadRoot(layoutBytes);
        return ParseNode(root, layoutSpecs);
    }

    private static VortexLayout ParseNode(
        Format.Layout node,
        IReadOnlyList<string> layoutSpecs)
    {
        var idx = node.EncodingIndex;
        if (idx >= layoutSpecs.Count)
            throw new VortexFormatException(
                $"Layout encoding index {idx} is out of range (registry has {layoutSpecs.Count} entries).");
        var encodingId = layoutSpecs[idx];

        var metaVec = node.Metadata;
        byte[] metadata = metaVec.Length == 0
            ? Array.Empty<byte>()
            : metaVec.RawBytes(metaVec.Length).ToArray();

        var childCount = node.ChildCount;
        var children = childCount == 0
            ? Array.Empty<VortexLayout>()
            : new VortexLayout[childCount];
        for (int i = 0; i < childCount; i++)
            children[i] = ParseNode(node.Child(i), layoutSpecs);

        var segCount = node.SegmentCount;
        var segmentRefs = segCount == 0
            ? Array.Empty<uint>()
            : new uint[segCount];
        for (int i = 0; i < segCount; i++)
            segmentRefs[i] = node.SegmentIndex(i);

        return new VortexLayout(encodingId, node.RowCount, metadata, children, segmentRefs);
    }
}
