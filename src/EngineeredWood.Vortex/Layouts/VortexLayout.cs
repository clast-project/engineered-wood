// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// Materialized layout tree node. Built once at file-open time from the root
/// <see cref="Format.Layout"/> FlatBuffer; persists for the life of the
/// <see cref="VortexFileReader"/> so the rest of the read path doesn't need
/// to keep the underlying span alive.
/// </summary>
internal sealed class VortexLayout
{
    /// <summary>Registry-resolved encoding id (e.g. <c>vortex.flat</c>, <c>vortex.struct</c>).</summary>
    public string EncodingId { get; }

    /// <summary>Number of logical rows this node represents.</summary>
    public ulong RowCount { get; }

    /// <summary>Layout-specific opaque metadata bytes (typically tiny).</summary>
    public byte[] Metadata { get; }

    /// <summary>Child layouts. For vortex.struct these are columns; for vortex.chunked, row chunks.</summary>
    public IReadOnlyList<VortexLayout> Children { get; }

    /// <summary>
    /// Indices into <see cref="VortexFileReader.SegmentSpecs"/> for the data
    /// segments this node points to. Typically only leaf layouts (vortex.flat)
    /// reference segments directly.
    /// </summary>
    public IReadOnlyList<uint> SegmentRefs { get; }

    public VortexLayout(
        string encodingId,
        ulong rowCount,
        byte[] metadata,
        IReadOnlyList<VortexLayout> children,
        IReadOnlyList<uint> segmentRefs)
    {
        EncodingId = encodingId;
        RowCount = rowCount;
        Metadata = metadata;
        Children = children;
        SegmentRefs = segmentRefs;
    }
}
