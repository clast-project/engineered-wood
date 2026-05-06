// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// One row chunk for a single column: a segment ref into
/// <see cref="VortexFileReader.SegmentSpecs"/> plus the row count that segment
/// holds. Multiple chunks per column come from <c>vortex.chunked</c> wrappers.
/// </summary>
internal readonly record struct SegmentChunk(uint SegmentRef, ulong RowCount);

/// <summary>
/// Per-column zoned-stats descriptor. Set when the layout was wrapped in
/// <c>vortex.stats</c>; null otherwise. Captures everything the reader
/// needs to materialize the per-zone stats table on demand.
/// </summary>
internal sealed class ZoneInfo
{
    /// <summary>Logical row count of each zone (final zone may be shorter).</summary>
    public int ZoneLen { get; }
    /// <summary>Sorted list of <see cref="EngineeredWood.Vortex.Stat"/>
    /// values present in the zones table — derived from the bitset bytes
    /// in the layout's metadata (vortex's <c>as_stat_bitset_bytes</c>).</summary>
    public IReadOnlyList<int> PresentStats { get; }
    /// <summary>Segment-spec index of the zones-table data segment.</summary>
    public uint ZonesSegmentRef { get; }
    /// <summary>Number of zones (= zones-table row count = batch count for our writer).</summary>
    public int ZoneCount { get; }

    public ZoneInfo(int zoneLen, IReadOnlyList<int> presentStats, uint zonesSegmentRef, int zoneCount)
    {
        ZoneLen = zoneLen;
        PresentStats = presentStats;
        ZonesSegmentRef = zonesSegmentRef;
        ZoneCount = zoneCount;
    }
}

/// <summary>
/// Per-Arrow-field plan. Today there are two concrete kinds:
/// <see cref="FlatColumnPlan"/> (segments hold the column directly) and
/// <see cref="DictColumnPlan"/> (the layout is <c>vortex.dict</c>: the column
/// is materialized from a values dictionary plus per-row codes). Future:
/// chunked-of-X, zoned, etc.
/// </summary>
internal abstract class ColumnPlan
{
    public IArrowType ArrowType { get; }
    /// <summary>Set when this column was wrapped in <c>vortex.stats</c> at
    /// file write time. Null otherwise — pruning is unavailable.</summary>
    public ZoneInfo? ZoneInfo { get; init; }
    public abstract ulong TotalRows { get; }
    public abstract int ChunkCount { get; }
    /// <summary>Logical row count of the chunk at <paramref name="chunkIndex"/>.</summary>
    public abstract ulong ChunkRowCount(int chunkIndex);

    protected ColumnPlan(IArrowType arrowType) { ArrowType = arrowType; }
}

/// <summary>The column lives in 1+ flat segments (the common case).</summary>
internal sealed class FlatColumnPlan : ColumnPlan
{
    public IReadOnlyList<SegmentChunk> Chunks { get; }

    public FlatColumnPlan(IArrowType arrowType, IReadOnlyList<SegmentChunk> chunks)
        : base(arrowType)
    {
        Chunks = chunks;
    }

    public override int ChunkCount => Chunks.Count;

    public override ulong TotalRows
    {
        get { ulong sum = 0; foreach (var c in Chunks) sum += c.RowCount; return sum; }
    }

    public override ulong ChunkRowCount(int chunkIndex) => Chunks[chunkIndex].RowCount;
}

/// <summary>
/// The column is dict-encoded at the LAYOUT level (<c>vortex.dict</c>):
/// <see cref="Values"/> holds the dictionary array, <see cref="Codes"/> holds
/// per-row indices into it. Reconstruction at read time is
/// <c>output[i] = Values[Codes[i]]</c>.
/// </summary>
internal sealed class DictColumnPlan : ColumnPlan
{
    public ColumnPlan Values { get; }
    public ColumnPlan Codes { get; }

    public DictColumnPlan(IArrowType arrowType, ColumnPlan values, ColumnPlan codes)
        : base(arrowType)
    {
        Values = values;
        Codes = codes;
    }

    public override int ChunkCount => Codes.ChunkCount;
    public override ulong TotalRows => Codes.TotalRows;
    public override ulong ChunkRowCount(int chunkIndex) => Codes.ChunkRowCount(chunkIndex);
}
