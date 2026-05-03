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
/// Per-Arrow-field plan. Today there are two concrete kinds:
/// <see cref="FlatColumnPlan"/> (segments hold the column directly) and
/// <see cref="DictColumnPlan"/> (the layout is <c>vortex.dict</c>: the column
/// is materialized from a values dictionary plus per-row codes). Future:
/// chunked-of-X, zoned, etc.
/// </summary>
internal abstract class ColumnPlan
{
    public IArrowType ArrowType { get; }
    public abstract ulong TotalRows { get; }
    public abstract int ChunkCount { get; }

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
}
