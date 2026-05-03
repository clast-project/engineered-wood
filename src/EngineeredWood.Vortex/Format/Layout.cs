// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.FlatBuffers;

namespace EngineeredWood.Vortex.Format;

/// <summary>
/// Reader for the recursive <c>Layout</c> FlatBuffers table from layout.fbs.
/// Slots: 0=encoding (u16, indexes <see cref="Footer.LayoutSpecs"/>),
/// 1=row_count (u64), 2=metadata (ubyte vector, opaque to this layer),
/// 3=children (Layout vector), 4=segments (u32 vector, indexes
/// <see cref="Footer.SegmentSpecs"/>).
/// </summary>
internal readonly ref struct Layout
{
    private readonly FlatBufferTable _table;

    public Layout(FlatBufferTable table) { _table = table; }

    public static Layout ReadRoot(ReadOnlySpan<byte> buf) =>
        new(FlatBufferTable.ReadRoot(buf));

    public ushort EncodingIndex => _table.ReadUInt16(0);
    public ulong RowCount => _table.ReadUInt64(1);

    public FlatBufferVector Metadata => _table.ReadVector(2);
    public FlatBufferVector Children => _table.ReadVector(3);
    public FlatBufferVector Segments => _table.ReadVector(4);

    public int ChildCount => Children.Length;
    public Layout Child(int i) => new(Children.Table(i));

    public int SegmentCount => Segments.Length;
    public uint SegmentIndex(int i) => Segments.UInt32(i);
}
