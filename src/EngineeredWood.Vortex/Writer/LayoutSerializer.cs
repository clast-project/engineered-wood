// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.FlatBuffers;

namespace EngineeredWood.Vortex.Writer;

/// <summary>
/// Builds the root <c>Layout</c> FlatBuffer for the writer's MVP shape:
/// <c>vortex.struct(vortex.flat × N)</c>. Each flat child references one
/// segment_spec index emitted by <see cref="SegmentWriter"/>.
///
/// <para>Layout slots (per layout.fbs / <see cref="EngineeredWood.Vortex.Format.Layout"/>):
/// 0=encoding (u16), 1=row_count (u64), 2=metadata (ubyte vec, absent here),
/// 3=children (Layout vec), 4=segments (u32 vec).</para>
/// </summary>
internal static class LayoutSerializer
{
    /// <summary>
    /// Serializes a vortex.struct root with one vortex.flat child per column.
    /// </summary>
    /// <param name="structEncodingIdx">Layout-spec index for "vortex.struct".</param>
    /// <param name="flatEncodingIdx">Layout-spec index for "vortex.flat".</param>
    /// <param name="totalRows">Row count (same for all columns at this MVP scope).</param>
    /// <param name="perColumnSegmentIdx">One segment-spec index per column (the column's data segment).</param>
    public static byte[] SerializeStructFlat(
        ushort structEncodingIdx,
        ushort flatEncodingIdx,
        ulong totalRows,
        IReadOnlyList<uint> perColumnSegmentIdx)
    {
        var b = new BackwardsFlatBufferBuilder();

        // Emit each flat child. Order doesn't matter for ticket validity.
        var childTickets = new int[perColumnSegmentIdx.Count];
        for (int i = 0; i < perColumnSegmentIdx.Count; i++)
            childTickets[i] = EmitFlatLayout(b, flatEncodingIdx, totalRows, perColumnSegmentIdx[i]);

        // Children vector (uoffset_t per child).
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);

        // Struct layout table:
        //   slots populated: 0 (encoding u16), 1 (row_count u64), 3 (children uoffset).
        //   inline: soffset(4) + children_uoffset(4@4) + row_count(8@8) + encoding(2@16) = 18 bytes.
        //   vt: vt_size = 4 + 4*2 = 12 (header + slots 0..3). slot2 absent → 0.
        //   slot offsets: slot0=16, slot1=8, slot2=0, slot3=4.
        // u64 at offset 8 from soffset → soffset must be 8-aligned.
        var structVt = b.WriteUInt16s(new ushort[] { 12, 18, 16, 8, 0, 4 });
        var structTicket = b.StartTable(alignment: 8, inlineSize: 18)
            .EmitU16(structEncodingIdx)
            .EmitU64(totalRows)
            .EmitUOffset(childrenVecTicket)
            .EmitSOffsetTo(structVt);

        return b.Finish(structTicket);
    }

    /// <summary>
    /// Serializes a vortex.struct root where each column is a vortex.chunked
    /// containing one vortex.flat per batch. Used when the writer streams more
    /// than one batch.
    /// </summary>
    /// <param name="perColumnSegmentIdx">[column][batch] segment-spec index.</param>
    /// <param name="perBatchRowCount">Row count for each batch.</param>
    public static byte[] SerializeStructChunked(
        ushort structEncodingIdx,
        ushort chunkedEncodingIdx,
        ushort flatEncodingIdx,
        ulong totalRows,
        IReadOnlyList<ulong> perBatchRowCount,
        uint[][] perColumnSegmentIdx)
    {
        if (perColumnSegmentIdx.Length == 0)
            throw new ArgumentException("perColumnSegmentIdx must be non-empty.", nameof(perColumnSegmentIdx));
        int batchCount = perBatchRowCount.Count;
        for (int c = 0; c < perColumnSegmentIdx.Length; c++)
            if (perColumnSegmentIdx[c].Length != batchCount)
                throw new ArgumentException(
                    $"Column {c} has {perColumnSegmentIdx[c].Length} segments but expected {batchCount} (one per batch).",
                    nameof(perColumnSegmentIdx));

        var b = new BackwardsFlatBufferBuilder();

        // Build one chunked layout per column.
        var columnTickets = new int[perColumnSegmentIdx.Length];
        for (int c = 0; c < perColumnSegmentIdx.Length; c++)
        {
            // Build M flat children for this column.
            var flatTickets = new int[batchCount];
            for (int batch = 0; batch < batchCount; batch++)
                flatTickets[batch] = EmitFlatLayout(
                    b, flatEncodingIdx, perBatchRowCount[batch], perColumnSegmentIdx[c][batch]);
            var childrenVecTicket = b.WriteUOffsetVector(flatTickets);

            // Chunked layout table: same vtable shape as struct (slots 0, 1, 3).
            //   inline (18): soffset(4) + children(4@4) + row_count(8@8) + encoding(2@16)
            //   vt: vt_size=12, slot0=16, slot1=8, slot2=0, slot3=4
            var chunkedVt = b.WriteUInt16s(new ushort[] { 12, 18, 16, 8, 0, 4 });
            columnTickets[c] = b.StartTable(alignment: 8, inlineSize: 18)
                .EmitU16(chunkedEncodingIdx)
                .EmitU64(totalRows)
                .EmitUOffset(childrenVecTicket)
                .EmitSOffsetTo(chunkedVt);
        }

        var rootChildrenTicket = b.WriteUOffsetVector(columnTickets);

        var structVt = b.WriteUInt16s(new ushort[] { 12, 18, 16, 8, 0, 4 });
        var structTicket = b.StartTable(alignment: 8, inlineSize: 18)
            .EmitU16(structEncodingIdx)
            .EmitU64(totalRows)
            .EmitUOffset(rootChildrenTicket)
            .EmitSOffsetTo(structVt);

        return b.Finish(structTicket);
    }

    /// <summary>
    /// Emits a vortex.flat Layout table (encoding, row_count, segments=[segIdx]).
    /// Returns the table ticket. Other slots (metadata, children) are absent.
    /// </summary>
    private static int EmitFlatLayout(
        BackwardsFlatBufferBuilder b, ushort flatEncodingIdx, ulong rowCount, uint segIdx)
    {
        var segVecTicket = b.WriteUInt32Vector(new uint[] { segIdx });

        // Flat layout table:
        //   slots populated: 0 (encoding u16), 1 (row_count u64), 4 (segments uoffset).
        //   inline: soffset(4) + segments_uoffset(4@4) + row_count(8@8) + encoding(2@16) = 18 bytes.
        //   vt: vt_size = 4 + 5*2 = 14 (header + slots 0..4). slots 2,3 absent → 0.
        //   slot offsets: slot0=16, slot1=8, slot2=0, slot3=0, slot4=4.
        var flatVt = b.WriteUInt16s(new ushort[] { 14, 18, 16, 8, 0, 0, 4 });
        return b.StartTable(alignment: 8, inlineSize: 18)
            .EmitU16(flatEncodingIdx)
            .EmitU64(rowCount)
            .EmitUOffset(segVecTicket)
            .EmitSOffsetTo(flatVt);
    }
}
