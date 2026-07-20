// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Encodings;
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
    /// Like <see cref="SerializeStructChunked"/> but additionally wraps each
    /// column in a <c>vortex.stats</c> layout carrying a per-zone stats
    /// table. The stats layout has 2 children: the chunked data layout, and
    /// a vortex.flat layout pointing to a segment with the zones array.
    /// Metadata is <c>[u32 zone_len LE, bitset]</c>; for phase A the bitset
    /// is <c>[0x40, 0x00]</c> — only Stat::NullCount (= bit 6) is present.
    /// </summary>
    /// <param name="zoneLen">Logical row count of each zone (= row count of
    /// the first non-final batch). The trailing batch may be shorter.</param>
    /// <param name="numZones">Number of zones (= batch count).</param>
    /// <param name="perColumnZonesSegmentIdx">One segment-spec index per
    /// column pointing at the zones-table segment for that column.</param>
    public static byte[] SerializeStructStatsChunked(
        ushort structEncodingIdx,
        ushort statsEncodingIdx,
        ushort chunkedEncodingIdx,
        ushort flatEncodingIdx,
        ulong totalRows,
        int zoneLen,
        int numZones,
        IReadOnlyList<ulong> perBatchRowCount,
        uint[][] perColumnSegmentIdx,
        IReadOnlyList<uint> perColumnZonesSegmentIdx,
        IReadOnlyList<byte[]> perColumnStatsBitset)
    {
        if (perColumnSegmentIdx.Length == 0)
            throw new ArgumentException("perColumnSegmentIdx must be non-empty.", nameof(perColumnSegmentIdx));
        if (perColumnZonesSegmentIdx.Count != perColumnSegmentIdx.Length)
            throw new ArgumentException(
                "perColumnZonesSegmentIdx must have one entry per column.",
                nameof(perColumnZonesSegmentIdx));
        if (perColumnStatsBitset.Count != perColumnSegmentIdx.Length)
            throw new ArgumentException(
                "perColumnStatsBitset must have one entry per column.",
                nameof(perColumnStatsBitset));
        int batchCount = perBatchRowCount.Count;

        var b = new BackwardsFlatBufferBuilder();

        // Per-column metadata: [u32 zone_len LE][bitset bytes]. The bitset
        // shape matches upstream's `as_stat_bitset_bytes` (a packed 9-bit
        // field covering all Stats; Phase A used [0x40, 0x00] for NullCount,
        // Phase B uses [0x58, 0x00] for {Max, Min, NullCount} on integer
        // columns). Each column carries its own metadata since schemes can
        // differ across types.
        var columnMetadataTickets = new int[perColumnSegmentIdx.Length];
        for (int c = 0; c < columnMetadataTickets.Length; c++)
        {
            var bitset = perColumnStatsBitset[c];
            var meta = new byte[4 + bitset.Length];
            meta[0] = (byte)(zoneLen & 0xFF);
            meta[1] = (byte)((zoneLen >> 8) & 0xFF);
            meta[2] = (byte)((zoneLen >> 16) & 0xFF);
            meta[3] = (byte)((zoneLen >> 24) & 0xFF);
            bitset.CopyTo(meta, 4);
            columnMetadataTickets[c] = b.WriteByteVector(meta);
        }

        var columnTickets = new int[perColumnSegmentIdx.Length];
        for (int c = 0; c < perColumnSegmentIdx.Length; c++)
        {
            // Build the chunked data layout for this column.
            var flatTickets = new int[batchCount];
            for (int batch = 0; batch < batchCount; batch++)
                flatTickets[batch] = EmitFlatLayout(
                    b, flatEncodingIdx, perBatchRowCount[batch], perColumnSegmentIdx[c][batch]);
            var dataChildrenTicket = b.WriteUOffsetVector(flatTickets);
            var chunkedVt = b.WriteUInt16s(new ushort[] { 12, 18, 16, 8, 0, 4 });
            int dataLayoutTicket = b.StartTable(alignment: 8, inlineSize: 18)
                .EmitU16(chunkedEncodingIdx)
                .EmitU64(totalRows)
                .EmitUOffset(dataChildrenTicket)
                .EmitSOffsetTo(chunkedVt);

            // Build the zones layout (vortex.flat with row_count = numZones).
            int zonesLayoutTicket = EmitFlatLayout(
                b, flatEncodingIdx, (ulong)numZones, perColumnZonesSegmentIdx[c]);

            // Wrap in vortex.stats. Layout slots populated: 0=encoding (u16),
            // 1=row_count (u64), 2=metadata (uoffset), 3=children (uoffset).
            // Canonical FB inline body (low→high address):
            //   offset 0..3:  soffset
            //   offset 4..7:  metadata uoffset    (slot2)
            //   offset 8..15: row_count u64       (slot1, 8-aligned)
            //   offset 16..19: children uoffset   (slot3)
            //   offset 20..21: encoding u16       (slot0)
            //   inline_size = 22.
            // vt: vt_size = 4 + 5*2 = 14 (header + slots 0..4), slot4=0 (absent).
            var statsChildrenTicket = b.WriteUOffsetVector(new[] { dataLayoutTicket, zonesLayoutTicket });
            var statsVt = b.WriteUInt16s(new ushort[] { 14, 22, 20, 8, 4, 16, 0 });
            // Emit in high→low-offset order (BackwardsFlatBufferBuilder writes
            // from the end of the inline body backwards):
            columnTickets[c] = b.StartTable(alignment: 8, inlineSize: 22)
                .EmitU16(statsEncodingIdx)             // offset 20..21
                .EmitUOffset(statsChildrenTicket)      // offset 16..19
                .EmitU64(totalRows)                    // offset 8..15
                .EmitUOffset(columnMetadataTickets[c]) // offset 4..7
                .EmitSOffsetTo(statsVt);               // offset 0..3
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
    /// Like <see cref="SerializeStructChunked"/> but additionally wraps a
    /// subset of columns in <c>vortex.dict</c>. Each entry of
    /// <paramref name="dictColumns"/> is non-null for columns whose
    /// per-batch segments in <paramref name="perColumnSegmentIdx"/> hold
    /// PER-BATCH CODES (instead of the column's data segments). The dict
    /// shape per column is:
    /// <c>vortex.dict(values=vortex.flat(dict-segment), codes=vortex.chunked(vortex.flat × M))</c>.
    /// Stats co-existence on dict columns is deferred — caller falls back
    /// to the non-zoned chunked layout when any dict column is present.
    /// </summary>
    public static byte[] SerializeStructDictMixed(
        ushort structEncodingIdx,
        ushort dictEncodingIdx,
        ushort chunkedEncodingIdx,
        ushort flatEncodingIdx,
        ulong totalRows,
        IReadOnlyList<ulong> perBatchRowCount,
        uint[][] perColumnSegmentIdx,
        VortexFileWriter.DictColumnInfo[] dictColumns)
    {
        if (perColumnSegmentIdx.Length != dictColumns.Length)
            throw new ArgumentException(
                "perColumnSegmentIdx must align with dictColumns.", nameof(dictColumns));
        int batchCount = perBatchRowCount.Count;

        var b = new BackwardsFlatBufferBuilder();

        // Reused per-column metadata scratch — DictLayoutMetadata is at most
        // ~4 bytes (tag + varint codes_ptype + tag + bool nullable). Reused
        // across columns to avoid stack allocs in a loop.
        Span<byte> metaBuf = stackalloc byte[16];
        var columnTickets = new int[perColumnSegmentIdx.Length];
        for (int c = 0; c < perColumnSegmentIdx.Length; c++)
        {
            // Codes / data chunked layout: one flat child per batch with the
            // per-batch segment index and the batch's row count.
            var flatTickets = new int[batchCount];
            for (int batch = 0; batch < batchCount; batch++)
                flatTickets[batch] = EmitFlatLayout(
                    b, flatEncodingIdx, perBatchRowCount[batch], perColumnSegmentIdx[c][batch]);
            var chunkedChildrenTicket = b.WriteUOffsetVector(flatTickets);
            var chunkedVt = b.WriteUInt16s(new ushort[] { 12, 18, 16, 8, 0, 4 });
            int chunkedTicket = b.StartTable(alignment: 8, inlineSize: 18)
                .EmitU16(chunkedEncodingIdx)
                .EmitU64(totalRows)
                .EmitUOffset(chunkedChildrenTicket)
                .EmitSOffsetTo(chunkedVt);

            if (dictColumns[c].ValuesSegmentIdx == 0 && dictColumns[c].DictRowCount == 0)
            {
                // Sentinel "no dict" — just use the chunked layout directly.
                columnTickets[c] = chunkedTicket;
                continue;
            }

            // Values flat layout: 1 segment, row_count = dict_size.
            int valuesFlatTicket = EmitFlatLayout(
                b, flatEncodingIdx, dictColumns[c].DictRowCount, dictColumns[c].ValuesSegmentIdx);

            // DictLayoutMetadata proto bytes: field 1 = codes_ptype (varint),
            // field 2 = is_nullable_codes (varint bool, 0/1).
            // Proto3 default 0/false would normally be omitted, but vortex's
            // ProstMetadata serializer always emits Some(...) explicitly when
            // the writer set the field — so write both fields unconditionally
            // for fidelity with upstream's wire output (the reader is robust
            // either way).
            int metaPos = 0;
            metaBuf[metaPos++] = 0x08;
            metaPos += Varint.WriteUnsigned(metaBuf.Slice(metaPos), dictColumns[c].CodesPtype);
            metaBuf[metaPos++] = 0x10;
            metaBuf[metaPos++] = (byte)(dictColumns[c].IsNullableCodes ? 1 : 0);
            int dictMetaTicket = b.WriteByteVector(metaBuf.Slice(0, metaPos).ToArray());

            // Dict layout: 2 children (values, codes), metadata. Same
            // vtable shape as vortex.stats: slots 0+1+2+3, no segments.
            // inline (22): soffset(4) + meta(4@4) + row_count(8@8) +
            // children(4@16) + encoding(2@20).
            // vt: vt_size = 4 + 5*2 = 14, slot offsets {0:20, 1:8, 2:4, 3:16, 4:0}.
            var dictChildrenTicket = b.WriteUOffsetVector(new[] { valuesFlatTicket, chunkedTicket });
            var dictVt = b.WriteUInt16s(new ushort[] { 14, 22, 20, 8, 4, 16, 0 });
            columnTickets[c] = b.StartTable(alignment: 8, inlineSize: 22)
                .EmitU16(dictEncodingIdx)
                .EmitUOffset(dictChildrenTicket)
                .EmitU64(totalRows)
                .EmitUOffset(dictMetaTicket)
                .EmitSOffsetTo(dictVt);
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
    /// Like <see cref="SerializeStructDictMixed"/> but additionally wraps
    /// each column's data layout in <c>vortex.stats(data, zones-flat)</c>.
    /// Co-exists dict-layout sharing with zone-pruning predicates: dict
    /// columns get <c>vortex.stats(vortex.dict(values, codes-chunked), zones)</c>;
    /// non-dict columns get <c>vortex.stats(vortex.chunked(...), zones)</c>.
    /// The reader's <see cref="LayoutPlanner.WithZoneInfo"/> already attaches
    /// <see cref="ZoneInfo"/> to either plan shape, so no reader changes
    /// were needed.
    /// </summary>
    public static byte[] SerializeStructDictMixedStats(
        ushort structEncodingIdx,
        ushort statsEncodingIdx,
        ushort dictEncodingIdx,
        ushort chunkedEncodingIdx,
        ushort flatEncodingIdx,
        ulong totalRows,
        int zoneLen,
        int numZones,
        IReadOnlyList<ulong> perBatchRowCount,
        uint[][] perColumnSegmentIdx,
        VortexFileWriter.DictColumnInfo[] dictColumns,
        IReadOnlyList<uint> perColumnZonesSegmentIdx,
        IReadOnlyList<byte[]> perColumnStatsBitset)
    {
        if (perColumnSegmentIdx.Length != dictColumns.Length)
            throw new ArgumentException(
                "perColumnSegmentIdx must align with dictColumns.", nameof(dictColumns));
        if (perColumnZonesSegmentIdx.Count != perColumnSegmentIdx.Length)
            throw new ArgumentException(
                "perColumnZonesSegmentIdx must have one entry per column.",
                nameof(perColumnZonesSegmentIdx));
        if (perColumnStatsBitset.Count != perColumnSegmentIdx.Length)
            throw new ArgumentException(
                "perColumnStatsBitset must have one entry per column.",
                nameof(perColumnStatsBitset));
        int batchCount = perBatchRowCount.Count;

        var b = new BackwardsFlatBufferBuilder();

        // Per-column stats metadata: [u32 zone_len LE][bitset bytes].
        var columnMetadataTickets = new int[perColumnSegmentIdx.Length];
        for (int c = 0; c < columnMetadataTickets.Length; c++)
        {
            var bitset = perColumnStatsBitset[c];
            var meta = new byte[4 + bitset.Length];
            meta[0] = (byte)(zoneLen & 0xFF);
            meta[1] = (byte)((zoneLen >> 8) & 0xFF);
            meta[2] = (byte)((zoneLen >> 16) & 0xFF);
            meta[3] = (byte)((zoneLen >> 24) & 0xFF);
            bitset.CopyTo(meta, 4);
            columnMetadataTickets[c] = b.WriteByteVector(meta);
        }

        Span<byte> dictMetaBuf = stackalloc byte[16];
        var columnTickets = new int[perColumnSegmentIdx.Length];
        for (int c = 0; c < perColumnSegmentIdx.Length; c++)
        {
            // Build chunked layout for the per-batch segments (codes for
            // dict columns, raw data for non-dict).
            var flatTickets = new int[batchCount];
            for (int batch = 0; batch < batchCount; batch++)
                flatTickets[batch] = EmitFlatLayout(
                    b, flatEncodingIdx, perBatchRowCount[batch], perColumnSegmentIdx[c][batch]);
            var chunkedChildrenTicket = b.WriteUOffsetVector(flatTickets);
            var chunkedVt = b.WriteUInt16s(new ushort[] { 12, 18, 16, 8, 0, 4 });
            int chunkedTicket = b.StartTable(alignment: 8, inlineSize: 18)
                .EmitU16(chunkedEncodingIdx)
                .EmitU64(totalRows)
                .EmitUOffset(chunkedChildrenTicket)
                .EmitSOffsetTo(chunkedVt);

            // Possibly wrap in vortex.dict.
            int dataLayoutTicket;
            if (dictColumns[c].ValuesSegmentIdx == 0 && dictColumns[c].DictRowCount == 0)
            {
                dataLayoutTicket = chunkedTicket;
            }
            else
            {
                int valuesFlatTicket = EmitFlatLayout(
                    b, flatEncodingIdx, dictColumns[c].DictRowCount, dictColumns[c].ValuesSegmentIdx);

                int metaPos = 0;
                dictMetaBuf[metaPos++] = 0x08;
                metaPos += Varint.WriteUnsigned(dictMetaBuf.Slice(metaPos), dictColumns[c].CodesPtype);
                dictMetaBuf[metaPos++] = 0x10;
                dictMetaBuf[metaPos++] = (byte)(dictColumns[c].IsNullableCodes ? 1 : 0);
                int dictMetaTicket = b.WriteByteVector(dictMetaBuf.Slice(0, metaPos).ToArray());

                var dictChildrenTicket = b.WriteUOffsetVector(new[] { valuesFlatTicket, chunkedTicket });
                var dictVt = b.WriteUInt16s(new ushort[] { 14, 22, 20, 8, 4, 16, 0 });
                dataLayoutTicket = b.StartTable(alignment: 8, inlineSize: 22)
                    .EmitU16(dictEncodingIdx)
                    .EmitUOffset(dictChildrenTicket)
                    .EmitU64(totalRows)
                    .EmitUOffset(dictMetaTicket)
                    .EmitSOffsetTo(dictVt);
            }

            // Zones layout (vortex.flat with row_count = numZones).
            int zonesLayoutTicket = EmitFlatLayout(
                b, flatEncodingIdx, (ulong)numZones, perColumnZonesSegmentIdx[c]);

            // Wrap data + zones in vortex.stats.
            var statsChildrenTicket = b.WriteUOffsetVector(new[] { dataLayoutTicket, zonesLayoutTicket });
            var statsVt = b.WriteUInt16s(new ushort[] { 14, 22, 20, 8, 4, 16, 0 });
            columnTickets[c] = b.StartTable(alignment: 8, inlineSize: 22)
                .EmitU16(statsEncodingIdx)
                .EmitUOffset(statsChildrenTicket)
                .EmitU64(totalRows)
                .EmitUOffset(columnMetadataTickets[c])
                .EmitSOffsetTo(statsVt);
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
