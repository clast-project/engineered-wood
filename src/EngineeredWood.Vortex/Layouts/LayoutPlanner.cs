// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// Walks a materialized layout tree against the Arrow schema to produce one
/// <see cref="ColumnPlan"/> per top-level Arrow field. Handles
/// <c>vortex.struct</c> at the root — or any other layout holding whole rows, whose fields become
/// <see cref="StructFieldColumnPlan"/>s — then per-field: <c>vortex.stats</c> and
/// <c>vortex.zoned</c> (skip-to-data), <c>vortex.chunked</c> (concatenated row chunks),
/// <c>vortex.flat</c> (leaf segment), and <c>vortex.dict</c> (a layout-level
/// dictionary with values + codes children → <see cref="DictColumnPlan"/>).
///
/// <para>Phase 1 scope: top-level primitive-or-utf8/binary fields. Composite
/// top-level fields (Struct of Struct, List, etc.) need richer plan structures.</para>
/// </summary>
internal static class LayoutPlanner
{
    public static ColumnPlan[] Plan(Apache.Arrow.Schema schema, VortexLayout root)
    {
        if (root.EncodingId != VortexLayoutEncodings.Struct)
        {
            // The root struct is stored whole (upstream's flat layout strategy writes it into a
            // single vortex.flat segment), so each field is projected out of the decoded rows.
            // A zone map over whole rows says nothing per field, so none is carried over.
            var rows = PlanField(new StructType(schema.FieldsList), root);
            var fields = new ColumnPlan[schema.FieldsList.Count];
            for (int i = 0; i < fields.Length; i++)
                fields[i] = new StructFieldColumnPlan(schema.FieldsList[i].DataType, rows, i);
            return fields;
        }
        if (root.Children.Count != schema.FieldsList.Count)
            throw new VortexFormatException(
                $"Layout has {root.Children.Count} children but schema has {schema.FieldsList.Count} fields.");

        var plans = new ColumnPlan[schema.FieldsList.Count];
        for (int i = 0; i < plans.Length; i++)
            plans[i] = PlanField(schema.FieldsList[i].DataType, root.Children[i]);
        return plans;
    }

    /// <summary>Build a plan for a single field given its expected Arrow type.</summary>
    private static ColumnPlan PlanField(IArrowType arrowType, VortexLayout layout)
    {
        switch (layout.EncodingId)
        {
            case VortexLayoutEncodings.Stats:
            case VortexLayoutEncodings.Zoned:
                {
                    // Both zoned layouts have two children: child[0] is the
                    // transparent data layout, child[1] is the auxiliary
                    // zones-table flat segment. Their metadata differs:
                    // vortex.stats carries zone_len + a present_stats bitset,
                    // vortex.zoned carries zone_len + aggregate specs. We
                    // capture it so the reader can materialize per-zone
                    // stats and drive Predicate-based pruning on demand.
                    if (layout.Children.Count != 2)
                        throw new VortexFormatException(
                            $"{layout.EncodingId} layout must have 2 children (data, zones), got {layout.Children.Count}.");
                    var dataPlan = PlanField(arrowType, layout.Children[0]);
                    var zoneInfo = layout.EncodingId == VortexLayoutEncodings.Stats
                        ? TryParseStatsLayout(layout)
                        : TryParseZonedLayout(arrowType, layout);
                    if (zoneInfo is not null)
                        dataPlan = WithZoneInfo(dataPlan, zoneInfo);
                    return dataPlan;
                }

            case VortexLayoutEncodings.Dictionary:
                {
                    if (layout.Children.Count != 2)
                        throw new VortexFormatException(
                            $"vortex.dict layout must have exactly 2 children (values, codes), got {layout.Children.Count}.");
                    // Per upstream: child[0] = values, child[1] = codes.
                    var codesArrowType = ResolveDictCodesArrowType(layout.Metadata);
                    var valuesPlan = PlanField(arrowType, layout.Children[0]);
                    var codesPlan = PlanField(codesArrowType, layout.Children[1]);
                    return new DictColumnPlan(arrowType, valuesPlan, codesPlan);
                }

            case VortexLayoutEncodings.Chunked:
                {
                    var chunks = new List<SegmentChunk>();
                    foreach (var child in layout.Children)
                        CollectFlatChunks(child, chunks);
                    return new FlatColumnPlan(arrowType, chunks);
                }

            case VortexLayoutEncodings.Flat:
                {
                    if (layout.SegmentRefs.Count != 1)
                        throw new VortexFormatException(
                            $"vortex.flat layout must have exactly 1 segment ref, got {layout.SegmentRefs.Count}.");
                    var chunks = new[] { new SegmentChunk(layout.SegmentRefs[0], layout.RowCount) };
                    return new FlatColumnPlan(arrowType, chunks);
                }

            default:
                throw new NotSupportedException(
                    $"Layout encoding '{layout.EncodingId}' is not yet supported by the planner. " +
                    "Add a case and a fixture that exercises it.");
        }
    }

    /// <summary>
    /// Parses a <c>vortex.stats</c> layout: extracts <c>zone_len</c> +
    /// <c>present_stats</c> from the metadata and pulls the zones segment
    /// ref out of children[1] (which we expect to be a <c>vortex.flat</c>
    /// pointing at a single segment). Returns null when the metadata is
    /// malformed or zone_len is 0 (legacy / pruning-disabled marker).
    /// </summary>
    private static ZoneInfo? TryParseStatsLayout(VortexLayout layout)
    {
        var meta = layout.Metadata;
        if (meta.Length < 4) return null;
        // u32 LE zone_len at bytes 0..3.
        int zoneLen =
            meta[0] |
            (meta[1] << 8) |
            (meta[2] << 16) |
            (meta[3] << 24);
        if (zoneLen <= 0) return null;

        var presentStats = ParseStatBitset(meta, 4);

        if (!TryGetZonesSegment(layout, out var zonesSegmentRef, out var zoneCount)) return null;
        return new ZoneInfo(zoneLen, presentStats, zonesSegmentRef, zoneCount);
    }

    /// <summary>
    /// Parses a <c>vortex.zoned</c> layout (see <see cref="ZonedZoneMap"/>). Returns null, so
    /// the column is read without pruning, when the metadata version or an aggregate is one
    /// this reader doesn't know, or when zone_len is 0.
    /// </summary>
    private static ZoneInfo? TryParseZonedLayout(IArrowType arrowType, VortexLayout layout)
    {
        if (!ZonedZoneMap.TryParseMetadata(layout.Metadata, out var zoneLen, out var aggregates))
            return null;
        if (zoneLen <= 0) return null;
        if (ZonedZoneMap.BuildStructType(arrowType, aggregates) is null) return null;
        if (!TryGetZonesSegment(layout, out var zonesSegmentRef, out var zoneCount)) return null;
        return new ZoneInfo(zoneLen, aggregates, zonesSegmentRef, zoneCount);
    }

    /// <summary>
    /// The zones table (child[1]) is expected to be a <c>vortex.flat</c> with one segment.
    /// </summary>
    private static bool TryGetZonesSegment(VortexLayout layout, out uint segmentRef, out int zoneCount)
    {
        segmentRef = 0;
        zoneCount = 0;
        var zonesLayout = layout.Children[1];
        if (zonesLayout.EncodingId != VortexLayoutEncodings.Flat) return false;
        if (zonesLayout.SegmentRefs.Count != 1) return false;
        segmentRef = zonesLayout.SegmentRefs[0];
        zoneCount = checked((int)zonesLayout.RowCount);
        return true;
    }

    /// <summary>
    /// Returns the sorted Stat enum values whose bits are set in
    /// <paramref name="bytes"/> starting at <paramref name="offset"/>. The
    /// bitset shape matches upstream's <c>as_stat_bitset_bytes</c>: 9-bit
    /// packed field covering Stats 0..8.
    /// </summary>
    private static IReadOnlyList<int> ParseStatBitset(byte[] bytes, int offset)
    {
        var stats = new List<int>();
        int totalBits = (bytes.Length - offset) * 8;
        // Stat enum has values 0..8; ignore higher indices we don't recognize.
        int maxStat = 8;
        for (int i = 0; i <= maxStat && i < totalBits; i++)
        {
            int byteIdx = offset + (i >> 3);
            int bitIdx = i & 7;
            if ((bytes[byteIdx] & (1 << bitIdx)) != 0)
                stats.Add(i);
        }
        return stats;
    }

    /// <summary>
    /// Returns a copy of <paramref name="plan"/> with <see cref="ColumnPlan.ZoneInfo"/>
    /// populated. Concrete types: <see cref="FlatColumnPlan"/> / <see cref="DictColumnPlan"/>.
    /// </summary>
    private static ColumnPlan WithZoneInfo(ColumnPlan plan, ZoneInfo zoneInfo) => plan switch
    {
        FlatColumnPlan f => new FlatColumnPlan(f.ArrowType, f.Chunks) { ZoneInfo = zoneInfo },
        DictColumnPlan d => new DictColumnPlan(d.ArrowType, d.Values, d.Codes) { ZoneInfo = zoneInfo },
        _ => plan,
    };

    /// <summary>
    /// Parses <c>DictLayoutMetadata</c> (vortex-layout/src/layouts/dict/mod.rs)
    /// to find the codes ptype. The proto schema is:
    ///   field 1 (varint): codes_ptype enum (PType: U8=0, U16=1, U32=2, U64=3, I8=4, ...)
    ///   field 2 (varint): is_nullable_codes (optional bool)
    ///   field 3 (varint): all_values_referenced (optional bool)
    /// We only care about field 1 today.
    /// </summary>
    private static IArrowType ResolveDictCodesArrowType(byte[] metadata)
    {
        // Proto3 enum default = 0 (PType.U8). If metadata is empty or field 1
        // is absent, codes are u8.
        int pos = 0;
        int ptype = 0;
        var span = metadata.AsSpan();
        while (pos < span.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(span, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
            {
                ptype = (int)Varint.ReadUnsigned(span, ref pos);
                break;
            }
            switch (wireType)
            {
                case 0: Varint.ReadUnsigned(span, ref pos); break;
                case 1: pos += 8; break;
                case 2:
                    {
                        var len = (int)Varint.ReadUnsigned(span, ref pos);
                        pos += len;
                        break;
                    }
                case 5: pos += 4; break;
                default:
                    throw new VortexFormatException(
                        $"Unsupported wire type {wireType} in DictLayoutMetadata.");
            }
        }

        // PType enum values: U8=0, U16=1, U32=2, U64=3, I8=4, I16=5, I32=6, I64=7
        return ptype switch
        {
            0 => UInt8Type.Default,
            1 => UInt16Type.Default,
            2 => UInt32Type.Default,
            3 => UInt64Type.Default,
            4 => Int8Type.Default,
            5 => Int16Type.Default,
            6 => Int32Type.Default,
            7 => Int64Type.Default,
            _ => throw new VortexFormatException(
                $"vortex.dict codes_ptype {ptype} is not an integer ptype."),
        };
    }

    /// <summary>
    /// Used inside <c>vortex.chunked</c>: each child is expected to resolve to
    /// flat segment chunks. Recurses through nested stats/chunked wrappers.
    /// </summary>
    private static void CollectFlatChunks(VortexLayout layout, List<SegmentChunk> sink)
    {
        switch (layout.EncodingId)
        {
            case VortexLayoutEncodings.Flat:
                if (layout.SegmentRefs.Count != 1)
                    throw new VortexFormatException(
                        $"vortex.flat layout must have exactly 1 segment ref, got {layout.SegmentRefs.Count}.");
                sink.Add(new SegmentChunk(layout.SegmentRefs[0], layout.RowCount));
                break;

            case VortexLayoutEncodings.Stats:
            case VortexLayoutEncodings.Zoned:
                if (layout.Children.Count < 1)
                    throw new VortexFormatException($"{layout.EncodingId} layout has no children.");
                CollectFlatChunks(layout.Children[0], sink);
                break;

            case VortexLayoutEncodings.Chunked:
                foreach (var child in layout.Children)
                    CollectFlatChunks(child, sink);
                break;

            default:
                throw new NotSupportedException(
                    $"Inside chunked: layout encoding '{layout.EncodingId}' is not yet supported. " +
                    "Add a case and a fixture that exercises it.");
        }
    }
}
