// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// Walks a materialized layout tree against the Arrow schema to produce one
/// <see cref="ColumnPlan"/> per top-level Arrow field. Handles
/// <c>vortex.struct</c> at the root, then per-field: <c>vortex.stats</c>
/// (skip-to-data), <c>vortex.chunked</c> (concatenated row chunks),
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
            throw new VortexFormatException(
                $"Expected root layout encoding '{VortexLayoutEncodings.Struct}', got '{root.EncodingId}'.");
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
                // Skip the stats wrapper; child[0] is the actual data layout.
                if (layout.Children.Count < 1)
                    throw new VortexFormatException("vortex.stats layout has no children.");
                return PlanField(arrowType, layout.Children[0]);

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
                if (layout.Children.Count < 1)
                    throw new VortexFormatException("vortex.stats layout has no children.");
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
