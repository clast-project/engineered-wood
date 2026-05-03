// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.FlatBuffers;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Writer;

/// <summary>
/// Builds the <c>Footer</c> FlatBuffer (footer.fbs): the dictionary-encoded
/// registries (array_specs, layout_specs, segment_specs) referenced by the
/// per-array, per-layout, and per-segment indices throughout the file.
///
/// <para>Phase 1 omits <c>compression_specs</c> and <c>encryption_specs</c> —
/// segment compression isn't emitted (vortex's writer doesn't either) and
/// encryption is unsupported. Both vectors read as empty downstream because
/// trailing absent slots are valid in FlatBuffers.</para>
/// </summary>
internal static class FooterSerializer
{
    public static byte[] Serialize(
        IReadOnlyList<string> arraySpecs,
        IReadOnlyList<string> layoutSpecs,
        IReadOnlyList<SegmentLocator> segmentSpecs)
    {
        var b = new BackwardsFlatBufferBuilder();

        // 1. ArraySpec tables (each: { id: string }).
        var arraySpecTickets = new int[arraySpecs.Count];
        for (int i = 0; i < arraySpecs.Count; i++)
            arraySpecTickets[i] = EmitSpec(b, arraySpecs[i]);
        var arraySpecsVecTicket = b.WriteUOffsetVector(arraySpecTickets);

        // 2. LayoutSpec tables (each: { id: string }).
        var layoutSpecTickets = new int[layoutSpecs.Count];
        for (int i = 0; i < layoutSpecs.Count; i++)
            layoutSpecTickets[i] = EmitSpec(b, layoutSpecs[i]);
        var layoutSpecsVecTicket = b.WriteUOffsetVector(layoutSpecTickets);

        // 3. SegmentSpec inline-struct vector (16 bytes per struct):
        //    offset u64@0 + length u32@8 + align_exp u8@12 + _compression u8@13 + _encryption u16@14.
        // Inline structs have no internal padding — each struct is exactly 16 bytes
        // and they pack tightly. The struct's natural alignment is 8 (largest
        // field). Pre-Prep so the count(u32) + structs land at correct alignment:
        // after writing 4 + 16N bytes, _used must be 8-aligned (so the last-written
        // struct, at the start of the vector, is 8-aligned).
        b.Prep(8, segmentSpecs.Count * 16 + 4);
        for (int i = segmentSpecs.Count - 1; i >= 0; i--)
        {
            // Backwards-build per struct: encryption(2) → compression(1) → align(1) → length(4) → offset(8).
            // Raw writes — alignment within a struct is implicit (tightly packed).
            b.WriteU16(0); // _encryption
            b.WriteByte(0); // _compression (index 0 = uncompressed)
            b.WriteByte(segmentSpecs[i].AlignmentExponent);
            b.WriteU32(segmentSpecs[i].Length);
            b.WriteU64(segmentSpecs[i].Offset);
        }
        b.WriteU32(checked((uint)segmentSpecs.Count));
        var segSpecsVecTicket = b.CurrentTicket();

        // 4. Footer table:
        //    slots populated: 0 (array_specs), 1 (layout_specs), 2 (segment_specs).
        //    Slots 3 (compression_specs) and 4 (encryption_specs) are trailing-absent.
        //    inline (16): soffset(4) + array_specs(4@4) + layout_specs(4@8) + segment_specs(4@12).
        //    Only uoffsets, so 4-byte alignment.
        var footerVt = b.WriteUInt16s(new ushort[] { 10, 16, 4, 8, 12 });
        var footerTicket = b.StartTable(alignment: 4, inlineSize: 16)
            .EmitUOffset(segSpecsVecTicket)
            .EmitUOffset(layoutSpecsVecTicket)
            .EmitUOffset(arraySpecsVecTicket)
            .EmitSOffsetTo(footerVt);

        return b.Finish(footerTicket);
    }

    /// <summary>
    /// Emits an ArraySpec or LayoutSpec table (same shape: one string field).
    /// vt_size=6, inline_size=8 (soffset + id_uoffset). Returns the table ticket.
    /// </summary>
    private static int EmitSpec(BackwardsFlatBufferBuilder b, string id)
    {
        var idTicket = b.WriteString(id);
        var specVt = b.WriteUInt16s(new ushort[] { 6, 8, 4 });
        return b.StartTable(alignment: 4, inlineSize: 8)
            .EmitUOffset(idTicket)
            .EmitSOffsetTo(specVt);
    }
}
