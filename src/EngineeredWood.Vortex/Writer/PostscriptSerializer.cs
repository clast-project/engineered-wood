// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Vortex.FlatBuffers;

namespace EngineeredWood.Vortex.Writer;

/// <summary>
/// Builds the <c>Postscript</c> FlatBuffer (footer.fbs): four optional pointers
/// to the postscript-resident segments DType, Layout, Statistics, Footer.
/// Phase 1 omits Statistics. Per upstream <c>vortex-file/src/footer/postscript.rs</c>,
/// the <c>_compression</c> and <c>_encryption</c> nested tables on each
/// <c>PostscriptSegment</c> are always None (reserved for future use).
/// </summary>
internal static class PostscriptSerializer
{
    public static byte[] Serialize(PostscriptBlock dtype, PostscriptBlock layout, PostscriptBlock footer)
    {
        var b = new BackwardsFlatBufferBuilder();

        // Build each PostscriptSegment table.
        var dtypeTicket = EmitPsSegment(b, dtype);
        var layoutTicket = EmitPsSegment(b, layout);
        var footerTicket = EmitPsSegment(b, footer);

        // Postscript root table — only uoffsets so 4-byte alignment suffices.
        //   slots: 0 (dtype), 1 (layout), 3 (footer). Slot 2 (statistics) absent.
        //   inline (16): soffset(4) + dtype_uoff(4@4) + layout_uoff(4@8) + footer_uoff(4@12)
        //   vt: vt_size = 4 + 4*2 = 12, slot0=4, slot1=8, slot2=0, slot3=12
        var psVt = b.WriteUInt16s(new ushort[] { 12, 16, 4, 8, 0, 12 });
        var psTicket = b.StartTable(alignment: 4, inlineSize: 16)
            .EmitUOffset(footerTicket)
            .EmitUOffset(layoutTicket)
            .EmitUOffset(dtypeTicket)
            .EmitSOffsetTo(psVt);

        return b.Finish(psTicket);
    }

    /// <summary>
    /// PostscriptSegment table — FB-canonical layout for the u64 offset:
    ///   inline (21): soffset(0..4) + pad(4..8) + offset_u64(8..16) + length_u32(16..20) + align_exp_u8(20..21)
    ///   vt: vt_size = 4 + 3*2 = 10, inline_size = 21, slot0=8, slot1=16, slot2=20.
    ///   Slots 3 (_compression) and 4 (_encryption) are trailing-absent (none).
    /// Pre-Preps to alignment 8 with the actual table size so the soffset_t lands
    /// at a position where offset+8 is 8-aligned.
    /// </summary>
    private static int EmitPsSegment(BackwardsFlatBufferBuilder b, PostscriptBlock block)
    {
        var psSegVt = b.WriteUInt16s(new ushort[] { 10, 21, 8, 16, 20 });
        return b.StartTable(alignment: 8, inlineSize: 21)
            .EmitU8(block.AlignmentExponent)   // byte 20
            .EmitU32(block.Length)             // bytes 16..20
            .EmitU64(block.Offset)             // bytes 8..16
            .EmitPad(4)                        // bytes 4..8 (canonical-layout gap before soffset)
            .EmitSOffsetTo(psSegVt);           // bytes 0..4
    }
}
