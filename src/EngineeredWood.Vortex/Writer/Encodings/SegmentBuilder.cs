// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Vortex.FlatBuffers;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Accumulates the pieces of a single Vortex segment (one column's encoded
/// data): buffer payloads + per-buffer descriptors + the recursive ArrayNode
/// subtree. Encoders register buffers (returning indices used in
/// <c>buffer_indices</c>) and emit ArrayNode tables into the shared
/// <see cref="BackwardsFlatBufferBuilder"/>; <see cref="FinishSegment"/> wraps
/// everything in an ArrayMessage and produces the segment bytes.
/// </summary>
internal sealed class SegmentBuilder
{
    private readonly BackwardsFlatBufferBuilder _b = new();
    private readonly List<byte[]> _payloads = new();
    private readonly List<BufferEntry> _entries = new();

    /// <summary>The shared FB builder. Encoders write their ArrayNode subtree directly into it.</summary>
    public BackwardsFlatBufferBuilder Builder => _b;

    /// <summary>
    /// Registers a buffer with payload bytes. Returns the buffer's index for
    /// use in <c>ArrayNode.buffer_indices</c>. Indices are assigned in
    /// registration order.
    /// </summary>
    public ushort AddBuffer(byte[] payload, byte alignmentExponent)
    {
        _entries.Add(new BufferEntry(0, alignmentExponent, BufferCompression.None, (uint)payload.Length));
        _payloads.Add(payload);
        return checked((ushort)(_entries.Count - 1));
    }

    /// <summary>
    /// Wraps <paramref name="rootArrayNodeTicket"/> in an ArrayMessage, emits
    /// the buffers descriptor vector, and produces the full segment bytes
    /// (concatenated payloads + Array FB + u32 fb_length).
    /// </summary>
    public byte[] FinishSegment(int rootArrayNodeTicket)
    {
        // Buffers vector: [count: u32][BufferDescriptor × N] inline-struct array.
        // Each BufferDescriptor (8 bytes) layout:
        //   padding u16@0 + align_exp u8@2 + compression u8@3 + length u32@4
        // Backwards-build per struct (last addr first): length(4) → compression(1) → align(1) → padding(2).
        // Struct alignment = 4 (largest field is u32 length). Pre-Prep so that
        // count(u32) + N structs lands at 4-aligned position (each struct is 8
        // bytes, so consecutive structs remain 4-aligned automatically).
        _b.Prep(4, _entries.Count * 8 + 4);
        for (int i = _entries.Count - 1; i >= 0; i--)
        {
            var e = _entries[i];
            _b.WriteU32(e.Length);
            _b.WriteByte((byte)e.Compression);
            _b.WriteByte(e.AlignmentExponent);
            _b.WriteU16(e.Padding);
        }
        _b.WriteU32(checked((uint)_entries.Count));
        var buffersVecTicket = _b.CurrentTicket();

        // ArrayMessage table:
        //   vt: vt_size=8, inline_size=12, slot0(root_node)@4, slot1(buffers)@8.
        //   inline (12): soffset(4) + root_uoff(4@4) + buffers_uoff(4@8).
        var msgVt = _b.WriteUInt16s(new ushort[] { 8, 12, 4, 8 });
        var msgTicket = _b.StartTable(alignment: 4, inlineSize: 12)
            .EmitUOffset(buffersVecTicket)
            .EmitUOffset(rootArrayNodeTicket)
            .EmitSOffsetTo(msgVt);

        var fbBytes = _b.Finish(msgTicket);

        // Segment: [buffer_payloads…][Array FB][u32 fb_length LE]
        // Per-buffer padding stays 0 in our MVP; payloads concatenate directly.
        int payloadLen = 0;
        for (int i = 0; i < _payloads.Count; i++) payloadLen += _payloads[i].Length;
        var segment = new byte[payloadLen + fbBytes.Length + 4];
        int pos = 0;
        for (int i = 0; i < _payloads.Count; i++)
        {
            Buffer.BlockCopy(_payloads[i], 0, segment, pos, _payloads[i].Length);
            pos += _payloads[i].Length;
        }
        Buffer.BlockCopy(fbBytes, 0, segment, pos, fbBytes.Length);
        pos += fbBytes.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(segment.AsSpan(pos), (uint)fbBytes.Length);
        return segment;
    }

    private readonly record struct BufferEntry(
        ushort Padding, byte AlignmentExponent, BufferCompression Compression, uint Length);
}

/// <summary>
/// Reusable helpers for emitting ArrayNode FB tables from within an encoder.
/// Centralizes vtable layouts so primitive/varbin/list/fsl encoders can share
/// the few common shapes.
/// </summary>
internal static class ArrayNodeEmitter
{
    /// <summary>
    /// Emits ArrayNode { encoding, buffer_indices=[bufIdx] } — slots 0 + 3.
    /// vt_size=12, inline=10, slot0(encoding u16)@8, slot3(buffer_indices uoff)@4.
    /// </summary>
    public static int EmitWithSingleBuffer(BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort bufIdx)
    {
        var bufIdxTicket = b.WriteUInt16Vector(new ushort[] { bufIdx });
        var nodeVt = b.WriteUInt16s(new ushort[] { 12, 10, 8, 0, 0, 4 });
        return b.StartTable(alignment: 4, inlineSize: 10)
            .EmitU16(encodingIdx)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, buffer_indices=[bufIdx], children=[...] } — slots 0+2+3.
    /// vt_size=12, inline=14. slot0=12, slot2=8, slot3=4.
    /// </summary>
    public static int EmitWithBufferAndChildren(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort bufIdx, int[] childTickets)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var bufIdxTicket = b.WriteUInt16Vector(new ushort[] { bufIdx });
        var nodeVt = b.WriteUInt16s(new ushort[] { 12, 14, 12, 0, 8, 4 });
        return b.StartTable(alignment: 4, inlineSize: 14)
            .EmitU16(encodingIdx)
            .EmitUOffset(childrenVecTicket)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, children=[...] } — slots 0+2 only.
    /// vt_size=8, inline=6. Used by vortex.fixed_size_list (no buffers, just element + validity children).
    /// inline (6): soffset(4) + children_uoff(4@4) + … wait, inline_size is the field-storage size.
    /// We pack: soffset(4) at 0..3, children_uoff at 4..7, encoding(2) at 8..9. inline=10.
    /// vt: vt_size = 4 + 3*2 = 10 (header + slots 0,1,2). slot0=8, slot1=0, slot2=4.
    /// </summary>
    public static int EmitWithChildrenOnly(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, int[] childTickets)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var nodeVt = b.WriteUInt16s(new ushort[] { 10, 10, 8, 0, 4 });
        return b.StartTable(alignment: 4, inlineSize: 10)
            .EmitU16(encodingIdx)
            .EmitUOffset(childrenVecTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, metadata, children=[...] } — slots 0+1+2.
    /// Used by vortex.list (no own buffers; metadata + children).
    /// inline (14): soffset(4) + metadata_uoff(4@4) + children_uoff(4@8) + encoding(2@12).
    /// vt: vt_size = 4 + 3*2 = 10. slot0=12, slot1=4, slot2=8.
    /// </summary>
    public static int EmitWithMetadataAndChildren(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, int metadataTicket, int[] childTickets)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var nodeVt = b.WriteUInt16s(new ushort[] { 10, 14, 12, 4, 8 });
        return b.StartTable(alignment: 4, inlineSize: 14)
            .EmitU16(encodingIdx)
            .EmitUOffset(childrenVecTicket)
            .EmitUOffset(metadataTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, buffer_indices, stats } — slots 0+3+4.
    /// inline (14): soffset(4) + buffer_indices_uoff(4@4) + stats_uoff(4@8) + encoding(2@12).
    /// vt: vt_size = 4 + 5*2 = 14. slot offsets {0:12, 1:0, 2:0, 3:4, 4:8}.
    /// </summary>
    public static int EmitWithSingleBufferAndStats(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort bufIdx, int statsTicket)
    {
        var bufIdxTicket = b.WriteUInt16Vector(new ushort[] { bufIdx });
        var nodeVt = b.WriteUInt16s(new ushort[] { 14, 14, 12, 0, 0, 4, 8 });
        return b.StartTable(alignment: 4, inlineSize: 14)
            .EmitU16(encodingIdx)
            .EmitUOffset(statsTicket)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, children, buffer_indices, stats } — slots 0+2+3+4.
    /// inline (18): soffset(4) + buffer_indices_uoff(4@4) + children_uoff(4@8) + stats_uoff(4@12) + encoding(2@16).
    /// vt: vt_size = 4 + 5*2 = 14. slot offsets {0:16, 1:0, 2:8, 3:4, 4:12}.
    /// </summary>
    public static int EmitWithBufferChildrenAndStats(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort bufIdx, int[] childTickets, int statsTicket)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var bufIdxTicket = b.WriteUInt16Vector(new ushort[] { bufIdx });
        var nodeVt = b.WriteUInt16s(new ushort[] { 14, 18, 16, 0, 8, 4, 12 });
        return b.StartTable(alignment: 4, inlineSize: 18)
            .EmitU16(encodingIdx)
            .EmitUOffset(statsTicket)
            .EmitUOffset(childrenVecTicket)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, children=[...], stats } — slots 0+2+4.
    /// Used by FSL with stats (no own buffers).
    /// inline (14): soffset(4) + children_uoff(4@4) + stats_uoff(4@8) + encoding(2@12).
    /// vt: vt_size = 4 + 5*2 = 14. slot offsets {0:12, 1:0, 2:4, 3:0, 4:8}.
    /// </summary>
    public static int EmitWithChildrenAndStats(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, int[] childTickets, int statsTicket)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var nodeVt = b.WriteUInt16s(new ushort[] { 14, 14, 12, 0, 4, 0, 8 });
        return b.StartTable(alignment: 4, inlineSize: 14)
            .EmitU16(encodingIdx)
            .EmitUOffset(statsTicket)
            .EmitUOffset(childrenVecTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, metadata, children, stats } — slots 0+1+2+4.
    /// Used by vortex.list with stats. inline (18): soffset(4) + metadata_uoff(4@4) +
    /// children_uoff(4@8) + stats_uoff(4@12) + encoding(2@16).
    /// vt: vt_size = 4 + 5*2 = 14. slot offsets {0:16, 1:4, 2:8, 3:0, 4:12}.
    /// </summary>
    public static int EmitWithMetadataChildrenAndStats(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, int metadataTicket,
        int[] childTickets, int statsTicket)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var nodeVt = b.WriteUInt16s(new ushort[] { 14, 18, 16, 4, 8, 0, 12 });
        return b.StartTable(alignment: 4, inlineSize: 18)
            .EmitU16(encodingIdx)
            .EmitUOffset(statsTicket)
            .EmitUOffset(childrenVecTicket)
            .EmitUOffset(metadataTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, metadata, buffer_indices } — slots 0+1+3.
    /// Used by fastlanes.bitpacked (non-nullable, no patches): single packed
    /// buffer + bit_width metadata, no children.
    /// inline (14): soffset(4) + buffer_indices_uoff(4@4) + metadata_uoff(4@8) + encoding(2@12).
    /// vt: vt_size = 4 + 4*2 = 12. slot offsets {0:12, 1:8, 2:0, 3:4}.
    /// </summary>
    public static int EmitWithMetadataAndBuffer(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort bufIdx, int metadataTicket)
    {
        var bufIdxTicket = b.WriteUInt16Vector(new ushort[] { bufIdx });
        var nodeVt = b.WriteUInt16s(new ushort[] { 12, 14, 12, 8, 0, 4 });
        return b.StartTable(alignment: 4, inlineSize: 14)
            .EmitU16(encodingIdx)
            .EmitUOffset(metadataTicket)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, metadata, buffer_indices, stats } — slots 0+1+3+4.
    /// Same shape as <see cref="EmitWithMetadataAndBuffer"/> with the stats slot added.
    /// inline (18): soffset(4) + buffer_indices_uoff(4@4) + stats_uoff(4@8) + metadata_uoff(4@12) + encoding(2@16).
    /// vt: vt_size = 4 + 5*2 = 14. slot offsets {0:16, 1:12, 2:0, 3:4, 4:8}.
    /// </summary>
    public static int EmitWithMetadataBufferAndStats(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort bufIdx,
        int metadataTicket, int statsTicket)
    {
        var bufIdxTicket = b.WriteUInt16Vector(new ushort[] { bufIdx });
        var nodeVt = b.WriteUInt16s(new ushort[] { 14, 18, 16, 12, 0, 4, 8 });
        return b.StartTable(alignment: 4, inlineSize: 18)
            .EmitU16(encodingIdx)
            .EmitUOffset(metadataTicket)
            .EmitUOffset(statsTicket)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, metadata, children, buffer_indices } — slots 0+1+2+3.
    /// Used by vortex.varbin (offsets+validity children + bytes buffer + offsets-ptype
    /// metadata), fastlanes.bitpacked-with-validity (validity child + packed buffer +
    /// bit_width metadata), and vortex.decimal-with-validity (validity child + values
    /// buffer + values-type metadata).
    /// inline (18): soffset(4) + buffer_indices_uoff(4@4) + children_uoff(4@8) + metadata_uoff(4@12) + encoding(2@16).
    /// vt: vt_size = 4 + 4*2 = 12. slot offsets {0:16, 1:12, 2:8, 3:4}.
    /// </summary>
    public static int EmitWithMetadataBufferAndChildren(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort bufIdx,
        int metadataTicket, int[] childTickets)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var bufIdxTicket = b.WriteUInt16Vector(new ushort[] { bufIdx });
        var nodeVt = b.WriteUInt16s(new ushort[] { 12, 18, 16, 12, 8, 4 });
        return b.StartTable(alignment: 4, inlineSize: 18)
            .EmitU16(encodingIdx)
            .EmitUOffset(metadataTicket)
            .EmitUOffset(childrenVecTicket)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Emits ArrayNode { encoding, metadata, children, buffer_indices, stats } — slots 0+1+2+3+4.
    /// Same shape as <see cref="EmitWithMetadataBufferAndChildren"/> with stats slot added.
    /// inline (22): soffset(4) + buffer_indices(4@4) + children(4@8) + metadata(4@12) + stats(4@16) + encoding(2@20).
    /// vt: vt_size = 4 + 5*2 = 14. slot offsets {0:20, 1:12, 2:8, 3:4, 4:16}.
    /// </summary>
    public static int EmitWithMetadataBufferChildrenAndStats(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort bufIdx,
        int metadataTicket, int[] childTickets, int statsTicket)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var bufIdxTicket = b.WriteUInt16Vector(new ushort[] { bufIdx });
        var nodeVt = b.WriteUInt16s(new ushort[] { 14, 22, 20, 12, 8, 4, 16 });
        return b.StartTable(alignment: 4, inlineSize: 22)
            .EmitU16(encodingIdx)
            .EmitUOffset(statsTicket)
            .EmitUOffset(metadataTicket)
            .EmitUOffset(childrenVecTicket)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Slots 0+1+2+3 with a multi-entry buffer-indices vector. Same vtable
    /// shape as <see cref="EmitWithMetadataBufferAndChildren"/> — only the
    /// buffer-vector ticket's payload changes, not the inline layout.
    /// Used by <c>vortex.fsst</c> (3 buffers: symbols, symbol_lengths, codes).
    /// </summary>
    public static int EmitWithMetadataBuffersAndChildren(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort[] bufIdxs,
        int metadataTicket, int[] childTickets)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var bufIdxTicket = b.WriteUInt16Vector(bufIdxs);
        var nodeVt = b.WriteUInt16s(new ushort[] { 12, 18, 16, 12, 8, 4 });
        return b.StartTable(alignment: 4, inlineSize: 18)
            .EmitU16(encodingIdx)
            .EmitUOffset(metadataTicket)
            .EmitUOffset(childrenVecTicket)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }

    /// <summary>
    /// Slots 0+1+2+3+4 with a multi-entry buffer-indices vector and stats.
    /// Vtable shape matches <see cref="EmitWithMetadataBufferChildrenAndStats"/>.
    /// </summary>
    public static int EmitWithMetadataBuffersChildrenAndStats(
        BackwardsFlatBufferBuilder b, ushort encodingIdx, ushort[] bufIdxs,
        int metadataTicket, int[] childTickets, int statsTicket)
    {
        var childrenVecTicket = b.WriteUOffsetVector(childTickets);
        var bufIdxTicket = b.WriteUInt16Vector(bufIdxs);
        var nodeVt = b.WriteUInt16s(new ushort[] { 14, 22, 20, 12, 8, 4, 16 });
        return b.StartTable(alignment: 4, inlineSize: 22)
            .EmitU16(encodingIdx)
            .EmitUOffset(statsTicket)
            .EmitUOffset(metadataTicket)
            .EmitUOffset(childrenVecTicket)
            .EmitUOffset(bufIdxTicket)
            .EmitSOffsetTo(nodeVt);
    }
}

/// <summary>
/// Optional values for a per-array <c>ArrayStats</c> table. Set only the
/// fields the encoder has computed; <see cref="ArrayStatsEmitter.Emit"/>
/// emits slots only for non-null entries and returns null when nothing is set.
/// Sum (slot 4) absent — would need ScalarValue accumulation across rows.
/// </summary>
internal struct ArrayStatsValues
{
    public byte[]? MinBytes;        // slot 0 — protobuf-encoded ScalarValue bytes
    public byte? MinPrecision;      // slot 1 — Precision enum (0=Inexact, 1=Exact)
    public byte[]? MaxBytes;        // slot 2
    public byte? MaxPrecision;      // slot 3
    public byte[]? SumBytes;        // slot 4 — protobuf-encoded ScalarValue bytes
    public bool? IsSorted;          // slot 5
    public bool? IsStrictSorted;    // slot 6
    public bool? IsConstant;        // slot 7
    public ulong? NullCount;        // slot 8
    public ulong? UncompressedSizeInBytes; // slot 9
    public ulong? NanCount;         // slot 10

    public readonly bool IsEmpty =>
        MinBytes is null && MinPrecision is null
        && MaxBytes is null && MaxPrecision is null
        && SumBytes is null
        && IsSorted is null && IsStrictSorted is null && IsConstant is null
        && NullCount is null && UncompressedSizeInBytes is null && NanCount is null;
}

/// <summary>
/// Builds <c>ArrayStats</c> FlatBuffer tables (per <c>array.fbs</c>). Emits a
/// vtable that covers slots 0..max_populated, with absent slots set to 0.
/// Inline body packs populated fields in slot order after the soffset.
/// </summary>
internal static class ArrayStatsEmitter
{
    /// <summary>
    /// Emits an ArrayStats table for <paramref name="stats"/>; returns the
    /// table ticket, or null if no fields are populated.
    /// </summary>
    public static int? Emit(BackwardsFlatBufferBuilder b, in ArrayStatsValues stats)
    {
        if (stats.IsEmpty) return null;

        // 1. Emit byte-vector tickets for MinBytes/MaxBytes/SumBytes ahead of the table
        //    (FB byte vectors are referenced via uoffset_t from the table inline).
        int? minBytesTicket = stats.MinBytes is not null ? b.WriteByteVector(stats.MinBytes) : null;
        int? maxBytesTicket = stats.MaxBytes is not null ? b.WriteByteVector(stats.MaxBytes) : null;
        int? sumBytesTicket = stats.SumBytes is not null ? b.WriteByteVector(stats.SumBytes) : null;

        // 2. Collect populated fields with (slot, size, kind, value). Kind:
        //    0 = bool (1 byte), 1 = u8 (1 byte), 2 = u64 (8 bytes), 3 = uoffset (4 bytes).
        Span<FieldEntry> fields = stackalloc FieldEntry[11];
        int n = 0;
        if (minBytesTicket is { } mb) fields[n++] = new FieldEntry(0, 4, FieldKind.UOffset, 0, false, 0, mb);
        if (stats.MinPrecision is { } mp) fields[n++] = new FieldEntry(1, 1, FieldKind.U8, 0, false, mp, 0);
        if (maxBytesTicket is { } xb) fields[n++] = new FieldEntry(2, 4, FieldKind.UOffset, 0, false, 0, xb);
        if (stats.MaxPrecision is { } xp) fields[n++] = new FieldEntry(3, 1, FieldKind.U8, 0, false, xp, 0);
        if (sumBytesTicket is { } sb) fields[n++] = new FieldEntry(4, 4, FieldKind.UOffset, 0, false, 0, sb);
        if (stats.IsSorted is { } iss) fields[n++] = new FieldEntry(5, 1, FieldKind.Bool, 0, iss, 0, 0);
        if (stats.IsStrictSorted is { } isss) fields[n++] = new FieldEntry(6, 1, FieldKind.Bool, 0, isss, 0, 0);
        if (stats.IsConstant is { } ic) fields[n++] = new FieldEntry(7, 1, FieldKind.Bool, 0, ic, 0, 0);
        if (stats.NullCount is { } nc) fields[n++] = new FieldEntry(8, 8, FieldKind.U64, nc, false, 0, 0);
        if (stats.UncompressedSizeInBytes is { } usz) fields[n++] = new FieldEntry(9, 8, FieldKind.U64, usz, false, 0, 0);
        if (stats.NanCount is { } nan) fields[n++] = new FieldEntry(10, 8, FieldKind.U64, nan, false, 0, 0);
        var populated = fields.Slice(0, n);

        // 3. FB-canonical inline layout: place fields by descending alignment,
        //    breaking ties by ascending slot. u64 fields go first (offset 8 after
        //    a 4-byte gap from soffset_t at offset 0..4), then 4-byte uoffsets
        //    (tight), then 1-byte u8/bool fields. Pad 4 bytes at offset 4..8 only
        //    when at least one 8-byte field is present.
        int maxSlot = 0;
        for (int i = 0; i < populated.Length; i++)
            if (populated[i].Slot > maxSlot) maxSlot = populated[i].Slot;
        Span<ushort> slotOffsets = stackalloc ushort[maxSlot + 1];
        bool hasU64 = false;
        for (int i = 0; i < populated.Length; i++)
            if (populated[i].Size == 8) { hasU64 = true; break; }
        int tableAlign = hasU64 ? 8 : 4;
        int inlineOffset = hasU64 ? 8 : 4; // soffset(4) + optional pad(4) = first field offset
        int gapSize = inlineOffset - 4;    // 0 or 4 bytes of pad after soffset

        // Sort by descending size, ascending slot (within same size).
        Span<int> placementOrder = stackalloc int[populated.Length];
        for (int i = 0; i < populated.Length; i++) placementOrder[i] = i;
        // Simple insertion sort — placementOrder is at most 11 entries.
        for (int i = 1; i < placementOrder.Length; i++)
        {
            int key = placementOrder[i];
            int j = i - 1;
            while (j >= 0 && CompareForLayout(populated[placementOrder[j]], populated[key]) > 0)
            {
                placementOrder[j + 1] = placementOrder[j];
                j--;
            }
            placementOrder[j + 1] = key;
        }

        // Assign offsets in placement order.
        for (int i = 0; i < placementOrder.Length; i++)
        {
            var f = populated[placementOrder[i]];
            slotOffsets[f.Slot] = (ushort)inlineOffset;
            inlineOffset += f.Size;
        }
        int inlineSize = inlineOffset;

        // 4. Build vtable: [vt_size, inline_size, slot0..slotMax].
        var vtArr = new ushort[2 + (maxSlot + 1)];
        vtArr[0] = (ushort)(4 + (maxSlot + 1) * 2);
        vtArr[1] = (ushort)inlineSize;
        for (int s = 0; s <= maxSlot; s++) vtArr[2 + s] = slotOffsets[s];

        // 5. Write vtable, then start the table (StartTable Preps for table_pos
        //    alignment matching the max-field alignment). Inline body is written
        //    in REVERSE placement order (highest forward offset first in time =
        //    lowest mem first).
        var vtTicket = b.WriteUInt16s(vtArr);

        var em = b.StartTable(tableAlign, inlineSize);
        for (int i = placementOrder.Length - 1; i >= 0; i--)
        {
            var f = populated[placementOrder[i]];
            em = f.Kind switch
            {
                FieldKind.Bool => em.EmitBool(f.BoolVal),
                FieldKind.U8 => em.EmitU8(f.U8Val),
                FieldKind.U64 => em.EmitU64(f.U64Val),
                FieldKind.UOffset => em.EmitUOffset(f.UOffsetVal),
                _ => throw new InvalidOperationException(),
            };
        }
        // Insert canonical-layout pad (4 bytes between u64 fields and soffset_t)
        // when any 8-byte field is present.
        if (gapSize > 0) em = em.EmitPad(gapSize);
        return em.EmitSOffsetTo(vtTicket);
    }

    /// <summary>
    /// Comparison for FB-canonical inline placement: descending Size (largest
    /// alignment first), ascending Slot for tie-breaking.
    /// </summary>
    private static int CompareForLayout(FieldEntry a, FieldEntry b)
    {
        int sizeOrder = b.Size.CompareTo(a.Size); // descending Size
        if (sizeOrder != 0) return sizeOrder;
        return a.Slot.CompareTo(b.Slot);          // ascending Slot
    }

    private enum FieldKind : byte { Bool, U8, U64, UOffset }

    private readonly record struct FieldEntry(
        int Slot, int Size, FieldKind Kind,
        ulong U64Val, bool BoolVal, byte U8Val, int UOffsetVal);
}
