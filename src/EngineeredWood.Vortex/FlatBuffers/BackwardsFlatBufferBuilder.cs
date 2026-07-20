// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Vortex.FlatBuffers;

/// <summary>
/// Hand-rolled backwards-growing FlatBuffers builder, paired with the
/// hand-rolled reader at <see cref="FlatBufferTable"/> / <see cref="FlatBufferVector"/>.
/// Used by the writer to emit DType, Layout, Footer, Postscript, and per-array
/// Message FlatBuffers without taking a dependency on Google.FlatBuffers.
///
/// <para>Conforms to the FlatBuffers alignment spec at top-level boundaries:
/// each top-level object (vector, string, vtable, table soffset) is preceded
/// by <see cref="Prep"/> padding so its final position is naturally aligned.
/// The buffer is padded to the largest alignment used before the root uoffset
/// is written, so backwards <c>_used</c> values translate cleanly to forward
/// positions.</para>
///
/// <para>Within a table's inline data, <see cref="TableEmitter"/> does NOT
/// auto-prep — callers are responsible for choosing FB-canonical layouts
/// (largest-aligned fields placed first in time = highest in memory) and
/// pre-Prep'ing so the table_pos satisfies the largest field's alignment.
/// When canonical layouts require a gap (e.g., 4 bytes between soffset_t and
/// the first u64 field), use <see cref="TableEmitter.EmitPad"/> to insert it.</para>
///
/// <para>Tickets: each Write* method returns <c>_used</c> (bytes from the end
/// of the work buffer) at the moment its first byte is written. A ticket is
/// therefore the "compacted offset from the end" of that target — independent
/// of the final buffer size. To compute a uoffset_t at write site U whose
/// target has ticket T: <c>delta = _used_after_writing_uoffset - T</c>. That
/// formula is independent of <c>final_used</c>: both sides cancel.</para>
/// </summary>
internal sealed class BackwardsFlatBufferBuilder
{
    private byte[] _bytes = new byte[256];
    private int _used = 0;
    /// <summary>Largest alignment requested via <see cref="Prep"/>; the buffer is padded to a multiple of this in <see cref="Finish"/>.</summary>
    private int _minAlign = 1;

    /// <summary>
    /// Pads with zero bytes so that after writing <paramref name="additionalBytes"/>
    /// more, <c>_used</c> is a multiple of <paramref name="alignment"/>. Tracks the
    /// largest <paramref name="alignment"/> seen so the buffer can be padded at
    /// finalize time. Reserved bytes are already zero (the underlying byte[] is
    /// zero-initialized and the unused prefix is never touched).
    /// </summary>
    public void Prep(int alignment, int additionalBytes)
    {
        if (alignment > _minAlign) _minAlign = alignment;
        int target = _used + additionalBytes;
        int pad = (alignment - target % alignment) % alignment;
        if (pad > 0) Reserve(pad);
    }

    private Span<byte> Reserve(int count)
    {
        var newUsed = _used + count;
        while (newUsed > _bytes.Length)
        {
            var bigger = new byte[_bytes.Length * 2];
            Buffer.BlockCopy(_bytes, _bytes.Length - _used, bigger, bigger.Length - _used, _used);
            _bytes = bigger;
        }
        _used = newUsed;
        return _bytes.AsSpan(_bytes.Length - _used, count);
    }

    /// <summary>
    /// FlatBuffers strings: <c>[length: u32 LE][bytes][null terminator]</c>.
    /// The null terminator is required by the spec for C compatibility and is
    /// NOT counted in the length prefix.
    /// </summary>
    public int WriteString(string s)
    {
        var utf8 = System.Text.Encoding.UTF8.GetBytes(s);
        // Total bytes about to land: utf8.Length + 1 (null term) + 4 (count u32).
        // Align so the count u32 is 4-aligned in the final buffer.
        Prep(4, utf8.Length + 5);
        Reserve(1)[0] = 0; // null terminator (highest mem in backwards order)
        var bytesSlot = Reserve(utf8.Length);
        utf8.AsSpan().CopyTo(bytesSlot);
        BinaryPrimitives.WriteUInt32LittleEndian(Reserve(4), (uint)utf8.Length);
        return _used;
    }

    public int WriteByteVector(ReadOnlySpan<byte> values)
    {
        Prep(4, values.Length + 4);
        var slot = Reserve(values.Length);
        values.CopyTo(slot);
        BinaryPrimitives.WriteUInt32LittleEndian(Reserve(4), (uint)values.Length);
        return _used;
    }

    public int WriteUInt32Vector(uint[] values)
    {
        Prep(4, values.Length * 4 + 4);
        for (int i = values.Length - 1; i >= 0; i--)
            BinaryPrimitives.WriteUInt32LittleEndian(Reserve(4), values[i]);
        BinaryPrimitives.WriteUInt32LittleEndian(Reserve(4), (uint)values.Length);
        return _used;
    }

    public int WriteUOffsetVector(ReadOnlySpan<int> targetTickets)
    {
        // Each uoffset_t is a forward delta from its own slot to the target.
        // Walking last-to-first, after writing a 4-byte slot _used has just
        // advanced by 4, so the slot's "current ticket" equals _used and
        // delta = _used - targetTicket.
        Prep(4, targetTickets.Length * 4 + 4);
        for (int i = targetTickets.Length - 1; i >= 0; i--)
        {
            Reserve(4);
            BinaryPrimitives.WriteUInt32LittleEndian(
                _bytes.AsSpan(_bytes.Length - _used, 4),
                (uint)(_used - targetTickets[i]));
        }
        BinaryPrimitives.WriteUInt32LittleEndian(Reserve(4), (uint)targetTickets.Length);
        return _used;
    }

    /// <summary>
    /// Writes a raw sequence of u16 values, last-to-first. Used to emit vtables
    /// (the values are <c>vt_size, inline_size, slot0, slot1, ...</c>). The
    /// vtable's start position must be 2-aligned because it's referenced by an
    /// soffset_t and read as a u16 array.
    /// </summary>
    public int WriteUInt16s(ushort[] values)
    {
        Prep(2, values.Length * 2);
        for (int i = values.Length - 1; i >= 0; i--)
            BinaryPrimitives.WriteUInt16LittleEndian(Reserve(2), values[i]);
        return _used;
    }

    /// <summary>Writes a FlatBuffer vector of u16 values (count u32 + entries).</summary>
    public int WriteUInt16Vector(ushort[] values)
    {
        // Vector header is u32 count, so 4-align governs the whole thing.
        Prep(4, values.Length * 2 + 4);
        for (int i = values.Length - 1; i >= 0; i--)
            BinaryPrimitives.WriteUInt16LittleEndian(Reserve(2), values[i]);
        BinaryPrimitives.WriteUInt32LittleEndian(Reserve(4), (uint)values.Length);
        return _used;
    }

    /// <summary>
    /// Writes a single byte at the current backwards position. Raw — no
    /// alignment prep. Used for inline-struct fields (whose struct-level
    /// alignment is set by one pre-<see cref="Prep"/>).
    /// </summary>
    public void WriteByte(byte value) => Reserve(1)[0] = value;

    /// <summary>Raw u16 LE write. See <see cref="WriteByte"/>.</summary>
    public void WriteU16(ushort value) =>
        BinaryPrimitives.WriteUInt16LittleEndian(Reserve(2), value);

    /// <summary>Raw u32 LE write. See <see cref="WriteByte"/>.</summary>
    public void WriteU32(uint value) =>
        BinaryPrimitives.WriteUInt32LittleEndian(Reserve(4), value);

    /// <summary>Raw u64 LE write. See <see cref="WriteByte"/>.</summary>
    public void WriteU64(ulong value) =>
        BinaryPrimitives.WriteUInt64LittleEndian(Reserve(8), value);

    /// <summary>Returns the current ticket (used for vector headers etc.).</summary>
    public int CurrentTicket() => _used;

    /// <summary>
    /// Begins emitting a table's inline data. Pre-Preps so the
    /// <paramref name="inlineSize"/>-byte inline ends with <c>_used</c> aligned
    /// to <paramref name="alignment"/> — which is the largest alignment of any
    /// inline field (4 for tables of u32/uoffset/u8/bool fields, 8 for tables
    /// containing u64). Caller must have already written the table's vtable
    /// (the Prep follows the vtable; otherwise vtable's own 2-alignment pad can
    /// land inline fields off-alignment).
    /// </summary>
    public TableEmitter StartTable(int alignment, int inlineSize)
    {
        Prep(alignment, inlineSize);
        return new(this);
    }

    public byte[] Finish(int rootTableTicket)
    {
        // Pad so the buffer's total size is a multiple of _minAlign. After
        // writing the root uoffset, _used = file_size; we need
        // file_size % _minAlign == 0 so final_position[X] = file_size - _used_X
        // satisfies any alignment ≤ _minAlign whenever _used_X is aligned.
        Prep(_minAlign, 4);
        Reserve(4);
        BinaryPrimitives.WriteUInt32LittleEndian(
            _bytes.AsSpan(_bytes.Length - _used, 4),
            (uint)(_used - rootTableTicket));

        var result = new byte[_used];
        Buffer.BlockCopy(_bytes, _bytes.Length - _used, result, 0, _used);
        return result;
    }

    /// <summary>
    /// Fluent emitter for one table's inline data. Emit calls go in tail-first
    /// order — the highest forward inline offset is written first, the leading
    /// soffset_t is written last via <see cref="EmitSOffsetTo"/>. Within a
    /// single table, Emit* methods write straight into the buffer with NO
    /// alignment prep — the caller must pre-Prep before <see cref="StartTable"/>
    /// to align table_pos for the largest field, and use <see cref="EmitPad"/>
    /// to insert any explicit gaps the canonical layout requires (e.g., 4 bytes
    /// between an 8-byte u64 field and the soffset_t).
    /// </summary>
    public readonly struct TableEmitter
    {
        private readonly BackwardsFlatBufferBuilder _b;
        public TableEmitter(BackwardsFlatBufferBuilder b) { _b = b; }

        public TableEmitter EmitU8(byte value)
        {
            _b.Reserve(1)[0] = value;
            return this;
        }

        public TableEmitter EmitBool(bool value) => EmitU8(value ? (byte)1 : (byte)0);

        public TableEmitter EmitU16(ushort value)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(_b.Reserve(2), value);
            return this;
        }

        public TableEmitter EmitU32(uint value)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(_b.Reserve(4), value);
            return this;
        }

        public TableEmitter EmitU64(ulong value)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(_b.Reserve(8), value);
            return this;
        }

        public TableEmitter EmitUOffset(int targetTicket)
        {
            _b.Reserve(4);
            BinaryPrimitives.WriteUInt32LittleEndian(
                _b._bytes.AsSpan(_b._bytes.Length - _b._used, 4),
                (uint)(_b._used - targetTicket));
            return this;
        }

        /// <summary>
        /// Reserves <paramref name="bytes"/> bytes of zero padding inside the
        /// table inline. Used for canonical-FB-layout gaps (e.g., 4 bytes
        /// between u64 fields and soffset_t when soffset is 4-aligned but the
        /// u64 needs 8-alignment).
        /// </summary>
        public TableEmitter EmitPad(int bytes)
        {
            _b.Reserve(bytes);
            return this;
        }

        /// <summary>Emits the table's leading soffset_t and returns the table ticket.</summary>
        public int EmitSOffsetTo(int vtableTicket)
        {
            _b.Reserve(4);
            BinaryPrimitives.WriteInt32LittleEndian(
                _b._bytes.AsSpan(_b._bytes.Length - _b._used, 4),
                vtableTicket - _b._used);
            return _b._used;
        }
    }
}
