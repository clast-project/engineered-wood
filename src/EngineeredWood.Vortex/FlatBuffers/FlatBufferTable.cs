// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace EngineeredWood.Vortex.FlatBuffers;

/// <summary>
/// Reader for a FlatBuffers table. Holds a buffer plus the absolute offset of
/// the table data, and resolves field positions through the vtable.
///
/// <para>FlatBuffers wire format quick reference:
/// <list type="bullet">
///   <item>Primitives are little-endian.</item>
///   <item>A table position points to an int32 soffset_t whose negation yields
///         the absolute vtable position. The vtable starts with two u16 values
///         (vtable size, inline table size) followed by one u16 per field
///         giving the byte offset of that field within the table data
///         (0 means the field is absent).</item>
///   <item>Indirect fields (tables, strings, vectors) store a uoffset_t (u32)
///         which is added to its own position to yield the target.</item>
///   <item>A string is a uoffset_t to <c>(u32 length)(utf8 bytes)(zero byte)</c>.</item>
///   <item>A vector is a uoffset_t to <c>(u32 length)(elements)</c> where
///         element size depends on the element type.</item>
///   <item>Inline structs are written verbatim at the field offset, no
///         indirection.</item>
///   <item>A union is encoded as two adjacent fields: a u8 type tag, then a
///         uoffset_t to the value table.</item>
/// </list></para>
/// </summary>
internal readonly ref struct FlatBufferTable
{
    private readonly ReadOnlySpan<byte> _buf;
    private readonly int _pos;

    public FlatBufferTable(ReadOnlySpan<byte> buf, int tablePos)
    {
        _buf = buf;
        _pos = tablePos;
    }

    /// <summary>True if this table reference is empty (field-absent sentinel).</summary>
    public bool IsNull => _buf.IsEmpty;

    /// <summary>The buffer this table is part of.</summary>
    public ReadOnlySpan<byte> Buffer => _buf;

    /// <summary>The absolute offset of the table data start.</summary>
    public int Position => _pos;

    /// <summary>
    /// Reads a root table from a complete FlatBuffer. The first u32 of the
    /// buffer is a uoffset_t to the root table.
    /// </summary>
    public static FlatBufferTable ReadRoot(ReadOnlySpan<byte> buf)
    {
        if (buf.Length < 4)
            throw new VortexFormatException(
                $"FlatBuffer too small to contain a root offset: {buf.Length} bytes.");
        var rootOffset = (int)BinaryPrimitives.ReadUInt32LittleEndian(buf);
        if ((uint)rootOffset > (uint)buf.Length)
            throw new VortexFormatException(
                $"FlatBuffer root offset {rootOffset} is out of bounds (buffer is {buf.Length} bytes).");
        return new FlatBufferTable(buf, rootOffset);
    }

    /// <summary>
    /// Returns the absolute offset of the field at the given vtable slot, or
    /// 0 if the field is absent. Slot 0 is the first field.
    /// </summary>
    public int FieldOffset(int slot)
    {
        var soffset = BinaryPrimitives.ReadInt32LittleEndian(_buf.Slice(_pos));
        var vtablePos = _pos - soffset;
        if ((uint)vtablePos >= (uint)_buf.Length)
            throw new VortexFormatException(
                $"FlatBuffer vtable offset {vtablePos} is out of bounds (slot {slot}).");
        var vtableSize = BinaryPrimitives.ReadUInt16LittleEndian(_buf.Slice(vtablePos));
        var slotByteOffset = 4 + slot * 2;
        if (slotByteOffset + 2 > vtableSize)
            return 0;
        var fieldOffset = BinaryPrimitives.ReadUInt16LittleEndian(_buf.Slice(vtablePos + slotByteOffset));
        return fieldOffset == 0 ? 0 : _pos + fieldOffset;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Indirect(int absOffset) =>
        absOffset + (int)BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(absOffset));

    public byte ReadByte(int slot, byte defaultValue = 0)
    {
        var off = FieldOffset(slot);
        return off == 0 ? defaultValue : _buf[off];
    }

    public sbyte ReadSByte(int slot, sbyte defaultValue = 0)
    {
        var off = FieldOffset(slot);
        return off == 0 ? defaultValue : (sbyte)_buf[off];
    }

    public bool ReadBool(int slot, bool defaultValue = false)
    {
        var off = FieldOffset(slot);
        return off == 0 ? defaultValue : _buf[off] != 0;
    }

    public ushort ReadUInt16(int slot, ushort defaultValue = 0)
    {
        var off = FieldOffset(slot);
        return off == 0 ? defaultValue : BinaryPrimitives.ReadUInt16LittleEndian(_buf.Slice(off));
    }

    public short ReadInt16(int slot, short defaultValue = 0)
    {
        var off = FieldOffset(slot);
        return off == 0 ? defaultValue : BinaryPrimitives.ReadInt16LittleEndian(_buf.Slice(off));
    }

    public uint ReadUInt32(int slot, uint defaultValue = 0)
    {
        var off = FieldOffset(slot);
        return off == 0 ? defaultValue : BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(off));
    }

    public int ReadInt32(int slot, int defaultValue = 0)
    {
        var off = FieldOffset(slot);
        return off == 0 ? defaultValue : BinaryPrimitives.ReadInt32LittleEndian(_buf.Slice(off));
    }

    public ulong ReadUInt64(int slot, ulong defaultValue = 0)
    {
        var off = FieldOffset(slot);
        return off == 0 ? defaultValue : BinaryPrimitives.ReadUInt64LittleEndian(_buf.Slice(off));
    }

    public long ReadInt64(int slot, long defaultValue = 0)
    {
        var off = FieldOffset(slot);
        return off == 0 ? defaultValue : BinaryPrimitives.ReadInt64LittleEndian(_buf.Slice(off));
    }

    /// <summary>
    /// Returns the absolute offset of an inline struct at the given slot, or
    /// -1 if the field is absent. Inline structs are written verbatim at the
    /// field offset (no <c>uoffset_t</c> indirection).
    /// </summary>
    public int StructOffset(int slot)
    {
        var off = FieldOffset(slot);
        return off == 0 ? -1 : off;
    }

    /// <summary>
    /// Reads a child table at the given slot. Returns an empty table (IsNull) if
    /// the field is absent.
    /// </summary>
    public FlatBufferTable ReadTable(int slot)
    {
        var off = FieldOffset(slot);
        return off == 0 ? default : new FlatBufferTable(_buf, Indirect(off));
    }

    /// <summary>
    /// Reads a string at the given slot. Returns null if absent.
    /// </summary>
    public string? ReadString(int slot)
    {
        var off = FieldOffset(slot);
        if (off == 0) return null;
        var stringPos = Indirect(off);
        var len = (int)BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(stringPos));
        var bytes = _buf.Slice(stringPos + 4, len);
#if NET8_0_OR_GREATER
        return System.Text.Encoding.UTF8.GetString(bytes);
#else
        return System.Text.Encoding.UTF8.GetString(bytes.ToArray());
#endif
    }

    /// <summary>
    /// Reads a vector at the given slot. Returns an empty vector (IsNull) if
    /// the field is absent. Element kind is decided by the caller.
    /// </summary>
    public FlatBufferVector ReadVector(int slot)
    {
        var off = FieldOffset(slot);
        if (off == 0) return default;
        var vecPos = Indirect(off);
        var len = (int)BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(vecPos));
        return new FlatBufferVector(_buf, vecPos + 4, len);
    }
}
