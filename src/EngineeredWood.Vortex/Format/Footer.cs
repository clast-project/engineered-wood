// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Vortex.FlatBuffers;

namespace EngineeredWood.Vortex.Format;

/// <summary>
/// Reader for the <c>Footer</c> FlatBuffers table from footer.fbs.
/// All five vectors are dictionary-encoded registries that the per-file
/// <see cref="Layout"/>, <see cref="ArrayNode"/>, and <see cref="SegmentSpec"/>
/// references index into.
/// Slots: 0=array_specs, 1=layout_specs, 2=segment_specs, 3=compression_specs, 4=encryption_specs.
/// </summary>
internal readonly ref struct Footer
{
    private readonly FlatBufferTable _table;

    public Footer(FlatBufferTable table) { _table = table; }

    public static Footer ReadRoot(ReadOnlySpan<byte> buf) =>
        new(FlatBufferTable.ReadRoot(buf));

    public FlatBufferVector ArraySpecs => _table.ReadVector(0);
    public FlatBufferVector LayoutSpecs => _table.ReadVector(1);
    public FlatBufferVector SegmentSpecs => _table.ReadVector(2);
    public FlatBufferVector CompressionSpecs => _table.ReadVector(3);
    public FlatBufferVector EncryptionSpecs => _table.ReadVector(4);

    /// <summary>Reads array spec <paramref name="i"/>'s id string.</summary>
    public string ArraySpecId(int i) => new ArraySpec(ArraySpecs.Table(i)).Id;

    /// <summary>Reads layout spec <paramref name="i"/>'s id string.</summary>
    public string LayoutSpecId(int i) => new LayoutSpec(LayoutSpecs.Table(i)).Id;

    /// <summary>Reads segment spec <paramref name="i"/> (inline struct).</summary>
    public SegmentSpec SegmentSpec(int i)
    {
        var v = SegmentSpecs;
        return new SegmentSpec(v.Buffer, v.StructPosition(i, Format.SegmentSpec.SizeBytes));
    }
}

/// <summary>Reader for <c>ArraySpec</c>. Slot 0 = id string.</summary>
internal readonly ref struct ArraySpec
{
    private readonly FlatBufferTable _table;

    public ArraySpec(FlatBufferTable table) { _table = table; }

    public string Id => _table.ReadString(0)
        ?? throw new VortexFormatException("Vortex Footer.array_specs entry has no id.");
}

/// <summary>Reader for <c>LayoutSpec</c>. Slot 0 = id string.</summary>
internal readonly ref struct LayoutSpec
{
    private readonly FlatBufferTable _table;

    public LayoutSpec(FlatBufferTable table) { _table = table; }

    public string Id => _table.ReadString(0)
        ?? throw new VortexFormatException("Vortex Footer.layout_specs entry has no id.");
}

/// <summary>
/// Reader for the <c>SegmentSpec</c> inline struct. Layout (16 bytes, aligned to 8):
/// <c>offset</c> u64@0, <c>length</c> u32@8, <c>alignment_exponent</c> u8@12,
/// <c>_compression</c> u8@13, <c>_encryption</c> u16@14.
/// </summary>
internal readonly ref struct SegmentSpec
{
    public const int SizeBytes = 16;

    private readonly ReadOnlySpan<byte> _buf;
    private readonly int _pos;

    public SegmentSpec(ReadOnlySpan<byte> buf, int pos) { _buf = buf; _pos = pos; }

    public ulong Offset => BinaryPrimitives.ReadUInt64LittleEndian(_buf.Slice(_pos));
    public uint Length => BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(_pos + 8));
    public byte AlignmentExponent => _buf[_pos + 12];
    public byte CompressionIndex => _buf[_pos + 13];
    public ushort EncryptionIndex => BinaryPrimitives.ReadUInt16LittleEndian(_buf.Slice(_pos + 14));
}
