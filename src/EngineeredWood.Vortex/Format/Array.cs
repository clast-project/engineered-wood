// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Vortex.FlatBuffers;

namespace EngineeredWood.Vortex.Format;

/// <summary>
/// Compression applied to an individual array buffer. Distinct from the
/// per-segment <see cref="CompressionScheme"/>: array buffers support only
/// None and LZ4 (array.fbs).
/// </summary>
internal enum BufferCompression : byte
{
    None = 0,
    LZ4 = 1,
}

/// <summary>
/// Indicates whether a stat value is exact or a (looser) bound. Used inside
/// <see cref="ArrayStats"/>.
/// </summary>
internal enum Precision : byte
{
    Inexact = 0,
    Exact = 1,
}

/// <summary>
/// Reader for the root <c>Array</c> FlatBuffers table from array.fbs.
/// Slots: 0=root (ArrayNode), 1=buffers ([Buffer] inline struct vector).
/// </summary>
internal readonly ref struct ArrayMessage
{
    private readonly FlatBufferTable _table;

    public ArrayMessage(FlatBufferTable table) { _table = table; }

    public static ArrayMessage ReadRoot(ReadOnlySpan<byte> buf) =>
        new(FlatBufferTable.ReadRoot(buf));

    public ArrayNode Root => new(_table.ReadTable(0));
    public FlatBufferVector Buffers => _table.ReadVector(1);

    public int BufferCount => Buffers.Length;
    public BufferDescriptor Buffer(int i)
    {
        var v = Buffers;
        return new BufferDescriptor(v.Buffer, v.StructPosition(i, BufferDescriptor.SizeBytes));
    }
}

/// <summary>
/// Reader for the recursive <c>ArrayNode</c> FlatBuffers table.
/// Slots: 0=encoding (u16, indexes <see cref="Footer.ArraySpecs"/>),
/// 1=metadata (ubyte vector), 2=children (ArrayNode vector),
/// 3=buffers (u16 vector indexing into the parent <see cref="ArrayMessage.Buffers"/>),
/// 4=stats (ArrayStats).
/// </summary>
internal readonly ref struct ArrayNode
{
    private readonly FlatBufferTable _table;

    public ArrayNode(FlatBufferTable table) { _table = table; }

    public ushort EncodingIndex => _table.ReadUInt16(0);
    public FlatBufferVector Metadata => _table.ReadVector(1);
    public FlatBufferVector Children => _table.ReadVector(2);
    public FlatBufferVector BufferIndices => _table.ReadVector(3);
    public ArrayStats Stats => new(_table.ReadTable(4));

    public int ChildCount => Children.Length;
    public ArrayNode Child(int i) => new(Children.Table(i));

    public int BufferRefCount => BufferIndices.Length;
    public ushort BufferRef(int i) => BufferIndices.UInt16(i);
}

/// <summary>
/// Reader for the inline <c>Buffer</c> struct (array.fbs). Layout (8 bytes,
/// aligned to 4): <c>padding</c> u16@0, <c>alignment_exponent</c> u8@2,
/// <c>compression</c> u8@3, <c>length</c> u32@4.
/// </summary>
internal readonly ref struct BufferDescriptor
{
    public const int SizeBytes = 8;

    private readonly ReadOnlySpan<byte> _buf;
    private readonly int _pos;

    public BufferDescriptor(ReadOnlySpan<byte> buf, int pos) { _buf = buf; _pos = pos; }

    public ushort Padding => BinaryPrimitives.ReadUInt16LittleEndian(_buf.Slice(_pos));
    public byte AlignmentExponent => _buf[_pos + 2];
    public BufferCompression Compression => (BufferCompression)_buf[_pos + 3];
    public uint Length => BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(_pos + 4));
}

/// <summary>
/// Reader for the optional <c>ArrayStats</c> table. Phase 1 keeps the raw
/// <c>min</c>/<c>max</c>/<c>sum</c> bytes (protobuf-serialized
/// <c>vortex-proto</c> ScalarValue) and exposes only the cheap booleans/counts;
/// scalar decoding is deferred until we add a <c>vortex-proto</c> reader.
/// </summary>
internal readonly ref struct ArrayStats
{
    private readonly FlatBufferTable _t;

    public ArrayStats(FlatBufferTable t) { _t = t; }

    public bool IsPresent => !_t.IsNull;

    public FlatBufferVector MinBytes => _t.ReadVector(0);
    public Precision MinPrecision => (Precision)_t.ReadByte(1);
    public FlatBufferVector MaxBytes => _t.ReadVector(2);
    public Precision MaxPrecision => (Precision)_t.ReadByte(3);
    public FlatBufferVector SumBytes => _t.ReadVector(4);

    /// <summary>True if <c>is_sorted</c> is present (else null/unknown).</summary>
    public bool HasIsSorted => _t.FieldOffset(5) != 0;
    public bool IsSorted => _t.ReadBool(5);

    public bool HasIsStrictSorted => _t.FieldOffset(6) != 0;
    public bool IsStrictSorted => _t.ReadBool(6);

    public bool HasIsConstant => _t.FieldOffset(7) != 0;
    public bool IsConstant => _t.ReadBool(7);

    public bool HasNullCount => _t.FieldOffset(8) != 0;
    public ulong NullCount => _t.ReadUInt64(8);

    public bool HasUncompressedSize => _t.FieldOffset(9) != 0;
    public ulong UncompressedSizeInBytes => _t.ReadUInt64(9);

    public bool HasNanCount => _t.FieldOffset(10) != 0;
    public ulong NanCount => _t.ReadUInt64(10);
}
