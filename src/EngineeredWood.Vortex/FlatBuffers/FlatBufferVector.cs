// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Vortex.FlatBuffers;

/// <summary>
/// A FlatBuffers vector. Element layout depends on the element type — element
/// accessors are exposed as separate methods rather than via generics so the
/// type stays a non-generic <c>readonly ref struct</c>.
/// </summary>
internal readonly ref struct FlatBufferVector
{
    private readonly ReadOnlySpan<byte> _buf;

    /// <summary>Absolute offset of the first element.</summary>
    private readonly int _elementsPos;

    public int Length { get; }

    public FlatBufferVector(ReadOnlySpan<byte> buf, int elementsPos, int length)
    {
        _buf = buf;
        _elementsPos = elementsPos;
        Length = length;
    }

    public bool IsNull => _buf.IsEmpty;

    /// <summary>The underlying buffer this vector lives in.</summary>
    public ReadOnlySpan<byte> Buffer => _buf;

    /// <summary>Reads element <paramref name="i"/> as a child table (uoffset_t).</summary>
    public FlatBufferTable Table(int i)
    {
        CheckIndex(i);
        var off = _elementsPos + i * 4;
        var tablePos = off + (int)BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(off));
        return new FlatBufferTable(_buf, tablePos);
    }

    /// <summary>Reads element <paramref name="i"/> as a UTF-8 string (uoffset_t).</summary>
    public string String(int i)
    {
        CheckIndex(i);
        var off = _elementsPos + i * 4;
        var stringPos = off + (int)BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(off));
        var len = (int)BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(stringPos));
        var bytes = _buf.Slice(stringPos + 4, len);
#if NET8_0_OR_GREATER
        return System.Text.Encoding.UTF8.GetString(bytes);
#else
        return System.Text.Encoding.UTF8.GetString(bytes.ToArray());
#endif
    }

    /// <summary>Reads element <paramref name="i"/> as a u8.</summary>
    public byte UInt8(int i)
    {
        CheckIndex(i);
        return _buf[_elementsPos + i];
    }

    /// <summary>Reads element <paramref name="i"/> as a u16.</summary>
    public ushort UInt16(int i)
    {
        CheckIndex(i);
        return BinaryPrimitives.ReadUInt16LittleEndian(_buf.Slice(_elementsPos + i * 2));
    }

    /// <summary>Reads element <paramref name="i"/> as a u32.</summary>
    public uint UInt32(int i)
    {
        CheckIndex(i);
        return BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(_elementsPos + i * 4));
    }

    /// <summary>
    /// Returns the absolute offset of inline struct element <paramref name="i"/>
    /// for a struct of size <paramref name="structSize"/>.
    /// </summary>
    public int StructPosition(int i, int structSize)
    {
        CheckIndex(i);
        return _elementsPos + i * structSize;
    }

    /// <summary>
    /// Returns a slice over the raw element bytes — convenient for ubyte vectors
    /// and zero-copy access to packed primitive arrays.
    /// </summary>
    public ReadOnlySpan<byte> RawBytes(int totalBytes) =>
        _buf.Slice(_elementsPos, totalBytes);

    private void CheckIndex(int i)
    {
        if ((uint)i >= (uint)Length)
            throw new ArgumentOutOfRangeException(nameof(i), i,
                $"Index {i} out of range for FlatBuffer vector of length {Length}.");
    }
}
