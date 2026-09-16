// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.bool</c>: a packed bitmap (LSB-first within each
/// byte, matching Arrow's convention). One data buffer holding the values;
/// optionally one child (a nested <c>vortex.bool</c> ArrayNode) carrying the
/// validity bitmap when nullable.
///
/// <para>Metadata is <c>BoolMetadata { 1: uint32 offset }</c>: the bit (under 8) of the
/// buffer's first byte where the array starts, which a sliced array carries.</para>
/// </summary>
internal static class BoolArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not BooleanType)
            throw new VortexFormatException(
                $"vortex.bool decoder requires BooleanType expected, got {expectedType}.");

        // Read the values bitmap directly (don't go through ReadBitmap, which
        // is the leaf-only path used by other encodings' validity-child reads).
        var valueBuffer = ReadValueBitmap(node, serialized, expectedRowCount);
        var rowCount = checked((int)expectedRowCount);

        // Optional validity child: a nested vortex.bool ArrayNode whose own
        // single buffer is the validity bitmap.
        ArrowBuffer nullBuffer; int nullCount;
        if (node.ChildCount == 0)
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }
        else if (node.ChildCount == 1)
        {
            nullBuffer = ReadBitmap(node.Child(0), serialized, expectedRowCount);
            nullCount = CountNulls(nullBuffer.Span, rowCount);
        }
        else
        {
            throw new VortexFormatException(
                $"vortex.bool decoder expects 0 or 1 children, got {node.ChildCount}.");
        }

        return new BooleanArray(valueBuffer, nullBuffer, rowCount, nullCount, offset: 0);
    }

    /// <summary>
    /// Reads the values bitmap from a <c>vortex.bool</c> ArrayNode. Tolerates
    /// children (the validity-child case) — the caller (Decode) handles those
    /// separately.
    /// </summary>
    private static ArrowBuffer ReadValueBitmap(
        ArrayNode node, SerializedArray serialized, long rowCount)
    {
        if (node.BufferRefCount != 1)
            throw new VortexFormatException(
                $"vortex.bool ArrayNode should have 1 buffer ref, got {node.BufferRefCount}.");

        var bufferRef = node.BufferRef(0);
        var bufferDesc = serialized.Message.Buffer(bufferRef);
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.bool buffer compression {bufferDesc.Compression} not yet implemented.");

        var metaVec = node.Metadata;
        int bitOffset = ParseOffset(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));

        var data = serialized.BufferBytes(bufferRef);
        var minBytes = (int)((bitOffset + rowCount + 7) / 8);
        if (data.Length < minBytes)
            throw new VortexFormatException(
                $"vortex.bool buffer is {data.Length} bytes but needs at least {minBytes} for {rowCount} bits at offset {bitOffset}.");

        if (bitOffset == 0)
            return new ArrowBuffer(data.Slice(0, minBytes).ToArray());

        // Re-pack so bit 0 of the result is the array's first bit.
        var count = checked((int)rowCount);
        var packed = new byte[(count + 7) / 8];
        for (int i = 0; i < count; i++)
        {
            int src = bitOffset + i;
            if ((data[src >> 3] & (1 << (src & 7))) != 0)
                packed[i >> 3] |= (byte)(1 << (i & 7));
        }
        return new ArrowBuffer(packed);
    }

    /// <summary>Reads <c>BoolMetadata.offset</c> (field 1), which upstream keeps under 8.</summary>
    private static int ParseOffset(ReadOnlySpan<byte> bytes)
    {
        ulong offset = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            if ((tag & 7) != 0)
                throw new VortexFormatException(
                    $"Unsupported protobuf wire type {tag & 7} in vortex.bool metadata.");
            var value = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            if (tag >> 3 == 1)
                offset = value;
        }
        if (offset >= 8)
            throw new VortexFormatException($"vortex.bool bit offset {offset} must be less than 8.");
        return (int)offset;
    }

    /// <summary>
    /// Reads the bitmap from a leaf <c>vortex.bool</c> ArrayNode used as a
    /// validity child by other encodings (primitive, varbin, list, …). Such
    /// nodes themselves can't have a validity child, so children are rejected
    /// here. For top-level nullable bool columns, use <see cref="Decode"/>.
    /// </summary>
    public static ArrowBuffer ReadBitmap(
        ArrayNode node, SerializedArray serialized, long rowCount)
    {
        if (node.ChildCount != 0)
            throw new NotSupportedException(
                "vortex.bool used as a validity child must not itself have children.");
        return ReadValueBitmap(node, serialized, rowCount);
    }

    /// <summary>Counts unset bits in the first <paramref name="bitCount"/> bits of <paramref name="bitmap"/>.</summary>
    public static int CountNulls(ReadOnlySpan<byte> bitmap, int bitCount)
    {
        int nulls = 0;
        for (int i = 0; i < bitCount; i++)
        {
            if ((bitmap[i / 8] & (1 << (i % 8))) == 0)
                nulls++;
        }
        return nulls;
    }
}
