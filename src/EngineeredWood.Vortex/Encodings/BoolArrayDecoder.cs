// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.bool</c>: a packed bitmap (LSB-first within each
/// byte, matching Arrow's convention). One data buffer holding the values;
/// optionally one child (a nested <c>vortex.bool</c> ArrayNode) carrying the
/// validity bitmap when nullable.
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

        var data = serialized.BufferBytes(bufferRef);
        var minBytes = (int)((rowCount + 7) / 8);
        if (data.Length < minBytes)
            throw new VortexFormatException(
                $"vortex.bool buffer is {data.Length} bytes but needs at least {minBytes} for {rowCount} bits.");

        return new ArrowBuffer(data.Slice(0, minBytes).ToArray());
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
