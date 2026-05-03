// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.bytebool</c>: one byte per row (0 = false, non-zero = true).
/// 1 buffer, 0-1 children (optional validity).
/// </summary>
internal static class ByteBoolArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not BooleanType)
            throw new NotSupportedException(
                $"vortex.bytebool only produces BooleanType arrays, got {expectedType}.");
        if (node.BufferRefCount != 1)
            throw new VortexFormatException(
                $"vortex.bytebool expects 1 buffer, got {node.BufferRefCount}.");

        var bufferRef = node.BufferRef(0);
        var bufferDesc = serialized.Message.Buffer(bufferRef);
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.bytebool buffer compression {bufferDesc.Compression} not yet implemented.");

        var data = serialized.BufferBytes(bufferRef);
        var rowCount = checked((int)expectedRowCount);
        if (data.Length < rowCount)
            throw new VortexFormatException(
                $"vortex.bytebool buffer is {data.Length} bytes but needs at least {rowCount}.");

        // Pack per-byte → Arrow bitmap (LSB-first).
        var byteCount = (rowCount + 7) / 8;
        var bitmap = new byte[byteCount];
        for (int i = 0; i < rowCount; i++)
        {
            if (data[i] != 0)
                bitmap[i / 8] |= (byte)(1 << (i % 8));
        }

        ArrowBuffer nullBuffer;
        int nullCount;
        if (node.ChildCount == 0)
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }
        else if (node.ChildCount == 1)
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(0), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }
        else
        {
            throw new NotSupportedException(
                $"vortex.bytebool with {node.ChildCount} children is not supported.");
        }

        return new BooleanArray(new ArrowBuffer(bitmap), nullBuffer, rowCount, nullCount, 0);
    }
}
