// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decodes a <c>vortex.primitive</c> ArrayNode into an Apache Arrow primitive
/// array.
///
/// <para>Phase 1 scope: non-nullable primitives only, with a single data
/// buffer that holds packed little-endian primitive values. Nullable variants
/// require a child validity array in <c>node.children</c>; that path is added
/// alongside a fixture that exercises it.</para>
/// </summary>
internal static class PrimitiveArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.BufferRefCount != 1)
            throw new NotSupportedException(
                $"vortex.primitive decoder expects exactly 1 buffer ref, got {node.BufferRefCount}.");

        var bufferRef = node.BufferRef(0);
        var bufferDesc = serialized.Message.Buffer(bufferRef);
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.primitive buffer compression {bufferDesc.Compression} not yet implemented.");

        // Validity: zero children = non-nullable; one child (vortex.bool) = bitmap.
        ArrowBuffer nullBuffer;
        int nullCount;
        if (node.ChildCount == 0)
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }
        else if (node.ChildCount == 1)
        {
            var validityChild = node.Child(0);
            // We only handle vortex.bool as a validity child today. If a future
            // fixture uses vortex.constant<bool> here, we'd dispatch through
            // ArrayDecoder.DecodeNode and pull the bitmap from the resulting
            // BooleanArray. Keep simple for now.
            nullBuffer = BoolArrayDecoder.ReadBitmap(validityChild, serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, checked((int)expectedRowCount));
        }
        else
        {
            throw new NotSupportedException(
                $"vortex.primitive with {node.ChildCount} children is not supported (max 1 validity child).");
        }

        var data = serialized.BufferBytes(bufferRef);
        var rowCount = checked((int)expectedRowCount);

        return expectedType switch
        {
            Int8Type => BuildArray<sbyte>(data, rowCount, sizeof(sbyte), nullBuffer, nullCount,
                (vb, nb, len, nc) => new Int8Array(vb, nb, len, nc, offset: 0)),
            Int16Type => BuildArray<short>(data, rowCount, sizeof(short), nullBuffer, nullCount,
                (vb, nb, len, nc) => new Int16Array(vb, nb, len, nc, offset: 0)),
            Int32Type => BuildArray<int>(data, rowCount, sizeof(int), nullBuffer, nullCount,
                (vb, nb, len, nc) => new Int32Array(vb, nb, len, nc, offset: 0)),
            Int64Type => BuildArray<long>(data, rowCount, sizeof(long), nullBuffer, nullCount,
                (vb, nb, len, nc) => new Int64Array(vb, nb, len, nc, offset: 0)),
            UInt8Type => BuildArray<byte>(data, rowCount, sizeof(byte), nullBuffer, nullCount,
                (vb, nb, len, nc) => new UInt8Array(vb, nb, len, nc, offset: 0)),
            UInt16Type => BuildArray<ushort>(data, rowCount, sizeof(ushort), nullBuffer, nullCount,
                (vb, nb, len, nc) => new UInt16Array(vb, nb, len, nc, offset: 0)),
            UInt32Type => BuildArray<uint>(data, rowCount, sizeof(uint), nullBuffer, nullCount,
                (vb, nb, len, nc) => new UInt32Array(vb, nb, len, nc, offset: 0)),
            UInt64Type => BuildArray<ulong>(data, rowCount, sizeof(ulong), nullBuffer, nullCount,
                (vb, nb, len, nc) => new UInt64Array(vb, nb, len, nc, offset: 0)),
            FloatType => BuildArray<float>(data, rowCount, sizeof(float), nullBuffer, nullCount,
                (vb, nb, len, nc) => new FloatArray(vb, nb, len, nc, offset: 0)),
            DoubleType => BuildArray<double>(data, rowCount, sizeof(double), nullBuffer, nullCount,
                (vb, nb, len, nc) => new DoubleArray(vb, nb, len, nc, offset: 0)),
#if NET6_0_OR_GREATER
            HalfFloatType => BuildArray<Half>(data, rowCount, elementSize: 2, nullBuffer, nullCount,
                (vb, nb, len, nc) => new HalfFloatArray(vb, nb, len, nc, offset: 0)),
#else
            HalfFloatType => throw new NotSupportedException(
                "HalfFloat (F16) decode requires System.Half (net6+); netstandard2.0 builds "
                + "of Apache.Arrow don't ship HalfFloatArray."),
#endif
            _ => throw new NotSupportedException(
                $"vortex.primitive decoder doesn't support Arrow type {expectedType}."),
        };
    }

    private static IArrowArray BuildArray<T>(
        ReadOnlySpan<byte> data,
        int rowCount,
        int elementSize,
        ArrowBuffer nullBuffer,
        int nullCount,
        Func<ArrowBuffer, ArrowBuffer, int, int, IArrowArray> ctor)
        where T : struct
    {
        var expectedBytes = (long)rowCount * elementSize;
        if (data.Length != expectedBytes)
            throw new VortexFormatException(
                $"vortex.primitive buffer is {data.Length} bytes but expected {expectedBytes} for {rowCount} elements of size {elementSize}.");

        var bytes = data.ToArray();
        var valueBuffer = new ArrowBuffer(bytes);
        return ctor(valueBuffer, nullBuffer, rowCount, nullCount);
    }
}
