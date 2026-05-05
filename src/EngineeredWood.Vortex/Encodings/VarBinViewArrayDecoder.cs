// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.varbinview</c>: an Arrow-StringView-style encoding.
/// Each row is a 16-byte <c>BinaryView</c> entry. Strings ≤ 12 bytes are inlined
/// in the view; longer strings reference a separate data buffer.
///
/// <para>Buffer layout (per <c>vortex-array/src/arrays/varbinview/vtable/mod.rs</c>):
/// data buffers come first, the views buffer comes last. So with N total
/// buffers, indices 0..N-2 are data buffers and N-1 is views.</para>
///
/// <para>Output: an Apache Arrow <see cref="StringArray"/> (for
/// <see cref="StringType"/>) or <see cref="BinaryArray"/> (for
/// <see cref="BinaryType"/>) — the wire shape is identical, only the
/// surfaced typed array differs. The view-format → contiguous-string
/// conversion copies the data; a future optimization could produce a
/// <c>StringViewArray</c> if Apache.Arrow grows one we can target.</para>
/// </summary>
internal static class VarBinViewArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not StringType and not BinaryType)
            throw new NotSupportedException(
                $"vortex.varbinview decoder currently only supports StringType / BinaryType, got {expectedType}.");

        var bufferCount = node.BufferRefCount;
        if (bufferCount < 1)
            throw new VortexFormatException(
                "vortex.varbinview must have at least 1 buffer (the views buffer).");

        // Last buffer is views; everything before it is a data buffer.
        var viewsBufRef = node.BufferRef(bufferCount - 1);
        var viewsDesc = serialized.Message.Buffer(viewsBufRef);
        if (viewsDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.varbinview views buffer compression {viewsDesc.Compression} not yet implemented.");
        var viewsData = serialized.BufferBytes(viewsBufRef);

        var rowCount = checked((int)expectedRowCount);
        if (viewsData.Length < rowCount * 16)
            throw new VortexFormatException(
                $"vortex.varbinview views buffer is {viewsData.Length} bytes but needs {rowCount * 16} for {rowCount} rows.");

        // Validity child handling parallels PrimitiveArrayDecoder.
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
                $"vortex.varbinview with {node.ChildCount} children is not supported (max 1 validity child).");
        }

        // Materialize offsets + concatenated values. For inlined views (len ≤ 12)
        // copy from the view itself; for long views read from data_buffers[buf_idx].
        var offsetBytes = new byte[(rowCount + 1) * 4];
        using var values = new MemoryStream();
        int total = 0;
        for (int i = 0; i < rowCount; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4), total);

            var view = viewsData.Slice(i * 16, 16);
            var len = (int)BinaryPrimitives.ReadUInt32LittleEndian(view);
            if (len <= 12)
            {
                Append(values, view.Slice(4, len));
            }
            else
            {
                var bufIdx = (int)BinaryPrimitives.ReadUInt32LittleEndian(view.Slice(8));
                var bufOff = (int)BinaryPrimitives.ReadUInt32LittleEndian(view.Slice(12));
                if ((uint)bufIdx >= (uint)(bufferCount - 1))
                    throw new VortexFormatException(
                        $"vortex.varbinview row {i} references data buffer {bufIdx} but only {bufferCount - 1} are present.");
                var dataBufRef = node.BufferRef(bufIdx);
                var dataDesc = serialized.Message.Buffer(dataBufRef);
                if (dataDesc.Compression != BufferCompression.None)
                    throw new NotSupportedException(
                        $"vortex.varbinview data buffer compression {dataDesc.Compression} not yet implemented.");
                var dataBytes = serialized.BufferBytes(dataBufRef);
                Append(values, dataBytes.Slice(bufOff, len));
            }
            total += len;
        }
        BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(rowCount * 4), total);

        var offsetsBuf = new ArrowBuffer(offsetBytes);
        var valuesBuf = new ArrowBuffer(values.ToArray());
        return expectedType is StringType
            ? new StringArray(rowCount, offsetsBuf, valuesBuf, nullBuffer, nullCount, offset: 0)
            : new BinaryArray(BinaryType.Default, rowCount, offsetsBuf, valuesBuf, nullBuffer, nullCount, offset: 0);
    }

    private static void Append(MemoryStream sink, ReadOnlySpan<byte> data)
    {
#if NET8_0_OR_GREATER
        sink.Write(data);
#else
        sink.Write(data.ToArray(), 0, data.Length);
#endif
    }
}
