// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.PrimitiveArrayDecoder"/>:
/// emits a <c>vortex.primitive</c> ArrayNode subtree (or a complete segment via
/// the convenience <see cref="Encode"/>). Supports non-nullable AND nullable
/// primitives (Int8..Int64, UInt8..UInt64, Float32/64). Sliced inputs handled
/// via <c>data.Offset</c>.
///
/// <para>Emitted shape (non-nullable):
/// <c>ArrayNode { encoding=primitive, buffer_indices=[valuesBuf] }</c>.</para>
/// <para>Emitted shape (nullable): same root with one child
/// <c>ArrayNode { encoding=bool, buffer_indices=[bitmapBuf] }</c>.</para>
/// </summary>
internal static class PrimitiveArrayEncoder
{
    /// <summary>
    /// Emits the ArrayNode subtree for <paramref name="array"/> into
    /// <paramref name="sb"/>, registering all needed buffers. Returns the
    /// ArrayNode table ticket. Pass <paramref name="statsTicket"/> non-null
    /// to attach an ArrayStats slot at the root (top-level columns only —
    /// recursive children should pass null).
    /// </summary>
    public static int Emit(
        SegmentBuilder sb, IArrowArray array,
        ushort primitiveEncodingIdx, ushort boolEncodingIdx,
        int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        var data = ((Apache.Arrow.Array)array).Data;

        var (valueBytes, alignmentExp) = ExtractValueBytes(array);
        ushort valuesBufIdx = sb.AddBuffer(valueBytes, alignmentExp);

        if (data.GetNullCount() > 0)
        {
            var bitmapBytes = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: array.Length);
            ushort bitmapBufIdx = sb.AddBuffer(bitmapBytes, 0);
            int validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, boolEncodingIdx, bitmapBufIdx);
            return statsTicket is null
                ? ArrayNodeEmitter.EmitWithBufferAndChildren(
                    sb.Builder, primitiveEncodingIdx, valuesBufIdx, new[] { validityNodeTicket })
                : ArrayNodeEmitter.EmitWithBufferChildrenAndStats(
                    sb.Builder, primitiveEncodingIdx, valuesBufIdx,
                    new[] { validityNodeTicket }, statsTicket.Value);
        }

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, primitiveEncodingIdx, valuesBufIdx)
            : ArrayNodeEmitter.EmitWithSingleBufferAndStats(
                sb.Builder, primitiveEncodingIdx, valuesBufIdx, statsTicket.Value);
    }

    /// <summary>
    /// Convenience: builds a complete segment for one primitive column.
    /// Equivalent to <c>new SegmentBuilder() ... Emit(sb, ...) ... sb.FinishSegment(...)</c>.
    /// </summary>
    public static byte[] Encode(IArrowArray array, ushort primitiveEncodingIdx, ushort boolEncodingIdx)
    {
        var sb = new SegmentBuilder();
        var rootTicket = Emit(sb, array, primitiveEncodingIdx, boolEncodingIdx);
        return sb.FinishSegment(rootTicket);
    }

    private static (byte[] Bytes, byte AlignmentExp) ExtractValueBytes(IArrowArray array)
    {
        int rowCount = array.Length;
        var data = ((Apache.Arrow.Array)array).Data;
        int byteOffset = data.Offset;
        return array switch
        {
            Int8Array => (CopyBytes(data.Buffers[1].Span, byteOffset * 1, rowCount * 1), (byte)0),
            UInt8Array => (CopyBytes(data.Buffers[1].Span, byteOffset * 1, rowCount * 1), (byte)0),
            Int16Array => (CopyBytes(data.Buffers[1].Span, byteOffset * 2, rowCount * 2), (byte)1),
            UInt16Array => (CopyBytes(data.Buffers[1].Span, byteOffset * 2, rowCount * 2), (byte)1),
            Int32Array => (CopyBytes(data.Buffers[1].Span, byteOffset * 4, rowCount * 4), (byte)2),
            UInt32Array => (CopyBytes(data.Buffers[1].Span, byteOffset * 4, rowCount * 4), (byte)2),
            FloatArray => (CopyBytes(data.Buffers[1].Span, byteOffset * 4, rowCount * 4), (byte)2),
            Int64Array => (CopyBytes(data.Buffers[1].Span, byteOffset * 8, rowCount * 8), (byte)3),
            UInt64Array => (CopyBytes(data.Buffers[1].Span, byteOffset * 8, rowCount * 8), (byte)3),
            DoubleArray => (CopyBytes(data.Buffers[1].Span, byteOffset * 8, rowCount * 8), (byte)3),
            _ => throw new NotSupportedException(
                $"vortex.primitive writer doesn't support Arrow array {array.GetType().Name}."),
        };
    }

    private static byte[] CopyBytes(ReadOnlySpan<byte> source, int byteOffset, int length)
    {
        var bytes = new byte[length];
        source.Slice(byteOffset, length).CopyTo(bytes);
        return bytes;
    }
}
