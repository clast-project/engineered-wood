// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.BoolArrayDecoder"/>:
/// emits a <c>vortex.bool</c> ArrayNode subtree for an Arrow
/// <see cref="BooleanArray"/>, nullable or not.
///
/// <para>Wire shape: 1 buffer (the values bitmap, LSB-first per byte) plus
/// 0 or 1 children. For nullable inputs the single child is a nested
/// <c>vortex.bool</c> ArrayNode whose own buffer holds the validity bitmap.</para>
/// </summary>
internal static class BoolArrayEncoder
{
    public static int Emit(SegmentBuilder sb, IArrowArray array, ushort boolEncodingIdx, int? statsTicket = null)
    {
        if (array is not BooleanArray b)
            throw new NotSupportedException(
                $"vortex.bool writer requires Apache.Arrow.BooleanArray, got {array.GetType().Name}.");

        var data = b.Data;
        int rowCount = array.Length;

        // Apache.Arrow stores BooleanArray values as a packed bitmap at
        // Buffers[1] (and validity at Buffers[0]). For sliced inputs the
        // visible bits start at data.Offset. ExtractValidityBitmap works for
        // any LSB-first bitmap, not just validity.
        var valuesBitmap = EncoderHelpers.ExtractValidityBitmap(
            data.Buffers[1].Span, srcBitOffset: data.Offset, rowCount: rowCount);
        ushort valuesBufIdx = sb.AddBuffer(valuesBitmap, alignmentExponent: 0);

        if (data.GetNullCount() > 0)
        {
            var validityBitmap = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: rowCount);
            ushort validityBufIdx = sb.AddBuffer(validityBitmap, alignmentExponent: 0);
            int validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, boolEncodingIdx, validityBufIdx);
            return statsTicket is null
                ? ArrayNodeEmitter.EmitWithBufferAndChildren(
                    sb.Builder, boolEncodingIdx, valuesBufIdx, new[] { validityNodeTicket })
                : ArrayNodeEmitter.EmitWithBufferChildrenAndStats(
                    sb.Builder, boolEncodingIdx, valuesBufIdx,
                    new[] { validityNodeTicket }, statsTicket.Value);
        }

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, boolEncodingIdx, valuesBufIdx)
            : ArrayNodeEmitter.EmitWithSingleBufferAndStats(
                sb.Builder, boolEncodingIdx, valuesBufIdx, statsTicket.Value);
    }

    /// <summary>Convenience: encode one column's segment in isolation.</summary>
    public static byte[] Encode(IArrowArray array, ushort boolEncodingIdx)
    {
        var sb = new SegmentBuilder();
        var rootTicket = Emit(sb, array, boolEncodingIdx);
        return sb.FinishSegment(rootTicket);
    }
}
