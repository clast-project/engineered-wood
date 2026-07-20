// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.FixedSizeListArrayDecoder"/>:
/// emits a <c>vortex.fixed_size_list</c> ArrayNode subtree.
///
/// <para>Wire shape: 0 buffers on the FSL itself; children = [elements,
/// validity?]; empty metadata. The element count is implied
/// (<c>parent_len × list_size</c>) — <c>list_size</c> comes from the dtype.</para>
/// </summary>
internal static class FixedSizeListArrayEncoder
{
    public static int Emit(SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is not FixedSizeListArray fsl)
            throw new NotSupportedException(
                $"vortex.fixed_size_list writer requires Apache.Arrow.FixedSizeListArray, got {array.GetType().Name}.");

        var data = fsl.Data;
        int rowCount = fsl.Length;
        var fslType = (FixedSizeListType)fsl.Data.DataType;
        int listSize = fslType.ListSize;

        // The full elements array; for sliced parents we restrict to the visible
        // window [data.Offset * listSize, data.Offset * listSize + rowCount * listSize).
        var fullElements = fsl.Values;
        int elementsStart = data.Offset * listSize;
        int elementsLen = rowCount * listSize;
        IArrowArray slicedElements = elementsStart == 0 && elementsLen == fullElements.Length
            ? fullElements
            : ((Apache.Arrow.Array)fullElements).Slice(elementsStart, elementsLen);

        int elementsTicket = ArrayEncoderDispatch.Emit(sb, slicedElements, idx);

        int? validityNodeTicket = null;
        if (data.GetNullCount() > 0)
        {
            var bitmapBytes = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: rowCount);
            ushort bitmapBufIdx = sb.AddBuffer(bitmapBytes, 0);
            validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, idx.Bool, bitmapBufIdx);
        }

        var childTickets = validityNodeTicket is null
            ? new[] { elementsTicket }
            : new[] { elementsTicket, validityNodeTicket.Value };

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithChildrenOnly(sb.Builder, idx.FixedSizeList, childTickets)
            : ArrayNodeEmitter.EmitWithChildrenAndStats(
                sb.Builder, idx.FixedSizeList, childTickets, statsTicket.Value);
    }
}
