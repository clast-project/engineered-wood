// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.fixed_size_list</c>: a fixed-size list of N inner
/// elements per row. 0 buffers, 1-2 children (elements + optional validity),
/// empty metadata. Element count = <c>parent_len × list_size</c>.
/// </summary>
internal static class FixedSizeListArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not FixedSizeListType fsl)
            throw new VortexFormatException(
                $"vortex.fixed_size_list requires FixedSizeListType, got {expectedType}.");
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.fixed_size_list expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount is not 1 and not 2)
            throw new VortexFormatException(
                $"vortex.fixed_size_list expects 1 or 2 children, got {node.ChildCount}.");

        var rowCount = checked((int)expectedRowCount);
        var elementCount = (long)rowCount * fsl.ListSize;
        var elementType = fsl.Fields[0].DataType;
        var elements = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, elementType, elementCount);

        ArrowBuffer nullBuffer; int nullCount;
        if (node.ChildCount == 1)
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }
        else
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(1), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }

        var elementsData = ((Apache.Arrow.Array)elements).Data;
        var data = new ArrayData(
            fsl, rowCount, nullCount, offset: 0,
            new[] { nullBuffer },
            new[] { elementsData });
        return new FixedSizeListArray(data);
    }
}
