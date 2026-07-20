// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for the <c>vortex.struct</c> ARRAY encoding (distinct from the
/// layout-level <c>vortex.struct</c> at the file root). Reconstructs an
/// <see cref="Apache.Arrow.StructArray"/> by recursively decoding each field
/// child via <see cref="ArrayDecoder.DecodeNode"/>.
///
/// <para>Wire format (per <c>arrays/struct_/vtable/mod.rs</c>):
/// <list type="bullet">
///   <item>0 buffers, empty metadata.</item>
///   <item>If <c>children == nfields</c>: no struct-level validity, all
///     children are field arrays.</item>
///   <item>If <c>children == nfields + 1</c>: child[0] is the struct-level
///     validity (a <c>vortex.bool</c> bitmap), child[1..] are field arrays.</item>
/// </list></para>
/// </summary>
internal static class StructArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.struct expects 0 buffers, got {node.BufferRefCount}.");
        if (expectedType is not StructType structType)
            throw new VortexFormatException(
                $"vortex.struct array requires StructType, got {expectedType}.");

        int nfields = structType.Fields.Count;
        int childCount = node.ChildCount;

        ArrowBuffer nullBuffer;
        int nullCount;
        int firstFieldChildIdx;
        if (childCount == nfields)
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
            firstFieldChildIdx = 0;
        }
        else if (childCount == nfields + 1)
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(0), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, checked((int)expectedRowCount));
            firstFieldChildIdx = 1;
        }
        else
        {
            throw new VortexFormatException(
                $"vortex.struct: expected {nfields} or {nfields + 1} children, got {childCount}.");
        }

        // Recursively decode each field with its declared dtype.
        var fieldArrays = new IArrowArray[nfields];
        for (int i = 0; i < nfields; i++)
        {
            fieldArrays[i] = ArrayDecoder.DecodeNode(
                node.Child(firstFieldChildIdx + i),
                serialized,
                arraySpecs,
                structType.Fields[i].DataType,
                expectedRowCount);
        }

        return new StructArray(
            structType,
            checked((int)expectedRowCount),
            fieldArrays,
            nullBuffer,
            nullCount,
            offset: 0);
    }
}
