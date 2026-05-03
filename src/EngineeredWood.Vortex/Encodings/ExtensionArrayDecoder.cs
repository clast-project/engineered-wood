// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.ext</c>: a transparent wrapper around a storage array
/// for an extension dtype (Date / Time / UUID / etc.). Unwraps to child[0]
/// decoded with the storage Arrow type, then re-wraps the underlying buffers
/// as the matching Apache Arrow extension type (Date32Array, Time64Array, …).
/// </summary>
internal static class ExtensionArrayDecoder
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
                $"vortex.ext expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount != 1)
            throw new VortexFormatException(
                $"vortex.ext expects 1 child (the storage array), got {node.ChildCount}.");

        var storageType = StorageTypeFor(expectedType);
        var inner = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, storageType, expectedRowCount);

        return Rewrap(expectedType, inner);
    }

    /// <summary>Maps an extension Arrow type to its underlying storage Arrow type.</summary>
    private static IArrowType StorageTypeFor(IArrowType ext) => ext switch
    {
        Date32Type => Int32Type.Default,
        Date64Type => Int64Type.Default,
        Time32Type => Int32Type.Default,
        Time64Type => Int64Type.Default,
        TimestampType => Int64Type.Default,
        FixedSizeBinaryType fsb =>
            // UUID is stored as FixedSizeList(U8, 16). Decode as FSL of UInt8;
            // Rewrap will pull the byte buffer out and re-package as FSB.
            new FixedSizeListType(new Field("item", UInt8Type.Default, nullable: false), fsb.ByteWidth),
        _ => throw new NotSupportedException(
            $"vortex.ext: no storage type mapping for {ext}."),
    };

    /// <summary>
    /// Re-wraps the inner integer array's value buffer as the extension type.
    /// Apache.Arrow's Date/Time arrays share the same memory layout as Int32/Int64
    /// (just different DataType in the metadata), so we construct a new
    /// <see cref="ArrayData"/> with the extension type and the same buffers.
    /// </summary>
    private static IArrowArray Rewrap(IArrowType extType, IArrowArray inner)
    {
        // UUID: inner is a FixedSizeListArray of UInt8. Pull the underlying byte
        // buffer out and re-wrap as Apache.Arrow.Arrays.FixedSizeBinaryArray.
        if (extType is FixedSizeBinaryType fsb)
        {
            var fslArr = (FixedSizeListArray)inner;
            var fslData = fslArr.Data;
            var elementsData = fslArr.Values.Data;
            // FSL's value buffer is the elements' value buffer (16 × len bytes for UUID).
            var newData = new ArrayData(
                fsb,
                fslArr.Length,
                fslData.NullCount,
                fslData.Offset,
                new[] { fslData.NullCount > 0 ? fslData.Buffers[0] : ArrowBuffer.Empty,
                        elementsData.Buffers[1] });
            return new Apache.Arrow.Arrays.FixedSizeBinaryArray(newData);
        }

        var innerData = ((Apache.Arrow.Array)inner).Data;
        var data = new ArrayData(
            extType,
            innerData.Length,
            innerData.NullCount,
            innerData.Offset,
            innerData.Buffers);

        return extType switch
        {
            Date32Type => new Date32Array(data),
            Date64Type => new Date64Array(data),
            Time32Type => new Time32Array(data),
            Time64Type => new Time64Array(data),
            TimestampType => new TimestampArray(data),
            _ => throw new NotSupportedException(
                $"vortex.ext: cannot rewrap as {extType}."),
        };
    }
}
