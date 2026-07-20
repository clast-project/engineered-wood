// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V21;

namespace EngineeredWood.Lance.Encodings.V21;

/// <summary>
/// Decodes a Lance v2.1 <see cref="ConstantLayout"/> page. The current
/// scope is the all-null case pylance emits for columns whose entire
/// page is null: <c>layers = [NULLABLE_ITEM]</c>, <c>inline_value</c>
/// absent, and no rep/def buffers — every row at this page is null.
///
/// <para>The spec also permits an <c>inline_value</c> (≤ 32 bytes) that
/// repeats across rows, plus rep/def buffers for partial-null shapes.
/// Pylance doesn't emit those today; both throw with a clear message
/// pointing at the missing handler.</para>
/// </summary>
internal static class ConstantLayoutDecoder
{
    public static IArrowArray Decode(
        ConstantLayout layout, long numRows, IArrowType targetType)
    {
        if (layout.RepCompression is not null || layout.DefCompression is not null)
            throw new NotImplementedException(
                "ConstantLayout with rep/def buffers is not yet supported.");
        if (layout.NumRepValues != 0 || layout.NumDefValues != 0)
            throw new NotImplementedException(
                $"ConstantLayout with num_rep_values={layout.NumRepValues} / " +
                $"num_def_values={layout.NumDefValues} is not yet supported.");

        if (layout.Layers.Count != 1
            || layout.Layers[0] != RepDefLayer.RepdefNullableItem)
            throw new NotImplementedException(
                $"ConstantLayout with layers other than [NULLABLE_ITEM] is not yet supported " +
                $"(got [{string.Join(", ", layout.Layers)}]).");

        if (layout.HasInlineValue)
            throw new NotImplementedException(
                "ConstantLayout with inline_value present is not yet supported " +
                "(pylance only emits the all-null variant today).");

        // All-null page: build a typed Arrow array of `numRows` nulls.
        int length = checked((int)numRows);
        if (length == 0)
            return BuildEmptyArray(targetType);

        byte[] allZeroValidity = new byte[(length + 7) / 8];
        var validityBuf = new ArrowBuffer(allZeroValidity);

        if (targetType is StringType or BinaryType)
        {
            byte[] offsetsZero = new byte[(length + 1) * sizeof(int)];
            // offsets stay all zero (every string is empty + nulled out)
            var data = new ArrayData(
                targetType, length, length, offset: 0,
                new[] { validityBuf, new ArrowBuffer(offsetsZero), ArrowBuffer.Empty });
            return ArrowArrayFactory.BuildArray(data);
        }

        if (targetType is FixedWidthType fw)
        {
            int bytesPerValue = fw.BitWidth / 8;
            byte[] valueZero = new byte[length * bytesPerValue];
            var data = new ArrayData(
                targetType, length, length, offset: 0,
                new[] { validityBuf, new ArrowBuffer(valueZero) });
            return ArrowArrayFactory.BuildArray(data);
        }

        throw new NotImplementedException(
            $"ConstantLayout(all-null) for target type {targetType} is not yet supported.");
    }

    private static IArrowArray BuildEmptyArray(IArrowType targetType)
    {
        // Defensive: numRows = 0 is unlikely (Lance pages have a positive
        // row count), but produce a valid empty array if it does occur.
        if (targetType is StringType or BinaryType)
        {
            var data = new ArrayData(
                targetType, length: 0, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, new ArrowBuffer(new byte[sizeof(int)]), ArrowBuffer.Empty });
            return ArrowArrayFactory.BuildArray(data);
        }
        var emptyData = new ArrayData(
            targetType, length: 0, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, ArrowBuffer.Empty });
        return ArrowArrayFactory.BuildArray(emptyData);
    }
}
