// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.masked</c>: overlays a validity bitmap onto an
/// otherwise-non-null child array. Per upstream
/// <c>vortex-array/src/arrays/masked/</c>, the encoding's invariants are:
/// <list type="bullet">
///   <item>The child has no actual null values (its underlying dtype may be
///     either nullable or non-nullable, but every row is valid).</item>
///   <item>The masked array's dtype is the child's dtype made nullable.</item>
///   <item>Validity comes from a separate child array of dtype <c>Bool</c>
///     (non-nullable) — typically <c>vortex.bool</c>, though a
///     <c>vortex.constant</c> or other Bool-valued encoding is permitted.</item>
/// </list>
///
/// <para>Wire shape: 0 buffers, empty metadata, 1 or 2 children.
/// <list type="bullet">
///   <item>1 child: <c>[data]</c> — no explicit validity. Per upstream
///     <c>deserialize</c>: <c>validity = Validity::from(dtype.nullability())</c>,
///     which for both Nullable and NonNullable resolves to AllValid. So the
///     decoder just returns the inner array unchanged.</item>
///   <item>2 children: <c>[data, validity]</c> — overlay the validity bitmap
///     on the inner array, producing an Apache.Arrow array with the same
///     value buffer(s) but a fresh null bitmap.</item>
/// </list></para>
///
/// <para>Apache.Arrow's typed array classes don't differentiate
/// nullable-vs-non-nullable at the array level — every <c>Int32Array</c> etc.
/// carries a (possibly empty) validity bitmap — so the "make-nullable" wrap
/// reduces to swapping <c>ArrayData.Buffers[0]</c> for the new bitmap and
/// rebuilding the typed Arrow array around the resulting <c>ArrayData</c>.</para>
/// </summary>
internal static class MaskedArrayDecoder
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
                $"vortex.masked expects 0 buffers, got {node.BufferRefCount}.");
        if (node.Metadata.Length != 0)
            throw new VortexFormatException(
                $"vortex.masked expects empty metadata, got {node.Metadata.Length} bytes.");
        if (node.ChildCount < 1 || node.ChildCount > 2)
            throw new VortexFormatException(
                $"vortex.masked expects 1 or 2 children, got {node.ChildCount}.");

        // child[0] = the inner array. Upstream's invariant says the child has
        // a non-nullable dtype; since Apache.Arrow doesn't carry nullability
        // at the array level, we decode using the masked array's overall
        // expected type and rely on the downstream typed array to expose the
        // (possibly null) values either way.
        var inner = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, expectedType, expectedRowCount);

        if (node.ChildCount == 1)
        {
            // Per upstream: missing validity child ⇒ AllValid. Return inner
            // unchanged — its existing null bitmap is empty / all-valid.
            return inner;
        }

        // child[1] = validity. Decode through the dispatcher so any Bool-valued
        // encoding works (vortex.bool, vortex.constant, etc.). The result must
        // be a BooleanArray; we then pull its value buffer (the bitmap) out
        // and overlay it as the new null bitmap on `inner`.
        var validityArray = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs, BooleanType.Default, expectedRowCount);
        if (validityArray is not BooleanArray boolArr)
            throw new VortexFormatException(
                $"vortex.masked validity child must decode to BooleanArray, got {validityArray.GetType().Name}.");

        var validityBuffer = boolArr.ValueBuffer;
        int rowCount = checked((int)expectedRowCount);
        int nullCount = BoolArrayDecoder.CountNulls(validityBuffer.Span, rowCount);

        return RebuildWithValidity(inner, validityBuffer, nullCount);
    }

    /// <summary>
    /// Returns a new typed Arrow array with the same buffers + children as
    /// <paramref name="inner"/> but its <c>Buffers[0]</c> swapped for
    /// <paramref name="validity"/>. Per upstream's invariant the inner
    /// array's null bitmap is already empty / all-valid, so the swap is
    /// always safe.
    /// </summary>
    private static IArrowArray RebuildWithValidity(
        IArrowArray inner, ArrowBuffer validity, int nullCount)
    {
        var data = ((Apache.Arrow.Array)inner).Data;

        // Clone Buffers, swapping slot 0 (the validity bitmap) for the new
        // mask. Defensive: every Apache.Arrow primitive carries Buffers[0]
        // for validity and Buffers[1+] for values, but if a future child type
        // only reports values we still want a valid bitmap slot.
        ArrowBuffer[] newBuffers;
        if (data.Buffers.Length == 0)
        {
            newBuffers = new[] { validity };
        }
        else
        {
            newBuffers = new ArrowBuffer[data.Buffers.Length];
            System.Array.Copy(data.Buffers, newBuffers, data.Buffers.Length);
            newBuffers[0] = validity;
        }

        var newData = new ArrayData(
            data.DataType, data.Length, nullCount, data.Offset, newBuffers, data.Children);
        return WrapArrayData(newData);
    }

    /// <summary>
    /// Constructs the appropriate Apache.Arrow concrete <c>IArrowArray</c>
    /// for <paramref name="data"/>. Centralised here so the dtype switch
    /// stays in one place; new dtypes get added as fixtures arrive.
    /// </summary>
    private static IArrowArray WrapArrayData(ArrayData data) => data.DataType switch
    {
        Int8Type => new Int8Array(data),
        Int16Type => new Int16Array(data),
        Int32Type => new Int32Array(data),
        Int64Type => new Int64Array(data),
        UInt8Type => new UInt8Array(data),
        UInt16Type => new UInt16Array(data),
        UInt32Type => new UInt32Array(data),
        UInt64Type => new UInt64Array(data),
        FloatType => new FloatArray(data),
        DoubleType => new DoubleArray(data),
        BooleanType => new BooleanArray(data),
        StringType => new StringArray(data),
        BinaryType => new BinaryArray(data),
        // Decimal128Type / Decimal256Type both inherit from FixedSizeBinaryType,
        // so they MUST be matched before any FixedSizeBinaryType case.
        Decimal128Type => new Decimal128Array(data),
        Decimal256Type => new Decimal256Array(data),
        _ => throw new NotSupportedException(
            $"vortex.masked decoder doesn't yet support Arrow type {data.DataType}. "
            + "Add a case in WrapArrayData and a fixture that exercises it."),
    };
}
