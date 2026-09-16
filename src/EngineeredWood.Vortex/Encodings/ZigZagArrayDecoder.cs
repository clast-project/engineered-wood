// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.zigzag</c> (edition <c>core2025.05.0</c>), which upstream's default
/// compressor applies to signed integers ahead of bit-packing, so that values near zero of
/// either sign pack into few bits. Wire format (per <c>encodings/zigzag/src/array.rs</c>):
/// <list type="bullet">
///   <item>no buffers and empty metadata</item>
///   <item>1 child: the encoded values, the unsigned integer of the same width and nullability,
///     which carries the column's validity</item>
/// </list>
///
/// <para>Each value decodes as <c>(u &gt;&gt; 1) ^ -(u &amp; 1)</c>: 0, 1, 2, 3, … map back to
/// 0, -1, 1, -2, ….</para>
/// </summary>
internal static class ZigZagArrayDecoder
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
                $"vortex.zigzag expects no buffers, got {node.BufferRefCount}.");
        if (node.ChildCount != 1)
            throw new VortexFormatException(
                $"vortex.zigzag expects 1 child (encoded), got {node.ChildCount}.");
        if (node.Metadata.Length != 0)
            throw new VortexFormatException(
                $"vortex.zigzag expects empty metadata, got {node.Metadata.Length} bytes.");

        IArrowType encodedType = expectedType switch
        {
            Int8Type => UInt8Type.Default,
            Int16Type => UInt16Type.Default,
            Int32Type => UInt32Type.Default,
            Int64Type => UInt64Type.Default,
            _ => throw new NotSupportedException(
                $"vortex.zigzag decodes signed integers only, got {expectedType}."),
        };

        var encoded = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, encodedType, expectedRowCount);
        if (encoded.Length != expectedRowCount)
            throw new VortexFormatException(
                $"vortex.zigzag encoded child has {encoded.Length} values, expected {expectedRowCount}.");

        return encoded switch
        {
            UInt8Array u8 => Build(u8, u8.Values, (byte v) => (sbyte)((v >> 1) ^ -(v & 1)),
                (data, validity, nulls) => new Int8Array(data, validity, u8.Length, nulls, 0)),
            UInt16Array u16 => Build(u16, u16.Values, (ushort v) => (short)((v >> 1) ^ -(v & 1)),
                (data, validity, nulls) => new Int16Array(data, validity, u16.Length, nulls, 0)),
            UInt32Array u32 => Build(u32, u32.Values, (uint v) => (int)(v >> 1) ^ -(int)(v & 1),
                (data, validity, nulls) => new Int32Array(data, validity, u32.Length, nulls, 0)),
            UInt64Array u64 => Build(u64, u64.Values, (ulong v) => (long)(v >> 1) ^ -(long)(v & 1),
                (data, validity, nulls) => new Int64Array(data, validity, u64.Length, nulls, 0)),
            _ => throw new VortexFormatException(
                $"vortex.zigzag encoded child decoded to {encoded.GetType().Name}, expected {encodedType}."),
        };
    }

    private static IArrowArray Build<TIn, TOut>(
        IArrowArray encoded,
        ReadOnlySpan<TIn> values,
        Func<TIn, TOut> decode,
        Func<ArrowBuffer, ArrowBuffer, int, IArrowArray> ctor)
        where TIn : struct
        where TOut : struct
    {
        var output = new TOut[values.Length];
        for (int i = 0; i < values.Length; i++)
            output[i] = decode(values[i]);
        var data = new ArrowBuffer(MemoryMarshal.AsBytes(output.AsSpan()).ToArray());
        var (validity, nulls) = Validity(encoded);
        return ctor(data, validity, nulls);
    }

    /// <summary>The child's validity, rebased to offset 0 so it lines up with the new values.</summary>
    private static (ArrowBuffer Validity, int NullCount) Validity(IArrowArray encoded)
    {
        int nulls = encoded.NullCount;
        if (nulls == 0)
            return (ArrowBuffer.Empty, 0);
        if (encoded.Offset == 0)
            return (encoded.Data.Buffers[0], nulls);

        var bitmap = new ArrowBuffer.BitmapBuilder(encoded.Length);
        for (int i = 0; i < encoded.Length; i++)
            bitmap.Append(encoded.IsValid(i));
        return (bitmap.Build(), nulls);
    }
}
