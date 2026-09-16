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

        var (validity, nulls) = Validity(encoded);
        int length = encoded.Length;
        switch (encoded)
        {
            case UInt8Array u8:
                {
                    var bytes = new byte[length];
                    var src = u8.Values;
                    var dst = MemoryMarshal.Cast<byte, sbyte>(bytes.AsSpan());
                    for (int i = 0; i < src.Length; i++)
                        dst[i] = (sbyte)((src[i] >> 1) ^ -(src[i] & 1));
                    return new Int8Array(new ArrowBuffer(bytes), validity, length, nulls, 0);
                }
            case UInt16Array u16:
                {
                    var bytes = new byte[(long)length * sizeof(short)];
                    var src = u16.Values;
                    var dst = MemoryMarshal.Cast<byte, short>(bytes.AsSpan());
                    for (int i = 0; i < src.Length; i++)
                        dst[i] = (short)((src[i] >> 1) ^ -(src[i] & 1));
                    return new Int16Array(new ArrowBuffer(bytes), validity, length, nulls, 0);
                }
            case UInt32Array u32:
                {
                    var bytes = new byte[(long)length * sizeof(int)];
                    var src = u32.Values;
                    var dst = MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
                    for (int i = 0; i < src.Length; i++)
                        dst[i] = (int)(src[i] >> 1) ^ -(int)(src[i] & 1);
                    return new Int32Array(new ArrowBuffer(bytes), validity, length, nulls, 0);
                }
            case UInt64Array u64:
                {
                    var bytes = new byte[(long)length * sizeof(long)];
                    var src = u64.Values;
                    var dst = MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
                    for (int i = 0; i < src.Length; i++)
                        dst[i] = (long)(src[i] >> 1) ^ -(long)(src[i] & 1);
                    return new Int64Array(new ArrowBuffer(bytes), validity, length, nulls, 0);
                }
            default:
                throw new VortexFormatException(
                    $"vortex.zigzag encoded child decoded to {encoded.GetType().Name}, expected {encodedType}.");
        }
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
