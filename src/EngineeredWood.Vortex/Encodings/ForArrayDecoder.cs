// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>fastlanes.for</c> (Frame of Reference): subtracts a
/// reference value from each row at encode time, encodes the (small) deltas,
/// and adds the reference back at decode time.
///
/// <para>Wire format: 0 buffers, 1 child (the encoded deltas — typically
/// <c>fastlanes.bitpacked</c>). Metadata is a vortex-proto <c>ScalarValue</c>
/// of the SAME dtype as the parent — the reference scalar.</para>
/// </summary>
internal static class ForArrayDecoder
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
                $"fastlanes.for expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount != 1)
            throw new VortexFormatException(
                $"fastlanes.for expects 1 child (encoded deltas), got {node.ChildCount}.");

        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException(
                "fastlanes.for ArrayNode has empty metadata; expected ScalarValue proto.");
        var reference = ScalarValueProto.Parse(metaVec.RawBytes(metaVec.Length));

        // Decode the encoded child (same dtype as parent).
        var encoded = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, expectedType, expectedRowCount);

        var rowCount = checked((int)expectedRowCount);
        return AddReference(expectedType, encoded, rowCount, reference);
    }

    /// <summary>
    /// Adds the reference scalar to the encoded residuals to recover the
    /// original values. Reads raw value bytes from <paramref name="encoded"/>'s
    /// data buffer (avoiding any per-row null check) and preserves the
    /// encoded child's validity bitmap so nullable FoR survives the read path.
    /// Null-position residual bytes can be anything (the validity bitmap masks
    /// them at the consumer); we still compute <c>ref + residual</c> at those
    /// slots to keep the loop branch-free.
    /// </summary>
    private static IArrowArray AddReference(
        IArrowType type, IArrowArray encoded, int rowCount, ScalarValueProto reference)
    {
        var encData = ((Apache.Arrow.Array)encoded).Data;
        var nullBuffer = encData.Buffers.Length > 0 ? encData.Buffers[0] : ArrowBuffer.Empty;
        int nullCount = encData.GetNullCount();
        if (nullCount < 0) nullCount = 0;

        return (type, reference.Kind) switch
        {
            (Int8Type, ScalarValueKind.Int64) => ShiftSigned8(rowCount, encData, (sbyte)reference.Int64Value, nullBuffer, nullCount),
            (Int16Type, ScalarValueKind.Int64) => ShiftSigned16(rowCount, encData, (short)reference.Int64Value, nullBuffer, nullCount),
            (Int32Type, ScalarValueKind.Int64) => ShiftSigned32(rowCount, encData, (int)reference.Int64Value, nullBuffer, nullCount),
            (Int64Type, ScalarValueKind.Int64) => ShiftSigned64(rowCount, encData, reference.Int64Value, nullBuffer, nullCount),
            (UInt8Type, ScalarValueKind.UInt64) => ShiftUnsigned8(rowCount, encData, (byte)reference.UInt64Value, nullBuffer, nullCount),
            (UInt16Type, ScalarValueKind.UInt64) => ShiftUnsigned16(rowCount, encData, (ushort)reference.UInt64Value, nullBuffer, nullCount),
            (UInt32Type, ScalarValueKind.UInt64) => ShiftUnsigned32(rowCount, encData, (uint)reference.UInt64Value, nullBuffer, nullCount),
            (UInt64Type, ScalarValueKind.UInt64) => ShiftUnsigned64(rowCount, encData, reference.UInt64Value, nullBuffer, nullCount),
            _ => throw new NotSupportedException(
                $"fastlanes.for: unsupported combination ({type}, ref={reference.Kind}, encoded={encoded.GetType().Name})."),
        };
    }

    private static Int8Array ShiftSigned8(int rowCount, ArrayData encData, sbyte reference, ArrowBuffer nullBuffer, int nullCount)
    {
        var src = MemoryMarshal.Cast<byte, sbyte>(encData.Buffers[1].Span.Slice(0, rowCount));
        var bytes = new byte[rowCount];
        var dst = MemoryMarshal.Cast<byte, sbyte>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++) dst[i] = (sbyte)(reference + src[i]);
        return new Int8Array(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }

    private static Int16Array ShiftSigned16(int rowCount, ArrayData encData, short reference, ArrowBuffer nullBuffer, int nullCount)
    {
        var src = MemoryMarshal.Cast<byte, short>(encData.Buffers[1].Span.Slice(0, rowCount * 2));
        var bytes = new byte[rowCount * 2];
        var dst = MemoryMarshal.Cast<byte, short>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++) dst[i] = (short)(reference + src[i]);
        return new Int16Array(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }

    private static Int32Array ShiftSigned32(int rowCount, ArrayData encData, int reference, ArrowBuffer nullBuffer, int nullCount)
    {
        var src = MemoryMarshal.Cast<byte, int>(encData.Buffers[1].Span.Slice(0, rowCount * 4));
        var bytes = new byte[rowCount * 4];
        var dst = MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++) dst[i] = reference + src[i];
        return new Int32Array(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }

    private static Int64Array ShiftSigned64(int rowCount, ArrayData encData, long reference, ArrowBuffer nullBuffer, int nullCount)
    {
        var src = MemoryMarshal.Cast<byte, long>(encData.Buffers[1].Span.Slice(0, rowCount * 8));
        var bytes = new byte[rowCount * 8];
        var dst = MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++) dst[i] = reference + src[i];
        return new Int64Array(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }

    private static UInt8Array ShiftUnsigned8(int rowCount, ArrayData encData, byte reference, ArrowBuffer nullBuffer, int nullCount)
    {
        var src = encData.Buffers[1].Span.Slice(0, rowCount);
        var bytes = new byte[rowCount];
        for (int i = 0; i < rowCount; i++) bytes[i] = (byte)(reference + src[i]);
        return new UInt8Array(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }

    private static UInt16Array ShiftUnsigned16(int rowCount, ArrayData encData, ushort reference, ArrowBuffer nullBuffer, int nullCount)
    {
        var src = MemoryMarshal.Cast<byte, ushort>(encData.Buffers[1].Span.Slice(0, rowCount * 2));
        var bytes = new byte[rowCount * 2];
        var dst = MemoryMarshal.Cast<byte, ushort>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++) dst[i] = (ushort)(reference + src[i]);
        return new UInt16Array(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }

    private static UInt32Array ShiftUnsigned32(int rowCount, ArrayData encData, uint reference, ArrowBuffer nullBuffer, int nullCount)
    {
        var src = MemoryMarshal.Cast<byte, uint>(encData.Buffers[1].Span.Slice(0, rowCount * 4));
        var bytes = new byte[rowCount * 4];
        var dst = MemoryMarshal.Cast<byte, uint>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++) dst[i] = reference + src[i];
        return new UInt32Array(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }

    private static UInt64Array ShiftUnsigned64(int rowCount, ArrayData encData, ulong reference, ArrowBuffer nullBuffer, int nullCount)
    {
        var src = MemoryMarshal.Cast<byte, ulong>(encData.Buffers[1].Span.Slice(0, rowCount * 8));
        var bytes = new byte[rowCount * 8];
        var dst = MemoryMarshal.Cast<byte, ulong>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++) dst[i] = reference + src[i];
        return new UInt64Array(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }
}
