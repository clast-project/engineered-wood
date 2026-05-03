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

    private static IArrowArray AddReference(
        IArrowType type, IArrowArray encoded, int rowCount, ScalarValueProto reference)
    {
        return (type, reference.Kind, encoded) switch
        {
            (Int8Type, ScalarValueKind.Int64, Int8Array a) => Shift<sbyte>(rowCount, i => a.GetValue(i)!.Value,
                (sbyte)reference.Int64Value, (b, n) => (sbyte)(b + n),
                (data, len) => new Int8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (Int16Type, ScalarValueKind.Int64, Int16Array a) => Shift<short>(rowCount, i => a.GetValue(i)!.Value,
                (short)reference.Int64Value, (b, n) => (short)(b + n),
                (data, len) => new Int16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (Int32Type, ScalarValueKind.Int64, Int32Array a) => Shift<int>(rowCount, i => a.GetValue(i)!.Value,
                (int)reference.Int64Value, (b, n) => b + n,
                (data, len) => new Int32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (Int64Type, ScalarValueKind.Int64, Int64Array a) => Shift<long>(rowCount, i => a.GetValue(i)!.Value,
                reference.Int64Value, (b, n) => b + n,
                (data, len) => new Int64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt8Type, ScalarValueKind.UInt64, UInt8Array a) => Shift<byte>(rowCount, i => a.GetValue(i)!.Value,
                (byte)reference.UInt64Value, (b, n) => (byte)(b + n),
                (data, len) => new UInt8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt16Type, ScalarValueKind.UInt64, UInt16Array a) => Shift<ushort>(rowCount, i => a.GetValue(i)!.Value,
                (ushort)reference.UInt64Value, (b, n) => (ushort)(b + n),
                (data, len) => new UInt16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt32Type, ScalarValueKind.UInt64, UInt32Array a) => Shift<uint>(rowCount, i => a.GetValue(i)!.Value,
                (uint)reference.UInt64Value, (b, n) => b + n,
                (data, len) => new UInt32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt64Type, ScalarValueKind.UInt64, UInt64Array a) => Shift<ulong>(rowCount, i => a.GetValue(i)!.Value,
                reference.UInt64Value, (b, n) => b + n,
                (data, len) => new UInt64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            _ => throw new NotSupportedException(
                $"fastlanes.for: unsupported combination ({type}, ref={reference.Kind}, encoded={encoded.GetType().Name})."),
        };
    }

    private static IArrowArray Shift<T>(
        int rowCount,
        Func<int, T> getEncoded,
        T reference,
        Func<T, T, T> add,
        Func<byte[], int, IArrowArray> ctor)
        where T : unmanaged
    {
        var bytes = new byte[(long)rowCount * Marshal.SizeOf<T>()];
        var span = MemoryMarshal.Cast<byte, T>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++)
            span[i] = add(reference, getEncoded(i));
        return ctor(bytes, rowCount);
    }
}
