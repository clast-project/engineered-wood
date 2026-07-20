// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.constant</c>: a single scalar broadcast over N rows.
/// The scalar is stored as a vortex-proto <c>ScalarValue</c> in buffer 0 (no
/// metadata). See <c>vortex-array/src/arrays/constant/vtable/mod.rs</c>.
/// </summary>
internal static class ConstantArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.BufferRefCount != 1)
            throw new VortexFormatException(
                $"vortex.constant ArrayNode should have 1 buffer ref, got {node.BufferRefCount}.");
        if (node.ChildCount != 0)
            throw new VortexFormatException(
                $"vortex.constant ArrayNode should have no children, got {node.ChildCount}.");

        var bufferRef = node.BufferRef(0);
        var bufferDesc = serialized.Message.Buffer(bufferRef);
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.constant buffer compression {bufferDesc.Compression} not yet implemented.");

        var data = serialized.BufferBytes(bufferRef);
        var scalar = ScalarValueProto.Parse(data);
        return BuildArray(expectedType, expectedRowCount, scalar);
    }

    private static IArrowArray BuildArray(IArrowType type, long rowCount, ScalarValueProto scalar)
    {
        var n = checked((int)rowCount);
        return (type, scalar.Kind) switch
        {
            (Int8Type, ScalarValueKind.Int64) => Filled<sbyte>(n,
                (data, len) => new Int8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (sbyte)scalar.Int64Value),
            (Int16Type, ScalarValueKind.Int64) => Filled<short>(n,
                (data, len) => new Int16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (short)scalar.Int64Value),
            (Int32Type, ScalarValueKind.Int64) => Filled<int>(n,
                (data, len) => new Int32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (int)scalar.Int64Value),
            (Int64Type, ScalarValueKind.Int64) => Filled<long>(n,
                (data, len) => new Int64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                scalar.Int64Value),
            (UInt8Type, ScalarValueKind.UInt64) => Filled<byte>(n,
                (data, len) => new UInt8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (byte)scalar.UInt64Value),
            (UInt16Type, ScalarValueKind.UInt64) => Filled<ushort>(n,
                (data, len) => new UInt16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (ushort)scalar.UInt64Value),
            (UInt32Type, ScalarValueKind.UInt64) => Filled<uint>(n,
                (data, len) => new UInt32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (uint)scalar.UInt64Value),
            (UInt64Type, ScalarValueKind.UInt64) => Filled<ulong>(n,
                (data, len) => new UInt64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                scalar.UInt64Value),
            (FloatType, ScalarValueKind.F32) => Filled<float>(n,
                (data, len) => new FloatArray(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                scalar.F32Value),
            (DoubleType, ScalarValueKind.F64) => Filled<double>(n,
                (data, len) => new DoubleArray(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                scalar.F64Value),
            (BooleanType, ScalarValueKind.Bool) => BuildBool(n, scalar.BoolValue),
            // Repeated-string / repeated-binary constants typically arise as
            // a zone-stats Min or Max when every zone has the same lex-min /
            // lex-max (or is the only zone). vortex's writer collapses both
            // into vortex.constant carrying a single StringValue / BytesValue.
            (StringType, ScalarValueKind.String) => BuildVarBin(
                n, isString: true, System.Text.Encoding.UTF8.GetBytes(scalar.StringValue ?? string.Empty)),
            (BinaryType, ScalarValueKind.Bytes) => BuildVarBin(
                n, isString: false, scalar.BytesValue ?? System.Array.Empty<byte>()),
            _ => throw new NotSupportedException(
                $"vortex.constant decoder doesn't support Arrow {type} with ScalarValue {scalar.Kind}."),
        };
    }

    /// <summary>
    /// Builds a repeated-string / repeated-binary array of length
    /// <paramref name="rowCount"/>. Offsets are <c>{0, len, 2*len, …, n*len}</c>;
    /// the values buffer is the bytes repeated once per row.
    /// </summary>
    private static IArrowArray BuildVarBin(int rowCount, bool isString, byte[] valueBytes)
    {
        // Offsets buffer: (n+1) i32 values stepping by valueBytes.Length each row.
        var offsetsBytes = new byte[(long)(rowCount + 1) * 4];
        var offsetsSpan = MemoryMarshal.Cast<byte, int>(offsetsBytes.AsSpan());
        for (int i = 0; i <= rowCount; i++) offsetsSpan[i] = i * valueBytes.Length;

        // Values buffer: valueBytes copied rowCount times. For empty strings
        // (valueBytes.Length == 0) this is a zero-length buffer.
        var valuesBytes = new byte[(long)rowCount * valueBytes.Length];
        for (int i = 0; i < rowCount && valueBytes.Length > 0; i++)
            Buffer.BlockCopy(valueBytes, 0, valuesBytes, i * valueBytes.Length, valueBytes.Length);

        var offsetsBuf = new ArrowBuffer(offsetsBytes);
        var valuesBuf = new ArrowBuffer(valuesBytes);
        return isString
            ? new StringArray(rowCount, offsetsBuf, valuesBuf, ArrowBuffer.Empty, 0, 0)
            : new BinaryArray(BinaryType.Default, rowCount, offsetsBuf, valuesBuf, ArrowBuffer.Empty, 0, 0);
    }

    private static IArrowArray Filled<T>(
        int rowCount, Func<byte[], int, IArrowArray> ctor, T value) where T : struct
    {
        var bytes = new byte[(long)rowCount * Marshal.SizeOf<T>()];
        var span = MemoryMarshal.Cast<byte, T>(bytes.AsSpan());
        for (int i = 0; i < span.Length; i++) span[i] = value;
        return ctor(bytes, rowCount);
    }

    private static IArrowArray BuildBool(int rowCount, bool value)
    {
        var byteCount = (rowCount + 7) / 8;
        var bytes = new byte[byteCount];
        if (value)
        {
            for (int i = 0; i < rowCount; i++)
                bytes[i / 8] |= (byte)(1 << (i % 8));
        }
        return new BooleanArray(new ArrowBuffer(bytes), ArrowBuffer.Empty, rowCount, 0, 0);
    }
}
