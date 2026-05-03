// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// Materializes a <c>vortex.dict</c>-encoded column from its values dictionary
/// and per-row codes. <c>output[i] = values[codes[i]]</c>.
///
/// <para>Phase 1 scope: <see cref="StringType"/> output backed by a
/// <see cref="StringArray"/> dictionary. Other Arrow types land alongside
/// fixtures that need them.</para>
/// </summary>
internal static class DictReconstructor
{
    public static IArrowArray Reconstruct(
        IArrowType arrowType,
        IArrowArray values,
        IArrowArray codes)
    {
        var codeIndices = ResolveCodes(codes);

        return (arrowType, values) switch
        {
            (StringType, _) => ReconstructString(values, codeIndices),
            (Int8Type, Int8Array a8) => Build<sbyte>(codeIndices, i => a8.GetValue(i)!.Value,
                (data, len) => new Int8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (Int16Type, Int16Array a16) => Build<short>(codeIndices, i => a16.GetValue(i)!.Value,
                (data, len) => new Int16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (Int32Type, Int32Array a32) => Build<int>(codeIndices, i => a32.GetValue(i)!.Value,
                (data, len) => new Int32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (Int64Type, Int64Array a64) => Build<long>(codeIndices, i => a64.GetValue(i)!.Value,
                (data, len) => new Int64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt8Type, UInt8Array u8) => Build<byte>(codeIndices, i => u8.GetValue(i)!.Value,
                (data, len) => new UInt8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt16Type, UInt16Array u16) => Build<ushort>(codeIndices, i => u16.GetValue(i)!.Value,
                (data, len) => new UInt16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt32Type, UInt32Array u32) => Build<uint>(codeIndices, i => u32.GetValue(i)!.Value,
                (data, len) => new UInt32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (UInt64Type, UInt64Array u64) => Build<ulong>(codeIndices, i => u64.GetValue(i)!.Value,
                (data, len) => new UInt64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (FloatType, FloatArray f) => Build<float>(codeIndices, i => f.GetValue(i)!.Value,
                (data, len) => new FloatArray(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            (DoubleType, DoubleArray d) => Build<double>(codeIndices, i => d.GetValue(i)!.Value,
                (data, len) => new DoubleArray(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            _ => throw new NotSupportedException(
                $"Dict reconstruction for Arrow type {arrowType} with values {values.GetType().Name} is not yet supported."),
        };
    }

    private static IArrowArray Build<T>(
        int[] codes, Func<int, T> get, Func<byte[], int, IArrowArray> ctor)
        where T : struct
    {
        var elemSize = System.Runtime.InteropServices.Marshal.SizeOf<T>();
        var bytes = new byte[(long)codes.Length * elemSize];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, T>(bytes.AsSpan());
        for (int i = 0; i < codes.Length; i++)
            span[i] = get(codes[i]);
        return ctor(bytes, codes.Length);
    }

    private static int[] ResolveCodes(IArrowArray codes)
    {
        return codes switch
        {
            UInt32Array u32 => CopyUInt32(u32),
            UInt16Array u16 => CopyUInt16(u16),
            UInt8Array u8 => CopyUInt8(u8),
            UInt64Array u64 => CopyUInt64(u64),
            Int32Array i32 => CopyInt32(i32),
            Int64Array i64 => CopyInt64(i64),
            _ => throw new NotSupportedException(
                $"vortex.dict codes type {codes.GetType().Name} is not supported."),
        };
    }

    private static int[] CopyUInt32(UInt32Array a)
    {
        var r = new int[a.Length];
        for (int i = 0; i < a.Length; i++)
            r[i] = checked((int)a.GetValue(i)!.Value);
        return r;
    }

    private static int[] CopyUInt16(UInt16Array a)
    {
        var r = new int[a.Length];
        for (int i = 0; i < a.Length; i++) r[i] = a.GetValue(i)!.Value;
        return r;
    }

    private static int[] CopyUInt8(UInt8Array a)
    {
        var r = new int[a.Length];
        for (int i = 0; i < a.Length; i++) r[i] = a.GetValue(i)!.Value;
        return r;
    }

    private static int[] CopyUInt64(UInt64Array a)
    {
        var r = new int[a.Length];
        for (int i = 0; i < a.Length; i++)
            r[i] = checked((int)a.GetValue(i)!.Value);
        return r;
    }

    private static int[] CopyInt32(Int32Array a)
    {
        var r = new int[a.Length];
        for (int i = 0; i < a.Length; i++) r[i] = a.GetValue(i)!.Value;
        return r;
    }

    private static int[] CopyInt64(Int64Array a)
    {
        var r = new int[a.Length];
        for (int i = 0; i < a.Length; i++)
            r[i] = checked((int)a.GetValue(i)!.Value);
        return r;
    }

    private static IArrowArray ReconstructString(IArrowArray values, int[] codes)
    {
        var dict = values as StringArray
            ?? throw new VortexFormatException(
                $"vortex.dict for StringType expected a StringArray dictionary, got {values.GetType().Name}.");

        // Build offsets + concatenated values from the picked dict entries.
        var offsetBytes = new byte[(codes.Length + 1) * 4];
        using var sink = new System.IO.MemoryStream();
        int total = 0;
        for (int i = 0; i < codes.Length; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4), total);
            var idx = codes[i];
            if ((uint)idx >= (uint)dict.Length)
                throw new VortexFormatException(
                    $"vortex.dict code {idx} at row {i} is out of range (dict has {dict.Length} entries).");
            var bytes = dict.GetBytes(idx);
#if NET8_0_OR_GREATER
            sink.Write(bytes);
#else
            sink.Write(bytes.ToArray(), 0, bytes.Length);
#endif
            total += bytes.Length;
        }
        BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(codes.Length * 4), total);

        return new StringArray(
            codes.Length,
            new ArrowBuffer(offsetBytes),
            new ArrowBuffer(sink.ToArray()),
            ArrowBuffer.Empty,
            nullCount: 0,
            offset: 0);
    }
}
