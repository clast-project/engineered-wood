// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// Materializes a <c>vortex.dict</c>-encoded column from its values dictionary
/// and per-row codes. <c>output[i] = values[codes[i]]</c> for valid rows;
/// <c>output[i] = null</c> when the code at row i is null (the codes child
/// carries the validity bitmap).
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
        var (codeIndices, codesValidity, codesNullCount) = ResolveCodes(codes);

        return (arrowType, values) switch
        {
            (StringType, _) => ReconstructString(values, codeIndices, codesValidity, codesNullCount),
            (Int8Type, Int8Array a8) => Build<sbyte>(codeIndices, i => a8.GetValue(i)!.Value,
                (data, len) => new Int8Array(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
            (Int16Type, Int16Array a16) => Build<short>(codeIndices, i => a16.GetValue(i)!.Value,
                (data, len) => new Int16Array(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
            (Int32Type, Int32Array a32) => Build<int>(codeIndices, i => a32.GetValue(i)!.Value,
                (data, len) => new Int32Array(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
            (Int64Type, Int64Array a64) => Build<long>(codeIndices, i => a64.GetValue(i)!.Value,
                (data, len) => new Int64Array(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
            (UInt8Type, UInt8Array u8) => Build<byte>(codeIndices, i => u8.GetValue(i)!.Value,
                (data, len) => new UInt8Array(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
            (UInt16Type, UInt16Array u16) => Build<ushort>(codeIndices, i => u16.GetValue(i)!.Value,
                (data, len) => new UInt16Array(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
            (UInt32Type, UInt32Array u32) => Build<uint>(codeIndices, i => u32.GetValue(i)!.Value,
                (data, len) => new UInt32Array(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
            (UInt64Type, UInt64Array u64) => Build<ulong>(codeIndices, i => u64.GetValue(i)!.Value,
                (data, len) => new UInt64Array(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
            (FloatType, FloatArray f) => Build<float>(codeIndices, i => f.GetValue(i)!.Value,
                (data, len) => new FloatArray(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
            (DoubleType, DoubleArray d) => Build<double>(codeIndices, i => d.GetValue(i)!.Value,
                (data, len) => new DoubleArray(new ArrowBuffer(data), codesValidity, len, codesNullCount, 0)),
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
        {
            // Negative sentinel means "null row" — leave default(T) in output.
            // The validity bitmap on the wrapping array masks it at the consumer.
            if (codes[i] < 0) continue;
            span[i] = get(codes[i]);
        }
        return ctor(bytes, codes.Length);
    }

    /// <summary>
    /// Returns the codes' index values + validity buffer + null count. Null
    /// positions in the codes' validity bitmap get a sentinel index of -1 so
    /// downstream gather can skip them (the (uint)idx range check in
    /// ReconstructString would still reject negatives, but the explicit
    /// null-skip in Build/ReconstructString keeps gather logic simple).
    /// </summary>
    private static (int[] Codes, ArrowBuffer ValidityBuf, int NullCount) ResolveCodes(IArrowArray codes)
    {
        var data = ((Apache.Arrow.Array)codes).Data;
        var validityBuf = data.Buffers.Length > 0 ? data.Buffers[0] : ArrowBuffer.Empty;
        int nullCount = data.GetNullCount();
        if (nullCount < 0) nullCount = 0;

        int[] indices = codes switch
        {
            UInt32Array u32 => CopyUInt32(u32, validityBuf.Span, nullCount),
            UInt16Array u16 => CopyUInt16(u16, validityBuf.Span, nullCount),
            UInt8Array u8 => CopyUInt8(u8, validityBuf.Span, nullCount),
            UInt64Array u64 => CopyUInt64(u64, validityBuf.Span, nullCount),
            Int32Array i32 => CopyInt32(i32, validityBuf.Span, nullCount),
            Int64Array i64 => CopyInt64(i64, validityBuf.Span, nullCount),
            _ => throw new NotSupportedException(
                $"vortex.dict codes type {codes.GetType().Name} is not supported."),
        };
        return (indices, validityBuf, nullCount);
    }

    /// <summary>True at bit position i in the validity bitmap (LSB-first).</summary>
    private static bool BitAt(ReadOnlySpan<byte> bitmap, int i, int dataOffset) =>
        (bitmap[(i + dataOffset) >> 3] & (1 << ((i + dataOffset) & 7))) != 0;

    // Read raw code values without going through GetValue (which would NPE
    // on null positions). Null positions get the sentinel -1 so callers can
    // skip the gather. Validity bitmap is empty for non-nullable codes; in
    // that case all positions stay valid.
    private static int[] CopyUInt32(UInt32Array a, ReadOnlySpan<byte> validity, int nullCount)
    {
        var r = new int[a.Length];
        var src = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, uint>(
            a.Data.Buffers[1].Span.Slice(a.Offset * 4, a.Length * 4));
        bool hasNulls = nullCount > 0;
        for (int i = 0; i < a.Length; i++)
            r[i] = (hasNulls && !BitAt(validity, i, a.Offset)) ? -1 : checked((int)src[i]);
        return r;
    }

    private static int[] CopyUInt16(UInt16Array a, ReadOnlySpan<byte> validity, int nullCount)
    {
        var r = new int[a.Length];
        var src = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ushort>(
            a.Data.Buffers[1].Span.Slice(a.Offset * 2, a.Length * 2));
        bool hasNulls = nullCount > 0;
        for (int i = 0; i < a.Length; i++)
            r[i] = (hasNulls && !BitAt(validity, i, a.Offset)) ? -1 : src[i];
        return r;
    }

    private static int[] CopyUInt8(UInt8Array a, ReadOnlySpan<byte> validity, int nullCount)
    {
        var r = new int[a.Length];
        var src = a.Data.Buffers[1].Span.Slice(a.Offset, a.Length);
        bool hasNulls = nullCount > 0;
        for (int i = 0; i < a.Length; i++)
            r[i] = (hasNulls && !BitAt(validity, i, a.Offset)) ? -1 : src[i];
        return r;
    }

    private static int[] CopyUInt64(UInt64Array a, ReadOnlySpan<byte> validity, int nullCount)
    {
        var r = new int[a.Length];
        var src = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(
            a.Data.Buffers[1].Span.Slice(a.Offset * 8, a.Length * 8));
        bool hasNulls = nullCount > 0;
        for (int i = 0; i < a.Length; i++)
            r[i] = (hasNulls && !BitAt(validity, i, a.Offset)) ? -1 : checked((int)src[i]);
        return r;
    }

    private static int[] CopyInt32(Int32Array a, ReadOnlySpan<byte> validity, int nullCount)
    {
        var r = new int[a.Length];
        var src = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(
            a.Data.Buffers[1].Span.Slice(a.Offset * 4, a.Length * 4));
        bool hasNulls = nullCount > 0;
        for (int i = 0; i < a.Length; i++)
            r[i] = (hasNulls && !BitAt(validity, i, a.Offset)) ? -1 : src[i];
        return r;
    }

    private static int[] CopyInt64(Int64Array a, ReadOnlySpan<byte> validity, int nullCount)
    {
        var r = new int[a.Length];
        var src = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(
            a.Data.Buffers[1].Span.Slice(a.Offset * 8, a.Length * 8));
        bool hasNulls = nullCount > 0;
        for (int i = 0; i < a.Length; i++)
            r[i] = (hasNulls && !BitAt(validity, i, a.Offset)) ? -1 : checked((int)src[i]);
        return r;
    }

    private static IArrowArray ReconstructString(
        IArrowArray values, int[] codes, ArrowBuffer codesValidity, int codesNullCount)
    {
        var dict = values as StringArray
            ?? throw new VortexFormatException(
                $"vortex.dict for StringType expected a StringArray dictionary, got {values.GetType().Name}.");

        // Build offsets + concatenated values from the picked dict entries.
        // For null rows (codes[i] == -1), emit a zero-length string at the
        // current cursor — the validity bitmap on the wrapping StringArray
        // marks the row as null so the consumer ignores the empty payload.
        var offsetBytes = new byte[(codes.Length + 1) * 4];
        using var sink = new System.IO.MemoryStream();
        int total = 0;
        for (int i = 0; i < codes.Length; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4), total);
            var idx = codes[i];
            if (idx < 0) continue; // null row — leave offset == previous, no bytes written
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
            codesValidity,
            codesNullCount,
            offset: 0);
    }
}
