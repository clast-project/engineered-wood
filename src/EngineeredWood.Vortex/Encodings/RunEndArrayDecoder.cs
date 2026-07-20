// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.runend</c>: run-length-encoded primitive arrays.
/// Two children: <c>ends</c> (monotonic run-end positions) and <c>values</c>
/// (one value per run). For row <c>i</c>, find the smallest <c>j</c> where
/// <c>ends[j] &gt; i</c>, output <c>values[j]</c>.
///
/// <para>Metadata proto <c>RunEndMetadata { ends_ptype, num_runs, offset }</c>.
/// We use <c>ends_ptype</c> to resolve the Arrow type for the ends child;
/// <c>offset</c> is for slicing (currently always 0 for top-level use).</para>
///
/// <para>Phase 1 scope: integer values only. Float / bool / string run-end
/// arrays land alongside fixtures.</para>
/// </summary>
internal static class RunEndArrayDecoder
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
                $"vortex.runend expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount != 2)
            throw new VortexFormatException(
                $"vortex.runend expects 2 children (ends, values), got {node.ChildCount}.");

        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException("vortex.runend ArrayNode has empty metadata.");
        var (endsPtype, numRuns, offset) = ParseRunEndMetadata(metaVec.RawBytes(metaVec.Length));
        if (offset != 0)
            throw new NotSupportedException(
                $"vortex.runend with non-zero offset ({offset}) not yet supported.");

        var endsArrowType = PtypeIntToArrowType(endsPtype);
        var endsArr = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, endsArrowType, checked((long)numRuns));
        var valuesArr = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs, expectedType, checked((long)numRuns));

        var rowCount = checked((int)expectedRowCount);
        return Expand(expectedType, rowCount, endsArr, valuesArr);
    }

    /// <summary>
    /// Materializes a flat primitive array of <paramref name="rowCount"/> rows
    /// from run-end encoded <paramref name="ends"/> + <paramref name="values"/>.
    /// When <paramref name="values"/> has a validity bitmap (some runs are null),
    /// the output's validity is derived by expanding values_validity over runs:
    /// the output's bit at row <c>i</c> equals values_validity[run_for_row_i].
    /// </summary>
    private static IArrowArray Expand(
        IArrowType expectedType, int rowCount, IArrowArray ends, IArrowArray values)
    {
        return (expectedType, values) switch
        {
            (Int8Type, Int8Array v) => ExpandPrimitive<sbyte>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new Int8Array(new ArrowBuffer(data), val, len, nc, 0)),
            (Int16Type, Int16Array v) => ExpandPrimitive<short>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new Int16Array(new ArrowBuffer(data), val, len, nc, 0)),
            (Int32Type, Int32Array v) => ExpandPrimitive<int>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new Int32Array(new ArrowBuffer(data), val, len, nc, 0)),
            (Int64Type, Int64Array v) => ExpandPrimitive<long>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new Int64Array(new ArrowBuffer(data), val, len, nc, 0)),
            (UInt8Type, UInt8Array v) => ExpandPrimitive<byte>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new UInt8Array(new ArrowBuffer(data), val, len, nc, 0)),
            (UInt16Type, UInt16Array v) => ExpandPrimitive<ushort>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new UInt16Array(new ArrowBuffer(data), val, len, nc, 0)),
            (UInt32Type, UInt32Array v) => ExpandPrimitive<uint>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new UInt32Array(new ArrowBuffer(data), val, len, nc, 0)),
            (UInt64Type, UInt64Array v) => ExpandPrimitive<ulong>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new UInt64Array(new ArrowBuffer(data), val, len, nc, 0)),
            (FloatType, FloatArray v) => ExpandPrimitive<float>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new FloatArray(new ArrowBuffer(data), val, len, nc, 0)),
            (DoubleType, DoubleArray v) => ExpandPrimitive<double>(rowCount, ends, v,
                i => v.GetValue(i) ?? default,
                static (data, val, len, nc) => new DoubleArray(new ArrowBuffer(data), val, len, nc, 0)),
            _ => throw new NotSupportedException(
                $"vortex.runend: expansion for ({expectedType}, {values.GetType().Name}) not yet implemented."),
        };
    }

    private static IArrowArray ExpandPrimitive<T>(
        int rowCount,
        IArrowArray ends,
        IArrowArray values,
        Func<int, T> getValue,
        Func<byte[], ArrowBuffer, int, int, IArrowArray> ctor)
        where T : struct
    {
        var bytes = new byte[(long)rowCount * Marshal.SizeOf<T>()];
        var span = MemoryMarshal.Cast<byte, T>(bytes.AsSpan());

        var valuesData = ((Apache.Arrow.Array)values).Data;
        bool valuesHaveNulls = valuesData.GetNullCount() > 0;
        var valuesValidity = valuesHaveNulls ? valuesData.Buffers[0].Span : default;
        int valuesOffset = valuesData.Offset;

        // Output validity bitmap (LSB-first per byte). Allocated only when the
        // values child has nulls — non-nullable values produce non-nullable rows.
        byte[]? outValidity = valuesHaveNulls ? new byte[(rowCount + 7) / 8] : null;
        int outNullCount = 0;

        int run = 0;
        int runEnd = GetIntAtIndex(ends, 0);
        bool runIsValid = !valuesHaveNulls
            || (valuesValidity[(valuesOffset + run) >> 3] & (1 << ((valuesOffset + run) & 7))) != 0;
        for (int i = 0; i < rowCount; i++)
        {
            while (i >= runEnd)
            {
                run++;
                if (run >= ends.Length)
                    throw new VortexFormatException(
                        $"vortex.runend: row {i} exceeds last run end ({runEnd}).");
                runEnd = GetIntAtIndex(ends, run);
                runIsValid = !valuesHaveNulls
                    || (valuesValidity[(valuesOffset + run) >> 3] & (1 << ((valuesOffset + run) & 7))) != 0;
            }
            // For non-null runs, the typed GetValue returns the value; for
            // null runs we leave the slot as default(T) since the validity
            // bit will mask it. The caller's lambda coalesces null back to
            // default to avoid an NPE on the typed accessor.
            span[i] = getValue(run);
            if (outValidity is not null)
            {
                if (runIsValid) outValidity[i >> 3] |= (byte)(1 << (i & 7));
                else outNullCount++;
            }
        }

        var validityBuf = outValidity is null ? ArrowBuffer.Empty : new ArrowBuffer(outValidity);
        return ctor(bytes, validityBuf, rowCount, outNullCount);
    }

    private static (int EndsPtype, ulong NumRuns, ulong Offset) ParseRunEndMetadata(ReadOnlySpan<byte> bytes)
    {
        int? endsPtype = null;
        ulong numRuns = 0, offset = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                endsPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                numRuns = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 0)
                offset = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else SkipField(bytes, ref pos, wireType);
        }
        // Proto3 default for enum fields is 0 (= PType.U8). Missing fields are
        // not serialized, so we treat absence as U8.
        return (endsPtype ?? 0, numRuns, offset);
    }

    private static void SkipField(ReadOnlySpan<byte> bytes, ref int pos, uint wireType)
    {
        switch (wireType)
        {
            case 0: Varint.ReadUnsigned(bytes, ref pos); break;
            case 1: pos += 8; break;
            case 2:
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                pos += len;
                break;
            case 5: pos += 4; break;
            default:
                throw new VortexFormatException(
                    $"Unsupported protobuf wire type {wireType} in RunEndMetadata.");
        }
    }

    private static IArrowType PtypeIntToArrowType(int ptype) => ptype switch
    {
        0 => UInt8Type.Default,
        1 => UInt16Type.Default,
        2 => UInt32Type.Default,
        3 => UInt64Type.Default,
        4 => Int8Type.Default,
        5 => Int16Type.Default,
        6 => Int32Type.Default,
        7 => Int64Type.Default,
        _ => throw new VortexFormatException(
            $"vortex.runend: ptype {ptype} is not a supported integer type."),
    };

    private static int GetIntAtIndex(IArrowArray array, int i) => array switch
    {
        UInt8Array u8 => u8.GetValue(i)!.Value,
        UInt16Array u16 => u16.GetValue(i)!.Value,
        UInt32Array u32 => checked((int)u32.GetValue(i)!.Value),
        UInt64Array u64 => checked((int)u64.GetValue(i)!.Value),
        Int8Array i8 => i8.GetValue(i)!.Value,
        Int16Array i16 => i16.GetValue(i)!.Value,
        Int32Array i32 => i32.GetValue(i)!.Value,
        Int64Array i64 => checked((int)i64.GetValue(i)!.Value),
        _ => throw new VortexFormatException(
            $"vortex.runend ends array type {array.GetType().Name} not supported."),
    };
}
