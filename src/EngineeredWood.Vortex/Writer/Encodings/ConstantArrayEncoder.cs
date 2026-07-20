// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.ConstantArrayDecoder"/>:
/// emits a <c>vortex.constant</c> ArrayNode subtree for columns where every
/// value is the same scalar.
///
/// <para>Wire shape: 1 buffer (the value as a <c>vortex.scalar.ScalarValue</c>
/// protobuf), 0 children, no metadata. Row count is taken from the layout's
/// <c>row_count</c>, not stored in the array.</para>
///
/// <para>Scope:
/// <list type="bullet">
///   <item>Non-nullable primitives (Int8..Int64, UInt8..UInt64, Float32/64) and Bool.</item>
///   <item>Sliced inputs honored — reads through <c>data.Offset</c>.</item>
///   <item>All values byte-identical (covers integer/float/bool; for floats this
///     means same bits, so positive vs negative zero or differing NaN payloads
///     would NOT match — vortex's reader will broadcast a single bit pattern).</item>
/// </list>
/// String/binary/list/FSL/decimal columns are deferred — vortex's constant
/// encoder supports them via <c>BinaryScalar</c>/<c>ListScalar</c> etc., but
/// our writer's MVP only emits the primitive ScalarValue variants.</para>
/// </summary>
internal static class ConstantArrayEncoder
{
    /// <summary>
    /// True iff the column is non-nullable and all values are byte-identical
    /// at the value buffer level (slicing offsets are honored).
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is null) return false;
        if (array.Length == 0) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        // GetNullCount() returns the count over the sliced range, so this is
        // accurate even for offset != 0.
        if (data.GetNullCount() > 0) return false;

        return array switch
        {
            BooleanArray b => AllBoolEqual(b),
            Int8Array or UInt8Array or Int16Array or UInt16Array
                or Int32Array or UInt32Array or FloatArray
                or Int64Array or UInt64Array or DoubleArray => AllPrimitiveEqual(data, ElementSize(array)),
            _ => false,
        };
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, ushort constantEncodingIdx, int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        if (!IsApplicable(array))
            throw new InvalidOperationException(
                $"vortex.constant writer requires a non-nullable column with all values equal; {array.GetType().Name} doesn't qualify.");

        byte[] scalarBytes = SerializeFirstValue(array);
        ushort scalarBufIdx = sb.AddBuffer(scalarBytes, alignmentExponent: 0);

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, constantEncodingIdx, scalarBufIdx)
            : ArrayNodeEmitter.EmitWithSingleBufferAndStats(
                sb.Builder, constantEncodingIdx, scalarBufIdx, statsTicket.Value);
    }

    /// <summary>Convenience: encode one column's segment in isolation.</summary>
    public static byte[] Encode(IArrowArray array, ushort constantEncodingIdx)
    {
        var sb = new SegmentBuilder();
        var rootTicket = Emit(sb, array, constantEncodingIdx);
        return sb.FinishSegment(rootTicket);
    }

    private static int ElementSize(IArrowArray array) => array switch
    {
        Int8Array or UInt8Array => 1,
        Int16Array or UInt16Array => 2,
        Int32Array or UInt32Array or FloatArray => 4,
        Int64Array or UInt64Array or DoubleArray => 8,
        _ => 0,
    };

    private static bool AllPrimitiveEqual(ArrayData data, int elemSize)
    {
        int n = data.Length;
        var span = data.Buffers[1].Span.Slice(data.Offset * elemSize, n * elemSize);
        var first = span.Slice(0, elemSize);
        for (int pos = elemSize; pos < span.Length; pos += elemSize)
            if (!span.Slice(pos, elemSize).SequenceEqual(first)) return false;
        return true;
    }

    private static bool AllBoolEqual(BooleanArray array)
    {
        int n = array.Length;
        if (n <= 1) return true;
        bool first = array.GetValue(0)!.Value;
        for (int i = 1; i < n; i++)
            if (array.GetValue(i)!.Value != first) return false;
        return true;
    }

    private static byte[] SerializeFirstValue(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int off = data.Offset;
        return array switch
        {
            // BooleanArray.GetValue indexes into the SLICED logical view, so
            // index 0 is the first row of the slice.
            BooleanArray b => ScalarValueSerializer.FromBool(b.GetValue(0)!.Value),
            Int8Array => ScalarValueSerializer.FromSignedInt((sbyte)data.Buffers[1].Span[off]),
            Int16Array => ScalarValueSerializer.FromSignedInt(MemoryMarshal.Read<short>(data.Buffers[1].Span.Slice(off * 2))),
            Int32Array => ScalarValueSerializer.FromSignedInt(MemoryMarshal.Read<int>(data.Buffers[1].Span.Slice(off * 4))),
            Int64Array => ScalarValueSerializer.FromSignedInt(MemoryMarshal.Read<long>(data.Buffers[1].Span.Slice(off * 8))),
            UInt8Array => ScalarValueSerializer.FromUnsignedInt(data.Buffers[1].Span[off]),
            UInt16Array => ScalarValueSerializer.FromUnsignedInt(MemoryMarshal.Read<ushort>(data.Buffers[1].Span.Slice(off * 2))),
            UInt32Array => ScalarValueSerializer.FromUnsignedInt(MemoryMarshal.Read<uint>(data.Buffers[1].Span.Slice(off * 4))),
            UInt64Array => ScalarValueSerializer.FromUnsignedInt(MemoryMarshal.Read<ulong>(data.Buffers[1].Span.Slice(off * 8))),
            FloatArray => ScalarValueSerializer.FromFloat32(MemoryMarshal.Read<float>(data.Buffers[1].Span.Slice(off * 4))),
            DoubleArray => ScalarValueSerializer.FromFloat64(MemoryMarshal.Read<double>(data.Buffers[1].Span.Slice(off * 8))),
            _ => throw new NotSupportedException(
                $"vortex.constant doesn't support Arrow {array.GetType().Name}."),
        };
    }
}
