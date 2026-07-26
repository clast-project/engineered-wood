// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// Builds Arrow arrays straight from raw buffers rather than through the typed array builders.
///
/// <para>Tests for a gather routine cannot use the builders to construct their inputs: the builders are
/// the thing under suspicion, and they cannot express the cases that matter here at all — a non-zero
/// logical offset, a declared-but-unknown null count, or a value too wide for the .NET surface type.</para>
/// </summary>
internal static class RawArrays
{
    /// <summary>
    /// A fixed-width array over <paramref name="physicalValues"/>, whose length is inferred from the buffer
    /// size. <paramref name="offset"/> is a LOGICAL offset: the array reports
    /// <c>physicalCount - offset</c> rows and row <c>i</c> lives in slot <c>offset + i</c>.
    /// <paramref name="physicalValid"/>, when given, is indexed by PHYSICAL slot, matching Arrow's
    /// validity-bitmap convention.
    /// </summary>
    public static IArrowArray Fixed(
        IArrowType type, int byteWidth, byte[] physicalValues,
        bool[]? physicalValid = null, int offset = 0, int? nullCountOverride = null)
    {
        int physical = physicalValues.Length / byteWidth;
        return ArrowArrayFactory.BuildArray(new ArrayData(
            type, physical - offset,
            nullCountOverride ?? CountNulls(physicalValid, offset, physical),
            offset,
            new[] { Validity(physicalValid, physical), new ArrowBuffer(physicalValues) }));
    }

    /// <summary>Fixed-width array from typed values, laid out little-endian.</summary>
    public static IArrowArray Fixed<T>(
        IArrowType type, T[] physicalValues,
        bool[]? physicalValid = null, int offset = 0, int? nullCountOverride = null)
        where T : struct
    {
        var bytes = new byte[physicalValues.Length * System.Runtime.CompilerServices.Unsafe.SizeOf<T>()];
        System.Runtime.InteropServices.MemoryMarshal
            .AsBytes(physicalValues.AsSpan()).CopyTo(bytes);
        return Fixed(
            type, System.Runtime.CompilerServices.Unsafe.SizeOf<T>(), bytes,
            physicalValid, offset, nullCountOverride);
    }

    /// <summary>
    /// A String/Binary array with 32-bit offsets, or a LargeString/LargeBinary array with 64-bit offsets
    /// when <paramref name="large"/> is set.
    /// </summary>
    public static IArrowArray VarBinary(
        IArrowType type, string[] physicalValues, bool large,
        bool[]? physicalValid = null, int offset = 0)
    {
        int physical = physicalValues.Length;
        var data = new List<byte>();
        var starts = new List<long> { 0 };
        foreach (string s in physicalValues)
        {
            data.AddRange(System.Text.Encoding.UTF8.GetBytes(s));
            starts.Add(data.Count);
        }

        ArrowBuffer offsets;
        if (large)
        {
            var b = new ArrowBuffer.Builder<long>(starts.Count);
            foreach (long o in starts) b.Append(o);
            offsets = b.Build();
        }
        else
        {
            var b = new ArrowBuffer.Builder<int>(starts.Count);
            foreach (long o in starts) b.Append(checked((int)o));
            offsets = b.Build();
        }

        return ArrowArrayFactory.BuildArray(new ArrayData(
            type, physical - offset, CountNulls(physicalValid, offset, physical), offset,
            new[] { Validity(physicalValid, physical), offsets, new ArrowBuffer(data.ToArray()) }));
    }

    /// <summary>A boolean array; values and validity are both indexed by physical slot.</summary>
    public static IArrowArray Boolean(
        bool[] physicalValues, bool[]? physicalValid = null, int offset = 0)
    {
        int physical = physicalValues.Length;
        var bits = new ArrowBuffer.BitmapBuilder(physical);
        foreach (bool v in physicalValues) bits.Append(v);

        return new BooleanArray(new ArrayData(
            BooleanType.Default, physical - offset,
            CountNulls(physicalValid, offset, physical), offset,
            new[] { Validity(physicalValid, physical), bits.Build() }));
    }

    /// <summary>
    /// A struct array over pre-built children. Children are indexed by PHYSICAL slot — they are not
    /// pre-shifted by <paramref name="offset"/>, matching Arrow's layout.
    /// </summary>
    public static StructArray Struct(
        StructType type, int physicalCount, IArrowArray[] children,
        bool[]? physicalValid = null, int offset = 0)
    {
        var childData = new ArrayData[children.Length];
        for (int i = 0; i < children.Length; i++)
            childData[i] = children[i].Data;

        return (StructArray)ArrowArrayFactory.BuildArray(new ArrayData(
            type, physicalCount - offset,
            CountNulls(physicalValid, offset, physicalCount), offset,
            new[] { Validity(physicalValid, physicalCount) }, childData));
    }

    /// <summary>Raw value-slot bytes for one LOGICAL row of a fixed-width array.</summary>
    public static byte[] Slot(IArrowArray array, int logicalRow, int byteWidth) =>
        array.Data.Buffers[1].Span
            .Slice((array.Data.Offset + logicalRow) * byteWidth, byteWidth)
            .ToArray();

    private static ArrowBuffer Validity(bool[]? physicalValid, int physical)
    {
        if (physicalValid is null)
            return ArrowBuffer.Empty;

        var bm = new ArrowBuffer.BitmapBuilder(physical);
        foreach (bool v in physicalValid) bm.Append(v);
        return bm.Build();
    }

    private static int CountNulls(bool[]? physicalValid, int offset, int physical)
    {
        if (physicalValid is null)
            return 0;

        int n = 0;
        for (int i = offset; i < physical; i++)
            if (!physicalValid[i]) n++;
        return n;
    }
}
