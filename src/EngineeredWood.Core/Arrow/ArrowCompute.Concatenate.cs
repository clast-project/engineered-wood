// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Arrow;

// The concatenation ArrowCompute does itself rather than leave to Arrow's ArrayDataConcatenator (#667).
// Arrow 23 builds every buffer in a managed builder and then copies it into a native one, and for a string or
// binary column it starts the value builder empty, so it doubles its way up to the column's size: joining a
// 23 MB string column allocated 78 MB of builders before the final copy. Here each buffer is allocated once,
// at its exact size, and written in place.
//
// A STOPGAP: apache/arrow-dotnet#463 (merged 2026-10-18, after 23.0.0) fixes this in Arrow itself, for every
// type. Once EngineeredWood depends on an Apache.Arrow release that includes it, delete this file and the
// ConcatenatesItself branch in ArrowCompute.Concatenate; ArrayDataConcatenator will then do as well or better
// (it also stops building fixed-width buffers twice, which this file never touched).
public static partial class ArrowCompute
{
    /// <summary>
    /// Whether <see cref="ConcatenateData"/> joins arrays of this type itself rather than handing them to Arrow.
    /// A struct qualifies so that its string and binary children do too: Arrow's struct concatenation recurses
    /// through its own, which would put them back on the doubling path. VARIANT's storage is such a struct.
    /// </summary>
    private static bool ConcatenatesItself(IArrowType type) => type.TypeId is ArrowTypeId.String
        or ArrowTypeId.Binary or ArrowTypeId.LargeString or ArrowTypeId.LargeBinary or ArrowTypeId.Struct;

    /// <summary>
    /// Joins arrays that carry no extension type, at any depth. String, binary and struct arrays are joined
    /// here and everything else by Arrow. Takes two or more inputs.
    /// </summary>
    private static ArrayData ConcatenateData(List<ArrayData> inputs) => inputs[0].DataType.TypeId switch
    {
        ArrowTypeId.String or ArrowTypeId.Binary => ConcatenateBinary(inputs),
        ArrowTypeId.LargeString or ArrowTypeId.LargeBinary => ConcatenateLargeBinary(inputs),
        ArrowTypeId.Struct => ConcatenateStruct(inputs),
        _ => ArrayDataConcatenator.Concatenate(inputs),
    };

    private static ArrayData ConcatenateBinary(List<ArrayData> inputs)
    {
        EnsureSameTypeId(inputs);

        int length = 0;
        int bytes = 0;
        foreach (var data in inputs)
        {
            var offsets = Offsets32(data);
            length = checked(length + data.Length);
            bytes = checked(bytes + (offsets[data.Length] - offsets[0]));
        }

        // Every slot of both is written below.
        var offsetBytes = Uninitialized<byte>(checked((length + 1) * sizeof(int)));
        var values = Uninitialized<byte>(bytes);
        var outOffsets = MemoryMarshal.Cast<byte, int>(offsetBytes.AsSpan());

        int row = 0;
        int position = 0;
        foreach (var data in inputs)
        {
            var offsets = Offsets32(data);
            int first = offsets[0];
            int shift = position - first;
            var target = outOffsets.Slice(row, data.Length);
            if (shift == 0)
            {
                offsets.Slice(0, data.Length).CopyTo(target);
            }
            else
            {
                for (int i = 0; i < target.Length; i++)
                    target[i] = offsets[i] + shift;
            }

            int count = offsets[data.Length] - first;
            data.Buffers[2].Span.Slice(first, count).CopyTo(values.AsSpan(position));
            position += count;
            row += data.Length;
        }
        outOffsets[length] = position;

        var (validity, nullCount) = ConcatenateValidity(inputs, length);
        return new ArrayData(
            inputs[0].DataType, length, nullCount, offset: 0,
            new[] { validity, new ArrowBuffer(offsetBytes), new ArrowBuffer(values) });
    }

    private static ArrayData ConcatenateLargeBinary(List<ArrayData> inputs)
    {
        EnsureSameTypeId(inputs);

        int length = 0;
        long bytes = 0;
        foreach (var data in inputs)
        {
            var offsets = Offsets64(data);
            length = checked(length + data.Length);
            bytes = checked(bytes + (offsets[data.Length] - offsets[0]));
        }

        // Every slot of both is written below. A managed array holds at most int.MaxValue bytes, as do the
        // spans every input's values are read through.
        var offsetBytes = Uninitialized<byte>(checked((length + 1) * sizeof(long)));
        var values = Uninitialized<byte>(checked((int)bytes));
        var outOffsets = MemoryMarshal.Cast<byte, long>(offsetBytes.AsSpan());

        int row = 0;
        int position = 0;
        foreach (var data in inputs)
        {
            var offsets = Offsets64(data);
            long first = offsets[0];
            long shift = position - first;
            var target = outOffsets.Slice(row, data.Length);
            if (shift == 0)
            {
                offsets.Slice(0, data.Length).CopyTo(target);
            }
            else
            {
                for (int i = 0; i < target.Length; i++)
                    target[i] = offsets[i] + shift;
            }

            int count = checked((int)(offsets[data.Length] - first));
            data.Buffers[2].Span.Slice(checked((int)first), count).CopyTo(values.AsSpan(position));
            position += count;
            row += data.Length;
        }
        outOffsets[length] = position;

        var (validity, nullCount) = ConcatenateValidity(inputs, length);
        return new ArrayData(
            inputs[0].DataType, length, nullCount, offset: 0,
            new[] { validity, new ArrowBuffer(offsetBytes), new ArrowBuffer(values) });
    }

    /// <summary>
    /// Joins structs child by child, each child cut to the rows its parent exposes, as Arrow does. The children
    /// go back through <see cref="ConcatenateData"/>, so a string child is joined here too.
    /// </summary>
    private static ArrayData ConcatenateStruct(List<ArrayData> inputs)
    {
        EnsureSameTypeId(inputs);
        int fieldCount = ((StructType)inputs[0].DataType).Fields.Count;

        int length = 0;
        foreach (var data in inputs)
        {
            if (data.Children is null || data.Children.Length != fieldCount)
            {
                throw new ArgumentException(
                    $"Cannot concatenate a struct of {data.Children?.Length ?? 0} field(s) onto one of {fieldCount}.");
            }
            length = checked(length + data.Length);
        }

        var children = new ArrayData[fieldCount];
        for (int c = 0; c < fieldCount; c++)
        {
            var parts = new List<ArrayData>(inputs.Count);
            foreach (var data in inputs)
                parts.Add(data.Children![c].Slice(data.Offset, data.Length));
            children[c] = ConcatenateData(parts);
        }

        var (validity, nullCount) = ConcatenateValidity(inputs, length);
        return new ArrayData(inputs[0].DataType, length, nullCount, offset: 0, new[] { validity }, children);
    }

    /// <summary>
    /// The inputs' validity bitmaps joined, each read from its own offset; none at all when no input has a null.
    /// An input without a bitmap is all valid, unless its null count says every row is null (as Arrow reads it).
    /// </summary>
    private static (ArrowBuffer Validity, int NullCount) ConcatenateValidity(List<ArrayData> inputs, int length)
    {
        int nullCount = 0;
        foreach (var data in inputs)
            nullCount += data.GetNullCount();
        if (nullCount == 0)
            return (ArrowBuffer.Empty, 0);

        var bits = Uninitialized<byte>(BitmapBytes(length));
        // The bits past the last row are never written below; they are zero, like those of a builder's bitmap.
        if (bits.Length > 0)
            bits[bits.Length - 1] = 0;

        int row = 0;
        foreach (var data in inputs)
        {
            var source = data.Buffers[0].Span;
            if (source.Length == 0)
                FillBits(bits, row, data.Length, data.GetNullCount() == 0);
            else
                CopyBits(source, data.Offset, bits, row, data.Length);
            row += data.Length;
        }

        return (new ArrowBuffer(bits), nullCount);
    }

    /// <summary>Copies <paramref name="count"/> bits, a whole byte at a time where both ends are on one.</summary>
    private static void CopyBits(ReadOnlySpan<byte> source, int sourceBit, Span<byte> target, int targetBit, int count)
    {
        int i = 0;
        if (((sourceBit | targetBit) & 7) == 0)
        {
            int wholeBytes = count >> 3;
            source.Slice(sourceBit >> 3, wholeBytes).CopyTo(target.Slice(targetBit >> 3));
            i = wholeBytes << 3;
        }

        for (; i < count; i++)
        {
            int s = sourceBit + i;
            SetBitTo(target, targetBit + i, (source[s >> 3] & (1 << (s & 7))) != 0);
        }
    }

    private static void FillBits(Span<byte> target, int targetBit, int count, bool value)
    {
        int i = 0;
        while (i < count && ((targetBit + i) & 7) != 0)
        {
            SetBitTo(target, targetBit + i, value);
            i++;
        }

        int wholeBytes = (count - i) >> 3;
        target.Slice((targetBit + i) >> 3, wholeBytes).Fill(value ? (byte)0xFF : (byte)0);
        i += wholeBytes << 3;

        for (; i < count; i++)
            SetBitTo(target, targetBit + i, value);
    }

    private static void SetBitTo(Span<byte> bitmap, int index, bool value)
    {
        if (value)
            bitmap[index >> 3] |= (byte)(1 << (index & 7));
        else
            bitmap[index >> 3] &= (byte)~(1 << (index & 7));
    }

    private static ReadOnlySpan<int> Offsets32(ArrayData data) =>
        MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span).Slice(data.Offset, data.Length + 1);

    private static ReadOnlySpan<long> Offsets64(ArrayData data) =>
        MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span).Slice(data.Offset, data.Length + 1);

    /// <summary>As Arrow's concatenator checks: every input of the first one's kind. Deeper agreement is
    /// checked as the children are joined.</summary>
    private static void EnsureSameTypeId(List<ArrayData> inputs)
    {
        var expected = inputs[0].DataType;
        for (int i = 1; i < inputs.Count; i++)
        {
            if (inputs[i].DataType.TypeId != expected.TypeId)
            {
                throw new ArgumentException(
                    $"Cannot concatenate {inputs[i].DataType.Name} onto {expected.Name}.");
            }
        }
    }
}
