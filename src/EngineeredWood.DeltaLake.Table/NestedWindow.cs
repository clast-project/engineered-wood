// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The rows a nested array covers, for the reshaping that rebuilds what is inside one (#853): a field added to a struct
/// (<see cref="SchemaEvolution"/>) or widened inside one (<see cref="TypeWidening.ValueWidener"/>), at any depth.
/// <para>A struct's children are not sliced with it, and a list's values are indexed by its offsets, so a batch sliced
/// from a larger decode shares children that span the whole decode. A batched read slices each nested column from a
/// decode of the whole row group, so rebuilding those children built (or widened) the whole row group's, once for
/// every batch. Here the reshaping works on the batch's own rows instead: its children windowed (zero-copy) to the rows
/// it covers, and a container that changes rebuilt at offset 0 over them, its validity bits and offsets copied to
/// start there. A container that changes nothing is returned as it came, with nothing copied.</para>
/// </summary>
internal static class NestedWindow
{
    /// <summary>A struct's (or map entry's) children over its own rows, each a zero-copy slice of the child.</summary>
    public static ArrayData[] StructChildren(ArrayData data)
    {
        var children = new ArrayData[data.Children.Length];
        for (int i = 0; i < children.Length; i++)
            children[i] = Window(data.Children[i], data.Offset, data.Length);
        return children;
    }

    /// <summary>The child entries a list or map covers, [<c>Start</c>, <c>End</c>), from its offsets.</summary>
    public static (int Start, int End) ElementRange(ArrayData data)
    {
        if (data.DataType is LargeListType)
        {
            var offsets = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span);
            return (checked((int)offsets[data.Offset]), checked((int)offsets[data.Offset + data.Length]));
        }
        var offsets32 = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        return (offsets32[data.Offset], offsets32[data.Offset + data.Length]);
    }

    /// <summary>A list's or map's child over the entries <see cref="ElementRange"/> found.</summary>
    public static ArrayData Elements(ArrayData data, (int Start, int End) range) =>
        Window(data.Children[0], range.Start, range.End - range.Start);

    /// <summary>A struct over its own rows, at offset 0, with <paramref name="children"/> (built over those rows).</summary>
    public static ArrayData Struct(ArrayData original, IArrowType type, ArrayData[] children)
    {
        var (validity, nulls) = Validity(original);
        return new ArrayData(type, original.Length, nulls, 0, [validity], children);
    }

    /// <summary>A list or map over its own rows, at offset 0, with its offsets rebased to the entries
    /// <paramref name="range"/> holds and <paramref name="elements"/> (built over those entries) as its child.</summary>
    public static ArrayData List(ArrayData original, IArrowType type, (int Start, int End) range, ArrayData elements)
    {
        var (validity, nulls) = Validity(original);
        return new ArrayData(type, original.Length, nulls, 0, [validity, Offsets(original, range.Start)], [elements]);
    }

    private static ArrayData Window(ArrayData child, int offset, int length) =>
        offset == 0 && child.Length == length ? child : child.Slice(offset, length);

    // The validity bits of the rows `data` covers, starting at bit 0: its own buffer when it starts there, none when it
    // has no nulls, else a copy.
    private static (ArrowBuffer Validity, int Nulls) Validity(ArrayData data)
    {
        int nulls = data.GetNullCount();
        if (nulls == 0)
            return (ArrowBuffer.Empty, 0);
        var source = data.Buffers[0].Span;
        if (data.Offset == 0 && source.Length > 0)
            return (data.Buffers[0], nulls);

        var bits = new byte[(data.Length + 7) / 8];
        if (source.Length > 0)
        {
            for (int i = 0; i < data.Length; i++)
            {
                int s = data.Offset + i;
                if ((source[s >> 3] & (1 << (s & 7))) != 0)
                    bits[i >> 3] |= (byte)(1 << (i & 7));
            }
        }
        // A buffer-less array with nulls is all NULL: the zeroed bitmap says so.
        return (new ArrowBuffer(bits), nulls);
    }

    // The offsets of the rows `data` covers, starting at 0: its own buffer when they already do.
    private static ArrowBuffer Offsets(ArrayData data, int start)
    {
        if (data.DataType is LargeListType)
        {
            var source = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span).Slice(data.Offset, data.Length + 1);
            if (data.Offset == 0 && start == 0)
                return data.Buffers[1];
            var bytes = new byte[(data.Length + 1) * sizeof(long)];
            var rebased = MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
            for (int i = 0; i < rebased.Length; i++)
                rebased[i] = source[i] - start;
            return new ArrowBuffer(bytes);
        }
        else
        {
            var source = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span).Slice(data.Offset, data.Length + 1);
            if (data.Offset == 0 && start == 0)
                return data.Buffers[1];
            var bytes = new byte[(data.Length + 1) * sizeof(int)];
            var rebased = MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
            for (int i = 0; i < rebased.Length; i++)
                rebased[i] = source[i] - start;
            return new ArrowBuffer(bytes);
        }
    }
}
