// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Restates a sliced Arrow array in the layout the write path reads: every node at offset 0 and every child
/// exactly as long as its parent's rows need. Unlike a gather, it copies no values (#667).
///
/// <para>The encoders index a column's buffers from slot 0 and expect a nested column's children to hold just
/// its rows. A slice satisfies neither: it carries an offset, and its children are left whole. Gathering the
/// rows with <c>Take</c> gives that layout by copying every value. Here the value buffers are sliced in place,
/// as views onto the same memory, and only what a view cannot express is rebuilt:</para>
/// <list type="bullet">
/// <item>offsets, rebased to start at 0 (4 or 8 bytes a row);</item>
/// <item>a bitmap (validity, or a boolean column's values) that does not start on a byte boundary, shifted
/// into one (1 bit a row).</item>
/// </list>
///
/// <para>The result shares the source's memory without owning it, so it is valid only while the source is,
/// and the CALLER must keep the source reachable until it is done with the view: a view holds the memory but
/// not Arrow's reference count on it. In Apache.Arrow 23 that count is released by a finalizer, so a source
/// the GC collects frees its native buffers under a live view (apache/arrow-dotnet#397). Seen on .NET
/// Framework as a NullReferenceException out of a read of the freed buffer. The write path roots the caller's
/// batch for the whole write; see <c>ParquetFileWriter.WriteRowGroupAsync</c>. It never disposes a view, and
/// disposing one would free nothing.</para>
/// </summary>
internal static class ZeroOffsetView
{
    /// <summary>
    /// <paramref name="length"/> rows of <paramref name="array"/> from <paramref name="offset"/>, in the
    /// write path's layout, or null for a type this does not handle (the caller gathers those instead).
    /// </summary>
    public static IArrowArray? TrySlice(IArrowArray array, int offset, int length)
    {
        if (array is ExtensionArray extension)
        {
            var storage = TrySlice(extension.Storage, offset, length);
            return storage is null ? null : extension.ExtensionType.CreateArray(storage);
        }

        var data = Rebase(array.Data.Slice(offset, length));
        return data is null ? null : Apache.Arrow.ArrowArrayFactory.BuildArray(data);
    }

    /// <summary>The same node at offset 0, its children cut to its rows; null if any node cannot be.</summary>
    private static ArrayData? Rebase(ArrayData data)
    {
        // An extension inside a nested column keeps its type on the node; the layout is its storage's.
        var layout = data.DataType;
        while (layout is ExtensionType extension)
            layout = extension.StorageType;

        if (data.Dictionary is not null)
            return null;

        switch (layout.TypeId)
        {
            case ArrowTypeId.Boolean:
            {
                if (!TryValidity(data, out var validity, out int nullCount))
                    return null;
                var values = Bits(data.Buffers[1], data.Offset, data.Length);
                return new ArrayData(data.DataType, data.Length, nullCount, 0, new[] { validity, values });
            }

            case ArrowTypeId.String or ArrowTypeId.Binary:
            {
                if (!TryValidity(data, out var validity, out int nullCount))
                    return null;
                var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span).Slice(data.Offset, data.Length + 1);
                int first = offsets[0];
                var rebased = new int[data.Length + 1];
                for (int i = 0; i < rebased.Length; i++)
                    rebased[i] = offsets[i] - first;
                var values = Window(data.Buffers[2], first, offsets[data.Length] - first);
                return new ArrayData(data.DataType, data.Length, nullCount, 0,
                    new[] { validity, new ArrowBuffer(MemoryMarshal.AsBytes(rebased.AsSpan()).ToArray()), values });
            }

            case ArrowTypeId.LargeString or ArrowTypeId.LargeBinary:
            {
                if (!TryValidity(data, out var validity, out int nullCount))
                    return null;
                var offsets = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span).Slice(data.Offset, data.Length + 1);
                long first = offsets[0];
                var rebased = new long[data.Length + 1];
                for (int i = 0; i < rebased.Length; i++)
                    rebased[i] = offsets[i] - first;
                var values = Window(data.Buffers[2], checked((int)first), checked((int)(offsets[data.Length] - first)));
                return new ArrayData(data.DataType, data.Length, nullCount, 0,
                    new[] { validity, new ArrowBuffer(MemoryMarshal.AsBytes(rebased.AsSpan()).ToArray()), values });
            }

            case ArrowTypeId.Struct:
            {
                if (!TryValidity(data, out var validity, out int nullCount))
                    return null;
                var children = new ArrayData[data.Children.Length];
                for (int c = 0; c < children.Length; c++)
                {
                    var child = Rebase(data.Children[c].Slice(data.Offset, data.Length));
                    if (child is null)
                        return null;
                    children[c] = child;
                }
                return new ArrayData(data.DataType, data.Length, nullCount, 0, new[] { validity }, children);
            }

            case ArrowTypeId.List or ArrowTypeId.Map:
            {
                if (!TryValidity(data, out var validity, out int nullCount))
                    return null;
                var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span).Slice(data.Offset, data.Length + 1);
                int first = offsets[0];
                var rebased = new int[data.Length + 1];
                for (int i = 0; i < rebased.Length; i++)
                    rebased[i] = offsets[i] - first;
                var child = Rebase(data.Children[0].Slice(first, offsets[data.Length] - first));
                if (child is null)
                    return null;
                return new ArrayData(data.DataType, data.Length, nullCount, 0,
                    new[] { validity, new ArrowBuffer(MemoryMarshal.AsBytes(rebased.AsSpan()).ToArray()) },
                    new[] { child });
            }

            case ArrowTypeId.LargeList:
            {
                if (!TryValidity(data, out var validity, out int nullCount))
                    return null;
                var offsets = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span).Slice(data.Offset, data.Length + 1);
                long first = offsets[0];
                var rebased = new long[data.Length + 1];
                for (int i = 0; i < rebased.Length; i++)
                    rebased[i] = offsets[i] - first;
                var child = Rebase(data.Children[0].Slice(
                    checked((int)first), checked((int)(offsets[data.Length] - first))));
                if (child is null)
                    return null;
                return new ArrayData(data.DataType, data.Length, nullCount, 0,
                    new[] { validity, new ArrowBuffer(MemoryMarshal.AsBytes(rebased.AsSpan()).ToArray()) },
                    new[] { child });
            }

            case ArrowTypeId.FixedSizeList:
            {
                if (!TryValidity(data, out var validity, out int nullCount))
                    return null;
                int size = ((FixedSizeListType)layout).ListSize;
                var child = Rebase(data.Children[0].Slice(
                    checked(data.Offset * size), checked(data.Length * size)));
                if (child is null)
                    return null;
                return new ArrayData(data.DataType, data.Length, nullCount, 0, new[] { validity }, new[] { child });
            }

            default:
            {
                // Every remaining fixed-width layout whose slots are whole bytes: integers, floats, decimals,
                // temporal types, fixed-size binary, intervals. Anything else (views, unions, run-end
                // encoding, null) is left to the caller's gather.
                if (layout is not FixedWidthType fixedWidth || fixedWidth.BitWidth % 8 != 0 || data.Buffers.Length != 2)
                    return null;
                if (!TryValidity(data, out var validity, out int nullCount))
                    return null;
                int width = fixedWidth.BitWidth / 8;
                var values = Window(data.Buffers[1], checked(data.Offset * width), checked(data.Length * width));
                return new ArrayData(data.DataType, data.Length, nullCount, 0, new[] { validity, values });
            }
        }
    }

    /// <summary>
    /// The node's validity from bit 0, and its null count. A node without nulls keeps no bitmap, as a gather
    /// leaves it. False for a node that reports nulls but has no bitmap to say which rows they are.
    /// </summary>
    private static bool TryValidity(ArrayData data, out ArrowBuffer validity, out int nullCount)
    {
        bool hasBitmap = data.Buffers.Length > 0 && data.Buffers[0].Length > 0;
        nullCount = hasBitmap ? data.GetNullCount() : (data.NullCount > 0 ? -1 : 0);
        if (nullCount < 0)
        {
            validity = ArrowBuffer.Empty;
            return false;
        }

        validity = nullCount == 0 ? ArrowBuffer.Empty : Bits(data.Buffers[0], data.Offset, data.Length);
        return true;
    }

    /// <summary>A view of <paramref name="count"/> bytes of a buffer from <paramref name="start"/>.</summary>
    private static ArrowBuffer Window(ArrowBuffer buffer, int start, int count) =>
        new(buffer.Memory.Slice(start, count));

    /// <summary>
    /// <paramref name="count"/> bits of a bitmap from bit <paramref name="start"/>, re-based to bit 0: a view
    /// when <paramref name="start"/> is on a byte boundary, otherwise a shifted copy.
    /// </summary>
    private static ArrowBuffer Bits(ArrowBuffer buffer, int start, int count)
    {
        int bytes = (count + 7) / 8;
        int shift = start & 7;
        if (shift == 0)
            return Window(buffer, start >> 3, bytes);

        var source = buffer.Span;
        int first = start >> 3;
        var shifted = new byte[bytes];
        for (int i = 0; i < bytes; i++)
        {
            int low = source[first + i] >> shift;
            int high = first + i + 1 < source.Length ? source[first + i + 1] << (8 - shift) : 0;
            shifted[i] = (byte)(low | high);
        }

        // The bits past the last row came from whatever followed in the source; clear them, as a builder would.
        int tail = count & 7;
        if (tail != 0)
            shifted[bytes - 1] &= (byte)((1 << tail) - 1);
        return new ArrowBuffer(shifted);
    }
}
