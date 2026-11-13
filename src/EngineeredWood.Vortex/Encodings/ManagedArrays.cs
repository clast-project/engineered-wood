// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Buffers the decoders build in managed memory, where Arrow's own helpers would allocate native memory.
/// </summary>
/// <remarks>
/// The decoders read their children through spans, and a span over native memory does not keep the child alive: once
/// nothing references the child, a collection runs its buffer's finalizer and the span reads freed memory. Every
/// other buffer a Vortex decoder builds is a <c>byte[]</c>, which the span itself roots, so the decoders need no
/// <see cref="GC.KeepAlive(object)"/>. These helpers keep it that way for the two places that used Arrow's builders:
/// validity bitmaps and the join of a chunked child (#837; doc/arrow-span-lifetime.md).
/// </remarks>
internal static class ManagedArrays
{
    /// <summary>A validity bitmap of <paramref name="length"/> bits, bit <c>i</c> set when
    /// <paramref name="isValid"/> says row <c>i</c> is valid.</summary>
    public static ArrowBuffer Bitmap(int length, Func<int, bool> isValid)
    {
        var bits = new byte[(length + 7) / 8];
        for (int i = 0; i < length; i++)
        {
            if (isValid(i))
                bits[i >> 3] |= (byte)(1 << (i & 7));
        }
        return new ArrowBuffer(bits);
    }

    /// <summary>
    /// Joins the chunks of a chunked array end to end. A fixed-width or boolean type, which is what the decoders
    /// read child spans from, is joined here into managed buffers; any other type goes to
    /// <see cref="ArrowCompute.Concatenate"/>, which joins strings, binaries and structs managed too.
    /// </summary>
    public static IArrowArray Concatenate(IReadOnlyList<IArrowArray> chunks)
    {
        var inputs = new List<IArrowArray>(chunks.Count);
        foreach (var chunk in chunks)
        {
            if (chunk.Length > 0)
                inputs.Add(chunk);
        }

        // Anything else is joined (and its types checked, decimals widened) by ArrowCompute. That joins strings,
        // binaries and structs into managed buffers, but leaves every other type, a struct's or list's primitive
        // children included, to Arrow's native concatenator, so its result is copied wherever it is native.
        if (inputs.Count < 2 || !Joinable(inputs))
            return ToManaged(ArrowCompute.Concatenate(chunks));

        var type = inputs[0].Data.DataType;
        int length = 0;
        int nullCount = 0;
        foreach (var chunk in inputs)
        {
            length = checked(length + chunk.Length);
            nullCount += chunk.Data.GetNullCount();
        }

        byte[] values;
        if (type is BooleanType)
        {
            values = new byte[(length + 7) / 8];
            int row = 0;
            foreach (var chunk in inputs)
            {
                CopyBits(chunk.Data.Buffers[1].Span, chunk.Offset, values, row, chunk.Length);
                row += chunk.Length;
            }
        }
        else
        {
            int width = ((FixedWidthType)type).BitWidth / 8;
            values = new byte[checked(length * width)];
            int position = 0;
            foreach (var chunk in inputs)
            {
                int bytes = chunk.Length * width;
                chunk.Data.Buffers[1].Span.Slice(chunk.Offset * width, bytes).CopyTo(values.AsSpan(position));
                position += bytes;
            }
        }

        var validity = ArrowBuffer.Empty;
        if (nullCount > 0)
        {
            var bits = new byte[(length + 7) / 8];
            int row = 0;
            foreach (var chunk in inputs)
            {
                var source = chunk.Data.Buffers[0].Span;
                if (source.IsEmpty)
                {
                    // No bitmap: every row valid, unless the null count says every row is null (as Arrow reads it).
                    if (chunk.Data.GetNullCount() == 0)
                    {
                        for (int i = 0; i < chunk.Length; i++)
                            bits[(row + i) >> 3] |= (byte)(1 << ((row + i) & 7));
                    }
                }
                else
                {
                    CopyBits(source, chunk.Offset, bits, row, chunk.Length);
                }
                row += chunk.Length;
            }
            validity = new ArrowBuffer(bits);
        }

        // The chunks are kept alive until every span over their buffers has been read.
        GC.KeepAlive(inputs);
        return Apache.Arrow.ArrowArrayFactory.BuildArray(
            new ArrayData(type, length, nullCount, offset: 0, new[] { validity, new ArrowBuffer(values) }));
    }

    /// <summary>Whether every chunk is a fixed-width type of whole bytes, or boolean, laid out as validity and
    /// values only, and of the very same type instance as the first. A chunked node decodes every chunk at its own
    /// type, so that is the common case; anything else (a decimal of another scale or precision, say) is left to
    /// <see cref="ArrowCompute.Concatenate"/>, which checks the whole type and widens a decimal's precision.</summary>
    private static bool Joinable(List<IArrowArray> inputs)
    {
        var type = inputs[0].Data.DataType;
        bool boolean = type is BooleanType;
        if (!boolean && (type is not FixedWidthType { BitWidth: > 0 } fixedWidth || fixedWidth.BitWidth % 8 != 0))
            return false;

        foreach (var chunk in inputs)
        {
            var data = chunk.Data;
            if (!ReferenceEquals(data.DataType, type)
                || data.Buffers.Length != 2
                || data.Children is { Length: > 0 }
                || data.Dictionary is not null)
                return false;
        }
        return true;
    }

    /// <summary><paramref name="array"/> itself when every buffer in it, at any depth, is managed; otherwise a copy
    /// whose native buffers are copied into <c>byte[]</c> (managed ones are shared).</summary>
    internal static IArrowArray ToManaged(IArrowArray array)
    {
        if (IsManaged(array.Data))
            return array;
        var copy = Apache.Arrow.ArrowArrayFactory.BuildArray(ToManaged(array.Data));
        GC.KeepAlive(array); // its native buffers are read above
        return copy;
    }

    private static ArrayData ToManaged(ArrayData data)
    {
        var buffers = new ArrowBuffer[data.Buffers.Length];
        for (int i = 0; i < buffers.Length; i++)
        {
            var buffer = data.Buffers[i];
            buffers[i] = IsManaged(buffer) ? buffer : new ArrowBuffer(buffer.Span.ToArray());
        }
        var children = data.Children?.Select(ToManaged).ToArray();
        var dictionary = data.Dictionary is null ? null : ToManaged(data.Dictionary);
        return new ArrayData(data.DataType, data.Length, data.NullCount, data.Offset, buffers, children, dictionary);
    }

    private static bool IsManaged(ArrayData data) =>
        data.Buffers.All(IsManaged)
        && (data.Children is null || data.Children.All(IsManaged))
        && (data.Dictionary is null || IsManaged(data.Dictionary));

    private static bool IsManaged(ArrowBuffer buffer) =>
        buffer.IsEmpty || System.Runtime.InteropServices.MemoryMarshal.TryGetArray(buffer.Memory, out _);

    private static void CopyBits(ReadOnlySpan<byte> source, int sourceBit, Span<byte> target, int targetBit, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int s = sourceBit + i;
            int t = targetBit + i;
            if ((source[s >> 3] & (1 << (s & 7))) != 0)
                target[t >> 3] |= (byte)(1 << (t & 7));
        }
    }
}
