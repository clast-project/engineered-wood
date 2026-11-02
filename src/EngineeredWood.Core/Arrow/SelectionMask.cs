// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Apache.Arrow;

namespace EngineeredWood.Arrow;

/// <summary>
/// A row selection over one batch, as a span of <c>ulong</c> words: bit <c>i</c> (LSB-first, word
/// <c>i / 64</c>) selects row <c>i</c>. That is Arrow's own bitmap layout, so a <see cref="BooleanArray"/>
/// mask combines with it a word at a time and no form has to be converted into another.
///
/// <para>This is where independent row filters meet: a deletion vector clears its deleted rows
/// (<c>DeletionVectorBitmap.ClearDeleted</c>), each predicate mask is ANDed in (<see cref="And"/>), and the
/// result says which rows of the remaining columns to materialize — gathered once, by
/// <see cref="ToIndices"/>, rather than once per filter.</para>
///
/// <para>Bits at or past the batch length are kept clear by every operation here, so <see cref="Count"/> and
/// <see cref="ToIndices"/> never see them.</para>
/// </summary>
internal static class SelectionMask
{
    /// <summary>Words needed for <paramref name="rows"/> rows.</summary>
    public static int WordCount(int rows) => (int)(((long)rows + 63) >> 6);

    /// <summary>Selects rows [0, <paramref name="rows"/>) and clears the rest of the last word.</summary>
    public static void SelectAll(Span<ulong> words, int rows)
    {
        int n = WordCount(rows);
        words.Slice(0, n).Fill(ulong.MaxValue);
        if ((rows & 63) != 0)
            words[n - 1] = ulong.MaxValue >> (64 - (rows & 63));
    }

    /// <summary>Deselects rows [<paramref name="from"/>, <paramref name="to"/>).</summary>
    public static void ClearRange(Span<ulong> words, int from, int to)
    {
        if (from >= to)
            return;
        int fw = from >> 6, lw = (to - 1) >> 6;
        ulong first = ulong.MaxValue << (from & 63);
        ulong last = ulong.MaxValue >> (63 - ((to - 1) & 63));
        if (fw == lw)
        {
            words[fw] &= ~(first & last);
            return;
        }
        words[fw] &= ~first;
        words.Slice(fw + 1, lw - fw - 1).Clear();
        words[lw] &= ~last;
    }

    /// <summary>Selects exactly the rows of [0, <paramref name="rows"/>) that were not selected.</summary>
    public static void Invert(Span<ulong> words, int rows)
    {
        int n = WordCount(rows);
        for (int w = 0; w < n; w++)
            words[w] = ~words[w];
        if ((rows & 63) != 0)
            words[n - 1] &= ulong.MaxValue >> (64 - (rows & 63));
    }

    /// <summary>Selects rows [<paramref name="from"/>, <paramref name="to"/>).</summary>
    public static void SetRange(Span<ulong> words, int from, int to)
    {
        if (from >= to)
            return;
        int fw = from >> 6, lw = (to - 1) >> 6;
        ulong first = ulong.MaxValue << (from & 63);
        ulong last = ulong.MaxValue >> (63 - ((to - 1) & 63));
        if (fw == lw)
        {
            words[fw] |= first & last;
            return;
        }
        words[fw] |= first;
        words.Slice(fw + 1, lw - fw - 1).Fill(ulong.MaxValue);
        words[lw] |= last;
    }

    /// <summary>
    /// Keeps selected only the rows <paramref name="mask"/> selects: those whose value is true. A null
    /// deselects its row, as a SQL <c>WHERE</c> treats an unknown condition.
    /// </summary>
    /// <exception cref="ArgumentException">The mask's length is not <paramref name="rows"/>.</exception>
    public static void And(Span<ulong> words, int rows, BooleanArray mask)
    {
        if (mask is null)
            throw new ArgumentNullException(nameof(mask));
        if (mask.Length != rows)
            throw new ArgumentException($"The mask has {mask.Length} rows and the selection {rows}.", nameof(mask));

        AndBits(words, rows, mask.ValueBuffer.Span, mask.Offset);
        if (mask.NullCount != 0)
            AndBits(words, rows, mask.NullBitmapBuffer.Span, mask.Offset);
        GC.KeepAlive(mask); // spans over its buffers are read above
    }

    // ANDs bits [bitOffset, bitOffset + rows) of an Arrow bitmap into words[0..WordCount(rows)).
    private static void AndBits(Span<ulong> words, int rows, ReadOnlySpan<byte> bitmap, int bitOffset)
    {
        int n = WordCount(rows);
        for (int w = 0; w < n; w++)
            words[w] &= ReadWord(bitmap, bitOffset + (w << 6));
        // Bits past `rows` in the last word were clear already and AND keeps them clear.
    }

    // 64 bits of an Arrow bitmap starting at bit `bit`. Bytes past the buffer's end read as zero: a buffer
    // need not be padded to a whole word, and those bits are past the array's length anyway.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadWord(ReadOnlySpan<byte> bitmap, int bit)
    {
        int at = bit >> 3, shift = bit & 7;
        ulong lo;
        if (bitmap.Length - at >= 8)
        {
            lo = BinaryPrimitives.ReadUInt64LittleEndian(bitmap.Slice(at, 8));
        }
        else
        {
            lo = 0;
            for (int i = 0; at + i < bitmap.Length; i++)
                lo |= (ulong)bitmap[at + i] << (8 * i);
        }
        if (shift == 0)
            return lo;
        ulong hi = at + 8 < bitmap.Length ? bitmap[at + 8] : 0UL;
        return (lo >> shift) | (hi << (64 - shift));
    }

    /// <summary>How many rows are selected.</summary>
    public static int Count(ReadOnlySpan<ulong> words)
    {
        int count = 0;
        foreach (ulong w in words)
            count += PopCount(w);
        return count;
    }

    /// <summary>
    /// Writes the selected row indices to <paramref name="destination"/> in ascending order and returns how
    /// many there are. <paramref name="destination"/> must hold at least <see cref="Count"/> entries.
    /// </summary>
    public static int ToIndices(ReadOnlySpan<ulong> words, Span<int> destination)
    {
        int n = 0;
        for (int w = 0; w < words.Length; w++)
        {
            ulong bits = words[w];
            int baseIndex = w << 6;
            while (bits != 0)
            {
                destination[n++] = baseIndex + System.Numerics.BitOperations.TrailingZeroCount(bits);
                bits &= bits - 1;
            }
        }
        return n;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int PopCount(ulong value)
    {
#if NETCOREAPP3_0_OR_GREATER
        return System.Numerics.BitOperations.PopCount(value);
#else
        value -= (value >> 1) & 0x5555555555555555UL;
        value = (value & 0x3333333333333333UL) + ((value >> 2) & 0x3333333333333333UL);
        value = (value + (value >> 4)) & 0x0F0F0F0F0F0F0F0FUL;
        return (int)((value * 0x0101010101010101UL) >> 56);
#endif
    }
}
