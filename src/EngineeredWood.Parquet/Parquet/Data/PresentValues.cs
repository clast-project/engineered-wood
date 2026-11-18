// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Per-value work over a flat column's definition levels (0 = null, anything else = present) done without
/// a branch on the level, which a random null pattern mispredicts.
/// </summary>
internal static class PresentValues
{
    /// <summary>
    /// Copies the values whose level is not zero, in order, into <paramref name="dense"/>, stopping once it
    /// is full, and returns how many it copied.
    /// </summary>
    /// <remarks>
    /// Every value is stored at the next free slot and the slot advances only for a present one: MEASURED
    /// 1.4 -> 0.5 ms per million values at one in seven null at random, and 3.1 -> 0.5 at half (#919). An
    /// absent value's store lands in a slot a later present value overwrites; stopping once
    /// <paramref name="dense"/> is full keeps the stores of any trailing absent values inside it.
    /// </remarks>
    public static int Gather<T>(ReadOnlySpan<T> values, ReadOnlySpan<int> levels, Span<T> dense)
        where T : unmanaged
    {
        int count = Math.Min(values.Length, levels.Length);
        int capacity = dense.Length;
        ref T src = ref MemoryMarshal.GetReference(values);
        ref int level = ref MemoryMarshal.GetReference(levels);
        ref T dst = ref MemoryMarshal.GetReference(dense);
        int idx = 0;
        for (int i = 0; i < count && idx < capacity; i++)
        {
            Unsafe.Add(ref dst, idx) = Unsafe.Add(ref src, i);
            idx += Unsafe.Add(ref level, i) != 0 ? 1 : 0;
        }

        return idx;
    }

    /// <summary>
    /// Spreads the first <paramref name="present"/> values of <paramref name="values"/>, in place, out to
    /// the rows whose bit is set in <paramref name="validity"/> (LSB first), keeping their order. What the
    /// rows whose bit is clear end up holding is unspecified.
    /// </summary>
    /// <remarks>
    /// Walks down from the last row, so each value moves up and is read before anything overwrites it. A
    /// whole byte of present rows moves as one block; a mixed byte stores at all eight rows and steps the
    /// read index by each bit; an all-null byte is skipped. It stops as soon as the read index meets the
    /// row (everything below is present and already in place) or runs out (everything below is null).
    /// MEASURED per bit -> this, ms per million doubles: 0.9 -> 0 with no nulls, ~1.6 -> ~0.76 at one in
    /// seven null at random. Reads and writes stay below <paramref name="rows"/> for any
    /// <paramref name="present"/> up to it; a count that disagrees with the bitmap gives wrong values or an
    /// exception, never an access outside <paramref name="values"/>.
    /// </remarks>
    public static void Scatter<T>(Span<T> values, ReadOnlySpan<byte> validity, int rows, int present)
        where T : unmanaged
    {
        if (rows < 0)
            throw new ArgumentOutOfRangeException(nameof(rows));
        if ((uint)present > (uint)rows)
            throw new ArgumentOutOfRangeException(nameof(present));

        // Nothing to move. An all-null page may never have allocated its value buffer.
        if (present == 0)
            return;

        if (values.Length < rows || validity.Length < (rows + 7) / 8)
            throw new ArgumentOutOfRangeException(nameof(rows));

        ref T v = ref MemoryMarshal.GetReference(values);
        ref byte bits = ref MemoryMarshal.GetReference(validity);
        int read = present - 1;
        int i = rows - 1;

        // The final partial byte, one row at a time, until i is the top row of a whole byte (or -1).
        for (; (i & 7) != 7 && read >= 0 && read < i; i--)
        {
            if (((Unsafe.Add(ref bits, i >> 3) >> (i & 7)) & 1) != 0)
                Unsafe.Add(ref v, i) = Unsafe.Add(ref v, read--);
        }

        // Whole bytes, highest first; i is always 8k + 7 here.
        for (; read >= 0 && read < i; i -= 8)
        {
            int b = Unsafe.Add(ref bits, i >> 3);
            if (b == 0xFF)
            {
                // Overlapping is fine: CopyTo moves as memmove does.
                values.Slice(read - 7, 8).CopyTo(values.Slice(i - 7, 8));
                read -= 8;
            }
            else if (b != 0)
            {
                // A null row takes a copy of the next value down. That value sits below the row, so this
                // never overwrites one still to be moved.
                for (int j = 7; j >= 0; j--)
                {
                    Unsafe.Add(ref v, i - 7 + j) = Unsafe.Add(ref v, Math.Max(read, 0));
                    read -= (b >> j) & 1;
                }
            }
        }
    }

    /// <summary><paramref name="value"/> if <paramref name="present"/>, else <paramref name="otherwise"/>,
    /// chosen by mask for a 4- or 8-byte type rather than by a branch.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T Select<T>(bool present, T value, T otherwise)
        where T : unmanaged
    {
        if (Unsafe.SizeOf<T>() == sizeof(long))
        {
            long mask = -(long)(present ? 1 : 0);
            long bits = (Unsafe.As<T, long>(ref value) & mask) | (Unsafe.As<T, long>(ref otherwise) & ~mask);
            return Unsafe.As<long, T>(ref bits);
        }

        if (Unsafe.SizeOf<T>() == sizeof(int))
        {
            int mask = -(present ? 1 : 0);
            int bits = (Unsafe.As<T, int>(ref value) & mask) | (Unsafe.As<T, int>(ref otherwise) & ~mask);
            return Unsafe.As<int, T>(ref bits);
        }

        return present ? value : otherwise;
    }
}
