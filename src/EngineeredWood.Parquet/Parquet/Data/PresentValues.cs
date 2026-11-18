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
