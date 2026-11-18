// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Numerics;
#endif
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// The least and greatest INT32 or INT64 values among those present, for statistics: signed, or unsigned
/// for <c>INT(32|64, false)</c>.
/// </summary>
/// <remarks>
/// <para>Unsigned order is signed order with the sign bit flipped, so both orders run one signed scan
/// over the values XORed with a bias (<see cref="int.MinValue"/> for unsigned, 0 for signed), and the
/// bias comes off the result.</para>
/// <para>A null stands in as the first present value, a duplicate that moves neither bound, so the
/// level picks the value without a branch. On .NET 8 and later the scan runs a <see cref="Vector{T}"/>
/// at a time; elsewhere, and for the tail, a value at a time.</para>
/// <para>MEASURED per million values (<c>stats-profile</c>), against the <see cref="Comparison{T}"/>
/// delegate call per comparison this replaced: INT32 3.3 -> 0.06 ms, INT64 3.8 -> 0.18, INT64 with 14%
/// nulls 4.1 -> 0.27. That was over half the time of writing an INT32 column without a dictionary.</para>
/// </remarks>
internal static class IntegerBounds
{
    /// <summary>
    /// The bounds of the values of <paramref name="values"/> whose level is not 0 (every value when
    /// <paramref name="levels"/> is empty); false when there is none.
    /// </summary>
    public static bool TryInt32(
        ReadOnlySpan<int> values, ReadOnlySpan<int> levels, bool unsigned, out int min, out int max)
    {
        int first = FirstPresent(levels, values.Length);
        if (first < 0)
        {
            min = max = 0;
            return false;
        }

        int bias = unsigned ? int.MinValue : 0;
        int seed = values[first] ^ bias;
        int lo = seed, hi = seed;
        int i = first + 1;
        ref int v = ref MemoryMarshal.GetReference(values);
        ref int l = ref MemoryMarshal.GetReference(levels);
        bool dense = levels.IsEmpty;

#if NET8_0_OR_GREATER
        int width = Vector<int>.Count;
        if (Vector.IsHardwareAccelerated && values.Length - i >= width)
        {
            var vBias = new Vector<int>(bias);
            var vSeed = new Vector<int>(seed);
            Vector<int> vLo = vSeed, vHi = vSeed;
            for (; i <= values.Length - width; i += width)
            {
                var x = Vector.LoadUnsafe(ref v, (nuint)i) ^ vBias;
                if (!dense)
                    x = Vector.ConditionalSelect(Vector.Equals(Vector.LoadUnsafe(ref l, (nuint)i), Vector<int>.Zero), vSeed, x);
                vLo = Vector.Min(vLo, x);
                vHi = Vector.Max(vHi, x);
            }

            for (int k = 0; k < width; k++)
            {
                lo = Math.Min(lo, vLo[k]);
                hi = Math.Max(hi, vHi[k]);
            }
        }
#endif

        for (; i < values.Length; i++)
        {
            int x = Unsafe.Add(ref v, i) ^ bias;
            if (!dense)
                x = PresentValues.Select(Unsafe.Add(ref l, i) != 0, x, seed);
            lo = Math.Min(lo, x);
            hi = Math.Max(hi, x);
        }

        min = lo ^ bias;
        max = hi ^ bias;
        return true;
    }

    /// <inheritdoc cref="TryInt32"/>
    public static bool TryInt64(
        ReadOnlySpan<long> values, ReadOnlySpan<int> levels, bool unsigned, out long min, out long max)
    {
        int first = FirstPresent(levels, values.Length);
        if (first < 0)
        {
            min = max = 0;
            return false;
        }

        long bias = unsigned ? long.MinValue : 0;
        long seed = values[first] ^ bias;
        long lo = seed, hi = seed;
        int i = first + 1;
        ref long v = ref MemoryMarshal.GetReference(values);
        ref int l = ref MemoryMarshal.GetReference(levels);
        bool dense = levels.IsEmpty;

#if NET8_0_OR_GREATER
        // A vector of levels covers two of values: Widen splits it into the levels of each.
        int width = Vector<long>.Count;
        if (Vector.IsHardwareAccelerated && values.Length - i >= 2 * width)
        {
            var vBias = new Vector<long>(bias);
            var vSeed = new Vector<long>(seed);
            Vector<long> vLo = vSeed, vHi = vSeed;
            for (; i <= values.Length - 2 * width; i += 2 * width)
            {
                var x0 = Vector.LoadUnsafe(ref v, (nuint)i) ^ vBias;
                var x1 = Vector.LoadUnsafe(ref v, (nuint)(i + width)) ^ vBias;
                if (!dense)
                {
                    Vector.Widen(Vector.LoadUnsafe(ref l, (nuint)i), out Vector<long> l0, out Vector<long> l1);
                    x0 = Vector.ConditionalSelect(Vector.Equals(l0, Vector<long>.Zero), vSeed, x0);
                    x1 = Vector.ConditionalSelect(Vector.Equals(l1, Vector<long>.Zero), vSeed, x1);
                }

                vLo = Vector.Min(vLo, Vector.Min(x0, x1));
                vHi = Vector.Max(vHi, Vector.Max(x0, x1));
            }

            for (int k = 0; k < width; k++)
            {
                lo = Math.Min(lo, vLo[k]);
                hi = Math.Max(hi, vHi[k]);
            }
        }
#endif

        for (; i < values.Length; i++)
        {
            long x = Unsafe.Add(ref v, i) ^ bias;
            if (!dense)
                x = PresentValues.Select(Unsafe.Add(ref l, i) != 0, x, seed);
            lo = Math.Min(lo, x);
            hi = Math.Max(hi, x);
        }

        min = lo ^ bias;
        max = hi ^ bias;
        return true;
    }

    /// <summary>
    /// The first position whose level is not 0, 0 when there are no levels, or -1. Checks that there is a
    /// level for every value, which the unchecked loads rely on.
    /// </summary>
    private static int FirstPresent(ReadOnlySpan<int> levels, int count)
    {
        if (!levels.IsEmpty && levels.Length < count)
            throw new ArgumentException("Fewer definition levels than values.", nameof(levels));
        if (count == 0)
            return -1;
        if (levels.IsEmpty)
            return 0;

        for (int i = 0; i < count; i++)
        {
            if (levels[i] != 0)
                return i;
        }

        return -1;
    }
}
