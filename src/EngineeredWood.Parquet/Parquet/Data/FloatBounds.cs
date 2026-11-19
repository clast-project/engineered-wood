// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Numerics;
#endif
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// The least and greatest non-NaN FLOAT or DOUBLE values, with -0 below +0, and the NaN count, for
/// statistics.
/// </summary>
/// <remarks>
/// <para>For non-NaN values that order (numeric, -0 below +0) is IEEE 754 total order, and total order is
/// signed integer order of the key <c>bits ^ ((bits &gt;&gt; 63) &amp; 0x7FFF…)</c>: a negative value's
/// magnitude bits are flipped so larger magnitudes sort lower. The key is its own inverse. So the bounds
/// are an integer min/max over the keys, NaN lanes replaced by the neutral key (the greatest for the
/// minimum, the least for the maximum) and counted. On .NET 8 and later a <see cref="Vector{T}"/> at a
/// time; elsewhere, and for the tail, a value at a time.</para>
/// <para>Returns the bounds' bits, as the scan they replace returned the values: the bytes written are the
/// same. MEASURED per million values (<c>stats-profile</c>): see the PR that introduced this class.</para>
/// </remarks>
internal static class FloatBounds
{
    /// <summary>
    /// The bits of the least and greatest non-NaN values and the NaN count; false when every value is
    /// NaN (or there is none), which leaves the bounds to the caller.
    /// </summary>
    public static bool TryFloat(ReadOnlySpan<float> values, out int minBits, out int maxBits, out long nanCount)
    {
        var bits = MemoryMarshal.Cast<float, int>(values);
        ref int b = ref MemoryMarshal.GetReference(bits);
        int lo = int.MaxValue, hi = int.MinValue;
        long nans = 0;
        int i = 0;

#if NET8_0_OR_GREATER
        int width = Vector<int>.Count;
        if (Vector.IsHardwareAccelerated && bits.Length >= width)
        {
            var magnitude = new Vector<int>(0x7FFF_FFFF);
            var infinity = new Vector<int>(0x7F80_0000);
            var vMax = new Vector<int>(int.MaxValue);
            var vMin = new Vector<int>(int.MinValue);
            Vector<int> vLo = vMax, vHi = vMin, vNans = Vector<int>.Zero;
            for (; i <= bits.Length - width; i += width)
            {
                var x = Vector.LoadUnsafe(ref b, (nuint)i);
                var isNaN = Vector.GreaterThan(x & magnitude, infinity);
                var key = x ^ (Vector.ShiftRightArithmetic(x, 31) & magnitude);
                vLo = Vector.Min(vLo, Vector.ConditionalSelect(isNaN, vMax, key));
                vHi = Vector.Max(vHi, Vector.ConditionalSelect(isNaN, vMin, key));
                vNans -= isNaN; // a true lane is -1
            }

            for (int k = 0; k < width; k++)
            {
                lo = Math.Min(lo, vLo[k]);
                hi = Math.Max(hi, vHi[k]);
                nans += vNans[k];
            }
        }
#endif

        for (; i < bits.Length; i++)
        {
            int x = Unsafe.Add(ref b, i);
            if ((x & 0x7FFF_FFFF) > 0x7F80_0000)
            {
                nans++;
                continue;
            }

            int key = x ^ ((x >> 31) & 0x7FFF_FFFF);
            lo = Math.Min(lo, key);
            hi = Math.Max(hi, key);
        }

        nanCount = nans;
        minBits = lo ^ ((lo >> 31) & 0x7FFF_FFFF);
        maxBits = hi ^ ((hi >> 31) & 0x7FFF_FFFF);
        return nans < bits.Length;
    }

    /// <inheritdoc cref="TryFloat"/>
    public static bool TryDouble(ReadOnlySpan<double> values, out long minBits, out long maxBits, out long nanCount)
    {
        var bits = MemoryMarshal.Cast<double, long>(values);
        ref long b = ref MemoryMarshal.GetReference(bits);
        long lo = long.MaxValue, hi = long.MinValue;
        long nans = 0;
        int i = 0;

#if NET8_0_OR_GREATER
        int width = Vector<long>.Count;
        if (Vector.IsHardwareAccelerated && bits.Length >= width)
        {
            var magnitude = new Vector<long>(0x7FFF_FFFF_FFFF_FFFF);
            var infinity = new Vector<long>(0x7FF0_0000_0000_0000);
            var vMax = new Vector<long>(long.MaxValue);
            var vMin = new Vector<long>(long.MinValue);
            Vector<long> vLo = vMax, vHi = vMin, vNans = Vector<long>.Zero;
            for (; i <= bits.Length - width; i += width)
            {
                var x = Vector.LoadUnsafe(ref b, (nuint)i);
                var isNaN = Vector.GreaterThan(x & magnitude, infinity);
                var key = x ^ (Vector.ShiftRightArithmetic(x, 63) & magnitude);
                vLo = Vector.Min(vLo, Vector.ConditionalSelect(isNaN, vMax, key));
                vHi = Vector.Max(vHi, Vector.ConditionalSelect(isNaN, vMin, key));
                vNans -= isNaN;
            }

            for (int k = 0; k < width; k++)
            {
                lo = Math.Min(lo, vLo[k]);
                hi = Math.Max(hi, vHi[k]);
                nans += vNans[k];
            }
        }
#endif

        for (; i < bits.Length; i++)
        {
            long x = Unsafe.Add(ref b, i);
            if ((x & 0x7FFF_FFFF_FFFF_FFFF) > 0x7FF0_0000_0000_0000)
            {
                nans++;
                continue;
            }

            long key = x ^ ((x >> 63) & 0x7FFF_FFFF_FFFF_FFFF);
            lo = Math.Min(lo, key);
            hi = Math.Max(hi, key);
        }

        nanCount = nans;
        minBits = lo ^ ((lo >> 63) & 0x7FFF_FFFF_FFFF_FFFF);
        maxBits = hi ^ ((hi >> 63) & 0x7FFF_FFFF_FFFF_FFFF);
        return nans < bits.Length;
    }
}
