// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#if NET8_0_OR_GREATER
using System.Runtime.Intrinsics;
#endif

namespace EngineeredWood.Arrow;

/// <summary>
/// Utility methods for building Arrow validity bitmaps.
/// Uses SIMD (Vector256/Vector128) where available with scalar fallback.
/// </summary>
internal static class BitmapHelper
{
    /// <summary>
    /// Sets bit <paramref name="index"/> in <paramref name="bitmap"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SetBit(Span<byte> bitmap, int index)
    {
        bitmap[index >> 3] |= (byte)(1 << (index & 7));
    }

    /// <summary>
    /// Builds a validity bitmap where bit i is set if <paramref name="values"/>[i] == <paramref name="target"/>.
    /// Uses SIMD comparison where hardware-accelerated.
    /// The bitmap must be pre-allocated to at least <c>(count + 7) / 8</c> bytes.
    /// </summary>
    public static void BuildFromEquality(
        ReadOnlySpan<byte> values, Span<byte> bitmap, int count, byte target)
    {
        ref byte valRef = ref MemoryMarshal.GetReference(values);
        int i = 0;

#if NET8_0_OR_GREATER
        if (Vector256.IsHardwareAccelerated && count >= 32)
        {
            var targetVec = Vector256.Create(target);
            int vectorEnd = count - 31;
            for (; i < vectorEnd; i += 32)
            {
                var v = Vector256.LoadUnsafe(ref valRef, (nuint)i);
                uint mask = Vector256.Equals(v, targetVec).ExtractMostSignificantBits();
                Unsafe.WriteUnaligned(ref bitmap[i >> 3], mask);
            }
        }
        else if (Vector128.IsHardwareAccelerated && count >= 16)
        {
            var targetVec = Vector128.Create(target);
            int vectorEnd = count - 15;
            for (; i < vectorEnd; i += 16)
            {
                var v = Vector128.LoadUnsafe(ref valRef, (nuint)i);
                uint mask = Vector128.Equals(v, targetVec).ExtractMostSignificantBits();
                Unsafe.WriteUnaligned(ref bitmap[i >> 3], (ushort)mask);
            }
        }
#endif

        // Scalar tail
        int tailBitmapStart = i >> 3;
        int totalBitmapBytes = (count + 7) / 8;
        bitmap.Slice(tailBitmapStart, totalBitmapBytes - tailBitmapStart).Clear();
        for (; i < count; i++)
        {
            if (values[i] == target)
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
        }
    }

    /// <summary>
    /// Builds a validity bitmap from a <c>bool[]</c> where bit i is set if <paramref name="values"/>[i] is true.
    /// In .NET, <c>bool</c> is 1 byte (0 = false, 1 = true), so this reinterprets as a byte comparison against 1.
    /// Uses SIMD where hardware-accelerated.
    /// </summary>
    public static void BuildFromBooleans(
        ReadOnlySpan<bool> values, Span<byte> bitmap, int count)
    {
        // bool in .NET is guaranteed 1 byte, with true=1, false=0
        var asBytes = MemoryMarshal.AsBytes(values);
        BuildFromEquality(asBytes, bitmap, count, 1);
    }
}
