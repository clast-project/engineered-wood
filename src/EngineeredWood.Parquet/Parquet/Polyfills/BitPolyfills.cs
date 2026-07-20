// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if !NET8_0_OR_GREATER
using System.Runtime.CompilerServices;

namespace EngineeredWood.Parquet;

/// <summary>
/// Scalar fallbacks for <see cref="System.Numerics.BitOperations"/> methods
/// that are unavailable on netstandard2.0.
/// </summary>
internal static class BitPolyfills
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int LeadingZeroCount(uint value)
    {
        if (value == 0) return 32;
        int n = 1;
        if ((value >> 16) == 0) { n += 16; value <<= 16; }
        if ((value >> 24) == 0) { n += 8; value <<= 8; }
        if ((value >> 28) == 0) { n += 4; value <<= 4; }
        if ((value >> 30) == 0) { n += 2; value <<= 2; }
        n -= (int)(value >> 31);
        return n;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int LeadingZeroCount(ulong value)
    {
        uint hi = (uint)(value >> 32);
        if (hi != 0) return LeadingZeroCount(hi);
        return 32 + LeadingZeroCount((uint)value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int PopCount(uint value)
    {
        value -= (value >> 1) & 0x55555555u;
        value = (value & 0x33333333u) + ((value >> 2) & 0x33333333u);
        return (int)((((value + (value >> 4)) & 0x0F0F0F0Fu) * 0x01010101u) >> 24);
    }
}
#endif
