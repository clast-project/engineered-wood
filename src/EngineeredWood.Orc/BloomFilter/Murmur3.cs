// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace EngineeredWood.Orc.BloomFilter;

/// <summary>
/// Implements murmur3 hash functions used by ORC bloom filters.
/// </summary>
/// <remarks>
/// ORC has two incompatible implementations:
/// <list type="bullet">
/// <item><description>
/// <b>Java ORC</b>: uses <c>murmur3_x64_128</c> (two accumulators, 16-byte blocks)
/// and returns h1. Thomas Wang's hash uses unsigned right shifts (<c>&gt;&gt;&gt;</c>).
/// </description></item>
/// <item><description>
/// <b>C++ ORC</b>: uses a simplified <c>murmur3_64</c> (single accumulator, 8-byte blocks).
/// Thomas Wang's hash uses signed right shifts (<c>&gt;&gt;</c>), which produces different
/// results for negative intermediate values.
/// </description></item>
/// </list>
/// This class implements both variants. The C++ variant is the default since PyArrow
/// and the Apache Arrow ORC adapter use the C++ implementation.
/// </remarks>
internal static class Murmur3
{
    private const ulong C1 = 0x87c37b91114253d5UL;
    private const ulong C2 = 0x4cf5ad432745937fUL;
    internal const int DefaultSeed = 104729;

    // ────────────────────────────────────────────────────────
    // C++ ORC variant: single-accumulator murmur3_64 (8-byte blocks)
    // This matches Apache ORC C++ (used by PyArrow, Arrow ORC adapter)
    // ────────────────────────────────────────────────────────

    /// <summary>
    /// C++ ORC murmur3_64: single accumulator, 8-byte blocks.
    /// This is the default for files written by the C++ ORC library (PyArrow, Arrow).
    /// </summary>
    public static long Hash64Cpp(ReadOnlySpan<byte> data, int seed = DefaultSeed)
    {
        ulong h = unchecked((ulong)(uint)seed);
        int blocks = data.Length >> 3;

        for (int i = 0; i < blocks; i++)
        {
            ulong k = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(i * 8));
            k *= C1;
            k = RotateLeft(k, 31);
            k *= C2;

            h ^= k;
            h = RotateLeft(h, 27);
            h = h * 5 + 0x52dce729;
        }

        ulong tail = 0;
        int idx = blocks << 3;
        int remaining = data.Length - idx;
        if (remaining >= 7) tail ^= (ulong)data[idx + 6] << 48;
        if (remaining >= 6) tail ^= (ulong)data[idx + 5] << 40;
        if (remaining >= 5) tail ^= (ulong)data[idx + 4] << 32;
        if (remaining >= 4) tail ^= (ulong)data[idx + 3] << 24;
        if (remaining >= 3) tail ^= (ulong)data[idx + 2] << 16;
        if (remaining >= 2) tail ^= (ulong)data[idx + 1] << 8;
        if (remaining >= 1)
        {
            tail ^= (ulong)data[idx];
            tail *= C1;
            tail = RotateLeft(tail, 31);
            tail *= C2;
            h ^= tail;
        }

        h ^= (ulong)data.Length;
        h = FMix64(h);
        return (long)h;
    }

    /// <summary>
    /// Thomas Wang's 64-bit integer hash (C++ ORC variant).
    /// Uses <b>signed</b> right shifts, matching the C++ ORC implementation.
    /// Produces different results from the Java variant for inputs where
    /// intermediate values are negative (e.g., key=0 or negative keys).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetLongHashCpp(long key)
    {
        key = (~key) + (key << 21);
        key ^= key >> 24;  // signed right shift (C++ int64_t >>)
        key = (key + (key << 3)) + (key << 8);
        key ^= key >> 14;  // signed right shift
        key = (key + (key << 2)) + (key << 4);
        key ^= key >> 28;  // signed right shift
        key += key << 31;
        return key;
    }

    // ────────────────────────────────────────────────────────
    // Java ORC variant: murmur3_x64_128 returning h1 (16-byte blocks)
    // This matches Apache ORC Java (used by Hive, Spark, Presto)
    // ────────────────────────────────────────────────────────

    /// <summary>
    /// Java ORC murmur3_x64_128: two accumulators, 16-byte blocks, returns h1.
    /// Matches the Java ORC library used by Hive, Spark, and Presto.
    /// </summary>
    public static long Hash64Java(ReadOnlySpan<byte> data, int seed = DefaultSeed)
    {
        ulong h1 = unchecked((ulong)seed);
        ulong h2 = unchecked((ulong)seed);

        int nblocks = data.Length / 16;

        for (int i = 0; i < nblocks; i++)
        {
            ulong k1 = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(i * 16));
            ulong k2 = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(i * 16 + 8));

            k1 *= C1;
            k1 = RotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;

            h1 = RotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= C2;
            k2 = RotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;

            h2 = RotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        ulong tail1 = 0;
        ulong tail2 = 0;
        int tailStart = nblocks * 16;
        int remaining = data.Length - tailStart;

        if (remaining >= 15) tail2 ^= (ulong)data[tailStart + 14] << 48;
        if (remaining >= 14) tail2 ^= (ulong)data[tailStart + 13] << 40;
        if (remaining >= 13) tail2 ^= (ulong)data[tailStart + 12] << 32;
        if (remaining >= 12) tail2 ^= (ulong)data[tailStart + 11] << 24;
        if (remaining >= 11) tail2 ^= (ulong)data[tailStart + 10] << 16;
        if (remaining >= 10) tail2 ^= (ulong)data[tailStart + 9] << 8;
        if (remaining >= 9)
        {
            tail2 ^= (ulong)data[tailStart + 8];
            tail2 *= C2;
            tail2 = RotateLeft(tail2, 33);
            tail2 *= C1;
            h2 ^= tail2;
        }

        if (remaining >= 8) tail1 ^= (ulong)data[tailStart + 7] << 56;
        if (remaining >= 7) tail1 ^= (ulong)data[tailStart + 6] << 48;
        if (remaining >= 6) tail1 ^= (ulong)data[tailStart + 5] << 40;
        if (remaining >= 5) tail1 ^= (ulong)data[tailStart + 4] << 32;
        if (remaining >= 4) tail1 ^= (ulong)data[tailStart + 3] << 24;
        if (remaining >= 3) tail1 ^= (ulong)data[tailStart + 2] << 16;
        if (remaining >= 2) tail1 ^= (ulong)data[tailStart + 1] << 8;
        if (remaining >= 1)
        {
            tail1 ^= (ulong)data[tailStart];
            tail1 *= C1;
            tail1 = RotateLeft(tail1, 31);
            tail1 *= C2;
            h1 ^= tail1;
        }

        h1 ^= (ulong)data.Length;
        h2 ^= (ulong)data.Length;

        h1 += h2;
        h2 += h1;

        h1 = FMix64(h1);
        h2 = FMix64(h2);

        h1 += h2;

        return (long)h1;
    }

    /// <summary>
    /// Thomas Wang's 64-bit integer hash (Java ORC variant).
    /// Uses <b>unsigned</b> right shifts, matching Java's <c>&gt;&gt;&gt;</c> operator.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetLongHashJava(long key)
    {
        key = (~key) + (key << 21);
        key ^= (long)((ulong)key >> 24);  // unsigned right shift (Java >>>)
        key = (key + (key << 3)) + (key << 8);
        key ^= (long)((ulong)key >> 14);
        key = (key + (key << 2)) + (key << 4);
        key ^= (long)((ulong)key >> 28);
        key += key << 31;
        return key;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong RotateLeft(ulong x, int r) => (x << r) | (x >> (64 - r));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong FMix64(ulong k)
    {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdUL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53UL;
        k ^= k >> 33;
        return k;
    }
}
