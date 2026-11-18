// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// The positions of the least and greatest FIXED_LEN_BYTE_ARRAY values of up to 16 bytes, for statistics:
/// unsigned lexicographic, or as big-endian two's-complement integers (a DECIMAL).
/// </summary>
/// <remarks>
/// <para>Each value becomes a 128-bit key, its bytes read big-endian into two words and left-aligned,
/// zero-filled past its length. Unsigned lexicographic order of equal-length values is the unsigned order of
/// those keys. Signed order compares the first byte signed and the rest unsigned, which is the unsigned
/// order of the keys with the top bit flipped. So a value is two word reads and a word compare or two,
/// where a byte comparison of a decimal ran through its leading sign bytes, which are mostly equal.</para>
/// <para>Ties keep the first position, as the byte comparison did; equal keys are equal bytes anyway.</para>
/// <para>MEASURED per million values (<c>stats-profile</c>), against the byte comparison: DECIMAL(38,4)
/// 9.1 -> 2.2 ms, random fixed_size_binary(16) 6.4 -> 1.5.</para>
/// </remarks>
internal static class FixedLenBounds
{
    /// <summary>The longest value this handles.</summary>
    public const int MaxLength = 16;

    /// <summary>
    /// The positions in <paramref name="start"/> .. <paramref name="end"/> of the least and greatest values
    /// whose level is not 0 (every value when <paramref name="levels"/> is null), each
    /// <paramref name="typeLength"/> bytes of <paramref name="buffer"/>; -1 for both when there is none.
    /// </summary>
    public static (int MinIdx, int MaxIdx) Find(
        ReadOnlySpan<byte> buffer, int typeLength, int[]? levels, bool signed, int start, int end)
    {
        if (typeLength is < 1 or > MaxLength)
            throw new ArgumentOutOfRangeException(nameof(typeLength));

        ulong flip = signed ? 1UL << 63 : 0;
        int minIdx = -1, maxIdx = -1;
        ulong minHi = 0, minLo = 0, maxHi = 0, maxLo = 0;

        for (int i = start; i < end; i++)
        {
            if (levels != null && levels[i] == 0)
                continue;

            Key(buffer.Slice(i * typeLength, typeLength), out ulong hi, out ulong lo);
            hi ^= flip;
            if (minIdx < 0)
            {
                minIdx = maxIdx = i;
                minHi = maxHi = hi;
                minLo = maxLo = lo;
                continue;
            }

            // A decimal's first word is mostly sign bytes, so whether it ties the bound's is a coin toss:
            // there the comparison is made without short-circuiting, one branch that is rarely taken.
            // Unsigned values (UUIDs, hashes) seldom tie on the first word, and short-circuiting is
            // cheaper. MEASURED per million: decimal 4.6 -> 2.2 ms this way; fixed_size_binary(16)
            // 1.2 short-circuited, 2.2 not.
            bool less = signed
                ? (hi < minHi) | ((hi == minHi) & (lo < minLo))
                : hi < minHi || (hi == minHi && lo < minLo);
            if (less)
            {
                minIdx = i;
                minHi = hi;
                minLo = lo;
            }

            bool greater = signed
                ? (hi > maxHi) | ((hi == maxHi) & (lo > maxLo))
                : hi > maxHi || (hi == maxHi && lo > maxLo);
            if (greater)
            {
                maxIdx = i;
                maxHi = hi;
                maxLo = lo;
            }
        }

        return (minIdx, maxIdx);
    }

    /// <summary>
    /// The 128-bit key of <paramref name="value"/>: its bytes big-endian, left-aligned, zero-filled.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Key(ReadOnlySpan<byte> value, out ulong hi, out ulong lo)
    {
        if (value.Length >= 8)
        {
            hi = BinaryPrimitives.ReadUInt64BigEndian(value);
            lo = value.Length == 16
                ? BinaryPrimitives.ReadUInt64BigEndian(value.Slice(8))
                : Partial(value.Slice(8));
        }
        else
        {
            hi = Partial(value);
            lo = 0;
        }
    }

    /// <summary>Up to 8 bytes big-endian, left-aligned in a word: the rest of the word is zero.</summary>
    private static ulong Partial(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
            return 0;

        ulong word = 0;
        for (int i = 0; i < bytes.Length; i++)
            word = (word << 8) | bytes[i];
        return word << (8 * (8 - bytes.Length));
    }
}
