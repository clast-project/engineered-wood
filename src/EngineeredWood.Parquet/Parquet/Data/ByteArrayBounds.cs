// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// The positions of the least and greatest BYTE_ARRAY values (unsigned lexicographic), for statistics.
/// </summary>
/// <remarks>
/// <para>Most values are neither a new least nor a new greatest, and a word settles that for most of
/// them without comparing bytes. Every value strictly between the bounds starts with the prefix the two
/// bounds share, so past that prefix, of length p, each value has a key: its next 8 bytes big-endian,
/// zero-filled past its end. If a value starts with the prefix and its key lies strictly between the
/// bounds' keys, it lies strictly between the bounds. A key strictly less than another's means the value
/// is less: the two differ at some byte in those 8, and a value that has ended there (zero fill) is a
/// prefix of the other, which orders first too.</para>
/// <para>Anything else is compared byte by byte, as before: a value that does not start with the
/// prefix, or whose key ties a bound's. Ties keep the first position, so the result is the same.</para>
/// <para>MEASURED per million values (<c>stats-profile</c>): see the PR that introduced this class.</para>
/// </remarks>
internal static class ByteArrayBounds
{
    /// <summary>
    /// The positions in <paramref name="start"/> .. <paramref name="end"/> of the least and greatest values
    /// whose level is not 0 (every value when <paramref name="levels"/> is null); -1 for both when there is
    /// none. Value i is <paramref name="data"/>[<paramref name="offsets"/>[i] .. <paramref name="offsets"/>[i + 1]].
    /// </summary>
    public static (int MinIdx, int MaxIdx) Find(
        ReadOnlySpan<int> offsets, ReadOnlySpan<byte> data, int[]? levels, int start, int end)
    {
        int minIdx = -1, maxIdx = -1;
        ReadOnlySpan<byte> min = default, max = default;
        int prefix = 0;
        ulong minKey = 0, maxKey = 0;

        for (int i = start; i < end; i++)
        {
            if (levels != null && levels[i] == 0)
                continue;

            int at = offsets[i];
            int length = offsets[i + 1] - at;
            var value = data.Slice(at, length);

            if (minIdx < 0)
            {
                minIdx = maxIdx = i;
                min = max = value;
                (prefix, minKey, maxKey) = Keys(data, offsets, minIdx, maxIdx, min, max);
                continue;
            }

            if (length >= prefix && value.Slice(0, prefix).SequenceEqual(min.Slice(0, prefix)))
            {
                ulong key = Key(data, at + prefix, length - prefix);
                if (key > minKey && key < maxKey)
                    continue;
            }

            bool moved = false;
            if (value.SequenceCompareTo(min) < 0)
            {
                minIdx = i;
                min = value;
                moved = true;
            }

            if (value.SequenceCompareTo(max) > 0)
            {
                maxIdx = i;
                max = value;
                moved = true;
            }

            if (moved)
                (prefix, minKey, maxKey) = Keys(data, offsets, minIdx, maxIdx, min, max);
        }

        return (minIdx, maxIdx);
    }

    /// <summary>The bounds' shared prefix length and their keys past it.</summary>
    private static (int Prefix, ulong MinKey, ulong MaxKey) Keys(
        ReadOnlySpan<byte> data, ReadOnlySpan<int> offsets, int minIdx, int maxIdx,
        ReadOnlySpan<byte> min, ReadOnlySpan<byte> max)
    {
        int prefix = CommonPrefixLength(min, max);
        return (
            prefix,
            Key(data, offsets[minIdx] + prefix, min.Length - prefix),
            Key(data, offsets[maxIdx] + prefix, max.Length - prefix));
    }

    /// <summary>
    /// The first 8 of the <paramref name="length"/> bytes at <paramref name="at"/>, big-endian and
    /// left-aligned, zero past the value's end.
    /// </summary>
    private static ulong Key(ReadOnlySpan<byte> data, int at, int length)
    {
        if (length <= 0)
            return 0;

        int n = Math.Min(length, 8);
        ulong word;
        if (at + 8 <= data.Length)
        {
            // Whole word; the bytes past the value belong to the next one and are masked off.
            word = BinaryPrimitives.ReadUInt64BigEndian(data.Slice(at));
        }
        else
        {
            word = 0;
            for (int b = 0; b < n; b++)
                word |= (ulong)data[at + b] << (56 - 8 * b);
        }

        return n == 8 ? word : word & ~(ulong.MaxValue >> (8 * n));
    }

    private static int CommonPrefixLength(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
#if NET8_0_OR_GREATER
        return a.CommonPrefixLength(b);
#else
        int n = Math.Min(a.Length, b.Length);
        int i = 0;
        while (i < n && a[i] == b[i])
            i++;
        return i;
#endif
    }
}
