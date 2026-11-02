// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Encodings;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Writes deletion vectors: the union of an existing <see cref="DeletionVectorBitmap"/> (or none) and a run of
/// ascending positions, serialized as Delta's RoaringBitmapArray in the portable CRoaring format.
///
/// <para>It works one 65,536-row container at a time, on a 64K-bit scratch bitmap: the existing container is
/// ORed in from whatever form it is stored in, the new positions are set (noting which were not already
/// deleted), and the container is written in its smallest form, as CRoaring's <c>runOptimize</c> picks it: a
/// sorted array (up to 4,096 values), a 1,024-word bitmap, or (when smaller than both) runs. No position is
/// ever held in a per-row set.</para>
/// </summary>
internal static class DeletionVectorBuilder
{
    private const uint Magic = 1681511377; // "MT1d", RoaringBitmapArray
    private const uint NoRunCookie = 12346;
    private const uint RunCookie = 12347;
    private const int NoOffsetThreshold = 4; // a run-cookie bitmap carries offsets only from 4 containers
    private const int ArrayMaxCardinality = 4096;
    private const int BitmapBytes = 8192;

    /// <summary>
    /// The largest position a Delta deletion vector holds: Delta's <c>RoaringBitmapArray</c> uses high keys up
    /// to <c>int.MaxValue - 1</c>, and <see cref="DeletionVectorBitmap"/> refuses any beyond.
    /// </summary>
    internal const long MaxPosition = ((long)(int.MaxValue - 1) << 32) | 0xFFFFFFFFL;

    internal const int EmptyBlobLength = 12;

    /// <summary>Magic and a sub-bitmap count of zero: the vector deleting nothing.</summary>
    internal static byte[] EmptyBlob()
    {
        var blob = new byte[EmptyBlobLength];
        BinaryPrimitives.WriteUInt32LittleEndian(blob, Magic);
        return blob; // the int64 count after it is zero
    }

    /// <summary>
    /// <paramref name="positions"/> sorted ascending with duplicates removed, refusing (under
    /// <paramref name="paramName"/>, the caller's own argument) any a Delta deletion vector cannot hold.
    /// </summary>
    internal static long[] SortedDistinct(IEnumerable<long> positions, string paramName)
    {
        long[] sorted = SortedDistinct(positions);
        if (sorted.Length > 0 && (sorted[0] < 0 || sorted[sorted.Length - 1] > MaxPosition))
        {
            long bad = sorted[0] < 0 ? sorted[0] : sorted[sorted.Length - 1];
            throw new ArgumentOutOfRangeException(
                paramName, bad, $"A deleted row position must be in [0, {MaxPosition}].");
        }
        return sorted;
    }

    /// <summary><paramref name="positions"/> sorted ascending with duplicates removed.</summary>
    internal static long[] SortedDistinct(IEnumerable<long> positions)
    {
        long[] sorted = positions.ToArray();
        Array.Sort(sorted);
        int n = 0;
        for (int i = 0; i < sorted.Length; i++)
            if (n == 0 || sorted[i] != sorted[n - 1])
                sorted[n++] = sorted[i];
        if (n != sorted.Length)
            Array.Resize(ref sorted, n);
        return sorted;
    }

    /// <summary>
    /// The union of <paramref name="existing"/> (null for none) and <paramref name="ascendingDistinct"/>,
    /// appending to <paramref name="newlyDeleted"/> each position <paramref name="existing"/> did not delete.
    /// </summary>
    /// <exception cref="ArgumentException">The positions are not strictly ascending.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A position is negative or past
    /// <see cref="MaxPosition"/>.</exception>
    internal static DeletionVectorBitmap Build(
        DeletionVectorBitmap? existing, ReadOnlySpan<long> ascendingDistinct, List<long>? newlyDeleted,
        bool allowRuns = true)
    {
        if (ascendingDistinct.Length > 0)
        {
            if (ascendingDistinct[0] < 0 || ascendingDistinct[ascendingDistinct.Length - 1] > MaxPosition)
            {
                long bad = ascendingDistinct[0] < 0 ? ascendingDistinct[0] : ascendingDistinct[ascendingDistinct.Length - 1];
                throw new ArgumentOutOfRangeException(
                    nameof(ascendingDistinct), bad, $"A deleted row position must be in [0, {MaxPosition}].");
            }
        }
        if (ascendingDistinct.Length == 0 && existing is not null)
            return existing;

        var containers = new List<Container>();
        var scratch = new ulong[1024];
        int c = 0, containerCount = existing?.ContainerCount ?? 0;
        int i = 0;
        long previous = -1;
        while (c < containerCount || i < ascendingDistinct.Length)
        {
            long fromExisting = c < containerCount ? existing!.ContainerBase(c) : long.MaxValue;
            long fromNew = i < ascendingDistinct.Length ? ascendingDistinct[i] & ~0xFFFFL : long.MaxValue;
            long b = Math.Min(fromExisting, fromNew);

            Array.Clear(scratch, 0, scratch.Length);
            if (fromExisting == b)
                existing!.OrContainerInto(c++, scratch);
            for (; i < ascendingDistinct.Length; i++)
            {
                long p = ascendingDistinct[i];
                if ((p & ~0xFFFFL) != b)
                    break;
                if (p <= previous)
                    throw new ArgumentException("Positions must be strictly ascending.", nameof(ascendingDistinct));
                previous = p;
                int v = (int)(p & 0xFFFF);
                ulong bit = 1UL << (v & 63);
                if ((scratch[v >> 6] & bit) == 0)
                {
                    scratch[v >> 6] |= bit;
                    newlyDeleted?.Add(p);
                }
            }

            var container = Encode(b, scratch, allowRuns);
            if (container.Cardinality > 0)
                containers.Add(container);
        }

        byte[] blob = Serialize(containers);
        return RoaringBitmapReader.Deserialize(blob);
    }

    /// <summary>The rows <paramref name="minuend"/> deletes and <paramref name="subtrahend"/> does not.</summary>
    internal static DeletionVectorBitmap Difference(
        DeletionVectorBitmap minuend, DeletionVectorBitmap subtrahend, bool allowRuns = true)
    {
        if (minuend.IsEmpty || subtrahend.IsEmpty)
            return minuend;
        var containers = new List<Container>();
        var scratch = new ulong[1024];
        var other = new ulong[1024];
        int s = 0;
        for (int c = 0; c < minuend.ContainerCount; c++)
        {
            long b = minuend.ContainerBase(c);
            Array.Clear(scratch, 0, scratch.Length);
            minuend.OrContainerInto(c, scratch);
            while (s < subtrahend.ContainerCount && subtrahend.ContainerBase(s) < b)
                s++;
            if (s < subtrahend.ContainerCount && subtrahend.ContainerBase(s) == b)
            {
                Array.Clear(other, 0, other.Length);
                subtrahend.OrContainerInto(s, other);
                for (int w = 0; w < scratch.Length; w++)
                    scratch[w] &= ~other[w];
            }
            var container = Encode(b, scratch, allowRuns);
            if (container.Cardinality > 0)
                containers.Add(container);
        }
        return RoaringBitmapReader.Deserialize(Serialize(containers));
    }

    private readonly struct Container(long @base, RoaringContainerKind kind, int cardinality, byte[] payload)
    {
        public long Base { get; } = @base;
        public RoaringContainerKind Kind { get; } = kind;
        public int Cardinality { get; } = cardinality;
        public byte[] Payload { get; } = payload;
    }

    private static Container Encode(long b, ulong[] words, bool allowRuns)
    {
        int cardinality = 0, runs = 0;
        ulong carry = 0; // the previous word's top bit, moved to bit 0
        for (int w = 0; w < words.Length; w++)
        {
            ulong x = words[w];
            cardinality += PopCount(x);
            runs += PopCount(x & ~((x << 1) | carry)); // bits that start a run
            carry = x >> 63;
        }
        if (cardinality == 0)
            return new Container(b, RoaringContainerKind.Array, 0, []);

        int arrayBytes = cardinality <= ArrayMaxCardinality ? 2 * cardinality : int.MaxValue;
        int runBytes = 2 + 4 * runs;
        if (allowRuns && runBytes < Math.Min(arrayBytes, BitmapBytes))
        {
            var payload = new byte[runBytes];
            BinaryPrimitives.WriteUInt16LittleEndian(payload, (ushort)runs);
            int at = 2, v = 0;
            while (v < 65536)
            {
                v = NextSet(words, v);
                if (v < 0)
                    break;
                int end = NextClear(words, v); // exclusive
                BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(at), (ushort)v);
                BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(at + 2), (ushort)(end - v - 1));
                at += 4;
                v = end;
            }
            return new Container(b, RoaringContainerKind.Run, cardinality, payload);
        }
        if (cardinality > ArrayMaxCardinality)
        {
            var payload = new byte[BitmapBytes];
            for (int w = 0; w < words.Length; w++)
                BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(8 * w), words[w]);
            return new Container(b, RoaringContainerKind.Bitmap, cardinality, payload);
        }
        {
            var payload = new byte[2 * cardinality];
            int at = 0;
            for (int w = 0; w < words.Length; w++)
            {
                ulong x = words[w];
                while (x != 0)
                {
                    int v = (w << 6) + System.Numerics.BitOperations.TrailingZeroCount(x);
                    BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(at), (ushort)v);
                    at += 2;
                    x &= x - 1;
                }
            }
            return new Container(b, RoaringContainerKind.Array, cardinality, payload);
        }
    }

    // First set bit at or after `from`, or -1.
    private static int NextSet(ulong[] words, int from)
    {
        int w = from >> 6;
        ulong x = words[w] & (ulong.MaxValue << (from & 63));
        while (true)
        {
            if (x != 0)
                return (w << 6) + System.Numerics.BitOperations.TrailingZeroCount(x);
            if (++w == words.Length)
                return -1;
            x = words[w];
        }
    }

    // First clear bit at or after `from`, or 65536.
    private static int NextClear(ulong[] words, int from)
    {
        int w = from >> 6;
        ulong x = ~words[w] & (ulong.MaxValue << (from & 63));
        while (true)
        {
            if (x != 0)
                return (w << 6) + System.Numerics.BitOperations.TrailingZeroCount(x);
            if (++w == words.Length)
                return words.Length << 6;
            x = ~words[w];
        }
    }

    // Magic, int64 sub-bitmap count, then per sub-bitmap: uint32 high key and a portable 32-bit bitmap.
    private static byte[] Serialize(List<Container> containers)
    {
        // Sub-bitmaps: runs of containers sharing their high 32 bits.
        var groups = new List<(uint High, int Start, int Count)>();
        for (int k = 0; k < containers.Count; k++)
        {
            uint high = (uint)(containers[k].Base >> 32);
            if (groups.Count == 0 || groups[groups.Count - 1].High != high)
                groups.Add((high, k, 0));
            var g = groups[groups.Count - 1];
            groups[groups.Count - 1] = (g.High, g.Start, g.Count + 1);
        }

        int total = 12;
        foreach (var g in groups)
            total += 4 + PortableSize(containers, g.Start, g.Count);

        var blob = new byte[total];
        BinaryPrimitives.WriteUInt32LittleEndian(blob, Magic);
        BinaryPrimitives.WriteInt64LittleEndian(blob.AsSpan(4), groups.Count);
        int at = 12;
        foreach (var g in groups)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(blob.AsSpan(at), g.High);
            at += 4;
            at = WritePortable(blob, at, containers, g.Start, g.Count);
        }
        return blob;
    }

    private static bool AnyRun(List<Container> containers, int start, int count)
    {
        for (int k = start; k < start + count; k++)
            if (containers[k].Kind == RoaringContainerKind.Run)
                return true;
        return false;
    }

    private static int HeaderSize(List<Container> containers, int start, int n)
    {
        if (AnyRun(containers, start, n))
            return 4 + (n + 7) / 8 + 4 * n + (n >= NoOffsetThreshold ? 4 * n : 0);
        return 4 + 4 + 4 * n + 4 * n;
    }

    private static int PortableSize(List<Container> containers, int start, int n)
    {
        int size = HeaderSize(containers, start, n);
        for (int k = start; k < start + n; k++)
            size += containers[k].Payload.Length;
        return size;
    }

    private static int WritePortable(byte[] blob, int at, List<Container> containers, int start, int n)
    {
        int begin = at;
        bool anyRun = AnyRun(containers, start, n);
        bool offsets;
        if (anyRun)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(blob.AsSpan(at), RunCookie | ((uint)(n - 1) << 16));
            at += 4;
            for (int k = 0; k < n; k++)
                if (containers[start + k].Kind == RoaringContainerKind.Run)
                    blob[at + (k >> 3)] |= (byte)(1 << (k & 7));
            at += (n + 7) / 8;
            offsets = n >= NoOffsetThreshold;
        }
        else
        {
            BinaryPrimitives.WriteUInt32LittleEndian(blob.AsSpan(at), NoRunCookie);
            BinaryPrimitives.WriteUInt32LittleEndian(blob.AsSpan(at + 4), (uint)n);
            at += 8;
            offsets = true;
        }

        for (int k = 0; k < n; k++)
        {
            var container = containers[start + k];
            BinaryPrimitives.WriteUInt16LittleEndian(blob.AsSpan(at), (ushort)((container.Base >> 16) & 0xFFFF));
            BinaryPrimitives.WriteUInt16LittleEndian(blob.AsSpan(at + 2), (ushort)(container.Cardinality - 1));
            at += 4;
        }

        if (offsets)
        {
            // Offsets are from the start of this portable bitmap.
            int payloadAt = HeaderSize(containers, start, n);
            for (int k = 0; k < n; k++)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(blob.AsSpan(at), (uint)payloadAt);
                at += 4;
                payloadAt += containers[start + k].Payload.Length;
            }
        }

        for (int k = 0; k < n; k++)
        {
            var payload = containers[start + k].Payload;
            Buffer.BlockCopy(payload, 0, blob, at, payload.Length);
            at += payload.Length;
        }
        System.Diagnostics.Debug.Assert(at - begin == PortableSize(containers, start, n));
        return at;
    }

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
