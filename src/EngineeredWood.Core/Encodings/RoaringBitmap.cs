// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Encodings;

/// <summary>
/// Reader for the "portable" Roaring Bitmap serialization format.
/// See https://github.com/RoaringBitmap/RoaringFormatSpec for the spec.
/// Used by Lance's deletion-vector BITMAP files and (with a 4-byte
/// magic prefix) by Delta Lake's deletion vectors.
///
/// <para>The spec is genuinely ambiguous about the no-run cookie when
/// the high 16 bits are zero (i.e., container_count = 1):</para>
/// <list type="bullet">
/// <item><b>Spec / Java / our DeltaLake writer</b>: the cookie's high
///   16 bits encode <c>container_count - 1</c>; for one container the
///   cookie is exactly <c>0x0000_303A</c> and the descriptor follows
///   immediately. Offset header is only present when N &gt;= 4.</item>
/// <item><b>CRoaring / Lance / pylance</b>: when the cookie is exactly
///   <c>0x0000_303A</c>, the container count lives in the next u32 and
///   the offset header is always present.</item>
/// </list>
///
/// <para>The reader takes a <see cref="RoaringFormat"/> flag so callers
/// can pick. <see cref="ParseContainers"/> is the one parser: it validates the layout and
/// locates every container's payload without expanding it, for a caller that reads containers
/// in place (Delta's deletion-vector view). <see cref="DeserializePortable"/> expands the same
/// containers into values through a callback.</para>
/// </summary>
internal static class RoaringBitmap
{
    private const ushort NoRunCookie = 12346;
    private const ushort RunCookie = 12347;

    // The spec's NO_OFFSET_THRESHOLD: below this many containers, a bitmap that is not using the CRoaring
    // no-run variant carries no offset header.
    private const int NoOffsetThreshold = 4;

    public enum RoaringFormat
    {
        /// <summary>
        /// Java / spec interpretation: container count is in the cookie's
        /// high 16 bits; offset header is present only when N &gt;= 4.
        /// Used by Delta Lake.
        /// </summary>
        SpecCompliant = 0,

        /// <summary>
        /// CRoaring / pylance / Lance variant: when cookie equals exactly
        /// <c>0x0000_303A</c>, the container count is in the following
        /// u32, and the offset header is always present.
        /// </summary>
        CRoaring = 1,
    }

    /// <summary>
    /// Deserialize a portable Roaring bitmap. Each set bit is delivered
    /// to <paramref name="emit"/> as a <c>long</c> value (the high 16
    /// bits come from the container key; in the typical 32-bit-bitmap
    /// case the values fit in a uint), in ascending order. Returns the bytes consumed, or 0 when
    /// <paramref name="data"/> is too short to hold a cookie.
    /// </summary>
    /// <exception cref="InvalidDataException">The bitmap is truncated or malformed.</exception>
    public static int DeserializePortable(
        ReadOnlySpan<byte> data, Action<long> emit,
        RoaringFormat format = RoaringFormat.SpecCompliant)
    {
        if (data.Length < 4) return 0;

        var containers = new List<RoaringContainer>();
        int consumed = ParseContainers(data, format, containers);
        foreach (var c in containers)
        {
            long highBits = (long)c.Key << 16;
            int pos = c.Offset;
            switch (c.Kind)
            {
                case RoaringContainerKind.Run:
                    for (int r = 0; r < c.Length; r++, pos += 4)
                    {
                        int start = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
                        int last = start + BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos + 2, 2));
                        for (int v = start; v <= last; v++)
                            emit(highBits | (uint)v);
                    }
                    break;
                case RoaringContainerKind.Bitmap:
                    for (int word = 0; word < 1024; word++, pos += 8)
                    {
                        ulong bits = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos, 8));
                        while (bits != 0)
                        {
                            int bit = System.Numerics.BitOperations.TrailingZeroCount(bits);
                            emit(highBits | (uint)(word * 64 + bit));
                            bits &= bits - 1;
                        }
                    }
                    break;
                default:
                    for (int j = 0; j < c.Length; j++, pos += 2)
                        emit(highBits | BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2)));
                    break;
            }
        }

        return consumed; // bytes consumed (lets a RoaringBitmapArray reader advance across sub-bitmaps)
    }

    /// <summary>
    /// Validates one portable bitmap at the start of <paramref name="data"/> and appends a descriptor for each
    /// of its containers to <paramref name="containers"/>, in ascending key order. Payload offsets are relative
    /// to the start of <paramref name="data"/>. Returns the bytes the bitmap occupies.
    /// </summary>
    /// <remarks>
    /// Everything a reader of the payloads relies on is checked here, so it can read them unchecked: every
    /// payload lies inside <paramref name="data"/>, keys strictly ascend, array values strictly ascend, runs
    /// ascend without overlapping and stay inside their container. (Touching runs, which CRoaring would have
    /// merged, are accepted: they name no row twice, and Java's RoaringBitmap reads them too.) Each container's
    /// <see cref="RoaringContainer.Cardinality"/> is counted from its payload and must equal the descriptive
    /// header's: a reader that trusts the header (Java's RoaringBitmap takes a bitmap container's count from it)
    /// would otherwise see a different set size than one that counts, and the bytes, written back unchanged,
    /// would carry the disagreement on. CRoaring's own validation refuses the same mismatch.
    /// </remarks>
    /// <exception cref="InvalidDataException">The bitmap is truncated or malformed.</exception>
    public static int ParseContainers(
        ReadOnlySpan<byte> data, RoaringFormat format, List<RoaringContainer> containers)
    {
        int pos = 0;
        uint cookie = ReadU32(data, ref pos);

        long containerCount;
        bool hasOffsetHeader;
        int runBitmapAt = -1;

        if (cookie == NoRunCookie && format == RoaringFormat.CRoaring)
        {
            // CRoaring no-run variant: cookie equals exactly 12346 (high 16 bits = 0).
            // Container count is in the NEXT u32, and the offset header is
            // always present.
            containerCount = ReadU32(data, ref pos);
            hasOffsetHeader = true;
        }
        else if ((cookie & 0xFFFF) == NoRunCookie)
        {
            // Spec interpretation: high 16 bits hold (container_count - 1).
            // Offset header only when N >= 4.
            containerCount = (cookie >> 16) + 1;
            hasOffsetHeader = containerCount >= NoOffsetThreshold;
        }
        else if ((cookie & 0xFFFF) == RunCookie)
        {
            containerCount = (cookie >> 16) + 1;
            // The spec (and CRoaring / Java, which agree here) writes the offset header after the run cookie
            // once there are NO_OFFSET_THRESHOLD (4) or more containers. Skipping it misreads every container.
            hasOffsetHeader = containerCount >= NoOffsetThreshold;
            runBitmapAt = pos;
            Skip(data, ref pos, (containerCount + 7) / 8);
        }
        else
        {
            // Legacy serial cookie: cookie itself is the u32 container count.
            containerCount = cookie;
            hasOffsetHeader = false;
        }

        // A bitmap of 32-bit values has at most 65,536 containers.
        if (containerCount > 65536)
            throw Malformed($"{containerCount} containers (at most 65,536)");
        int n = (int)containerCount;
        if (n == 0) return pos;

        int descriptors = pos;
        Skip(data, ref pos, 4L * n);
        if (hasOffsetHeader)
            Skip(data, ref pos, 4L * n);

        int previousKey = -1;
        for (int i = 0; i < n; i++)
        {
            ushort key = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(descriptors + 4 * i, 2));
            int headerCardinality = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(descriptors + 4 * i + 2, 2)) + 1;
            if (key <= previousKey)
                throw Malformed($"container keys out of order ({key} after {previousKey})");
            previousKey = key;

            bool isRun = runBitmapAt >= 0 && (data[runBitmapAt + (i >> 3)] & (1 << (i & 7))) != 0;
            if (isRun)
            {
                int runs = ReadU16(data, ref pos);
                int payload = pos;
                Skip(data, ref pos, 4L * runs);
                int cardinality = 0, nextFree = 0;
                for (int r = 0; r < runs; r++)
                {
                    int start = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(payload + 4 * r, 2));
                    int lengthMinus1 = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(payload + 4 * r + 2, 2));
                    if (start < nextFree || start + lengthMinus1 > 65535)
                        throw Malformed($"run {r} of container {key} overlaps, is out of order or overflows");
                    nextFree = start + lengthMinus1 + 1; // a following run starts past this one's last value
                    cardinality += lengthMinus1 + 1;
                }
                if (cardinality != headerCardinality)
                    throw Malformed(
                        $"run container {key} holds {cardinality} values; its header says {headerCardinality}");
                containers.Add(new RoaringContainer(key, RoaringContainerKind.Run, payload, runs, cardinality));
            }
            else if (headerCardinality > 4096)
            {
                // Bitmap container: 1024 × u64 = 8 KB.
                int payload = pos;
                Skip(data, ref pos, 8192);
                int cardinality = 0;
                for (int w = 0; w < 1024; w++)
                    cardinality += PopCount(BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(payload + 8 * w, 8)));
                if (cardinality != headerCardinality)
                    throw Malformed(
                        $"bitmap container {key} holds {cardinality} values; its header says {headerCardinality}");
                containers.Add(new RoaringContainer(key, RoaringContainerKind.Bitmap, payload, 1024, cardinality));
            }
            else
            {
                // Array container: sorted u16 values.
                int payload = pos;
                Skip(data, ref pos, 2L * headerCardinality);
                int previous = -1;
                for (int j = 0; j < headerCardinality; j++)
                {
                    int value = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(payload + 2 * j, 2));
                    if (value <= previous)
                        throw Malformed($"array container {key} is not strictly ascending");
                    previous = value;
                }
                containers.Add(new RoaringContainer(
                    key, RoaringContainerKind.Array, payload, headerCardinality, headerCardinality));
            }
        }

        return pos;
    }

    private static uint ReadU32(ReadOnlySpan<byte> data, ref int pos)
    {
        if (data.Length - pos < 4)
            throw Malformed("truncated");
        uint value = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(pos, 4));
        pos += 4;
        return value;
    }

    private static int ReadU16(ReadOnlySpan<byte> data, ref int pos)
    {
        if (data.Length - pos < 2)
            throw Malformed("truncated");
        int value = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
        pos += 2;
        return value;
    }

    private static void Skip(ReadOnlySpan<byte> data, ref int pos, long bytes)
    {
        if (data.Length - pos < bytes)
            throw Malformed("truncated");
        pos += (int)bytes;
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

    private static InvalidDataException Malformed(string detail) =>
        new($"Malformed roaring bitmap: {detail}.");
}

/// <summary>How a roaring container stores its values.</summary>
internal enum RoaringContainerKind : byte
{
    /// <summary>Sorted <c>u16</c> values.</summary>
    Array,

    /// <summary>1,024 little-endian <c>u64</c> words, one bit per value.</summary>
    Bitmap,

    /// <summary>Sorted (<c>u16</c> start, <c>u16</c> length - 1) pairs.</summary>
    Run,
}

/// <summary>
/// One container of a parsed portable roaring bitmap: its key (the high 16 bits of its values), how it is
/// stored, where its payload starts, and the payload's element count — values for an array, 1,024 words for a
/// bitmap, runs for a run container (whose payload starts after the run count).
/// </summary>
internal readonly struct RoaringContainer(
    ushort key, RoaringContainerKind kind, int offset, int length, int cardinality)
{
    public ushort Key { get; } = key;

    public RoaringContainerKind Kind { get; } = kind;

    public int Offset { get; } = offset;

    public int Length { get; } = length;

    /// <summary>How many values the container holds, counted from its payload.</summary>
    public int Cardinality { get; } = cardinality;
}
