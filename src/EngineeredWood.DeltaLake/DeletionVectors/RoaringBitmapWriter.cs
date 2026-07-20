// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Serializes a set of row indices into the RoaringBitmapArray format
/// used by Delta Lake deletion vectors.
/// </summary>
internal static class RoaringBitmapWriter
{
    /// <summary>
    /// Magic number for the RoaringBitmapArray format.
    /// </summary>
    private const uint RoaringBitmapArrayMagic = 1681511377; // 0x6439D3D1

    /// <summary>
    /// Serializes a set of row indices into the Delta Lake DV binary format.
    /// Format: 4-byte magic + portable Roaring Bitmap serialization.
    /// </summary>
    public static byte[] Serialize(IEnumerable<long> rowIndices)
    {
        // Delta DV bitmaps are a 64-bit RoaringBitmapArray: indices are split by their high 32 bits into
        // sub-bitmaps, each a standard 32-bit portable Roaring bitmap (whose own 16-bit container keys cover the
        // low 32 bits). On-disk (and inline): magic, then int64 sub-bitmap count, then per sub-bitmap an int32
        // high-32-bit key followed by the portable bitmap — this is what delta-kernel / Spark / Fabric decode.
        var subBitmaps = new SortedDictionary<uint, SortedDictionary<ushort, List<ushort>>>();
        foreach (long idx in rowIndices)
        {
            uint high = (uint)((ulong)idx >> 32);
            uint low = (uint)((ulong)idx & 0xFFFFFFFF);
            ushort key = (ushort)(low >> 16);
            ushort value = (ushort)(low & 0xFFFF);

            if (!subBitmaps.TryGetValue(high, out var containers))
            {
                containers = new SortedDictionary<ushort, List<ushort>>();
                subBitmaps[high] = containers;
            }
            if (!containers.TryGetValue(key, out var values))
            {
                values = new List<ushort>();
                containers[key] = values;
            }
            values.Add(value);
        }

        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);

        bw.Write(RoaringBitmapArrayMagic);     // 4 bytes LE
        bw.Write((long)subBitmaps.Count);      // 8 bytes LE: number of sub-bitmaps (Portable RoaringBitmapArray)
        foreach (var kvp in subBitmaps)
        {
            bw.Write(kvp.Key);                 // 4 bytes LE: the high-32-bit key
            WritePortableRoaringBitmap(bw, kvp.Value);
        }

        bw.Flush();
        return ms.ToArray();
    }

    private static void WritePortableRoaringBitmap(
        BinaryWriter bw, SortedDictionary<ushort, List<ushort>> containers)
    {
        // Standard CRoaring "portable" NO_RUNCONTAINER format (what delta-kernel / Spark / Fabric decode):
        //   [u32 cookie = SERIAL_COOKIE_NO_RUNCONTAINER (12346)]
        //   [u32 size = container count]
        //   [descriptive header: (u16 key, u16 cardinality-1) × size]
        //   [offset header: u32 × size]   (ALWAYS present for the no-run cookie)
        //   [containers]
        const uint SerialCookieNoRunContainer = 12346;
        int containerCount = containers.Count;
        bw.Write(SerialCookieNoRunContainer); // full u32 cookie (NOT count<<16 | cookie — that's non-standard)
        bw.Write((uint)containerCount);       // size as a separate u32
        if (containerCount == 0)
        {
            return; // empty bitmap: cookie + size 0, no headers/containers
        }

        // Prepare container data
        var keys = new ushort[containerCount];
        var cardinalities = new int[containerCount];
        var sortedValues = new List<ushort>[containerCount];

        int i = 0;
        foreach (var kvp in containers)
        {
            keys[i] = kvp.Key;
            var vals = kvp.Value;
            vals.Sort();
            // Deduplicate
            var deduped = new List<ushort>(vals.Count);
            ushort prev = ushort.MaxValue;
            foreach (ushort v in vals)
            {
                if (v != prev) deduped.Add(v);
                prev = v;
            }
            sortedValues[i] = deduped;
            cardinalities[i] = deduped.Count;
            i++;
        }

        // Write descriptive header: (key, cardinality-1) pairs
        for (i = 0; i < containerCount; i++)
        {
            bw.Write(keys[i]);
            bw.Write((ushort)(cardinalities[i] - 1));
        }

        // Offset header — ALWAYS present for the NO_RUNCONTAINER cookie. headerSize = cookie(4) + size(4) +
        // descriptive(4×count) + offsets(4×count).
        {
            int headerSize = 4 + 4 + containerCount * 4 + containerCount * 4;
            int offset = headerSize;
            for (i = 0; i < containerCount; i++)
            {
                bw.Write(offset);
                if (cardinalities[i] > 4096)
                    offset += 8192; // Bitmap container: 1024 * 8 bytes
                else
                    offset += cardinalities[i] * 2; // Array container: cardinality * 2 bytes
            }
        }

        // Write container data
        for (i = 0; i < containerCount; i++)
        {
            if (cardinalities[i] > 4096)
            {
                // Bitmap container: 1024 uint64 words
                var bitmap = new ulong[1024];
                foreach (ushort v in sortedValues[i])
                    bitmap[v / 64] |= 1UL << (v % 64);

                foreach (ulong word in bitmap)
                    bw.Write(word);
            }
            else
            {
                // Array container: sorted uint16 values
                foreach (ushort v in sortedValues[i])
                    bw.Write(v);
            }
        }
    }
}
