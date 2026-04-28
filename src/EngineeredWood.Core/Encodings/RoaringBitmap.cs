// Copyright (c) Curt Hagenlocher. All rights reserved.
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
/// can pick. Values are emitted to a caller-supplied callback so the
/// caller picks its own destination container (HashSet, BitArray,
/// sorted list, etc.) without an intermediate allocation.</para>
/// </summary>
internal static class RoaringBitmap
{
    private const ushort NoRunCookie = 12346;
    private const ushort RunCookie = 12347;

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
    /// case the values fit in a uint).
    /// </summary>
    public static void DeserializePortable(
        ReadOnlySpan<byte> data, Action<long> emit,
        RoaringFormat format = RoaringFormat.SpecCompliant)
    {
        if (data.Length < 4) return;

        int pos = 0;
        uint cookie = BinaryPrimitives.ReadUInt32LittleEndian(data);
        pos += 4;

        int containerCount;
        bool hasRunContainers;
        bool hasOffsetHeader;
        byte[]? runBitmap = null;

        if (cookie == NoRunCookie && format == RoaringFormat.CRoaring)
        {
            // CRoaring no-run variant: cookie equals exactly 12346 (high 16 bits = 0).
            // Container count is in the NEXT u32, and the offset header is
            // always present.
            containerCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(pos, 4));
            pos += 4;
            hasRunContainers = false;
            hasOffsetHeader = true;
        }
        else if ((cookie & 0xFFFF) == NoRunCookie)
        {
            // Spec interpretation: high 16 bits hold (container_count - 1).
            // Offset header only when N >= 4.
            containerCount = (int)((cookie >> 16) + 1);
            hasRunContainers = false;
            hasOffsetHeader = containerCount >= 4;
        }
        else if ((cookie & 0xFFFF) == RunCookie)
        {
            containerCount = (int)((cookie >> 16) + 1);
            hasRunContainers = true;
            hasOffsetHeader = false;
            int runBitmapBytes = (containerCount + 7) / 8;
            runBitmap = data.Slice(pos, runBitmapBytes).ToArray();
            pos += runBitmapBytes;
        }
        else
        {
            // Legacy serial cookie: cookie itself is the u32 container count.
            containerCount = (int)cookie;
            hasRunContainers = false;
            hasOffsetHeader = false;
        }

        if (containerCount == 0) return;

        // Descriptive header: (key, cardinality-1) pairs.
        var keys = new ushort[containerCount];
        var cardinalities = new int[containerCount];
        for (int i = 0; i < containerCount; i++)
        {
            keys[i] = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
            pos += 2;
            cardinalities[i] = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2)) + 1;
            pos += 2;
        }

        if (hasOffsetHeader)
            pos += containerCount * 4;

        for (int i = 0; i < containerCount; i++)
        {
            long highBits = (long)keys[i] << 16;
            bool isRun = hasRunContainers
                && runBitmap is not null
                && (runBitmap[i / 8] & (1 << (i % 8))) != 0;

            if (isRun)
            {
                int numRuns = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
                pos += 2;
                for (int r = 0; r < numRuns; r++)
                {
                    ushort start = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
                    pos += 2;
                    ushort length = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
                    pos += 2;
                    for (int v = start; v <= start + length; v++)
                        emit(highBits | (uint)v);
                }
            }
            else if (cardinalities[i] > 4096)
            {
                // Bitmap container: 1024 × u64 = 8 KB.
                for (int word = 0; word < 1024; word++)
                {
                    ulong bits = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos, 8));
                    pos += 8;
                    while (bits != 0)
                    {
                        int bit = System.Numerics.BitOperations.TrailingZeroCount(bits);
                        emit(highBits | (uint)(word * 64 + bit));
                        bits &= bits - 1;
                    }
                }
            }
            else
            {
                // Array container: sorted u16 values.
                for (int j = 0; j < cardinalities[i]; j++)
                {
                    ushort value = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
                    pos += 2;
                    emit(highBits | value);
                }
            }
        }
    }
}
