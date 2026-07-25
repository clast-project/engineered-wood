// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using EngineeredWood.Encodings;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Detects, from the <em>encoded</em> definition and repetition level streams of a data page,
/// that a repeated column actually holds fixed-length lists with no nulls anywhere — the shape
/// produced by embeddings, coordinates, and other vector-valued columns.
/// </summary>
/// <remarks>
/// <para>
/// Parquet has no fixed-size list type, so a 768-dimensional embedding is written as a variable
/// length list and the Dremel machinery pays for that generality on every read: one definition
/// level and one repetition level per <em>element</em>, materialised, then scanned again to derive
/// list offsets. When the lists are in fact all the same length and fully defined, all of that
/// work is redundant — the offsets are <c>i * n</c> and the validity bitmaps are all-ones.
/// </para>
/// <para>
/// This detector proves those two facts without decoding either level stream. Both checks walk the
/// RLE/bit-packing hybrid <em>runs</em> rather than the values inside them:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <b>Definition levels.</b> Every level must equal <c>maxDefLevel</c>. An RLE run is checked
///     in O(1) from its run value. A bit-packed run of <c>g</c> groups is <c>g * bitWidth</c> bytes
///     that must equal a repeating <c>bitWidth</c>-byte stamp (the packing of eight copies of
///     <c>maxDefLevel</c>), which a vectorised <see cref="MemoryExtensions.SequenceEqual{T}(ReadOnlySpan{T}, ReadOnlySpan{T})"/>
///     settles in a few instructions per 64 values. Writers emit one whole-page RLE run for the
///     common all-defined case, making this O(1) in practice.
///   </description></item>
///   <item><description>
///     <b>Repetition levels.</b> The stream must be <c>0, 1×(n-1)</c> repeated. An RLE run of 1s is
///     accepted in O(1) if the next multiple of <c>n</c> lies past the end of the run. A bit-packed
///     byte is compared against the stamp implied by its offset within the record — for <c>n &lt; 8</c>
///     a small precomputed table, for <c>n ≥ 8</c> two arithmetic ops. Either way the cost is per
///     <em>byte</em>, i.e. per eight levels.
///   </description></item>
/// </list>
/// <para>
/// The list length itself is derived once per column chunk by decoding levels up to the second
/// record boundary (O(n) values, once), then verified against every page.
/// </para>
/// <para>
/// Only <c>maxRepetitionLevel == 1</c> is handled: a single list level, which is what the
/// fixed-length shape means in practice. Nested lists fall back to the general path.
/// </para>
/// </remarks>
/// <summary>
/// How <see cref="FixedListDetector.MatchesFixedPattern"/> scans the bit-packed portion of a
/// small-list (<c>length &lt; 8</c>) repetition stream. Exposed so the two strategies can be
/// benchmarked against each other; the reader uses <see cref="Scalar"/>.
/// </summary>
internal enum RepScanStrategy
{
    /// <summary>Compare each bit-packed byte against its expected stamp in a scalar loop.</summary>
    Scalar,

    /// <summary>
    /// Tile only bit-packed runs long enough to amortise building the stamp tile, scalar otherwise,
    /// and compare the tiled portion via vectorised <c>SequenceEqual</c>. On long contiguous runs
    /// (as parquet-mr / arrow emit for small lists) this is ~90× faster than <see cref="Scalar"/>;
    /// on the header-interleaved single-byte runs EngineeredWood's own encoder emits it stays scalar,
    /// so it never regresses. This is what the reader uses.
    /// </summary>
    Adaptive,
}

internal static class FixedListDetector
{
    /// <summary>
    /// Probes one page's encoded level streams.
    /// </summary>
    /// <param name="repEncoded">Encoded repetition levels (RLE/bit-packing hybrid, no length prefix).</param>
    /// <param name="defEncoded">Encoded definition levels (RLE/bit-packing hybrid, no length prefix).</param>
    /// <param name="maxDefLevel">The column's maximum definition level.</param>
    /// <param name="numValues">Number of levels in the page.</param>
    /// <param name="length">
    /// The list length. Pass 0 on the first page to have it derived; pass the previously derived
    /// length on subsequent pages to require the whole chunk to agree.
    /// </param>
    /// <param name="startIndex">
    /// Index of this page's first level within the column chunk. Writers are free to split pages by
    /// value count rather than at record boundaries, so a page may open part-way through a list;
    /// the expected pattern is a function of the chunk-global index, not the page-local one.
    /// The caller checks that the chunk's total value count is a whole multiple of the length.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if every level in the page is consistent with fully-defined lists of
    /// exactly <paramref name="length"/> elements.
    /// </returns>
    public static bool TryDetectPage(
        ReadOnlySpan<byte> repEncoded,
        ReadOnlySpan<byte> defEncoded,
        int maxDefLevel,
        int numValues,
        ref int length,
        long startIndex = 0)
    {
        if (numValues <= 0 || maxDefLevel <= 0 || maxDefLevel > 255 || startIndex < 0)
            return false;

        if (!AllLevelsAtMax(defEncoded, maxDefLevel, numValues))
            return false;

        if (length == 0)
        {
            // The length can only be derived from a page that opens a record.
            if (startIndex != 0)
                return false;

            length = DeriveLength(repEncoded, numValues);
            if (length <= 0)
                return false;
        }

        return MatchesFixedPattern(
            repEncoded, numValues, length, (int)(startIndex % length), RepScanStrategy.Adaptive);
    }

    /// <summary>
    /// Returns <see langword="true"/> when every one of <paramref name="numValues"/> encoded levels
    /// equals <paramref name="maxLevel"/> — i.e. nothing in this page is null or an empty list.
    /// </summary>
    internal static bool AllLevelsAtMax(ReadOnlySpan<byte> encoded, int maxLevel, int numValues)
    {
        int bitWidth = LevelDecoder.GetBitWidth(maxLevel);
        if (bitWidth is < 1 or > 8)
            return false;

        // The packing of eight copies of maxLevel occupies exactly bitWidth bytes; tile it eight
        // times so the comparison buffer is a multiple of 8 bytes and SequenceEqual can vectorise.
        Span<byte> tile = stackalloc byte[bitWidth * 8];
        BuildAllMaxTile(tile, maxLevel, bitWidth);

        int pos = 0;
        int idx = 0;

        while (idx < numValues)
        {
            if (!TryReadHeader(encoded, ref pos, out bool isRle, out int count))
                return false;

            if (isRle)
            {
                if (!TryReadRleValue(encoded, ref pos, bitWidth, out int value))
                    return false;
                if (value != maxLevel)
                    return false;
                idx += count;
                continue;
            }

            int runValues = count * 8;
            int byteLength = count * bitWidth;
            if (pos + byteLength > encoded.Length)
                return false;

            var block = encoded.Slice(pos, byteLength);
            pos += byteLength;

            // The final group of a stream may be padded past numValues; those padding values are
            // written as zero and must not be compared against the stamp.
            int valid = Math.Min(runValues, numValues - idx);
            int fullGroups = valid / 8;

            if (!BlockMatchesTile(block.Slice(0, fullGroups * bitWidth), tile))
                return false;

            int tailCount = valid - fullGroups * 8;
            if (tailCount > 0)
            {
                var tail = block.Slice(fullGroups * bitWidth);
                for (int j = 0; j < tailCount; j++)
                {
                    if (ReadPackedValue(tail, j, bitWidth) != maxLevel)
                        return false;
                }
            }

            idx += runValues;
        }

        return true;
    }

    /// <summary>
    /// Decodes repetition levels only as far as the second record boundary and returns the distance
    /// between them. Bounded by <paramref name="numValues"/>, and run once per column chunk.
    /// </summary>
    internal static int DeriveLength(ReadOnlySpan<byte> repEncoded, int numValues)
    {
        var decoder = new RleBitPackedDecoder(repEncoded, bitWidth: 1);

        try
        {
            // A page always starts a new record.
            if (decoder.ReadNext() != 0)
                return 0;

            for (int i = 1; i < numValues; i++)
            {
                if (decoder.ReadNext() == 0)
                    return i;
            }
        }
        catch (ParquetFormatException)
        {
            // Malformed levels: decline the fast path and let the general reader report it.
            return 0;
        }

        // The whole page is one record — a legitimate (if unusual) fixed length.
        return numValues;
    }

    /// <summary>
    /// Verifies the encoded repetition stream is exactly <c>0, 1×(length-1)</c> repeated, for
    /// <paramref name="numValues"/> levels, where the first level sits at offset
    /// <paramref name="startOffset"/> within a record.
    /// </summary>
    internal static bool MatchesFixedPattern(
        ReadOnlySpan<byte> repEncoded, int numValues, int length, int startOffset = 0,
        RepScanStrategy strategy = RepScanStrategy.Scalar)
    {
        if (length <= 0 || (uint)startOffset >= (uint)length)
            return false;

        // For n < 8 a single byte spans several record boundaries, so the expected byte is a
        // function of the offset within the record; precompute all n of them.
        Span<byte> smallStamps = stackalloc byte[8];
        if (length < 8)
        {
            for (int r = 0; r < length; r++)
            {
                int stamp = 0xFF;
                for (int j = 0; j < 8; j++)
                {
                    if ((r + j) % length == 0)
                        stamp &= ~(1 << j);
                }
                smallStamps[r] = (byte)stamp;
            }
        }

        int pos = 0;
        int idx = 0;

        // Offset of the current level within its record. Advances with idx, wrapping at `length`.
        long recordPos = startOffset;

        while (idx < numValues)
        {
            if (!TryReadHeader(repEncoded, ref pos, out bool isRle, out int count))
                return false;

            if (isRle)
            {
                if (!TryReadRleValue(repEncoded, ref pos, bitWidth: 1, out int value))
                    return false;

                int runLength = Math.Min(count, numValues - idx);

                if (value == 0)
                {
                    // Every level in the run starts a record: only single-element lists can do
                    // that for a run longer than one.
                    if (length != 1 && !(runLength == 1 && recordPos % length == 0))
                        return false;
                }
                else if (value == 1)
                {
                    // No position in [recordPos, recordPos + runLength) may be a record start.
                    long nextBoundary = ((recordPos + length - 1) / length) * length;
                    if (nextBoundary < recordPos + runLength)
                        return false;
                }
                else
                {
                    return false;
                }

                idx += count;
                recordPos = (recordPos + count) % length;
                continue;
            }

            int runValues = count * 8;
            if (pos + count > repEncoded.Length)
                return false;

            var block = repEncoded.Slice(pos, count);
            pos += count;

            int valid = Math.Min(runValues, numValues - idx);
            int fullBytes = valid / 8;
            int r0 = (int)recordPos;

            // Below this many contiguous bytes, building a 32-byte stamp tile costs more than the
            // scalar per-byte compares it replaces (measured break-even ~26 bytes). EngineeredWood's
            // encoder emits one-byte runs, so it always stays scalar; dense-run writers clear it easily.
            const int TileThresholdBytes = 32;

            bool fullOk;
            if (length < 8 && strategy == RepScanStrategy.Adaptive && fullBytes >= TileThresholdBytes)
            {
                // The expected byte depends only on the record offset at the byte's start, which
                // advances by 8 (mod length) per byte and so repeats every length/gcd(length,8)
                // bytes. Build a repeating tile from that period and let SequenceEqual compare the
                // block in vectorised chunks — worthwhile only when a run spans many contiguous
                // bytes, which happens for foreign writers that emit multi-group bit-packed runs.
                fullOk = SmallNBlockMatches(block.Slice(0, fullBytes), r0, length, smallStamps);
            }
            else
            {
                fullOk = true;
                int r = r0;
                for (int b = 0; b < fullBytes; b++)
                {
                    byte expected = length < 8 ? smallStamps[r] : ExpectedByte(r, length);
                    if (block[b] != expected) { fullOk = false; break; }
                    r += 8;
                    if (r >= length) r %= length;
                }
            }
            if (!fullOk)
                return false;

            int tailCount = valid - fullBytes * 8;
            if (tailCount > 0)
            {
                int rTail = (int)((recordPos + (long)fullBytes * 8) % length);
                byte tail = block[fullBytes];
                for (int j = 0; j < tailCount; j++)
                {
                    int bit = (tail >> j) & 1;
                    int expected = (rTail + j) % length == 0 ? 0 : 1;
                    if (bit != expected)
                        return false;
                }
            }

            idx += runValues;
            recordPos = (recordPos + runValues) % length;
        }

        return true;
    }

    /// <summary>
    /// Expected bit-packed byte for eight repetition levels starting at offset <paramref name="r"/>
    /// within a record of <paramref name="length"/> elements, for <c>length &gt;= 8</c>
    /// (so at most one record boundary can fall inside the byte).
    /// </summary>
    /// <summary>
    /// Compares a contiguous bit-packed block (one byte per eight repetition levels) against the
    /// pattern for lists of <paramref name="length"/> elements (with <c>length &lt; 8</c>), where the
    /// first byte begins at record offset <paramref name="r0"/>. Uses a tiled
    /// <see cref="MemoryExtensions.SequenceEqual{T}(ReadOnlySpan{T}, ReadOnlySpan{T})"/> so the runtime
    /// can vectorise the comparison.
    /// </summary>
    private static bool SmallNBlockMatches(
        ReadOnlySpan<byte> block, int r0, int length, ReadOnlySpan<byte> smallStamps)
    {
        if (block.IsEmpty)
            return true;

        // The pattern of expected bytes repeats every `period` bytes; widen to a tile of at least
        // 32 bytes (a whole number of periods) so SequenceEqual works on vector-sized chunks.
        int period = length / Gcd(length, 8);
        int tileLen = period;
        while (tileLen < 32)
            tileLen += period;

        Span<byte> tile = stackalloc byte[40]; // period <= 7 → tileLen in [32, 38]
        int r = r0;
        for (int i = 0; i < tileLen; i++)
        {
            tile[i] = smallStamps[r];
            r += 8;
            if (r >= length) r %= length;
        }

        return BlockMatchesTile(block, tile.Slice(0, tileLen));
    }

    private static int Gcd(int a, int b)
    {
        while (b != 0)
            (a, b) = (b, a % b);
        return a;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte ExpectedByte(int r, int length)
    {
        if (r == 0)
            return 0xFE; // boundary at bit 0

        int zeroBit = length - r;
        return zeroBit < 8 ? (byte)(0xFF ^ (1 << zeroBit)) : (byte)0xFF;
    }

    private static void BuildAllMaxTile(Span<byte> tile, int maxLevel, int bitWidth)
    {
        // Eight values of bitWidth bits each, LSB-first — at most 64 bits.
        ulong bits = 0;
        for (int j = 0; j < 8; j++)
            bits |= (ulong)(uint)maxLevel << (j * bitWidth);

        for (int b = 0; b < bitWidth; b++)
            tile[b] = (byte)(bits >> (b * 8));

        for (int copy = 1; copy < 8; copy++)
            tile.Slice(0, bitWidth).CopyTo(tile.Slice(copy * bitWidth, bitWidth));
    }

    private static bool BlockMatchesTile(ReadOnlySpan<byte> block, ReadOnlySpan<byte> tile)
    {
        int offset = 0;
        while (block.Length - offset >= tile.Length)
        {
            if (!block.Slice(offset, tile.Length).SequenceEqual(tile))
                return false;
            offset += tile.Length;
        }

        int remainder = block.Length - offset;
        return remainder == 0 || block.Slice(offset).SequenceEqual(tile.Slice(0, remainder));
    }

    private static int ReadPackedValue(ReadOnlySpan<byte> data, int index, int bitWidth)
    {
        int bitPos = index * bitWidth;
        int byteIdx = bitPos >> 3;
        int bitIdx = bitPos & 7;

        uint raw = 0;
        for (int i = 0; i < 3 && byteIdx + i < data.Length; i++)
            raw |= (uint)data[byteIdx + i] << (i * 8);

        return (int)((raw >> bitIdx) & ((1u << bitWidth) - 1));
    }

    private static bool TryReadHeader(ReadOnlySpan<byte> data, ref int pos, out bool isRle, out int count)
    {
        isRle = false;
        count = 0;

        if (pos >= data.Length)
            return false;

        long header;
        try
        {
            header = (long)Varint.ReadUnsigned(data, ref pos);
        }
        catch
        {
            return false;
        }

        isRle = (header & 1) == 0;
        long value = header >> 1;
        if (value <= 0 || value > int.MaxValue)
            return false;

        count = (int)value;
        return true;
    }

    private static bool TryReadRleValue(ReadOnlySpan<byte> data, ref int pos, int bitWidth, out int value)
    {
        value = 0;
        int byteWidth = (bitWidth + 7) / 8;
        if (pos + byteWidth > data.Length)
            return false;

        for (int i = 0; i < byteWidth; i++)
            value |= data[pos++] << (i * 8);

        return true;
    }
}
