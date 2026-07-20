// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes definition and repetition levels from Parquet data pages.
/// </summary>
internal static class LevelDecoder
{
    /// <summary>
    /// Decodes levels from a V1 data page. The format is a 4-byte little-endian length
    /// prefix followed by encoded level data.
    /// </summary>
    /// <param name="data">Raw page data starting at the level section.</param>
    /// <param name="maxLevel">Maximum level value for this column.</param>
    /// <param name="valueCount">Number of values to decode.</param>
    /// <param name="levels">Destination span for decoded levels.</param>
    /// <param name="matchCount">
    /// Output: number of decoded levels equal to <paramref name="maxLevel"/>.
    /// Equals <paramref name="valueCount"/> when <paramref name="maxLevel"/> is 0 (non-nullable column).
    /// </param>
    /// <param name="encoding">
    /// Level encoding from the page header. <see cref="Encoding.Rle"/> uses
    /// RLE/Bit-Packing Hybrid; <see cref="Encoding.BitPacked"/> uses the
    /// deprecated pure bit-packed format.
    /// </param>
    /// <returns>The number of bytes consumed (including the 4-byte length prefix).</returns>
    public static int DecodeV1(
        ReadOnlySpan<byte> data,
        int maxLevel,
        int valueCount,
        Span<byte> levels,
        out int matchCount,
        Encoding encoding = Encoding.Rle)
    {
        if (maxLevel == 0)
        {
            levels.Slice(0, valueCount).Clear();
            matchCount = valueCount;
            return 0;
        }

        if (encoding == Encoding.BitPacked)
            return DecodeBitPacked(data, maxLevel, valueCount, levels, out matchCount);

        if (data.Length < 4)
            throw new ParquetFormatException("Not enough data for V1 level length prefix.");

        int encodedLength = BinaryPrimitives.ReadInt32LittleEndian(data);
        if (encodedLength < 0 || 4 + encodedLength > data.Length)
            throw new ParquetFormatException(
                $"Invalid V1 level encoded length: {encodedLength}.");

        int bitWidth = GetBitWidth(maxLevel);
        var rleData = data.Slice(4, encodedLength);
        var decoder = new RleBitPackedDecoder(rleData, bitWidth);
        decoder.ReadBatch(levels.Slice(0, valueCount), maxLevel, out matchCount);

        return 4 + encodedLength;
    }

    /// <summary>
    /// Decodes levels from a V2 data page. The format is raw RLE/bit-packed data
    /// with no length prefix (the byte length is provided in the page header).
    /// </summary>
    /// <param name="matchCount">
    /// Output: number of decoded levels equal to <paramref name="maxLevel"/>.
    /// </param>
    public static void DecodeV2(
        ReadOnlySpan<byte> data,
        int maxLevel,
        int valueCount,
        Span<byte> levels,
        out int matchCount)
    {
        if (maxLevel == 0)
        {
            levels.Slice(0, valueCount).Clear();
            matchCount = valueCount;
            return;
        }

        int bitWidth = GetBitWidth(maxLevel);
        var decoder = new RleBitPackedDecoder(data, bitWidth);
        decoder.ReadBatch(levels.Slice(0, valueCount), maxLevel, out matchCount);
    }

    /// <summary>
    /// Returns the minimum number of bits needed to represent values 0..maxLevel.
    /// </summary>
    internal static int GetBitWidth(int maxLevel)
    {
        if (maxLevel == 0) return 0;
#if NET8_0_OR_GREATER
        return 32 - int.LeadingZeroCount(maxLevel);
#else
        return 32 - BitPolyfills.LeadingZeroCount((uint)maxLevel);
#endif
    }

    /// <summary>
    /// Decodes the deprecated BIT_PACKED encoding (encoding 4) for V1 levels.
    /// Values are packed contiguously using MSB bit ordering (most significant bit first)
    /// with no length prefix — the byte length is deterministic: <c>ceil(valueCount * bitWidth / 8)</c>.
    /// </summary>
    private static int DecodeBitPacked(
        ReadOnlySpan<byte> data,
        int maxLevel,
        int valueCount,
        Span<byte> levels,
        out int matchCount)
    {
        int bitWidth = GetBitWidth(maxLevel);
        int byteLength = (valueCount * bitWidth + 7) / 8;

        if (data.Length < byteLength)
            throw new ParquetFormatException(
                $"Not enough data for BitPacked levels: need {byteLength} bytes, have {data.Length}.");

        var packed = data.Slice(0, byteLength);
        int mask = (1 << bitWidth) - 1;
        matchCount = 0;

        for (int i = 0; i < valueCount; i++)
        {
            int globalBitPos = i * bitWidth;
            int byteIdx = globalBitPos >> 3;
            int bitIdx = globalBitPos & 7;

            // Read up to 4 bytes big-endian for MSB-packed extraction
            int remaining = packed.Length - byteIdx;
            uint raw = remaining >= 4
                ? BinaryPrimitives.ReadUInt32BigEndian(packed.Slice(byteIdx))
                : AssemblePartialBigEndian(packed, byteIdx, remaining);

            int shift = 32 - bitIdx - bitWidth;
            byte val = (byte)((raw >> shift) & (uint)mask);
            levels[i] = val;
            if (val == (byte)maxLevel) matchCount++;
        }

        return byteLength;
    }

    private static uint AssemblePartialBigEndian(ReadOnlySpan<byte> data, int byteIndex, int remaining)
    {
        uint raw = 0;
        for (int i = 0; i < remaining; i++)
            raw |= (uint)data[byteIndex + i] << (24 - i * 8);
        return raw;
    }
}
