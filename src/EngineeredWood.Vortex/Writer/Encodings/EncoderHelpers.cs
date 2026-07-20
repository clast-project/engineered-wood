// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Shared helpers for the writer encoders. Currently a single bit-aligned
/// validity-bitmap copy used when the input array has <c>Data.Offset != 0</c>
/// (Apache.Arrow stores validity as a bitmap with a bit-level offset, so a
/// byte copy isn't sufficient when slicing).
/// </summary>
internal static class EncoderHelpers
{
    /// <summary>
    /// Copies <paramref name="rowCount"/> validity bits starting at bit
    /// <paramref name="srcBitOffset"/> in <paramref name="srcBitmap"/> into a
    /// freshly-allocated byte-aligned bitmap (LSB-first per byte). Returns null
    /// if <paramref name="rowCount"/> is zero.
    /// </summary>
    public static byte[] ExtractValidityBitmap(
        ReadOnlySpan<byte> srcBitmap, int srcBitOffset, int rowCount)
    {
        int dstBytes = (rowCount + 7) / 8;
        var dst = new byte[dstBytes];
        if (rowCount == 0) return dst;

        int requiredSrcBytes = (srcBitOffset + rowCount + 7) / 8;
        if (srcBitmap.Length < requiredSrcBytes)
            throw new InvalidOperationException(
                $"Source validity bitmap is {srcBitmap.Length} bytes; need at least {requiredSrcBytes} for offset+rowCount bits.");

        // Byte-aligned fast path.
        if ((srcBitOffset & 7) == 0)
        {
            srcBitmap.Slice(srcBitOffset >> 3, dstBytes).CopyTo(dst);
            // Mask off any garbage trailing bits in the last byte.
            int trailing = rowCount & 7;
            if (trailing != 0)
                dst[dstBytes - 1] &= (byte)((1 << trailing) - 1);
            return dst;
        }

        // Bit-level copy.
        for (int i = 0; i < rowCount; i++)
        {
            int srcBit = srcBitOffset + i;
            if ((srcBitmap[srcBit >> 3] & (1 << (srcBit & 7))) != 0)
                dst[i >> 3] |= (byte)(1 << (i & 7));
        }
        return dst;
    }
}
