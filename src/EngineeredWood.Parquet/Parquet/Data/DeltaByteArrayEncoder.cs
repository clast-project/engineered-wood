// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY values using the DELTA_BYTE_ARRAY encoding.
/// Mirror of <see cref="DeltaByteArrayDecoder"/>.
/// </summary>
/// <remarks>
/// Format:
/// <list type="number">
/// <item>Prefix lengths: encoded as DELTA_BINARY_PACKED (INT32)</item>
/// <item>Suffixes: encoded as DELTA_LENGTH_BYTE_ARRAY (suffix lengths as DELTA_BINARY_PACKED + raw suffix bytes)</item>
/// </list>
/// Each value shares a common prefix with the previous value. This encoding is most
/// effective for sorted or prefix-heavy string data (e.g., URLs, file paths, keys).
/// </remarks>
internal static class DeltaByteArrayEncoder
{
    /// <summary>
    /// Encodes byte array values into the provided destination buffer.
    /// Returns the number of bytes written.
    /// </summary>
    /// <param name="arrowOffsets">Arrow-style offset array into <paramref name="arrowData"/>.</param>
    /// <param name="arrowData">Raw byte data buffer from the Arrow array.</param>
    /// <param name="offset">Start index in the Arrow array.</param>
    /// <param name="numValues">Number of values to consider (including nulls).</param>
    /// <param name="nonNullCount">Number of non-null values.</param>
    /// <param name="defLevels">Definition levels (null for required columns).</param>
    /// <param name="dest">Destination buffer (must be large enough).</param>
    /// <returns>Number of bytes written to <paramref name="dest"/>.</returns>
    public static int Encode(
        ReadOnlySpan<int> arrowOffsets, ReadOnlySpan<byte> arrowData,
        int offset, int numValues, int nonNullCount, int[]? defLevels,
        byte[] dest)
    {
        if (nonNullCount == 0)
        {
            // Empty: two delta headers (0 values each)
            var emptyEncoder = new DeltaBinaryPackedEncoder(32);
            emptyEncoder.EncodeInt32s(ReadOnlySpan<int>.Empty);
            int emptyLen = emptyEncoder.Length;
            emptyEncoder.WrittenSpan.CopyTo(dest);
            emptyEncoder.WrittenSpan.CopyTo(dest.AsSpan(emptyLen)); // suffix lengths
            return emptyLen * 2;
        }

        // Pass 1: compute prefix lengths and suffix metadata
        var prefixLengths = new int[nonNullCount];
        var suffixLengths = new int[nonNullCount];
        int totalSuffixBytes = 0;

        int prevStart = -1, prevLen = 0;
        int idx = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;

            int valStart = arrowOffsets[offset + i];
            int valLen = arrowOffsets[offset + i + 1] - valStart;

            if (idx == 0)
            {
                prefixLengths[0] = 0;
                suffixLengths[0] = valLen;
            }
            else
            {
                // Compute common prefix with previous value
                int commonLen = CommonPrefixLength(
                    arrowData.Slice(prevStart, prevLen),
                    arrowData.Slice(valStart, valLen));
                prefixLengths[idx] = commonLen;
                suffixLengths[idx] = valLen - commonLen;
            }

            totalSuffixBytes += suffixLengths[idx];
            prevStart = valStart;
            prevLen = valLen;
            idx++;
        }

        // Pass 2: encode prefix lengths as DELTA_BINARY_PACKED
        var prefixEncoder = new DeltaBinaryPackedEncoder();
        prefixEncoder.EncodeInt32s(prefixLengths);

        // Pass 3: encode suffix lengths as DELTA_BINARY_PACKED
        var suffixLenEncoder = new DeltaBinaryPackedEncoder();
        suffixLenEncoder.EncodeInt32s(suffixLengths);

        // Pass 4: assemble output = prefix_lengths_delta + suffix_lengths_delta + suffix_data
        int pos = 0;
        prefixEncoder.WrittenSpan.CopyTo(dest.AsSpan(pos));
        pos += prefixEncoder.Length;

        suffixLenEncoder.WrittenSpan.CopyTo(dest.AsSpan(pos));
        pos += suffixLenEncoder.Length;

        // Copy suffix bytes
        idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;

            int valStart = arrowOffsets[offset + i];
            int valLen = arrowOffsets[offset + i + 1] - valStart;
            int suffixStart = valStart + prefixLengths[idx];
            int suffixLen = suffixLengths[idx];

            if (suffixLen > 0)
            {
                arrowData.Slice(suffixStart, suffixLen).CopyTo(dest.AsSpan(pos));
                pos += suffixLen;
            }
            idx++;
        }

        return pos;
    }

    /// <summary>
    /// Encodes fixed-length byte array values into the provided destination buffer.
    /// Returns the number of bytes written.
    /// </summary>
    public static int EncodeFixed(
        ReadOnlySpan<byte> valueBuffer, int typeLength,
        int offset, int numValues, int nonNullCount, int[]? defLevels,
        byte[] dest)
    {
        if (nonNullCount == 0)
        {
            var emptyEncoder = new DeltaBinaryPackedEncoder(32);
            emptyEncoder.EncodeInt32s(ReadOnlySpan<int>.Empty);
            int emptyLen = emptyEncoder.Length;
            emptyEncoder.WrittenSpan.CopyTo(dest);
            emptyEncoder.WrittenSpan.CopyTo(dest.AsSpan(emptyLen));
            return emptyLen * 2;
        }

        var prefixLengths = new int[nonNullCount];
        var suffixLengths = new int[nonNullCount];
        int totalSuffixBytes = 0;

        int prevIdx = -1;
        int idx = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;

            int valIdx = offset + i;

            if (idx == 0)
            {
                prefixLengths[0] = 0;
                suffixLengths[0] = typeLength;
            }
            else
            {
                int commonLen = CommonPrefixLength(
                    valueBuffer.Slice(prevIdx * typeLength, typeLength),
                    valueBuffer.Slice(valIdx * typeLength, typeLength));
                prefixLengths[idx] = commonLen;
                suffixLengths[idx] = typeLength - commonLen;
            }

            totalSuffixBytes += suffixLengths[idx];
            prevIdx = valIdx;
            idx++;
        }

        var prefixEncoder = new DeltaBinaryPackedEncoder();
        prefixEncoder.EncodeInt32s(prefixLengths);

        var suffixLenEncoder = new DeltaBinaryPackedEncoder();
        suffixLenEncoder.EncodeInt32s(suffixLengths);

        int pos = 0;
        prefixEncoder.WrittenSpan.CopyTo(dest.AsSpan(pos));
        pos += prefixEncoder.Length;

        suffixLenEncoder.WrittenSpan.CopyTo(dest.AsSpan(pos));
        pos += suffixLenEncoder.Length;

        idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;

            int valIdx = offset + i;
            int suffixStart = valIdx * typeLength + prefixLengths[idx];
            int suffixLen = suffixLengths[idx];

            if (suffixLen > 0)
            {
                valueBuffer.Slice(suffixStart, suffixLen).CopyTo(dest.AsSpan(pos));
                pos += suffixLen;
            }
            idx++;
        }

        return pos;
    }

    /// <summary>
    /// Estimates the maximum encoded size for budget/buffer sizing.
    /// Worst case: no prefix sharing, so suffix data == original data.
    /// Delta headers are bounded by ~10 bytes per 128 values.
    /// </summary>
    public static int EstimateMaxSize(int nonNullCount, int totalDataBytes)
    {
        // Two DELTA_BINARY_PACKED headers (prefix lengths + suffix lengths):
        // header = 4 varints (~40 bytes) + per-block overhead
        // Conservative: 20 bytes per 128 values for each delta block
        int deltaOverhead = 2 * (40 + (nonNullCount / 128 + 1) * 20);
        return deltaOverhead + totalDataBytes;
    }

    private static int CommonPrefixLength(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int maxLen = Math.Min(a.Length, b.Length);
        int i = 0;
        while (i < maxLen && a[i] == b[i])
            i++;
        return i;
    }
}
