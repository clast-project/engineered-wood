// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes DELTA_BYTE_ARRAY encoded values for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns.
/// </summary>
/// <remarks>
/// Format:
/// <list type="number">
/// <item>Prefix lengths: encoded as DELTA_BINARY_PACKED (INT32)</item>
/// <item>Suffixes: encoded as DELTA_LENGTH_BYTE_ARRAY (suffix lengths as DELTA_BINARY_PACKED + raw bytes)</item>
/// </list>
/// Each value is reconstructed as: previous_value[0..prefix_length] + suffix.
/// </remarks>
internal static class DeltaByteArrayDecoder
{
    /// <summary>
    /// Decodes <paramref name="count"/> byte array values and appends them to <paramref name="state"/>.
    /// </summary>
    /// <param name="typeLength">
    /// Fixed value width for a FIXED_LEN_BYTE_ARRAY column, or 0 for BYTE_ARRAY. The two physical types
    /// share this encoding but not their destination buffers, and the state only allocates the pair the
    /// column's physical type calls for -- so the width has to reach here rather than be inferred.
    /// </param>
    public static void Decode(ReadOnlySpan<byte> data, int count, ColumnBuildState state, int typeLength = 0)
    {
        bool fixedWidth = state.PhysicalType == PhysicalType.FixedLenByteArray;
        if (fixedWidth && typeLength <= 0)
            throw new ParquetFormatException(
                "A FIXED_LEN_BYTE_ARRAY column decoded as DELTA_BYTE_ARRAY has no type_length.");

        // Step 1: Decode prefix lengths
        var prefixDecoder = new DeltaBinaryPackedDecoder(data);
        var prefixLengths = new int[count];
        prefixDecoder.DecodeInt32s(prefixLengths);

        // Step 2: Decode suffix lengths (another DELTA_BINARY_PACKED block)
        var suffixData = data.Slice(prefixDecoder.BytesConsumed);
        var suffixLengthDecoder = new DeltaBinaryPackedDecoder(suffixData);
        var suffixLengths = new int[count];
        suffixLengthDecoder.DecodeInt32s(suffixLengths);

        // Step 3: Raw suffix bytes follow the suffix length block
        var rawSuffixes = suffixData.Slice(suffixLengthDecoder.BytesConsumed);

        // Step 4: Reconstruct values by combining prefix from previous value + suffix
        // Compute total output size
        var valueLengths = new int[count];
        long totalBytes = 0;
        long totalSuffixBytes = 0;
        for (int i = 0; i < count; i++)
        {
            int prefixLen = prefixLengths[i];
            int suffixLen = suffixLengths[i];

            if (prefixLen < 0 || suffixLen < 0)
                throw new ParquetFormatException(
                    $"DELTA_BYTE_ARRAY value at index {i} has a negative prefix ({prefixLen}) or suffix " +
                    $"({suffixLen}) length.");

            // A value is the first prefixLen bytes of the PREVIOUS value plus a suffix. A prefix that does
            // not fit inside the previous value -- including ANY prefix on the first value, which has no
            // predecessor -- would be reconstructed from the zero-filled bytes reserved for this value:
            // neither what was encoded nor an error. The output buffer is sized from these same lengths,
            // so nothing reads out of bounds and nothing throws; it just comes out wrong.
            int previousLength = i == 0 ? 0 : valueLengths[i - 1];
            if (prefixLen > previousLength)
                throw new ParquetFormatException(
                    $"DELTA_BYTE_ARRAY value at index {i} claims a {prefixLen}-byte prefix of a value that " +
                    $"is {previousLength} bytes long.");

            valueLengths[i] = prefixLen + suffixLen;
            if (fixedWidth && valueLengths[i] != typeLength)
                throw new ParquetFormatException(
                    $"DELTA_BYTE_ARRAY value at index {i} is {valueLengths[i]} bytes, but the column is " +
                    $"FIXED_LEN_BYTE_ARRAY({typeLength}). Every value in such a column is exactly that wide.");
            totalBytes += valueLengths[i];
            totalSuffixBytes += suffixLen;
        }

        // Accumulated as long: prefixes let the total grow faster than the page does, so a malformed
        // page can describe more output than an int can hold.
        if (totalBytes > int.MaxValue)
            throw new ParquetFormatException(
                $"DELTA_BYTE_ARRAY page describes {totalBytes} bytes of values, which exceeds the maximum " +
                "addressable buffer.");

        if (totalSuffixBytes > rawSuffixes.Length)
            throw new ParquetFormatException(
                $"DELTA_BYTE_ARRAY page declares {totalSuffixBytes} suffix bytes but carries " +
                $"{rawSuffixes.Length}.");

        var outputData = new byte[(int)totalBytes];
        var offsets = new int[count + 1];
        int outputPos = 0;
        int suffixPos = 0;
        int prevOffset = 0;
        int prevLength = 0;

        for (int i = 0; i < count; i++)
        {
            offsets[i] = outputPos;
            int prefixLen = prefixLengths[i];
            int suffixLen = suffixLengths[i];

            // Copy prefix from previous value
            if (prefixLen > 0)
                outputData.AsSpan(prevOffset, prefixLen).CopyTo(outputData.AsSpan(outputPos));

            // Copy suffix from raw data
            if (suffixLen > 0)
                rawSuffixes.Slice(suffixPos, suffixLen).CopyTo(outputData.AsSpan(outputPos + prefixLen));

            prevOffset = outputPos;
            prevLength = prefixLen + suffixLen;
            outputPos += prevLength;
            suffixPos += suffixLen;
        }
        offsets[count] = outputPos;

        if (fixedWidth)
        {
            // Every value is exactly typeLength bytes, so the reconstruction above is already the packed
            // layout the fixed-width buffer wants and the offsets are redundant. AddByteArrayValues is NOT
            // an option here: it writes through the data/offsets buffer pair, which ColumnBuildState only
            // allocates for BYTE_ARRAY columns -- reaching it with a FIXED_LEN_BYTE_ARRAY column threw a
            // NullReferenceException.
            outputData.AsSpan(0, count * typeLength).CopyTo(state.ReserveFixedBytes(count, typeLength));
            return;
        }

        state.AddByteArrayValues(offsets, outputData, count);
    }
}
