// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes DELTA_LENGTH_BYTE_ARRAY encoded values for BYTE_ARRAY columns.
/// </summary>
/// <remarks>
/// Format:
/// <list type="number">
/// <item>Lengths: encoded as DELTA_BINARY_PACKED (INT32)</item>
/// <item>Data: concatenated raw bytes for all values</item>
/// </list>
/// </remarks>
internal static class DeltaLengthByteArrayDecoder
{
    /// <summary>
    /// Decodes <paramref name="count"/> byte array values and appends them to <paramref name="state"/>.
    /// </summary>
    /// <param name="selected">Append only the values the page's row selection keeps
    /// (<see cref="ColumnBuildState.PageValueMask"/>); every length is still decoded and checked.</param>
    public static void Decode(ReadOnlySpan<byte> data, int count, ColumnBuildState state, bool selected = false)
    {
        // Step 1: Decode lengths using DELTA_BINARY_PACKED
        var lengthDecoder = new DeltaBinaryPackedDecoder(data);
        var lengths = new int[count];
        lengthDecoder.DecodeInt32s(lengths);

        // Step 2: Build offsets array (count + 1 entries, Arrow format). Every length is checked here, before any
        // value is written and whether or not a row selection keeps it: a negative length, or lengths adding up past
        // the page's bytes, is a corrupt page. Unchecked, the offset-buffer outputs wrote such offsets as given
        // (non-monotonic, or wrapped past int.MaxValue) without complaint, and a selection that dropped the bad value
        // hid it from the view output's per-value slice too (#787 review).
        long available = data.Length - lengthDecoder.BytesConsumed;
        var offsets = new int[count + 1];
        offsets[0] = 0;
        long total = 0;
        for (int i = 0; i < count; i++)
        {
            int length = lengths[i];
            if (length < 0)
            {
                throw new ParquetFormatException(
                    $"DELTA_LENGTH_BYTE_ARRAY page: value {i} has a negative length ({length}).");
            }

            total += length;
            if (total > available)
            {
                throw new ParquetFormatException(
                    $"DELTA_LENGTH_BYTE_ARRAY page: its lengths add up to more than the {available} bytes it holds.");
            }

            offsets[i + 1] = (int)total;
        }

        // Step 3: Raw byte data follows immediately after the length block
        int totalDataBytes = offsets[count];
        var byteData = data.Slice(lengthDecoder.BytesConsumed, totalDataBytes);

        if (selected)
            state.AddSelectedByteArrayValues(offsets, byteData, stride: 0);
        else
            state.AddByteArrayValues(offsets, byteData, count);
    }
}
