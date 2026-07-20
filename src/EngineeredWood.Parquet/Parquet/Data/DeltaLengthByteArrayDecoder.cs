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
    public static void Decode(ReadOnlySpan<byte> data, int count, ColumnBuildState state)
    {
        // Step 1: Decode lengths using DELTA_BINARY_PACKED
        var lengthDecoder = new DeltaBinaryPackedDecoder(data);
        var lengths = new int[count];
        lengthDecoder.DecodeInt32s(lengths);

        // Step 2: Build offsets array (count + 1 entries, Arrow format)
        var offsets = new int[count + 1];
        offsets[0] = 0;
        for (int i = 0; i < count; i++)
            offsets[i + 1] = offsets[i] + lengths[i];

        // Step 3: Raw byte data follows immediately after the length block
        int totalDataBytes = offsets[count];
        var byteData = data.Slice(lengthDecoder.BytesConsumed, totalDataBytes);

        state.AddByteArrayValues(offsets, byteData, count);
    }
}
