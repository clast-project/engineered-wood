// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes BYTE_ARRAY values using the DELTA_LENGTH_BYTE_ARRAY encoding.
/// Mirror of <see cref="DeltaLengthByteArrayDecoder"/>.
/// </summary>
/// <remarks>
/// Format:
/// <list type="number">
/// <item>Lengths: encoded as DELTA_BINARY_PACKED (INT32)</item>
/// <item>Data: concatenated raw bytes for all values</item>
/// </list>
/// </remarks>
internal static class DeltaLengthByteArrayEncoder
{
    /// <summary>
    /// Encodes byte array values given their offsets and data.
    /// </summary>
    /// <param name="offsets">Arrow-style offset array (count + 1 entries).</param>
    /// <param name="data">Raw byte data referenced by offsets.</param>
    /// <param name="count">Number of values.</param>
    /// <returns>Encoded bytes.</returns>
    public static byte[] Encode(ReadOnlySpan<int> offsets, ReadOnlySpan<byte> data, int count)
    {
        // Step 1: Extract lengths
        var lengths = new int[count];
        for (int i = 0; i < count; i++)
            lengths[i] = offsets[i + 1] - offsets[i];

        // Step 2: Encode lengths with DELTA_BINARY_PACKED
        var deltaEncoder = new DeltaBinaryPackedEncoder();
        deltaEncoder.EncodeInt32s(lengths);
        var lengthBytes = deltaEncoder.ToArray();

        // Step 3: Concatenated raw data
        int totalDataBytes = offsets[count] - offsets[0];
        var rawData = data.Slice(offsets[0], totalDataBytes);

        // Step 4: Combine
        var result = new byte[lengthBytes.Length + totalDataBytes];
        lengthBytes.CopyTo(result.AsSpan(0));
        rawData.CopyTo(result.AsSpan(lengthBytes.Length));

        return result;
    }
}
