// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Orc.Encodings;

/// <summary>
/// Maps between encoded width values (5-bit) and actual bit widths for RLE v2.
/// </summary>
internal static class WidthEncoding
{
    // Encoded value (0-31) -> actual bit width
    // For non-delta contexts: 0→1, 1→2, ..., 23→24, 24→26, 25→28, 26→30, 27→32, 28→40, 29→48, 30→56, 31→64
    private static readonly int[] DecodingTable =
    [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        17, 18, 19, 20, 21, 22, 23, 24, 26, 28, 30, 32, 40, 48, 56, 64
    ];

    /// <summary>
    /// Decodes width for non-delta sub-encodings (Short Repeat, Direct, Patched Base).
    /// Encoded 0 → 1 bit.
    /// </summary>
    public static int Decode(int encoded)
    {
        if (encoded < 0 || encoded >= DecodingTable.Length)
            throw new InvalidDataException($"Invalid encoded width: {encoded}");
        return DecodingTable[encoded];
    }

    /// <summary>
    /// Decodes width for Delta sub-encoding.
    /// Encoded 0 → 0 bits (constant delta).
    /// </summary>
    public static int DecodeDelta(int encoded)
    {
        if (encoded == 0) return 0;
        return Decode(encoded);
    }

    /// <summary>
    /// Gets the closest encoded width value for a given bit width.
    /// </summary>
    public static int GetClosestWidth(int bitWidth)
    {
        for (int i = 0; i < DecodingTable.Length; i++)
        {
            if (DecodingTable[i] >= bitWidth) return DecodingTable[i];
        }
        return 64;
    }

    /// <summary>
    /// Encodes a bit width to its 5-bit encoded value.
    /// The bitWidth must be one of the valid values from the decoding table.
    /// </summary>
    public static int Encode(int bitWidth)
    {
        for (int i = 0; i < DecodingTable.Length; i++)
        {
            if (DecodingTable[i] == bitWidth) return i;
        }
        throw new ArgumentException($"Invalid bit width for encoding: {bitWidth}");
    }
}
