// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Z85 (ZeroMQ Base-85) encoder/decoder as specified in
/// https://rfc.zeromq.org/spec/32/
/// Used for inline deletion vector encoding.
/// </summary>
/// <summary>
/// Z85 (ZeroMQ Base-85) encoder/decoder.
/// </summary>
public static class Base85
{
    private static readonly char[] s_encodeTable =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
        .ToCharArray();

    private static readonly byte[] s_decodeTable = BuildDecodeTable();

    private static byte[] BuildDecodeTable()
    {
        var table = new byte[128];
        for (int j = 0; j < table.Length; j++) table[j] = 0xFF;
        for (int i = 0; i < s_encodeTable.Length; i++)
            table[s_encodeTable[i]] = (byte)i;
        return table;
    }

    /// <summary>
    /// Decodes a Z85-encoded string to bytes.
    /// Input length must be a multiple of 5, producing output length = input * 4 / 5.
    /// </summary>
    public static byte[] Decode(string encoded)
    {
        if (encoded.Length % 5 != 0)
            throw new DeltaFormatException(
                $"Invalid Z85 string length {encoded.Length} (must be multiple of 5).");

        int outputLen = encoded.Length * 4 / 5;
        var result = new byte[outputLen];
        int outIdx = 0;

        for (int i = 0; i < encoded.Length; i += 5)
        {
            uint value = 0;
            for (int j = 0; j < 5; j++)
            {
                char c = encoded[i + j];
                if (c >= 128 || s_decodeTable[c] == 0xFF)
                    throw new DeltaFormatException($"Invalid Z85 character: '{c}'");
                value = value * 85 + s_decodeTable[c];
            }

            // Write as big-endian 4 bytes
            result[outIdx++] = (byte)(value >> 24);
            result[outIdx++] = (byte)(value >> 16);
            result[outIdx++] = (byte)(value >> 8);
            result[outIdx++] = (byte)value;
        }

        return result;
    }

    /// <summary>
    /// Encodes bytes to a Z85 string.
    /// Input length must be a multiple of 4, producing output length = input * 5 / 4.
    /// </summary>
    public static string Encode(byte[] data)
    {
        if (data.Length % 4 != 0)
            throw new ArgumentException(
                $"Input length {data.Length} must be a multiple of 4 for Z85 encoding.");

        var chars = new char[data.Length * 5 / 4];
        int outIdx = 0;

        for (int i = 0; i < data.Length; i += 4)
        {
            uint value = ((uint)data[i] << 24) | ((uint)data[i + 1] << 16) |
                         ((uint)data[i + 2] << 8) | data[i + 3];

            // Encode as 5 Z85 characters (big-endian)
            for (int j = 4; j >= 0; j--)
            {
                chars[outIdx + j] = s_encodeTable[value % 85];
                value /= 85;
            }
            outIdx += 5;
        }

        return new string(chars);
    }
}
