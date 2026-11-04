// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// The JSON-stats form of a variant column's <c>minValues</c> / <c>maxValues</c>: a binary Variant
/// (an object keyed by normalized JSON paths such as <c>$['a']['b']</c>), Z85-encoded as a string.
/// </summary>
/// <remarks>
/// <para>The protocol ("Statistics for Variant Columns") says only that the bound is "z85-encoded binary
/// Variant". The framing is delta-spark 4.3's, measured from its stats: the Variant's metadata bytes, then
/// its value bytes, zero-padded to a multiple of four (Z85 encodes 4-byte groups). Neither part carries a
/// length, so decoding finds the split by reading the metadata header, and drops the padding by reading the
/// value header.</para>
///
/// <para>In a checkpoint's <c>stats_parsed</c> the same bound is a Parquet Variant group; that form is the
/// two byte arrays this class splits the string into.</para>
/// </remarks>
internal static class VariantStatsEncoding
{
    /// <summary>The Z85 string for a Variant's metadata and value.</summary>
    public static string Encode(ReadOnlySpan<byte> metadata, ReadOnlySpan<byte> value)
    {
        int length = metadata.Length + value.Length;
        var bytes = new byte[(length + 3) / 4 * 4];
        metadata.CopyTo(bytes);
        value.CopyTo(bytes.AsSpan(metadata.Length));
        return Base85.Encode(bytes);
    }

    /// <summary>
    /// Splits a Z85 variant bound into its metadata and value, without the padding. False for anything that is
    /// not a well-formed Z85 string holding a version-1 metadata followed by a complete value.
    /// </summary>
    public static bool TryDecode(string encoded, out byte[] metadata, out byte[] value)
    {
        metadata = value = [];
        byte[] bytes;
        try
        {
            bytes = Base85.Decode(encoded);
        }
        catch (DeltaFormatException)
        {
            return false;
        }

        if (!TryMetadataSize(bytes, out int metadataSize)
            || !TryValueSize(bytes.AsSpan(metadataSize), out int valueSize))
        {
            return false;
        }

        // Only padding may follow the value: anything else means the split was wrong.
        for (int i = metadataSize + valueSize; i < bytes.Length; i++)
        {
            if (bytes[i] != 0)
                return false;
        }

        metadata = bytes.AsSpan(0, metadataSize).ToArray();
        value = bytes.AsSpan(metadataSize, valueSize).ToArray();
        return true;
    }

    /// <summary>
    /// The size of the Variant metadata at the start of <paramref name="bytes"/>: a header byte (version 1 in the
    /// low four bits, offset size minus one in the top two), the dictionary size, its offsets, and its strings.
    /// </summary>
    internal static bool TryMetadataSize(ReadOnlySpan<byte> bytes, out int size)
    {
        size = 0;
        if (bytes.Length < 1 || (bytes[0] & 0x0F) != 1)
            return false;

        int offsetSize = ((bytes[0] >> 6) & 0x3) + 1;
        if (!TryReadUnsigned(bytes, 1, offsetSize, out long dictionarySize))
            return false;
        long lastOffsetAt = 1 + offsetSize + dictionarySize * offsetSize;
        if (!TryReadUnsigned(bytes, lastOffsetAt, offsetSize, out long stringBytes))
            return false;
        return TryFits(lastOffsetAt + offsetSize + stringBytes, bytes.Length, out size);
    }

    /// <summary>The size of the Variant value at the start of <paramref name="bytes"/>, from its header.</summary>
    internal static bool TryValueSize(ReadOnlySpan<byte> bytes, out int size)
    {
        size = 0;
        if (bytes.Length < 1)
            return false;

        int header = bytes[0];
        int basicType = header & 0x3;
        int typeInfo = header >> 2;
        switch (basicType)
        {
            case 0: // primitive
                long? fixedSize = typeInfo switch
                {
                    0 or 1 or 2 => 0, // null, true, false
                    3 => 1, // int8
                    4 => 2, // int16
                    5 or 11 or 14 => 4, // int32, date, float
                    6 or 7 or 12 or 13 or 17 or 18 or 19 => 8, // int64, double, timestamps, time
                    8 => 1 + 4, // decimal4: scale + unscaled
                    9 => 1 + 8, // decimal8
                    10 => 1 + 16, // decimal16
                    20 => 16, // uuid
                    15 or 16 => null, // binary, string: a 4-byte length first
                    _ => -1,
                };
                if (fixedSize is long n)
                    return n >= 0 && TryFits(1 + n, bytes.Length, out size);
                return TryReadUnsigned(bytes, 1, 4, out long length) && TryFits(1 + 4 + length, bytes.Length, out size);

            case 1: // short string: the length is the type info
                return TryFits(1 + typeInfo, bytes.Length, out size);

            case 2: // object
            {
                int offsetSize = (typeInfo & 0x3) + 1;
                int idSize = ((typeInfo >> 2) & 0x3) + 1;
                int countSize = ((typeInfo >> 4) & 0x1) == 1 ? 4 : 1;
                if (!TryReadUnsigned(bytes, 1, countSize, out long count))
                    return false;
                long lastOffsetAt = 1 + countSize + count * idSize + count * offsetSize;
                return TryReadUnsigned(bytes, lastOffsetAt, offsetSize, out long dataBytes)
                    && TryFits(lastOffsetAt + offsetSize + dataBytes, bytes.Length, out size);
            }

            default: // array
            {
                int offsetSize = (typeInfo & 0x3) + 1;
                int countSize = ((typeInfo >> 2) & 0x1) == 1 ? 4 : 1;
                if (!TryReadUnsigned(bytes, 1, countSize, out long count))
                    return false;
                long lastOffsetAt = 1 + countSize + count * offsetSize;
                return TryReadUnsigned(bytes, lastOffsetAt, offsetSize, out long dataBytes)
                    && TryFits(lastOffsetAt + offsetSize + dataBytes, bytes.Length, out size);
            }
        }
    }

    private static bool TryReadUnsigned(ReadOnlySpan<byte> bytes, long at, int width, out long value)
    {
        value = 0;
        if (at < 0 || at + width > bytes.Length)
            return false;
        Span<byte> buffer = stackalloc byte[8];
        buffer.Clear();
        bytes.Slice((int)at, width).CopyTo(buffer);
        value = BinaryPrimitives.ReadInt64LittleEndian(buffer);
        return true;
    }

    private static bool TryFits(long needed, int available, out int size)
    {
        size = needed >= 0 && needed <= available ? (int)needed : 0;
        return needed >= 0 && needed <= available;
    }
}
