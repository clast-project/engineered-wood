// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Reads a Roaring Bitmap from the portable serialization format used by
/// Delta Lake's deletion vectors. Delta wraps the standard portable
/// Roaring blob with a 4-byte "MT1d" magic; the rest is delegated to
/// <see cref="EngineeredWood.Encodings.RoaringBitmap"/>.
/// </summary>
internal static class RoaringBitmapReader
{
    /// <summary>
    /// Magic number for the "RoaringBitmapArray" format used by Delta Lake DVs.
    /// The first 4 bytes of a DV file are this magic number (little-endian).
    /// </summary>
    private const uint RoaringBitmapArrayMagic = 1681511377; // 0x6431544D "MT1d"

    /// <summary>
    /// Deserializes a RoaringBitmapArray (Delta Lake DV format) into a set
    /// of deleted row indices. The format is 4-byte magic + portable
    /// Roaring serialization.
    /// </summary>
    public static HashSet<long> Deserialize(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4)
            throw new DeltaFormatException("Deletion vector data too short.");

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(data);
        if (magic != RoaringBitmapArrayMagic)
            throw new DeltaFormatException(
                $"Invalid deletion vector magic: 0x{magic:X8}, expected 0x{RoaringBitmapArrayMagic:X8}.");

        // Portable RoaringBitmapArray: int64 sub-bitmap count, then per sub-bitmap an int32 high-32-bit key +
        // a standard portable 32-bit CRoaring bitmap. This is the only accepted form — the writer emits it and
        // Spark / delta-kernel / Fabric all read and write it.
        int pos = 4;
        if (data.Length < pos + 8)
            throw new DeltaFormatException("Deletion vector truncated: missing RoaringBitmapArray sub-bitmap count.");

        long subBitmaps = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(pos, 8));
        pos += 8;
        if (subBitmaps < 0 || subBitmaps > int.MaxValue)
            throw new DeltaFormatException($"Deletion vector has an invalid sub-bitmap count: {subBitmaps}.");

        var result = new HashSet<long>();
        for (long s = 0; s < subBitmaps; s++)
        {
            if (data.Length < pos + 4)
                throw new DeltaFormatException("Deletion vector truncated: missing sub-bitmap key.");
            uint key = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(pos, 4));
            pos += 4;
            long highBits = (long)key << 32;
            int consumed = EngineeredWood.Encodings.RoaringBitmap.DeserializePortable(
                data.Slice(pos), v => result.Add(highBits | v),
                EngineeredWood.Encodings.RoaringBitmap.RoaringFormat.CRoaring);
            if (consumed <= 0)
                throw new DeltaFormatException("Deletion vector sub-bitmap could not be decoded.");
            pos += consumed;
        }

        return result;
    }
}
