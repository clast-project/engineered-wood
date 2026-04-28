// Copyright (c) Curt Hagenlocher. All rights reserved.
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

        var result = new HashSet<long>();
        EngineeredWood.Encodings.RoaringBitmap.DeserializePortable(data.Slice(4), v => result.Add(v));
        return result;
    }
}
