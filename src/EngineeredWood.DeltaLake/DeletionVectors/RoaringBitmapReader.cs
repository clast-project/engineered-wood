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
    /// Deserializes a RoaringBitmapArray (Delta Lake DV format: 4-byte magic + portable Roaring serialization)
    /// into the rows it deletes. Copies <paramref name="data"/>; the <see cref="Deserialize(byte[])"/> overload
    /// does not.
    /// </summary>
    public static DeletionVectorBitmap Deserialize(ReadOnlySpan<byte> data) => Deserialize(data.ToArray());

    /// <summary>
    /// Deserializes a RoaringBitmapArray into the rows it deletes, reading <paramref name="data"/> in place:
    /// the result keeps it, so the caller must not change it afterwards.
    /// </summary>
    public static DeletionVectorBitmap Deserialize(byte[] data)
    {
        if (data is null)
            throw new ArgumentNullException(nameof(data));
        if (data.Length < 4)
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector, "Deletion vector data too short.");

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(data);
        if (magic != RoaringBitmapArrayMagic)
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                $"Invalid deletion vector magic: 0x{magic:X8}, expected 0x{RoaringBitmapArrayMagic:X8}.");

        // Portable RoaringBitmapArray: int64 sub-bitmap count, then per sub-bitmap an int32 high-32-bit key +
        // a standard portable 32-bit CRoaring bitmap. This is the only accepted form — the writer emits it and
        // Spark / delta-kernel / Fabric all read and write it.
        int pos = 4;
        if (data.Length < pos + 8)
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                "Deletion vector truncated: missing RoaringBitmapArray sub-bitmap count.");

        long subBitmaps = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(pos, 8));
        pos += 8;
        if (subBitmaps < 0 || subBitmaps > int.MaxValue)
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                $"Deletion vector has an invalid sub-bitmap count: {subBitmaps}.");

        return DeletionVectorBitmap.Parse(data, pos, subBitmaps);
    }
}
