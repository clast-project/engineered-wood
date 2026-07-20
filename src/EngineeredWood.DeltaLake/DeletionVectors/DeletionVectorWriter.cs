// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Creates deletion vectors from sets of deleted row indices.
/// Small DVs are stored inline (Base85-encoded); large DVs are written to files.
/// </summary>
public sealed class DeletionVectorWriter
{
    private readonly ITableFileSystem _fs;

    /// <summary>
    /// Maximum size in bytes for inline DVs. DVs larger than this are written to files.
    /// Default: 1 KB.
    /// </summary>
    public int InlineThreshold { get; init; } = 1024;

    public DeletionVectorWriter(ITableFileSystem fileSystem)
    {
        _fs = fileSystem;
    }

    /// <summary>
    /// Creates a deletion vector from the given deleted row indices.
    /// Returns the DV descriptor to include in the <see cref="AddFile"/> action.
    /// </summary>
    public async ValueTask<DeletionVector> CreateAsync(
        IEnumerable<long> deletedRowIndices,
        long cardinality,
        CancellationToken cancellationToken = default)
    {
        byte[] dvBlob = RoaringBitmapWriter.Serialize(deletedRowIndices);

        if (dvBlob.Length <= InlineThreshold)
            return CreateInline(dvBlob, cardinality);

        return await CreateFileAsync(dvBlob, cardinality, cancellationToken)
            .ConfigureAwait(false);
    }

    private DeletionVector CreateInline(byte[] dvBlob, long cardinality)
    {
        // Pad to multiple of 4 for Z85 encoding
        byte[] padded = dvBlob;
        int remainder = dvBlob.Length % 4;
        if (remainder != 0)
        {
            padded = new byte[dvBlob.Length + (4 - remainder)];
            dvBlob.CopyTo(padded, 0);
        }

        string encoded = Base85.Encode(padded);

        return new DeletionVector
        {
            StorageType = "i",
            PathOrInlineDv = encoded,
            SizeInBytes = dvBlob.Length,
            Cardinality = cardinality,
        };
    }

    private async ValueTask<DeletionVector> CreateFileAsync(
        byte[] dvBlob, long cardinality, CancellationToken cancellationToken)
    {
        // Spec on-disk DV file (the shape Spark writes AND reads): stored in the TABLE ROOT (next to the
        // data files — NOT _delta_log/) as "deletion_vector_<uuid>.bin", where <uuid> is the CANONICAL
        // (big-endian) rendering of the same 16 bytes z85-encoded into pathOrInlineDv. File layout:
        // <format version: 1 byte = 1><dataSize: 4-byte big-endian int><bitmap bytes><CRC-32 of the
        // bitmap: 4-byte big-endian>, with the descriptor's offset pointing at the size field.
        string uuid = Guid.NewGuid().ToString();
        string fileName = $"deletion_vector_{uuid}.bin";

        // Canonical big-endian UUID bytes = the string's hex digits in order (NOT Guid.ToByteArray(),
        // which shuffles the first three groups little-endian).
        string hex = uuid.Replace("-", "");
        byte[] uuidBytes = new byte[16];
        for (int i = 0; i < 16; i++)
            uuidBytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
        string encodedUuid = Base85.Encode(uuidBytes);

        var crc = new System.IO.Hashing.Crc32();
        crc.Append(dvBlob);
        byte[] crcLe = crc.GetCurrentHash(); // System.IO.Hashing returns little-endian bytes

        byte[] fileBytes = new byte[1 + 4 + dvBlob.Length + 4];
        fileBytes[0] = 1; // format version
        fileBytes[1] = (byte)(dvBlob.Length >> 24);
        fileBytes[2] = (byte)(dvBlob.Length >> 16);
        fileBytes[3] = (byte)(dvBlob.Length >> 8);
        fileBytes[4] = (byte)dvBlob.Length;
        Array.Copy(dvBlob, 0, fileBytes, 5, dvBlob.Length);
        int tail = 5 + dvBlob.Length;
        fileBytes[tail] = crcLe[3];
        fileBytes[tail + 1] = crcLe[2];
        fileBytes[tail + 2] = crcLe[1];
        fileBytes[tail + 3] = crcLe[0];

        await _fs.WriteAllBytesAsync(fileName, fileBytes, cancellationToken)
            .ConfigureAwait(false);

        return new DeletionVector
        {
            StorageType = "u",
            PathOrInlineDv = encodedUuid,
            Offset = 1,
            SizeInBytes = dvBlob.Length,
            Cardinality = cardinality,
        };
    }
}
