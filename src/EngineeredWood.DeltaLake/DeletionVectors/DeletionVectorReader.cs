// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Text;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Resolves and reads deletion vectors from inline data or files.
/// </summary>
public sealed class DeletionVectorReader
{
    private readonly ITableFileSystem _fs;

    public DeletionVectorReader(ITableFileSystem fileSystem)
    {
        _fs = fileSystem;
    }

    /// <summary>
    /// Reads a deletion vector and returns the set of deleted row indices.
    /// </summary>
    public async ValueTask<HashSet<long>> ReadAsync(
        DeletionVector dv,
        CancellationToken cancellationToken = default)
    {
        byte[] data = dv.StorageType switch
        {
            "i" => ReadInline(dv),
            "u" => await ReadUuidFileAsync(dv, cancellationToken).ConfigureAwait(false),
            "p" => await ReadAbsoluteFileAsync(dv, cancellationToken).ConfigureAwait(false),
            _ => throw new DeltaFormatException(
                $"Unknown deletion vector storage type: '{dv.StorageType}'"),
        };

        return RoaringBitmapReader.Deserialize(data);
    }

    /// <summary>
    /// Reads an inline deletion vector (Base85/Z85-encoded).
    /// </summary>
    private static byte[] ReadInline(DeletionVector dv)
    {
        // The first character of pathOrInlineDv indicates the size encoding
        // For inline DVs, the data is Base85-encoded after a size prefix
        string encoded = dv.PathOrInlineDv;

        if (encoded.Length == 0)
            throw new DeltaFormatException("Empty inline deletion vector.");

        // The first 4 bytes (encoded as 5 Z85 chars) encode the size
        // But in practice, Delta stores the entire DV as Z85-encoded bytes
        return Base85.Decode(encoded);
    }

    /// <summary>
    /// Reads a file-based DV with a UUID-relative path. The path derivation lives in
    /// <see cref="DeletionVectorPath"/> because VACUUM must resolve it identically — a divergence
    /// there means vacuum deletes a live DV and its masked rows silently reappear.
    /// </summary>
    private async ValueTask<byte[]> ReadUuidFileAsync(
        DeletionVector dv, CancellationToken cancellationToken)
    {
        string filePath = DeletionVectorPath.GetRelativePath(dv)
            ?? throw new DeltaFormatException(
                $"Deletion vector storage type '{dv.StorageType}' has no table-relative path.");

        return await ReadDvFileAsync(filePath, dv.Offset ?? 0, dv.SizeInBytes, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a file-based DV with an absolute path.
    /// </summary>
    private async ValueTask<byte[]> ReadAbsoluteFileAsync(
        DeletionVector dv, CancellationToken cancellationToken)
    {
        return await ReadDvFileAsync(dv.PathOrInlineDv, dv.Offset ?? 0, dv.SizeInBytes, cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask<byte[]> ReadDvFileAsync(
        string path, int offset, int size,
        CancellationToken cancellationToken)
    {
        byte[] allBytes = await _fs.ReadAllBytesAsync(path, cancellationToken)
            .ConfigureAwait(false);

        if (offset == 0 && size == allBytes.Length)
            return allBytes;

        // Spec on-disk layout (the shape Spark writes): the file starts with a 1-byte format version,
        // and each stored DV is "<dataSize: 4-byte BIG-ENDIAN int><bitmap: dataSize bytes><CRC-32:
        // 4 bytes>" with the descriptor's offset pointing at the SIZE field and sizeInBytes == dataSize.
        // Detect that shape by the size field matching, and return the bitmap bytes (skip the length
        // prefix; the trailing checksum is not verified). Anything else falls back to a raw slice
        // (a bare bitmap written at the offset).
        if (offset + 4 + size <= allBytes.Length)
        {
            int be = (allBytes[offset] << 24) | (allBytes[offset + 1] << 16)
                     | (allBytes[offset + 2] << 8) | allBytes[offset + 3];
            if (be == size)
            {
                var bitmap = new byte[size];
                Array.Copy(allBytes, offset + 4, bitmap, 0, size);
                return bitmap;
            }
        }

        // Extract the relevant slice
        if (offset + size > allBytes.Length)
            throw new DeltaFormatException(
                $"Deletion vector at {path} offset {offset} size {size} " +
                $"exceeds file length {allBytes.Length}.");

        var result = new byte[size];
        Array.Copy(allBytes, offset, result, 0, size);
        return result;
    }

}
