// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
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
    /// Reads a deletion vector and returns the rows it deletes.
    /// </summary>
    public async ValueTask<DeletionVectorBitmap> ReadAsync(
        DeletionVector dv,
        CancellationToken cancellationToken = default)
    {
        return dv.StorageType switch
        {
            "i" => RoaringBitmapReader.Deserialize(ReadInline(dv)),
            "u" => await ReadUuidFileAsync(dv, cancellationToken).ConfigureAwait(false),
            "p" => await ReadAbsoluteFileAsync(dv, cancellationToken).ConfigureAwait(false),
            _ => throw new DeltaFormatException(
                DeltaErrorCodes.UnsupportedDeletionVectorStorageType,
                $"Unknown deletion vector storage type: '{dv.StorageType}'"),
        };
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
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector, "Empty inline deletion vector.");

        // The first 4 bytes (encoded as 5 Z85 chars) encode the size
        // But in practice, Delta stores the entire DV as Z85-encoded bytes
        return Base85.Decode(encoded);
    }

    /// <summary>
    /// Reads a file-based DV with a UUID-relative path. The path derivation lives in
    /// <see cref="DeletionVectorPath"/> because VACUUM must resolve it identically — a divergence
    /// there means vacuum deletes a live DV and its masked rows silently reappear.
    /// </summary>
    private async ValueTask<DeletionVectorBitmap> ReadUuidFileAsync(
        DeletionVector dv, CancellationToken cancellationToken)
    {
        string filePath = DeletionVectorPath.GetRelativePath(dv)
            ?? throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                $"Deletion vector storage type '{dv.StorageType}' has no table-relative path.");

        return await ReadDvFileAsync(filePath, dv.Offset ?? 0, dv.SizeInBytes, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a file-based DV with an absolute path.
    /// </summary>
    private async ValueTask<DeletionVectorBitmap> ReadAbsoluteFileAsync(
        DeletionVector dv, CancellationToken cancellationToken)
    {
        return await ReadDvFileAsync(dv.PathOrInlineDv, dv.Offset ?? 0, dv.SizeInBytes, cancellationToken)
            .ConfigureAwait(false);
    }

    private const int DataSizeLength = 4;
    private const int ChecksumLength = 4;

    /// <summary>
    /// <para>Reads ONE stored deletion vector out of an on-disk DV file, fetching only its own bytes.</para>
    ///
    /// <para>Spec layout (what Spark, delta-rs and this library write): the file starts with a 1-byte format
    /// version, then each stored DV is <c>&lt;dataSize: 4-byte big-endian&gt;&lt;bitmap: dataSize
    /// bytes&gt;&lt;CRC-32 of the bitmap: 4-byte big-endian&gt;</c>, and the descriptor's <c>offset</c> points
    /// at the size field with <c>sizeInBytes == dataSize</c>. Spark packs the DVs of many data files into one
    /// file, so reading the whole file per descriptor costs O(files x file size); one ranged read of
    /// <c>[offset, offset + 4 + size + 4)</c> costs only this DV's bytes, in one request on every backend.</para>
    ///
    /// <para>Like Spark's <c>DeletionVectorStore.readRangeFromStream</c>, a size field that disagrees with the
    /// descriptor and a checksum that disagrees with the bitmap are both refused. There is no fallback to a bare
    /// bitmap at the offset: no writer produces one, and accepting one would need a second read.</para>
    /// </summary>
    private async ValueTask<DeletionVectorBitmap> ReadDvFileAsync(
        string path, int offset, int size,
        CancellationToken cancellationToken)
    {
        // The read covers the size field and checksum too, and every backend allocates it as one buffer:
        // a size within 8 of int.MaxValue would overflow that allocation rather than read as malformed.
        if (offset < 0 || size < 0 || size > int.MaxValue - DataSizeLength - ChecksumLength)
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                $"Deletion vector at {path} has an invalid offset ({offset}) or size ({size}).");

        var range = new FileRange(offset, (long)DataSizeLength + size + ChecksumLength);

        IRandomAccessFile file = await _fs.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
        try
        {
            IMemoryOwner<byte> owner;
            try
            {
                owner = await file.ReadAsync(range, cancellationToken).ConfigureAwait(false);
            }
            catch (IOException ex)
            {
                // Every backend reports a range past the end as an IOException, as it reports other I/O
                // failures. Ask for the length only now, so the common path stays one request.
                long? length = await TryGetLengthAsync(file, cancellationToken).ConfigureAwait(false);
                if (length is long fileLength && range.End > fileLength)
                    throw new DeltaFormatException(
                        DeltaErrorCodes.InvalidDeletionVector,
                        $"Deletion vector at {path} offset {offset} size {size} " +
                        $"exceeds file length {fileLength}.",
                        ex);
                throw;
            }

            using (owner)
                return Decode(owner.Memory.Span.Slice(0, checked((int)range.Length)), path, offset, size);
        }
        finally
        {
            await file.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async ValueTask<long?> TryGetLengthAsync(
        IRandomAccessFile file, CancellationToken cancellationToken)
    {
        try
        {
            return await file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            return null; // the original failure is the one worth reporting
        }
    }

    private static DeletionVectorBitmap Decode(ReadOnlySpan<byte> stored, string path, int offset, int size)
    {
        int dataSize = BinaryPrimitives.ReadInt32BigEndian(stored);
        if (dataSize != size)
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                $"Deletion vector at {path} offset {offset}: the stored size {dataSize} does not match " +
                $"the descriptor's sizeInBytes {size}.");

        ReadOnlySpan<byte> bitmap = stored.Slice(DataSizeLength, size);
        uint expected = BinaryPrimitives.ReadUInt32BigEndian(stored.Slice(DataSizeLength + size));
        uint actual = Crc32.HashToUInt32(bitmap);
        if (actual != expected)
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                $"Deletion vector at {path} offset {offset}: checksum mismatch " +
                $"(stored 0x{expected:X8}, computed 0x{actual:X8}).");

        return RoaringBitmapReader.Deserialize(bitmap);
    }

}
