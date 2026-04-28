// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Ipc;
using EngineeredWood.IO;
using EngineeredWood.Lance.Table.Proto;

namespace EngineeredWood.Lance.Table.Deletions;

/// <summary>
/// Reads a Lance deletion file referenced by
/// <see cref="DataFragment.DeletionFile"/>. Two formats are supported,
/// matching the proto enum:
/// <list type="bullet">
/// <item><c>ARROW_ARRAY</c> (sparse): an Arrow IPC file with a single
///   <see cref="Apache.Arrow.Int32Array"/> column of deleted row
///   offsets. Used when only a few rows are deleted.</item>
/// <item><c>BITMAP</c> (dense): a portable Roaring bitmap of deleted
///   row offsets. Used when many rows are deleted.</item>
/// </list>
///
/// <para>Path format per the proto:
/// <c>{root}/_deletions/{fragment_id}-{read_version}-{id}.{ext}</c>.</para>
/// </summary>
internal static class DeletionFileReader
{
    private const string DeletionsDirectory = "_deletions";

    public static async ValueTask<DeletionMask> ReadAsync(
        ITableFileSystem fs, ulong fragmentId, DeletionFile deletionFile,
        CancellationToken cancellationToken = default)
    {
        string path = BuildPath(fragmentId, deletionFile);
        byte[] bytes = await fs.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);

        return deletionFile.FileType switch
        {
            DeletionFile.Types.DeletionFileType.ArrowArray => ReadArrowArray(bytes, path),
            DeletionFile.Types.DeletionFileType.Bitmap => ReadRoaringBitmap(bytes),
            _ => throw new LanceTableFormatException(
                $"Unknown deletion file type '{deletionFile.FileType}' for fragment {fragmentId}."),
        };
    }

    private static string BuildPath(ulong fragmentId, DeletionFile deletionFile)
    {
        string ext = deletionFile.FileType == DeletionFile.Types.DeletionFileType.ArrowArray
            ? ".arrow"
            : ".bin";
        return $"{DeletionsDirectory}/{fragmentId}-{deletionFile.ReadVersion}-{deletionFile.Id}{ext}";
    }

    private static DeletionMask ReadArrowArray(byte[] bytes, string pathForErrors)
    {
        // Apache.Arrow's ArrowFileReader expects a Stream and an
        // ICompressionCodecFactory if the file uses per-buffer compression
        // (pylance compresses with ZSTD).
        using var ms = new MemoryStream(bytes, writable: false);
        using var reader = new ArrowFileReader(
            ms, allocator: null, compressionCodecFactory: LanceArrowCompressionFactory.Instance);
        var batch = reader.ReadNextRecordBatch();
        if (batch is null)
            throw new LanceTableFormatException(
                $"Deletion file '{pathForErrors}' has no record batch.");

        if (batch.ColumnCount != 1)
            throw new LanceTableFormatException(
                $"Deletion file '{pathForErrors}' should have 1 column, has {batch.ColumnCount}.");

        // Spec says Int32Array but pylance emits UInt32Array. Accept either.
        var col = batch.Column(0);
        var set = new HashSet<int>();
        switch (col)
        {
            case Apache.Arrow.Int32Array i32:
                for (int i = 0; i < i32.Length; i++)
                {
                    int? v = i32.GetValue(i);
                    if (v is not null) set.Add(v.Value);
                }
                break;
            case Apache.Arrow.UInt32Array u32:
                for (int i = 0; i < u32.Length; i++)
                {
                    uint? v = u32.GetValue(i);
                    if (v is not null) set.Add(checked((int)v.Value));
                }
                break;
            default:
                throw new LanceTableFormatException(
                    $"Deletion file '{pathForErrors}' column should be Int32Array or UInt32Array, " +
                    $"got {col.GetType().Name}.");
        }
        return new DeletionMask(set);
    }

    private static DeletionMask ReadRoaringBitmap(byte[] bytes)
    {
        var set = new HashSet<int>();
        EngineeredWood.Encodings.RoaringBitmap.DeserializePortable(
            bytes,
            v => set.Add(checked((int)v)),
            EngineeredWood.Encodings.RoaringBitmap.RoaringFormat.CRoaring);
        return new DeletionMask(set);
    }
}
