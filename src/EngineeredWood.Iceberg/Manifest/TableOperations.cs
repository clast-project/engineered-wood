// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>
/// Provides high-level operations for mutating an Iceberg table, including appending, overwriting,
/// and listing data and delete files. Each mutation produces a new <see cref="TableMetadata"/> snapshot.
/// </summary>
public sealed class TableOperations
{
    private readonly ITableFileSystem _fs;

    /// <summary>
    /// Initializes a new instance using the specified file system for reading and writing manifests.
    /// </summary>
    public TableOperations(ITableFileSystem fs)
    {
        _fs = fs;
    }

    /// <summary>
    /// Append data files to the table, creating a new snapshot.
    /// </summary>
    public async ValueTask<TableMetadata> AppendFilesAsync(
        TableMetadata current, IReadOnlyList<DataFile> files,
        CancellationToken ct = default)
    {
        if (files.Count == 0)
            throw new ArgumentException("Must append at least one file");

        return await CommitNewFilesAsync(current, files, ManifestContent.Data, "append", ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Append delete files (position or equality) to the table, creating a new snapshot.
    /// </summary>
    public async ValueTask<TableMetadata> AppendDeleteFilesAsync(
        TableMetadata current, IReadOnlyList<DataFile> deleteFiles,
        CancellationToken ct = default)
    {
        if (deleteFiles.Count == 0)
            throw new ArgumentException("Must append at least one delete file");

        foreach (var df in deleteFiles)
        {
            if (df.Content == FileContent.Data)
                throw new ArgumentException($"Expected a delete file but got data content: {df.FilePath}");
        }

        return await CommitNewFilesAsync(current, deleteFiles, ManifestContent.Deletes, "delete", ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Overwrite files: atomically remove old data files and add new ones (copy-on-write).
    /// </summary>
    public async ValueTask<TableMetadata> OverwriteFilesAsync(
        TableMetadata current,
        IReadOnlyList<DataFile> filesToDelete,
        IReadOnlyList<DataFile> filesToAdd,
        CancellationToken ct = default)
    {
        if (filesToDelete.Count == 0 && filesToAdd.Count == 0)
            throw new ArgumentException("Must delete or add at least one file");
        current.ThrowIfNotWritable();
        if (filesToAdd.Count > 0)
            ThrowIfSpecNotWritable(current);
        ThrowIfEncryptedFiles(filesToAdd);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var snapshotId = GenerateSnapshotId();
        var sequenceNumber = current.LastSequenceNumber + 1;
        var metadataDir = $"{current.Location}/metadata";

        var manifestListEntries = new List<ManifestListEntry>();
        var deletedPaths = new HashSet<string>(filesToDelete.Select(FileKey));
        var foundPaths = new HashSet<string>();

        // Read the current manifests and mark the entries of the files being deleted. Nothing is written
        // until the deletes are known to be valid.
        var carried = new List<(ManifestListEntry Manifest, List<ManifestEntry>? Rewritten)>();
        if (current.CurrentSnapshotId is not null)
        {
            var currentSnapshot = current.Snapshots.First(
                s => s.SnapshotId == current.CurrentSnapshotId);

            foreach (var mle in await ManifestIO.ReadSnapshotManifestListForRewriteAsync(_fs, current, currentSnapshot, ct)
                         .ConfigureAwait(false))
            {
                var decoded = await ManifestIO.ReadSnapshotManifestDecodedAsync(_fs, current, currentSnapshot, mle, ct)
                    .ConfigureAwait(false);
                if (!decoded.Entries.Any(e => e.Status != ManifestEntryStatus.Deleted && deletedPaths.Contains(FileKey(e.DataFile))))
                {
                    carried.Add((mle, null));
                    continue;
                }

                // This manifest must be rewritten. Do so only if every field it holds survives: a field the
                // codec does not model would be silently dropped from another engine's metadata.
                if (decoded.UnmodelledFields.Count > 0)
                    throw new NotSupportedException(
                        $"Cannot rewrite manifest '{mle.ManifestPath}': it holds fields this library does not " +
                        $"model ({string.Join(", ", decoded.UnmodelledFields)}), and rewriting it would drop them.");

                // Entries move to a manifest of a new snapshot, so they can no longer inherit: every inherited
                // value is written out, ADDED entries become EXISTING, and entries this manifest already marked
                // DELETED (informational only, for their own snapshot) are not carried.
                var filtered = new List<ManifestEntry>(decoded.Entries.Count);
                foreach (var entry in ManifestCodec.Inherit(decoded, mle))
                {
                    if (entry.Status == ManifestEntryStatus.Deleted)
                        continue;
                    if (deletedPaths.Contains(FileKey(entry.DataFile)))
                    {
                        foundPaths.Add(FileKey(entry.DataFile));
                        filtered.Add(entry with { Status = ManifestEntryStatus.Deleted, SnapshotId = snapshotId });
                    }
                    else
                    {
                        filtered.Add(entry with { Status = ManifestEntryStatus.Existing });
                    }
                }

                carried.Add((mle, filtered));
            }
        }

        // A file to delete that the table no longer holds means the overwrite was planned against a state
        // that has since changed (another writer removed or rewrote it). Committing anyway would record a
        // replacement for rows that are already gone, so refuse; retrying cannot help.
        var missing = deletedPaths.Where(p => !foundPaths.Contains(p)).Select(p => p.Replace(DvKeySeparator, " @ offset ")).ToList();
        if (missing.Count > 0)
            throw new CommitValidationException(
                $"Cannot delete files the table does not contain: {string.Join(", ", missing.Take(5))}" +
                (missing.Count > 5 ? $" and {missing.Count - 5} more" : "") + ".");

        // Carry the manifests forward, rewriting those that held a deleted file.
        foreach (var (mle, rewritten) in carried)
        {
            if (rewritten is null)
            {
                manifestListEntries.Add(mle);
                continue;
            }

            var rewrittenPath = $"{metadataDir}/{Guid.NewGuid():N}.avro";
            var options = ManifestWriteOptions.For(current, mle.PartitionSpecId, mle.Content) with { RewritesExistingManifest = true };
            var length = await ManifestIO.WriteManifestAsync(_fs, rewrittenPath, rewritten, options, ct)
                .ConfigureAwait(false);
            var live = rewritten.Where(e => e.Status != ManifestEntryStatus.Deleted).ToList();
            var gone = rewritten.Where(e => e.Status == ManifestEntryStatus.Deleted).ToList();
            manifestListEntries.Add(mle with
            {
                ManifestPath = rewrittenPath,
                ManifestLength = length,
                SequenceNumber = sequenceNumber,
                MinSequenceNumber = live.Count > 0 ? live.Min(e => e.SequenceNumber ?? sequenceNumber) : sequenceNumber,
                AddedSnapshotId = snapshotId,
                AddedDataFilesCount = 0,
                ExistingDataFilesCount = live.Count,
                DeletedDataFilesCount = gone.Count,
                AddedRowsCount = 0,
                ExistingRowsCount = live.Sum(e => e.DataFile.RecordCount),
                DeletedRowsCount = gone.Sum(e => e.DataFile.RecordCount),
                // Every entry now carries its first_row_id, so nothing inherits one. The partition summaries
                // still bound what is left, so they are kept.
                FirstRowId = null,
            });
        }

        // Write new files manifest
        if (filesToAdd.Count > 0)
        {
            manifestListEntries.Add(await WriteNewManifestAsync(
                current, filesToAdd, ManifestContent.Data, snapshotId, sequenceNumber, metadataDir, ct).ConfigureAwait(false));
        }

        // Write manifest list
        var manifestListPath = $"{metadataDir}/snap-{snapshotId}.avro";
        await WriteManifestListAsync(current, manifestListPath, manifestListEntries, snapshotId, sequenceNumber, ct)
            .ConfigureAwait(false);

        var deletedRecords = filesToDelete.Sum(f => f.RecordCount);
        var addedRecords = filesToAdd.Sum(f => f.RecordCount);
        var prevTotal = CurrentTotalRecords(current);

        var snapshot = new Snapshot(
            SnapshotId: snapshotId,
            ParentSnapshotId: current.CurrentSnapshotId,
            SequenceNumber: sequenceNumber,
            TimestampMs: now,
            ManifestList: manifestListPath,
            Summary: new Dictionary<string, string>
            {
                ["operation"] = "overwrite",
                ["added-data-files"] = filesToAdd.Count.ToString(),
                ["deleted-data-files"] = filesToDelete.Count.ToString(),
                ["added-records"] = addedRecords.ToString(),
                ["deleted-records"] = deletedRecords.ToString(),
                ["total-records"] = (prevTotal - deletedRecords + addedRecords).ToString(),
            },
            SchemaId: current.CurrentSchemaId);

        var snapshots = current.Snapshots.ToList();
        snapshots.Add(snapshot);
        var snapshotLog = current.SnapshotLog.ToList();
        snapshotLog.Add(new SnapshotLogEntry(now, snapshotId));

        // "There is always a main branch reference pointing to the current-snapshot-id": moving one without
        // the other leaves metadata other readers reject.
        return SnapshotRefs.SyncMainBranch(current with
        {
            LastSequenceNumber = sequenceNumber,
            LastUpdatedMs = now,
            CurrentSnapshotId = snapshotId,
            Snapshots = snapshots,
            SnapshotLog = snapshotLog,
        });
    }

    /// <summary>
    /// List active data files (content = Data) in the current snapshot.
    /// </summary>
    public async ValueTask<IReadOnlyList<DataFile>> ListDataFilesAsync(
        TableMetadata metadata, CancellationToken ct = default)
    {
        return await ListFilesByContentAsync(metadata, FileContent.Data, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// List delete files (position deletes + equality deletes) in the current snapshot.
    /// </summary>
    public async ValueTask<IReadOnlyList<DataFile>> ListDeleteFilesAsync(
        TableMetadata metadata, CancellationToken ct = default)
    {
        var pos = await ListFilesByContentAsync(metadata, FileContent.PositionDeletes, ct)
            .ConfigureAwait(false);
        var eq = await ListFilesByContentAsync(metadata, FileContent.EqualityDeletes, ct)
            .ConfigureAwait(false);
        return [.. pos, .. eq];
    }

    /// <summary>
    /// List all active file entries from manifests, grouped as data files and their applicable delete files.
    /// </summary>
    public async ValueTask<(IReadOnlyList<DataFile> DataFiles, IReadOnlyList<DataFile> DeleteFiles)> ScanFilesAsync(
        TableMetadata metadata, CancellationToken ct = default)
    {
        var dataFiles = await ListDataFilesAsync(metadata, ct).ConfigureAwait(false);
        var deleteFiles = await ListDeleteFilesAsync(metadata, ct).ConfigureAwait(false);
        return (dataFiles, deleteFiles);
    }

    // --- Internal helpers ---

    private async ValueTask<TableMetadata> CommitNewFilesAsync(
        TableMetadata current,
        IReadOnlyList<DataFile> files,
        ManifestContent manifestContent,
        string operation,
        CancellationToken ct)
    {
        // Refuse before writing anything: these manifests would be plaintext files in an encrypted table, or
        // files under a partition spec this library cannot apply.
        current.ThrowIfNotWritable();
        ThrowIfSpecNotWritable(current);
        ThrowIfEncryptedFiles(files);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var snapshotId = GenerateSnapshotId();
        var sequenceNumber = current.LastSequenceNumber + 1;
        var metadataDir = $"{current.Location}/metadata";

        // Read (and check) the manifests to carry forward before writing anything.
        var manifestListEntries = new List<ManifestListEntry>();
        if (current.CurrentSnapshotId is not null)
        {
            var currentSnapshot = current.Snapshots.First(
                s => s.SnapshotId == current.CurrentSnapshotId);

            manifestListEntries.AddRange(
                await ManifestIO.ReadSnapshotManifestListForRewriteAsync(_fs, current, currentSnapshot, ct).ConfigureAwait(false));
        }

        // Write the new files' manifest
        manifestListEntries.Add(await WriteNewManifestAsync(
            current, files, manifestContent, snapshotId, sequenceNumber, metadataDir, ct).ConfigureAwait(false));

        // Write manifest list
        var manifestListPath = $"{metadataDir}/snap-{snapshotId}.avro";
        await WriteManifestListAsync(current, manifestListPath, manifestListEntries, snapshotId, sequenceNumber, ct)
            .ConfigureAwait(false);

        // Build summary
        var totalRows = files.Sum(f => f.RecordCount);
        var summary = new Dictionary<string, string> { ["operation"] = operation };

        if (manifestContent == ManifestContent.Data)
        {
            summary["added-data-files"] = files.Count.ToString();
            summary["added-records"] = totalRows.ToString();
            summary["total-records"] = (CurrentTotalRecords(current) + totalRows).ToString();
        }
        else
        {
            summary["added-delete-files"] = files.Count.ToString();
            summary["added-position-deletes"] = files
                .Where(f => f.Content == FileContent.PositionDeletes)
                .Sum(f => f.RecordCount).ToString();
            summary["added-equality-deletes"] = files
                .Where(f => f.Content == FileContent.EqualityDeletes)
                .Sum(f => f.RecordCount).ToString();
        }

        var snapshot = new Snapshot(
            SnapshotId: snapshotId,
            ParentSnapshotId: current.CurrentSnapshotId,
            SequenceNumber: sequenceNumber,
            TimestampMs: now,
            ManifestList: manifestListPath,
            Summary: summary,
            SchemaId: current.CurrentSchemaId);

        var snapshots = current.Snapshots.ToList();
        snapshots.Add(snapshot);
        var snapshotLog = current.SnapshotLog.ToList();
        snapshotLog.Add(new SnapshotLogEntry(now, snapshotId));

        // "There is always a main branch reference pointing to the current-snapshot-id": moving one without
        // the other leaves metadata other readers reject.
        return SnapshotRefs.SyncMainBranch(current with
        {
            LastSequenceNumber = sequenceNumber,
            LastUpdatedMs = now,
            CurrentSnapshotId = snapshotId,
            Snapshots = snapshots,
            SnapshotLog = snapshotLog,
        });
    }

    private async ValueTask<IReadOnlyList<DataFile>> ListFilesByContentAsync(
        TableMetadata metadata, FileContent contentType, CancellationToken ct)
    {
        var snapshot = metadata.CurrentSnapshotOrNull();
        metadata.ThrowIfDataInaccessible(snapshot);
        if (snapshot is null)
            return [];

        var manifestList = await ReadManifestListAsync(metadata, snapshot, ct).ConfigureAwait(false);
        var result = new List<DataFile>();

        foreach (var mle in manifestList)
        {
            var entries = await ReadManifestAsync(metadata, snapshot, mle, ct).ConfigureAwait(false);
            foreach (var entry in entries)
            {
                if (entry.Status != ManifestEntryStatus.Deleted && entry.DataFile.Content == contentType)
                    result.Add(entry.DataFile);
            }
        }

        return result;
    }

    private ValueTask<List<ManifestListEntry>> ReadManifestListAsync(
        TableMetadata metadata, Snapshot snapshot, CancellationToken ct) =>
        ManifestIO.ReadSnapshotManifestListAsync(_fs, metadata, snapshot, ct);

    private ValueTask<List<ManifestEntry>> ReadManifestAsync(
        TableMetadata metadata, Snapshot snapshot, ManifestListEntry manifest, CancellationToken ct) =>
        ManifestIO.ReadSnapshotManifestAsync(_fs, metadata, snapshot, manifest, ct);

    /// <summary>
    /// Writes a manifest of new files in the table's default spec, and returns its manifest-list entry. The
    /// entries are ADDED with their sequence numbers left to inheritance, as Iceberg writers do, so they take
    /// the sequence number of the snapshot that commits them.
    /// </summary>
    private async ValueTask<ManifestListEntry> WriteNewManifestAsync(
        TableMetadata current, IReadOnlyList<DataFile> files, ManifestContent content,
        long snapshotId, long sequenceNumber, string metadataDir, CancellationToken ct)
    {
        var entries = files.Select(f => new ManifestEntry
        {
            Status = ManifestEntryStatus.Added,
            SnapshotId = snapshotId,
            DataFile = f,
        }).ToList();

        var options = ManifestWriteOptions.For(current, current.DefaultSpecId, content);
        var manifestPath = $"{metadataDir}/{Guid.NewGuid():N}.avro";
        var manifestLength = await ManifestIO.WriteManifestAsync(_fs, manifestPath, entries, options, ct)
            .ConfigureAwait(false);

        return new ManifestListEntry
        {
            ManifestPath = manifestPath,
            ManifestLength = manifestLength,
            PartitionSpecId = current.DefaultSpecId,
            Content = content,
            SequenceNumber = sequenceNumber,
            MinSequenceNumber = sequenceNumber,
            AddedSnapshotId = snapshotId,
            AddedDataFilesCount = files.Count,
            ExistingDataFilesCount = 0,
            DeletedDataFilesCount = 0,
            AddedRowsCount = files.Sum(f => f.RecordCount),
            ExistingRowsCount = 0,
            DeletedRowsCount = 0,
            Partitions = PartitionSummaries.Build(options.ResolvePartitionType(entries), files, encodeBound: null),
        };
    }

    private async ValueTask WriteManifestListAsync(
        TableMetadata current, string path, IReadOnlyList<ManifestListEntry> entries,
        long snapshotId, long sequenceNumber, CancellationToken ct)
    {
        var options = new ManifestListWriteOptions
        {
            FormatVersion = current.FormatVersion,
            SnapshotId = snapshotId,
            ParentSnapshotId = current.CurrentSnapshotId,
            SequenceNumber = sequenceNumber,
        };
        await ManifestIO.WriteManifestListAsync(_fs, path, entries, options, ct).ConfigureAwait(false);
    }

    private static long CurrentTotalRecords(TableMetadata metadata)
    {
        if (metadata.CurrentSnapshotId is null)
            return 0;

        var snapshot = metadata.Snapshots.First(
            s => s.SnapshotId == metadata.CurrentSnapshotId);

        return snapshot.Summary.TryGetValue("total-records", out var val)
            ? long.Parse(val)
            : 0;
    }

    private static long GenerateSnapshotId() => SnapshotIdFactory.Generate();

    private const string DvKeySeparator = "\u0000";

    /// <summary>
    /// How a commit names a file to remove. Several deletion vectors can share one Puffin file, each at its
    /// own offset, so a DV is its path and content offset; any other file is its path.
    /// </summary>
    private static string FileKey(DataFile file) =>
        file.ContentOffset is { } offset
            ? file.FilePath + DvKeySeparator + offset.ToString(System.Globalization.CultureInfo.InvariantCulture)
            : file.FilePath;

    /// <summary>
    /// A reader must accept a partition spec with a transform it does not know, but the spec forbids
    /// committing new files under one: their partition values cannot be derived or checked.
    /// </summary>
    private static void ThrowIfSpecNotWritable(TableMetadata table)
    {
        var spec = table.PartitionSpecs.FirstOrDefault(s => s.SpecId == table.DefaultSpecId);
        var unknown = spec?.Fields.FirstOrDefault(f => f.Transform is UnknownTransform || f.SourceIds is { Count: > 1 });
        if (unknown is not null)
            throw new UnsupportedTableFeatureException(
                $"Cannot add files to table {table.TableUuid}: its default partition spec {table.DefaultSpecId} has " +
                $"field '{unknown.Name}' with transform '{TransformName(unknown.Transform)}', which this " +
                "library does not know.");
    }

    /// <summary>
    /// A file with key metadata is encrypted. Listing it in a plaintext manifest would publish an encrypted
    /// file the table cannot describe, and the next scan would refuse the table, so refuse it now.
    /// </summary>
    private static void ThrowIfEncryptedFiles(IReadOnlyList<DataFile> files)
    {
        if (files.FirstOrDefault(f => f.KeyMetadata is not null) is { } encrypted)
            throw new UnsupportedTableFeatureException(
                $"Cannot add '{encrypted.FilePath}': it is encrypted (it has key metadata), and table encryption " +
                "is not supported.");
    }

    private static string TransformName(Transform transform) =>
        transform is UnknownTransform u ? u.Name : transform.GetType().Name;
}
