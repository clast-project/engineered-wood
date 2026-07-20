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

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var snapshotId = GenerateSnapshotId();
        var sequenceNumber = current.LastSequenceNumber + 1;
        var metadataDir = $"{current.Location}/metadata";

        var manifestListEntries = new List<ManifestListEntry>();
        var deletedPaths = new HashSet<string>(filesToDelete.Select(f => f.FilePath));

        // Carry forward existing manifests, rewriting entries that reference deleted files
        if (current.CurrentSnapshotId is not null)
        {
            var currentSnapshot = current.Snapshots.First(
                s => s.SnapshotId == current.CurrentSnapshotId);

            if (await _fs.ExistsAsync(currentSnapshot.ManifestList, ct).ConfigureAwait(false))
            {
                var existing = await ManifestIO.ReadManifestListAsync(_fs, currentSnapshot.ManifestList, ct)
                    .ConfigureAwait(false);

                foreach (var mle in existing)
                {
                    if (!await _fs.ExistsAsync(mle.ManifestPath, ct).ConfigureAwait(false))
                        continue;

                    var entries = await ManifestIO.ReadManifestAsync(_fs, mle.ManifestPath, ct)
                        .ConfigureAwait(false);
                    var rewritten = false;
                    var filtered = new List<ManifestEntry>();

                    foreach (var entry in entries)
                    {
                        if (entry.Status != ManifestEntryStatus.Deleted &&
                            deletedPaths.Contains(entry.DataFile.FilePath))
                        {
                            filtered.Add(entry with { Status = ManifestEntryStatus.Deleted, SnapshotId = snapshotId });
                            rewritten = true;
                        }
                        else
                        {
                            filtered.Add(entry);
                        }
                    }

                    if (rewritten)
                    {
                        var rewrittenPath = $"{metadataDir}/{Guid.NewGuid():N}.avro";
                        var length = await ManifestIO.WriteManifestAsync(_fs, rewrittenPath, filtered, ct)
                            .ConfigureAwait(false);
                        manifestListEntries.Add(mle with
                        {
                            ManifestPath = rewrittenPath,
                            ManifestLength = length,
                            DeletedDataFilesCount = mle.DeletedDataFilesCount + filtered.Count(e => e.Status == ManifestEntryStatus.Deleted),
                        });
                    }
                    else
                    {
                        manifestListEntries.Add(mle);
                    }
                }
            }
        }

        // Write new files manifest
        if (filesToAdd.Count > 0)
        {
            var newEntries = filesToAdd.Select(f => new ManifestEntry
            {
                Status = ManifestEntryStatus.Added,
                SnapshotId = snapshotId,
                SequenceNumber = sequenceNumber,
                FileSequenceNumber = sequenceNumber,
                DataFile = f,
            }).ToList();

            var manifestPath = $"{metadataDir}/{Guid.NewGuid():N}.avro";
            var manifestLength = await ManifestIO.WriteManifestAsync(_fs, manifestPath, newEntries, ct)
                .ConfigureAwait(false);

            manifestListEntries.Add(new ManifestListEntry
            {
                ManifestPath = manifestPath,
                ManifestLength = manifestLength,
                PartitionSpecId = current.DefaultSpecId,
                Content = ManifestContent.Data,
                SequenceNumber = sequenceNumber,
                MinSequenceNumber = sequenceNumber,
                AddedSnapshotId = snapshotId,
                AddedDataFilesCount = filesToAdd.Count,
                AddedRowsCount = filesToAdd.Sum(f => f.RecordCount),
            });
        }

        // Write manifest list
        var manifestListPath = $"{metadataDir}/snap-{snapshotId}.avro";
        await ManifestIO.WriteManifestListAsync(_fs, manifestListPath, manifestListEntries, ct)
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

        return current with
        {
            LastSequenceNumber = sequenceNumber,
            LastUpdatedMs = now,
            CurrentSnapshotId = snapshotId,
            Snapshots = snapshots,
            SnapshotLog = snapshotLog,
        };
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
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var snapshotId = GenerateSnapshotId();
        var sequenceNumber = current.LastSequenceNumber + 1;
        var metadataDir = $"{current.Location}/metadata";

        // Create manifest entries
        var entries = files.Select(f => new ManifestEntry
        {
            Status = ManifestEntryStatus.Added,
            SnapshotId = snapshotId,
            SequenceNumber = sequenceNumber,
            FileSequenceNumber = sequenceNumber,
            DataFile = f,
        }).ToList();

        // Write manifest
        var manifestPath = $"{metadataDir}/{Guid.NewGuid():N}.avro";
        var manifestLength = await ManifestIO.WriteManifestAsync(_fs, manifestPath, entries, ct)
            .ConfigureAwait(false);

        // Build manifest list
        var manifestListEntries = new List<ManifestListEntry>();

        // Carry forward from current snapshot
        if (current.CurrentSnapshotId is not null)
        {
            var currentSnapshot = current.Snapshots.First(
                s => s.SnapshotId == current.CurrentSnapshotId);

            if (await _fs.ExistsAsync(currentSnapshot.ManifestList, ct).ConfigureAwait(false))
            {
                var existing = await ManifestIO.ReadManifestListAsync(_fs, currentSnapshot.ManifestList, ct)
                    .ConfigureAwait(false);
                manifestListEntries.AddRange(existing);
            }
        }

        manifestListEntries.Add(new ManifestListEntry
        {
            ManifestPath = manifestPath,
            ManifestLength = manifestLength,
            PartitionSpecId = current.DefaultSpecId,
            Content = manifestContent,
            SequenceNumber = sequenceNumber,
            MinSequenceNumber = sequenceNumber,
            AddedSnapshotId = snapshotId,
            AddedDataFilesCount = manifestContent == ManifestContent.Data ? files.Count : 0,
            DeletedDataFilesCount = manifestContent == ManifestContent.Deletes ? files.Count : 0,
            AddedRowsCount = files.Sum(f => f.RecordCount),
        });

        // Write manifest list
        var manifestListPath = $"{metadataDir}/snap-{snapshotId}.avro";
        await ManifestIO.WriteManifestListAsync(_fs, manifestListPath, manifestListEntries, ct)
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

        return current with
        {
            LastSequenceNumber = sequenceNumber,
            LastUpdatedMs = now,
            CurrentSnapshotId = snapshotId,
            Snapshots = snapshots,
            SnapshotLog = snapshotLog,
        };
    }

    private async ValueTask<IReadOnlyList<DataFile>> ListFilesByContentAsync(
        TableMetadata metadata, FileContent contentType, CancellationToken ct)
    {
        if (metadata.CurrentSnapshotId is null)
            return [];

        var snapshot = metadata.Snapshots.First(
            s => s.SnapshotId == metadata.CurrentSnapshotId);

        if (!await _fs.ExistsAsync(snapshot.ManifestList, ct).ConfigureAwait(false))
            return [];

        var manifestList = await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList, ct)
            .ConfigureAwait(false);
        var result = new List<DataFile>();

        foreach (var mle in manifestList)
        {
            if (!await _fs.ExistsAsync(mle.ManifestPath, ct).ConfigureAwait(false))
                continue;

            var entries = await ManifestIO.ReadManifestAsync(_fs, mle.ManifestPath, ct)
                .ConfigureAwait(false);
            foreach (var entry in entries)
            {
                if (entry.Status != ManifestEntryStatus.Deleted && entry.DataFile.Content == contentType)
                    result.Add(entry.DataFile);
            }
        }

        return result;
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

    private static long GenerateSnapshotId()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000
#if NET6_0_OR_GREATER
            + Random.Shared.Next(1000);
#else
            + new Random().Next(1000);
#endif
    }
}
