// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.IO;

namespace EngineeredWood.Iceberg;

/// <summary>
/// Builds and executes a snapshot expiration policy, removing old snapshots and optionally
/// deleting their orphaned manifest files from storage.
/// </summary>
public sealed class ExpireSnapshots
{
    private long? _expireBefore;
    private int? _retainLast;
    private readonly HashSet<long> _idsToExpire = [];

    /// <summary>
    /// Marks snapshots older than the specified timestamp for expiration.
    /// </summary>
    public ExpireSnapshots ExpireOlderThan(long timestampMs)
    {
        _expireBefore = timestampMs;
        return this;
    }

    /// <summary>
    /// Marks a specific snapshot for expiration by its ID.
    /// </summary>
    public ExpireSnapshots ExpireSnapshotId(long snapshotId)
    {
        _idsToExpire.Add(snapshotId);
        return this;
    }

    /// <summary>
    /// Retains the most recent <paramref name="count"/> snapshots regardless of age.
    /// </summary>
    public ExpireSnapshots RetainLast(int count)
    {
        _retainLast = count;
        return this;
    }

    /// <summary>
    /// <para>Applies the expiration policy, returning updated metadata and the files no longer referenced.</para>
    /// <para>When a resolver is provided, those files are deleted from storage, but only those under the table's own
    /// location: a manifest the metadata names elsewhere (another table's, after a migration or a shallow copy)
    /// may still be another table's, so it is left in place and reported in
    /// <see cref="ExpireResult.FilesOutsideTable"/>, as Delta's VACUUM touches only files under its table.</para>
    /// </summary>
    public async ValueTask<ExpireResult> ApplyAsync(
        TableMetadata metadata, StorageResolver? resolver = null,
        CancellationToken ct = default)
    {
        ITableFileSystem? fs = resolver is null ? null : new LocationFileSystem(resolver);
        // Expiry rewrites the table's metadata and deletes its files, so it is a write: refuse an encrypted
        // table up front, whatever would be expired and whether or not a file system is given.
        metadata.ThrowIfNotWritable();

        var allSnapshots = metadata.Snapshots.ToList();

        // Determine snapshots that must be retained
        var retained = new HashSet<long>();

        // Always keep the current snapshot
        if (metadata.CurrentSnapshotId is not null)
            retained.Add(metadata.CurrentSnapshotId.Value);

        // Retain last N snapshots by timestamp
        if (_retainLast is not null)
        {
            // Sequence number breaks ties: snapshots committed within one clock tick
            // share a timestamp, and ordering by timestamp alone would retain the
            // oldest of such a group instead of the newest.
            foreach (var snap in allSnapshots
                .OrderByDescending(s => s.TimestampMs)
                .ThenByDescending(s => s.SequenceNumber)
                .Take(_retainLast.Value))
            {
                retained.Add(snap.SnapshotId);
            }
        }

        // Determine which snapshots to expire
        var expired = allSnapshots.Where(s =>
        {
            if (retained.Contains(s.SnapshotId))
                return false;

            if (_idsToExpire.Contains(s.SnapshotId))
                return true;

            if (_expireBefore is not null && s.TimestampMs < _expireBefore.Value)
                return true;

            return false;
        }).ToList();

        if (expired.Count == 0)
            return new ExpireResult(metadata, []);

        var expiredIds = new HashSet<long>(expired.Select(s => s.SnapshotId));
        var keptSnapshots = allSnapshots.Where(s => !expiredIds.Contains(s.SnapshotId)).ToList();

        // Collect files referenced by kept snapshots
        var keptManifestLists = new HashSet<string>(keptSnapshots.Select(s => s.ManifestList));
        var keptManifestPaths = new HashSet<string>();

        if (fs is not null)
        {
            // A kept snapshot's manifest list must be readable: without it there is no knowing which
            // manifests it still needs, and deleting on that basis could remove them.
            foreach (var snap in keptSnapshots)
            {
                var entries = await ManifestIO.ReadSnapshotManifestListAsync(fs, metadata, snap, ct)
                    .ConfigureAwait(false);
                ManifestIO.ThrowIfEncryptedManifest(entries, snap, "its snapshots cannot be expired");
                foreach (var e in entries)
                    keptManifestPaths.Add(e.ManifestPath);
            }
        }

        // Delete files only referenced by expired snapshots
        var deletedFiles = new List<string>();

        foreach (var snap in expired)
        {
            if (!keptManifestLists.Contains(snap.ManifestList))
            {
                deletedFiles.Add(snap.ManifestList);

                if (fs is not null && await fs.ExistsAsync(snap.ManifestList, ct).ConfigureAwait(false))
                {
                    metadata.ThrowIfDataInaccessible(snap);
                    var entries = await ManifestIO.ReadManifestListAsync(fs, snap.ManifestList, ct)
                        .ConfigureAwait(false);
                    ManifestIO.ThrowIfEncryptedManifest(entries, snap, "its snapshots cannot be expired");
                    foreach (var e in entries)
                    {
                        if (!keptManifestPaths.Contains(e.ManifestPath))
                            deletedFiles.Add(e.ManifestPath);
                    }
                }
            }
        }

        // Perform deletions, under the table's location only.
        var outside = new List<string>();
        if (fs is not null)
        {
            var tableLocation = StorageLocation.Parse(metadata.Location);
            foreach (var file in deletedFiles)
            {
                if (!StorageLocation.Parse(file).IsUnder(tableLocation))
                {
                    outside.Add(file);
                    continue;
                }

                await fs.DeleteAsync(file, ct).ConfigureAwait(false);
            }

            deletedFiles.RemoveAll(outside.Contains);
        }

        // Fix parent references in kept snapshots
        var keptIds = new HashSet<long>(keptSnapshots.Select(s => s.SnapshotId));
        keptSnapshots = keptSnapshots.Select(s =>
            s.ParentSnapshotId is not null && !keptIds.Contains(s.ParentSnapshotId.Value)
                ? s with { ParentSnapshotId = null }
                : s
        ).ToList();

        var keptSnapshotLog = metadata.SnapshotLog
            .Where(e => keptIds.Contains(e.SnapshotId))
            .ToList();

        // Statistics computed for an expired snapshot describe a snapshot that no longer exists, so their
        // entries go with it, as Java's RemoveSnapshots drops them. Their files are not deleted yet: the
        // Puffin- and statistics-aware cleanup is Phase 4 (doc/iceberg-v3-plan.md, #957).
        var updatedMetadata = metadata with
        {
            Snapshots = keptSnapshots,
            SnapshotLog = keptSnapshotLog,
            Statistics = metadata.Statistics?.Where(s => keptIds.Contains(s.SnapshotId)).ToList(),
            PartitionStatistics = metadata.PartitionStatistics?.Where(s => keptIds.Contains(s.SnapshotId)).ToList(),
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };

        return new ExpireResult(updatedMetadata, deletedFiles) { FilesOutsideTable = outside };
    }
}

/// <summary>
/// The result of a snapshot expiration operation, containing the updated metadata and paths of deleted files
/// (without a resolver, the files that would be deleted).
/// </summary>
public sealed record ExpireResult(TableMetadata Metadata, IReadOnlyList<string> DeletedFiles)
{
    /// <summary>
    /// Files no longer referenced that lie outside the table's location, and so were left in place.
    /// </summary>
    public IReadOnlyList<string> FilesOutsideTable { get; init; } = [];
}
