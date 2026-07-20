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
    /// Applies the expiration policy, returning updated metadata and the list of deleted files.
    /// When a file system is provided, orphaned manifest files are deleted from storage.
    /// </summary>
    public async ValueTask<ExpireResult> ApplyAsync(
        TableMetadata metadata, ITableFileSystem? fs = null,
        CancellationToken ct = default)
    {
        var allSnapshots = metadata.Snapshots.ToList();

        // Determine snapshots that must be retained
        var retained = new HashSet<long>();

        // Always keep the current snapshot
        if (metadata.CurrentSnapshotId is not null)
            retained.Add(metadata.CurrentSnapshotId.Value);

        // Retain last N snapshots by timestamp
        if (_retainLast is not null)
        {
            foreach (var snap in allSnapshots
                .OrderByDescending(s => s.TimestampMs)
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
            foreach (var snap in keptSnapshots)
            {
                if (await fs.ExistsAsync(snap.ManifestList, ct).ConfigureAwait(false))
                {
                    var entries = await ManifestIO.ReadManifestListAsync(fs, snap.ManifestList, ct)
                        .ConfigureAwait(false);
                    foreach (var e in entries)
                        keptManifestPaths.Add(e.ManifestPath);
                }
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
                    var entries = await ManifestIO.ReadManifestListAsync(fs, snap.ManifestList, ct)
                        .ConfigureAwait(false);
                    foreach (var e in entries)
                    {
                        if (!keptManifestPaths.Contains(e.ManifestPath))
                            deletedFiles.Add(e.ManifestPath);
                    }
                }
            }
        }

        // Perform deletions
        if (fs is not null)
        {
            foreach (var file in deletedFiles)
            {
                await fs.DeleteAsync(file, ct).ConfigureAwait(false);
            }
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

        var updatedMetadata = metadata with
        {
            Snapshots = keptSnapshots,
            SnapshotLog = keptSnapshotLog,
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };

        return new ExpireResult(updatedMetadata, deletedFiles);
    }
}

/// <summary>
/// The result of a snapshot expiration operation, containing the updated metadata and paths of deleted files.
/// </summary>
public sealed record ExpireResult(TableMetadata Metadata, IReadOnlyList<string> DeletedFiles);
