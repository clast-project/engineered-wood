// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Snapshot;

/// <summary>
/// Builds a <see cref="Snapshot"/> by replaying transaction log entries
/// and applying action reconciliation rules.
/// </summary>
public sealed class SnapshotBuilder
{
    private MetadataAction? _metadata;
    private ProtocolAction? _protocol;
    private readonly Dictionary<string, AddFile> _activeFiles = new();
    private readonly Dictionary<string, TransactionId> _appTransactions = new();
    private readonly Dictionary<string, DomainMetadata> _domainMetadata = new();
    private readonly Dictionary<string, RemoveFile> _tombstones = new();
    private long _version = -1;
    private long? _inCommitTimestamp;

    /// <summary>
    /// Builds a snapshot by first loading from the latest checkpoint (if available),
    /// then replaying only the commits after the checkpoint version up to the target.
    /// </summary>
    public static async ValueTask<Snapshot> BuildAsync(
        TransactionLog log,
        CheckpointReader? checkpointReader = null,
        long? atVersion = null,
        CancellationToken cancellationToken = default)
    {
        var builder = new SnapshotBuilder();

        long targetVersion = atVersion ??
            await log.GetLatestVersionAsync(cancellationToken).ConfigureAwait(false);

        if (targetVersion < 0)
            throw new DeltaFormatException("Table has no commits.");

        // Try to bootstrap from a checkpoint
        long replayFrom = 0;
        if (checkpointReader is not null)
        {
            var lastCheckpoint = await checkpointReader.ReadLastCheckpointAsync(cancellationToken)
                .ConfigureAwait(false);

            if (lastCheckpoint is not null && lastCheckpoint.Version <= targetVersion)
            {
                try
                {
                    var checkpointActions = await checkpointReader.ReadCheckpointAsync(
                        lastCheckpoint, cancellationToken).ConfigureAwait(false);
                    builder.ApplyCommit(lastCheckpoint.Version, checkpointActions);
                    replayFrom = lastCheckpoint.Version + 1;
                }
                catch (Exception)
                {
                    // If checkpoint reading fails, fall back to full replay
                    builder = new SnapshotBuilder();
                    replayFrom = 0;
                }
            }
        }

        // Check for log compaction files that cover subranges
        LogCompaction? logCompaction = null;
        IReadOnlyList<(long Start, long End, string Path)> compactedFiles = [];

        try
        {
            logCompaction = new LogCompaction(log.FileSystem, log);
            compactedFiles = await logCompaction.ListCompactedFilesAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            // If listing fails, fall back to reading individual commits
            compactedFiles = [];
        }

        // Find the best compacted file covering [replayFrom..targetVersion]
        (long Start, long End, string Path)? bestCompacted = null;
        foreach (var cf in compactedFiles)
        {
            if (cf.Start >= replayFrom && cf.End <= targetVersion)
            {
                if (bestCompacted is null ||
                    (cf.End - cf.Start) > (bestCompacted.Value.End - bestCompacted.Value.Start))
                    bestCompacted = cf;
            }
        }

        // Use compacted file if available
        if (bestCompacted is not null && logCompaction is not null)
        {
            // First, read any commits before the compacted range
            for (long v = replayFrom; v < bestCompacted.Value.Start; v++)
            {
                try
                {
                    var preActions = await log.ReadCommitAsync(v, cancellationToken)
                        .ConfigureAwait(false);
                    builder.ApplyCommit(v, preActions);
                }
                catch { /* Skip missing commits */ }
            }

            // Apply the compacted file
            var compactedActions = await logCompaction.ReadCompactedAsync(
                bestCompacted.Value.Path, cancellationToken).ConfigureAwait(false);
            builder.ApplyCommit(bestCompacted.Value.End, compactedActions);
            replayFrom = bestCompacted.Value.End + 1;
        }

        // Read remaining commits after the compacted range
        var versions = new List<long>();
        await foreach (long v in log.ListVersionsAsync(replayFrom, cancellationToken)
            .ConfigureAwait(false))
        {
            if (v <= targetVersion)
                versions.Add(v);
        }

        versions.Sort();

        // Read commits concurrently for performance
        var commitTasks = versions.Select(v =>
            new { Version = v, Task = log.ReadCommitAsync(v, cancellationToken) })
            .ToList();

        var commits = new (long Version, IReadOnlyList<DeltaAction> Actions)[commitTasks.Count];
        for (int i = 0; i < commitTasks.Count; i++)
        {
            commits[i] = (commitTasks[i].Version,
                await commitTasks[i].Task.ConfigureAwait(false));
        }

        // Apply in version order
        foreach (var (version, actions) in commits.OrderBy(c => c.Version))
        {
            builder.ApplyCommit(version, actions);
        }

        return builder.Build();
    }

    /// <summary>
    /// Incrementally updates an existing snapshot by replaying only
    /// the commits newer than the snapshot's version.
    /// </summary>
    public static async ValueTask<Snapshot> UpdateAsync(
        Snapshot current,
        TransactionLog log,
        CancellationToken cancellationToken = default)
    {
        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion <= current.Version)
            return current;

        var builder = SnapshotBuilder.FromSnapshot(current);

        var versions = new List<long>();
        await foreach (long v in log.ListVersionsAsync(current.Version + 1, cancellationToken)
            .ConfigureAwait(false))
        {
            if (v <= latestVersion)
                versions.Add(v);
        }

        versions.Sort();

        foreach (long v in versions)
        {
            var actions = await log.ReadCommitAsync(v, cancellationToken)
                .ConfigureAwait(false);
            builder.ApplyCommit(v, actions);
        }

        return builder.Build();
    }

    /// <summary>
    /// Creates a builder pre-populated from an existing snapshot.
    /// </summary>
    private static SnapshotBuilder FromSnapshot(Snapshot snapshot)
    {
        var builder = new SnapshotBuilder
        {
            _metadata = snapshot.Metadata,
            _protocol = snapshot.Protocol,
            _version = snapshot.Version,
            _inCommitTimestamp = snapshot.InCommitTimestamp,
        };

        foreach (var kvp in snapshot.ActiveFiles)
            builder._activeFiles[kvp.Key] = kvp.Value;

        foreach (var kvp in snapshot.AppTransactions)
            builder._appTransactions[kvp.Key] = kvp.Value;

        foreach (var kvp in snapshot.DomainMetadata)
            builder._domainMetadata[kvp.Key] = kvp.Value;

        foreach (var kvp in snapshot.Tombstones)
            builder._tombstones[kvp.Key] = kvp.Value;

        return builder;
    }

    /// <summary>
    /// Applies a single commit's actions to the builder state.
    /// </summary>
    internal void ApplyCommit(long version, IReadOnlyList<DeltaAction> actions)
    {
        _version = version;
        _inCommitTimestamp = null; // Reset for this version

        foreach (var action in actions)
        {
            switch (action)
            {
                case MetadataAction metadata:
                    _metadata = metadata;
                    break;

                case ProtocolAction protocol:
                    _protocol = protocol;
                    break;

                case AddFile add:
                    _activeFiles[add.ReconciliationKey] = add;
                    _tombstones.Remove(add.ReconciliationKey);
                    break;

                case RemoveFile remove:
                    _activeFiles.Remove(remove.ReconciliationKey);
                    _tombstones[remove.ReconciliationKey] = remove;
                    break;

                case TransactionId txn:
                    _appTransactions[txn.AppId] = txn;
                    break;

                case DomainMetadata dm:
                    if (dm.Removed)
                        _domainMetadata.Remove(dm.Domain);
                    else
                        _domainMetadata[dm.Domain] = dm;
                    break;

                case CommitInfo ci:
                    _inCommitTimestamp = Log.InCommitTimestamp.GetTimestamp(ci);
                    break;
            }
        }
    }

    /// <summary>
    /// Builds the final immutable <see cref="Snapshot"/> from the current state.
    /// </summary>
    public Snapshot Build()
    {
        if (_metadata is null)
            throw new DeltaFormatException("Table has no metadata action.");
        if (_protocol is null)
            throw new DeltaFormatException("Table has no protocol action.");

        var deltaSchema = DeltaSchemaSerializer.Parse(_metadata.SchemaString);
        var arrowSchema = SchemaConverter.ToArrowSchema(deltaSchema);

        var activeFiles = new Dictionary<string, AddFile>(_activeFiles);

        return new Snapshot
        {
            Version = _version,
            Metadata = _metadata,
            Protocol = _protocol,
            Schema = deltaSchema,
            ArrowSchema = arrowSchema,
            ActiveFiles = activeFiles,
            AppTransactions = new Dictionary<string, TransactionId>(_appTransactions),
            DomainMetadata = new Dictionary<string, DomainMetadata>(_domainMetadata),
            Tombstones = new Dictionary<string, RemoveFile>(_tombstones),
            InCommitTimestamp = _inCommitTimestamp,
            // The delta.rowTracking domainMetadata is the spec source of truth for the high-water mark;
            // the active-file derivation alone under-counts after removes (a later writer could then
            // reassign used row ids). Take the max so either source protects the invariant.
            RowIdHighWaterMark = System.Math.Max(
                RowTracking.RowTrackingConfig.ComputeHighWaterMark(activeFiles),
                (RowTracking.RowTrackingConfig.TryReadHighWaterMark(_domainMetadata) ?? -1) + 1),
        };
    }
}
