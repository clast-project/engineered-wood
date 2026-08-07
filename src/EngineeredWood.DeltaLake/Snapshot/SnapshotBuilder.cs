// Copyright (c) clast-project. All rights reserved.
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

        // One classified pass over _delta_log answers everything below: the target version, the newest
        // checkpoint, the compaction files, and the commits to replay. They used to be four separate
        // walks of the same directory, which also let them disagree about a concurrent commit.
        var listing = await log.ReadListingAsync(cancellationToken).ConfigureAwait(false);

        long targetVersion = atVersion ?? listing.LatestVersion;

        if (targetVersion < 0)
            throw new DeltaTableNotFoundException("Table has no commits.");

        // Try to bootstrap from a checkpoint
        long replayFrom = 0;
        if (checkpointReader is not null)
        {
            // _last_checkpoint is an advisory hint, so it can be absent, unusable, or STALE — naming a
            // checkpoint that log cleanup has since deleted. It can also sit above targetVersion on a
            // time-travel read. Each of those falls through to listing the log, which is the truth the
            // hint summarizes. Without that fallback, a table whose pre-checkpoint commits were cleaned
            // up replays from a log that no longer starts at 0 and silently loses their files.
            CheckpointReader reader = checkpointReader;
            var hint = await reader.ReadLastCheckpointAsync(cancellationToken)
                .ConfigureAwait(false);
            if (hint is not null && hint.Version > targetVersion)
                hint = null;

            if (hint is null || !await TryBootstrapAsync(hint).ConfigureAwait(false))
            {
                var listed = CheckpointReader.SelectLatestCheckpoint(listing, targetVersion);

                // Skip the re-read only when listing found the SAME checkpoint that already failed —
                // same version AND same file. Comparing versions alone was too coarse: a hint whose
                // path is wrong names the version correctly, so the listing's candidate (which has
                // the right path) was discarded as a duplicate and the table failed to open at all.
                if (listed is not null && !SameCandidate(listed, hint))
                    await TryBootstrapAsync(listed).ConfigureAwait(false);
            }

            async ValueTask<bool> TryBootstrapAsync(LastCheckpointInfo info)
            {
                try
                {
                    var checkpointActions = await reader.ReadCheckpointAsync(
                        info, cancellationToken).ConfigureAwait(false);
                    builder.ApplyCommit(info.Version, checkpointActions);
                    replayFrom = info.Version + 1;
                    return true;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception)
                {
                    // Discard whatever was applied before the failure — a half-read checkpoint is not a
                    // valid starting state — and let the caller try the next candidate or full replay.
                    builder = new SnapshotBuilder();
                    replayFrom = 0;
                    return false;
                }
            }
        }

        // Replay has to cover every version from here to the target. Anything it cannot apply leaves a
        // hole, and a snapshot with a hole is not a version of this table — it is missing whatever those
        // commits added, removed or renamed, with nothing in the result to say so. Recorded as we go and
        // reported at the end, so the message names the first gap rather than the first symptom.
        long firstNeeded = replayFrom;
        long nextNeeded = replayFrom;
        long? firstMissing = null;

        // Find the best compaction file covering [replayFrom..targetVersion]
        (long Start, long End, string Path)? bestCompacted = null;
        foreach (var cf in listing.CompactedRanges)
        {
            if (cf.Start >= replayFrom && cf.End <= targetVersion)
            {
                if (bestCompacted is null ||
                    (cf.End - cf.Start) > (bestCompacted.Value.End - bestCompacted.Value.Start))
                    bestCompacted = cf;
            }
        }

        // Use compacted file if available
        if (bestCompacted is not null)
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
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch
                {
                    // Missing or unreadable — either way this version does not reach the snapshot.
                    firstMissing ??= v;
                }
            }

            // Apply the compacted file
            var compactedActions = await new LogCompaction(log.FileSystem, log)
                .ReadCompactedAsync(bestCompacted.Value.Path, cancellationToken)
                .ConfigureAwait(false);
            builder.ApplyCommit(bestCompacted.Value.End, compactedActions);
            replayFrom = bestCompacted.Value.End + 1;
            nextNeeded = replayFrom;
        }

        // Read remaining commits after the compacted range. Already ascending from the one listing.
        var versions = listing.CommitsInRange(replayFrom, targetVersion).ToList();

        // Reads run ahead of application so a long tail is not one serial round-trip per commit — but the
        // look-ahead is BOUNDED. Starting every read at once put one request per commit in flight
        // simultaneously, which on a table whose log cleanup has not run is thousands against the object
        // store (the throttling response to which is a 503, not faster reads), and held every commit's
        // decoded actions in memory until the last one landed. A window makes both proportional to the
        // window rather than to the tail. It never binds in the normal case: the tail is the commits since
        // the last checkpoint, which delta.checkpointInterval keeps at ~10.
        const int ReplayLookAhead = 32;

        // AsTask because these are held in a queue rather than awaited where produced — a ValueTask is
        // only valid to consume once, and storing one is exactly the pattern that makes that hard to see.
        var inFlight = new Queue<(long Version, Task<IReadOnlyList<DeltaAction>> Read)>(ReplayLookAhead);
        int nextToStart = 0;

        void StartNext()
        {
            long v = versions[nextToStart++];
            inFlight.Enqueue((v, log.ReadCommitAsync(v, cancellationToken).AsTask()));
        }

        while (nextToStart < versions.Count && inFlight.Count < ReplayLookAhead)
            StartNext();

        // Applied in dequeue order, which is `versions` order, which the one listing already sorted.
        while (inFlight.Count > 0)
        {
            var (version, read) = inFlight.Dequeue();
            var actions = await read.ConfigureAwait(false);

            if (nextToStart < versions.Count)
                StartNext();

            if (version != nextNeeded)
                firstMissing ??= nextNeeded;
            nextNeeded = version + 1;
            builder.ApplyCommit(version, actions);
        }

        if (nextNeeded <= targetVersion)
            firstMissing ??= nextNeeded;

        if (firstMissing is long missing)
            throw IncompleteLog(missing, firstNeeded, targetVersion);

        return builder.Build();
    }

    /// <summary>
    /// A replay that cannot cover its whole range would silently return a snapshot missing whatever the
    /// absent commits did. Naming the first uncovered version turns that into something diagnosable: on a
    /// table whose log has been cleaned it points at the checkpoint that should have been found.
    /// </summary>
    /// <summary>
    /// Whether two checkpoint candidates name the same file, so that retrying the second after the
    /// first failed would only repeat the same read.
    /// </summary>
    private static bool SameCandidate(LastCheckpointInfo listed, LastCheckpointInfo? hint) =>
        hint is not null
        && listed.Version == hint.Version
        && listed.Parts == hint.Parts
        && string.Equals(listed.V2CheckpointPath, hint.V2CheckpointPath, StringComparison.Ordinal);

    private static DeltaFormatException IncompleteLog(long missing, long from, long through) =>
        new(DeltaErrorCodes.TruncatedTransactionLog,
            $"Delta log is incomplete: version {missing} is missing or unreadable and no checkpoint " +
            $"covers it. Building a snapshot at version {through} requires every version in " +
            $"[{from}..{through}].");

    /// <summary>
    /// Incrementally updates an existing snapshot by replaying only
    /// the commits newer than the snapshot's version.
    /// </summary>
    public static async ValueTask<Snapshot> UpdateAsync(
        Snapshot current,
        TransactionLog log,
        CancellationToken cancellationToken = default)
    {
        // One listing here too: the newest version and the commits to reach it came from two separate
        // walks that a concurrent commit could land between.
        var listing = await log.ReadListingAsync(cancellationToken).ConfigureAwait(false);
        long latestVersion = listing.LatestVersion;

        if (latestVersion <= current.Version)
            return current;

        var builder = SnapshotBuilder.FromSnapshot(current);

        var versions = listing.CommitsInRange(current.Version + 1, latestVersion).ToList();

        // Same rule as BuildAsync: an incremental update that skips a version produces a snapshot that
        // never existed. There is no checkpoint fallback here — this path only moves forward from one.
        long nextNeeded = current.Version + 1;
        foreach (long v in versions)
        {
            if (v != nextNeeded)
                throw IncompleteLog(nextNeeded, current.Version + 1, latestVersion);
            nextNeeded = v + 1;
        }
        if (nextNeeded <= latestVersion)
            throw IncompleteLog(nextNeeded, current.Version + 1, latestVersion);

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
    internal static SnapshotBuilder FromSnapshot(Snapshot snapshot)
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
            throw new DeltaFormatException(
                DeltaErrorCodes.StateRecoverError, "Table has no metadata action.");
        if (_protocol is null)
            throw new DeltaFormatException(
                DeltaErrorCodes.StateRecoverError, "Table has no protocol action.");

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
