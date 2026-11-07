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
        // One classified pass over _delta_log answers everything below: the target version, the newest
        // checkpoint, the compaction files, and the commits to replay. They used to be four separate
        // walks of the same directory, which also let them disagree about a concurrent commit.
        var listedAt = DateTimeOffset.UtcNow; // before the listing, so it errs early (#697)
        var listing = await log.ReadListingAsync(cancellationToken).ConfigureAwait(false);

        return await BuildFromListingAsync(log, checkpointReader, atVersion, listing, listedAt, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// <see cref="BuildAsync"/> from a listing the caller already took, so a refresh that falls back to a rebuild
    /// does not list the log a second time (#746 review).
    /// </summary>
    /// <param name="listedAt">When <paramref name="listing"/> was taken, read just before it.</param>
    private static async ValueTask<Snapshot> BuildFromListingAsync(
        TransactionLog log,
        CheckpointReader? checkpointReader,
        long? atVersion,
        LogListing listing,
        DateTimeOffset listedAt,
        CancellationToken cancellationToken)
    {
        var builder = new SnapshotBuilder();

        long targetVersion = atVersion ?? listing.LatestVersion;

        if (targetVersion < 0)
            throw new DeltaTableNotFoundException("Table has no commits.");

        // Try to bootstrap from a checkpoint
        long replayFrom = 0;
        var unreadable = new List<(LastCheckpointInfo Info, Exception Error)>();
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

            // The newest checkpoint can be unreadable too — a crash part-way through a write leaves a
            // truncated file at a listable name — so every listed one is a candidate, newest first, and
            // the first that reads wins. Stopping after the newest replayed from 0 instead, which fails
            // outright once log cleanup has run, although an older checkpoint and the commits after it
            // would have rebuilt the table. delta-spark retries the same way ("Trying a different
            // checkpoint"); it caps the retries at 2, and this does not need to, because a checkpoint
            // whose replay would cross a hole is set aside without being read, and log cleanup keeps
            // the rest few.
            // A set, not a list: when every candidate fails, all of them pass through here, and a table
            // whose log has never been cleaned can list thousands.
            var seen = new HashSet<(long Version, int? Parts, string? V2Path)>();
            var stranded = new List<LastCheckpointInfo>();
            var candidates = WithHint(
                hint, CheckpointReader.ListCheckpointCandidates(listing, targetVersion));

            bool bootstrapped = false;
            foreach (var candidate in candidates)
            {
                // Skip a re-read only of the SAME checkpoint that already failed — same version AND same
                // file. Comparing versions alone was too coarse: a hint whose path is wrong names the
                // version correctly, so the listing's candidate (which has the right path) was discarded
                // as a duplicate and the table failed to open at all.
                if (!seen.Add(CandidateKey(candidate)))
                    continue;

                // Judged by the plan the replay below will follow, not by a looser model of it: a
                // checkpoint whose replay crosses a hole is no starting point even when it reads, and
                // taking it anyway shut out a full replay that would have succeeded.
                if (!listing.PlanReplay(candidate.Version + 1, targetVersion).IsComplete)
                {
                    stranded.Add(candidate);
                    continue;
                }

                bootstrapped = await TryBootstrapAsync(candidate).ConfigureAwait(false);
                if (bootstrapped)
                    break;
            }

            // Nothing usable read, and a full replay has a hole too, so the table cannot be rebuilt at
            // this version. Starting from the newest checkpoint that reads anyway does not change that,
            // but it makes the error name the hole above that checkpoint rather than claim version 0
            // is missing.
            if (!bootstrapped && !listing.PlanReplay(0, targetVersion).IsComplete)
            {
                foreach (var candidate in stranded.OrderByDescending(c => c.Version))
                {
                    if (await TryBootstrapAsync(candidate).ConfigureAwait(false))
                        break;
                }
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
                catch (Exception ex)
                {
                    // Discard whatever was applied before the failure — a half-read checkpoint is not a
                    // valid starting state — and let the caller try the next candidate or full replay.
                    // Kept, so that a replay which then cannot cover the range names this as the cause.
                    builder = new SnapshotBuilder();
                    replayFrom = 0;
                    unreadable.Add((info, ex));
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

        // The same plan the checkpoint choice above was judged by.
        var bestCompacted = listing.PlanReplay(replayFrom, targetVersion).Compacted;

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
            throw ReplayGap(listing, unreadable, missing, firstNeeded, targetVersion);

        // A checkpoint, or a compacted range, carries no commitInfo, so a snapshot that ENDS on one has no timestamp
        // for its own version. On a table with in-commit timestamps that is the floor the next commit's timestamp
        // must stay above, and the field a version checksum needs (#660 review), so it is read from the version's
        // own commit when that is still on the log. Only then: elsewhere nothing depends on it, and the read costs
        // a request. A commit that cannot be read leaves it unknown, as before.
        if (builder._inCommitTimestamp is null
            && builder._version == targetVersion
            && Log.InCommitTimestamp.IsEnabled(builder._metadata?.Configuration)
            && listing.CommitsInRange(targetVersion, targetVersion).Any())
        {
            try
            {
                var own = await log.ReadCommitAsync(targetVersion, cancellationToken).ConfigureAwait(false);
                builder._inCommitTimestamp = Log.InCommitTimestamp.GetTimestampFromActions(own);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                // Unknown, as it was before this read.
            }
        }

        return builder.Build(atVersion is null ? listedAt : null, listing.AnchorAt(targetVersion));
    }

    /// <summary>
    /// The listed candidates, newest first, with the hint placed ahead of the listed ones at its own
    /// version and never ahead of a newer one.
    /// </summary>
    /// <remarks>
    /// The hint wins ties because it names the file a writer chose. It does not outrank a newer
    /// checkpoint, because a stale one usually names an OLDER checkpoint, and the listing can say only
    /// that the commits after it are there, not that they read. Starting from it chose a replay through a
    /// corrupt commit that the newer checkpoint lies past, and the build failed without trying it. Newest
    /// first means every newer candidate has been tried by the time one is taken.
    /// </remarks>
    private static IEnumerable<LastCheckpointInfo> WithHint(
        LastCheckpointInfo? hint, IEnumerable<LastCheckpointInfo> listed)
    {
        bool placed = hint is null;
        foreach (var candidate in listed)
        {
            if (!placed && candidate.Version <= hint!.Version)
            {
                yield return hint;
                placed = true;
            }
            yield return candidate;
        }

        if (!placed)
            yield return hint!;
    }

    /// <summary>
    /// What makes two checkpoint candidates the same file, so that trying the second after the first
    /// would only repeat the same read. The path compares ordinally, as the tuple's default comparer does.
    /// </summary>
    private static (long Version, int? Parts, string? V2Path) CandidateKey(LastCheckpointInfo info) =>
        (info.Version, info.Parts, info.V2CheckpointPath);

    /// <summary>
    /// The exception for a replay that could not cover its whole range — which is two different
    /// failures wearing the same symptom, and this decides which one it was.
    /// </summary>
    /// <remarks>
    /// A checkpoint that would have covered the hole, but which this reader passed over, leaves exactly
    /// the same hole in the replay as a deleted commit. Reporting both as "the log is incomplete" sent a
    /// user to look at retention settings for what is really a property of the checkpoint, so the
    /// checkpoint is named as the cause instead. Three shapes of that: a checkpoint that was tried and
    /// failed to read, a body this reader has no decoder for, and a multi-part checkpoint whose parts did
    /// not all land.
    /// </remarks>
    private static DeltaFormatException ReplayGap(
        LogListing listing, IReadOnlyList<(LastCheckpointInfo Info, Exception Error)> unreadable,
        long missing, long from, long through)
    {
        // The newest first: it is the one that would have needed the fewest commits behind it.
        foreach (var (info, error) in unreadable.OrderByDescending(u => u.Info.Version))
        {
            if (info.Version >= missing && info.Version <= through)
            {
                return new DeltaFormatException(
                    DeltaErrorCodes.TruncatedTransactionLog,
                    $"Delta log is incomplete: version {missing} is missing, and the checkpoint at " +
                    $"version {info.Version} ('{DescribeCheckpoint(info)}') that would have covered it " +
                    $"could not be read: {error.Message} Building a snapshot at version {through} " +
                    $"requires every version in [{from}..{through}].",
                    error);
            }
        }

        foreach (var (version, path) in listing.UndecodableCheckpoints)
        {
            // Only a checkpoint inside the hole is an explanation for it: one below `missing` was
            // never needed, and one above `through` is not on the path to the requested version.
            if (version >= missing && version <= through)
            {
                return new DeltaFormatException(
                    DeltaErrorCodes.UnsupportedCheckpointFormat,
                    $"Version {through} needs the checkpoint at version {version} ('{path}'), whose " +
                    "body is neither of the two forms a UUID-named V2 checkpoint may take (JSON or " +
                    "Parquet), so the versions it covers cannot be reconstructed. The log is intact — " +
                    "this is a limitation of this reader, not missing history.");
            }
        }

        if (listing.TornMultiPartCheckpoint(missing, through) is var (torn, present, declared))
        {
            return new DeltaFormatException(
                DeltaErrorCodes.TruncatedTransactionLog,
                $"Delta log is incomplete: version {missing} is missing, and the multi-part checkpoint " +
                $"at version {torn} that would have covered it has only {present} of its {declared} " +
                "parts. A checkpoint write that did not finish leaves a prefix like this, and loading " +
                "it would silently drop the files in the parts that never landed. Building a snapshot " +
                $"at version {through} requires every version in [{from}..{through}].");
        }

        return IncompleteLog(missing, from, through);
    }

    /// <summary>The checkpoint's file, or for a multi-part one the shape of its name.</summary>
    private static string DescribeCheckpoint(LastCheckpointInfo info) =>
        info.V2CheckpointPath
        ?? (info.Parts is int parts
            ? $"{DeltaVersion.LogPrefix}{DeltaVersion.Format(info.Version)}.checkpoint.*.{parts:D10}.parquet"
            : DeltaVersion.CheckpointPath(info.Version));

    /// <summary>
    /// A replay that cannot cover its whole range would silently return a snapshot missing whatever the
    /// absent commits did. Naming the first uncovered version turns that into something diagnosable: on a
    /// table whose log has been cleaned it points at the checkpoint that should have been found.
    /// </summary>
    private static DeltaFormatException IncompleteLog(long missing, long from, long through) =>
        new(DeltaErrorCodes.TruncatedTransactionLog,
            $"Delta log is incomplete: version {missing} is missing or unreadable and no checkpoint " +
            $"covers it. Building a snapshot at version {through} requires every version in " +
            $"[{from}..{through}].");

    /// <summary>
    /// Incrementally updates an existing snapshot by replaying only
    /// the commits newer than the snapshot's version.
    /// </summary>
    /// <exception cref="DeltaFormatException">
    /// <see cref="DeltaErrorCodes.TransactionLogReplaced"/> when the log is no longer the one
    /// <paramref name="current"/> was built from (#740); <see cref="DeltaErrorCodes.TruncatedTransactionLog"/> when
    /// a commit after it is missing.
    /// </exception>
    /// <remarks>
    /// Only certain evidence refuses: a different size or ETag, or a log that no longer reaches the snapshot's
    /// version. A snapshot that cannot be checked (one built in memory, a log file cleanup has removed, or a
    /// backend without ETags whose file shows only a different modification time) is updated as before: a
    /// committer refreshing after its own durable commit must not fail on a touched file.
    /// <see cref="UpdateOrRebuildAsync"/> rebuilds such a snapshot instead.
    /// </remarks>
    public static async ValueTask<Snapshot> UpdateAsync(
        Snapshot current,
        TransactionLog log,
        CancellationToken cancellationToken = default)
    {
        var (snapshot, _, _) = await TryUpdateAsync(current, log, forReader: false, cancellationToken)
            .ConfigureAwait(false);
        return snapshot ?? throw LogReplaced(current);
    }

    /// <summary>
    /// <see cref="UpdateAsync"/>, or a null snapshot where <paramref name="current"/> cannot be continued: its log
    /// was replaced, or, <paramref name="forReader"/>, it cannot be shown not to have been, or log cleanup has removed
    /// commits after it. The listing comes back either way, for the rebuild.
    /// </summary>
    /// <param name="forReader">The lenient mode <see cref="UpdateOrRebuildAsync"/> uses. Off, a gap after
    /// <paramref name="current"/> throws, as a committer's refresh must (#697).</param>
    private static async ValueTask<(Snapshot? Snapshot, LogListing Listing, DateTimeOffset ListedAt)> TryUpdateAsync(
        Snapshot current,
        TransactionLog log,
        bool forReader,
        CancellationToken cancellationToken)
    {
        // One listing here too: the newest version and the commits to reach it came from two separate
        // walks that a concurrent commit could land between.
        var listedAt = DateTimeOffset.UtcNow; // before the listing, so it errs early (#697)
        var listing = await log.ReadListingAsync(cancellationToken).ConfigureAwait(false);
        long latestVersion = listing.LatestVersion;

        var continuity = ContinuityOf(current, listing);
        if (continuity == LogContinuity.Replaced
            || (continuity == LogContinuity.Unverified && forReader))
        {
            return (null, listing, listedAt);
        }

        if (latestVersion <= current.Version)
            return (current.WithListedLatestAt(listedAt), listing, listedAt);

        var builder = SnapshotBuilder.FromSnapshot(current);

        var versions = listing.CommitsInRange(current.Version + 1, latestVersion).ToList();

        // Same rule as BuildAsync: an incremental update that skips a version produces a snapshot that
        // never existed. A committer's refresh refuses it; a reader's rebuilds from the checkpoint that log
        // cleanup left (#697), from this same listing.
        long nextNeeded = current.Version + 1;
        foreach (long v in versions)
        {
            if (v != nextNeeded)
                break;
            nextNeeded = v + 1;
        }
        if (nextNeeded <= latestVersion)
        {
            return forReader
                ? (null, listing, listedAt)
                : throw IncompleteLog(nextNeeded, current.Version + 1, latestVersion);
        }

        foreach (long v in versions)
        {
            var actions = await log.ReadCommitAsync(v, cancellationToken)
                .ConfigureAwait(false);
            builder.ApplyCommit(v, actions);
        }

        return (builder.Build(listedAt, listing.AnchorAt(latestVersion)), listing, listedAt);
    }

    /// <summary>Whether a listing still shows the log a snapshot was built from.</summary>
    private enum LogContinuity
    {
        /// <summary>The file that stood for the snapshot's version is the same object.</summary>
        Same,

        /// <summary>
        /// Cannot be shown either way: the snapshot records no log file, log cleanup has since removed it, or
        /// nothing but a modification time tells the two apart (<see cref="LogFileMatch.Unknown"/>). A reader
        /// rebuilds; a committer continues.
        /// </summary>
        Unverified,

        /// <summary>
        /// Certainly a different log: another object stands at that path (a different size or ETag), or the log
        /// no longer reaches the snapshot's version.
        /// </summary>
        Replaced,
    }

    private static LogContinuity ContinuityOf(Snapshot current, LogListing listing)
    {
        // A log only grows, and cleanup always keeps the newest version, so one that no longer reaches this
        // version is not the log the snapshot came from: it was deleted, or re-created shorter.
        if (listing.LatestVersion < current.Version)
            return LogContinuity.Replaced;

        if (current.LogAnchor is not { } anchor || listing.IdentityOf(anchor.Path) is not { } listed)
            return LogContinuity.Unverified;

        return anchor.Compare(listed) switch
        {
            LogFileMatch.Same => LogContinuity.Same,
            LogFileMatch.Different => LogContinuity.Replaced,
            _ => LogContinuity.Unverified,
        };
    }

    private static DeltaFormatException LogReplaced(Snapshot current) =>
        new(DeltaErrorCodes.TransactionLogReplaced,
            $"The Delta log is no longer the one the snapshot at version {current.Version} was built from: the " +
            "file that names that version is a different object now, or the log no longer reaches it. The table " +
            "was most likely dropped and re-created at the same path, and continuing from the snapshot would mix " +
            "the two tables. Open the table again.");

    /// <summary>
    /// Brings <paramref name="current"/> to the latest version for a READER: incrementally where it can, and from
    /// the latest checkpoint where log cleanup has removed commits after it (#697).
    /// </summary>
    /// <remarks>
    /// <para><see cref="UpdateAsync"/> refuses such a gap, which is right for it: an incremental replay that skips a
    /// version produces a state that never existed. But the gap is what log cleanup leaves once a checkpoint covers
    /// the commits it removed, and the table is still perfectly readable from that checkpoint, so a reader holding an
    /// old snapshot need not fail.</para>
    ///
    /// <para><b>⚠ Not for a committer's own refresh.</b> A commit based on a snapshot whose successors were cleaned
    /// up would land below the checkpoint, where no reader looks, and be lost; jumping over the gap after committing
    /// would hide exactly that. A commit path refuses such a base before writing
    /// (<see cref="Log.CleanedBaseGuard"/>), and refreshes after committing with <see cref="UpdateAsync"/>, which
    /// still throws.</para>
    ///
    /// <para><b>Cleanup can also land mid-replay</b> (#698 review): the listing names a commit that is deleted before
    /// it is read. That surfaces as the file system's own not-found error, which differs by backend, so it is not
    /// recognised by type. Any other failure re-lists the log instead: if commits after <paramref name="current"/>
    /// are now missing, the snapshot is built from the checkpoint; otherwise the original error is rethrown.</para>
    /// </remarks>
    public static async ValueTask<Snapshot> UpdateOrRebuildAsync(
        Snapshot current,
        TransactionLog log,
        CheckpointReader? checkpointReader,
        CancellationToken cancellationToken = default)
    {
        (Snapshot? Snapshot, LogListing Listing, DateTimeOffset ListedAt) attempt;
        try
        {
            attempt = await TryUpdateAsync(current, log, forReader: true, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            var listedAt = DateTimeOffset.UtcNow;
            var listing = await log.ReadListingAsync(cancellationToken).ConfigureAwait(false);
            long missing = listing.LatestVersion - current.Version;
            if (missing <= 0 || listing.CommitsInRange(current.Version + 1, listing.LatestVersion).Count() == missing)
                throw;
            return await BuildFromListingAsync(log, checkpointReader, atVersion: null, listing, listedAt, cancellationToken)
                .ConfigureAwait(false);
        }

        // Outside the try, so a rebuild that fails reports its own error rather than being retried by the recovery
        // above. An unverified snapshot is rebuilt as well as a replaced one: a rebuild costs what an open costs,
        // while continuing a log that was replaced mixes two tables (#740). Either way, and for a gap log cleanup
        // left, the rebuild starts from the listing just taken: one listing in all, as an open has (#746 review).
        return attempt.Snapshot
            ?? await BuildFromListingAsync(
                    log, checkpointReader, atVersion: null, attempt.Listing, attempt.ListedAt, cancellationToken)
                .ConfigureAwait(false);
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
    public Snapshot Build() => Build(listedLatestAt: null, logAnchor: null);

    /// <summary>
    /// As <see cref="Build()"/>, recording when a listing showed this version to be the latest
    /// (<see cref="Snapshot.ListedLatestAt"/>).
    /// </summary>
    internal Snapshot Build(DateTimeOffset? listedLatestAt, LogFileIdentity? logAnchor)
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
            ListedLatestAt = listedLatestAt,
            LogAnchor = logAnchor,
            // The delta.rowTracking domainMetadata is the spec source of truth for the high-water mark;
            // the active-file derivation alone under-counts after removes (a later writer could then
            // reassign used row ids). Take the max so either source protects the invariant.
            RowIdHighWaterMark = System.Math.Max(
                RowTracking.RowTrackingConfig.ComputeHighWaterMark(activeFiles),
                (RowTracking.RowTrackingConfig.TryReadHighWaterMark(_domainMetadata) ?? -1) + 1),
        };
    }
}
