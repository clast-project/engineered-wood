// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Refuses a commit whose base snapshot log cleanup has overtaken (#697), before anything is written.
/// </summary>
/// <remarks>
/// <para><b>The failure it prevents</b> (measured). A commit is "create version base + 1 if absent". Once a newer
/// checkpoint covers the versions after the base and cleanup has deleted their commits, base + 1 is absent again, so
/// a handle that has not moved since creates it. That commit sits below the checkpoint, where this library, Spark and
/// delta-rs all start reading, so it is invisible, and the next cleanup deletes it: the write is lost, after being
/// reported durable.</para>
///
/// <para><b>Why one existence check suffices.</b> Cleanup deletes the oldest commits first and never leaves a hole,
/// so while the base version's own commit file exists, every commit after it does too, and a create of base + 1
/// collides as any concurrent commit would. Only when that file is gone is the log listed: the commit is refused when
/// the table has moved past the base and base + 1 is missing. A base whose commit is gone but nothing follows (a
/// snapshot read from a checkpoint whose commit was cleaned up) commits normally.</para>
///
/// <para><b>Why it is usually free</b> where a base carries no log anchor (a snapshot built in memory, or a base
/// known only by its version). A snapshot read from a listing gets <see cref="EnsureCommittableBaseAsync"/> instead,
/// which checks its base commit on every commit (#750). Every commit after the base was written after the base was last seen as the
/// latest version (<see cref="Snapshot.Snapshot.ListedLatestAt"/>), and cleanup deletes only commits older than
/// <c>delta.logRetentionDuration</c>. So until that long has passed, none can be gone. A handle refreshes after each
/// of its own commits, so a writer that keeps writing never pays; an idle one pays one existence check. The window is
/// also capped at <see cref="MaxFreshWindow"/>: another writer may have shortened the retention since this snapshot
/// was read, and a short cap makes that matter only for a retention set below it.</para>
///
/// <para>Refused rather than rebased: a rebase checks the commits after the base for conflicts, and those are
/// gone. Refreshing the handle and retrying is the remedy, which the message says.</para>
///
/// <para><b>Known limits</b> (#698 review). (1) A retention shortened below <see cref="MaxFreshWindow"/> by another
/// writer after this snapshot was read, followed by a checkpoint and cleanup within that window, is not seen: the
/// window trusts this snapshot's retention up to the cap. (2) A cleanup that deletes the base between the existence
/// check and the create is not seen either. Delta has no lock to close that window, and closing it after the fact
/// would mean deleting a commit already reported durable; delta-spark has the same theoretical gap between a
/// transaction's snapshot and its commit. Both need cleanup to run against a handle at exactly that moment, where
/// without this guard every write through an overtaken handle was lost.</para>
/// </remarks>
internal static class CleanedBaseGuard
{
    /// <summary>The longest a snapshot counts as fresh, whatever the retention (see the class remarks).</summary>
    internal static readonly TimeSpan MaxFreshWindow = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Before a commit of version <paramref name="baseSnapshot"/>.Version + 1, makes sure the log still holds the
    /// base: throws <see cref="DeltaFormatException"/> with <see cref="DeltaErrorCodes.TransactionLogReplaced"/> when
    /// the log is provably not the one the base was read from (#750), and with
    /// <see cref="DeltaErrorCodes.TruncatedTransactionLog"/> when log cleanup has removed the versions after it (#697).
    /// </summary>
    /// <remarks>
    /// <para><b>The failure it prevents</b> (#750, measured). A handle held while its table is dropped and re-created
    /// at the same path commits into the NEW log: the create of base + 1 is free whenever the new log is no longer
    /// than the base, and a blind append passes the conflict check against the new table's commits when it is
    /// longer. Against a shorter log that leaves a hole (log 0, 1, 4) that every reader of the new table then fails on,
    /// and the write was reported as a success.</para>
    ///
    /// <para><b>What it checks.</b> The base's commit file, by a listing of that one path, against the identity the
    /// base recorded (<see cref="Snapshot.Snapshot.LogAnchor"/>). A listing rather than a metadata request, because the
    /// identity came from a listing and is compared against the same report of it. Refused only on proof, as the
    /// refresh is: a different size or ETag, a commit at a version the base saw only a checkpoint for, or a log that
    /// no longer reaches the base. A file that only shows a different modification time is not proof, and is committed
    /// to: a touched or copied file must not fail a write. On a backend without ETags (a local file system), a
    /// re-created commit of exactly the old size is therefore not seen.</para>
    ///
    /// <para><b>Cost.</b> One single-file listing per commit, made in place of the existence check the cleanup guard
    /// made outside its freshness window. Unlike that check it is made every time: a drop and re-create can happen
    /// moments after the base was read. When the base's commit is absent (cleaned up, or never there) the log is
    /// listed as before.</para>
    ///
    /// <para>Refused rather than rebased: the commits after the base belong to another table. Nothing is written.
    /// A snapshot that records no anchor (one built in memory) gets the cleanup guard alone.</para>
    ///
    /// <para><b>Known limit</b>, as for the cleanup guard: a drop and re-create between this check and the create
    /// is not seen on the first attempt. A create-if-absent cannot be made conditional on another object, and Delta has
    /// no lock. A committer whose create then collides checks again before it retries
    /// (<see cref="EnsureLogNotReplacedAsync"/>), so only a new log no longer than the base can take that commit.</para>
    /// </remarks>
    internal static async ValueTask EnsureCommittableBaseAsync(
        TransactionLog log, Snapshot.Snapshot baseSnapshot, CancellationToken cancellationToken)
    {
        long baseVersion = baseSnapshot.Version;
        if (baseSnapshot.LogAnchor is not { Files.Count: > 0 } anchor)
        {
            await EnsureBaseNotOvertakenAsync(
                log, baseVersion, baseSnapshot.ListedLatestAt, baseSnapshot.Metadata.Configuration,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        // Null when the base's commit was there, which also rules out cleanup having removed anything after it:
        // cleanup never leaves a hole.
        if (await CheckSameLogAsync(log, baseVersion, anchor, cancellationToken).ConfigureAwait(false)
                is { } listing
            && listing.LatestVersion > baseVersion
            && !listing.CommitsInRange(baseVersion + 1, baseVersion + 1).Any())
        {
            throw Overtaken(baseVersion, listing.LatestVersion);
        }
    }

    /// <summary>
    /// The replaced-log half of <see cref="EnsureCommittableBaseAsync"/> alone: throws
    /// <see cref="DeltaErrorCodes.TransactionLogReplaced"/> when the log provably is not the one
    /// <paramref name="baseSnapshot"/> was read from; does nothing for a snapshot with no anchor.
    /// </summary>
    /// <remarks>For a commit that has already passed the full check and is about to try again (a retry after a
    /// collision, which is exactly what a longer re-created log produces; #755 review), and for a commit that needs
    /// no base but must still not land in another table's log (VACUUM END).</remarks>
    internal static async ValueTask EnsureLogNotReplacedAsync(
        TransactionLog log, Snapshot.Snapshot baseSnapshot, CancellationToken cancellationToken)
    {
        if (baseSnapshot.LogAnchor is { Files.Count: > 0 } anchor)
            await CheckSameLogAsync(log, baseSnapshot.Version, anchor, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Throws when the log provably is not the one <paramref name="anchor"/> was recorded from. Returns
    /// null when the base's commit was found by its own listing, or the full listing it had to read otherwise.</summary>
    private static async ValueTask<LogListing?> CheckSameLogAsync(
        TransactionLog log, long baseVersion, LogVersionAnchor anchor, CancellationToken cancellationToken)
    {
        string commitPath = DeltaVersion.CommitPath(baseVersion);
        // Matched by the full path, not the file name: a backend may list a prefix recursively (the local one does),
        // and a same-named file in a directory under the log is not the commit (#755 review).
        LogFileIdentity? listed = null;
        await foreach (var file in log.FileSystem.ListAsync(commitPath, cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(file.Path, commitPath, StringComparison.Ordinal))
                listed = LogFileIdentity.Of(file);
        }

        if (listed is { } current)
        {
            EnsureSameCommit(anchor, baseVersion, commitPath, current);
            return null;
        }

        // The base's commit is gone, or the base never had one: list the log. The table can be re-created between
        // the two listings, so this one's commit at the base, if any, is checked too (#755 review).
        var listing = await log.ReadListingAsync(cancellationToken).ConfigureAwait(false);
        if (listing.LatestVersion < baseVersion)
            throw LogReplaced(baseVersion, $"the log now ends at version {listing.LatestVersion}");
        if (listing.IdentityOf(commitPath) is { } relisted)
        {
            EnsureSameCommit(anchor, baseVersion, commitPath, relisted);
            return null;
        }
        return listing;
    }

    /// <summary>Throws when <paramref name="listed"/>, the base's commit as a listing shows it now, proves the log
    /// is another one.</summary>
    private static void EnsureSameCommit(
        LogVersionAnchor anchor, long baseVersion, string commitPath, LogFileIdentity listed)
    {
        // A commit is never written at a version after its checkpoint, so one beside a checkpoint-only base is
        // another log's.
        if (!anchor.HasCommit)
            throw LogReplaced(baseVersion, "a commit file now stands at that version, where there was none");

        foreach (var recorded in anchor.Files)
        {
            if (string.Equals(recorded.Path, commitPath, StringComparison.Ordinal)
                && recorded.Compare(listed) == LogFileMatch.Different)
            {
                throw LogReplaced(baseVersion, "the commit file for that version is a different object now");
            }
        }
    }

    private static DeltaFormatException LogReplaced(long baseVersion, string evidence) =>
        new(DeltaErrorCodes.TransactionLogReplaced,
            $"Cannot commit on version {baseVersion}: the Delta log is no longer the one that version was read from "
            + $"({evidence}). The table was most likely dropped and re-created at the same path, and this commit "
            + "would land in the new table's log. Nothing was written. Open the table again and retry.");

    private static DeltaFormatException Overtaken(long baseVersion, long latestVersion) =>
        new(DeltaErrorCodes.TruncatedTransactionLog,
            $"Cannot commit on version {baseVersion}: the table is at version {latestVersion}, and log "
            + $"cleanup has removed the commits after version {baseVersion}, so this commit would land below the "
            + "latest checkpoint where no reader would see it. Nothing was written. Refresh the table (or reopen it) "
            + "and retry.");

    /// <summary>
    /// As the snapshot overload, for a base known only by its version: <paramref name="latestAt"/> is a moment at
    /// or before which <paramref name="baseVersion"/> was the latest version (null when unknown), and
    /// <paramref name="configuration"/> supplies the retention.
    /// </summary>
    internal static async ValueTask EnsureBaseNotOvertakenAsync(
        TransactionLog log, long baseVersion, DateTimeOffset? latestAt,
        IReadOnlyDictionary<string, string>? configuration, CancellationToken cancellationToken)
    {
        if (await FindOvertakingVersionAsync(log, baseVersion, latestAt, configuration, cancellationToken)
                .ConfigureAwait(false) is not { } latestVersion)
        {
            return;
        }

        throw Overtaken(baseVersion, latestVersion);
    }

    /// <summary>
    /// The table's latest version when log cleanup has removed the commits after <paramref name="baseVersion"/>,
    /// so a commit at <paramref name="baseVersion"/> + 1 would land below them; null when it would not. For a
    /// commit that needs no base (a commitInfo-only one) and can move to after the returned version instead of
    /// being refused.
    /// </summary>
    internal static async ValueTask<long?> FindOvertakingVersionAsync(
        TransactionLog log, long baseVersion, DateTimeOffset? latestAt,
        IReadOnlyDictionary<string, string>? configuration, CancellationToken cancellationToken)
    {
        if (latestAt is { } seenAt)
        {
            var age = DateTimeOffset.UtcNow - seenAt;
            var window = LogCleanup.Retention(configuration);
            if (window > MaxFreshWindow)
                window = MaxFreshWindow;
            if (age >= TimeSpan.Zero && age < window)
                return null;
        }

        if (await log.FileSystem.ExistsAsync(DeltaVersion.CommitPath(baseVersion), cancellationToken)
                .ConfigureAwait(false))
        {
            return null;
        }

        var listing = await log.ReadListingAsync(cancellationToken).ConfigureAwait(false);
        if (listing.LatestVersion <= baseVersion || listing.CommitsInRange(baseVersion + 1, baseVersion + 1).Any())
            return null;
        return listing.LatestVersion;
    }
}
