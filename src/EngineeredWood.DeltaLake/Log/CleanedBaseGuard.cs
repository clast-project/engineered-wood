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
/// <para><b>Why it is usually free.</b> Every commit after the base was written after the base was last seen as the
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
    /// Throws <see cref="DeltaFormatException"/> (<see cref="DeltaErrorCodes.TruncatedTransactionLog"/>) when a commit
    /// of version <paramref name="baseSnapshot"/>.Version + 1 would land below versions log cleanup has removed.
    /// </summary>
    internal static async ValueTask EnsureBaseNotOvertakenAsync(
        TransactionLog log, Snapshot.Snapshot baseSnapshot, CancellationToken cancellationToken)
    {
        if (baseSnapshot.ListedLatestAt is { } listedAt)
        {
            var age = DateTimeOffset.UtcNow - listedAt;
            var window = LogCleanup.Retention(baseSnapshot.Metadata.Configuration);
            if (window > MaxFreshWindow)
                window = MaxFreshWindow;
            if (age >= TimeSpan.Zero && age < window)
                return;
        }

        long baseVersion = baseSnapshot.Version;
        if (await log.FileSystem.ExistsAsync(DeltaVersion.CommitPath(baseVersion), cancellationToken)
                .ConfigureAwait(false))
        {
            return;
        }

        var listing = await log.ReadListingAsync(cancellationToken).ConfigureAwait(false);
        if (listing.LatestVersion <= baseVersion || listing.CommitsInRange(baseVersion + 1, baseVersion + 1).Any())
            return;

        throw new DeltaFormatException(
            DeltaErrorCodes.TruncatedTransactionLog,
            $"Cannot commit on version {baseVersion}: the table is at version {listing.LatestVersion}, and log "
            + $"cleanup has removed the commits after version {baseVersion}, so this commit would land below the "
            + "latest checkpoint where no reader would see it. Nothing was written. Refresh the table (or reopen it) "
            + "and retry.");
    }
}
