// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Deletes commit and checkpoint files a checkpoint has made redundant, per the table's
/// <c>delta.logRetentionDuration</c>.
///
/// <para><b>Why this exists.</b> The property was accepted and stored and read by nobody, so
/// <c>_delta_log</c> grew for the life of a table: every scan replays or lists commits that a checkpoint
/// already subsumes. Measured on S3, a dead commit costs ~10 ms per scan, so an hourly model adds ~90 s of
/// pure metadata to every read after a year. Nothing about that is recoverable by the reader — the files
/// are simply there forever.</para>
///
/// <para><b>The rule, from Delta's <c>MetadataCleanup</c>.</b> A log file may be deleted only when BOTH
/// hold: a checkpoint exists AFTER it, and it is older than <c>now - logRetentionDuration</c>. The first
/// is what makes deletion safe at all — a commit no checkpoint covers is the only copy of its actions, and
/// removing it makes the table unreadable rather than merely older. With no <c>_last_checkpoint</c> this
/// deletes NOTHING, which is also Delta's behaviour.</para>
///
/// <para><b>⚠ Deletion is a contiguous PREFIX, and that is load-bearing rather than tidy.</b> Replay
/// demands unbroken coverage from the checkpoint onward, and a reader that meets a HOLE reports the table
/// as corrupt (upstream #36 made that loud on purpose). Deleting a middle version while keeping an older
/// one would produce exactly that, so the walk stops at the first file it may not delete instead of
/// skipping it.</para>
///
/// <para><b>⚠ A V2 checkpoint's SIDECARS are not reclaimed.</b> Deleting an expired
/// <c>&lt;version&gt;.checkpoint.&lt;uuid&gt;.json</c> leaves whatever it referenced in
/// <c>_delta_log/_sidecars/</c> behind, and nothing else collects it either — <c>VacuumExecutor</c>
/// excludes <c>_delta_log</c> entirely. See upstream #111; the short version is that deleting each
/// expired checkpoint's OWN sidecars is not the safe shortcut it looks like.</para>
///
/// <para>Two reasons, and the first is the one that bites today. A concurrent writer's sidecars exist
/// BEFORE the checkpoint that names them, so "referenced by no checkpoint I can see" does not mean dead —
/// it may mean a checkpoint is mid-publish. And nothing in PROTOCOL.md makes a sidecar exclusive to one
/// checkpoint: verified 2026-08-10 that neither delta-spark's <c>Checkpoints.scala</c> nor
/// delta-kernel-rs's checkpoint writer reuses one today (both write a fresh set every checkpoint), but
/// the spec permits it, and reuse is a change WE may want — rewriting an unchanged multi-million-row file
/// list per checkpoint is the cost that makes V2 checkpoints expensive on a streaming table. A cleanup
/// that assumes exclusivity would have to be revisited by exactly that work.</para>
///
/// <para>So reclaiming sidecars means marking the set every surviving checkpoint references and sweeping
/// the remainder, which is its own change. Until then this reclaims commits and checkpoint files and
/// leaves sidecars — strictly better than the nothing it replaces, and not a step toward deleting a live
/// one.</para>
/// </summary>
internal static class LogCleanup
{
    /// <summary>Delta's default when the table declares nothing: 30 days.</summary>
    public static readonly TimeSpan DefaultRetention = TimeSpan.FromDays(30);

    /// <summary>
    /// At or below this, a listing's modification time is treated as ABSENT rather than as a date. Covers
    /// both sentinels a filesystem might return for "I do not know" — <see cref="DateTimeOffset.MinValue"/>
    /// and the Unix epoch — because a Delta log predating 1970 is not a thing and a fake old timestamp is
    /// indistinguishable from a genuinely expired one at the point where the deletion is decided.
    /// </summary>
    /// <remarks>Spelled out rather than <c>DateTimeOffset.UnixEpoch</c>, which netstandard2.0 lacks — this
    /// assembly still targets it for the net472 leg.</remarks>
    private static readonly DateTimeOffset UnknownModifiedThreshold =
        new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// Deletes what <paramref name="snapshot"/>'s configuration says is expired. Returns how many files
    /// were deleted.
    ///
    /// <para><b>Best-effort and quiet.</b> A failed delete is swallowed: cleanup runs after a commit that
    /// has already succeeded, and turning a housekeeping failure into a commit failure would be a worse
    /// trade than leaving a file behind — which is precisely the state this improves on. It never throws
    /// for the same reason, cancellation excepted.</para>
    /// </summary>
    /// <param name="log">The log to clean.</param>
    /// <param name="configuration">The table's metadata configuration.</param>
    /// <param name="latestCheckpointVersion">The version of the newest checkpoint, or null when there is
    /// none — in which case nothing is deleted.</param>
    /// <param name="now">The clock, injected so a test can force a horizon without sleeping.</param>
    public static async ValueTask<int> RunAsync(
        TransactionLog log,
        IReadOnlyDictionary<string, string>? configuration,
        long? latestCheckpointVersion,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled(configuration) || latestCheckpointVersion is not { } checkpointVersion)
            return 0;

        // Everything strictly BELOW the checkpoint is covered by it. The checkpoint's own version is kept:
        // it is what the survivors are replayed from.
        long maxDeletableVersion = checkpointVersion - 1;
        if (maxDeletableVersion < 0)
            return 0;

        var cutoff = now - Retention(configuration);

        // Ordered by version so the prefix rule can be applied by walking, and so the boundary check below
        // compares the right pair of files.
        var candidates = new List<(long Version, string Path, DateTimeOffset Modified)>();
        try
        {
            await foreach (var file in log.FileSystem.ListAsync(DeltaVersion.LogPrefix, cancellationToken)
                .ConfigureAwait(false))
            {
                string name = FileName(file.Path);
                if (DeltaVersion.TryParseCommitVersion(name, out long commitVersion))
                    candidates.Add((commitVersion, file.Path, file.LastModified));
                else if (DeltaVersion.TryParseCheckpointVersion(name, out long ckptVersion))
                    candidates.Add((ckptVersion, file.Path, file.LastModified));
            }
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            return 0;
        }

        candidates.Sort((a, b) => a.Version != b.Version
            ? a.Version.CompareTo(b.Version)
            : string.CompareOrdinal(a.Path, b.Path));

        // ⚠ A FILESYSTEM THAT CANNOT DATE ITS FILES GETS NO CLEANUP, and this guard is not theoretical:
        // an ITableFileSystem is free to return a placeholder when its backing listing carries no
        // modification time, and one host was found reporting a constant epoch for every file. Under that
        // value every commit looks decades old and is therefore expired the instant it is written.
        // Deleting by age is only safe where the age is real, so an absent timestamp declines the whole
        // pass rather than defaulting to one — the opposite choice silently deletes live history.
        foreach (var candidate in candidates)
        {
            if (candidate.Modified <= UnknownModifiedThreshold)
                return 0;
        }

        int cut = 0;
        while (cut < candidates.Count
               && candidates[cut].Version <= maxDeletableVersion
               && candidates[cut].Modified < cutoff)
        {
            cut++;
        }
        if (cut == 0)
            return 0;

        // ⚠ THE ADJUSTMENT ANCHOR, and it is the one part not obvious from the rule above.
        //
        // Delta presents commit timestamps as strictly increasing with version, adjusting a file whose
        // modification time is not GREATER than its predecessor's to predecessor + 1 ms. A reader doing
        // time-travel-BY-TIMESTAMP therefore depends on the file its answer was adjusted from: delete that
        // and the same timestamp query starts answering with a different version.
        //
        // This library's own time travel is immune — GetSnapshotAtTimestampAsync reads the IN-COMMIT
        // timestamp out of the actions and never consults file metadata — but a Delta reader on the same
        // table is not, so the anchor is not ours to break.
        //
        // ⚠ It only matters for a SURVIVOR: if a file's dependent is ALSO being deleted, no reader can ask
        // about it. So rather than reproduce Delta's buffering iterator, walk the cut BACK over the chain
        // the first survivor depends on. Uniform timestamps therefore retain everything, which is the safe
        // direction and self-correcting — the next checkpoint retries with a different boundary.
        while (cut > 0 && cut < candidates.Count
               && candidates[cut].Modified <= candidates[cut - 1].Modified)
        {
            cut--;
        }
        if (cut == 0)
            return 0;

        int deleted = 0;
        for (int i = 0; i < cut; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await log.FileSystem.DeleteAsync(candidates[i].Path, cancellationToken).ConfigureAwait(false);
                deleted++;
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                // A file we could not remove ends the pass: continuing would delete a LATER version while
                // leaving this one, which is the hole the prefix rule exists to prevent.
                break;
            }
        }

        return deleted;
    }

    /// <summary>
    /// <c>delta.enableExpiredLogCleanup</c> — Delta's own opt-out, honoured so a table that has switched
    /// cleanup off (because something else owns its log retention) keeps every file.
    /// </summary>
    /// <remarks>
    /// Absent means enabled, which is Delta's default. But a value PRESENT and unparseable means disabled,
    /// which is the opposite of what "default true" would give: someone wrote something in this property,
    /// and the only intent worth attributing to a value like <c>no</c> or <c>off</c> is the one that turns
    /// deletion off. Same principle as <see cref="Retention"/> refusing an odd duration — an unreadable
    /// property must never be the thing that authorises deleting a table's log.
    /// </remarks>
    private static bool IsEnabled(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null
            || !configuration.TryGetValue("delta.enableExpiredLogCleanup", out string? raw))
        {
            return true;
        }
        return bool.TryParse(raw, out bool enabled) && enabled;
    }

    /// <summary>
    /// The table's <c>delta.logRetentionDuration</c>, or <see cref="DefaultRetention"/> when unset or
    /// unparseable. Unparseable falls back rather than throwing, and a NON-POSITIVE value is refused the
    /// same way: an odd property must not become an instruction to delete a table's whole log.
    /// </summary>
    private static TimeSpan Retention(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is not null
            && configuration.TryGetValue("delta.logRetentionDuration", out string? raw)
            && IntervalParser.TryParse(raw, out var parsed)
            && parsed > TimeSpan.Zero)
        {
            return parsed;
        }
        return DefaultRetention;
    }

    private static string FileName(string path)
    {
        int slash = path.LastIndexOfAny(['/', '\\']);
        return slash >= 0 ? path.Substring(slash + 1) : path;
    }
}
