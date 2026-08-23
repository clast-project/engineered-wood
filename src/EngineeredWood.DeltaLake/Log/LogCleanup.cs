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
/// <para><b>A V2 checkpoint's SIDECARS are reclaimed too, by marking rather than by ownership.</b> The
/// file actions of a large table live in <c>_delta_log/_sidecars/</c>, not in the checkpoint body, so
/// deleting the checkpoints without them would reclaim the index and leave the bulk — and nothing else
/// collects them either, since <c>VacuumExecutor</c> excludes <c>_delta_log</c> entirely. What is NOT
/// safe is deleting each expired checkpoint's own list: a sidecar is referenced by a checkpoint, not
/// owned by one, and a surviving checkpoint may name the same file. See
/// <see cref="SweepUnreferencedSidecarsAsync"/> for the rule and for why its age condition is
/// load-bearing rather than tidy.</para>
///
/// <para><b>Version-checksum files go too.</b> Both this library and delta-spark write a
/// <c>&lt;version&gt;.crc</c> beside every commit by default, so a table accumulates one per commit from
/// either writer — and unrecognised they were the one class of file left growing without bound. See
/// <see cref="SweepExpiredChecksumsAsync"/>, including why they are swept separately rather than walked
/// with the commits.</para>
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
    /// Deletes what <paramref name="configuration"/> says is expired. Returns how many files
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

        // Version-checksum files are collected SEPARATELY rather than into `candidates`, and that is a
        // decision rather than tidiness — see SweepExpiredChecksumsAsync.
        var checksums = new List<(long Version, string Path, DateTimeOffset Modified)>();
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
                else if (DeltaVersion.TryParseChecksumVersion(name, out long crcVersion))
                    checksums.Add((crcVersion, file.Path, file.LastModified));
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
        //
        // "No cleanup" is the safe answer here, not the only possible one. A table with
        // `delta.enableInCommitTimestamps` carries an authoritative, monotonic per-commit timestamp INSIDE
        // the commit, which is exactly the fact this pass is missing — and the spec already treats file
        // modification time as the unreliable one, requiring in-commit timestamps on catalog-managed
        // tables because "the file modification timestamp of the published file will not accurately
        // reflect the original commit time". Using it would make cleanup work where the listing cannot
        // date anything, at the cost of reading commits rather than only listing them. See upstream #115;
        // it is a bigger change than it looks (checkpoint files carry no such timestamp, and a mid-life
        // enablement leaves the commits before it with none), which is why this pass declines instead.
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

        // Deletion above is a strict prefix that stops at the first failure, so candidates[deleted..] is
        // exactly what is still on the log — which is what the sweeps below key off.
        deleted += await SweepUnreferencedSidecarsAsync(
            log, candidates, firstSurvivor: deleted, cutoff, cancellationToken).ConfigureAwait(false);

        // long.MaxValue when nothing survived: every commit this log had is gone, so every checksum
        // describes a version that is gone. Reachable only when the caller names a checkpoint version above
        // anything on the log, but an index out of range here would throw out of a commit that has already
        // succeeded, which this is documented never to do.
        long firstSurvivingVersion = deleted < candidates.Count
            ? candidates[deleted].Version
            : long.MaxValue;

        deleted += await SweepExpiredChecksumsAsync(
            log, checksums, firstSurvivingVersion, cutoff, cancellationToken).ConfigureAwait(false);

        return deleted;
    }

    /// <summary>
    /// Deletes version-checksum files (<c>&lt;version&gt;.crc</c>) for versions whose commit has just been
    /// removed. Returns how many were deleted.
    /// </summary>
    /// <remarks>
    /// <para>Both this library (<see cref="VersionChecksumWriter"/>) and delta-spark write one beside every
    /// commit by default, so a table accumulates one per commit whoever maintains it. Unrecognised, they
    /// were the one file class cleanup left growing without bound — the exact condition it exists to end,
    /// surviving in a name it did not parse. delta-spark deletes them alongside commits and checkpoints,
    /// filtering its own listing on
    /// <c>isCheckpointFile(f) || isDeltaFile(f) || isChecksumFile(f)</c>.</para>
    ///
    /// <para><b>⚠ Swept separately rather than folded into the prefix walk, so that a file outside the
    /// replay chain cannot gate it.</b> That walk stops at the first file it may not delete, because a hole
    /// in the COMMITS is corruption. A checksum file is not part of that chain — it is an optional
    /// per-version summary, and every version without one already looks fine to a reader — so letting one
    /// into the walk lets it stop the walk. delta-spark writes a checksum AFTER the commit it describes,
    /// so a <c>.crc</c> can be newer than its own commit and newer than the horizon while the commits
    /// around it are long expired; folded in, that one file would halt cleanup at its version and leave
    /// every later commit behind. Separate, it halts nothing.</para>
    ///
    /// <para>The boundary is therefore taken from the decision the walk already made:
    /// <paramref name="firstSurvivingVersion"/> is the oldest version still on the log, so a checksum
    /// below it describes a commit that is gone. That is strictly more conservative than deleting by age
    /// alone, and it means a checksum file can never outlive or predecease its own commit.</para>
    ///
    /// <para>Order does not matter here — a missing checksum reads as "no checksum", which is what every
    /// version without one already looks like — so a file that will not delete is skipped rather than
    /// ending the pass.</para>
    /// </remarks>
    private static async ValueTask<int> SweepExpiredChecksumsAsync(
        TransactionLog log,
        List<(long Version, string Path, DateTimeOffset Modified)> checksums,
        long firstSurvivingVersion,
        DateTimeOffset cutoff,
        CancellationToken cancellationToken)
    {
        int deleted = 0;
        foreach (var (version, path, modified) in checksums)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (version >= firstSurvivingVersion)
                continue;
            // Same reasoning as the commit pass: an age we cannot trust is not an age.
            if (modified <= UnknownModifiedThreshold || modified >= cutoff)
                continue;

            try
            {
                await log.FileSystem.DeleteAsync(path, cancellationToken).ConfigureAwait(false);
                deleted++;
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                continue;
            }
        }

        return deleted;
    }

    /// <summary>
    /// Deletes sidecar files no surviving checkpoint references and that are older than the retention
    /// horizon. Returns how many were deleted.
    /// </summary>
    /// <remarks>
    /// <para><b>Mark and sweep, and it has to be.</b> A sidecar is not owned by the checkpoint that names
    /// it: PROTOCOL.md describes a checkpoint as REFERENCING sidecars and nowhere makes that exclusive, so
    /// deleting an expired checkpoint's own list can take out a file a surviving checkpoint still needs.
    /// No engine relies on that today — verified 2026-08-10 that delta-spark and delta-kernel-rs both mint
    /// a fresh set per checkpoint — but the spec permits it and it is an optimisation worth making
    /// ourselves, so the safe shape is to mark what survivors reference rather than to assume ownership.
    /// This is what delta-spark's <c>identifyAndDeleteUnreferencedSidecarFiles</c> does.</para>
    ///
    /// <para><b>⚠ UNREFERENCED DOES NOT MEAN DEAD, which is why the age condition is not decoration.</b> A
    /// writer publishes its sidecars BEFORE the checkpoint that names them — it must, since the checkpoint
    /// records their paths — so a sweep running against a concurrent writer can see a complete set of
    /// sidecars that nothing references yet and destroy a checkpoint seconds from existing. The retention
    /// horizon is what makes that unreachable: a sidecar younger than the cutoff is never a candidate, no
    /// matter what does or does not point at it.</para>
    ///
    /// <para><b>Fails closed.</b> A surviving checkpoint that cannot be read means the referenced set is
    /// incomplete, and sweeping against an incomplete set deletes live data — so an unreadable survivor
    /// abandons the sweep rather than proceeding with what it managed to collect. The listing failing does
    /// the same.</para>
    ///
    /// <para>Unlike the commit prefix, order does not matter here: sidecars are named individually by the
    /// checkpoints that use them, so a gap in the set is not a thing a reader can trip over. A file that
    /// will not delete is therefore skipped rather than ending the pass.</para>
    /// </remarks>
    private static async ValueTask<int> SweepUnreferencedSidecarsAsync(
        TransactionLog log,
        List<(long Version, string Path, DateTimeOffset Modified)> candidates,
        int firstSurvivor,
        DateTimeOffset cutoff,
        CancellationToken cancellationToken)
    {
        // Listed FIRST, so a table that has never written a sidecar — every classic-checkpoint table, which
        // is most of them — pays one listing and stops, instead of reading checkpoint bodies to build a set
        // it would compare against nothing.
        var sidecars = new List<(string Path, DateTimeOffset Modified)>();
        try
        {
            await foreach (var file in log.FileSystem
                .ListAsync(DeltaVersion.SidecarPrefix, cancellationToken).ConfigureAwait(false))
            {
                sidecars.Add((file.Path, file.LastModified));
            }
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            return 0;
        }

        if (sidecars.Count == 0)
            return 0;

        var reader = new Checkpoint.CheckpointReader(log.FileSystem);
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        for (int i = firstSurvivor; i < candidates.Count; i++)
        {
            if (!DeltaVersion.TryParseCheckpointVersion(FileName(candidates[i].Path), out _))
                continue;

            try
            {
                var paths = await reader
                    .ReadSidecarPathsAsync(candidates[i].Path, cancellationToken).ConfigureAwait(false);
                foreach (string path in paths)
                    referenced.Add(Normalize(path));
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                return 0;
            }
        }

        int deleted = 0;
        foreach (var (path, modified) in sidecars)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Same reasoning as the commit pass: an age we cannot trust is not an age.
            if (modified <= UnknownModifiedThreshold || modified >= cutoff)
                continue;
            if (referenced.Contains(Normalize(path)))
                continue;

            try
            {
                await log.FileSystem.DeleteAsync(path, cancellationToken).ConfigureAwait(false);
                deleted++;
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                continue;
            }
        }

        return deleted;
    }

    /// <summary>
    /// A table-relative path in one spelling, so a reference resolved from a checkpoint action compares
    /// equal to the same file as a listing reports it. Only the separator differs in practice — an
    /// <see cref="IO.ITableFileSystem"/> over a local directory reports the platform's.
    /// </summary>
    private static string Normalize(string path) => path.Replace('\\', '/');

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
