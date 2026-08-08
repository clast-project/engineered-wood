// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using DeltaSnapshot = EngineeredWood.DeltaLake.Snapshot.Snapshot;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Table.Vacuum;

/// <summary>
/// Identifies and deletes unreferenced data files that are older than
/// the retention period.
/// </summary>
internal static class VacuumExecutor
{
    /// <summary>
    /// Finds unreferenced files and optionally deletes them.
    ///
    /// <para>A non-dry-run vacuum writes the Spark-parity <c>VACUUM START</c> / <c>VACUUM END</c>
    /// commitInfo-only commits around the physical deletes, so the operation is visible in the table
    /// history (auditability — and other engines can see WHY older versions stopped being physically
    /// readable). A dry run writes nothing.</para>
    /// </summary>
    public static async ValueTask<VacuumResult> ExecuteAsync(
        ITableFileSystem fs,
        TransactionLog log,
        DeltaSnapshot snapshot,
        TimeSpan retentionPeriod,
        bool dryRun,
        CancellationToken cancellationToken)
    {
        // ── Mark: the keep-set is the CURRENT version's files, plus their deletion vectors. ──
        //
        // Tombstones do NOT protect their files. That is measured, not assumed: Spark 4.0 /
        // delta-spark 4.0.0 delete a file orphaned seconds earlier under VACUUM RETAIN 0 HOURS, with
        // the tombstone still fresh in the log. It matches the documented contract — vacuum removes
        // files not referenced by the CURRENT version and thereby ends time travel past the retention
        // window. `delta.deletedFileRetentionDuration` governs the DEFAULT retention (applied by the
        // caller), not membership of this set.
        //
        // What each entry must contribute is its DATA path AND its deletion-vector path. Missing the
        // DV path is how a vacuum silently corrupts a table: the .bin disappears, the mask with it,
        // and every row it hid returns as live data with nothing logged anywhere.
        var keep = new HashSet<string>(StringComparer.Ordinal);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            // add.path is URL-encoded; decode to the on-disk name before comparing against the listing.
            keep.Add(EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path));
            AddDeletionVectorPath(keep, addFile.DeletionVector);
        }

        // A tombstone's DV is NOT kept — it masked rows in a file no longer part of the table, so it is
        // exactly the orphaned .bin this rewrite exists to collect. But a `p`-type vector anywhere in
        // the log still has to be rejected, because we cannot prove it lies outside the sweep.
        foreach (var tombstone in snapshot.Tombstones.Values)
            RejectIfAbsolute(tombstone.DeletionVector);

        // ── Sweep: everything under the table root, with NO extension filter. ──
        //
        // The old `.parquet`-only filter is why orphaned deletion_vector_*.bin leaked forever. Dropping
        // it means non-parquet files are now collectable, so the exclusions below carry real weight.
        // Keep-if-not-strictly-older matches delta-spark (`modificationTime < deleteBeforeTimestamp`),
        // including at the boundary. Caveat at NEAR-ZERO retention on .NET Framework: DateTime.UtcNow is
        // quantised to the ~15.6 ms system tick there, while an NTFS timestamp is finer, so a file
        // orphaned microseconds ago can compare NOT-older-than the cutoff and survive this pass. It is
        // collected by the next VACUUM once the clock ticks past it, so this costs a retry, not data.
        // .NET 8+ reads a precise clock and does not show it. Deliberately not "corrected" here: matching
        // Spark's comparison matters more than winning a sub-tick race, and any realistic retention
        // (hours/days) dwarfs the granularity.
        var cutoff = DateTimeOffset.UtcNow - retentionPeriod;
        var filesToDelete = new List<string>();
        long bytesToDelete = 0;

        await foreach (var file in fs.ListAsync("", cancellationToken).ConfigureAwait(false))
        {
            if (IsExcludedPath(file.Path, snapshot.Metadata.PartitionColumns))
                continue;

            if (keep.Contains(file.Path) || file.LastModified >= cutoff)
                continue;

            filesToDelete.Add(file.Path);
            bytesToDelete += file.Size;
        }

        int deleted = 0;
        if (!dryRun)
        {
            long startVersion = await WriteCommitInfoAsync(
                log, snapshot, "VACUUM START",
                new Dictionary<string, JsonElement>
                {
                    ["operationParameters"] = ParseJson(
                        $"{{\"retentionDurationMillis\":{(long)retentionPeriod.TotalMilliseconds}}}"),
                    ["operationMetrics"] = ParseJson(
                        $"{{\"numFilesToDelete\":\"{filesToDelete.Count}\",\"sizeOfDataToDelete\":\"{bytesToDelete}\"}}"),
                },
                firstCandidateVersion: snapshot.Version + 1,
                cancellationToken).ConfigureAwait(false);

            foreach (string path in filesToDelete)
            {
                await fs.DeleteAsync(path, cancellationToken).ConfigureAwait(false);
                deleted++;
            }

            await WriteCommitInfoAsync(
                log, snapshot, "VACUUM END",
                new Dictionary<string, JsonElement>
                {
                    ["operationParameters"] = ParseJson("{\"status\":\"COMPLETED\"}"),
                    ["operationMetrics"] = ParseJson(
                        $"{{\"numDeletedFiles\":\"{deleted}\",\"numVacuumedDirectories\":\"0\"}}"),
                },
                firstCandidateVersion: startVersion + 1,
                cancellationToken).ConfigureAwait(false);
        }

        return new VacuumResult
        {
            FilesToDelete = filesToDelete,
            FilesDeleted = deleted,
        };
    }

    /// <summary>
    /// Adds a deletion vector's backing file to the keep-set. Inline vectors live in the log and have
    /// no file of their own.
    /// </summary>
    private static void AddDeletionVectorPath(HashSet<string> keep, DeletionVector? dv)
    {
        RejectIfAbsolute(dv);

        string? path = dv is null ? null : DeletionVectorPath.GetRelativePath(dv);
        if (path is not null)
            keep.Add(path);
    }

    /// <summary>
    /// Absolute-path (<c>p</c>) vectors cannot be resolved against the table root from the action
    /// alone. Vacuum would have to guess whether each one lies inside the directory it is about to
    /// sweep, and a wrong guess deletes a live deletion vector — silently resurrecting every row it
    /// masks. Refusing is the only safe answer; EngineeredWood never writes <c>p</c> vectors, so a
    /// table containing them came from another engine.
    /// </summary>
    private static void RejectIfAbsolute(DeletionVector? dv)
    {
        if (dv is not null && DeletionVectorPath.IsAbsolute(dv))
        {
            throw new NotSupportedException(
                "This table contains absolute-path deletion vectors (storageType 'p'), whose targets "
                + "cannot be resolved against the table root. Vacuum cannot prove they lie outside the "
                + "swept directory, and deleting a live deletion vector would silently resurrect every "
                + "row it masks. Vacuum is refused for this table.");
        }
    }

    /// <summary>
    /// Whether vacuum must leave <paramref name="path"/> alone — the HIDDEN-NAME rule, applied to every
    /// component of the relative path.
    ///
    /// <para>A name beginning <c>_</c> or <c>.</c> is hidden by the Hadoop convention Delta inherits, and
    /// belongs to whoever wrote it: <c>_delta_log/</c> (governed by log cleanup, not vacuum),
    /// <c>_change_data/</c> (referenced by <c>cdc</c> actions, so never in <c>ActiveFiles</c> — sweeping it
    /// deletes live history), and anything a co-operating engine keeps beside the data, such as an index
    /// sidecar. None of them can be judged against this snapshot's active files, and a file vacuum cannot
    /// judge is a file it must not delete.</para>
    ///
    /// <para><b>⚠ PARTITION DIRECTORIES ARE THE EXCEPTION, and without it this rule silently stops
    /// collecting.</b> A partition column may be named with a leading underscore, so <c>_region=eu/</c> is a
    /// hidden NAME and live data — excluding it would make every orphan inside it immortal. Matched against
    /// the snapshot's declared partition columns rather than by looking for <c>=</c>, so an ordinary
    /// directory that happens to contain one is not mistaken for a partition.</para>
    ///
    /// <para><b>⚠ Applied PER COMPONENT and to FILES as well as directories</b>, which is what Delta does
    /// (<c>DeltaFileOperations.recursiveListDirs</c> filters on <c>getPath.getName</c> at every level, for
    /// both) — a listing that recurses would otherwise reach a hidden directory's contents by their full
    /// path and collect them one level down.</para>
    ///
    /// <para><b>Divergence from Delta, deliberately in the conservative direction:</b> Delta un-hides
    /// <c>_delta_index</c> and <c>_change_data</c> so that both ARE collected. Neither is safe here —
    /// there is no CDF keep-set (the snapshot does not track <c>cdc</c> actions), and an index this
    /// library does not write is one it cannot know is dead. Under-deleting leaves storage behind;
    /// over-deleting destroys another engine's data.</para>
    /// </summary>
    private static bool IsExcludedPath(string path, IReadOnlyList<string> partitionColumns)
    {
        foreach (var component in path.Split(PathSeparators, StringSplitOptions.RemoveEmptyEntries))
        {
            if (component.Length == 0)
                continue;
            if (component[0] != '_' && component[0] != '.')
                continue;
            // A partition directory is live data whatever it is called.
            bool isPartitionDir = false;
            foreach (var column in partitionColumns)
            {
                if (component.StartsWith(column + "=", StringComparison.Ordinal))
                {
                    isPartitionDir = true;
                    break;
                }
            }
            if (!isPartitionDir)
                return true;
        }
        return false;
    }

    private static readonly char[] PathSeparators = ['/', '\\'];

    // Writes a commitInfo-only commit, retrying past versions a concurrent writer takes (the commit carries
    // no data actions, so re-attempting at the next version is always safe). Returns the committed version.
    private static async ValueTask<long> WriteCommitInfoAsync(
        TransactionLog log, DeltaSnapshot snapshot, string operation,
        IDictionary<string, JsonElement> additionalValues, long firstCandidateVersion,
        CancellationToken cancellationToken)
    {
        var commitInfo = InCommitTimestamp.CreateCommitInfo(
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), operation, additionalValues,
            includeInCommitTimestamp: InCommitTimestamp.IsEnabled(snapshot.Metadata.Configuration));
        IReadOnlyList<DeltaAction> actions = new DeltaAction[] { commitInfo };

        const int maxAttempts = 16;
        long version = firstCandidateVersion;
        for (int attempt = 0; ; attempt++, version++)
        {
            try
            {
                await log.WriteCommitAsync(version, actions, cancellationToken).ConfigureAwait(false);
                return version;
            }
            catch (DeltaConflictException) when (attempt + 1 < maxAttempts)
            {
                // A concurrent writer took this version — try the next one.
            }
        }
    }

    private static JsonElement ParseJson(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }
}
