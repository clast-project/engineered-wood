// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Concurrency;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Snapshot;

namespace EngineeredWood.DeltaLake.Table;

public sealed partial class DeltaTable
{
    private const int MissingFilesReported = 100;
    private const int ExistenceChecksInFlight = 32;

    /// <summary>
    /// Restores the table to an earlier version: <c>RESTORE TABLE … TO VERSION AS OF version</c>. The restore is a
    /// new commit, not a rewrite of the log, so it can itself be undone, and history keeps every version.
    ///
    /// <para>The commit removes every file active now but not in <paramref name="version"/>, and re-adds every
    /// file active there but not now, exactly as that version recorded it: its statistics, deletion vector and
    /// row ids. Files are compared by path and deletion vector, so a file whose deletion vector changed since is
    /// removed under the new vector and re-added under the old one. No data file is read or written.</para>
    ///
    /// <para>As Spark restores it, measured against Spark 4.0 / Delta 4.0.0:</para>
    /// <list type="bullet">
    /// <item>The metadata is the restored version's (its schema, partitioning and properties), except that an
    /// identity column's high-water mark never moves back, so identity values are never reissued, and the names
    /// of row tracking's materialized columns are kept. This library also keeps
    /// <c>delta.columnMapping.maxColumnId</c> from moving back, so a later ADD COLUMN cannot reuse a column
    /// id that files written since may carry.</item>
    /// <item>The protocol is never downgraded: it is the union of the current one and the restored version's.</item>
    /// <item>Of the domain metadata, only liquid clustering's columns are restored. Other domains, and row
    /// tracking's high-water mark, are left as they are.</item>
    /// <item>A re-added file keeps its row ids. One that has none, from before row tracking was enabled, is
    /// given fresh ones when the protocol supports row tracking.</item>
    /// <item><c>delta.appendOnly</c> does not prevent it.</item>
    /// <item>No change data is written: a change-feed reader derives the changes from the adds and removes.</item>
    /// <item>Any commit that lands while it runs makes it fail, so it never lands anywhere but directly on the
    /// state it diffed against. Retry it.</item>
    /// </list>
    ///
    /// <para>Restoring to the table's current version commits nothing.</para>
    /// </summary>
    /// <param name="version">The version to restore.</param>
    /// <param name="options">Restore options; null takes <see cref="RestoreOptions.Default"/>.</param>
    /// <param name="cancellationToken">Cancels the restore.</param>
    /// <param name="commitProperties">The caller's own metadata for the commit.</param>
    /// <returns>The committed version and Spark's restore metrics.</returns>
    /// <exception cref="DeltaFormatException">The version cannot be read
    /// (<see cref="DeltaTableErrorCodes.CannotRestoreTableVersion"/>), or a file it would re-add is missing
    /// (<see cref="DeltaTableErrorCodes.RestoreMissingFiles"/>).</exception>
    /// <exception cref="DeltaConflictException">Another commit landed while the restore ran.</exception>
    public async ValueTask<RestoreResult> RestoreAsync(
        long version, RestoreOptions? options = null, CancellationToken cancellationToken = default,
        CommitProperties? commitProperties = null)
    {
        ThrowIfDisposed();
        return await RestoreCoreAsync(version, timestampText: null, options ?? RestoreOptions.Default,
            commitProperties, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Restores the table to the version that was current at <paramref name="timestamp"/>: <c>RESTORE TABLE … TO
    /// TIMESTAMP AS OF timestamp</c>. That is the latest version committed at or before it, an exact match
    /// included, resolved as <see cref="GetSnapshotAtTimestampAsync"/> resolves it. Only versions with a commit file
    /// count: one that log cleanup left as a checkpoint alone has no commit time, and, as in Spark, the earliest
    /// restorable time is then the first surviving commit's (it can still be restored by version). Otherwise as <see cref="RestoreAsync(long, RestoreOptions?, CancellationToken, CommitProperties?)"/>.
    /// </summary>
    /// <exception cref="DeltaFormatException">The timestamp is before the oldest readable version
    /// (<see cref="DeltaTableErrorCodes.CannotRestoreTimestampEarlier"/>) or after the latest commit
    /// (<see cref="DeltaTableErrorCodes.CannotRestoreTimestampGreater"/>), or as for a restore by version.</exception>
    public async ValueTask<RestoreResult> RestoreAsync(
        DateTimeOffset timestamp, RestoreOptions? options = null, CancellationToken cancellationToken = default,
        CommitProperties? commitProperties = null)
    {
        ThrowIfDisposed();
        long requested = timestamp.ToUnixTimeMilliseconds();
        var commits = await ListCommitsAsync(cancellationToken).ConfigureAwait(false);
        var readable = (await _log.ReadVersionsAsync(cancellationToken).ConfigureAwait(false)).ReadableVersions;
        var readableSet = new HashSet<long>(readable);

        // The earliest version that can still be reconstructed bounds the request from below, as in Spark. Usually
        // the first commit, so this walk usually reads one.
        (long Version, long Timestamp)? earliestReadable = null;
        await foreach (var commit in ReadCommitTimestampsAsync(commits, commits.Count, cancellationToken)
                           .ConfigureAwait(false))
        {
            if (readableSet.Contains(commit.Version))
            {
                earliestReadable = commit;
                break;
            }
        }

        // Against every commit, not only the readable ones: a timestamp that lands on a version the log can no
        // longer reconstruct must be refused as that version, not quietly moved to an older one that can be.
        (long Version, long Timestamp)? match = null;
        if (earliestReadable is { } earliestCommit && requested >= earliestCommit.Timestamp)
        {
            match = await FindCommitAtTimestampAsync(
                    commits, await LatestConfigurationAsync(commits, cancellationToken).ConfigureAwait(false), requested,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        // Null where the earliest readable commit is after the request, or (a table whose in-commit timestamps
        // contradict its enablement properties) where the search finds nothing; either way nothing is early enough.
        if (match is not { } found)
        {
            string earliest = earliestReadable is { } e ? FormatTimestamp(e.Timestamp) : "none";
            throw new DeltaFormatException(
                DeltaTableErrorCodes.CannotRestoreTimestampEarlier,
                $"Cannot restore table to timestamp ({FormatTimestamp(requested)}) as it is before the earliest version "
                + $"available. Please use a timestamp after ({earliest}).");
        }

        // After the latest commit exactly when the answer is the latest commit and is earlier than the request.
        if (found.Version == commits[commits.Count - 1].Version && requested > found.Timestamp)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.CannotRestoreTimestampGreater,
                $"Cannot restore table to timestamp ({FormatTimestamp(requested)}) as it is after the latest version "
                + $"available. Please use a timestamp before ({FormatTimestamp(found.Timestamp)})");
        }

        long version = found.Version;
        return await RestoreCoreAsync(
            version, timestampText: timestamp.ToString("o", CultureInfo.InvariantCulture),
            options ?? RestoreOptions.Default, commitProperties, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<RestoreResult> RestoreCoreAsync(
        long version, string? timestampText, RestoreOptions options, CommitProperties? commitProperties,
        CancellationToken cancellationToken)
    {
        // From the latest version, as Spark restores: the commit diffs against the state it lands on, and cannot
        // be rebased past anything, so a stale handle could only ever fail. From the latest checkpoint where log
        // cleanup removed commits after this handle's version (#697); the restore then commits on that fresh base.
        var current = await SnapshotBuilder.UpdateOrRebuildAsync(CurrentSnapshot, _log, _checkpointReader, cancellationToken)
            .ConfigureAwait(false);
        ValidateWriteSupport(current);

        var readable = (await _log.ReadVersionsAsync(cancellationToken).ConfigureAwait(false)).ReadableVersions;
        if (version < 0 || version > current.Version || !readable.Contains(version))
        {
            long earliest = readable.Count > 0 ? readable[0] : current.Version;
            throw new DeltaFormatException(
                DeltaTableErrorCodes.CannotRestoreTableVersion,
                $"Cannot restore table to version {version}. Available versions: [{earliest}, {current.Version}].");
        }

        if (version == current.Version)
        {
            // The handle reports the version the result names, as it would after a commit.
            _currentSnapshot = current;
            return new RestoreResult
            {
                Version = current.Version,
                Committed = false,
                RestoredVersion = version,
                NumRestoredFiles = 0,
                NumRemovedFiles = 0,
                RestoredFilesSize = 0,
                RemovedFilesSize = 0,
                NumOfFilesAfterRestore = current.ActiveFiles.Count,
                TableSizeAfterRestore = current.ActiveFiles.Values.Sum(f => f.Size),
            };
        }

        var target = await GetSnapshotAtVersionAsync(version, cancellationToken).ConfigureAwait(false);

        // Keyed by path and deletion vector, as both snapshots' active-file maps already are.
        var toAdd = target.ActiveFiles
            .Where(pair => !current.ActiveFiles.ContainsKey(pair.Key))
            .Select(pair => pair.Value)
            .ToList();
        var toRemove = current.ActiveFiles
            .Where(pair => !target.ActiveFiles.ContainsKey(pair.Key))
            .Select(pair => pair.Value)
            .ToList();

        if (!options.IgnoreMissingFiles)
            await EnsureRestoredFilesExistAsync(toAdd, version, cancellationToken).ConfigureAwait(false);

        var protocol = MergeReplacementProtocol(current.Protocol, target.Protocol);
        ProtocolVersions.ValidateWriteSupport(protocol);

        long restoreVersion = current.Version + 1;
        var metadata = RestoredMetadata(current, target);
        // The restored properties are the table's from this commit on, so they are judged as any new metadata is:
        // a restore must not re-enable UniForm, whose metadata this library does not maintain.
        EnsureUniversalFormatMaintained(metadata.Configuration, _options);
        // A restore can bring back IcebergCompat over a protocol that has since gained deletion vectors (#744).
        IcebergCompat.Validate(IcebergCompat.GetVersion(metadata.Configuration), metadata, protocol);

        // Spark records the one it was given and a null for the other; a null has no place in these string
        // parameters, so the other is left out.
        var parameters = new Dictionary<string, string>(StringComparer.Ordinal);
        if (timestampText is null)
            parameters["version"] = Invariant(version);
        else
            parameters["timestamp"] = timestampText;

        var actions = new List<DeltaAction>(toAdd.Count + toRemove.Count + 5);
        // A restore that turns in-commit timestamps back on is where they begin again. The committer sees that this
        // metadata enables them and the current one does not, so it timestamps the commit and stamps the enablement
        // point, as it does for any commit that enables them (#598).
        actions.Add(metadata);
        if (!ProtocolEquals(protocol, current.Protocol))
            actions.Add(protocol);

        bool assignRowIds = DeltaLake.RowTracking.RowTrackingConfig.IsSupported(protocol);
        long nextRowId = current.RowIdHighWaterMark;
        foreach (var add in toAdd)
        {
            var restored = add with { DataChange = true };
            if (assignRowIds && restored.BaseRowId is null)
            {
                restored = restored with { BaseRowId = nextRowId };
                nextRowId += RowCountForRowIds(add);
            }
            if (assignRowIds && restored.DefaultRowCommitVersion is null)
                restored = restored with { DefaultRowCommitVersion = restoreVersion };
            actions.Add(restored);
        }
        if (assignRowIds && nextRowId > current.RowIdHighWaterMark)
            actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));

        foreach (var remove in toRemove)
            actions.Add(RemoveOf(remove));

        if (RestoredClusteringDomain(current, target) is { } clustering)
            actions.Add(clustering);

        long restoredSize = toAdd.Sum(f => f.Size);
        long removedSize = toRemove.Sum(f => f.Size);
        var result = new RestoreResult
        {
            Version = -1,
            Committed = true,
            RestoredVersion = version,
            NumRestoredFiles = toAdd.Count,
            NumRemovedFiles = toRemove.Count,
            RestoredFilesSize = restoredSize,
            RemovedFilesSize = removedSize,
            NumOfFilesAfterRestore = target.ActiveFiles.Count,
            TableSizeAfterRestore = target.ActiveFiles.Values.Sum(f => f.Size),
        };
        var metrics = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["numRestoredFiles"] = Invariant(result.NumRestoredFiles),
            ["numRemovedFiles"] = Invariant(result.NumRemovedFiles),
            ["restoredFilesSize"] = Invariant(result.RestoredFilesSize),
            ["removedFilesSize"] = Invariant(result.RemovedFilesSize),
            ["numOfFilesAfterRestore"] = Invariant(result.NumOfFilesAfterRestore),
            ["tableSizeAfterRestore"] = Invariant(result.TableSizeAfterRestore),
        };
        // Not rebase-safe, whatever landed: the actions are a diff against `current`, so moving them past any
        // commit that touched files would leave the table at neither version. Spark makes one attempt for the
        // same reason (commitLarge). The whole-table read set makes a concurrent data change report as the
        // conflict it is, rather than as a rebase refusal.
        long committed = await CommitOccAsync(
            current, actions, new ReadSet { WholeTable = true }, IsolationLevel.Serializable, "RESTORE",
            rebaseSafe: false, cancellationToken, isBlindAppend: false, commitProperties: commitProperties,
            operationMetrics: _ => metrics, operationParameters: parameters).ConfigureAwait(false);

        return result with { Version = committed };
    }

    /// <summary>
    /// The restored version's metadata, with what must not move backwards carried forward from the current one:
    /// identity high-water marks (Spark's <c>copySchemaWithMergedHighWaterMarks</c>), row tracking's materialized
    /// column names (Spark keeps them), and <c>delta.columnMapping.maxColumnId</c>.
    /// </summary>
    private static MetadataAction RestoredMetadata(Snapshot.Snapshot current, Snapshot.Snapshot target)
    {
        var metadata = target.Metadata;

        // Matched by physical name, which a column-mapped RENAME does not change, so a column renamed since the
        // target version still carries its mark back; without mapping the physical name is the logical one.
        var currentMode = ColumnMapping.GetMode(current.Metadata.Configuration);
        var targetMode = ColumnMapping.GetMode(target.Metadata.Configuration);
        var currentFields = current.Schema.Fields
            .Where(IdentityColumn.IsIdentityColumn)
            .GroupBy(f => ColumnMapping.GetPhysicalName(f, currentMode), StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
        bool schemaChanged = false;
        var fields = new List<StructField>(target.Schema.Fields.Count);
        foreach (var field in target.Schema.Fields)
        {
            var merged = field;
            if (IdentityColumn.GetConfig(field) is { } targetConfig
                && currentFields.TryGetValue(ColumnMapping.GetPhysicalName(field, targetMode), out var currentField)
                && IdentityColumn.GetConfig(currentField)?.HighWaterMark is { } currentMark)
            {
                long mark = targetConfig.HighWaterMark is not { } targetMark
                    ? currentMark
                    : targetConfig.Step > 0 ? Math.Max(targetMark, currentMark) : Math.Min(targetMark, currentMark);
                if (mark != targetConfig.HighWaterMark)
                {
                    merged = IdentityColumn.UpdateHighWaterMark(field, mark);
                    schemaChanged = true;
                }
            }
            fields.Add(merged);
        }
        if (schemaChanged)
        {
            metadata = metadata with
            {
                SchemaString = DeltaSchemaSerializer.Serialize(new StructType { Fields = fields }),
            };
        }

        var currentConfiguration = current.Metadata.Configuration;
        if (currentConfiguration is null)
            return metadata;

        Dictionary<string, string>? configuration = null;
        Dictionary<string, string> Configuration() => configuration ??= metadata.Configuration is null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : new Dictionary<string, string>(metadata.Configuration.ToDictionary(p => p.Key, p => p.Value), StringComparer.Ordinal);

        foreach (string key in new[]
                 {
                     DeltaLake.RowTracking.RowTrackingConfig.MaterializedRowIdColumnNameKey,
                     DeltaLake.RowTracking.RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey,
                 })
        {
            if (currentConfiguration.TryGetValue(key, out string? name)
                && (metadata.Configuration is null || !metadata.Configuration.TryGetValue(key, out string? had) || had != name))
            {
                Configuration()[key] = name;
            }
        }

        if (currentConfiguration.TryGetValue(ColumnMapping.MaxColumnIdKey, out string? currentMax)
            && long.TryParse(currentMax, NumberStyles.Integer, CultureInfo.InvariantCulture, out long currentMaxId))
        {
            string? targetMax = null;
            bool hasTarget = metadata.Configuration?.TryGetValue(ColumnMapping.MaxColumnIdKey, out targetMax) == true;
            if (!hasTarget
                || !long.TryParse(targetMax, NumberStyles.Integer, CultureInfo.InvariantCulture, out long targetMaxId)
                || targetMaxId < currentMaxId)
            {
                Configuration()[ColumnMapping.MaxColumnIdKey] = currentMax;
            }
        }

        return configuration is null ? metadata : metadata with { Configuration = configuration };
    }

    /// <summary>
    /// The <c>delta.clustering</c> domain the restore must write, or null when it already matches: the restored
    /// version's columns, or a tombstone when that version was not clustered. Spark restores this domain and no
    /// other (<c>DomainMetadataUtils.handleDomainMetadataForRestoreTable</c>).
    /// </summary>
    private static DomainMetadata? RestoredClusteringDomain(Snapshot.Snapshot current, Snapshot.Snapshot target)
    {
        current.DomainMetadata.TryGetValue(ClusteringDomain, out var now);
        target.DomainMetadata.TryGetValue(ClusteringDomain, out var then);
        if (now is { Removed: true })
            now = null;
        if (then is { Removed: true })
            then = null;

        if (then is not null)
            return now is not null && now.Configuration == then.Configuration ? null : then;
        return now is null ? null : new DomainMetadata { Domain = ClusteringDomain, Configuration = "{}", Removed = true };
    }

    /// <summary>
    /// Refuses the restore when a file it would re-add is gone, naming the first hundred, as Spark does. Spark checks
    /// only the data files; a deletion vector's file is checked here too, since without it the restored table
    /// cannot be read either.
    /// </summary>
    private async ValueTask EnsureRestoredFilesExistAsync(
        IReadOnlyList<AddFile> files, long version, CancellationToken cancellationToken)
    {
        var paths = new List<string>(files.Count);
        foreach (var file in files)
        {
            paths.Add(DeltaPath.Decode(file.Path));
            if (file.DeletionVector is not { } dv)
                continue;
            // An absolute-path vector is read from its path as written, so it is checked the same way.
            if (DeletionVectorPath.GetRelativePath(dv) is { } dvPath)
                paths.Add(dvPath);
            else if (DeletionVectorPath.IsAbsolute(dv))
                paths.Add(dv.PathOrInlineDv);
        }

        var missing = new List<string>();
        for (int start = 0; start < paths.Count; start += ExistenceChecksInFlight)
        {
            var batch = paths.Skip(start).Take(ExistenceChecksInFlight).ToList();
            var exists = await Task.WhenAll(
                batch.Select(p => _fs.ExistsAsync(p, cancellationToken).AsTask())).ConfigureAwait(false);
            for (int i = 0; i < batch.Count; i++)
            {
                if (!exists[i])
                    missing.Add(batch[i]);
            }
        }

        if (missing.Count > 0)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.RestoreMissingFiles,
                $"Not all files from version {version} are available in file system. Missed files (top "
                + $"{MissingFilesReported} files): {string.Join(",", missing.Take(MissingFilesReported))}. Please use "
                + "a more recent version or timestamp for restoring. To restore without this check, set "
                + $"{nameof(RestoreOptions)}.{nameof(RestoreOptions.IgnoreMissingFiles)}; the missing files are then "
                + "re-added regardless, and reading them fails.");
        }
    }

    /// <summary>The versions of the log's commit files, ascending, with each file's modification time. One listing.</summary>
    private async ValueTask<List<(long Version, long Modified)>> ListCommitsAsync(CancellationToken cancellationToken)
    {
        var modified = new Dictionary<long, long>();
        await foreach (var file in _fs.ListAsync(DeltaVersion.LogPrefix, cancellationToken).ConfigureAwait(false))
        {
            if (DeltaVersion.TryParseCommitVersion(Path.GetFileName(file.Path), out long v))
                modified[v] = file.LastModified.ToUnixTimeMilliseconds();
        }
        var commits = new List<(long Version, long Modified)>(modified.Count);
        foreach (var pair in modified.OrderBy(p => p.Key))
            commits.Add((pair.Key, pair.Value));
        return commits;
    }

    /// <summary>
    /// The version and timestamp of each of <paramref name="commits"/> before index <paramref name="end"/>, ascending:
    /// the in-commit timestamp where the commit carries one, else <c>commitInfo.timestamp</c> (so a timestamp read
    /// from history resolves back to its version), else the commit file's modification time; raised where needed to
    /// one past the previous commit's so the sequence never goes backwards, as Spark does. Spark uses the modification
    /// time where this library uses <c>commitInfo.timestamp</c>; the two differ by the time the commit took to write,
    /// unless the log was copied.
    /// </summary>
    /// <remarks>Lazy: each commit is read only when it is reached, so a caller looking for an early version stops
    /// reading there. Always from the first commit, since each adjustment depends on every timestamp before it.</remarks>
    private async IAsyncEnumerable<(long Version, long Timestamp)> ReadCommitTimestampsAsync(
        List<(long Version, long Modified)> commits, int end,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long? previous = null;
        for (int i = 0; i < end; i++)
        {
            var (v, modified) = commits[i];
            var actions = await _log.ReadCommitAsync(v, cancellationToken).ConfigureAwait(false);
            long timestamp = InCommitTimestamp.GetTimestampFromActions(actions) ?? modified;
            if (previous is { } p && timestamp <= p)
                timestamp = p + 1;
            previous = timestamp;
            yield return (v, timestamp);
        }
    }

    /// <summary>
    /// The table configuration as of the newest of <paramref name="commits"/> or later: the handle's own when it is
    /// that current, else that of a snapshot brought up to date for the purpose (the handle does not move).
    /// </summary>
    /// <remarks>
    /// <para>A later configuration than the listing's is harmless: one that turns in-commit timestamps off makes the
    /// caller walk, and one that turns them on past the listing leaves it no commit to search.</para>
    ///
    /// <para><b>Incrementally where it can, from a checkpoint where it must</b> (#690 review). The incremental update
    /// is the cheap path, but it refuses a gap after the handle's version, which is what log cleanup leaves once a
    /// checkpoint covers the commits it removed. That table is still readable, so time travel on it must not fail
    /// because this handle predates the cleanup: the latest snapshot is then built from the checkpoint
    /// (<see cref="SnapshotBuilder.UpdateOrRebuildAsync"/>).</para>
    /// </remarks>
    private async ValueTask<IReadOnlyDictionary<string, string>?> LatestConfigurationAsync(
        List<(long Version, long Modified)> commits, CancellationToken cancellationToken)
    {
        var snapshot = CurrentSnapshot;
        if (commits.Count == 0 || commits[commits.Count - 1].Version <= snapshot.Version)
            return snapshot.Metadata.Configuration;

        snapshot = await SnapshotBuilder.UpdateOrRebuildAsync(snapshot, _log, _checkpointReader, cancellationToken)
            .ConfigureAwait(false);
        return snapshot.Metadata.Configuration;
    }

    /// <summary>
    /// The latest of <paramref name="commits"/> committed at or before <paramref name="targetMs"/>, with its
    /// timestamp; null when every commit is later.
    /// </summary>
    /// <remarks>
    /// <para><b>Binary-searched over the in-commit-timestamp range</b> (#662) when <paramref name="configuration"/> has
    /// them on: from <see cref="InCommitTimestamp.EnablementVersionKey"/> (or the first commit, when they were on from
    /// creation) to the latest. There a commit's time is its own <c>inCommitTimestamp</c>, which increases by
    /// construction, so the search reads O(log N) commits where the walk reads up to N. This is delta-spark's split
    /// (<c>DeltaHistoryManager.getActiveCommitAtTimeFromICTRange</c>): inside the range the in-commit timestamps are
    /// authoritative, and the walk with its adjustment applies only below it.</para>
    ///
    /// <para><b>Below the range.</b> A target before <see cref="InCommitTimestamp.EnablementTimestampKey"/> walks only
    /// the commits before the enablement version; the configuration says so without a read.</para>
    ///
    /// <para><b>⚠ <paramref name="configuration"/> must be the latest version's</b> (<see cref="LatestConfigurationAsync"/>),
    /// not a stale handle's. The range is only increasing from the version that LAST turned them on: a stale one can
    /// reach back over a later disable and re-enable, and if the re-enabled run was stamped by a clock set back, the
    /// search can skip the unstamped gap and answer from the earlier run (#690 review).</para>
    ///
    /// <para><b>A writer that never recorded where they began.</b> With no enablement recorded, the first listed commit
    /// is read first: one without an <c>inCommitTimestamp</c> means the range is not what the configuration claims,
    /// and the whole log is walked as before. A probe without one does the same.</para>
    /// </remarks>
    private async ValueTask<(long Version, long Timestamp)?> FindCommitAtTimestampAsync(
        List<(long Version, long Modified)> commits, IReadOnlyDictionary<string, string>? configuration,
        long targetMs, CancellationToken cancellationToken)
    {
        int end = commits.Count;
        if (InCommitTimestamp.IsEnabled(configuration))
        {
            var enablement = InCommitTimestamp.GetEnablement(configuration);
            int rangeStart = 0;
            if (enablement is { } e)
            {
                while (rangeStart < commits.Count && commits[rangeStart].Version < e.Version)
                    rangeStart++;
            }

            InCommitTimestampSearch search;
            if (enablement is { } en && targetMs < en.Timestamp)
            {
                search = InCommitTimestampSearch.BeforeFirst;
            }
            else if (enablement is null && commits.Count > 0
                     && InCommitTimestamp.GetStrictTimestamp(
                         await _log.ReadCommitAsync(commits[0].Version, cancellationToken).ConfigureAwait(false))
                         is null)
            {
                // No enablement recorded claims every commit is stamped. A first commit that is not says a writer
                // turned them on mid-life without recording where, and its unstamped commits form a prefix of the
                // listing (cleanup removes the oldest first), so this one read finds them. The search could otherwise
                // probe only the stamped suffix and miss that the walk raises it past a later informational
                // timestamp before it (#690 review).
                search = InCommitTimestampSearch.Unsearchable;
            }
            else
            {
                var versions = new List<long>(commits.Count - rangeStart);
                for (int i = rangeStart; i < commits.Count; i++)
                    versions.Add(commits[i].Version);
                search = await InCommitTimestamp.SearchAsync(_log, versions, targetMs, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (search.Outcome == InCommitTimestampSearchOutcome.Found)
                return (search.Version, search.Timestamp);
            if (search.Outcome == InCommitTimestampSearchOutcome.BeforeFirst)
                end = rangeStart; // only a commit before the range can be at or before the target
            // Unsearchable: walk every commit.
        }

        (long Version, long Timestamp)? best = null;
        await foreach (var commit in ReadCommitTimestampsAsync(commits, end, cancellationToken).ConfigureAwait(false))
        {
            if (commit.Timestamp > targetMs)
                break; // the sequence never goes backwards
            best = commit;
        }
        return best;
    }

    private static string FormatTimestamp(long unixMilliseconds) =>
        DateTimeOffset.FromUnixTimeMilliseconds(unixMilliseconds)
            .ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);

    private static string Invariant(long value) => value.ToString(CultureInfo.InvariantCulture);

}
