// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Concurrency;
using EngineeredWood.DeltaLake.DeletionVectors;
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
    /// included, resolved as <see cref="GetSnapshotAtTimestampAsync"/> resolves it. Otherwise as <see cref="RestoreAsync(long, RestoreOptions?, CancellationToken, CommitProperties?)"/>.
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
        var commits = await ReadCommitTimestampsAsync(cancellationToken).ConfigureAwait(false);
        var readable = (await _log.ReadVersionsAsync(cancellationToken).ConfigureAwait(false)).ReadableVersions;
        var readableSet = new HashSet<long>(readable);
        var recreatable = commits.Where(c => readableSet.Contains(c.Version)).ToList();

        if (recreatable.Count == 0 || requested < recreatable[0].Timestamp)
        {
            string earliest = recreatable.Count == 0 ? "none" : FormatTimestamp(recreatable[0].Timestamp);
            throw new DeltaFormatException(
                DeltaTableErrorCodes.CannotRestoreTimestampEarlier,
                $"Cannot restore table to timestamp ({FormatTimestamp(requested)}) as it is before the earliest version "
                + $"available. Please use a timestamp after ({earliest}).");
        }
        if (requested > commits[^1].Timestamp)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.CannotRestoreTimestampGreater,
                $"Cannot restore table to timestamp ({FormatTimestamp(requested)}) as it is after the latest version "
                + $"available. Please use a timestamp before ({FormatTimestamp(commits[^1].Timestamp)})");
        }

        long version = recreatable.Last(c => c.Timestamp <= requested).Version;
        return await RestoreCoreAsync(
            version, timestampText: timestamp.ToString("o", CultureInfo.InvariantCulture),
            options ?? RestoreOptions.Default, commitProperties, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<RestoreResult> RestoreCoreAsync(
        long version, string? timestampText, RestoreOptions options, CommitProperties? commitProperties,
        CancellationToken cancellationToken)
    {
        // From the latest version, as Spark restores: the commit diffs against the state it lands on, and cannot
        // be rebased past anything, so a stale handle could only ever fail.
        var current = await SnapshotBuilder.UpdateAsync(CurrentSnapshot, _log, cancellationToken)
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
        var actions = new List<DeltaAction>(toAdd.Count + toRemove.Count + 4)
        {
            RestoredMetadata(current, target),
        };
        if (!ProtocolEquals(protocol, current.Protocol))
            actions.Add(protocol);

        bool assignRowIds = protocol.WriterFeatures?.Contains("rowTracking") == true;
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
        // Spark records the one it was given and a null for the other; a null has no place in these string
        // parameters, so the other is left out.
        var parameters = new Dictionary<string, string>(StringComparer.Ordinal);
        if (timestampText is null)
            parameters["version"] = Invariant(version);
        else
            parameters["timestamp"] = timestampText;

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

        var currentFields = current.Schema.Fields
            .Where(IdentityColumn.IsIdentityColumn)
            .ToDictionary(f => f.Name, StringComparer.Ordinal);
        bool schemaChanged = false;
        var fields = new List<StructField>(target.Schema.Fields.Count);
        foreach (var field in target.Schema.Fields)
        {
            var merged = field;
            if (IdentityColumn.GetConfig(field) is { } targetConfig
                && currentFields.TryGetValue(field.Name, out var currentField)
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
            if (file.DeletionVector is { } dv && DeletionVectorPath.GetRelativePath(dv) is { } dvPath)
                paths.Add(dvPath);
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

    /// <summary>
    /// Each commit file's version and timestamp, ascending: the in-commit timestamp where the commit carries one,
    /// else <c>commitInfo.timestamp</c> (so a timestamp read from history resolves back to its version), else the
    /// commit file's modification time; raised where needed to one past the previous commit's so the sequence never
    /// goes backwards, as Spark does. Spark uses the modification time where this library uses
    /// <c>commitInfo.timestamp</c>; the two differ by the time the commit took to write, unless the log was copied.
    /// </summary>
    private async ValueTask<List<(long Version, long Timestamp)>> ReadCommitTimestampsAsync(
        CancellationToken cancellationToken)
    {
        var modified = new Dictionary<long, long>();
        await foreach (var file in _fs.ListAsync(DeltaVersion.LogPrefix, cancellationToken).ConfigureAwait(false))
        {
            if (DeltaVersion.TryParseCommitVersion(Path.GetFileName(file.Path), out long v))
                modified[v] = file.LastModified.ToUnixTimeMilliseconds();
        }

        var result = new List<(long Version, long Timestamp)>(modified.Count);
        long? previous = null;
        foreach (long v in modified.Keys.OrderBy(v => v))
        {
            var actions = await _log.ReadCommitAsync(v, cancellationToken).ConfigureAwait(false);
            long timestamp = Log.InCommitTimestamp.GetTimestampFromActions(actions) ?? modified[v];
            if (previous is { } p && timestamp <= p)
                timestamp = p + 1;
            result.Add((v, timestamp));
            previous = timestamp;
        }
        return result;
    }

    private static string FormatTimestamp(long unixMilliseconds) =>
        DateTimeOffset.FromUnixTimeMilliseconds(unixMilliseconds)
            .ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);

    private static string Invariant(long value) => value.ToString(CultureInfo.InvariantCulture);
}
