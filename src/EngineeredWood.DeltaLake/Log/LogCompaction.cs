// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Creates and reads log compaction files. A compacted file aggregates
/// the reconciled actions from a range of commits into a single NDJSON file,
/// allowing readers to skip individual commits in that range.
/// </summary>
public sealed class LogCompaction
{
    private readonly ITableFileSystem _fs;
    private readonly TransactionLog _log;

    public LogCompaction(ITableFileSystem fileSystem, TransactionLog log)
    {
        _fs = fileSystem;
        _log = log;
    }

    /// <summary>
    /// Creates a log compaction file for the commit range [startVersion, endVersion].
    /// The file contains the reconciled actions (add, remove, txn, domainMetadata)
    /// from all commits in the range. CommitInfo is excluded.
    /// </summary>
    public async ValueTask CompactRangeAsync(
        long startVersion, long endVersion,
        CancellationToken cancellationToken = default)
    {
        if (endVersion <= startVersion)
            throw new ArgumentException(
                $"endVersion ({endVersion}) must be greater than startVersion ({startVersion}).");

        // Read and reconcile all commits in the range
        var activeFiles = new Dictionary<string, AddFile>();
        var removedFiles = new Dictionary<string, RemoveFile>();
        var transactions = new Dictionary<string, TransactionId>();
        var domains = new Dictionary<string, DomainMetadata>();

        for (long v = startVersion; v <= endVersion; v++)
        {
            var commitActions = await _log.ReadCommitAsync(v, cancellationToken)
                .ConfigureAwait(false);

            foreach (var action in commitActions)
            {
                switch (action)
                {
                    case AddFile add:
                        activeFiles[add.ReconciliationKey] = add;
                        removedFiles.Remove(add.ReconciliationKey);
                        break;

                    case RemoveFile remove:
                        if (activeFiles.Remove(remove.ReconciliationKey))
                        {
                            // File was added then removed within this range —
                            // keep the remove as a tombstone
                        }
                        removedFiles[remove.ReconciliationKey] = remove;
                        break;

                    case TransactionId txn:
                        transactions[txn.AppId] = txn;
                        break;

                    case DomainMetadata dm:
                        if (dm.Removed)
                            domains.Remove(dm.Domain);
                        else
                            domains[dm.Domain] = dm;
                        break;

                    // CommitInfo, Protocol, Metadata — skip in compacted output
                    // (Protocol and Metadata are table-level; they appear in
                    // checkpoints, not compacted logs)
                }
            }
        }

        // Build the compacted action list
        var compactedActions = new List<DeltaAction>();

        foreach (var add in activeFiles.Values)
            compactedActions.Add(add);

        foreach (var remove in removedFiles.Values)
            compactedActions.Add(remove);

        foreach (var txn in transactions.Values)
            compactedActions.Add(txn);

        foreach (var dm in domains.Values)
            compactedActions.Add(dm);

        // Write the compacted file
        string path = DeltaVersion.CompactedPath(startVersion, endVersion);
        byte[] ndjson = ActionSerializer.Serialize(compactedActions);
        await _fs.WriteAllBytesAsync(path, ndjson, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Lists available compacted files in the log directory.
    /// Returns (startVersion, endVersion, path) tuples.
    /// </summary>
    public async ValueTask<IReadOnlyList<(long Start, long End, string Path)>>
        ListCompactedFilesAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<(long Start, long End, string Path)>();

        await foreach (var file in _fs.ListAsync(DeltaVersion.LogPrefix, cancellationToken)
            .ConfigureAwait(false))
        {
            string fileName = Path.GetFileName(file.Path);
            if (DeltaVersion.TryParseCompactedRange(fileName, out long start, out long end))
                result.Add((start, end, file.Path));
        }

        return result;
    }

    /// <summary>
    /// Reads the actions from a compacted file.
    /// </summary>
    public async ValueTask<IReadOnlyList<DeltaAction>> ReadCompactedAsync(
        string path, CancellationToken cancellationToken = default)
    {
        byte[] data = await _fs.ReadAllBytesAsync(path, cancellationToken)
            .ConfigureAwait(false);
        return ActionSerializer.Deserialize(data);
    }
}
