// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Reads and writes Delta Lake transaction log files from the
/// <c>_delta_log/</c> directory of a table.
/// </summary>
public sealed class TransactionLog
{
    private readonly ITableFileSystem _fs;

    /// <summary>Gets the underlying filesystem.</summary>
    internal ITableFileSystem FileSystem => _fs;

    /// <summary>
    /// Creates a new <see cref="TransactionLog"/> for the table at the given root.
    /// The <paramref name="fileSystem"/> should be rooted at the table directory.
    /// </summary>
    public TransactionLog(ITableFileSystem fileSystem)
    {
        _fs = fileSystem;
    }

    /// <summary>
    /// Reads all actions from a single commit file (NDJSON).
    /// </summary>
    public async ValueTask<IReadOnlyList<DeltaAction>> ReadCommitAsync(
        long version, CancellationToken cancellationToken = default)
    {
        string path = DeltaVersion.CommitPath(version);
        byte[] data = await _fs.ReadAllBytesAsync(path, cancellationToken)
            .ConfigureAwait(false);
        return ActionSerializer.Deserialize(data);
    }

    /// <summary>
    /// Atomically creates a commit file with its complete contents.
    /// Throws <see cref="DeltaConflictException"/> if the version already exists.
    /// </summary>
    public async ValueTask WriteCommitAsync(
        long version, IReadOnlyList<DeltaAction> actions,
        CancellationToken cancellationToken = default)
    {
        string targetPath = DeltaVersion.CommitPath(version);
        byte[] data = ActionSerializer.Serialize(actions);

        bool written = await _fs.TryWriteAllBytesAsync(targetPath, data, cancellationToken)
            .ConfigureAwait(false);
        if (!written)
        {
            throw new DeltaConflictException(version);
        }
    }

    /// <summary>
    /// Lists available commit versions in the log directory, starting from
    /// <paramref name="startVersion"/>, in ASCENDING order. The underlying directory listing's order is
    /// filesystem-dependent (Windows/S3/ADLS list sorted; Linux readdir returns inode-hash order), and the
    /// callers depend on ascending replay — snapshot reconciliation's latest-wins metadata/protocol,
    /// timestamp resolution's monotonic early-break, the history view — so the versions are materialized
    /// and sorted here (the log directory is bounded by the checkpoint interval).
    /// </summary>
    public async IAsyncEnumerable<long> ListVersionsAsync(
        long startVersion = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var versions = new List<long>();
        await foreach (var file in _fs.ListAsync(DeltaVersion.LogPrefix, cancellationToken)
            .ConfigureAwait(false))
        {
            string fileName = file.Path;
            if (DeltaVersion.TryParseCommitVersion(
                    Path.GetFileName(fileName), out long version) &&
                version >= startVersion)
            {
                versions.Add(version);
            }
        }
        versions.Sort();
        foreach (long version in versions)
        {
            yield return version;
        }
    }

    /// <summary>
    /// Gets the latest version number, or <c>-1</c> if the table does not exist.
    /// </summary>
    /// <remarks>
    /// A CHECKPOINT names a version just as a commit does, and metadata cleanup deletes commit files
    /// while keeping the checkpoint that subsumes them. On a table left idle longer than
    /// <c>delta.logRetentionDuration</c> that can remove every commit file, and reading only those would
    /// report a table with no versions at all — for a table that is perfectly readable from its
    /// checkpoint. So the newest of both kinds wins.
    /// </remarks>
    public async ValueTask<long> GetLatestVersionAsync(
        CancellationToken cancellationToken = default) =>
        (await LogListing.ReadAsync(_fs, cancellationToken).ConfigureAwait(false)).LatestVersion;

    /// <summary>
    /// One classified pass over <c>_delta_log</c>, for callers that need more than one view of it.
    /// </summary>
    internal ValueTask<LogListing> ReadListingAsync(CancellationToken cancellationToken = default) =>
        LogListing.ReadAsync(_fs, cancellationToken);
}
