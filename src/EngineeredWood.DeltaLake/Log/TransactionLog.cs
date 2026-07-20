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
    /// Writes a commit file atomically using write-to-temp-then-rename.
    /// Throws <see cref="DeltaConflictException"/> if the version already exists.
    /// </summary>
    public async ValueTask WriteCommitAsync(
        long version, IReadOnlyList<DeltaAction> actions,
        CancellationToken cancellationToken = default)
    {
        string targetPath = DeltaVersion.CommitPath(version);

        // Check if target already exists
        if (await _fs.ExistsAsync(targetPath, cancellationToken).ConfigureAwait(false))
            throw new DeltaConflictException(version);

        byte[] data = ActionSerializer.Serialize(actions);

        // Write to a temporary file first, then rename for atomicity
        string tempPath = $"_delta_log/.tmp.{Guid.NewGuid():N}.json";

        await _fs.WriteAllBytesAsync(tempPath, data, cancellationToken)
            .ConfigureAwait(false);

        bool renamed = await _fs.RenameAsync(tempPath, targetPath, cancellationToken)
            .ConfigureAwait(false);

        if (!renamed)
        {
            // Clean up temp file — another writer got there first
            await _fs.DeleteAsync(tempPath, cancellationToken).ConfigureAwait(false);
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
    public async ValueTask<long> GetLatestVersionAsync(
        CancellationToken cancellationToken = default)
    {
        long latest = -1;

        await foreach (long version in ListVersionsAsync(0, cancellationToken)
            .ConfigureAwait(false))
        {
            if (version > latest)
                latest = version;
        }

        return latest;
    }

    /// <summary>
    /// Lists checkpoint file versions in the log directory.
    /// </summary>
    public async IAsyncEnumerable<long> ListCheckpointVersionsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var seen = new HashSet<long>();

        await foreach (var file in _fs.ListAsync(DeltaVersion.LogPrefix, cancellationToken)
            .ConfigureAwait(false))
        {
            string fileName = Path.GetFileName(file.Path);
            if (DeltaVersion.TryParseCheckpointVersion(fileName, out long version) &&
                seen.Add(version))
            {
                yield return version;
            }
        }
    }
}
