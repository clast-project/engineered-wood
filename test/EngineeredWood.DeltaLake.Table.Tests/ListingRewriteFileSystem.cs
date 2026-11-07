// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A filesystem that rewrites what its listing reports about each file, and counts whole-file reads by path. It
/// stands in for a backend whose listings carry different metadata than the local disk's: no modification time at
/// all, or an ETag that decides identity on its own (#740).
/// </summary>
internal sealed class ListingRewriteFileSystem(ITableFileSystem inner, Func<TableFileInfo, TableFileInfo> rewrite)
    : ITableFileSystem
{
    private readonly ConcurrentDictionary<string, int> _reads = new(StringComparer.Ordinal);

    public PathNameConstraints PathConstraints => inner.PathConstraints;

    /// <summary>How many times <paramref name="path"/> was read whole (commits and JSON checkpoints are).</summary>
    public int ReadsOf(string path) => _reads.TryGetValue(path, out int n) ? n : 0;

    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var file in inner.ListAsync(prefix, cancellationToken).ConfigureAwait(false))
            yield return rewrite(file);
    }

    public ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default)
    {
        _reads.AddOrUpdate(path, 1, (_, n) => n + 1);
        return inner.OpenReadAsync(path, cancellationToken);
    }

    public ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
        inner.CreateAsync(path, overwrite, cancellationToken);

    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
        inner.DeleteAsync(path, cancellationToken);

    public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
        inner.ExistsAsync(path, cancellationToken);

    public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
    {
        _reads.AddOrUpdate(path, 1, (_, n) => n + 1);
        return inner.ReadAllBytesAsync(path, cancellationToken);
    }

    public ValueTask<bool> TryWriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        inner.TryWriteAllBytesAsync(path, data, cancellationToken);

    public ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        inner.WriteAllBytesAsync(path, data, cancellationToken);
}
