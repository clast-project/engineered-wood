// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using EngineeredWood.IO;

namespace EngineeredWood.Iceberg;

/// <summary>
/// A view of another file system rooted at one of its directories: a warehouse that a resolver reaches through a
/// file system registered higher up (a bucket, say). Paths are relative to the subtree, and listings come back that
/// way too.
/// </summary>
internal sealed class SubtreeFileSystem : ITableFileSystem
{
    private readonly ITableFileSystem _inner;
    private readonly string _prefix;

    private SubtreeFileSystem(ITableFileSystem inner, string root)
    {
        _inner = inner;
        _prefix = root + "/";
    }

    /// <summary>The subtree of <paramref name="inner"/> at <paramref name="root"/>, or <paramref name="inner"/>
    /// itself when <paramref name="root"/> is its root.</summary>
    public static ITableFileSystem Create(ITableFileSystem inner, string root) =>
        root.Length == 0 ? inner : new SubtreeFileSystem(inner, root);

    /// <summary>
    /// The inner file system's path for <paramref name="path"/>. Refuses a path that would climb out of the subtree
    /// (#962): the inner one only keeps paths inside ITS root, which is higher up. The subtree does not know whether
    /// the inner one splits segments at <c>\</c> (a Windows directory does, a POSIX one does not), so the path must
    /// stay inside under both readings: <c>a\b/../../x</c> stays inside if <c>\</c> separates, but climbs out if
    /// <c>a\b</c> is one name. What else the inner one refuses it refuses itself.
    /// </summary>
    private string Map(string path)
    {
        RootRelativePath.ThrowIfEscapesRoot(path, backslashSeparates: true);
        RootRelativePath.ThrowIfEscapesRoot(path, backslashSeparates: false);
        return _prefix + path;
    }

    public PathNameConstraints PathConstraints => _inner.PathConstraints;

    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var file in _inner.ListAsync(Map(prefix), cancellationToken).ConfigureAwait(false))
        {
            // The inner listing is by name prefix, so a file beside the subtree whose name starts with the
            // subtree's ("wh-x" beside "wh/") is not in the subtree, whatever the inner one returns.
            if (file.Path.StartsWith(_prefix, StringComparison.Ordinal))
                yield return file with { Path = file.Path.Substring(_prefix.Length) };
        }
    }

    public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.OpenReadAsync(Map(path), cancellationToken);

    public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
        _inner.CreateAsync(Map(path), overwrite, cancellationToken);

    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.DeleteAsync(Map(path), cancellationToken);

    public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.ExistsAsync(Map(path), cancellationToken);

    public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.ReadAllBytesAsync(Map(path), cancellationToken);

    public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        _inner.TryWriteAllBytesAsync(Map(path), data, cancellationToken);

    public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        _inner.WriteAllBytesAsync(Map(path), data, cancellationToken);
}
