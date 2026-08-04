// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A filesystem that works normally until a commit lands, then fails every directory listing.
///
/// <para>It reproduces the one window in a commit that no retry can undo: the version JSON is durable — every
/// reader can see it — but the writer's own post-commit work has not finished. A snapshot refresh reads the
/// log, so failing <see cref="ListAsync"/> from that instant makes the commit call THROW on a commit that
/// actually landed. Any cleanup keyed to "the commit failed" is then holding a list of files a committed
/// <c>add</c> references, and deleting them destroys the table.</para>
///
/// <para>Cancellation reaches the same state with no fault injection at all — a token cancelled between the
/// commit write and the refresh — which is why this window is worth a test rather than a comment.</para>
/// </summary>
internal sealed class FailAfterCommitFileSystem(ITableFileSystem inner) : ITableFileSystem
{
    private readonly ITableFileSystem _inner = inner;

    /// <summary>
    /// Set this once the table is set up, so the fault applies to the commit under test rather than to
    /// <c>CreateAsync</c>'s own version 0 (which commits and then reads the log exactly the same way).
    /// </summary>
    public bool Armed { get; set; }

    /// <summary>True once an ARMED commit's version JSON has been created — i.e. once the commit under test
    /// is durable.</summary>
    public bool Committed { get; private set; }

    private static bool IsCommitJson(string path) =>
        path.StartsWith("_delta_log/", StringComparison.Ordinal)
        && path.EndsWith(".json", StringComparison.Ordinal)
        && !path.Contains(".tmp.", StringComparison.Ordinal);

    public IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix, CancellationToken cancellationToken = default)
    {
        if (Committed)
            throw new IOException("the log listing is unavailable (injected: post-commit failure)");
        return _inner.ListAsync(prefix, cancellationToken);
    }

    public ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default) =>
        _inner.OpenReadAsync(path, cancellationToken);

    public ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
        _inner.CreateAsync(path, overwrite, cancellationToken);

    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.DeleteAsync(path, cancellationToken);

    public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.ExistsAsync(path, cancellationToken);

    public ValueTask<byte[]> ReadAllBytesAsync(
        string path, CancellationToken cancellationToken = default) =>
        _inner.ReadAllBytesAsync(path, cancellationToken);

    public async ValueTask<bool> TryWriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        bool written = await _inner.TryWriteAllBytesAsync(path, data, cancellationToken)
            .ConfigureAwait(false);
        if (written && Armed && IsCommitJson(path))
        {
            Committed = true;
        }
        return written;
    }

    public ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        _inner.WriteAllBytesAsync(path, data, cancellationToken);
}
