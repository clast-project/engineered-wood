// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Iceberg.Tests.Interop;

/// <summary>
/// <para>A local file system that also accepts absolute <c>file:///</c> URIs under its root, which is how
/// every other engine writes Iceberg paths: table location, manifest lists, manifests and data files are
/// all absolute URIs in iceberg-go's metadata. EW's Iceberg code passes those strings straight to its
/// <see cref="ITableFileSystem"/>, which takes root-relative paths, so reading another engine's table
/// needs this mapping. A URI outside the root throws rather than escaping it.</para>
///
/// <para>Test-only on purpose: how EW should resolve absolute locations in general (any scheme, any
/// store) is a library decision for the Iceberg read path, not something to settle here.</para>
/// </summary>
internal sealed class FileUriFileSystem : ITableFileSystem
{
    private readonly LocalTableFileSystem _inner;
    private readonly string _rootUri;

    public FileUriFileSystem(string root)
    {
        string full = Path.GetFullPath(root).Replace('\\', '/').TrimEnd('/');
        _inner = new LocalTableFileSystem(full);
        RootUri = "file://" + (full.StartsWith("/", StringComparison.Ordinal) ? "" : "/") + full;
        _rootUri = RootUri + "/";
    }

    /// <summary>The root as a <c>file:///</c> URI with no trailing slash, e.g. <c>file:///C:/tmp/wh</c>.</summary>
    public string RootUri { get; }

    /// <summary>Maps a <c>file:</c> URI under the root to a root-relative path; anything else passes through.</summary>
    public string Map(string path)
    {
        if (!path.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
            return path;

        string normalized = Uri.UnescapeDataString(path);
        if (!normalized.StartsWith(_rootUri, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException($"'{path}' is not under this file system's root {_rootUri}", nameof(path));
        return normalized.Substring(_rootUri.Length);
    }

    public PathNameConstraints PathConstraints => _inner.PathConstraints;

    public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) =>
        _inner.ListAsync(Map(prefix), cancellationToken);

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
