// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Iceberg.Tests;

/// <summary>Resolvers for tests whose tables live under one local directory.</summary>
internal static class TestResolvers
{
    /// <summary>A resolver that reaches everything under <paramref name="fs"/>'s root through it.</summary>
    public static StorageResolver For(LocalTableFileSystem fs) => new StorageResolver().Register(fs);

    /// <summary>A resolver that reaches everything under <paramref name="root"/> through <paramref name="fs"/>,
    /// which must be rooted there (a wrapper around a local file system, say).</summary>
    public static StorageResolver For(ITableFileSystem fs, string root) =>
        new StorageResolver().Register(StorageLocation.Parse(Path.GetFullPath(root)), fs);

    /// <summary>
    /// A file system for tests that mix the two kinds of path: an absolute location (what EW writes into metadata)
    /// goes through a resolver over <paramref name="fs"/>, and a relative path straight to <paramref name="fs"/>.
    /// </summary>
    public static ITableFileSystem Io(LocalTableFileSystem fs) => new MixedFileSystem(fs);

    /// <summary>The local path of a <c>file:</c> location, or <paramref name="path"/> itself when it is relative.</summary>
    public static string LocalPath(string path) =>
        StorageLocation.LooksAbsolute(path) ? StorageLocation.Parse(path).LocalPath! : path;

    private sealed class MixedFileSystem(LocalTableFileSystem fs) : ITableFileSystem
    {
        private readonly LocationFileSystem _absolute = new(For(fs));

        private ITableFileSystem Pick(string path) => StorageLocation.LooksAbsolute(path) ? _absolute : fs;

        public PathNameConstraints PathConstraints => fs.PathConstraints;
        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) => Pick(prefix).ListAsync(prefix, cancellationToken);
        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) => Pick(path).OpenReadAsync(path, cancellationToken);
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default) => Pick(path).CreateAsync(path, overwrite, cancellationToken);
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) => Pick(path).DeleteAsync(path, cancellationToken);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) => Pick(path).ExistsAsync(path, cancellationToken);
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) => Pick(path).ReadAllBytesAsync(path, cancellationToken);
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) => Pick(path).TryWriteAllBytesAsync(path, data, cancellationToken);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) => Pick(path).WriteAllBytesAsync(path, data, cancellationToken);
    }

    /// <summary>A catalog over the warehouse at <paramref name="root"/>, reached through <paramref name="fs"/>.</summary>
    public static FileSystemCatalog Catalog(ITableFileSystem fs, string root) =>
        new(For(fs, root), StorageLocation.Parse(Path.GetFullPath(root)));
}
