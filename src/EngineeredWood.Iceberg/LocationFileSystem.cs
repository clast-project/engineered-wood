// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using EngineeredWood.IO;

namespace EngineeredWood.Iceberg;

/// <summary>
/// <para>An <see cref="ITableFileSystem"/> whose paths are the absolute locations Iceberg metadata holds
/// (<c>s3://bucket/wh/t/metadata/snap-1.avro</c>, <c>file:/C:/wh/t/data/x.parquet</c>), each reached through a
/// <see cref="StorageResolver"/>. Iceberg names every file by an absolute location, so this is how the manifest and
/// planning code reads and writes them without knowing which backend holds which file.</para>
///
/// <para>A path that is not an absolute location is refused: the spec has none, and resolving one against some
/// base would guess at what its writer meant. Listing returns absolute locations too.</para>
/// </summary>
internal sealed class LocationFileSystem : ITableFileSystem
{
    private readonly StorageResolver _resolver;

    public LocationFileSystem(StorageResolver resolver)
    {
        _resolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
    }

    public StorageResolver Resolver => _resolver;

    /// <summary>The file system and path an absolute location resolves to.</summary>
    public ResolvedStorageLocation Resolve(string location)
    {
        if (location is null)
            throw new ArgumentNullException(nameof(location));
        if (!StorageLocation.LooksAbsolute(location))
            throw new ArgumentException(
                $"'{location}' is not an absolute location. Iceberg names every file by an absolute location, and " +
                "a relative one has no defined base.", nameof(location));
        return _resolver.Resolve(StorageLocation.Parse(location));
    }

    /// <summary>
    /// Constraints are a property of a backend and a table spans as many as its locations resolve to, so this
    /// reports none. Iceberg derives no directory names from data the way Delta's partition directories do.
    /// </summary>
    public PathNameConstraints PathConstraints => PathNameConstraints.None;

    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // A listing prefix is a directory location here ("<table>/metadata/"), never a partial file name.
        var location = StorageLocation.Parse(prefix);
        var resolved = Resolve(prefix);
        string listPrefix = resolved.Path.Length == 0 ? "" : resolved.Path + "/";
        await foreach (var file in resolved.FileSystem.ListAsync(listPrefix, cancellationToken).ConfigureAwait(false))
        {
            string relative = file.Path.Substring(listPrefix.Length);
            yield return file with { Path = location.Combine(relative).ToString() };
        }
    }

    public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
    {
        var r = Resolve(path);
        return r.FileSystem.OpenReadAsync(r.Path, cancellationToken);
    }

    public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
    {
        var r = Resolve(path);
        return r.FileSystem.CreateAsync(r.Path, overwrite, cancellationToken);
    }

    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
    {
        var r = Resolve(path);
        return r.FileSystem.DeleteAsync(r.Path, cancellationToken);
    }

    public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
    {
        var r = Resolve(path);
        return r.FileSystem.ExistsAsync(r.Path, cancellationToken);
    }

    public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
    {
        var r = Resolve(path);
        return r.FileSystem.ReadAllBytesAsync(r.Path, cancellationToken);
    }

    public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var r = Resolve(path);
        return r.FileSystem.TryWriteAllBytesAsync(r.Path, data, cancellationToken);
    }

    public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var r = Resolve(path);
        return r.FileSystem.WriteAllBytesAsync(r.Path, data, cancellationToken);
    }
}
