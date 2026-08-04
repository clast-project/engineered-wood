// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="ITableFileSystem"/> implementation for Azure Data Lake Storage Gen2,
/// rooted at a file system and optional directory.
/// </summary>
/// <remarks>
/// <see cref="RenameAsync"/> uses the DFS endpoint's native rename operation with a
/// destination condition, providing an atomic move that does not overwrite an existing path.
/// </remarks>
public sealed class AzureDataLakeTableFileSystem : ITableFileSystem
{
    private readonly DataLakeFileSystemClient _fileSystem;
    private readonly string _rootPrefix;
    private readonly BufferAllocator? _allocator;

    /// <summary>
    /// Creates a table filesystem rooted at <paramref name="fileSystem"/> and,
    /// optionally, <paramref name="rootPath"/>.
    /// </summary>
    public AzureDataLakeTableFileSystem(
        DataLakeFileSystemClient fileSystem,
        string? rootPath = null,
        BufferAllocator? allocator = null)
    {
        if (fileSystem is null)
        {
            throw new ArgumentNullException(nameof(fileSystem));
        }

        _fileSystem = fileSystem;
        _allocator = allocator;

        string normalized = NormalizePath(rootPath ?? string.Empty).Trim('/');
        _rootPrefix = normalized.Length == 0 ? string.Empty : normalized + "/";
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string fullPrefix = Resolve(prefix);
        string listingDirectory = GetListingDirectory(fullPrefix);
        var options = new DataLakeGetPathsOptions
        {
            Path = listingDirectory.Length == 0 ? null : listingDirectory,
            Recursive = true,
        };

        List<PathItem> paths = await GetPathsAsync(options, cancellationToken).ConfigureAwait(false);
        paths.Sort(static (left, right) => string.CompareOrdinal(left.Name, right.Name));

        foreach (PathItem item in paths)
        {
            if (item.IsDirectory == true ||
                !item.Name.StartsWith(fullPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            yield return new TableFileInfo(
                ToRelative(item.Name),
                item.ContentLength ?? 0L,
                item.LastModified);
        }
    }

    /// <inheritdoc/>
    public ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        IRandomAccessFile file = new AzureDataLakeRandomAccessFile(
            _fileSystem.GetFileClient(Resolve(path)), _allocator);
        return new ValueTask<IRandomAccessFile>(file);
    }

    /// <inheritdoc/>
    public async ValueTask<ISequentialFile> CreateAsync(
        string path,
        bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        string fullPath = Resolve(path);
        await EnsureParentDirectoriesAsync(fullPath, cancellationToken).ConfigureAwait(false);

        DataLakeFileClient file = _fileSystem.GetFileClient(fullPath);
        try
        {
            await file.CreateAsync(
                new DataLakePathCreateOptions
                {
                    Conditions = overwrite
                        ? null
                        : new DataLakeRequestConditions { IfNoneMatch = ETag.All },
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException exception) when (
            !overwrite && exception.Status is 409 or 412)
        {
            throw new IOException($"File already exists: {path}", exception);
        }

        return new AzureDataLakeSequentialFile(file);
    }

    /// <inheritdoc/>
    public async ValueTask<bool> RenameAsync(
        string sourcePath,
        string targetPath,
        CancellationToken cancellationToken = default)
    {
        string fullTargetPath = Resolve(targetPath);
        await EnsureParentDirectoriesAsync(fullTargetPath, cancellationToken).ConfigureAwait(false);

        try
        {
            await _fileSystem.GetFileClient(Resolve(sourcePath))
                .RenameAsync(
                    fullTargetPath,
                    destinationFileSystem: null,
                    sourceConditions: null,
                    destinationConditions: new DataLakeRequestConditions
                    {
                        IfNoneMatch = ETag.All,
                    },
                    cancellationToken)
                .ConfigureAwait(false);
            return true;
        }
        catch (RequestFailedException exception) when (exception.Status is 409 or 412)
        {
            return false;
        }
    }

    /// <inheritdoc/>
    public async ValueTask DeleteAsync(
        string path, CancellationToken cancellationToken = default)
    {
        await _fileSystem.GetFileClient(Resolve(path))
            .DeleteIfExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<bool> ExistsAsync(
        string path, CancellationToken cancellationToken = default)
    {
        Response<bool> response = await _fileSystem.GetFileClient(Resolve(path))
            .ExistsAsync(cancellationToken).ConfigureAwait(false);
        return response.Value;
    }

    /// <inheritdoc/>
    public async ValueTask<byte[]> ReadAllBytesAsync(
        string path, CancellationToken cancellationToken = default)
    {
        Response<DataLakeFileReadStreamingResult> response = await _fileSystem
            .GetFileClient(Resolve(path))
            .ReadStreamingAsync(cancellationToken)
            .ConfigureAwait(false);

#if NET8_0_OR_GREATER
        await using Stream content = response.Value.Content;
#else
        using Stream content = response.Value.Content;
#endif
        using var output = new MemoryStream();
        await content.CopyToAsync(output, 81920, cancellationToken).ConfigureAwait(false);
        return output.ToArray();
    }

    /// <inheritdoc/>
    public async ValueTask<bool> TryWriteAllBytesAsync(
        string path,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        string targetPath = Resolve(path);
        await EnsureParentDirectoriesAsync(targetPath, cancellationToken).ConfigureAwait(false);

        int separator = targetPath.LastIndexOf('/');
        string directoryPrefix = separator < 0
            ? string.Empty
            : targetPath.Substring(0, separator + 1);
        string tempPath = directoryPrefix + ".tmp." + Guid.NewGuid().ToString("N");
        DataLakeFileClient tempFile = _fileSystem.GetFileClient(tempPath);
        bool deleteTemp = true;

        try
        {
            await tempFile.CreateAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            if (!data.IsEmpty)
            {
                using Stream content = CreateReadStream(data);
                await tempFile.AppendAsync(
                    content,
                    0,
                    new DataLakeFileAppendOptions { Flush = true },
                    cancellationToken).ConfigureAwait(false);
            }

            try
            {
                await tempFile.RenameAsync(
                    targetPath,
                    destinationFileSystem: null,
                    sourceConditions: null,
                    destinationConditions: new DataLakeRequestConditions
                    {
                        IfNoneMatch = ETag.All,
                    },
                    cancellationToken).ConfigureAwait(false);
                deleteTemp = false;
                return true;
            }
            catch (RequestFailedException exception) when (exception.Status is 409 or 412)
            {
                return false;
            }
        }
        finally
        {
            if (deleteTemp)
            {
                await tempFile.DeleteIfExistsAsync(cancellationToken: CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask WriteAllBytesAsync(
        string path,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        string fullPath = Resolve(path);
        await EnsureParentDirectoriesAsync(fullPath, cancellationToken).ConfigureAwait(false);

        using Stream content = CreateReadStream(data);
        await _fileSystem.GetFileClient(fullPath)
            .UploadAsync(content, overwrite: true, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask EnsureParentDirectoriesAsync(
        string path, CancellationToken cancellationToken)
    {
        int separator = path.LastIndexOf('/');
        if (separator <= 0)
        {
            return;
        }

        await EnsureDirectoryAsync(path.Substring(0, separator), cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask EnsureDirectoryAsync(
        string path, CancellationToken cancellationToken)
    {
        DataLakeDirectoryClient directory = _fileSystem.GetDirectoryClient(path);
        if (await directory.ExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        int separator = path.LastIndexOf('/');
        if (separator > 0)
        {
            await EnsureDirectoryAsync(path.Substring(0, separator), cancellationToken)
                .ConfigureAwait(false);
        }

        await directory.CreateIfNotExistsAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask<List<PathItem>> GetPathsAsync(
        DataLakeGetPathsOptions options, CancellationToken cancellationToken)
    {
        var paths = new List<PathItem>();
        try
        {
            await foreach (PathItem item in _fileSystem
                .GetPathsAsync(options, cancellationToken)
                .ConfigureAwait(false))
            {
                paths.Add(item);
            }
        }
        catch (RequestFailedException exception) when (exception.Status == 404)
        {
            // A missing directory has the same listing semantics as an empty prefix.
        }

        return paths;
    }

    private string Resolve(string path) =>
        _rootPrefix + NormalizePath(path ?? string.Empty).TrimStart('/');

    private string ToRelative(string path) =>
        path.Length >= _rootPrefix.Length &&
        path.StartsWith(_rootPrefix, StringComparison.Ordinal)
            ? path.Substring(_rootPrefix.Length)
            : path;

    private static string GetListingDirectory(string prefix)
    {
        string trimmed = prefix.TrimEnd('/');
        if (prefix.EndsWith("/", StringComparison.Ordinal))
        {
            return trimmed;
        }

        int separator = trimmed.LastIndexOf('/');
        return separator < 0 ? string.Empty : trimmed.Substring(0, separator);
    }

    private static string NormalizePath(string path) => path.Replace('\\', '/');

    private static Stream CreateReadStream(ReadOnlyMemory<byte> data)
    {
        if (MemoryMarshal.TryGetArray(data, out ArraySegment<byte> segment) &&
            segment.Array is not null)
        {
            return new MemoryStream(
                segment.Array, segment.Offset, segment.Count, writable: false, publiclyVisible: false);
        }

        return new MemoryStream(data.ToArray(), writable: false);
    }
}
