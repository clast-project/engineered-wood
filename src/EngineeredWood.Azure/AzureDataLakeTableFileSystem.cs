// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="ITableFileSystem"/> implementation for Azure Data Lake Storage Gen2,
/// rooted at a file system and optional directory.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="TryWriteAllBytesAsync"/> — the create-only primitive table-format commit protocols are
/// built on — writes a temporary file and then MOVES it onto the target under a
/// destination-must-not-exist condition, using the DFS endpoint's native atomic rename.
/// </para>
/// <para>
/// The detour is deliberate. DFS has no single-shot conditional upload: creating a file publishes a
/// ZERO-LENGTH path immediately, and content only lands on the subsequent append/flush. A reader
/// listing the log between those two calls would find version N present and empty — a commit with no
/// actions, which replays as a version that silently recorded nothing. Renaming a fully written file
/// into place makes the target appear whole or not at all, which is what the commit protocol needs.
/// </para>
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

        // Collected and sorted rather than streamed, DELIBERATELY. ListAsync's contract is lexicographic
        // order — the S3 and GCS backends lean on their services returning keys that way, and the log
        // reader depends on it — but a DFS listing is a RECURSIVE directory walk, and whether that walk
        // interleaves nested paths into full-path lexicographic order is not something the API documents.
        // ('.' sorts before '/', so "a/b.json" precedes "a/b/c.json" lexicographically while a depth-first
        // walk need not emit them that way.) Buffering costs memory proportional to the listing, which
        // matters for a VACUUM over a large table; guessing at the order costs correctness. Until the
        // ordering is confirmed against a real hierarchical-namespace account, this pays the memory.
        List<PathItem> paths = await GetPathsAsync(options, cancellationToken).ConfigureAwait(false);
        paths.Sort(static (left, right) => string.CompareOrdinal(left.Name, right.Name));

        foreach (PathItem item in paths)
        {
            // GetPaths lists a DIRECTORY; ITableFileSystem lists a PREFIX. Anything the wider directory
            // sweep picked up that the prefix does not cover is filtered here.
            if (item.IsDirectory == true ||
                item.Name is null ||
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
        DataLakeFileClient file = _fileSystem.GetFileClient(fullPath);
        var options = new DataLakePathCreateOptions
        {
            Conditions = overwrite
                ? null
                : new DataLakeRequestConditions { IfNoneMatch = ETag.All },
        };

        try
        {
            await WithParentDirectoriesAsync(
                fullPath,
                () => new ValueTask<Response<PathInfo>>(
                    file.CreateAsync(options, cancellationToken)),
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
        int separator = targetPath.LastIndexOf('/');
        string directoryPrefix = separator < 0
            ? string.Empty
            : targetPath.Substring(0, separator + 1);
        string tempPath = directoryPrefix + ".tmp." + Guid.NewGuid().ToString("N");
        DataLakeFileClient tempFile = _fileSystem.GetFileClient(tempPath);
        bool deleteTemp = true;

        try
        {
            // The temp file is a sibling of the target, so creating IT is what discovers a missing parent.
            await WithParentDirectoriesAsync(
                tempPath,
                () => new ValueTask<Response<PathInfo>>(
                    tempFile.CreateAsync(cancellationToken: cancellationToken)),
                cancellationToken).ConfigureAwait(false);

            if (!data.IsEmpty)
            {
                using Stream content = ReadOnlyMemoryStream.Create(data);
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
        DataLakeFileClient file = _fileSystem.GetFileClient(fullPath);

        await WithParentDirectoriesAsync(
            fullPath,
            async () =>
            {
                using Stream content = ReadOnlyMemoryStream.Create(data);
                return await file.UploadAsync(content, overwrite: true, cancellationToken)
                    .ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs <paramref name="operation"/>; if the DFS endpoint rejects it because the parent path does not
    /// exist, creates the parent chain and runs it once more.
    /// </summary>
    /// <remarks>
    /// Probing for the parent BEFORE every write costs a round trip on the overwhelmingly common path
    /// where the directory is already there, which on a commit is a per-write tax on exactly the latency
    /// this backend exists to cut. A table's directories are created once and then persist, so paying
    /// only on the miss is strictly cheaper and no less correct — a concurrent creator racing us just
    /// makes the second attempt succeed.
    /// </remarks>
    private async ValueTask<T> WithParentDirectoriesAsync<T>(
        string fullPath,
        Func<ValueTask<T>> operation,
        CancellationToken cancellationToken)
    {
        try
        {
            return await operation().ConfigureAwait(false);
        }
        catch (RequestFailedException exception) when (
            exception.Status == 404 && fullPath.LastIndexOf('/') > 0)
        {
            await CreateParentDirectoriesAsync(fullPath, cancellationToken).ConfigureAwait(false);
            return await operation().ConfigureAwait(false);
        }
    }

    private async ValueTask CreateParentDirectoriesAsync(
        string path, CancellationToken cancellationToken)
    {
        int lastSeparator = path.LastIndexOf('/');
        if (lastSeparator <= 0)
        {
            return;
        }

        // Shallowest first: creating a DFS directory needs ITS parent to exist.
        for (int separator = path.IndexOf('/');
            separator >= 0 && separator <= lastSeparator;
            separator = path.IndexOf('/', separator + 1))
        {
            await _fileSystem.GetDirectoryClient(path.Substring(0, separator))
                .CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        }
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
            // A missing directory has the same listing semantics as an empty prefix. The enumerable is
            // built INSIDE the try because the failure can surface either from constructing it or from
            // advancing it, depending on how the client batches the first page.
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
}
