// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="ITableFileSystem"/> implementation for Azure Blob Storage, rooted at a
/// container and optional blob-name prefix. Backs table formats (Delta Lake, Iceberg,
/// Lance) whose transaction logs span many blobs.
/// </summary>
/// <remarks>
/// <para>
/// All paths are relative to the configured root prefix and use <c>'/'</c> separators
/// (Azure blob names are flat strings).
/// </para>
/// <para>
/// <see cref="TryWriteAllBytesAsync"/> — the create-only primitive table-format commit protocols are
/// built on — is a single upload carrying an <c>If-None-Match: *</c> condition, which the service
/// enforces atomically. A blob never becomes visible part-written, so the commit file appears whole
/// or not at all.
/// </para>
/// </remarks>
public sealed class AzureTableFileSystem : ITableFileSystem, IReadConcurrencyHint
{
    private readonly BlobContainerClient _container;
    private readonly string _rootPrefix;
    private readonly BufferAllocator? _allocator;
    private readonly int _maxConcurrency;
    private readonly UploadBufferPool? _uploadPool;

    /// <summary>
    /// The spare blocks the writers of a filesystem share by default (<c>uploadBuffers</c>): 8, so a writer can
    /// have up to 9 blocks staging at once, and all of a filesystem's writers together hold at most 32 MiB of
    /// 4 MiB blocks beyond their own.
    /// </summary>
    public const int DefaultUploadBuffers = 8;

    /// <summary>
    /// Creates a new filesystem rooted at <paramref name="container"/> and, optionally, a
    /// blob-name prefix within it. All operations resolve paths relative to that root.
    /// </summary>
    /// <param name="container">The container client that backs this filesystem.</param>
    /// <param name="rootPath">
    /// Optional blob-name prefix treated as the root "directory". When null or empty,
    /// the root is the container itself.
    /// </param>
    /// <param name="allocator">
    /// Buffer allocator passed to files opened via <see cref="OpenReadAsync"/>.
    /// </param>
    /// <param name="maxConcurrency">
    /// The range requests each file opened via <see cref="OpenReadAsync"/> lets be in flight at once, and
    /// the reads this filesystem suggests a table scan run at once (<see cref="PreferredConcurrentReads"/>).
    /// </param>
    /// <param name="uploadBuffers">
    /// The spare blocks every file created via <see cref="CreateAsync"/> shares, to stage a full block in the
    /// background while it fills the next (#713). A writer borrows one only when it is free and otherwise stages
    /// its blocks in turn, so this bounds the memory all of this filesystem's writers hold beyond their own one
    /// block each, however many are open. 0 stages every block in turn. Defaults to
    /// <see cref="DefaultUploadBuffers"/>.
    /// </param>
    public AzureTableFileSystem(
        BlobContainerClient container,
        string? rootPath = null,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        int uploadBuffers = DefaultUploadBuffers)
    {
        if (maxConcurrency < 1)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency), maxConcurrency, "Must be at least 1.");
        if (uploadBuffers < 0)
            throw new ArgumentOutOfRangeException(nameof(uploadBuffers), uploadBuffers, "Must not be negative.");
        _container = container ?? throw new ArgumentNullException(nameof(container));
        _allocator = allocator;
        _maxConcurrency = maxConcurrency;
        _uploadPool = uploadBuffers > 0 ? new UploadBufferPool(uploadBuffers) : null;

        string normalized = (rootPath ?? string.Empty).Replace('\\', '/').Trim('/');
        _rootPrefix = normalized.Length == 0 ? string.Empty : normalized + "/";
    }

    /// <summary>
    /// Azure Blob's documented blob-name rules. It accepts far more than Win32 does — MEASURED against
    /// Azurite, <c>&lt; &gt; |</c> and a trailing space all round-trip byte-identically — but it is NOT
    /// unconstrained: the naming rules bar control characters, and state that "no path segments should end
    /// with a dot", which Azurite happily accepts and so cannot be probed for locally. Taken from the
    /// vendor docs rather than from the emulator, because the emulator is the more permissive of the two.
    /// </summary>
    public PathNameConstraints PathConstraints =>
        PathNameConstraints.NoControlCharacters | PathNameConstraints.NoTrailingDot;

    /// <summary>
    /// The requests a file of this filesystem lets be in flight at once (its <c>maxConcurrency</c>): a table
    /// scan sizes its default window to it, since against an object store the time goes to waiting on
    /// requests rather than to decoding.
    /// </summary>
    public int? PreferredConcurrentReads => _maxConcurrency;

    /// <inheritdoc/>
    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string fullPrefix = _rootPrefix + (prefix ?? string.Empty).Replace('\\', '/').TrimStart('/');

        // Azure returns blobs in lexicographic order of name, satisfying the contract.
        await foreach (BlobItem item in _container
            .GetBlobsAsync(BlobTraits.None, BlobStates.None, fullPrefix, cancellationToken)
            .ConfigureAwait(false))
        {
            // Skip "directory" placeholder blobs (hierarchical namespace).
            if (item.Name.Length == 0 || item.Name[item.Name.Length - 1] == '/')
                continue;

            long size = item.Properties.ContentLength ?? 0L;
            DateTimeOffset lastModified = item.Properties.LastModified ?? default;

            yield return new TableFileInfo(
                ToRelative(item.Name), size, lastModified, item.Properties.ETag?.ToString());
        }
    }

    /// <inheritdoc/>
    public ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<IRandomAccessFile>(
            new AzureBlobRandomAccessFile(_container.GetBlobClient(Resolve(path)), _allocator, _maxConcurrency));
    }

    /// <inheritdoc/>
    public async ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        string blobName = Resolve(path);

        if (!overwrite)
        {
            Response<bool> exists = await _container.GetBlobClient(blobName)
                .ExistsAsync(cancellationToken).ConfigureAwait(false);
            if (exists.Value)
                throw new IOException($"Blob already exists: {path}");
        }

        return new AzureBlobSequentialFile(
            _container.GetBlockBlobClient(blobName), AzureBlobSequentialFile.DefaultBlockSize, overwrite, _uploadPool);
    }

    /// <inheritdoc/>
    public async ValueTask DeleteAsync(
        string path, CancellationToken cancellationToken = default)
    {
        // DeleteIfExists is a no-op (no throw) when the blob is absent.
        await _container.GetBlobClient(Resolve(path))
            .DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<bool> ExistsAsync(
        string path, CancellationToken cancellationToken = default)
    {
        Response<bool> exists = await _container.GetBlobClient(Resolve(path))
            .ExistsAsync(cancellationToken).ConfigureAwait(false);
        return exists.Value;
    }

    /// <inheritdoc/>
    public async ValueTask<byte[]> ReadAllBytesAsync(
        string path, CancellationToken cancellationToken = default)
    {
        Response<BlobDownloadResult> result = await _container.GetBlobClient(Resolve(path))
            .DownloadContentAsync(cancellationToken).ConfigureAwait(false);
        return result.Value.Content.ToArray();
    }

    /// <inheritdoc/>
    public async ValueTask<bool> TryWriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _container.GetBlobClient(Resolve(path))
                .UploadAsync(
                    new BinaryData(data),
                    new BlobUploadOptions
                    {
                        Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All },
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
    public async ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        // A single block-blob upload is atomic: the blob becomes visible only once committed.
        await _container.GetBlobClient(Resolve(path))
            .UploadAsync(new BinaryData(data), overwrite: true, cancellationToken)
            .ConfigureAwait(false);
    }

    private string Resolve(string path) =>
        _rootPrefix + (path ?? string.Empty).Replace('\\', '/').TrimStart('/');

    private string ToRelative(string blobName) =>
        blobName.Length >= _rootPrefix.Length && blobName.StartsWith(_rootPrefix, StringComparison.Ordinal)
            ? blobName.Substring(_rootPrefix.Length)
            : blobName;
}
