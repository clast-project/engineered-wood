// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Google;
using Google.Cloud.Storage.V1;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace EngineeredWood.IO.Gcs;

/// <summary>
/// <see cref="ITableFileSystem"/> implementation for Google Cloud Storage, rooted at a
/// bucket and optional object-name prefix. Backs table formats (Delta Lake, Iceberg,
/// Lance) whose transaction logs span many objects.
/// </summary>
/// <remarks>
/// <para>
/// All paths are relative to the configured root prefix and use <c>'/'</c> separators
/// (GCS object names are flat strings).
/// </para>
/// <para>
/// GCS has no native atomic rename. <see cref="RenameAsync"/> is implemented as a
/// server-side copy followed by deletion of the source, with an <c>IfGenerationMatch = 0</c>
/// precondition on the copy so the destination is written only if it does not already
/// exist. That precondition is enforced atomically by GCS, giving the conflict-free
/// "create target only if absent" guarantee table-format commit protocols depend on; the
/// copy+delete pair as a whole is not atomic.
/// </para>
/// </remarks>
public sealed class GcsTableFileSystem : ITableFileSystem
{
    private readonly StorageClient _client;
    private readonly string _bucket;
    private readonly string _rootPrefix;
    private readonly BufferAllocator? _allocator;

    /// <summary>
    /// Creates a new filesystem rooted at <paramref name="bucket"/> and, optionally, an
    /// object-name prefix within it. All operations resolve paths relative to that root.
    /// </summary>
    /// <param name="client">The GCS client used for all operations.</param>
    /// <param name="bucket">The bucket that backs this filesystem.</param>
    /// <param name="rootPath">
    /// Optional object-name prefix treated as the root "directory". When null or empty,
    /// the root is the bucket itself.
    /// </param>
    /// <param name="allocator">
    /// Buffer allocator passed to files opened via <see cref="OpenReadAsync"/>.
    /// </param>
    public GcsTableFileSystem(
        StorageClient client,
        string bucket,
        string? rootPath = null,
        BufferAllocator? allocator = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _allocator = allocator;

        string normalized = (rootPath ?? string.Empty).Replace('\\', '/').Trim('/');
        _rootPrefix = normalized.Length == 0 ? string.Empty : normalized + "/";
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string fullPrefix = _rootPrefix + (prefix ?? string.Empty).Replace('\\', '/').TrimStart('/');

        // GCS returns objects in lexicographic order of name, satisfying the contract.
        await foreach (Object obj in _client
            .ListObjectsAsync(_bucket, fullPrefix)
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            // Skip "directory placeholder" objects (zero-byte names ending in '/').
            if (obj.Name.Length == 0 || obj.Name[obj.Name.Length - 1] == '/')
                continue;

            long size = obj.Size is { } s ? checked((long)s) : 0L;
            DateTimeOffset lastModified =
                obj.UpdatedDateTimeOffset ?? obj.TimeCreatedDateTimeOffset ?? default;

            yield return new TableFileInfo(ToRelative(obj.Name), size, lastModified);
        }
    }

    /// <inheritdoc/>
    public ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<IRandomAccessFile>(
            new GcsRandomAccessFile(_client, _bucket, Resolve(path), _allocator));
    }

    /// <inheritdoc/>
    public async ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        string objectName = Resolve(path);

        if (!overwrite && await ObjectExistsAsync(objectName, cancellationToken).ConfigureAwait(false))
            throw new IOException($"Object already exists: {path}");

        return new GcsSequentialFile(_client, _bucket, objectName);
    }

    /// <inheritdoc/>
    public async ValueTask<bool> RenameAsync(
        string sourcePath, string targetPath,
        CancellationToken cancellationToken = default)
    {
        string source = Resolve(sourcePath);
        string target = Resolve(targetPath);

        try
        {
            // IfGenerationMatch = 0 means "destination must not exist"; GCS enforces this
            // atomically and fails with 412 Precondition Failed if the target is present.
            await _client.CopyObjectAsync(
                _bucket, source, _bucket, target,
                new CopyObjectOptions { IfGenerationMatch = 0 },
                cancellationToken).ConfigureAwait(false);
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.PreconditionFailed)
        {
            return false;
        }

        // Copy succeeded; remove the source. A failure here leaves both copies, which is
        // recoverable (the target — the committed state — exists).
        await _client.DeleteObjectAsync(_bucket, source, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return true;
    }

    /// <inheritdoc/>
    public async ValueTask DeleteAsync(
        string path, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.DeleteObjectAsync(_bucket, Resolve(path), cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            // Matches the contract: deleting a missing file does not throw.
        }
    }

    /// <inheritdoc/>
    public async ValueTask<bool> ExistsAsync(
        string path, CancellationToken cancellationToken = default) =>
        await ObjectExistsAsync(Resolve(path), cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public async ValueTask<byte[]> ReadAllBytesAsync(
        string path, CancellationToken cancellationToken = default)
    {
        using var stream = new MemoryStream();
        await _client.DownloadObjectAsync(
            _bucket, Resolve(path), stream, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return stream.ToArray();
    }

    /// <inheritdoc/>
    public async ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        byte[] array;
        int offset, count;
        if (MemoryMarshal.TryGetArray(data, out ArraySegment<byte> segment))
        {
            array = segment.Array!;
            offset = segment.Offset;
            count = segment.Count;
        }
        else
        {
            array = data.ToArray();
            offset = 0;
            count = array.Length;
        }

        // A single GCS upload is atomic: the object becomes visible only once fully written.
        using var stream = new MemoryStream(array, offset, count, writable: false);
        await _client.UploadObjectAsync(
            _bucket, Resolve(path), contentType: null, stream, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask<bool> ObjectExistsAsync(string objectName, CancellationToken cancellationToken)
    {
        try
        {
            await _client.GetObjectAsync(_bucket, objectName, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            return true;
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    private string Resolve(string path) =>
        _rootPrefix + (path ?? string.Empty).Replace('\\', '/').TrimStart('/');

    private string ToRelative(string objectName) =>
        objectName.Length >= _rootPrefix.Length && objectName.StartsWith(_rootPrefix, StringComparison.Ordinal)
            ? objectName.Substring(_rootPrefix.Length)
            : objectName;
}
