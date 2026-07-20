// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Amazon.S3;
using Amazon.S3.Model;

namespace EngineeredWood.IO.Aws;

/// <summary>
/// <see cref="ITableFileSystem"/> implementation for Amazon S3, rooted at a bucket and
/// optional key prefix. Backs table formats (Delta Lake, Iceberg, Lance) whose
/// transaction logs span many objects.
/// </summary>
/// <remarks>
/// <para>
/// All paths are relative to the configured root prefix and use <c>'/'</c> separators
/// (S3 keys are flat strings).
/// </para>
/// <para>
/// S3 has no native atomic rename. <see cref="RenameAsync"/> is implemented as a
/// server-side <c>CopyObject</c> followed by deletion of the source, with an
/// <c>If-None-Match: *</c> condition on the copy so the destination is written only if it
/// does not already exist. S3 enforces that condition atomically (returning
/// <c>412 Precondition Failed</c> otherwise), giving the conflict-free "create target only
/// if absent" guarantee table-format commit protocols depend on; the copy+delete pair as a
/// whole is not atomic. (Conditional writes require an S3 endpoint that supports them —
/// AWS S3 since 2024, and most current S3-compatible stores.)
/// </para>
/// </remarks>
public sealed class S3TableFileSystem : ITableFileSystem
{
    private readonly IAmazonS3 _client;
    private readonly string _bucket;
    private readonly string _rootPrefix;
    private readonly BufferAllocator? _allocator;

    /// <summary>
    /// Creates a new filesystem rooted at <paramref name="bucket"/> and, optionally, a key
    /// prefix within it. All operations resolve paths relative to that root.
    /// </summary>
    /// <param name="client">The S3 client used for all operations.</param>
    /// <param name="bucket">The bucket that backs this filesystem.</param>
    /// <param name="rootPath">
    /// Optional key prefix treated as the root "directory". When null or empty, the root
    /// is the bucket itself.
    /// </param>
    /// <param name="allocator">
    /// Buffer allocator passed to files opened via <see cref="OpenReadAsync"/>.
    /// </param>
    public S3TableFileSystem(
        IAmazonS3 client,
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

        var request = new ListObjectsV2Request { BucketName = _bucket, Prefix = fullPrefix };

        // S3 returns keys in lexicographic (UTF-8) order; the paginator transparently
        // follows continuation tokens.
        await foreach (S3Object obj in _client.Paginators
            .ListObjectsV2(request).S3Objects
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            // Skip "directory" placeholder keys ending in '/'.
            if (obj.Key.Length == 0 || obj.Key[obj.Key.Length - 1] == '/')
                continue;

            long size = obj.Size ?? 0L;
            DateTimeOffset lastModified = obj.LastModified is { } dt
                ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
                : default;

            yield return new TableFileInfo(ToRelative(obj.Key), size, lastModified);
        }
    }

    /// <inheritdoc/>
    public ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<IRandomAccessFile>(
            new S3RandomAccessFile(_client, _bucket, Resolve(path), _allocator));
    }

    /// <inheritdoc/>
    public async ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        string key = Resolve(path);

        if (!overwrite && await ObjectExistsAsync(key, cancellationToken).ConfigureAwait(false))
            throw new IOException($"Object already exists: {path}");

        return new S3SequentialFile(_client, _bucket, key);
    }

    /// <inheritdoc/>
    public async ValueTask<bool> RenameAsync(
        string sourcePath, string targetPath,
        CancellationToken cancellationToken = default)
    {
        string source = Resolve(sourcePath);
        string target = Resolve(targetPath);

        // Conditional-write primitive: read the source and PUT the target with If-None-Match:"*"
        // ("destination must not exist" — 412 Precondition Failed when it does). S3's documented
        // conditional writes cover PutObject/CompleteMultipartUpload, NOT CopyObject: a CopyObject
        // carrying IfNoneMatch is SILENTLY UNGUARDED on at least MinIO (probed — the copy succeeds over
        // an existing target), so the previous copy-based form gave no commit safety. Rename targets
        // here are small commit files, so read+put costs nothing.
        using var got = await _client.GetObjectAsync(_bucket, source, cancellationToken)
            .ConfigureAwait(false);
        using var buffer = new MemoryStream();
        await got.ResponseStream.CopyToAsync(buffer, 81920, cancellationToken).ConfigureAwait(false);
        buffer.Position = 0;
        try
        {
            await _client.PutObjectAsync(
                new PutObjectRequest
                {
                    BucketName = _bucket,
                    Key = target,
                    InputStream = buffer,
                    IfNoneMatch = "*",
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
        {
            return false;
        }

        // Copy succeeded; remove the source. A failure here leaves both copies, which is
        // recoverable (the target — the committed state — exists).
        await _client.DeleteObjectAsync(
            new DeleteObjectRequest { BucketName = _bucket, Key = source },
            cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <inheritdoc/>
    public async ValueTask DeleteAsync(
        string path, CancellationToken cancellationToken = default)
    {
        try
        {
            // S3 DeleteObject is idempotent, but some S3-compatible stores 404 on a missing
            // key; swallow that to honor the "no throw if absent" contract.
            await _client.DeleteObjectAsync(
                new DeleteObjectRequest { BucketName = _bucket, Key = Resolve(path) },
                cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
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
        using GetObjectResponse response = await _client.GetObjectAsync(
            new GetObjectRequest { BucketName = _bucket, Key = Resolve(path) },
            cancellationToken).ConfigureAwait(false);

        using var memory = new MemoryStream();
        // Explicit buffer size selects the CopyToAsync overload that exists on all TFMs
        // (netstandard2.0 lacks CopyToAsync(Stream, CancellationToken)).
        await response.ResponseStream.CopyToAsync(memory, 81920, cancellationToken).ConfigureAwait(false);
        return memory.ToArray();
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

        // A single PutObject is atomic: the object becomes visible only once fully written.
        using var stream = new MemoryStream(array, offset, count, writable: false);
        await _client.PutObjectAsync(
            new PutObjectRequest
            {
                BucketName = _bucket,
                Key = Resolve(path),
                InputStream = stream,
                AutoCloseStream = false,
            },
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<bool> ObjectExistsAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            await _client.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = _bucket, Key = key },
                cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    private string Resolve(string path) =>
        _rootPrefix + (path ?? string.Empty).Replace('\\', '/').TrimStart('/');

    private string ToRelative(string key) =>
        key.Length >= _rootPrefix.Length && key.StartsWith(_rootPrefix, StringComparison.Ordinal)
            ? key.Substring(_rootPrefix.Length)
            : key;
}
