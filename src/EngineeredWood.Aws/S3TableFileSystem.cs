// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Net;
using System.Runtime.CompilerServices;
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
/// <see cref="TryWriteAllBytesAsync"/> — the create-only primitive table-format commit protocols are
/// built on — is a single <c>PutObject</c> carrying <c>If-None-Match: *</c> ("destination must not
/// exist"). S3 enforces that condition atomically, returning <c>412 Precondition Failed</c> when the
/// object is already there.
/// </para>
/// <para>
/// This REQUIRES an endpoint that honors conditional writes: AWS S3 since November 2024, and current
/// MinIO / Ceph / R2 builds. An older or partial S3-compatible implementation may accept the header
/// and IGNORE it, in which case the PUT overwrites unconditionally and two writers racing for the
/// same commit version both believe they won — silently, with the loser's commit destroying the
/// winner's. Verify conditional-write support before pointing a table format at a non-AWS endpoint.
/// (The same header was probed on CopyObject and found silently unguarded on MinIO, which is why the
/// commit path uses PutObject rather than a copy.)
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

    /// <summary>
    /// S3 constrains object keys the least of the three. Its naming guidance lists
    /// <c>&lt; &gt; | { } ^ % ` [ ] ~ # "</c> and a backslash as "characters to avoid", but every one of
    /// them is legal — MEASURED against gofakes3 with an in-memory backend, all round-trip
    /// byte-identically with correct content. Avoid-lists are not constraints, so this reports
    /// <see cref="PathNameConstraints.None"/>.
    /// </summary>
    public PathNameConstraints PathConstraints => PathNameConstraints.None;

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
    public async ValueTask<bool> TryWriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        using Stream stream = ReadOnlyMemoryStream.Create(data);
        string key = Resolve(path);
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await _client.PutObjectAsync(
                    new PutObjectRequest
                    {
                        BucketName = _bucket,
                        Key = key,
                        InputStream = stream,
                        AutoCloseStream = false,
                        IfNoneMatch = "*",
                    },
                    cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (AmazonS3Exception exception) when (
                exception.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                // 412: the object is already there. This writer LOST the race — a definitive answer, not
                // a transient one, so it is reported rather than retried.
                return false;
            }
            catch (AmazonS3Exception exception) when (
                exception.StatusCode == HttpStatusCode.Conflict &&
                attempt < MaxConflictRetries)
            {
                // 409 ConditionalRequestConflict: a concurrent write to the SAME key was in flight, and S3
                // could not evaluate the precondition. Retrying is safe — If-None-Match means at most one
                // PUT can ever create the object, so a 409 provably did not create it.
                //
                // Backed off, not spun. 409 IS contention, and racing writers that all retry immediately
                // just collide again on the next tick; the delay grows and is jittered so they spread out
                // instead of resynchronizing.
                stream.Position = 0;
                await Task.Delay(ConflictRetryDelay(attempt), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private const int MaxConflictRetries = 3;

    private static TimeSpan ConflictRetryDelay(int attempt)
    {
        // 25ms, 50ms, 100ms, each with up to 100% jitter added. Commit files are small and the window a
        // conflicting write occupies is short, so the base stays well under the cost of the PUT itself.
        double baseMilliseconds = 25 * Math.Pow(2, attempt);
#if NET8_0_OR_GREATER
        double jitter = Random.Shared.NextDouble();
#else
        double jitter = ThreadLocalRandom.Value!.NextDouble();
#endif
        return TimeSpan.FromMilliseconds(baseMilliseconds * (1 + jitter));
    }

#if !NET8_0_OR_GREATER
    // Random is not thread-safe before .NET 6's Random.Shared.
    private static readonly ThreadLocal<Random> ThreadLocalRandom =
        new(static () => new Random(Guid.NewGuid().GetHashCode()));
#endif

    /// <inheritdoc/>
    public async ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        // A single PutObject is atomic: the object becomes visible only once fully written.
        using Stream stream = ReadOnlyMemoryStream.Create(data);
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
