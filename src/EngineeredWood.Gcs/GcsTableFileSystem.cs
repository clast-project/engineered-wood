// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Net;
using System.Runtime.CompilerServices;
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
/// <see cref="TryWriteAllBytesAsync"/> — the create-only primitive table-format commit protocols are
/// built on — is a single upload carrying an <c>IfGenerationMatch = 0</c>
/// precondition, so the destination is written only if it does not already
/// exist. That precondition is enforced atomically by GCS (returning <c>412 Precondition Failed</c>
/// otherwise), and an object never becomes visible part-written, so the commit file appears whole or
/// not at all.
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

    /// <summary>
    /// GCS's documented object-name rules: CR and LF are forbidden outright, and an object may not be
    /// named exactly <c>.</c> or <c>..</c>. Everything else GCS merely "strongly discourages"
    /// (<c># [ ] * ? : " &lt; &gt; |</c>) and accepts — MEASURED against fake-gcs-server with an
    /// in-memory backend, which round-trips all of them byte-identically. Discouraged is not a
    /// constraint, so it is not reported here.
    /// </summary>
    public PathNameConstraints PathConstraints =>
        PathNameConstraints.NoControlCharacters | PathNameConstraints.NoDotOnlySegments;

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

            yield return new TableFileInfo(ToRelative(obj.Name), size, LastModifiedOf(obj));
        }
    }

    /// <summary>
    /// <para>The object's last-modified time, or <see langword="default"/> when the server sent one this
    /// SDK cannot parse.</para>
    ///
    /// <para><c>UpdatedDateTimeOffset</c> and <c>TimeCreatedDateTimeOffset</c> are not fields — they PARSE
    /// the raw JSON string on every access and throw <see cref="FormatException"/> when it is not in the
    /// shape the generated client expects. Unguarded, one object with an odd timestamp aborts the whole
    /// enumeration mid-stream, and the caller loses the listing rather than one field of one entry. That is
    /// the wrong trade for this interface: a table format lists to find out what files exist and how big
    /// they are, and <see cref="TableFileInfo.LastModified"/> is metadata alongside that, not the reason
    /// for the call. MEASURED against fake-gcs-server, which reports a local UTC offset
    /// (<c>...855788-07:00</c>) where the real service always reports <c>Z</c>.</para>
    /// </summary>
    private static DateTimeOffset LastModifiedOf(Object obj)
    {
        try
        {
            return obj.UpdatedDateTimeOffset ?? obj.TimeCreatedDateTimeOffset ?? default;
        }
        catch (FormatException)
        {
            return default;
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
    public async ValueTask<bool> TryWriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        using Stream stream = ReadOnlyMemoryStream.Create(data);
        try
        {
            await _client.UploadObjectAsync(
                _bucket,
                Resolve(path),
                contentType: null,
                stream,
                new UploadObjectOptions { IfGenerationMatch = 0 },
                cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (GoogleApiException exception) when (
            exception.HttpStatusCode == HttpStatusCode.PreconditionFailed)
        {
            return false;
        }
    }

    /// <inheritdoc/>
    public async ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        // A single GCS upload is atomic: the object becomes visible only once fully written.
        using Stream stream = ReadOnlyMemoryStream.Create(data);
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
