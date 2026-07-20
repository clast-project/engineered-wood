// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Net.Http.Headers;
using Google.Cloud.Storage.V1;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace EngineeredWood.IO.Gcs;

/// <summary>
/// <see cref="IRandomAccessFile"/> implementation for Google Cloud Storage.
/// Uses <see cref="StorageClient.DownloadObjectAsync(string, string, Stream, DownloadObjectOptions, CancellationToken, IProgress{Google.Apis.Download.IDownloadProgress})"/>
/// with HTTP range requests.
/// Concurrent requests are throttled via a semaphore.
/// Multi-range reads automatically coalesce nearby ranges to reduce HTTP round-trips.
/// </summary>
public sealed class GcsRandomAccessFile : IRandomAccessFile
{
    private readonly StorageClient _client;
    private readonly string _bucket;
    private readonly string _objectName;
    private readonly BufferAllocator _allocator;
    private readonly SemaphoreSlim _semaphore;
    private readonly CoalescingOptions _coalescingOptions;
    private long _cachedLength = -1;

    /// <summary>
    /// Creates an instance addressing <paramref name="objectName"/> in <paramref name="bucket"/>.
    /// </summary>
    /// <param name="client">The GCS client used for download requests.</param>
    /// <param name="bucket">The bucket containing the object.</param>
    /// <param name="objectName">The object (blob) name within the bucket.</param>
    /// <param name="allocator">Buffer allocator; defaults to <see cref="PooledBufferAllocator.Default"/>.</param>
    /// <param name="maxConcurrency">Maximum number of concurrent range requests.</param>
    /// <param name="coalescingOptions">Controls how nearby ranges are merged for multi-range reads.</param>
    public GcsRandomAccessFile(
        StorageClient client,
        string bucket,
        string objectName,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        CoalescingOptions? coalescingOptions = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _objectName = objectName ?? throw new ArgumentNullException(nameof(objectName));
        _allocator = allocator ?? PooledBufferAllocator.Default;
        _semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        _coalescingOptions = coalescingOptions ?? new CoalescingOptions();
    }

    /// <summary>
    /// Creates an instance with a pre-known object size, avoiding the initial
    /// <c>GetObject</c> metadata request. Useful when the size is already known
    /// from a listing or prior metadata fetch.
    /// </summary>
    public GcsRandomAccessFile(
        StorageClient client,
        string bucket,
        string objectName,
        long knownLength,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        CoalescingOptions? coalescingOptions = null)
        : this(client, bucket, objectName, allocator, maxConcurrency, coalescingOptions)
    {
        _cachedLength = knownLength;
    }

    /// <inheritdoc/>
    public async ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
    {
        if (_cachedLength >= 0)
            return _cachedLength;

        Object obj = await _client
            .GetObjectAsync(_bucket, _objectName, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        if (obj.Size is not { } size)
            throw new IOException($"GCS object '{_bucket}/{_objectName}' has no reported size.");

        _cachedLength = checked((long)size);
        return _cachedLength;
    }

    /// <inheritdoc/>
    public async ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default)
    {
        if (range.Length == 0)
            return _allocator.Allocate(0);

        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await DownloadRangeAsync(range, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
    {
        // Delegate to CoalescingFileReader which merges nearby ranges into fewer
        // large HTTP requests, then slices the results back out.
        var coalescer = new CoalescingFileReader(this, _coalescingOptions, _allocator);
        return coalescer.ReadRangesAsync(ranges, cancellationToken);
    }

    private async ValueTask<IMemoryOwner<byte>> DownloadRangeAsync(
        FileRange range, CancellationToken cancellationToken)
    {
        IMemoryOwner<byte> buffer = _allocator.Allocate(checked((int)range.Length));
        try
        {
            var destination = new MemoryWriteStream(buffer.Memory);

            // RangeHeaderValue endpoints are inclusive on both ends.
            var options = new DownloadObjectOptions
            {
                Range = new RangeHeaderValue(range.Offset, range.Offset + range.Length - 1),
            };

            await _client.DownloadObjectAsync(
                _bucket, _objectName, destination, options, cancellationToken)
                .ConfigureAwait(false);

            if (destination.BytesWritten != range.Length)
                throw new IOException(
                    $"Unexpected end of GCS object '{_bucket}/{_objectName}'. " +
                    $"Expected {range.Length} bytes starting at offset {range.Offset}, " +
                    $"got {destination.BytesWritten}.");

            return buffer;
        }
        catch
        {
            buffer.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public void Dispose() => _semaphore.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }
}
