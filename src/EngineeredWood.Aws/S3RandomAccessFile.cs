// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Amazon.S3;
using Amazon.S3.Model;

namespace EngineeredWood.IO.Aws;

/// <summary>
/// <see cref="IRandomAccessFile"/> implementation for Amazon S3.
/// Uses <see cref="IAmazonS3.GetObjectAsync(GetObjectRequest, CancellationToken)"/> with
/// HTTP range requests. Concurrent requests are throttled via a semaphore.
/// Multi-range reads automatically coalesce nearby ranges to reduce HTTP round-trips.
/// </summary>
public sealed class S3RandomAccessFile : IRandomAccessFile
{
    private readonly IAmazonS3 _client;
    private readonly string _bucket;
    private readonly string _key;
    private readonly BufferAllocator _allocator;
    private readonly SemaphoreSlim _semaphore;
    private readonly CoalescingOptions _coalescingOptions;
    private long _cachedLength = -1;

    /// <summary>
    /// Creates an instance addressing <paramref name="key"/> in <paramref name="bucket"/>.
    /// </summary>
    /// <param name="client">The S3 client used for download requests.</param>
    /// <param name="bucket">The bucket containing the object.</param>
    /// <param name="key">The object key within the bucket.</param>
    /// <param name="allocator">Buffer allocator; defaults to <see cref="PooledBufferAllocator.Default"/>.</param>
    /// <param name="maxConcurrency">Maximum number of concurrent range requests.</param>
    /// <param name="coalescingOptions">Controls how nearby ranges are merged for multi-range reads.</param>
    public S3RandomAccessFile(
        IAmazonS3 client,
        string bucket,
        string key,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        CoalescingOptions? coalescingOptions = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _key = key ?? throw new ArgumentNullException(nameof(key));
        _allocator = allocator ?? PooledBufferAllocator.Default;
        _semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        _coalescingOptions = coalescingOptions ?? new CoalescingOptions();
    }

    /// <summary>
    /// Creates an instance with a pre-known object size, avoiding the initial
    /// <c>HeadObject</c> request. Useful when the size is already known from a listing
    /// or prior metadata fetch.
    /// </summary>
    public S3RandomAccessFile(
        IAmazonS3 client,
        string bucket,
        string key,
        long knownLength,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        CoalescingOptions? coalescingOptions = null)
        : this(client, bucket, key, allocator, maxConcurrency, coalescingOptions)
    {
        _cachedLength = knownLength;
    }

    /// <inheritdoc/>
    public async ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
    {
        if (_cachedLength >= 0)
            return _cachedLength;

        GetObjectMetadataResponse metadata = await _client.GetObjectMetadataAsync(
            new GetObjectMetadataRequest { BucketName = _bucket, Key = _key },
            cancellationToken).ConfigureAwait(false);

        _cachedLength = metadata.ContentLength;
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
            // ByteRange endpoints are inclusive on both ends.
            using GetObjectResponse response = await _client.GetObjectAsync(
                new GetObjectRequest
                {
                    BucketName = _bucket,
                    Key = _key,
                    ByteRange = new ByteRange(range.Offset, range.Offset + range.Length - 1),
                },
                cancellationToken).ConfigureAwait(false);

#if NET8_0_OR_GREATER
            await using Stream stream = response.ResponseStream;
#else
            using Stream stream = response.ResponseStream;
#endif
            Memory<byte> memory = buffer.Memory;
            int totalRead = 0;
            while (totalRead < memory.Length)
            {
#if NET8_0_OR_GREATER
                int bytesRead = await stream.ReadAsync(
                    memory.Slice(totalRead), cancellationToken).ConfigureAwait(false);
#else
                // Stream.ReadAsync(Memory<byte>) not available on netstandard2.0
                var tempBuf = new byte[memory.Length - totalRead];
                int bytesRead = await stream.ReadAsync(
                    tempBuf, 0, tempBuf.Length, cancellationToken).ConfigureAwait(false);
                tempBuf.AsMemory(0, bytesRead).CopyTo(memory.Slice(totalRead));
#endif

                if (bytesRead == 0)
                    throw new IOException(
                        $"Unexpected end of S3 object '{_bucket}/{_key}' at offset " +
                        $"{range.Offset + totalRead}. Expected {range.Length} bytes " +
                        $"starting at offset {range.Offset}.");

                totalRead += bytesRead;
            }

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
