// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="IRandomAccessFile"/> implementation for Azure Blob Storage.
/// Uses <see cref="BlobClient.DownloadStreamingAsync"/> with HTTP range requests.
/// Concurrent requests are throttled via a semaphore.
/// Multi-range reads automatically coalesce nearby ranges to reduce HTTP round-trips.
/// </summary>
public sealed class AzureBlobRandomAccessFile : IRandomAccessFile
{
    private readonly BlobClient _blobClient;
    private readonly BufferAllocator _allocator;
    private readonly SemaphoreSlim _semaphore;
    private readonly bool _ownsSemaphore;
    private readonly CoalescingOptions _coalescingOptions;
    private long _cachedLength = -1;

    public AzureBlobRandomAccessFile(
        BlobClient blobClient,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        CoalescingOptions? coalescingOptions = null)
    {
        _blobClient = blobClient;
        _allocator = allocator ?? PooledBufferAllocator.Default;
        _semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        _ownsSemaphore = true;
        _coalescingOptions = coalescingOptions ?? new CoalescingOptions();
    }

    /// <summary>
    /// Creates an instance with a pre-known file size, avoiding the initial
    /// <c>GetProperties</c> HEAD request. Useful when the size is already
    /// known from a listing or prior metadata fetch.
    /// </summary>
    public AzureBlobRandomAccessFile(
        BlobClient blobClient,
        long knownLength,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        CoalescingOptions? coalescingOptions = null)
        : this(blobClient, allocator, maxConcurrency, coalescingOptions)
    {
        _cachedLength = knownLength;
    }

    public async ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
    {
        if (_cachedLength >= 0)
            return _cachedLength;

        BlobProperties properties = await _blobClient
            .GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        _cachedLength = properties.ContentLength;
        return _cachedLength;
    }

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
            BlobDownloadStreamingResult result = await _blobClient.DownloadStreamingAsync(
                new BlobDownloadOptions
                {
                    Range = new global::Azure.HttpRange(range.Offset, range.Length),
                },
                cancellationToken).ConfigureAwait(false);

#if NET8_0_OR_GREATER
            await using Stream stream = result.Content;
#else
            using Stream stream = result.Content;
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
                        $"Unexpected end of blob stream at offset {range.Offset + totalRead}. " +
                        $"Expected {range.Length} bytes starting at offset {range.Offset}.");

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

    public void Dispose()
    {
        if (_ownsSemaphore)
            _semaphore.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }
}
