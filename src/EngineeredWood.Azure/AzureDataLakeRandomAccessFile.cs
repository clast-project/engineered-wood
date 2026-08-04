// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="IRandomAccessFile"/> implementation for Azure Data Lake Storage.
/// Uses range requests through the DFS endpoint.
/// </summary>
public sealed class AzureDataLakeRandomAccessFile : IRandomAccessFile
{
    private readonly DataLakeFileClient _fileClient;
    private readonly BufferAllocator _allocator;
    private readonly SemaphoreSlim _semaphore;
    private readonly CoalescingOptions _coalescingOptions;
    private long _cachedLength = -1;

    /// <summary>
    /// Creates a random-access file backed by <paramref name="fileClient"/>.
    /// </summary>
    public AzureDataLakeRandomAccessFile(
        DataLakeFileClient fileClient,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        CoalescingOptions? coalescingOptions = null)
    {
        if (fileClient is null)
        {
            throw new ArgumentNullException(nameof(fileClient));
        }
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
#else
        if (maxConcurrency < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency));
        }
#endif

        _fileClient = fileClient;
        _allocator = allocator ?? PooledBufferAllocator.Default;
        _semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        _coalescingOptions = coalescingOptions ?? new CoalescingOptions();
    }

    /// <summary>
    /// Creates a random-access file with a pre-known length.
    /// </summary>
    public AzureDataLakeRandomAccessFile(
        DataLakeFileClient fileClient,
        long knownLength,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        CoalescingOptions? coalescingOptions = null)
        : this(fileClient, allocator, maxConcurrency, coalescingOptions)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegative(knownLength);
#else
        if (knownLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(knownLength));
        }
#endif
        _cachedLength = knownLength;
    }

    /// <inheritdoc/>
    public async ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
    {
        if (_cachedLength >= 0)
        {
            return _cachedLength;
        }

        PathProperties properties = await _fileClient
            .GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        _cachedLength = properties.ContentLength;
        return _cachedLength;
    }

    /// <inheritdoc/>
    public async ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default)
    {
        if (range.Length == 0)
        {
            return _allocator.Allocate(0);
        }

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
        var coalescer = new CoalescingFileReader(this, _coalescingOptions, _allocator);
        return coalescer.ReadRangesAsync(ranges, cancellationToken);
    }

    private async ValueTask<IMemoryOwner<byte>> DownloadRangeAsync(
        FileRange range, CancellationToken cancellationToken)
    {
        IMemoryOwner<byte> buffer = _allocator.Allocate(checked((int)range.Length));
        try
        {
            Response<DataLakeFileReadStreamingResult> response = await _fileClient
                .ReadStreamingAsync(
                    new DataLakeFileReadOptions
                    {
                        Range = new HttpRange(range.Offset, range.Length),
                    },
                    cancellationToken)
                .ConfigureAwait(false);

#if NET8_0_OR_GREATER
            await using Stream stream = response.Value.Content;
#else
            using Stream stream = response.Value.Content;
#endif
            Memory<byte> memory = buffer.Memory;
            int totalRead = 0;
            while (totalRead < memory.Length)
            {
#if NET8_0_OR_GREATER
                int bytesRead = await stream.ReadAsync(
                    memory.Slice(totalRead), cancellationToken).ConfigureAwait(false);
#else
                var temporary = new byte[memory.Length - totalRead];
                int bytesRead = await stream.ReadAsync(
                    temporary, 0, temporary.Length, cancellationToken).ConfigureAwait(false);
                temporary.AsMemory(0, bytesRead).CopyTo(memory.Slice(totalRead));
#endif
                if (bytesRead == 0)
                {
                    throw new IOException(
                        $"Unexpected end of file at offset {range.Offset + totalRead}. " +
                        $"Expected {range.Length} bytes starting at offset {range.Offset}.");
                }

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
