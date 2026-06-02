using System.Buffers;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using EngineeredWood.IO;

namespace EngineeredWood.AWS;

public sealed class S3RandomAccessFile : IRandomAccessFile
{
    private readonly BufferAllocator _allocator;
    private readonly IAmazonS3 _client;
    private readonly AmazonS3Uri _uri;
    
    private readonly SemaphoreSlim _semaphore;
    private readonly bool _ownsSemaphore;
    private readonly CoalescingOptions _coalescingOptions;
    private long _cachedLength = -1;

    public S3RandomAccessFile(
        IAmazonS3 client,
        AmazonS3Uri uri,
        BufferAllocator? allocator = null,
        int maxConcurrency = 16,
        CoalescingOptions? coalescingOptions = null)
    {
        _client = client;
        _uri = uri;
        _allocator = allocator ?? PooledBufferAllocator.Default;
        _semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        _ownsSemaphore = true;
        _coalescingOptions = coalescingOptions ?? new CoalescingOptions();
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
    
    private async ValueTask<IMemoryOwner<byte>> DownloadRangeAsync(
        FileRange range, CancellationToken cancellationToken)
    {
        IMemoryOwner<byte> buffer = _allocator.Allocate(checked((int)range.Length));
        try
        {
            GetObjectRequest request = new()
            {
                BucketName = _uri.Bucket,
                Key = _uri.Key,
                ByteRange = new ByteRange(range.Offset, range.End - 1)
            };

            using GetObjectResponse response = await _client.GetObjectAsync(request, cancellationToken);

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
                        $"Unexpected end of response stream at offset {range.Offset + totalRead}. " +
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

    public async ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
    {
        if (_cachedLength >= 0)
            return _cachedLength;

        GetObjectMetadataResponse metadata = await _client.GetObjectMetadataAsync(_uri.Bucket, _uri.Key, cancellationToken).ConfigureAwait(false);
        _cachedLength = metadata.ContentLength;
        return _cachedLength;
    }

    public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
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
}