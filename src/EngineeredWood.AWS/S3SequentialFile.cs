// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using EngineeredWood.IO;

namespace EngineeredWood.AWS;

/// <summary>
/// <see cref="ISequentialFile"/> implementation for AWS S3 using multipart upload.
/// Buffers writes in memory and uploads parts when the buffer reaches a threshold.
/// All parts are uploaded and the multipart upload is completed on <see cref="FlushAsync"/> or disposal.
/// </summary>
/// <remarks>
/// S3 multipart uploads support up to 10,000 parts. The minimum part size is 5 MiB
/// (except the last part). The default part size of 5 MiB allows files up to ~48 GiB.
/// For larger files, increase <paramref name="partSize"/>.
/// </remarks>
public sealed class S3SequentialFile : ISequentialFile
{
    /// <summary>Default part size: 5 MiB (S3 minimum).</summary>
    private const int DefaultPartSize = 5 * 1024 * 1024;

    private readonly IAmazonS3 _client;
    private readonly string _bucket;
    private readonly string _key;
    private readonly int _partSize;
    private readonly byte[] _buffer;
    private int _bufferPosition;
    private long _position;
    private bool _completed;
    private bool _disposed;

    private string? _uploadId;
    private readonly List<PartETag> _partETags = [];
    private int _partNumber;

    /// <summary>
    /// Creates a new sequential file backed by an S3 multipart upload.
    /// </summary>
    /// <param name="client">The S3 client to use for uploads.</param>
    /// <param name="bucket">The S3 bucket name.</param>
    /// <param name="key">The S3 object key.</param>
    /// <param name="partSize">
    /// Size threshold at which buffered data is uploaded as a part.
    /// Defaults to 5 MiB. Must be at least 1.
    /// </param>
    public S3SequentialFile(IAmazonS3 client, string bucket, string key, int partSize = DefaultPartSize)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfLessThan(partSize, 1);
#else
        if (partSize < 1) throw new ArgumentOutOfRangeException(nameof(partSize));
#endif
        _client = client;
        _bucket = bucket;
        _key = key;
        _partSize = partSize;
        _buffer = new byte[partSize];
    }

    /// <summary>
    /// Creates a new sequential file backed by an S3 multipart upload.
    /// </summary>
    /// <param name="client">The S3 client to use for uploads.</param>
    /// <param name="uri">The S3 URI parsed into bucket and key.</param>
    /// <param name="partSize">
    /// Size threshold at which buffered data is uploaded as a part.
    /// Defaults to 5 MiB. Must be at least 1.
    /// </param>
    public S3SequentialFile(IAmazonS3 client, AmazonS3Uri uri, int partSize = DefaultPartSize)
        : this(client, uri.Bucket, uri.Key, partSize) { }

    /// <inheritdoc/>
    public long Position => _position;

    /// <inheritdoc/>
    public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        int remaining = data.Length;
        int sourceOffset = 0;

        while (remaining > 0)
        {
            int spaceInBuffer = _partSize - _bufferPosition;
            int toCopy = Math.Min(remaining, spaceInBuffer);

            data.Span.Slice(sourceOffset, toCopy).CopyTo(_buffer.AsSpan(_bufferPosition));
            _bufferPosition += toCopy;
            _position += toCopy;
            sourceOffset += toCopy;
            remaining -= toCopy;

            if (_bufferPosition >= _partSize)
                await UploadPartAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        if (_uploadId == null)
        {
            // No parts were uploaded — data fits within a single buffer.
            // Use a simple PutObject instead of multipart upload.
            if (_bufferPosition > 0)
            {
                _completed = true;
                using MemoryStream stream = new(_buffer, 0, _bufferPosition, writable: false);
                await _client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = _bucket,
                    Key = _key,
                    InputStream = stream,
                    AutoCloseStream = false,
                    AutoResetStreamPosition = false
                }, cancellationToken).ConfigureAwait(false);
                _bufferPosition = 0;
            }
        }
        else
        {
            // Upload any remaining buffered data as the last part
            if (_bufferPosition > 0)
                await UploadPartAsync(cancellationToken).ConfigureAwait(false);

            // Complete the multipart upload
            if (!_completed)
            {
                _completed = true;
                await _client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
                {
                    BucketName = _bucket,
                    Key = _key,
                    UploadId = _uploadId,
                    PartETags = _partETags
                }, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask UploadPartAsync(CancellationToken cancellationToken)
    {
        if (_bufferPosition == 0)
            return;

        // Lazy-init the multipart upload on first write
        if (_uploadId == null)
        {
            InitiateMultipartUploadResponse? initResponse = await _client.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest
                {
                    BucketName = _bucket,
                    Key = _key
                }, cancellationToken).ConfigureAwait(false);
            _uploadId = initResponse.UploadId;
        }

        _partNumber++;
        using MemoryStream stream = new MemoryStream(_buffer, 0, _bufferPosition, writable: false);
        UploadPartResponse? response = await _client.UploadPartAsync(new UploadPartRequest
        {
            BucketName = _bucket,
            Key = _key,
            UploadId = _uploadId,
            PartNumber = _partNumber,
            PartSize = _bufferPosition,
            InputStream = stream
        }, cancellationToken).ConfigureAwait(false);

        _partETags.Add(new PartETag(_partNumber, response.ETag));
        _bufferPosition = 0;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        
        if (!_completed)
            await FlushAsync().ConfigureAwait(false);
        
        _disposed = true;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _disposed = true;
    }
}
