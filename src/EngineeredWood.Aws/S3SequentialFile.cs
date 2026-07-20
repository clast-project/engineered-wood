// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Amazon.S3;
using Amazon.S3.Model;

namespace EngineeredWood.IO.Aws;

/// <summary>
/// <see cref="ISequentialFile"/> implementation for Amazon S3.
/// Streams writes using an S3 multipart upload, so memory stays bounded regardless of
/// object size.
/// </summary>
/// <remarks>
/// <para>
/// Writes accumulate in a buffer; once the buffer reaches the configured part size it is
/// uploaded as a multipart part, and the upload is finalized on the first
/// <see cref="FlushAsync"/> or on disposal. Because this driver owns the chunking loop,
/// peak memory is roughly one part (plus the largest single write) rather than the whole
/// object.
/// </para>
/// <para>
/// Objects smaller than one part skip multipart entirely and are written with a single
/// <c>PutObject</c>. An in-progress multipart upload is aborted if finalization fails.
/// The object becomes visible only when finalized; writing after finalization throws.
/// </para>
/// </remarks>
public sealed class S3SequentialFile : ISequentialFile
{
    /// <summary>Minimum size S3 allows for a non-final multipart part: 5 MiB.</summary>
    public const int MinPartSize = 5 * 1024 * 1024;

    /// <summary>Default multipart part size: 8 MiB.</summary>
    public const int DefaultPartSize = 8 * 1024 * 1024;

    private readonly IAmazonS3 _client;
    private readonly string _bucket;
    private readonly string _key;
    private readonly int _partSize;
    private readonly List<PartETag> _parts = new();

    private MemoryStream _buffer = new();
    private string? _uploadId;
    private int _partNumber = 1;
    private long _position;
    private bool _completed;
    private bool _disposed;

    /// <summary>
    /// Creates a new sequential file backed by an S3 object.
    /// </summary>
    /// <param name="client">The S3 client used for the upload requests.</param>
    /// <param name="bucket">The destination bucket.</param>
    /// <param name="key">The object key to create or overwrite within the bucket.</param>
    /// <param name="partSize">
    /// Multipart part size in bytes. Must be at least <see cref="MinPartSize"/> (5 MiB).
    /// Defaults to <see cref="DefaultPartSize"/>.
    /// </param>
    public S3SequentialFile(
        IAmazonS3 client,
        string bucket,
        string key,
        int partSize = DefaultPartSize)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _key = key ?? throw new ArgumentNullException(nameof(key));

        if (partSize < MinPartSize)
            throw new ArgumentOutOfRangeException(
                nameof(partSize), partSize,
                $"Part size must be at least {MinPartSize} bytes (5 MiB).");
        _partSize = partSize;
    }

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
        if (_completed)
            throw new InvalidOperationException(
                "Cannot write after the S3 object has been finalized by FlushAsync or disposal.");

#if NET8_0_OR_GREATER
        _buffer.Write(data.Span);
#else
        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(data, out ArraySegment<byte> segment))
            _buffer.Write(segment.Array!, segment.Offset, segment.Count);
        else
        {
            byte[] temp = data.ToArray();
            _buffer.Write(temp, 0, temp.Length);
        }
#endif
        _position += data.Length;

        if (_buffer.Length >= _partSize)
            await FlushBufferAsPartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        return FinalizeAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            await FinalizeAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _disposed = true;
            _buffer.Dispose();
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        // Finalize off the current synchronization context to avoid sync-over-async
        // deadlocks; DisposeAsync is the preferred path.
        try
        {
            Task.Run(() => FinalizeAsync(CancellationToken.None).AsTask()).GetAwaiter().GetResult();
        }
        finally
        {
            _disposed = true;
            _buffer.Dispose();
        }
    }

    private async ValueTask FlushBufferAsPartAsync(CancellationToken cancellationToken)
    {
        if (_uploadId is null)
        {
            InitiateMultipartUploadResponse init = await _client.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = _bucket, Key = _key },
                cancellationToken).ConfigureAwait(false);
            _uploadId = init.UploadId;
        }

        _buffer.Position = 0;
        UploadPartResponse part = await _client.UploadPartAsync(
            new UploadPartRequest
            {
                BucketName = _bucket,
                Key = _key,
                UploadId = _uploadId,
                PartNumber = _partNumber,
                PartSize = _buffer.Length,
                InputStream = _buffer,
            },
            cancellationToken).ConfigureAwait(false);

        _parts.Add(new PartETag(_partNumber, part.ETag));
        _partNumber++;

        // Start a fresh buffer; the uploaded one is no longer needed.
        _buffer.Dispose();
        _buffer = new MemoryStream();
    }

    private async ValueTask FinalizeAsync(CancellationToken cancellationToken)
    {
        if (_completed)
            return;
        _completed = true;

        try
        {
            if (_uploadId is null)
            {
                // The whole object fit in under one part: write it with a single PutObject.
                // Also handles the empty-object case (zero writes).
                _buffer.Position = 0;
                await _client.PutObjectAsync(
                    new PutObjectRequest
                    {
                        BucketName = _bucket,
                        Key = _key,
                        InputStream = _buffer,
                        AutoCloseStream = false,
                    },
                    cancellationToken).ConfigureAwait(false);
            }
            else
            {
                if (_buffer.Length > 0)
                    await FlushBufferAsPartAsync(cancellationToken).ConfigureAwait(false);

                await _client.CompleteMultipartUploadAsync(
                    new CompleteMultipartUploadRequest
                    {
                        BucketName = _bucket,
                        Key = _key,
                        UploadId = _uploadId,
                        PartETags = _parts,
                    },
                    cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            // Leave no dangling multipart upload (which would otherwise accrue storage cost).
            if (_uploadId is not null)
            {
                try
                {
                    await _client.AbortMultipartUploadAsync(
                        new AbortMultipartUploadRequest
                        {
                            BucketName = _bucket,
                            Key = _key,
                            UploadId = _uploadId,
                        },
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort cleanup; surface the original failure.
                }
            }

            throw;
        }
    }
}
