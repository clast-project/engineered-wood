// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Net;
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
/// uploaded as a multipart part, and the upload is completed by <see cref="CompleteAsync"/>.
/// Because this driver owns the chunking loop, peak memory is roughly one part (plus the
/// largest single write) rather than the whole object.
/// </para>
/// <para>
/// Objects smaller than one part skip multipart entirely and are written with a single
/// <c>PutObject</c>, sent by <see cref="CompleteAsync"/>. The object becomes visible only then;
/// writing afterwards throws. Disposing without completing aborts: an in-progress multipart
/// upload is aborted, so neither the object nor its parts are left behind. Without overwrite,
/// the completing request carries <c>If-None-Match: *</c>, so it cannot replace an object
/// another writer published in the meantime.
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
    private readonly bool _overwrite;
    private readonly List<PartETag> _parts = new();

    private MemoryStream _buffer = new();
    private string? _uploadId;
    private int _partNumber = 1;
    private long _position;
    private bool _completed;
    private bool _aborted;
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
    /// <param name="overwrite">
    /// Whether completion may replace an existing object. When false, <see cref="CompleteAsync"/>
    /// throws <see cref="IOException"/> if <paramref name="key"/> exists by then.
    /// </param>
    public S3SequentialFile(
        IAmazonS3 client,
        string bucket,
        string key,
        int partSize = DefaultPartSize,
        bool overwrite = true)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _key = key ?? throw new ArgumentNullException(nameof(key));

        if (partSize < MinPartSize)
            throw new ArgumentOutOfRangeException(
                nameof(partSize), partSize,
                $"Part size must be at least {MinPartSize} bytes (5 MiB).");
        _partSize = partSize;
        _overwrite = overwrite;
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
        if (_completed || _aborted)
            throw new InvalidOperationException(
                "Cannot write to an S3 object that has already been completed or aborted.");

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

    /// <summary>
    /// Does nothing: S3 makes an object visible only whole, and a part smaller than 5 MiB can be
    /// uploaded only as the last one, so there is nothing to send early.
    /// </summary>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        return default;
    }

    /// <inheritdoc/>
    public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        if (_completed)
            return;
        if (_aborted)
            throw new InvalidOperationException("Cannot complete an S3 object whose write was aborted.");

        try
        {
            await PublishAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex) when (!_overwrite && ex.StatusCode == HttpStatusCode.PreconditionFailed)
        {
            await AbortAsync().ConfigureAwait(false);
            throw new IOException($"Object already exists: {_key}", ex);
        }
        catch (AmazonS3Exception ex) when (!_overwrite && ex.StatusCode == HttpStatusCode.Conflict)
        {
            // Still conflicting after the retries: another writer is creating the same key, and this one
            // cannot tell whether it will win. Reported like a lost race, so a caller that checks whether the
            // object exists (the classic checkpoint writer does) can find out which way it went.
            await AbortAsync().ConfigureAwait(false);
            throw new IOException($"A concurrent write to {_key} kept conflicting with this one.", ex);
        }
        catch
        {
            await AbortAsync().ConfigureAwait(false);
            throw;
        }
        _completed = true;
    }

    /// <summary>Aborts the upload unless it was completed: nothing is published, and staged parts are discarded.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            if (!_completed)
                await AbortAsync().ConfigureAwait(false);
        }
        finally
        {
            _disposed = true;
            _buffer.Dispose();
        }
    }

    /// <inheritdoc cref="DisposeAsync"/>
    public void Dispose()
    {
        if (_disposed) return;
        // Abort off the current synchronization context to avoid sync-over-async deadlocks;
        // DisposeAsync is the preferred path.
        try
        {
            if (!_completed)
                Task.Run(() => AbortAsync().AsTask()).GetAwaiter().GetResult();
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

    private async ValueTask PublishAsync(CancellationToken cancellationToken)
    {
        string? ifNoneMatch = _overwrite ? null : "*";
        if (_uploadId is null)
        {
            // The whole object fit in under one part: write it with a single PutObject.
            // Also handles the empty-object case (zero writes).
            await RetryConflictsAsync(() =>
            {
                _buffer.Position = 0;
                return _client.PutObjectAsync(
                    new PutObjectRequest
                    {
                        BucketName = _bucket,
                        Key = _key,
                        InputStream = _buffer,
                        AutoCloseStream = false,
                        IfNoneMatch = ifNoneMatch,
                    },
                    cancellationToken);
            }, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            if (_buffer.Length > 0)
                await FlushBufferAsPartAsync(cancellationToken).ConfigureAwait(false);

            // The upload stays in progress until it is completed or aborted, so a retry completes the same
            // parts.
            await RetryConflictsAsync(() => _client.CompleteMultipartUploadAsync(
                new CompleteMultipartUploadRequest
                {
                    BucketName = _bucket,
                    Key = _key,
                    UploadId = _uploadId,
                    PartETags = _parts,
                    IfNoneMatch = ifNoneMatch,
                },
                cancellationToken), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Retries a 409 ConditionalRequestConflict the way <see cref="S3TableFileSystem.TryWriteAllBytesAsync"/>
    /// does: a concurrent write to the same key was in flight and S3 could not evaluate the precondition, so
    /// the request provably did not create the object. Backed off and jittered, so racing writers spread out
    /// instead of colliding again. The last conflict propagates.
    /// </summary>
    private static async ValueTask RetryConflictsAsync(Func<Task> request, CancellationToken cancellationToken)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await request().ConfigureAwait(false);
                return;
            }
            catch (AmazonS3Exception ex) when (
                ex.StatusCode == HttpStatusCode.Conflict && attempt < S3TableFileSystem.MaxConflictRetries)
            {
                await Task.Delay(S3TableFileSystem.ConflictRetryDelay(attempt), cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private async ValueTask AbortAsync()
    {
        if (_aborted)
            return;
        _aborted = true;

        // Leave no dangling multipart upload (which would otherwise accrue storage cost). A small object
        // has staged nothing: its bytes are only in the buffer.
        if (_uploadId is null)
            return;
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
            // Best-effort cleanup; an abandoned upload is invisible, and a bucket lifecycle rule
            // (AbortIncompleteMultipartUpload) reclaims it.
        }
    }
}
