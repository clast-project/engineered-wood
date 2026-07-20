// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Threading.Channels;
using Google.Cloud.Storage.V1;

namespace EngineeredWood.IO.Gcs;

/// <summary>
/// <see cref="ISequentialFile"/> implementation for Google Cloud Storage objects.
/// Streams writes to GCS using a resumable (chunked) upload, so memory stays bounded
/// regardless of object size.
/// </summary>
/// <remarks>
/// <para>
/// The upload runs on a background task driven by <see cref="StorageClient.UploadObjectAsync(string, string, string, Stream, UploadObjectOptions, CancellationToken, IProgress{Google.Apis.Upload.IUploadProgress})"/>,
/// which performs a GCS resumable upload and reads its source one chunk at a time.
/// <see cref="WriteAsync"/> hands buffers to that task through a bounded channel; when the
/// channel is full the writer awaits, providing backpressure. Peak memory is therefore
/// roughly the upload chunk size plus the small channel handoff buffer, not the file size.
/// </para>
/// <para>
/// GCS resumable uploads only become visible once finalized, which happens on the first
/// <see cref="FlushAsync"/> or on disposal. Writing after the object has been finalized
/// is not supported and throws.
/// </para>
/// </remarks>
public sealed class GcsSequentialFile : ISequentialFile
{
    /// <summary>Default resumable upload chunk size: 8 MiB (a multiple of 256 KiB).</summary>
    public const int DefaultChunkSize = 8 * 1024 * 1024;

    // Cap each buffer handed to the channel so a single large WriteAsync cannot
    // enqueue an unbounded allocation; large writes are split into pieces.
    private const int MaxHandoffPieceSize = 1024 * 1024;

    // Number of pending handoff buffers allowed before WriteAsync blocks (backpressure).
    private const int HandoffCapacity = 8;

    private readonly StorageClient _client;
    private readonly string _bucket;
    private readonly string _objectName;
    private readonly string? _contentType;
    private readonly int _chunkSize;
    private readonly Channel<byte[]> _channel;

    private Task? _uploadTask;
    private long _position;
    private bool _completed;
    private bool _disposed;

    /// <summary>
    /// Creates a new sequential file backed by a GCS object.
    /// </summary>
    /// <param name="client">The GCS client used for the upload request.</param>
    /// <param name="bucket">The destination bucket.</param>
    /// <param name="objectName">The object (blob) name to create or overwrite within the bucket.</param>
    /// <param name="contentType">
    /// Optional content type to record on the object. When <c>null</c>, GCS infers a default.
    /// </param>
    /// <param name="chunkSize">
    /// Resumable upload chunk size in bytes. Must be a positive multiple of
    /// <see cref="UploadObjectOptions.MinimumChunkSize"/> (256 KiB). Defaults to
    /// <see cref="DefaultChunkSize"/>. Larger values reduce request count at the cost of memory.
    /// </param>
    public GcsSequentialFile(
        StorageClient client,
        string bucket,
        string objectName,
        string? contentType = null,
        int chunkSize = DefaultChunkSize)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _objectName = objectName ?? throw new ArgumentNullException(nameof(objectName));
        _contentType = contentType;

        if (chunkSize <= 0 || chunkSize % UploadObjectOptions.MinimumChunkSize != 0)
            throw new ArgumentOutOfRangeException(
                nameof(chunkSize),
                chunkSize,
                $"Chunk size must be a positive multiple of {UploadObjectOptions.MinimumChunkSize} bytes (256 KiB).");
        _chunkSize = chunkSize;

        _channel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(HandoffCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
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
                "Cannot write after the GCS object has been finalized by FlushAsync or disposal.");

        EnsureUploadStarted();

        // If the background upload already failed (e.g. auth/permission error), surface it
        // now rather than blocking on a channel whose reader has stopped.
        if (_uploadTask!.IsCompleted)
            await _uploadTask.ConfigureAwait(false);

        int offset = 0;
        while (offset < data.Length)
        {
            int n = Math.Min(MaxHandoffPieceSize, data.Length - offset);
            byte[] piece = data.Slice(offset, n).ToArray();

            try
            {
                await _channel.Writer.WriteAsync(piece, cancellationToken).ConfigureAwait(false);
            }
            catch (ChannelClosedException)
            {
                // The reader side completed the channel, which only happens when the
                // upload task faulted. Await it to surface the underlying exception.
                await _uploadTask.ConfigureAwait(false);
                throw; // Upload somehow completed without faulting; preserve original signal.
            }

            offset += n;
            _position += n;
        }
    }

    /// <inheritdoc/>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        return FinalizeAsync();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            await FinalizeAsync().ConfigureAwait(false);
        }
        finally
        {
            _disposed = true;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        // Finalize off the current synchronization context to avoid sync-over-async
        // deadlocks; DisposeAsync is the preferred path.
        Task.Run(() => FinalizeAsync().AsTask()).GetAwaiter().GetResult();
        _disposed = true;
    }

    private void EnsureUploadStarted()
    {
        if (_uploadTask is not null)
            return;

        var source = new ChannelReaderStream(_channel.Reader);
        var options = new UploadObjectOptions { ChunkSize = _chunkSize };

        // The upload spans many WriteAsync calls; its lifetime is bounded by completion
        // of the channel, not by any single caller's cancellation token.
        _uploadTask = _client.UploadObjectAsync(
            _bucket, _objectName, _contentType, source, options, CancellationToken.None);
    }

    private async ValueTask FinalizeAsync()
    {
        if (_completed)
            return;
        _completed = true;

        // Ensure the upload exists even for an empty object (no prior writes).
        EnsureUploadStarted();

        // Signal end-of-stream to the reader, then wait for the upload to finalize.
        // Awaiting also surfaces any upload error.
        _channel.Writer.TryComplete();
        await _uploadTask!.ConfigureAwait(false);
    }
}
