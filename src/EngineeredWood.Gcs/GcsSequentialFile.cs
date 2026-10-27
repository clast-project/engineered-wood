// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Net;
using System.Threading.Channels;
using Google;
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
/// GCS resumable uploads only become visible once finalized, which happens when the source stream
/// ends. Only <see cref="CompleteAsync"/> ends it. Disposing without completing aborts: the channel
/// is completed with an error rather than closed, so the upload's next read throws instead of
/// reporting end-of-stream, and the upload is cancelled as well. It therefore never sends its final
/// chunk, and an unfinalized resumable upload is never visible. Without overwrite, the upload carries
/// <c>ifGenerationMatch=0</c>, so it cannot replace an object another writer published in the
/// meantime. Writing after completion throws.
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
    private readonly bool _overwrite;
    private readonly Channel<byte[]> _channel;
    private readonly CancellationTokenSource _abort = new();

    private Task? _uploadTask;
    private long _position;
    private bool _completed;
    private bool _aborted;
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
    /// <param name="overwrite">
    /// Whether completion may replace an existing object. When false, <see cref="CompleteAsync"/>
    /// throws <see cref="IOException"/> if <paramref name="objectName"/> exists by then.
    /// </param>
    public GcsSequentialFile(
        StorageClient client,
        string bucket,
        string objectName,
        string? contentType = null,
        int chunkSize = DefaultChunkSize,
        bool overwrite = true)
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
        _overwrite = overwrite;

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
        if (_completed || _aborted)
            throw new InvalidOperationException(
                "Cannot write to a GCS object that has already been completed or aborted.");

        EnsureUploadStarted();

        // If the background upload already failed (e.g. auth/permission error), surface it
        // now rather than blocking on a channel whose reader has stopped.
        if (_uploadTask!.IsCompleted)
            await AwaitUploadAsync().ConfigureAwait(false);

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
                await AwaitUploadAsync().ConfigureAwait(false);
                throw; // Upload somehow completed without faulting; preserve original signal.
            }

            offset += n;
            _position += n;
        }
    }

    /// <summary>
    /// Does nothing: the upload already streams each buffer as it is written, and ending the stream
    /// early is what would finalize the object.
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
            throw new InvalidOperationException("Cannot complete a GCS object whose write was aborted.");

        // Ensure the upload exists even for an empty object (no prior writes).
        EnsureUploadStarted();

        // Signal end-of-stream to the reader, then wait for the upload to finalize.
        // Awaiting also surfaces any upload error. Once the stream has ended, the upload may finalize
        // at any moment, so it can no longer be aborted; the caller's token is not passed on.
        _channel.Writer.TryComplete();
        try
        {
            await AwaitUploadAsync().ConfigureAwait(false);
        }
        catch
        {
            _aborted = true;
            throw;
        }
        _completed = true;
    }

    /// <summary>
    /// Aborts the upload unless it was completed: the object is never finalized, so nothing is published.
    /// </summary>
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
            _abort.Dispose();
        }
    }

    /// <inheritdoc cref="DisposeAsync"/>
    public void Dispose()
    {
        if (_disposed) return;
        // Abort off the current synchronization context to avoid sync-over-async
        // deadlocks; DisposeAsync is the preferred path.
        try
        {
            if (!_completed)
                Task.Run(() => AbortAsync().AsTask()).GetAwaiter().GetResult();
        }
        finally
        {
            _disposed = true;
            _abort.Dispose();
        }
    }

    private void EnsureUploadStarted()
    {
        if (_uploadTask is not null)
            return;

        var source = new ChannelReaderStream(_channel.Reader);
        var options = new UploadObjectOptions
        {
            ChunkSize = _chunkSize,
            IfGenerationMatch = _overwrite ? null : 0,
        };

        // The upload spans many WriteAsync calls; its lifetime is bounded by completion
        // of the channel or by an abort, not by any single caller's cancellation token.
        _uploadTask = _client.UploadObjectAsync(
            _bucket, _objectName, _contentType, source, options, _abort.Token);
    }

    private async ValueTask AwaitUploadAsync()
    {
        try
        {
            await _uploadTask!.ConfigureAwait(false);
        }
        catch (GoogleApiException ex) when (!_overwrite && ex.HttpStatusCode == HttpStatusCode.PreconditionFailed)
        {
            throw new IOException($"Object already exists: {_objectName}", ex);
        }
    }

    private async ValueTask AbortAsync()
    {
        if (_aborted)
            return;
        _aborted = true;
        if (_uploadTask is null)
            return;

        // Faulting the channel is what guarantees the object is not finalized: the upload's next read
        // throws rather than returning end-of-stream, which is the only thing that sends the final chunk.
        // Cancelling stops it promptly instead of waiting for that read.
        _channel.Writer.TryComplete(new OperationCanceledException("The GCS write was aborted."));
        _abort.Cancel();
        try
        {
            await _uploadTask.ConfigureAwait(false);
        }
        catch
        {
            // Expected: the upload fails because it was aborted, or had already failed on its own. An
            // unfinalized resumable session is invisible and expires on its own.
        }
    }
}
