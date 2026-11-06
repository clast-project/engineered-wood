// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="ISequentialFile"/> implementation for Azure Block Blob Storage.
/// Buffers writes in memory and stages blocks when the buffer reaches a threshold.
/// The staged blocks are committed by <see cref="CompleteAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// Disposing without completing aborts: the block list is never committed, so nothing is published,
/// and Azure garbage-collects the uncommitted blocks. Without overwrite, the commit carries
/// <c>If-None-Match: *</c>, so it cannot replace a blob another writer published in the meantime.
/// Block IDs start with a random per-file prefix, so two writers staging blocks for the same blob name
/// never overwrite each other's blocks.
/// </para>
/// <para>
/// A writer created by <see cref="AzureTableFileSystem"/> stages its blocks in the background while it fills the
/// next, up to as many at once as the file system's shared spare buffers allow (#713). Block IDs are fixed by
/// position when a block is handed over, so blocks may finish in any order; <see cref="CompleteAsync"/> waits for
/// all of them before committing the list. One constructed directly stages each block in turn.
/// </para>
/// <para>
/// Azure block blobs support up to 50,000 blocks of up to 4 GiB each (service version 2019-12-12+).
/// The default block size of 4 MiB allows files up to ~195 GiB, which is well beyond
/// typical Parquet file sizes. For larger files, increase the <c>blockSize</c> constructor argument.
/// </para>
/// </remarks>
public sealed class AzureBlobSequentialFile : ISequentialFile
{
    /// <summary>Default block size: 4 MiB.</summary>
    public const int DefaultBlockSize = 4 * 1024 * 1024;

    private readonly BlockBlobClient _blobClient;
    private readonly int _blockSize;
    private readonly bool _overwrite;
    private readonly byte[] _blockIdPrefix = Guid.NewGuid().ToByteArray();
    private readonly List<string> _stagedBlockIds = new();
    private readonly BackgroundUploads _uploads;
    private byte[] _buffer;
    private int _bufferPosition;
    private long _position;
    private bool _completed;
    private bool _aborted;
    private bool _disposed;

    /// <summary>
    /// Creates a new sequential file backed by an Azure block blob.
    /// </summary>
    /// <param name="blobClient">The block blob client to write to.</param>
    /// <param name="blockSize">
    /// Size threshold at which buffered data is staged as a block.
    /// Defaults to 4 MiB. Must be between 1 and 4 GiB.
    /// </param>
    /// <param name="overwrite">
    /// Whether completion may replace an existing blob. When false, <see cref="CompleteAsync"/> throws
    /// <see cref="IOException"/> if the blob exists by then.
    /// </param>
    public AzureBlobSequentialFile(
        BlockBlobClient blobClient, int blockSize = DefaultBlockSize, bool overwrite = true)
        : this(blobClient, blockSize, overwrite, uploadPool: null)
    {
    }

    /// <summary>As above, staging blocks in the background with the spare buffers of
    /// <paramref name="uploadPool"/>, when given.</summary>
    internal AzureBlobSequentialFile(
        BlockBlobClient blobClient, int blockSize, bool overwrite, UploadBufferPool? uploadPool)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfLessThan(blockSize, 1);
#else
        if (blockSize < 1) throw new ArgumentOutOfRangeException(nameof(blockSize));
#endif
        _blobClient = blobClient;
        _blockSize = blockSize;
        _overwrite = overwrite;
        _buffer = new byte[blockSize];
        _uploads = new BackgroundUploads(uploadPool);
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
                "Cannot write to an Azure blob that has already been completed or aborted.");
        _uploads.ThrowIfFailed();

        int remaining = data.Length;
        int sourceOffset = 0;

        while (remaining > 0)
        {
            int spaceInBuffer = _blockSize - _bufferPosition;
            int toCopy = Math.Min(remaining, spaceInBuffer);

            data.Span.Slice(sourceOffset, toCopy).CopyTo(_buffer.AsSpan(_bufferPosition));
            _bufferPosition += toCopy;
            _position += toCopy;
            sourceOffset += toCopy;
            remaining -= toCopy;

            if (_bufferPosition >= _blockSize)
                await StageCurrentBlockAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Does nothing: a block blob becomes visible only when its block list is committed, which is
    /// <see cref="CompleteAsync"/>'s job, and staging a partial buffer early would only make more blocks.
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
            throw new InvalidOperationException("Cannot complete an Azure blob whose write was aborted.");

        try
        {
            await StageCurrentBlockAsync(cancellationToken, last: true).ConfigureAwait(false);
            await _uploads.DrainAsync(cancellationToken).ConfigureAwait(false);
            await _blobClient.CommitBlockListAsync(
                _stagedBlockIds,
                new CommitBlockListOptions
                {
                    Conditions = _overwrite ? null : new BlobRequestConditions { IfNoneMatch = ETag.All },
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (!_overwrite && ex.Status is 409 or 412)
        {
            _aborted = true;
            throw new IOException($"Blob already exists: {_blobClient.Name}", ex);
        }
        catch (RequestFailedException ex) when (!_overwrite && ex.ErrorCode == "InvalidBlockList")
        {
            // Another writer publishing the blob while this one had blocks staged discards them. Azure then
            // still evaluates the precondition first and answers 409, handled above; Azurite instead fails
            // the commit because it names blocks that are gone. If the blob is there now, that is the same
            // lost race.
            _aborted = true;
            if (await BlobExistsQuietlyAsync().ConfigureAwait(false))
                throw new IOException($"Blob already exists: {_blobClient.Name}", ex);
            throw;
        }
        catch
        {
            _aborted = true;
            _uploads.Abort();
            throw;
        }
        _completed = true;
    }

    private async Task<bool> BlobExistsQuietlyAsync()
    {
        try
        {
            return (await _blobClient.ExistsAsync(CancellationToken.None).ConfigureAwait(false)).Value;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private async ValueTask StageCurrentBlockAsync(CancellationToken cancellationToken, bool last = false)
    {
        if (_bufferPosition == 0)
            return;

        // Block IDs must be base64 strings of uniform length. They are scoped to the blob name, not to
        // this writer, so the random prefix keeps a concurrent writer to the same name from replacing
        // this one's staged blocks with its own.
        byte[] rawId = new byte[_blockIdPrefix.Length + sizeof(int)];
        _blockIdPrefix.CopyTo(rawId, 0);
        BitConverter.GetBytes(_stagedBlockIds.Count).CopyTo(rawId, _blockIdPrefix.Length);
        string blockId = Convert.ToBase64String(rawId);

        // The ID takes its place in the list now, so the list keeps the blocks' order whenever they finish. An
        // upload refused or failed in turn takes it back out: the list must name only blocks that were staged.
        _stagedBlockIds.Add(blockId);
        try
        {
            _buffer = await _uploads.UploadAsync(
                    _buffer, _bufferPosition, (buffer, length, ct) => StageBlockAsync(blockId, buffer, length, ct),
                    cancellationToken, last)
                .ConfigureAwait(false);
        }
        catch
        {
            _stagedBlockIds.RemoveAt(_stagedBlockIds.Count - 1);
            throw;
        }
        _bufferPosition = 0;
    }

    private async Task StageBlockAsync(string blockId, byte[] buffer, int length, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream(buffer, 0, length, writable: false);
        await _blobClient.StageBlockAsync(
            blockId, stream, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Aborts the write unless it was completed, cancelling the blocks still staging. Nothing needs sending:
    /// uncommitted blocks are never visible, and Azure garbage-collects them.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    /// <inheritdoc cref="DisposeAsync"/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        if (!_completed)
        {
            _aborted = true;
            _uploads.Abort();
        }
    }
}
