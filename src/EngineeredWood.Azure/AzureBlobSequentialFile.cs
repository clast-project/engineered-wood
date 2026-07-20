// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Azure.Storage.Blobs.Specialized;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="ISequentialFile"/> implementation for Azure Block Blob Storage.
/// Buffers writes in memory and stages blocks when the buffer reaches a threshold.
/// All staged blocks are committed on <see cref="FlushAsync"/> or disposal.
/// </summary>
/// <remarks>
/// Azure block blobs support up to 50,000 blocks of up to 4 GiB each (service version 2019-12-12+).
/// The default block size of 4 MiB allows files up to ~195 GiB, which is well beyond
/// typical Parquet file sizes. For larger files, increase <paramref name="blockSize"/>.
/// </remarks>
public sealed class AzureBlobSequentialFile : ISequentialFile
{
    /// <summary>Default block size: 4 MiB.</summary>
    public const int DefaultBlockSize = 4 * 1024 * 1024;

    private readonly BlockBlobClient _blobClient;
    private readonly int _blockSize;
    private readonly List<string> _committedBlockIds = new();
    private byte[] _buffer;
    private int _bufferPosition;
    private long _position;
    private bool _committed;
    private bool _disposed;

    /// <summary>
    /// Creates a new sequential file backed by an Azure block blob.
    /// </summary>
    /// <param name="blobClient">The block blob client to write to.</param>
    /// <param name="blockSize">
    /// Size threshold at which buffered data is staged as a block.
    /// Defaults to 4 MiB. Must be between 1 and 4 GiB.
    /// </param>
    public AzureBlobSequentialFile(BlockBlobClient blobClient, int blockSize = DefaultBlockSize)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfLessThan(blockSize, 1);
#else
        if (blockSize < 1) throw new ArgumentOutOfRangeException(nameof(blockSize));
#endif
        _blobClient = blobClient;
        _blockSize = blockSize;
        _buffer = new byte[blockSize];
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

    /// <inheritdoc/>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        // Stage any remaining buffered data
        if (_bufferPosition > 0)
            await StageCurrentBlockAsync(cancellationToken).ConfigureAwait(false);

        // Commit the block list
        if (!_committed)
        {
            _committed = true;
            await _blobClient.CommitBlockListAsync(
                _committedBlockIds, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask StageCurrentBlockAsync(CancellationToken cancellationToken)
    {
        if (_bufferPosition == 0)
            return;

        // Block IDs must be base64 strings of uniform length
        string blockId = Convert.ToBase64String(
            BitConverter.GetBytes(_committedBlockIds.Count));

        using var stream = new MemoryStream(_buffer, 0, _bufferPosition, writable: false);
        await _blobClient.StageBlockAsync(
            blockId, stream, cancellationToken: cancellationToken).ConfigureAwait(false);

        _committedBlockIds.Add(blockId);
        _bufferPosition = 0;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Ensure remaining data is flushed and committed
        if (!_committed)
            await FlushAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        _disposed = true;
    }
}
