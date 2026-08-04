// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Azure.Storage.Files.DataLake;

namespace EngineeredWood.IO.Azure;

/// <summary>
/// <see cref="ISequentialFile"/> implementation using Azure Data Lake append and flush operations.
/// </summary>
public sealed class AzureDataLakeSequentialFile : ISequentialFile
{
    /// <summary>Default append size: 4 MiB.</summary>
    public const int DefaultAppendSize = 4 * 1024 * 1024;

    private readonly DataLakeFileClient _fileClient;
    private readonly byte[] _buffer;
    private int _bufferPosition;
    private long _position;
    private long _uploadedPosition;
    private long _flushedPosition;
    private bool _disposed;

    /// <summary>
    /// Creates a sequential file for an existing, empty DFS file.
    /// </summary>
    /// <param name="fileClient">The file client to append to.</param>
    /// <param name="appendSize">The amount buffered before each append request.</param>
    public AzureDataLakeSequentialFile(
        DataLakeFileClient fileClient,
        int appendSize = DefaultAppendSize)
    {
        if (fileClient is null)
        {
            throw new ArgumentNullException(nameof(fileClient));
        }
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfLessThan(appendSize, 1);
#else
        if (appendSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(appendSize));
        }
#endif

        _fileClient = fileClient;
        _buffer = new byte[appendSize];
    }

    /// <inheritdoc/>
    public long Position => _position;

    /// <inheritdoc/>
    public async ValueTask WriteAsync(
        ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        int remaining = data.Length;
        int sourceOffset = 0;
        while (remaining > 0)
        {
            int toCopy = Math.Min(remaining, _buffer.Length - _bufferPosition);
            data.Span.Slice(sourceOffset, toCopy).CopyTo(_buffer.AsSpan(_bufferPosition));
            _bufferPosition += toCopy;
            _position += toCopy;
            sourceOffset += toCopy;
            remaining -= toCopy;

            if (_bufferPosition == _buffer.Length)
            {
                await AppendBufferAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await FlushCoreAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask FlushCoreAsync(CancellationToken cancellationToken)
    {
        await AppendBufferAsync(cancellationToken).ConfigureAwait(false);
        if (_flushedPosition != _position)
        {
            await _fileClient.FlushAsync(
                _position, cancellationToken: cancellationToken).ConfigureAwait(false);
            _flushedPosition = _position;
        }
    }

    private async ValueTask AppendBufferAsync(CancellationToken cancellationToken)
    {
        if (_bufferPosition == 0)
        {
            return;
        }

        using var stream = new MemoryStream(_buffer, 0, _bufferPosition, writable: false);
        await _fileClient.AppendAsync(
            stream, _uploadedPosition, cancellationToken: cancellationToken).ConfigureAwait(false);
        _uploadedPosition += _bufferPosition;
        _bufferPosition = 0;
    }

    private void ThrowIfDisposed()
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed)
        {
            throw new ObjectDisposedException(GetType().FullName);
        }
#endif
    }

    /// <summary>
    /// Marks the file closed WITHOUT finalizing it. Buffered data that no <see cref="FlushAsync"/> has
    /// pushed is discarded, matching <c>AzureBlobSequentialFile</c>: finishing a DFS file needs append and
    /// flush requests, and blocking on them here would be sync-over-async on whatever thread happens to
    /// run the <c>using</c> — the shape that starved the .NET Framework thread pool once already. Use
    /// <see cref="DisposeAsync"/> (an <c>await using</c>) to close a file you have written to.
    /// </summary>
    public void Dispose()
    {
        _disposed = true;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            await FlushCoreAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _disposed = true;
        }
    }
}
