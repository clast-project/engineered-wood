// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Threading.Channels;

namespace EngineeredWood.IO.Gcs;

/// <summary>
/// A read-only, forward-only <see cref="Stream"/> that pulls its bytes from a
/// <see cref="ChannelReader{T}"/> of buffers. Used as the source stream for a GCS
/// resumable upload: the uploader reads from here while the producer writes into the
/// channel, so the payload streams to GCS in chunks instead of being buffered whole.
/// </summary>
/// <remarks>
/// When the channel is completed normally, reads return 0 (end of stream). When the
/// channel is completed with an exception, that exception surfaces from the read call,
/// which faults the upload and lets the producer observe the failure.
/// </remarks>
internal sealed class ChannelReaderStream : Stream
{
    private readonly ChannelReader<byte[]> _reader;
    private byte[]? _current;
    private int _offset;

    public ChannelReaderStream(ChannelReader<byte[]> reader) => _reader = reader;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    private async ValueTask<int> ReadCoreAsync(Memory<byte> destination, CancellationToken cancellationToken)
    {
        if (destination.Length == 0)
            return 0;

        // Advance to the next non-empty buffer, waiting for the producer if needed.
        while (_current is null || _offset >= _current.Length)
        {
            if (!await _reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                return 0; // Channel completed and drained → end of stream.

            if (_reader.TryRead(out byte[]? next))
            {
                _current = next;
                _offset = 0;
            }
        }

        int n = Math.Min(destination.Length, _current.Length - _offset);
        _current.AsMemory(_offset, n).CopyTo(destination);
        _offset += n;
        return n;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadCoreAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

#if NET8_0_OR_GREATER
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        => ReadCoreAsync(buffer, cancellationToken);
#endif

    public override int Read(byte[] buffer, int offset, int count)
        => ReadCoreAsync(buffer.AsMemory(offset, count), CancellationToken.None).AsTask().GetAwaiter().GetResult();

    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
