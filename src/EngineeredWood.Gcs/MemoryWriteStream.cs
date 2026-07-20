// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO.Gcs;

/// <summary>
/// A minimal write-only <see cref="Stream"/> over a fixed <see cref="Memory{Byte}"/> region.
/// Used as the download destination for ranged GCS reads so the object bytes land
/// directly in a caller-owned buffer with no intermediate copy.
/// </summary>
internal sealed class MemoryWriteStream : Stream
{
    private readonly Memory<byte> _buffer;
    private int _position;

    public MemoryWriteStream(Memory<byte> buffer) => _buffer = buffer;

    /// <summary>Number of bytes written so far.</summary>
    public int BytesWritten => _position;

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _buffer.Length;

    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        buffer.AsSpan(offset, count).CopyTo(_buffer.Span.Slice(_position));
        _position += count;
    }

#if NET8_0_OR_GREATER
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        buffer.CopyTo(_buffer.Span.Slice(_position));
        _position += buffer.Length;
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled(cancellationToken);
        buffer.Span.CopyTo(_buffer.Span.Slice(_position));
        _position += buffer.Length;
        return default;
    }
#endif

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled(cancellationToken);
        Write(buffer, offset, count);
        return Task.CompletedTask;
    }

    public override void Flush() { }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
