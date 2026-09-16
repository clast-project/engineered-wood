// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.IO;

namespace EngineeredWood.Vortex.Tests.TestHelpers;

/// <summary>An in-memory <see cref="IRandomAccessFile"/> over a byte array, for malformed-file tests.</summary>
internal sealed class ByteArrayRandomAccessFile : IRandomAccessFile
{
    private readonly byte[] _bytes;

    public ByteArrayRandomAccessFile(byte[] bytes) { _bytes = bytes; }

    public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
        => new(_bytes.LongLength);

    public ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default)
    {
        if (range.Offset < 0 || range.Offset + range.Length > _bytes.LongLength)
            throw new IOException(
                $"Range {range.Offset}..+{range.Length} is out of bounds for {_bytes.LongLength}-byte file.");
        var copy = new byte[range.Length];
        Array.Copy(_bytes, range.Offset, copy, 0, range.Length);
        return new(new ArrayMemoryOwner(copy));
    }

    public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync() => default;
    public void Dispose() { }

    private sealed class ArrayMemoryOwner : IMemoryOwner<byte>
    {
        public ArrayMemoryOwner(byte[] bytes) { Memory = bytes; }
        public Memory<byte> Memory { get; }
        public void Dispose() { }
    }
}
