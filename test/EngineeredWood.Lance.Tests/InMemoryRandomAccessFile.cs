// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.IO;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// Minimal <see cref="IRandomAccessFile"/> backed by a byte array. Keeps Phase
/// 1 tests free of temp-file ceremony.
/// </summary>
internal sealed class InMemoryRandomAccessFile : IRandomAccessFile
{
    private readonly byte[] _data;

    public InMemoryRandomAccessFile(byte[] data) => _data = data;

    public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
        new(_data.LongLength);

    public ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default)
    {
        if (range.Offset < 0 || range.Length < 0 || range.End > _data.Length)
            throw new IOException(
                $"Range [{range.Offset}, {range.End}) is outside the in-memory file (length {_data.Length}).");
        var owner = new ArrayMemoryOwner(_data, (int)range.Offset, (int)range.Length);
        return new ValueTask<IMemoryOwner<byte>>(owner);
    }

    public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
    {
        var results = new IMemoryOwner<byte>[ranges.Count];
        for (int i = 0; i < ranges.Count; i++)
            results[i] = await ReadAsync(ranges[i], cancellationToken).ConfigureAwait(false);
        return results;
    }

    public void Dispose() { }
    public ValueTask DisposeAsync() => default;

    private sealed class ArrayMemoryOwner : IMemoryOwner<byte>
    {
        private readonly byte[] _source;
        private readonly int _offset;
        private readonly int _length;

        public ArrayMemoryOwner(byte[] source, int offset, int length)
        {
            _source = source;
            _offset = offset;
            _length = length;
        }

        public Memory<byte> Memory => new(_source, _offset, _length);
        public void Dispose() { }
    }
}
