// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.IO;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Wraps a file and records every request made of it: a <see cref="ReadRangesAsync"/> is one request,
/// entered as the number of ranges it asked for, and a <see cref="ReadAsync"/> is one request of one.
/// </summary>
internal sealed class RequestCountingFile(IRandomAccessFile inner) : IRandomAccessFile
{
    public List<int> Requests { get; } = new();

    /// <summary>The bytes all requests asked for.</summary>
    public long Bytes => Interlocked.Read(ref _bytes);

    private long _bytes;

    public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
        inner.GetLengthAsync(cancellationToken);

    public ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
    {
        lock (Requests) Requests.Add(1);
        Interlocked.Add(ref _bytes, range.Length);
        return inner.ReadAsync(range, cancellationToken);
    }

    public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
    {
        lock (Requests) Requests.Add(ranges.Count);
        foreach (var range in ranges)
            Interlocked.Add(ref _bytes, range.Length);
        return inner.ReadRangesAsync(ranges, cancellationToken);
    }

    public ValueTask DisposeAsync() => inner.DisposeAsync();

    public void Dispose() => inner.Dispose();
}
