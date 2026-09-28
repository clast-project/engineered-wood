// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers;
using EngineeredWood.IO;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Counts requests (a ReadRangesAsync is one) and the bytes they ask for, and delays each by
/// <paramref name="latencyMs"/>, plus the time its bytes take at <paramref name="megabytesPerSecond"/>
/// when that is set: a store's time to first byte, then its transfer rate.
/// </summary>
internal sealed class LatencyCountingFile(IRandomAccessFile inner, int latencyMs, double megabytesPerSecond = 0) : IRandomAccessFile
{
    private int _requests;
    private long _bytes;

    public int Requests => _requests;

    public long Bytes => Interlocked.Read(ref _bytes);

    public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
        inner.GetLengthAsync(cancellationToken);

    public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _requests);
        Interlocked.Add(ref _bytes, range.Length);
        await DelayAsync(range.Length, cancellationToken);
        return await inner.ReadAsync(range, cancellationToken);
    }

    public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _requests);
        long bytes = 0;
        foreach (var range in ranges)
            bytes += range.Length;
        Interlocked.Add(ref _bytes, bytes);
        await DelayAsync(bytes, cancellationToken);
        return await inner.ReadRangesAsync(ranges, cancellationToken);
    }

    private Task DelayAsync(long bytes, CancellationToken cancellationToken)
    {
        double ms = latencyMs + (megabytesPerSecond > 0 ? bytes / (megabytesPerSecond * 1000) : 0);
        return ms >= 1 ? Task.Delay(TimeSpan.FromMilliseconds(ms), cancellationToken) : Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => inner.DisposeAsync();

    public void Dispose() => inner.Dispose();
}
#endif
