// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.IO;

namespace EngineeredWood.Parquet;

/// <summary>
/// A view of a file that starts reading some of its ranges at once, in one request, and serves later reads
/// inside them from what that request returned; any other read goes to the file. A selected read's second
/// pass reads through one: its bytes are requested together with the first pass's, rather than after it,
/// so deciding which pages to decode costs no extra round trip (doc/row-level-filtering.md, F5).
/// </summary>
/// <remarks>
/// The underlying file is NOT disposed with this one. The fetched bytes are, once nothing more is read: the
/// arrays decoded from them never point into them, as for any other read buffer.
/// </remarks>
internal sealed class PrefetchedFile : IRandomAccessFile
{
    private readonly IRandomAccessFile _inner;
    private readonly FileRange[] _ranges;
    private readonly Task<IReadOnlyList<IMemoryOwner<byte>>> _fetch;

    /// <summary>Starts reading <paramref name="ranges"/> from <paramref name="inner"/>.</summary>
    public PrefetchedFile(IRandomAccessFile inner, IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken)
    {
        _inner = inner;
        _ranges = ranges.OrderBy(r => r.Offset).ToArray();
        _fetch = _ranges.Length == 0
            ? Task.FromResult<IReadOnlyList<IMemoryOwner<byte>>>([])
            : inner.ReadRangesAsync(_ranges, cancellationToken).AsTask();
    }

    public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
        _inner.GetLengthAsync(cancellationToken);

    public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
    {
        var buffers = await _fetch.ConfigureAwait(false);
        return TryServe(buffers, range) ?? await _inner.ReadAsync(range, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
    {
        var buffers = await _fetch.ConfigureAwait(false);
        var result = new IMemoryOwner<byte>[ranges.Count];
        List<int>? missing = null;
        for (int i = 0; i < ranges.Count; i++)
        {
            if (TryServe(buffers, ranges[i]) is { } served)
                result[i] = served;
            else
                (missing ??= []).Add(i);
        }

        if (missing is not null)
        {
            var read = await _inner.ReadRangesAsync(missing.ConvertAll(i => ranges[i]), cancellationToken)
                .ConfigureAwait(false);
            for (int k = 0; k < missing.Count; k++)
                result[missing[k]] = read[k];
        }

        return result;
    }

    // The bytes of `range`, when one fetched range holds them all, as a buffer whose disposal releases nothing.
    private IMemoryOwner<byte>? TryServe(IReadOnlyList<IMemoryOwner<byte>> buffers, FileRange range)
    {
        int lo = 0, hi = _ranges.Length - 1, found = -1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (_ranges[mid].Offset <= range.Offset)
            {
                found = mid;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        if (found < 0)
            return null;
        var held = _ranges[found];
        if (range.Offset + range.Length > held.Offset + held.Length)
            return null;

        return new Borrowed(buffers[found].Memory.Slice(
            checked((int)(range.Offset - held.Offset)), checked((int)range.Length)));
    }

    public async ValueTask DisposeAsync()
    {
        IReadOnlyList<IMemoryOwner<byte>> buffers;
        try
        {
            buffers = await _fetch.ConfigureAwait(false);
        }
        catch
        {
            return; // nothing was fetched; the failure reached whoever read through this file, if anyone did
        }

        foreach (var buffer in buffers)
            buffer.Dispose();
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    private sealed class Borrowed(Memory<byte> memory) : IMemoryOwner<byte>
    {
        public Memory<byte> Memory { get; } = memory;

        public void Dispose()
        {
        }
    }
}
