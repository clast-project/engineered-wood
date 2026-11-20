// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Where a column chunk's encoded pages wait, in the order they were written, until its row group is written to
/// the file (#936 decision 2).
/// </summary>
/// <remarks>
/// <para>
/// The bytes are only ever appended and then copied out once, front to back, so a store need not keep them
/// contiguous, or even in memory. <see cref="SegmentedPageStore"/> is the only store today. The seam is shaped so
/// that a store that spills to disk can be added without touching its callers:
/// </para>
/// <list type="bullet">
/// <item><see cref="Resident"/> is kept apart from <see cref="Length"/>: it is what a memory budget (#719) would
/// reserve, and a spilling store holds less in memory than it has been given.</item>
/// <item><see cref="CopyToAsync"/> is asynchronous, so a store can read spilled bytes back as it copies.</item>
/// <item><see cref="Dispose"/> is called on every path that drops a store, whether its row group was written or
/// abandoned, so a store can free what the garbage collector does not (a temporary file, native memory).</item>
/// </list>
/// </remarks>
internal abstract class PageStore : IDisposable
{
    /// <summary>The bytes written so far.</summary>
    public abstract long Length { get; }

    /// <summary>
    /// The memory the store holds now, which is at least <see cref="Length"/> for a store that keeps everything in
    /// memory: the unused tail of its last allocation counts.
    /// </summary>
    public abstract long Resident { get; }

    /// <summary>Appends <paramref name="bytes"/>.</summary>
    public abstract void Write(ReadOnlySpan<byte> bytes);

    /// <summary>Writes every byte to <paramref name="file"/>, in order. The store is unchanged.</summary>
    public abstract ValueTask CopyToAsync(ISequentialFile file, CancellationToken cancellationToken);

    /// <summary>Copies every byte into a new array, for tests.</summary>
    public abstract byte[] ToArray();

    /// <summary>Releases what the store holds. It is not used again.</summary>
    public abstract void Dispose();
}

/// <summary>
/// A <see cref="PageStore"/> in managed segments: 4 KiB, doubling to 1 MiB, then 1 MiB each.
/// </summary>
/// <remarks>
/// Nothing is ever copied to grow it, unlike a <see cref="MemoryStream"/>, which at 2× growth holds the old and the
/// new buffer at once and leaves up to half its capacity unused: at 16M-row row groups that was ~1.3 GiB of peak
/// over ~330 MiB of pages (#936). Here the unused tail is under one segment per chunk, and a small chunk stays
/// small. Segments are plain arrays rather than pooled: a pool measured no better and would hold idle memory
/// outside any budget.
/// </remarks>
internal sealed class SegmentedPageStore : PageStore
{
    internal const int MinSegment = 4 << 10;
    internal const int MaxSegment = 1 << 20;

    private readonly List<byte[]> _segments = new();

    /// <summary>Bytes used in the last segment; every earlier segment is full.</summary>
    private int _lastUsed;

    private long _length;
    private long _resident;

    public override long Length => _length;

    public override long Resident => _resident;

    public override void Write(ReadOnlySpan<byte> bytes)
    {
        while (!bytes.IsEmpty)
        {
            byte[] last;
            if (_segments.Count == 0 || _lastUsed == (last = _segments[_segments.Count - 1]).Length)
            {
                int size = _segments.Count == 0
                    ? MinSegment
                    : Math.Min(MaxSegment, _segments[_segments.Count - 1].Length * 2);
                last = new byte[size];
                _segments.Add(last);
                _resident += size;
                _lastUsed = 0;
            }

            int n = Math.Min(bytes.Length, last.Length - _lastUsed);
            bytes.Slice(0, n).CopyTo(last.AsSpan(_lastUsed));
            _lastUsed += n;
            _length += n;
            bytes = bytes.Slice(n);
        }
    }

    public override async ValueTask CopyToAsync(ISequentialFile file, CancellationToken cancellationToken)
    {
        for (int i = 0; i < _segments.Count; i++)
        {
            var segment = _segments[i];
            int used = i == _segments.Count - 1 ? _lastUsed : segment.Length;
            await file.WriteAsync(segment.AsMemory(0, used), cancellationToken).ConfigureAwait(false);
        }
    }

    public override byte[] ToArray()
    {
        var bytes = new byte[checked((int)_length)];
        int offset = 0;
        for (int i = 0; i < _segments.Count; i++)
        {
            var segment = _segments[i];
            int used = i == _segments.Count - 1 ? _lastUsed : segment.Length;
            Buffer.BlockCopy(segment, 0, bytes, offset, used);
            offset += used;
        }
        return bytes;
    }

    public override void Dispose()
    {
        _segments.Clear();
        _lastUsed = 0;
        _length = 0;
        _resident = 0;
    }
}
