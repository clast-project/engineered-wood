// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;

namespace EngineeredWood.IO;

/// <summary>
/// A file whose length the caller already knows (a Delta <c>add</c>/<c>cdc</c> action records it), so
/// <see cref="GetLengthAsync"/> answers without asking the store. On an object store that ask is a HEAD
/// request: one round trip per file before the first byte is read. Reads and the concurrency hint go to the
/// wrapped file unchanged. Disposing this disposes the wrapped file.
/// </summary>
internal sealed class KnownLengthRandomAccessFile : IRandomAccessFile, IReadConcurrencyHint
{
    private readonly IRandomAccessFile _inner;
    private readonly long _length;

    internal KnownLengthRandomAccessFile(IRandomAccessFile inner, long length)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length), length, "Must not be negative.");
        _length = length;
    }

    /// <summary>The length this file answers with: the one its caller supplied.</summary>
    internal long KnownLength => _length;

    /// <summary>
    /// The wrapped file's own length, asked of the store (on an object store, a HEAD); null when that fails
    /// (the file may be gone). For explaining a failed read, never on the path of a good one.
    /// </summary>
    internal async ValueTask<long?> StoredLengthAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _inner.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
    }

    /// <summary>The wrapped file's hint, if it gives one.</summary>
    public int? PreferredConcurrentReads => (_inner as IReadConcurrencyHint)?.PreferredConcurrentReads;

    public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
        new(_length);

    public ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default) =>
        _inner.ReadAsync(range, cancellationToken);

    public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default) =>
        _inner.ReadRangesAsync(ranges, cancellationToken);

    public ValueTask DisposeAsync() => _inner.DisposeAsync();

    public void Dispose() => _inner.Dispose();

    /// <summary>
    /// Opens <paramref name="path"/> for reading, answering its length from <paramref name="length"/> when that
    /// is known (not null and not negative) instead of asking the store.
    /// </summary>
    internal static async ValueTask<IRandomAccessFile> OpenAsync(
        ITableFileSystem fs, string path, long? length, CancellationToken cancellationToken)
    {
        var file = await fs.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
        return length is { } known and >= 0 ? new KnownLengthRandomAccessFile(file, known) : file;
    }
}
