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
}
