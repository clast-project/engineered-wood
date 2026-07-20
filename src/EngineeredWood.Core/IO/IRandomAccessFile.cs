// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;

namespace EngineeredWood.IO;

/// <summary>
/// Provides offset-based random access reads into a file or blob.
/// Unlike <see cref="Stream"/>, there is no shared position cursor,
/// so concurrent reads of different ranges are safe.
/// </summary>
public interface IRandomAccessFile : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets the total length of the file in bytes.
    /// Implementations may cache the result after the first call.
    /// </summary>
    ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the specified byte range from the file.
    /// The caller owns the returned buffer and must dispose it.
    /// Throws <see cref="IOException"/> if the range extends past the end of the file.
    /// </summary>
    ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads multiple byte ranges from the file.
    /// Results are returned in the same order as the input <paramref name="ranges"/>.
    /// The caller owns each returned buffer and must dispose them.
    /// Throws <see cref="IOException"/> if any range extends past the end of the file.
    /// </summary>
    ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default);
}
