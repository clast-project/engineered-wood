// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO;

/// <summary>
/// Provides sequential (append-only) write access to a file or blob.
/// Unlike <see cref="IRandomAccessFile"/>, writes are always appended
/// at the current position and the position advances automatically.
/// </summary>
public interface ISequentialFile : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets the current write position (total bytes written so far).
    /// </summary>
    long Position { get; }

    /// <summary>
    /// Writes data at the current position and advances the position.
    /// </summary>
    ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);

    /// <summary>
    /// Flushes any buffered data to the underlying storage.
    /// </summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);
}
