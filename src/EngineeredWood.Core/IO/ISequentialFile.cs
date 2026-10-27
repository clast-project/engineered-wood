// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO;

/// <summary>
/// Provides sequential (append-only) write access to a file or blob.
/// Unlike <see cref="IRandomAccessFile"/>, writes are always appended
/// at the current position and the position advances automatically.
/// </summary>
/// <remarks>
/// <para>A file is published only by <see cref="CompleteAsync"/>. Disposing it without completing it
/// first aborts the write: nothing appears at the file's name, and whatever the backend staged is
/// discarded. So a write that throws part-way and is disposed by <c>await using</c> leaves no truncated
/// file behind, and a caller that forgets to complete publishes nothing rather than a partial file.</para>
///
/// <para>Format writers (<c>ParquetFileWriter</c>, <c>OrcWriter</c>, <c>LanceFileWriter</c>) complete
/// the file when they write their footer.</para>
/// </remarks>
public interface ISequentialFile : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets the current write position (total bytes written so far). Still readable after
    /// <see cref="CompleteAsync"/>, where it is the length of the published file.
    /// </summary>
    long Position { get; }

    /// <summary>
    /// Writes data at the current position and advances the position.
    /// </summary>
    /// <exception cref="InvalidOperationException">The file has already been completed.</exception>
    ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);

    /// <summary>
    /// Flushes any buffered data toward the underlying storage, where the backend can do that without
    /// publishing the file. It never publishes: an object store can only make an object visible whole,
    /// so on those backends this does nothing.
    /// </summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes everything written so far at the file's final name. No writes are allowed afterwards.
    /// Calling it again once it has succeeded does nothing.
    /// </summary>
    /// <remarks>
    /// When the file was created without overwrite, this is also where that is enforced, atomically
    /// where the backend supports it: if another writer published a file at the same name since this one
    /// was created, this throws <see cref="IOException"/> and publishes nothing. If it throws for any
    /// reason, the write is aborted; dispose the file to release what remains.
    /// </remarks>
    /// <exception cref="IOException">The file already exists and overwrite was not requested.</exception>
    ValueTask CompleteAsync(CancellationToken cancellationToken = default);
}
