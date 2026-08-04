// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO;

/// <summary>
/// Provides directory-level file operations required by table formats
/// that manage multiple files (e.g., Delta Lake transaction logs).
/// Implementations handle local filesystems, Azure Blob Storage, S3, etc.
/// </summary>
public interface ITableFileSystem
{
    /// <summary>
    /// Lists files matching the given prefix, returning paths relative to the root.
    /// Results are ordered lexicographically.
    /// </summary>
    IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens an existing file for random-access reading.
    /// The caller owns the returned handle and must dispose it.
    /// </summary>
    ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new file for sequential writing.
    /// The caller owns the returned handle and must dispose it.
    /// Fails if the file already exists when <paramref name="overwrite"/> is false.
    /// </summary>
    ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a file. Does not throw if the file does not exist.
    /// </summary>
    ValueTask DeleteAsync(
        string path, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks whether a file exists.
    /// </summary>
    ValueTask<bool> ExistsAsync(
        string path, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the entire contents of a small file (e.g., <c>_last_checkpoint</c>, commit JSON).
    /// For large files, use <see cref="OpenReadAsync"/> instead.
    /// </summary>
    ValueTask<byte[]> ReadAllBytesAsync(
        string path, CancellationToken cancellationToken = default);

    /// <summary>
    /// <para>
    /// Atomically writes the entire contents of a small file only if the file does not
    /// already exist. Returns <see langword="true"/> when the file was created, or
    /// <see langword="false"/> when an existing file was left unchanged.
    /// </para>
    /// <para>
    /// The atomicity is LOAD-BEARING, not a quality-of-implementation detail: this is the primitive
    /// table-format commit protocols are built on, and a Delta or Iceberg commit is exactly the claim
    /// that this writer — and no other — created version N. An implementation that writes
    /// unconditionally, or that checks existence and then writes, lets two concurrent writers both
    /// believe they won; the loser's commit silently overwrites the winner's and the table loses
    /// whatever that version recorded. There is no error path for this. Backends that cannot create a
    /// file exclusively in one atomic step should emulate it (write elsewhere, then move under a
    /// destination-does-not-exist precondition) rather than approximate it.
    /// </para>
    /// </summary>
    ValueTask<bool> TryWriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes the entire contents of a small file atomically where possible.
    /// </summary>
    ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Metadata about a file returned by <see cref="ITableFileSystem.ListAsync"/>.
/// </summary>
public readonly record struct TableFileInfo(
    string Path,
    long Size,
    DateTimeOffset LastModified);
