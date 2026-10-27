// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// A deletion vector's <c>.bin</c> is named and written inside <see cref="DeletionVectorWriter.CreateAsync"/>,
/// and its descriptor is the only way a caller learns that name. A write that fails part-way therefore leaves a
/// file no caller can collect, so the writer deletes its own. The faults here land AFTER the bytes reach
/// storage: one before the write never creates the file and proves nothing.
/// </summary>
public class DeletionVectorWriterPartialWriteTests : IDisposable
{
    private readonly string _tempDir;

    public DeletionVectorWriterPartialWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dv_partial_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>Every even position of 10,000 rows: too large to inline, so it becomes a file.</summary>
    private static long[] Sparse() => Enumerable.Range(0, 5000).Select(i => 2L * i).ToArray();

    private string[] VectorFiles() => Directory.GetFiles(_tempDir, "deletion_vector_*.bin");

    [Fact]
    public async Task FailedWrite_DeletesItsPartialFile()
    {
        var fs = new FailAfterWriteFileSystem(new LocalTableFileSystem(_tempDir));
        var positions = Sparse();

        await Assert.ThrowsAsync<IOException>(async () =>
            await new DeletionVectorWriter(fs).CreateAsync(positions, positions.Length));

        Assert.True(fs.BytesReachedStorage);
        Assert.Empty(VectorFiles());
    }

    /// <summary>The cleanup is best-effort: a delete that also fails must not replace the write's exception.</summary>
    [Fact]
    public async Task FailedWrite_WhoseCleanupAlsoFails_StillThrowsTheWriteFailure()
    {
        var fs = new FailAfterWriteFileSystem(new LocalTableFileSystem(_tempDir)) { FailDelete = true };
        var positions = Sparse();

        var ex = await Assert.ThrowsAsync<IOException>(async () =>
            await new DeletionVectorWriter(fs).CreateAsync(positions, positions.Length));

        Assert.Contains("after its bytes landed", ex.Message);
        Assert.Single(VectorFiles()); // the status quo when nothing could be done about it
    }

    [Fact]
    public async Task SuccessfulWrite_KeepsItsFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var positions = Sparse();

        var dv = await new DeletionVectorWriter(fs).CreateAsync(positions, positions.Length);

        Assert.Equal("u", dv.StorageType);
        Assert.Single(VectorFiles());
    }

    /// <summary>Lets a whole-file write land, then throws.</summary>
    private sealed class FailAfterWriteFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public bool BytesReachedStorage { get; private set; }

        public bool FailDelete { get; init; }

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public IAsyncEnumerable<TableFileInfo> ListAsync(
            string prefix, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(
            string path, CancellationToken cancellationToken = default) =>
            inner.OpenReadAsync(path, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(
            string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
            inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
            FailDelete
                ? throw new IOException($"delete refused (injected): {path}")
                : inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(
            string path, CancellationToken cancellationToken = default) =>
            inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask<bool> TryWriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            inner.TryWriteAllBytesAsync(path, data, cancellationToken);

        public async ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            await inner.WriteAllBytesAsync(path, data, cancellationToken);
            BytesReachedStorage = true;
            throw new IOException($"write failed after its bytes landed (injected): {path}");
        }
    }
}
