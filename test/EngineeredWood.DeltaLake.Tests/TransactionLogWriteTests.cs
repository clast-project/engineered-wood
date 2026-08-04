// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Tests;

public sealed class TransactionLogWriteTests
{
    [Fact]
    public async Task WriteCommit_UsesAtomicCreatePrimitive()
    {
        var fileSystem = new CommitOnlyFileSystem();
        var log = new TransactionLog(fileSystem);

        await log.WriteCommitAsync(
            7,
            [new TransactionId { AppId = "test", Version = 1 }]);

        Assert.Equal("_delta_log/00000000000000000007.json", fileSystem.WrittenPath);
        Assert.NotEmpty(fileSystem.WrittenData);
    }

    [Fact]
    public async Task WriteCommit_WhenVersionExists_ThrowsConflict()
    {
        var fileSystem = new CommitOnlyFileSystem { TryWriteResult = false };
        var log = new TransactionLog(fileSystem);

        DeltaConflictException exception = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await log.WriteCommitAsync(
                7,
                [new TransactionId { AppId = "test", Version = 1 }]));

        Assert.Equal(7, exception.AttemptedVersion);
    }

    private sealed class CommitOnlyFileSystem : ITableFileSystem
    {
        public bool TryWriteResult { get; init; } = true;

        public string? WrittenPath { get; private set; }

        public byte[] WrittenData { get; private set; } = [];

        public ValueTask<bool> TryWriteAllBytesAsync(
            string path,
            ReadOnlyMemory<byte> data,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            WrittenPath = path;
            WrittenData = data.ToArray();
            return new ValueTask<bool>(TryWriteResult);
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(
            string prefix, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<IRandomAccessFile> OpenReadAsync(
            string path, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<ISequentialFile> CreateAsync(
            string path, bool overwrite = false,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<bool> RenameAsync(
            string sourcePath, string targetPath,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask DeleteAsync(
            string path, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<bool> ExistsAsync(
            string path, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<byte[]> ReadAllBytesAsync(
            string path, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
