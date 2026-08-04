// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Tests.IO;

public sealed class LocalTableFileSystemTests : IDisposable
{
    private readonly string _rootPath =
        Path.Combine(Path.GetTempPath(), "engineered-wood-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task TryWriteAllBytes_CreatesOnce_AndPreservesExistingContent()
    {
        var fileSystem = new LocalTableFileSystem(_rootPath);

        Assert.True(await fileSystem.TryWriteAllBytesAsync("commit.json", "winner"u8.ToArray()));
        Assert.False(await fileSystem.TryWriteAllBytesAsync("commit.json", "loser"u8.ToArray()));

        Assert.Equal(
            "winner",
            Encoding.UTF8.GetString(await fileSystem.ReadAllBytesAsync("commit.json")));
    }

    [Fact]
    public async Task TryWriteAllBytes_ConcurrentWriters_CreateExactlyOnce()
    {
        var fileSystem = new LocalTableFileSystem(_rootPath);

        bool[] results = await Task.WhenAll(
            Enumerable.Range(0, 8)
                .Select(index => fileSystem.TryWriteAllBytesAsync(
                    "commit.json",
                    Encoding.UTF8.GetBytes(index.ToString())).AsTask()));

        Assert.Single(results, static result => result);
    }

    public void Dispose()
    {
        if (Directory.Exists(_rootPath))
        {
            Directory.Delete(_rootPath, recursive: true);
        }
    }
}
