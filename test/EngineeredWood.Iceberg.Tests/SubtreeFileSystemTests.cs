// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Iceberg.Tests;

/// <summary>
/// A warehouse subtree keeps its paths inside the subtree (#962). The local file system under it only keeps them
/// inside ITS root, higher up, so without this check <c>../other/x</c> reached a sibling of the warehouse.
/// </summary>
public sealed class SubtreeFileSystemTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"ew-subtree-{Guid.NewGuid():N}");

    public SubtreeFileSystemTests()
    {
        Directory.CreateDirectory(Path.Combine(_root, "wh", "db"));
        Directory.CreateDirectory(Path.Combine(_root, "other"));
        File.WriteAllText(Path.Combine(_root, "wh", "db", "x"), "IN");
        File.WriteAllText(Path.Combine(_root, "other", "x"), "OUT");
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    private ITableFileSystem Subtree() => SubtreeFileSystem.Create(new LocalTableFileSystem(_root), "wh");

    [Theory]
    [InlineData("../other/x")]
    [InlineData(@"..\other\x")]
    [InlineData("db/../../other/x")]
    [InlineData("../wh/db/x")]
    [InlineData("/other/x")]
    [InlineData(@"db\x/../../other/x")] // inside if '\' separates, out if "db\x" is one name (a POSIX directory)
    public async Task PathLeavingTheSubtree_IsRefused(string path)
    {
        var fs = Subtree();
        await Assert.ThrowsAsync<ArgumentException>(async () => await fs.ReadAllBytesAsync(path));
        await Assert.ThrowsAsync<ArgumentException>(async () => await fs.DeleteAsync(path));
        await Assert.ThrowsAsync<ArgumentException>(async () => await fs.WriteAllBytesAsync(path, new byte[] { 1 }));
        Assert.Equal("OUT", File.ReadAllText(Path.Combine(_root, "other", "x")));
    }

    [Fact]
    public async Task PathInsideTheSubtree_IsRead()
    {
        var fs = Subtree();
        Assert.Equal("IN", System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("db/x")));
        Assert.Equal("IN", System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("db/./x")));
    }

    /// <summary>A listing of the subtree's root spelled with dot segments lists the subtree, and not the files beside
    /// it whose names start with its name.</summary>
    [Theory]
    [InlineData("")]
    [InlineData(".")]
    [InlineData("db/..")]
    [InlineData("./")]
    [InlineData("db/../")]
    public async Task ListingTheSubtreeRoot_StaysInside(string prefix)
    {
        File.WriteAllText(Path.Combine(_root, "wh-secret.txt"), "OUT");
        Directory.CreateDirectory(Path.Combine(_root, "wh2"));
        File.WriteAllText(Path.Combine(_root, "wh2", "y"), "OUT");
        var paths = new List<string>();
        await foreach (var file in Subtree().ListAsync(prefix))
            paths.Add(file.Path);
        Assert.Equal(new[] { "db/x" }, paths);
    }
}
