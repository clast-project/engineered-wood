// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;

namespace EngineeredWood.Tests.IO;

public class LocalSequentialFileTests : IDisposable
{
    private readonly string _tempDir;

    public LocalSequentialFileTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Write_CreatesFile()
    {
        string path = Path.Combine(_tempDir, "test.bin");
        await using (var file = new LocalSequentialFile(path))
        {
            await file.WriteAsync(new byte[] { 1, 2, 3 });
        }

        Assert.True(File.Exists(path));
        Assert.Equal(new byte[] { 1, 2, 3 }, await ReadAllBytesAsync(path));
    }

    [Fact]
    public async Task Position_TracksWrittenBytes()
    {
        string path = Path.Combine(_tempDir, "test.bin");
        await using var file = new LocalSequentialFile(path);

        Assert.Equal(0, file.Position);
        await file.WriteAsync(new byte[] { 1, 2, 3 });
        Assert.Equal(3, file.Position);
        await file.WriteAsync(new byte[] { 4, 5 });
        Assert.Equal(5, file.Position);
    }

    [Fact]
    public async Task MultipleWrites_AppendSequentially()
    {
        string path = Path.Combine(_tempDir, "test.bin");
        await using (var file = new LocalSequentialFile(path))
        {
            await file.WriteAsync(new byte[] { 0x50, 0x41, 0x52, 0x31 }); // "PAR1"
            await file.WriteAsync(new byte[] { 0x00, 0x01, 0x02 });
            await file.WriteAsync(new byte[] { 0x50, 0x41, 0x52, 0x31 }); // "PAR1"
        }

        var content = await ReadAllBytesAsync(path);
        Assert.Equal(11, content.Length);
        Assert.Equal(
            new byte[] { 0x50, 0x41, 0x52, 0x31, 0x00, 0x01, 0x02, 0x50, 0x41, 0x52, 0x31 },
            content);
    }

    [Fact]
    public async Task Flush_DoesNotThrow()
    {
        string path = Path.Combine(_tempDir, "test.bin");
        await using var file = new LocalSequentialFile(path);
        await file.WriteAsync(new byte[] { 1, 2, 3 });
        await file.FlushAsync(); // should not throw
    }

    [Fact]
    public async Task OverwritesExistingFile()
    {
        string path = Path.Combine(_tempDir, "test.bin");
        await WriteAllBytesAsync(path, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });

        await using (var file = new LocalSequentialFile(path))
        {
            await file.WriteAsync(new byte[] { 1, 2, 3 });
        }

        Assert.Equal(new byte[] { 1, 2, 3 }, await ReadAllBytesAsync(path));
    }

    [Fact]
    public async Task EmptyWrite_LeavesFileEmpty()
    {
        string path = Path.Combine(_tempDir, "test.bin");
        await using (var file = new LocalSequentialFile(path))
        {
            // Write nothing
        }

        Assert.True(File.Exists(path));
        Assert.Empty(await ReadAllBytesAsync(path));
    }

    [Fact]
    public async Task LargeWrite_Succeeds()
    {
        string path = Path.Combine(_tempDir, "test.bin");
        var data = new byte[1_000_000];
        new Random(42).NextBytes(data);

        await using (var file = new LocalSequentialFile(path))
        {
            await file.WriteAsync(data);
        }

        Assert.Equal(data, await ReadAllBytesAsync(path));
    }

#if NET8_0_OR_GREATER
    private static Task<byte[]> ReadAllBytesAsync(string path) => File.ReadAllBytesAsync(path);
    private static Task WriteAllBytesAsync(string path, byte[] bytes) => File.WriteAllBytesAsync(path, bytes);
#else
    private static Task<byte[]> ReadAllBytesAsync(string path) => Task.FromResult(File.ReadAllBytes(path));
    private static Task WriteAllBytesAsync(string path, byte[] bytes) { File.WriteAllBytes(path, bytes); return Task.CompletedTask; }
#endif
}
