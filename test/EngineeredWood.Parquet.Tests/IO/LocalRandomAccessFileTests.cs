// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Tests.IO;

public class LocalRandomAccessFileTests : IDisposable
{
    private readonly string _tempFile;

    public LocalRandomAccessFileTests()
    {
        _tempFile = Path.GetTempFileName();
        // Write known content: bytes 0..255 repeated 4 times = 1024 bytes
        var data = new byte[1024];
        for (int i = 0; i < data.Length; i++)
            data[i] = (byte)(i % 256);
        File.WriteAllBytes(_tempFile, data);
    }

    public void Dispose()
    {
        File.Delete(_tempFile);
    }

    [Fact]
    public async Task GetLengthAsync_ReturnsFileLength()
    {
        using var file = new LocalRandomAccessFile(_tempFile);
        long length = await file.GetLengthAsync();
        Assert.Equal(1024, length);
    }

    [Fact]
    public async Task GetLengthAsync_CachesResult()
    {
        using var file = new LocalRandomAccessFile(_tempFile);
        long length1 = await file.GetLengthAsync();
        long length2 = await file.GetLengthAsync();
        Assert.Equal(length1, length2);
    }

    [Fact]
    public async Task ReadAsync_ReadsCorrectBytes()
    {
        using var file = new LocalRandomAccessFile(_tempFile);
        using IMemoryOwner<byte> buffer = await file.ReadAsync(new FileRange(10, 5));
        Assert.Equal(5, buffer.Memory.Length);
        Assert.Equal(10, buffer.Memory.Span[0]);
        Assert.Equal(11, buffer.Memory.Span[1]);
        Assert.Equal(14, buffer.Memory.Span[4]);
    }

    [Fact]
    public async Task ReadAsync_FromStart_ReadsCorrectBytes()
    {
        using var file = new LocalRandomAccessFile(_tempFile);
        using IMemoryOwner<byte> buffer = await file.ReadAsync(new FileRange(0, 3));
        Assert.Equal(0, buffer.Memory.Span[0]);
        Assert.Equal(1, buffer.Memory.Span[1]);
        Assert.Equal(2, buffer.Memory.Span[2]);
    }

    [Fact]
    public async Task ReadAsync_AtEnd_ReadsCorrectBytes()
    {
        using var file = new LocalRandomAccessFile(_tempFile);
        using IMemoryOwner<byte> buffer = await file.ReadAsync(new FileRange(1020, 4));
        Assert.Equal(4, buffer.Memory.Length);
        Assert.Equal((byte)(1020 % 256), buffer.Memory.Span[0]);
    }

    [Fact]
    public async Task ReadAsync_ZeroLength_ReturnsEmptyBuffer()
    {
        using var file = new LocalRandomAccessFile(_tempFile);
        using IMemoryOwner<byte> buffer = await file.ReadAsync(new FileRange(0, 0));
        Assert.Equal(0, buffer.Memory.Length);
    }

    [Fact]
    public async Task ReadAsync_PastEof_ThrowsIOException()
    {
        using var file = new LocalRandomAccessFile(_tempFile);
        await Assert.ThrowsAsync<IOException>(
            () => file.ReadAsync(new FileRange(1020, 10)).AsTask());
    }

    [Fact]
    public async Task ReadRangesAsync_MultipleRanges_ReturnsInOrder()
    {
        using var file = new LocalRandomAccessFile(_tempFile);

        var ranges = new FileRange[]
        {
            new(100, 10),
            new(0, 5),
            new(500, 20),
        };

        IReadOnlyList<IMemoryOwner<byte>> results = await file.ReadRangesAsync(ranges);
        try
        {
            Assert.Equal(3, results.Count);

            // First result: offset 100
            Assert.Equal(10, results[0].Memory.Length);
            Assert.Equal((byte)(100 % 256), results[0].Memory.Span[0]);

            // Second result: offset 0
            Assert.Equal(5, results[1].Memory.Length);
            Assert.Equal(0, results[1].Memory.Span[0]);

            // Third result: offset 500
            Assert.Equal(20, results[2].Memory.Length);
            Assert.Equal((byte)(500 % 256), results[2].Memory.Span[0]);
        }
        finally
        {
            foreach (var buf in results)
                buf.Dispose();
        }
    }

    [Fact]
    public async Task ReadRangesAsync_EmptyList_ReturnsEmptyList()
    {
        using var file = new LocalRandomAccessFile(_tempFile);
        IReadOnlyList<IMemoryOwner<byte>> results = await file.ReadRangesAsync([]);
        Assert.Empty(results);
    }

    [Fact]
    public async Task ReadRangesAsync_SingleRange_Works()
    {
        using var file = new LocalRandomAccessFile(_tempFile);
        IReadOnlyList<IMemoryOwner<byte>> results =
            await file.ReadRangesAsync([new FileRange(0, 10)]);
        try
        {
            Assert.Single(results);
            Assert.Equal(10, results[0].Memory.Length);
        }
        finally
        {
            foreach (var buf in results)
                buf.Dispose();
        }
    }
}
