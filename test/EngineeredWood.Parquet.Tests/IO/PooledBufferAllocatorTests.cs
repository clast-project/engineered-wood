// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.Tests.IO;

public class PooledBufferAllocatorTests
{
    [Fact]
    public void Allocate_ReturnsExactRequestedSize()
    {
        var allocator = PooledBufferAllocator.Default;
        using var buffer = allocator.Allocate(100);
        Assert.Equal(100, buffer.Memory.Length);
    }

    [Fact]
    public void Allocate_ZeroSize_ReturnsEmptyMemory()
    {
        var allocator = PooledBufferAllocator.Default;
        using var buffer = allocator.Allocate(0);
        Assert.Equal(0, buffer.Memory.Length);
    }

    [Fact]
    public void Allocate_LargeSize_ReturnsExactSize()
    {
        // ArrayPool will rent a larger array; verify we get exact slice
        var allocator = PooledBufferAllocator.Default;
        using var buffer = allocator.Allocate(1000);
        Assert.Equal(1000, buffer.Memory.Length);
    }

    [Fact]
    public void Dispose_PreventsSubsequentMemoryAccess()
    {
        var allocator = PooledBufferAllocator.Default;
        var buffer = allocator.Allocate(100);
        buffer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => buffer.Memory);
    }

    [Fact]
    public void DoubleDispose_DoesNotThrow()
    {
        var allocator = PooledBufferAllocator.Default;
        var buffer = allocator.Allocate(100);
        buffer.Dispose();
        buffer.Dispose(); // should not throw
    }

    [Fact]
    public void ZeroSizeBuffer_DoubleDispose_DoesNotThrow()
    {
        var allocator = PooledBufferAllocator.Default;
        var buffer = allocator.Allocate(0);
        buffer.Dispose();
        buffer.Dispose();
    }

    [Fact]
    public void Allocate_BufferIsWritable()
    {
        var allocator = PooledBufferAllocator.Default;
        using var buffer = allocator.Allocate(4);
        buffer.Memory.Span[0] = 0xAA;
        buffer.Memory.Span[3] = 0xBB;
        Assert.Equal(0xAA, buffer.Memory.Span[0]);
        Assert.Equal(0xBB, buffer.Memory.Span[3]);
    }
}
