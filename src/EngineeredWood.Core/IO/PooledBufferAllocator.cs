// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;

namespace EngineeredWood.IO;

/// <summary>
/// A <see cref="BufferAllocator"/> backed by <see cref="ArrayPool{T}.Shared"/>.
/// Rented arrays are sliced to the exact requested size.
/// </summary>
public sealed class PooledBufferAllocator : BufferAllocator
{
    /// <summary>
    /// Default shared instance.
    /// </summary>
    public static PooledBufferAllocator Default { get; } = new();

    public override IMemoryOwner<byte> Allocate(int size)
    {
        if (size == 0)
            return EmptyMemoryOwner.Instance;

        return new PooledMemoryOwner(size);
    }

    private sealed class PooledMemoryOwner : IMemoryOwner<byte>
    {
        private byte[]? _array;
        private readonly int _length;

        public PooledMemoryOwner(int length)
        {
            _length = length;
            _array = ArrayPool<byte>.Shared.Rent(length);
        }

        public Memory<byte> Memory
        {
            get
            {
                byte[]? array = _array;
#if NET8_0_OR_GREATER
                ObjectDisposedException.ThrowIf(array is null, this);
#else
                if (array is null)
                    throw new ObjectDisposedException(nameof(PooledMemoryOwner));
#endif
                return array.AsMemory(0, _length);
            }
        }

        public void Dispose()
        {
            byte[]? array = Interlocked.Exchange(ref _array, null);
            if (array is not null)
                ArrayPool<byte>.Shared.Return(array);
        }
    }

    private sealed class EmptyMemoryOwner : IMemoryOwner<byte>
    {
        public static EmptyMemoryOwner Instance { get; } = new();

        public Memory<byte> Memory => Memory<byte>.Empty;

        public void Dispose() { }
    }
}
