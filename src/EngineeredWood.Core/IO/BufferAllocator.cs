// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;

namespace EngineeredWood.IO;

/// <summary>
/// Abstract factory for allocating <see cref="IMemoryOwner{T}"/> buffers.
/// </summary>
public abstract class BufferAllocator
{
    /// <summary>
    /// Allocates a buffer of exactly <paramref name="size"/> bytes.
    /// The returned <see cref="IMemoryOwner{T}.Memory"/> will have length equal to <paramref name="size"/>.
    /// </summary>
    public abstract IMemoryOwner<byte> Allocate(int size);
}
