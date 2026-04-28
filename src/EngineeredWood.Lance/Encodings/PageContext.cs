// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Encodings;

/// <summary>
/// Pre-loaded buffers for a single page decode. Buffers are indexed by the
/// <see cref="Proto.Encodings.V20.Buffer.Types.BufferType"/> enum: a
/// <c>Buffer { buffer_index = i, buffer_type = page }</c> reference
/// resolves to <see cref="PageBuffers"/>[i].
/// </summary>
internal readonly struct PageContext
{
    public IReadOnlyList<ReadOnlyMemory<byte>> PageBuffers { get; }

    /// <summary>Column-metadata buffers (referenced when <c>buffer_type = column</c>).</summary>
    public IReadOnlyList<ReadOnlyMemory<byte>> ColumnBuffers { get; }

    /// <summary>File-metadata buffers (global buffers referenced when <c>buffer_type = file</c>).</summary>
    public IReadOnlyList<ReadOnlyMemory<byte>> FileBuffers { get; }

    public PageContext(
        IReadOnlyList<ReadOnlyMemory<byte>> pageBuffers,
        IReadOnlyList<ReadOnlyMemory<byte>>? columnBuffers = null,
        IReadOnlyList<ReadOnlyMemory<byte>>? fileBuffers = null)
    {
        PageBuffers = pageBuffers;
        ColumnBuffers = columnBuffers ?? Array.Empty<ReadOnlyMemory<byte>>();
        FileBuffers = fileBuffers ?? Array.Empty<ReadOnlyMemory<byte>>();
    }

    public ReadOnlyMemory<byte> Resolve(Proto.Encodings.V20.Buffer buffer)
    {
        return buffer.BufferType switch
        {
            Proto.Encodings.V20.Buffer.Types.BufferType.Page => GetAt(PageBuffers, buffer.BufferIndex, "page"),
            Proto.Encodings.V20.Buffer.Types.BufferType.Column => GetAt(ColumnBuffers, buffer.BufferIndex, "column"),
            Proto.Encodings.V20.Buffer.Types.BufferType.File => GetAt(FileBuffers, buffer.BufferIndex, "file"),
            _ => throw new LanceFormatException(
                $"Unknown buffer type {buffer.BufferType}."),
        };
    }

    private static ReadOnlyMemory<byte> GetAt(
        IReadOnlyList<ReadOnlyMemory<byte>> list, uint index, string label)
    {
        if (index >= list.Count)
            throw new LanceFormatException(
                $"Buffer index {index} is out of range for {label} buffers (count={list.Count}).");
        return list[(int)index];
    }
}
