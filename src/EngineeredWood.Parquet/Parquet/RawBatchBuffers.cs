// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow;

namespace EngineeredWood.Parquet;

/// <summary>
/// The buffers of a batch as read, recorded before anything works on it, so that the buffers its output no longer holds
/// can be released once the output is built (#855). A gather's release is one such case
/// (<see cref="BatchSelector.ReleaseAfterGather"/>); a Delta read's pipeline, from the batch a file reader hands it to the
/// one it returns, is another.
/// <para>A read pipeline takes over each batch a file reader hands it and passes on only what it builds from it. Every
/// step that drops something of the batch drops it unreleased: a column the table has dropped (renamed away by the column
/// mapping), a narrow column a widening replaced, a container a nested rebuild replaced. The reader slices each batch with
/// its own references to the decoded buffers, so a dropped one stays allocated until the collector finalizes it, out of
/// sight of the scan's memory budget.</para>
/// <para>Released buffer by buffer, never node by node: an array's <c>Dispose</c> also disposes its children, which the
/// output often still holds (a rebuilt struct over the read's own children). A buffer is kept when the output holds any
/// memory of the same allocation (the <see cref="MemoryManager{T}"/> or array under it), not just the same memory: a
/// step may wrap part of a buffer's memory in a buffer of its own, which owns nothing and reads the raw buffer's
/// allocation (#857 review round 4: compaction's <c>WriteTypeNormalization</c> slices a fixed-size binary column's values,
/// and a struct's validity at a byte-aligned offset).</para>
/// <para>Recorded up front because steps along the way release buffers themselves (a deletion vector's gather, a
/// representation change), and a released buffer can no longer be read. Releasing one twice releases it once: each
/// reference is its own handle, which forgets its owner on the first release.</para>
/// </summary>
internal sealed class RawBatchBuffers
{
    private readonly List<(ArrowBuffer Buffer, ReadOnlyMemory<byte> Memory)> _buffers = [];

    private RawBatchBuffers()
    {
    }

    /// <summary>Records <paramref name="raw"/>'s buffers, at every depth.</summary>
    public static RawBatchBuffers Of(RecordBatch raw)
    {
        var buffers = new RawBatchBuffers();
        for (int i = 0; i < raw.ColumnCount; i++)
            buffers.Add(raw.Column(i).Data);
        return buffers;
    }

    /// <summary>Records <paramref name="raw"/>'s buffers, at every depth: an array a step replaces with one it builds.</summary>
    public static RawBatchBuffers Of(IArrowArray raw)
    {
        var buffers = new RawBatchBuffers();
        buffers.Add(raw.Data);
        return buffers;
    }

    /// <summary>
    /// Releases every recorded buffer that <paramref name="output"/> and <paramref name="alsoKept"/> (arrays handed on
    /// beside the batch: positions, row ids, or another batch) don't hold. Null <paramref name="output"/> releases all
    /// but those.
    /// </summary>
    public void ReleaseAllBut(RecordBatch? output, params IArrowArray?[] alsoKept)
    {
        var held = new HashSet<ReadOnlyMemory<byte>>();
        Collect(output, held);
        foreach (var array in alsoKept)
        {
            // A RecordBatch is an IArrowArray whose Data throws: its columns are what it holds.
            if (array is RecordBatch batch)
                Collect(batch, held);
            else if (array is not null)
                BatchSelector.CollectBuffers(array.Data, held);
        }

        var keptAllocations = new HashSet<object>();
        foreach (var memory in held)
        {
            if (AllocationOf(memory) is { } allocation)
                keptAllocations.Add(allocation);
        }

        foreach (var (buffer, memory) in _buffers)
        {
            // Memory with no allocation to compare by is kept: releasing it is only safe when nothing reads it.
            if (AllocationOf(memory) is { } allocation && !keptAllocations.Contains(allocation))
                buffer.Dispose();
        }
    }

    private static void Collect(RecordBatch? batch, HashSet<ReadOnlyMemory<byte>> into)
    {
        for (int i = 0; i < (batch?.ColumnCount ?? 0); i++)
            BatchSelector.CollectBuffers(batch!.Column(i).Data, into);
    }

    /// <summary>What <paramref name="memory"/> is a window of: its memory manager (one per native allocation) or its
    /// array. Neither overrides <see cref="object.Equals(object)"/>, so they compare by reference.</summary>
    private static object? AllocationOf(ReadOnlyMemory<byte> memory)
    {
        if (MemoryMarshal.TryGetMemoryManager(memory, out MemoryManager<byte>? manager))
            return manager;
        return MemoryMarshal.TryGetArray(memory, out var segment) ? segment.Array : null;
    }

    private void Add(ArrayData data)
    {
        foreach (var buffer in data.Buffers ?? [])
        {
            if (!buffer.IsEmpty)
                _buffers.Add((buffer, buffer.Memory));
        }

        foreach (var child in data.Children ?? [])
        {
            if (child is not null)
                Add(child);
        }

        if (data.Dictionary is { } dictionary)
            Add(dictionary);
    }
}
