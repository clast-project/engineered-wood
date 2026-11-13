// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

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
/// output often still holds (a rebuilt struct over the read's own children). A buffer is kept when the output holds the
/// same memory. No step wraps part of another buffer's memory in a buffer of its own, so identity is enough.</para>
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

    /// <summary>
    /// Releases every recorded buffer that <paramref name="output"/> and <paramref name="alsoKept"/> (arrays handed on
    /// beside the batch: positions, row ids, or another batch) don't hold. Null <paramref name="output"/> releases all
    /// but those.
    /// </summary>
    public void ReleaseAllBut(RecordBatch? output, params IArrowArray?[] alsoKept)
    {
        var kept = new HashSet<ReadOnlyMemory<byte>>();
        Collect(output, kept);
        foreach (var array in alsoKept)
        {
            // A RecordBatch is an IArrowArray whose Data throws: its columns are what it holds.
            if (array is RecordBatch batch)
                Collect(batch, kept);
            else if (array is not null)
                BatchSelector.CollectBuffers(array.Data, kept);
        }

        foreach (var (buffer, memory) in _buffers)
        {
            if (!kept.Contains(memory))
                buffer.Dispose();
        }
    }

    private static void Collect(RecordBatch? batch, HashSet<ReadOnlyMemory<byte>> into)
    {
        for (int i = 0; i < (batch?.ColumnCount ?? 0); i++)
            BatchSelector.CollectBuffers(batch!.Column(i).Data, into);
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
