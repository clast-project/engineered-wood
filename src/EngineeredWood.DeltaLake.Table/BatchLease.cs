// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The buffers of a batch an internal caller hands to a buffering writer (<see cref="RollingDataFileWriter"/>,
/// <see cref="ChangeDataFeed.ChangeDataFileSink"/>) and will not read again: compaction's rows, a change file's rows read
/// back to be rewritten (#860). The writer holds the batch until the row group holding it is written, and nothing
/// released it after that, so its decoded buffers stayed allocated until the collector finalized them, out of sight of
/// the memory budget.
/// <para>The writer cannot dispose what it is handed: the public write path hands it the caller's own batches. A lease
/// says the buffers are the writers' to release. It counts its holders: the caller, which creates it and disposes it once
/// it has handed on what it holds, and each writer given a batch under it, which lets go once every row of that batch is
/// written or the write is abandoned. The buffers are released when the last holder lets go, so a batch sliced into runs
/// for several writers (a change file's rows of several change types, routed by partition) stays alive until every run
/// is written: a slice shares its parent's buffers.</para>
/// <para>Released buffer by buffer (<see cref="RawBatchBuffers"/>); a buffer already released, or managed, is left as
/// it is. Nothing else may read the buffers once the lease is handed on: they must be the caller's alone, not shared
/// with a batch it keeps.</para>
/// </summary>
internal sealed class BatchLease : IDisposable
{
    private readonly RawBatchBuffers _buffers;
    private int _holders = 1;

    /// <summary>A lease held by its creator, over <paramref name="buffers"/>.</summary>
    public BatchLease(RawBatchBuffers buffers) => _buffers = buffers;

    /// <summary>A lease held by its creator, over the buffers of <paramref name="batch"/> and of
    /// <paramref name="alsoOwned"/> (arrays handed on beside it).</summary>
    public static BatchLease Of(RecordBatch batch, params IArrowArray?[] alsoOwned)
    {
        var buffers = RawBatchBuffers.Of(batch);
        foreach (var array in alsoOwned)
            buffers.And(array);
        return new BatchLease(buffers);
    }

    /// <summary>One more holder, which must <see cref="Release"/> in turn.</summary>
    public BatchLease Acquire()
    {
        if (Interlocked.Increment(ref _holders) <= 1)
            throw new InvalidOperationException("The lease's buffers have already been released.");
        return this;
    }

    /// <summary>Lets go of one hold; the last releases the buffers.</summary>
    public void Release()
    {
        if (Interlocked.Decrement(ref _holders) == 0)
            _buffers.ReleaseAll();
    }

    /// <summary>The creator's <see cref="Release"/>.</summary>
    public void Dispose() => Release();
}
