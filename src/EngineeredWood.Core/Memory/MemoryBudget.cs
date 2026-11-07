// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Memory;

/// <summary>
/// A cap on the memory, in bytes of Arrow buffers, that the activities drawing on it may hold at once. Budgets
/// form a tree: a budget made by <see cref="CreateChild"/> counts everything charged to it against itself AND
/// every budget above it, so a child caps one activity while its parent caps the total of all its children.
/// </summary>
/// <remarks>
/// <para>Supply one as <c>MemoryBudget</c> on <c>ParquetReadOptions</c> or <c>DeltaTableOptions</c>, and every
/// parallel scan through those options draws from it, each through a child of its own capped at the options'
/// <c>MaxBufferedReadBytes</c>. Several readers or tables given the same budget share its total.</para>
/// <para><b>What counts.</b> Today, only parallel scans: the batches they have decoded and not yet handed to
/// the caller, and the compressed column chunks of the row groups they are reading. A batch leaves the budget
/// when the caller receives it. Serial reads, the planner's dictionary and page-index windows, a writer's
/// pending row group and its upload buffers are not counted yet (#719). The numbers are the bytes EngineeredWood
/// measures, not the process's memory: buffers inside storage SDKs, the GC's overhead and fragmentation are not
/// seen.</para>
/// <para><b>Overruns.</b> A reservation that does not fit waits until enough is released, with two exceptions
/// that keep every scan moving, so that contention slows scans down but never deadlocks them. A budget that
/// holds nothing admits one reservation even if it alone is larger than the budget. And the read a scan's
/// caller is waiting on is always admitted, however full the budgets are: that scan's consumer can release
/// nothing until it arrives.</para>
/// <para>What that read is admitted for is sized by ROW GROUP, not by batch: before its first batch it
/// reserves the row group's projected uncompressed size from the footer, and a Parquet read also holds the row
/// group's projected compressed column chunks for as long as it reads them. A small <c>BatchSize</c> shrinks
/// neither. So the total can run over by about one row group per scan in progress (its compressed plus its
/// uncompressed size), plus what a scan's estimates fall short of its batches (see
/// <c>MaxBufferedReadBytes</c>). Size a shared budget with the files' row-group sizes in mind.</para>
/// <para>Thread-safe. A budget holds no resources and needs no disposal.</para>
/// </remarks>
public sealed class MemoryBudget
{
    // One lock for a whole tree: a reservation checks and charges every level from the child to the root
    // atomically.
    private readonly object _gate;
    private readonly long _max;
    private long _used;
    private long _peak;
    private TaskCompletionSource<bool> _changed = NewSignal();

    /// <summary>A budget of its own, capped at <paramref name="maxBytes"/>.</summary>
    /// <param name="maxBytes">The most bytes it may hold at once; <see cref="long.MaxValue"/> for no cap, to
    /// only measure (<see cref="PeakBytes"/>).</param>
    public MemoryBudget(long maxBytes)
        : this(maxBytes, parent: null)
    {
    }

    private MemoryBudget(long maxBytes, MemoryBudget? parent)
    {
        if (maxBytes < 1)
            throw new ArgumentOutOfRangeException(nameof(maxBytes), maxBytes, "Must be at least 1.");
        _max = maxBytes;
        Parent = parent;
        _gate = parent?._gate ?? new object();
    }

    /// <summary>
    /// A budget capped at <paramref name="maxBytes"/> whose charges also count against this one (and the budgets
    /// above it). A child larger than its parent is allowed; the parent's cap still applies to it.
    /// </summary>
    public MemoryBudget CreateChild(long maxBytes) => new(maxBytes, this);

    /// <summary>The budget this one's charges also count against, or null for a root.</summary>
    public MemoryBudget? Parent { get; }

    /// <summary>The most bytes this budget may hold at once.</summary>
    public long MaxBytes => _max;

    /// <summary>The bytes charged to this budget and its children right now.</summary>
    public long UsedBytes
    {
        get { lock (_gate) return _used; }
    }

    /// <summary>The most bytes ever charged to this budget and its children at once.</summary>
    public long PeakBytes
    {
        get { lock (_gate) return _peak; }
    }

    /// <summary><see cref="UsedBytes"/>, for a caller that holds <see cref="Gate"/>.</summary>
    internal long UsedLocked => _used;

    /// <summary>The lock every budget in this one's tree shares.</summary>
    internal object Gate => _gate;

    /// <summary>
    /// Charges <paramref name="bytes"/> to this budget and every one above it, if each of them can take it:
    /// holds nothing yet, or stays within its cap. <paramref name="force"/> charges it regardless. When a level
    /// cannot, nothing is charged and <paramref name="blocking"/> is the nearest such level. Caller holds
    /// <see cref="Gate"/>.
    /// </summary>
    internal bool TryReserveLocked(long bytes, bool force, out MemoryBudget? blocking)
    {
        if (!force)
        {
            for (var level = this; level is not null; level = level.Parent)
            {
                if (level._used != 0 && level._used + bytes > level._max)
                {
                    blocking = level;
                    return false;
                }
            }
        }
        AdjustLocked(bytes);
        blocking = null;
        return true;
    }

    /// <summary>Adds <paramref name="delta"/> to this budget and every one above it, without waiting. A release
    /// (a negative delta) wakes whatever waits on each of them. Caller holds <see cref="Gate"/>.</summary>
    internal void AdjustLocked(long delta)
    {
        if (delta == 0)
            return;
        for (var level = this; level is not null; level = level.Parent)
        {
            level._used += delta;
            if (level._used > level._peak)
                level._peak = level._used;
            if (delta < 0)
                level.SignalLocked();
        }
    }

    /// <summary>Completes when this budget next releases anything (or <see cref="SignalLocked"/> is called).
    /// Caller holds <see cref="Gate"/>.</summary>
    internal Task ChangedLocked => _changed.Task;

    /// <summary>Wakes whatever waits on this budget. Caller holds <see cref="Gate"/>.</summary>
    internal void SignalLocked()
    {
        var old = _changed;
        _changed = NewSignal();
        old.TrySetResult(true);
    }

    private static TaskCompletionSource<bool> NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
