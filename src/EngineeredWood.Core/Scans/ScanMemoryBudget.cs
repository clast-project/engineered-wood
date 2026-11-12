// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Memory;

namespace EngineeredWood.Scans;

/// <summary>
/// A unit's handle on the scan's <see cref="ScanMemoryBudget"/>: a unit is one file of a table scan, or one row
/// group of a Parquet file. The unit's read calls
/// <see cref="ReserveNextAsync"/> before it DECODES each item, so the memory a decode is about to use is
/// counted before it is used; the pipeline then trues the reservation up to the item's measured size
/// (<see cref="Settle"/>). Not thread-safe: one unit's read uses it from one logical flow.
/// </summary>
internal sealed class ScanReservation
{
    private readonly ScanMemoryBudget? _budget;
    private readonly int _unit;
    private long _pending;
    private long _estimate;
    private long _held;

    internal ScanReservation(ScanMemoryBudget? budget, int unit)
    {
        _budget = budget;
        _unit = unit;
    }

    /// <summary>A reservation that reserves nothing, for a read that is not part of a parallel scan.</summary>
    internal static ScanReservation None { get; } = new(null, 0);

    /// <summary>A lower bound for every reservation of this unit — for a Parquet file, its largest row
    /// group's uncompressed size over the projected columns; for one row group, that row group's, scaled by
    /// how far decoded batches have exceeded the footer so far.</summary>
    internal void SetInitialEstimate(long bytes) => Observe(bytes);

    /// <summary>
    /// A size this unit's reads have been seen to reach — a RAW decoded batch, before a deletion vector or a
    /// row selection shrinks it. Reservations are the LARGEST size seen, never the last one: reserving the
    /// last would let a small batch (a deletion vector's survivors, a short final row group) set up an
    /// underestimate of the full-size batch after it. So a reservation can only fall short on a unit's
    /// first batch, or on one larger than any before it.
    /// </summary>
    /// <summary>What this unit's next reservation will be, for tests.</summary>
    internal long Estimate => _estimate;

    internal void Observe(long bytes)
    {
        // An estimate past MaxCharge can only come from a corrupt footer; capped, so the totals cannot wrap.
        if (bytes > _estimate)
            _estimate = Math.Min(bytes, MemoryBudget.MaxCharge);
    }

    /// <summary>Waits until the budget can take the next item, then reserves for it. A reservation still
    /// pending from the previous call — its item was dropped before reaching the pipeline, e.g. a batch every
    /// row of which a deletion vector removed — is released first.</summary>
    internal ValueTask ReserveNextAsync(CancellationToken cancellationToken)
    {
        if (_budget is null)
            return default;
        ReleasePending();
        return ReserveCoreAsync(cancellationToken);
    }

    private async ValueTask ReserveCoreAsync(CancellationToken cancellationToken)
    {
        await _budget!.ReserveAsync(_unit, _estimate, cancellationToken).ConfigureAwait(false);
        _pending = _estimate;
    }

    /// <summary>The decoded item measured <paramref name="actual"/> bytes: its charge replaces the pending
    /// reservation. An item LARGER than its reservation is charged in full without waiting — it already
    /// exists — which is the one way the budget can be exceeded (see <see cref="ScanMemoryBudget"/>).</summary>
    internal void Settle(long actual)
    {
        if (_budget is null)
            return;
        _budget.Adjust(actual - _pending);
        _pending = 0;
        Observe(actual);
    }

    /// <summary>
    /// Waits until the budget can take <paramref name="bytes"/> more, and holds them until
    /// <see cref="ReleaseHeld"/>: memory the unit's read keeps for its whole life rather than per item — a
    /// Parquet row group's compressed column chunks, which its read holds across the batches it yields. Unlike
    /// a pending reservation, a hold is not replaced when an item is settled.
    /// </summary>
    internal async ValueTask HoldAsync(long bytes, CancellationToken cancellationToken)
    {
        if (_budget is null || bytes <= 0)
            return;
        bytes = Math.Min(bytes, MemoryBudget.MaxCharge); // as in Observe
        await _budget.ReserveAsync(_unit, bytes, cancellationToken).ConfigureAwait(false);
        _held += bytes;
    }

    /// <summary>Releases what <see cref="HoldAsync"/> held. Safe to call more than once.</summary>
    internal void ReleaseHeld()
    {
        if (_budget is null || _held == 0)
            return;
        _budget.Adjust(-_held);
        _held = 0;
    }

    internal void ReleasePending()
    {
        if (_budget is null || _pending == 0)
            return;
        _budget.Adjust(-_pending);
        _pending = 0;
    }
}


/// <summary>
/// The bytes an <see cref="OrderedScan"/> may hold: reserved for items being decoded, plus items queued
/// for the consumer. They are charged to a <see cref="MemoryBudget"/> of the scan's own, capped at the scan's
/// limit — a root when the scan is given no budget, otherwise a child of the one it is given, so that the
/// scan's charges also count against that budget's total (#719). A reservation is admitted when it fits at
/// every level, and otherwise waits for some to be freed — with two exceptions that keep the scan moving:
/// <list type="bullet">
/// <item>A level that is EMPTY admits one item even if it alone is larger than that level's cap.</item>
/// <item>The unit is the FRONT unit and has nothing queued: the consumer is waiting on exactly this unit, so
/// making it wait for the units behind it to drain would deadlock (they drain only after it). It may run
/// over the budget by the one item it is decoding — over EVERY level, since a shared parent may be full of
/// other scans' items, and another scan's consumer may be waiting on this one.</item>
/// </list>
/// <para><b>Order.</b> Room goes to units in unit order: a unit is not admitted while one nearer the front of
/// this scan waits, even if its own reservation would fit (#829). Otherwise the units furthest back, of which
/// a wide window dispatches many, take the room as it frees: they decode items the consumer will not want for
/// a long time, and the units just behind the front cannot read until each in turn becomes the front — a wider
/// window then runs SLOWER than a narrow one. The ordering is within one scan; scans sharing a parent still
/// compete for the parent's room as before.</para>
/// <para><b>Underestimates.</b> A reservation is an estimate made before the decode, and an item that comes
/// out larger is charged in full without waiting (it already exists). Each unit reserves the largest size it
/// has seen (<see cref="ScanReservation.Observe"/>), so this happens only on a unit's first item — reserved
/// from the Parquet footer, which UNDERestimates dictionary-encoded strings — or on an item larger than any
/// before it. The peak is therefore about the budget, plus one item, plus each in-flight unit's shortfall on
/// its first item: up to the window's worth of first batches when every footer estimate is badly low.</para>
/// </summary>
internal sealed class ScanMemoryBudget
{
    private readonly MemoryBudget _memory;
    private readonly object _gate;
    private readonly Dictionary<int, int> _queued = [];
    // The units waiting for room, by ordinal, so the nearest the front is First(); the count is how many of a
    // unit's reservations wait (one, in practice: a unit reserves from one logical flow).
    private readonly SortedDictionary<int, int> _waiting = new();
    private int _head;
    private bool _cancelled;
    private bool _closed;

    /// <summary>A scan of its own, capped at <paramref name="maxBytes"/>, and also within
    /// <paramref name="parent"/> when one is given.</summary>
    internal ScanMemoryBudget(long maxBytes, MemoryBudget? parent = null)
    {
        _memory = parent?.CreateChild(maxBytes) ?? new MemoryBudget(maxBytes);
        _gate = _memory.Gate;
    }

    /// <summary>Bytes reserved or queued right now.</summary>
    internal long Used => _memory.UsedBytes;

    /// <summary>The most bytes ever reserved or queued at once.</summary>
    internal long Peak => _memory.PeakBytes;

    internal async ValueTask ReserveAsync(int unit, long bytes, CancellationToken cancellationToken)
    {
        bool waiting = false;
        try
        {
            while (true)
            {
                Task changed;
                lock (_gate)
                {
                    if (_cancelled)
                        cancellationToken.ThrowIfCancellationRequested();
                    bool front = unit == _head && QueuedLocked(unit) == 0;
                    // In unit order: while a unit nearer the front waits, room that frees up is its, not this
                    // one's (#829). Taking it would decode this unit's item early, to sit in the budget until the
                    // consumer gets this far, while the units in between wait for room to read at all.
                    MemoryBudget? blocking = _memory;
                    if ((front || !EarlierWaiterLocked(unit))
                        && _memory.TryReserveLocked(bytes, force: front, out blocking))
                    {
                        if (waiting)
                        {
                            StopWaitingLocked(unit);
                            waiting = false;
                        }
                        return;
                    }
                    if (!waiting)
                    {
                        _waiting[unit] = WaitingLocked(unit) + 1;
                        waiting = true;
                    }
                    // This scan's own budget signals releases, head changes, cancellation and an earlier unit
                    // ceasing to wait; a full parent signals only releases, so a wait on one also watches this
                    // scan's.
                    changed = ReferenceEquals(blocking, _memory)
                        ? _memory.ChangedLocked
                        : Task.WhenAny(blocking!.ChangedLocked, _memory.ChangedLocked);
                }
                await WaitAsync(changed, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (waiting)
            {
                lock (_gate)
                    StopWaitingLocked(unit);
            }
        }
    }

    /// <summary>Whether a unit nearer the front than <paramref name="unit"/> is waiting for room.</summary>
    private bool EarlierWaiterLocked(int unit) => _waiting.Count != 0 && _waiting.Keys.First() < unit;

    private int WaitingLocked(int unit) => _waiting.TryGetValue(unit, out int n) ? n : 0;

    /// <summary>Takes one wait of <paramref name="unit"/> off the list, and wakes the waiters behind it: one of
    /// them may now be first in line.</summary>
    private void StopWaitingLocked(int unit)
    {
        int left = WaitingLocked(unit) - 1;
        if (left <= 0)
            _waiting.Remove(unit);
        else
            _waiting[unit] = left;
        _memory.SignalLocked();
    }

    internal void Adjust(long delta)
    {
        if (delta == 0)
            return;
        lock (_gate)
            _memory.AdjustLocked(delta);
    }

    internal void Enqueue(int unit)
    {
        lock (_gate)
            _queued[unit] = QueuedLocked(unit) + 1;
    }

    internal void Dequeue(int unit, long charge)
    {
        lock (_gate)
        {
            int left = QueuedLocked(unit) - 1;
            if (left <= 0)
                _queued.Remove(unit);
            else
                _queued[unit] = left;
            // A release wakes the waiters itself. With nothing charged they still need waking: the unit's queue
            // count changed, which can make it the front unit.
            if (charge != 0)
                _memory.AdjustLocked(-charge);
            else
                _memory.SignalLocked();
        }
    }

    internal void SetHead(int unit)
    {
        lock (_gate)
        {
            _head = unit;
            _memory.SignalLocked();
        }
    }

    /// <summary>Wakes every waiter so it can observe its own (now cancelled) token.</summary>
    internal void Cancel()
    {
        lock (_gate)
        {
            _cancelled = true;
            _memory.SignalLocked();
        }
    }

    /// <summary>
    /// The scan is over: hands back whatever is still charged to it. Once every producer has finished and every
    /// queued item has been dequeued, nothing should be; this guards a shared parent, which outlives the scan,
    /// against a charge the scan lost track of. Safe to call more than once.
    /// </summary>
    internal void Close()
    {
        lock (_gate)
        {
            if (_closed)
                return;
            _closed = true;
            long left = _memory.UsedLocked;
            if (left != 0)
                _memory.AdjustLocked(-left);
        }
    }

    private int QueuedLocked(int unit) => _queued.TryGetValue(unit, out int n) ? n : 0;

    private static async Task WaitAsync(Task task, CancellationToken cancellationToken)
    {
#if NET8_0_OR_GREATER
        await task.WaitAsync(cancellationToken).ConfigureAwait(false);
#else
        if (!cancellationToken.CanBeCanceled)
        {
            await task.ConfigureAwait(false);
            return;
        }
        var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (cancellationToken.Register(static s => ((TaskCompletionSource<bool>)s!).TrySetResult(true), cancelled))
        {
            await Task.WhenAny(task, cancelled.Task).ConfigureAwait(false);
        }
        cancellationToken.ThrowIfCancellationRequested();
#endif
    }
}
