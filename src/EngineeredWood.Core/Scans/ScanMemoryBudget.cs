// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

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
    internal void Observe(long bytes)
    {
        if (bytes > _estimate)
            _estimate = bytes;
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
/// for the consumer. A reservation is admitted when it fits, and otherwise waits for the consumer to free
/// some — with two exceptions that keep the scan moving:
/// <list type="bullet">
/// <item>The budget is EMPTY: one item is admitted even if it alone is larger than the budget.</item>
/// <item>The unit is the FRONT unit and has nothing queued: the consumer is waiting on exactly this unit, so
/// making it wait for the units behind it to drain would deadlock (they drain only after it). It may run
/// over the budget by the one item it is decoding.</item>
/// </list>
/// <para><b>Underestimates.</b> A reservation is an estimate made before the decode, and an item that comes
/// out larger is charged in full without waiting (it already exists). Each unit reserves the largest size it
/// has seen (<see cref="ScanReservation.Observe"/>), so this happens only on a unit's first item — reserved
/// from the Parquet footer, which UNDERestimates dictionary-encoded strings — or on an item larger than any
/// before it. The peak is therefore about the budget, plus one item, plus each in-flight unit's shortfall on
/// its first item: up to the window's worth of first batches when every footer estimate is badly low.</para>
/// </summary>
internal sealed class ScanMemoryBudget
{
    private readonly object _gate = new();
    private readonly long _max;
    private readonly Dictionary<int, int> _queued = [];
    private long _used;
    private int _head;
    private TaskCompletionSource<bool> _changed = NewSignal();
    private bool _cancelled;

    internal ScanMemoryBudget(long maxBytes) => _max = maxBytes;

    /// <summary>Bytes reserved or queued right now.</summary>
    internal long Used
    {
        get { lock (_gate) return _used; }
    }

    /// <summary>The most bytes ever reserved or queued at once.</summary>
    internal long Peak { get; private set; }

    internal async ValueTask ReserveAsync(int unit, long bytes, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                if (_cancelled)
                    cancellationToken.ThrowIfCancellationRequested();
                if (_used == 0
                    || _used + bytes <= _max
                    || (unit == _head && QueuedLocked(unit) == 0))
                {
                    _used += bytes;
                    if (_used > Peak)
                        Peak = _used;
                    return;
                }
                changed = _changed.Task;
            }
            await WaitAsync(changed, cancellationToken).ConfigureAwait(false);
        }
    }

    internal void Adjust(long delta)
    {
        if (delta == 0)
            return;
        lock (_gate)
        {
            _used += delta;
            if (_used > Peak)
                Peak = _used;
            if (delta < 0)
                SignalLocked();
        }
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
            _used -= charge;
            SignalLocked();
        }
    }

    internal void SetHead(int unit)
    {
        lock (_gate)
        {
            _head = unit;
            SignalLocked();
        }
    }

    /// <summary>Wakes every waiter so it can observe its own (now cancelled) token.</summary>
    internal void Cancel()
    {
        lock (_gate)
        {
            _cancelled = true;
            SignalLocked();
        }
    }

    private int QueuedLocked(int unit) => _queued.TryGetValue(unit, out int n) ? n : 0;

    private void SignalLocked()
    {
        var old = _changed;
        _changed = NewSignal();
        old.TrySetResult(true);
    }

    private static TaskCompletionSource<bool> NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

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
