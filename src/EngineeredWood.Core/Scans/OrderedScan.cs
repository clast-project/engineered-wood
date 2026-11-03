// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace EngineeredWood.Scans;

/// <summary>
/// Reads several units of work at once — the files of a table scan (#607), the row groups of a Parquet file
/// (#612) — and yields their items in UNIT order, then item order within a unit: the order a serial read
/// would give.
///
/// <para><b>Shape.</b> The units come from an async sequence that is enumerated ONCE, in order, by a single
/// dispatcher, and only as far ahead as the scan may run. That sequence may do work of its own to decide what
/// the next unit is — a Parquet file's row-group pruning, which reads dictionaries and page indexes and keeps
/// state that is not thread-safe — and that work stays serial, overlapped with the reads of earlier units.
/// Each dispatched unit gets a producer task that reads it to the end into its own bounded channel, and the
/// consumer drains unit <c>i</c>'s channel completely before it touches unit <c>i+1</c>'s, which is all the
/// ordering there is. At most <c>maxConcurrent</c> producers run at once, and at most twice that many units
/// are dispatched and not yet drained.</para>
///
/// <para><b>Memory.</b> Bounded by <see cref="ScanMemoryBudget"/> in bytes, not by unit count: a unit's read
/// reserves budget BEFORE it decodes its next item (through its <see cref="ScanReservation"/>), the
/// reservation is trued up to the item's measured size once decoded, and it is released when the consumer
/// takes the item. See <see cref="ScanMemoryBudget"/> for the rules that keep the front unit from deadlocking
/// behind the units after it. Each channel also holds at most <see cref="ChannelCapacity"/> items, so tiny
/// items cannot pile up either, and a slow consumer applies backpressure through both.</para>
///
/// <para><b>Errors.</b> A failing producer completes its channel with the exception, and a failing unit
/// sequence is turned into one last unit whose channel is completed with it. Either way the consumer sees the
/// error only on reaching that position, after every item before it — where a serial read would have thrown.
/// No unit after a failed one is dispatched.</para>
///
/// <para><b>Early exit and cancellation.</b> However the enumeration ends (completion, <c>break</c>,
/// <c>DisposeAsync</c>, an exception, the token), the <c>finally</c> cancels the dispatcher and every
/// producer, WAITS for all of them, and only then drains every channel, disposing each item no caller
/// received. Waiting first is what makes the drain final: nothing can write after it.</para>
/// </summary>
internal static class OrderedScan
{
    /// <summary>Items one unit may queue ahead of the consumer, on top of the byte budget.</summary>
    internal const int ChannelCapacity = 4;

    /// <summary>
    /// Reads every unit of <paramref name="units"/> through <paramref name="readUnit"/> (given the unit, its
    /// reservation and a token that fires when the scan stops), at most <paramref name="maxConcurrent"/> at
    /// once, holding what <paramref name="budget"/> allows. <paramref name="sizeOf"/> measures an item;
    /// <paramref name="dispose"/> releases one no caller received.
    /// </summary>
    public static async IAsyncEnumerable<TItem> RunAsync<TUnit, TItem>(
        IAsyncEnumerable<TUnit> units,
        Func<TUnit, ScanReservation, CancellationToken, IAsyncEnumerable<TItem>> readUnit,
        Func<TItem, long> sizeOf,
        Action<TItem> dispose,
        int maxConcurrent,
        ScanMemoryBudget budget,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrent < 1)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrent), maxConcurrent, "Must be at least 1.");

        var run = new Run<TUnit, TItem>(units, readUnit, sizeOf, dispose, maxConcurrent, budget, cancellationToken);
        run.Start();
        try
        {
            var slots = run.Slots;
            while (await slots.WaitToReadAsync(run.Token).ConfigureAwait(false))
            {
                while (slots.TryRead(out var slot))
                {
                    budget.SetHead(slot.Ordinal);
                    var reader = slot.Channel.Reader;
                    while (await reader.WaitToReadAsync(run.Token).ConfigureAwait(false))
                    {
                        // The token is checked per item, not only when the channel runs dry: items already
                        // queued must not keep flowing to a caller that has cancelled. Checked BEFORE taking an
                        // item, so one that stays queued is disposed by the shutdown drain.
                        while (true)
                        {
                            run.Token.ThrowIfCancellationRequested();
                            if (!reader.TryRead(out var entry))
                                break;
                            budget.Dequeue(slot.Ordinal, entry.Charge);
                            yield return entry.Item;
                        }
                    }
                    run.Drained(slot.Ordinal);
                }
            }
        }
        finally
        {
            await run.ShutdownAsync().ConfigureAwait(false);
        }
    }

    /// <summary>The units <c>0 .. count-1</c>, for a scan whose units are known up front.</summary>
    internal static async IAsyncEnumerable<int> Range(int count)
    {
        for (int i = 0; i < count; i++)
            yield return i;
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private readonly record struct Slot<TItem>(int Ordinal, Channel<(TItem Item, long Charge)> Channel);

    private sealed class Run<TUnit, TItem>
    {
        private readonly IAsyncEnumerable<TUnit> _units;
        private readonly Func<TUnit, ScanReservation, CancellationToken, IAsyncEnumerable<TItem>> _readUnit;
        private readonly Func<TItem, long> _sizeOf;
        private readonly Action<TItem> _dispose;
        private readonly ScanMemoryBudget _budget;
        private readonly CancellationTokenSource _cts;
        private readonly SemaphoreSlim _running;

        // Dispatched units whose slots the consumer has not yet drained. The consumer holds one slot outside
        // this channel while it drains it, so the channel holds one fewer than the lookahead.
        private readonly Channel<Slot<TItem>> _slots;

        private readonly object _gate = new();
        private readonly Dictionary<int, Channel<(TItem Item, long Charge)>> _undrained = [];
        private readonly List<Task> _producers = [];
        private Task _dispatcher = Task.CompletedTask;
        private bool _failed;
        private bool _shutdown;

        internal Run(
            IAsyncEnumerable<TUnit> units,
            Func<TUnit, ScanReservation, CancellationToken, IAsyncEnumerable<TItem>> readUnit,
            Func<TItem, long> sizeOf,
            Action<TItem> dispose,
            int maxConcurrent,
            ScanMemoryBudget budget,
            CancellationToken cancellationToken)
        {
            _units = units;
            _readUnit = readUnit;
            _sizeOf = sizeOf;
            _dispose = dispose;
            _budget = budget;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _running = new SemaphoreSlim(maxConcurrent, maxConcurrent);
            // In long: doubling a window near int.MaxValue would overflow.
            int lookahead = (int)Math.Min(2L * maxConcurrent, int.MaxValue);
            _slots = Channel.CreateBounded<Slot<TItem>>(new BoundedChannelOptions(Math.Max(1, lookahead - 1))
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
            });
        }

        internal CancellationToken Token => _cts.Token;

        internal ChannelReader<Slot<TItem>> Slots => _slots.Reader;

        internal void Start() => _dispatcher = Task.Run(DispatchAsync);

        internal void Drained(int ordinal)
        {
            lock (_gate)
                _undrained.Remove(ordinal);
        }

        /// <summary>Enumerates the units and starts their producers. Never throws: every outcome completes
        /// the slot channel.</summary>
        private async Task DispatchAsync()
        {
            var token = _cts.Token;
            int ordinal = 0;
            try
            {
                var e = _units.GetAsyncEnumerator(token);
                try
                {
                    while (true)
                    {
                        bool more;
                        try
                        {
                            more = await e.MoveNextAsync().ConfigureAwait(false);
                        }
                        catch (Exception ex) when (!token.IsCancellationRequested)
                        {
                            // The units failed to say what comes next: that is where a serial read would have
                            // thrown, so it becomes one last unit that fails as soon as the consumer gets to it.
                            var failed = NewChannel();
                            failed.Writer.TryComplete(ex);
                            await AddSlotAsync(ordinal, failed, token).ConfigureAwait(false);
                            return;
                        }

                        if (!more || IsStopped())
                            return;

                        var channel = NewChannel();
                        await AddSlotAsync(ordinal, channel, token).ConfigureAwait(false);
                        await _running.WaitAsync(token).ConfigureAwait(false);

                        var unit = e.Current;
                        int u = ordinal;
                        lock (_gate)
                        {
                            // A unit before this one may have failed while this one waited for a slot: the
                            // consumer stops there, so this one would never be read.
                            if (_shutdown || _failed)
                            {
                                _running.Release();
                                return;
                            }
                            _producers.Add(Task.Run(() => ProduceAsync(u, unit, channel.Writer)));
                        }
                        ordinal++;
                    }
                }
                finally
                {
                    await e.DisposeAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                // Cancelled (the scan is shutting down), or the units failed to dispose: either way the
                // consumer has nothing more to read than what was dispatched.
            }
            finally
            {
                _slots.Writer.TryComplete();
            }
        }

        private bool IsStopped()
        {
            lock (_gate)
                return _failed || _shutdown;
        }

        private async ValueTask AddSlotAsync(int ordinal, Channel<(TItem, long)> channel, CancellationToken token)
        {
            lock (_gate)
                _undrained[ordinal] = channel;
            await _slots.Writer.WriteAsync(new Slot<TItem>(ordinal, channel), token).ConfigureAwait(false);
        }

        private static Channel<(TItem Item, long Charge)> NewChannel() =>
            Channel.CreateBounded<(TItem, long)>(new BoundedChannelOptions(ChannelCapacity)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
            });

        /// <summary>Reads one unit into its channel. Never throws: every outcome completes the channel.</summary>
        private async Task ProduceAsync(int ordinal, TUnit unit, ChannelWriter<(TItem Item, long Charge)> writer)
        {
            var token = _cts.Token;
            var reservation = new ScanReservation(_budget, ordinal);
            Exception? error = null;
            try
            {
                await foreach (var item in _readUnit(unit, reservation, token).WithCancellation(token).ConfigureAwait(false))
                {
                    long charge;
                    try
                    {
                        charge = _sizeOf(item);
                    }
                    catch
                    {
                        _dispose(item);
                        throw;
                    }
                    reservation.Settle(charge);
                    _budget.Enqueue(ordinal);
                    try
                    {
                        await writer.WriteAsync((item, charge), token).ConfigureAwait(false);
                    }
                    catch
                    {
                        // The item never reached the channel, so the drain will not see it: it is still ours.
                        _budget.Dequeue(ordinal, charge);
                        _dispose(item);
                        throw;
                    }
                }
            }
            catch (Exception ex)
            {
                error = ex;
            }
            finally
            {
                reservation.ReleasePending();
                reservation.ReleaseHeld();
                writer.TryComplete(error);
                if (error is not null)
                {
                    // The consumer stops here; a unit after it would never be read.
                    lock (_gate)
                        _failed = true;
                }
                _running.Release();
            }
        }

        internal async Task ShutdownAsync()
        {
            lock (_gate)
                _shutdown = true;
            _cts.Cancel();
            _budget.Cancel();

            // The dispatcher first: once it has finished, no producer can be added.
            await _dispatcher.ConfigureAwait(false);
            Task[] producers;
            lock (_gate)
                producers = [.. _producers];
            try
            {
                await Task.WhenAll(producers).ConfigureAwait(false);
            }
            catch
            {
                // ProduceAsync does not throw; nothing to surface here.
            }

            // Every producer has completed its channel, so nothing written below can be missed or written after.
            foreach (var pair in _undrained)
            {
                while (pair.Value.Reader.TryRead(out var entry))
                {
                    _budget.Dequeue(pair.Key, entry.Charge);
                    _dispose(entry.Item);
                }
            }
            _undrained.Clear();
            _cts.Dispose();
            _running.Dispose();
        }
    }
}
