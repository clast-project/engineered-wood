// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Reads several files at once and yields their items in FILE order, then item order within a file — the
/// order a serial read would give (#607).
///
/// <para><b>Shape.</b> Each running file has a producer task that reads it to the end and writes into its own
/// bounded channel. The consumer drains file <c>i</c>'s channel completely before it touches file <c>i+1</c>'s,
/// which is all the ordering there is. At most <c>maxConcurrentFiles</c> producers run at once; a finishing
/// producer starts the next file, so a slow file at the front does not idle the others.</para>
///
/// <para><b>Memory.</b> Bounded by <see cref="ScanMemoryBudget"/> in bytes, not by file count: a file's read
/// reserves budget BEFORE it decodes its next item (through its <see cref="ScanReservation"/>), the reservation
/// is trued up to the item's measured size once decoded, and it is released when the consumer takes the item.
/// See <see cref="ScanMemoryBudget"/> for the rules that keep the front file from deadlocking behind the files
/// ahead of it. Each channel also holds at most <see cref="ChannelCapacity"/> items, so tiny items cannot pile
/// up either, and a slow consumer applies backpressure through both.</para>
///
/// <para><b>Errors.</b> A failing producer completes its channel with the exception. The consumer sees it only
/// on reaching that file, after the items the file produced before failing — where a serial read would have
/// thrown. No file after it is started.</para>
///
/// <para><b>Early exit and cancellation.</b> However the enumeration ends (completion, <c>break</c>,
/// <c>DisposeAsync</c>, an exception, the token), the <c>finally</c> cancels every producer, WAITS for all of
/// them, and only then drains every channel, disposing each item no caller received. Waiting first is what
/// makes the drain final: no producer can write after it.</para>
/// </summary>
internal sealed class OrderedFileScan<T>
{
    /// <summary>Items one file may queue ahead of the consumer, on top of the byte budget.</summary>
    internal const int ChannelCapacity = 4;

    private readonly int _fileCount;
    private readonly Func<int, ScanReservation, CancellationToken, IAsyncEnumerable<T>> _readFile;
    private readonly Func<T, long> _sizeOf;
    private readonly Action<T> _dispose;
    private readonly int _maxConcurrentFiles;
    private readonly int _maxFilesAhead;
    private readonly ScanMemoryBudget _budget;
    private readonly CancellationTokenSource _cts;

    private readonly object _gate = new();
    private readonly Channel<(T Item, long Charge)>?[] _channels;
    private readonly List<Task> _producers = [];
    private int _head;
    private int _next;
    private int _running;
    private int _stopAfter = int.MaxValue;
    private bool _shutdown;

    private OrderedFileScan(
        int fileCount,
        Func<int, ScanReservation, CancellationToken, IAsyncEnumerable<T>> readFile,
        Func<T, long> sizeOf,
        Action<T> dispose,
        int maxConcurrentFiles,
        ScanMemoryBudget budget,
        CancellationToken cancellationToken)
    {
        _fileCount = fileCount;
        _readFile = readFile;
        _sizeOf = sizeOf;
        _dispose = dispose;
        _maxConcurrentFiles = maxConcurrentFiles;
        // Files whose producers finished but whose items the consumer has not reached still hold channels;
        // the budget bounds their bytes, this bounds how many of them there are.
        // In long, and capped at the file count: doubling a window near int.MaxValue would overflow to a
        // negative lookahead, and the scan would never start a file.
        _maxFilesAhead = (int)Math.Min(2L * maxConcurrentFiles, fileCount);
        _budget = budget;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _channels = new Channel<(T, long)>?[fileCount];
    }

    /// <summary>
    /// Reads <paramref name="fileCount"/> files through <paramref name="readFile"/> (given the file's index, its
    /// reservation and a token that fires when the scan stops), at most <paramref name="maxConcurrentFiles"/> at
    /// once, with at most about <paramref name="maxBufferedBytes"/> of items in memory.
    /// <paramref name="sizeOf"/> measures an item; <paramref name="dispose"/> releases one no caller received.
    /// </summary>
    public static IAsyncEnumerable<T> RunAsync(
        int fileCount,
        Func<int, ScanReservation, CancellationToken, IAsyncEnumerable<T>> readFile,
        Func<T, long> sizeOf,
        Action<T> dispose,
        int maxConcurrentFiles,
        long maxBufferedBytes,
        CancellationToken cancellationToken = default)
    {
        if (maxBufferedBytes < 1)
            throw new ArgumentOutOfRangeException(nameof(maxBufferedBytes), maxBufferedBytes, "Must be at least 1.");
        return RunAsync(fileCount, readFile, sizeOf, dispose, maxConcurrentFiles,
            new ScanMemoryBudget(maxBufferedBytes), cancellationToken);
    }

    /// <summary>As above, against a caller-supplied <paramref name="budget"/> (tests read its
    /// <see cref="ScanMemoryBudget.Peak"/>).</summary>
    internal static async IAsyncEnumerable<T> RunAsync(
        int fileCount,
        Func<int, ScanReservation, CancellationToken, IAsyncEnumerable<T>> readFile,
        Func<T, long> sizeOf,
        Action<T> dispose,
        int maxConcurrentFiles,
        ScanMemoryBudget budget,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrentFiles < 1)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrentFiles), maxConcurrentFiles, "Must be at least 1.");
        if (fileCount == 0)
            yield break;

        var scan = new OrderedFileScan<T>(
            fileCount, readFile, sizeOf, dispose, maxConcurrentFiles, budget, cancellationToken);
        try
        {
            for (int file = 0; file < fileCount; file++)
            {
                var reader = scan.Advance(file);
                while (await reader.WaitToReadAsync(scan._cts.Token).ConfigureAwait(false))
                {
                    // The token is checked per item, not only when the channel runs dry: items already queued
                    // must not keep flowing to a caller that has cancelled. Checked BEFORE taking an item, so
                    // one that stays queued is disposed by the shutdown drain.
                    while (true)
                    {
                        scan._cts.Token.ThrowIfCancellationRequested();
                        if (!reader.TryRead(out var entry))
                            break;
                        scan._budget.Dequeue(file, entry.Charge);
                        yield return entry.Item;
                    }
                }
            }
        }
        finally
        {
            await scan.ShutdownAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Makes <paramref name="file"/> the front of the scan, starting whatever may now start.</summary>
    private ChannelReader<(T Item, long Charge)> Advance(int file)
    {
        Channel<(T, long)> channel;
        lock (_gate)
        {
            if (file > 0)
                _channels[file - 1] = null; // drained and complete; nothing reads it again
            _head = file;
            channel = ChannelFor(file);
        }
        _budget.SetHead(file);
        StartMore();
        return channel.Reader;
    }

    private Channel<(T, long)> ChannelFor(int file) =>
        _channels[file] ??= Channel.CreateBounded<(T, long)>(new BoundedChannelOptions(ChannelCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

    private void StartMore()
    {
        lock (_gate)
        {
            while (!_shutdown
                   && _running < _maxConcurrentFiles
                   && _next < _fileCount
                   && _next <= _stopAfter
                   && _next - _head < _maxFilesAhead)
            {
                int file = _next++;
                _running++;
                var channel = ChannelFor(file);
                _producers.Add(Task.Run(() => ProduceAsync(file, channel.Writer)));
            }
        }
    }

    /// <summary>Reads one file into its channel. Never throws: every outcome completes the channel.</summary>
    private async Task ProduceAsync(int file, ChannelWriter<(T Item, long Charge)> writer)
    {
        var token = _cts.Token;
        var reservation = new ScanReservation(_budget, file);
        Exception? error = null;
        try
        {
            await foreach (var item in _readFile(file, reservation, token).WithCancellation(token).ConfigureAwait(false))
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
                _budget.Enqueue(file);
                try
                {
                    await writer.WriteAsync((item, charge), token).ConfigureAwait(false);
                }
                catch
                {
                    // The item never reached the channel, so the drain will not see it: it is still ours.
                    _budget.Dequeue(file, charge);
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
            writer.TryComplete(error);
            lock (_gate)
            {
                _running--;
                if (error is not null && file < _stopAfter)
                    _stopAfter = file; // the consumer stops here; a file after it would never be read
            }
            StartMore();
        }
    }

    private async Task ShutdownAsync()
    {
        Task[] producers;
        lock (_gate)
        {
            _shutdown = true;
            producers = _producers.ToArray();
        }
        _cts.Cancel();
        _budget.Cancel();
        try
        {
            await Task.WhenAll(producers).ConfigureAwait(false);
        }
        catch
        {
            // ProduceAsync does not throw; nothing to surface here.
        }

        // Every producer has completed its channel, so nothing written below can be missed or written after.
        for (int file = 0; file < _channels.Length; file++)
        {
            if (_channels[file] is not { } channel)
                continue;
            while (channel.Reader.TryRead(out var entry))
            {
                _budget.Dequeue(file, entry.Charge);
                _dispose(entry.Item);
            }
        }
        _cts.Dispose();
    }
}

/// <summary>
/// A file's handle on the scan's <see cref="ScanMemoryBudget"/>. The file's read calls
/// <see cref="ReserveNextAsync"/> before it DECODES each item, so the memory a decode is about to use is
/// counted before it is used; the pipeline then trues the reservation up to the item's measured size
/// (<see cref="Settle"/>). Not thread-safe: one file's read uses it from one logical flow.
/// </summary>
internal sealed class ScanReservation
{
    private readonly ScanMemoryBudget? _budget;
    private readonly int _file;
    private long _pending;
    private long _estimate;

    internal ScanReservation(ScanMemoryBudget? budget, int file)
    {
        _budget = budget;
        _file = file;
    }

    /// <summary>A reservation that reserves nothing, for a read that is not part of a parallel scan.</summary>
    internal static ScanReservation None { get; } = new(null, 0);

    /// <summary>A lower bound for every reservation of this file — for a Parquet file, its largest row
    /// group's uncompressed size over the projected columns.</summary>
    internal void SetInitialEstimate(long bytes) => Observe(bytes);

    /// <summary>
    /// A size this file's reads have been seen to reach — a RAW decoded batch, before a deletion vector or a
    /// row selection shrinks it. Reservations are the LARGEST size seen, never the last one: reserving the
    /// last would let a small batch (a deletion vector's survivors, a short final row group) set up an
    /// underestimate of the full-size batch after it. So a reservation can only fall short on a file's
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
        await _budget!.ReserveAsync(_file, _estimate, cancellationToken).ConfigureAwait(false);
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
/// The bytes an <see cref="OrderedFileScan{T}"/> may hold: reserved for items being decoded, plus items queued
/// for the consumer. A reservation is admitted when it fits, and otherwise waits for the consumer to free
/// some — with two exceptions that keep the scan moving:
/// <list type="bullet">
/// <item>The budget is EMPTY: one item is admitted even if it alone is larger than the budget.</item>
/// <item>The file is the FRONT file and has nothing queued: the consumer is waiting on exactly this file, so
/// making it wait for the files behind it to drain would deadlock (they drain only after it). It may run
/// over the budget by the one item it is decoding.</item>
/// </list>
/// <para><b>Underestimates.</b> A reservation is an estimate made before the decode, and an item that comes
/// out larger is charged in full without waiting (it already exists). Each file reserves the largest size it
/// has seen (<see cref="ScanReservation.Observe"/>), so this happens only on a file's first item — reserved
/// from the Parquet footer, which UNDERestimates dictionary-encoded strings — or on an item larger than any
/// before it. The peak is therefore about the budget, plus one item, plus each in-flight file's shortfall on
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

    internal async ValueTask ReserveAsync(int file, long bytes, CancellationToken cancellationToken)
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
                    || (file == _head && QueuedLocked(file) == 0))
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

    internal void Enqueue(int file)
    {
        lock (_gate)
            _queued[file] = QueuedLocked(file) + 1;
    }

    internal void Dequeue(int file, long charge)
    {
        lock (_gate)
        {
            int left = QueuedLocked(file) - 1;
            if (left <= 0)
                _queued.Remove(file);
            else
                _queued[file] = left;
            _used -= charge;
            SignalLocked();
        }
    }

    internal void SetHead(int file)
    {
        lock (_gate)
        {
            _head = file;
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

    private int QueuedLocked(int file) => _queued.TryGetValue(file, out int n) ? n : 0;

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
