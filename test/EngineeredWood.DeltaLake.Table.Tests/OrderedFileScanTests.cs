// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The parallel file read's pipeline on its own (#607): order, errors, early exit, cancellation, the memory
/// budget and the concurrency window, against synthetic files whose timing the tests control.
/// </summary>
public class OrderedFileScanTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    /// <summary>An item that knows whether it was disposed. Every item a file produces is recorded, so a test
    /// can check that each one was either handed to the caller or disposed — never neither.</summary>
    private sealed class Item(int file, int index, long size)
    {
        public int File { get; } = file;
        public int Index { get; } = index;
        public long Size { get; } = size;
        public int Disposals;
        public bool Delivered;
    }

    private sealed class Files
    {
        public readonly ConcurrentBag<Item> Produced = [];
        public int Running;
        public int MaxRunning;
        public readonly ConcurrentDictionary<int, bool> Cancelled = new();
        public readonly ConcurrentDictionary<int, bool> Started = new();

        /// <summary>A file of <paramref name="items"/> items, item <c>i</c> of <c>size(file, i)</c> bytes, each
        /// preceded by a reservation (the first estimated at <paramref name="estimate"/>), with
        /// <paramref name="delay"/> before each item. <paramref name="failAt"/> throws instead of producing that
        /// item.</summary>
        public async IAsyncEnumerable<Item> Read(
            int file, ScanReservation reservation, int items, Func<int, int, long> size, long estimate,
            Func<int, int, TimeSpan>? delay, int failAt, [EnumeratorCancellation] CancellationToken ct)
        {
            Started[file] = true;
            int now = Interlocked.Increment(ref Running);
            InterlockedMax(ref MaxRunning, now);
            try
            {
                reservation.SetInitialEstimate(estimate);
                for (int i = 0; i < items; i++)
                {
                    try
                    {
                        // Inside the try: a file the scheduler starts just as the scan shuts down is cancelled
                        // HERE, waiting for budget, before it ever reaches its delay.
                        await reservation.ReserveNextAsync(ct);
                        if (delay?.Invoke(file, i) is { } d && d > TimeSpan.Zero)
                            await Task.Delay(d, ct);
                        else
                            await Task.Yield();
                    }
                    catch (OperationCanceledException)
                    {
                        Cancelled[file] = true;
                        throw;
                    }
                    if (i == failAt)
                        throw new InvalidOperationException($"file {file} failed at item {i}");
                    var item = new Item(file, i, size(file, i));
                    Produced.Add(item);
                    yield return item;
                }
            }
            finally
            {
                Interlocked.Decrement(ref Running);
            }
        }

        public void AssertNothingLeaked()
        {
            foreach (var item in Produced)
            {
                Assert.True(item.Delivered || item.Disposals == 1,
                    $"file {item.File} item {item.Index}: delivered={item.Delivered}, disposals={item.Disposals}");
                Assert.False(item.Delivered && item.Disposals > 0,
                    $"file {item.File} item {item.Index} was delivered AND disposed");
            }
        }

        private static void InterlockedMax(ref int target, int value)
        {
            int seen;
            while (value > (seen = Volatile.Read(ref target)))
                Interlocked.CompareExchange(ref target, value, seen);
        }
    }

    private static IAsyncEnumerable<Item> Scan(
        Files files, int fileCount, int itemsPerFile, int window, long budget, long itemSize = 10,
        Func<int, int, TimeSpan>? delay = null, int failFile = -1, int failAt = -1,
        ScanMemoryBudget? memory = null, CancellationToken ct = default,
        Func<int, int, long>? sizes = null, long? estimate = null) =>
        OrderedFileScan<Item>.RunAsync(
            fileCount,
            (f, r, t) => files.Read(f, r, itemsPerFile, sizes ?? ((_, _) => itemSize), estimate ?? itemSize,
                delay, f == failFile ? failAt : -1, t),
            i => i.Size,
            i => Interlocked.Increment(ref i.Disposals),
            window,
            memory ?? new ScanMemoryBudget(budget),
            ct);

    private static async Task<List<Item>> CollectAsync(IAsyncEnumerable<Item> source, int take = int.MaxValue)
    {
        var got = new List<Item>();
        await foreach (var item in source)
        {
            item.Delivered = true;
            got.Add(item);
            if (got.Count == take)
                break;
        }
        return got;
    }

    [Fact]
    public async Task Items_ComeOutInFileOrder_WhenLaterFilesFinishFirst()
    {
        var files = new Files();
        // The EARLIER the file, the slower: file 0 finishes last, so any order but file order would show.
        var got = await CollectAsync(Scan(files, fileCount: 8, itemsPerFile: 5, window: 4, budget: 1 << 20,
            delay: (f, _) => TimeSpan.FromMilliseconds(2 * (8 - f)))).WaitAsync(Timeout);

        Assert.Equal(40, got.Count);
        for (int k = 0; k < got.Count; k++)
        {
            Assert.Equal(k / 5, got[k].File);
            Assert.Equal(k % 5, got[k].Index);
        }
        files.AssertNothingLeaked();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public async Task NoMoreThanTheWindow_IsReadAtOnce(int window)
    {
        var files = new Files();
        await CollectAsync(Scan(files, fileCount: 12, itemsPerFile: 3, window: window, budget: 1 << 20,
            delay: (_, _) => TimeSpan.FromMilliseconds(3))).WaitAsync(Timeout);

        Assert.InRange(files.MaxRunning, 1, window);
        if (window > 1)
            Assert.True(files.MaxRunning > 1, "the files were never read concurrently");
    }

    [Fact]
    public async Task AFailure_SurfacesAtItsFilesPosition_AfterEverythingBeforeIt()
    {
        var files = new Files();
        var got = new List<Item>();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            // File 3 fails at its third item, and FAST: the files before it are slower, so the failure
            // happens long before the consumer reaches file 3.
            await foreach (var item in Scan(files, fileCount: 8, itemsPerFile: 4, window: 4, budget: 1 << 20,
                               delay: (f, _) => f == 3 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(5),
                               failFile: 3, failAt: 2))
            {
                item.Delivered = true;
                got.Add(item);
            }
        }).WaitAsync(Timeout);

        Assert.Equal("file 3 failed at item 2", ex.Message);
        // Files 0-2 whole, then file 3's first two items: exactly what a serial read yields before throwing.
        Assert.Equal(3 * 4 + 2, got.Count);
        Assert.Equal(3, got[^1].File);
        Assert.Equal(1, got[^1].Index);
        files.AssertNothingLeaked();
        // Every read has stopped: the failure cancelled whatever was still running behind it.
        Assert.Equal(0, files.Running);
    }

    [Fact]
    public async Task FilesAfterAFailure_AreCancelled()
    {
        var files = new Files();
        // File 0 fails immediately; files 1-3 would take far longer than the test's timeout.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await CollectAsync(Scan(files, fileCount: 6, itemsPerFile: 3, window: 4, budget: 1 << 20,
                delay: (f, _) => f == 0 ? TimeSpan.Zero : TimeSpan.FromMinutes(5),
                failFile: 0, failAt: 0))).WaitAsync(Timeout);

        Assert.Equal(0, files.Running);
        foreach (var file in files.Started.Keys.Where(f => f > 0))
            Assert.True(files.Cancelled.ContainsKey(file), $"file {file} was not cancelled");
        // The window was 4, so files 4 and 5 can only have started had the scan kept going after file 0.
        Assert.False(files.Started.ContainsKey(5));
        files.AssertNothingLeaked();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(23)]
    public async Task EarlyExit_DisposesEveryItemTheCallerDidNotGet(int take)
    {
        var files = new Files();
        var got = await CollectAsync(
            Scan(files, fileCount: 10, itemsPerFile: 5, window: 4, budget: 1 << 20), take).WaitAsync(Timeout);

        Assert.Equal(take, got.Count);
        Assert.Equal(0, files.Running);
        Assert.True(files.Produced.Count > take, "nothing was read ahead, so nothing was tested");
        files.AssertNothingLeaked();
    }

    [Fact]
    public async Task Cancellation_StopsTheReads_AndDisposesWhatWasReadAhead()
    {
        var files = new Files();
        using var cts = new CancellationTokenSource();
        var got = new List<Item>();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var item in Scan(files, fileCount: 10, itemsPerFile: 5, window: 4, budget: 1 << 20,
                               delay: (_, _) => TimeSpan.FromMilliseconds(1), ct: cts.Token))
            {
                item.Delivered = true;
                got.Add(item);
                if (got.Count == 3)
                    cts.Cancel();
            }
        }).WaitAsync(Timeout);

        Assert.Equal(3, got.Count);
        Assert.Equal(0, files.Running);
        files.AssertNothingLeaked();
    }

    [Fact]
    public async Task TheBudget_BoundsMemory_ToAboutOneItemOverIt()
    {
        const long size = 100;
        const long budget = 350; // three and a half items
        var files = new Files();
        var memory = new ScanMemoryBudget(budget);
        var got = new List<Item>();
        await foreach (var item in Scan(files, fileCount: 8, itemsPerFile: 6, window: 4, budget: 0,
                           itemSize: size, memory: memory).WithCancellation(default))
        {
            item.Delivered = true;
            got.Add(item);
            await Task.Delay(1); // a slow consumer: the reads run ahead until the budget stops them
        }

        Assert.Equal(48, got.Count);
        Assert.InRange(memory.Peak, size, budget + size);
        Assert.Equal(0, memory.Used);
        files.AssertNothingLeaked();
    }

    [Fact]
    public async Task ABudgetSmallerThanOneItem_StillCompletes()
    {
        // Every reservation is over budget: only the "budget empty" and "front file" admissions let anything
        // through, so this deadlocks if either is wrong.
        var files = new Files();
        var memory = new ScanMemoryBudget(1);
        var got = await CollectAsync(Scan(files, fileCount: 6, itemsPerFile: 4, window: 4, budget: 0,
            itemSize: 1000, memory: memory)).WaitAsync(Timeout);

        Assert.Equal(24, got.Count);
        for (int k = 0; k < got.Count; k++)
            Assert.Equal(k / 4, got[k].File);
        Assert.Equal(0, memory.Used);
    }

    [Fact]
    public async Task AnUnderestimatedFirstItem_OverrunsTheBudgetOnlyOncePerFile()
    {
        // Every file's first reservation is 10 bytes for items of 1,000 (a footer that underestimates
        // dictionary-encoded strings). The first item of each file in flight is charged in full without
        // waiting; after that every reservation is the 1,000 bytes observed, and waits.
        const long size = 1000;
        const long budget = 2500;
        const int window = 4;
        var files = new Files();
        var memory = new ScanMemoryBudget(budget);
        int count = 0;
        await foreach (var item in Scan(files, fileCount: 12, itemsPerFile: 5, window: window, budget: 0,
                           itemSize: size, estimate: 10, memory: memory))
        {
            item.Delivered = true;
            count++;
            await Task.Delay(1);
        }

        Assert.Equal(60, count);
        // The budget, one item over it (the front file), and each other file in flight's first-item shortfall.
        Assert.InRange(memory.Peak, size, budget + size + (window - 1) * (size - 10));
        Assert.Equal(0, memory.Used);
        files.AssertNothingLeaked();
    }

    [Fact]
    public async Task ASmallItem_DoesNotLowerTheNextReservation()
    {
        // Items alternate 1,000 and 100 bytes (a deletion vector's survivors, then a full batch). Reserving the
        // LAST size seen would underestimate every large item; reserving the largest underestimates none.
        const long budget = 2500;
        var files = new Files();
        var memory = new ScanMemoryBudget(budget);
        var got = await CollectAsync(Scan(files, fileCount: 8, itemsPerFile: 8, window: 4, budget: 0,
            sizes: (_, i) => i % 2 == 0 ? 1000 : 100, estimate: 1000, memory: memory)).WaitAsync(Timeout);

        Assert.Equal(64, got.Count);
        // No reservation is smaller than its item, so nothing is charged without waiting: the budget plus the
        // front file's one admitted item at most.
        Assert.InRange(memory.Peak, 1000, budget + 1000);
    }

    [Fact]
    public async Task AHugeWindow_DoesNotOverflow()
    {
        // 2 x int.MaxValue overflowed the lookahead to a negative number, so no file ever started.
        var files = new Files();
        var got = await CollectAsync(Scan(files, fileCount: 3, itemsPerFile: 2, window: int.MaxValue,
            budget: 1 << 20)).WaitAsync(Timeout);
        Assert.Equal(6, got.Count);
    }

    [Fact]
    public async Task NoFiles_YieldsNothing()
    {
        var files = new Files();
        Assert.Empty(await CollectAsync(Scan(files, fileCount: 0, itemsPerFile: 1, window: 4, budget: 1)));
    }
}

#if NETFRAMEWORK
/// <summary><c>Task.WaitAsync(TimeSpan)</c> for .NET Framework, which lacks it: a hang fails the test instead
/// of stalling the run.</summary>
internal static class TaskTimeoutPolyfill
{
    public static async Task WaitAsync(this Task task, TimeSpan timeout)
    {
        if (await Task.WhenAny(task, Task.Delay(timeout)) != task)
            throw new TimeoutException($"Did not complete within {timeout}.");
        await task;
    }

    public static async Task<T> WaitAsync<T>(this Task<T> task, TimeSpan timeout)
    {
        await ((Task)task).WaitAsync(timeout);
        return await task;
    }
}
#endif
