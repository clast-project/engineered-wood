// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Small, independent requests to a table's file system, run several at once. Against an object store each one
/// costs a round trip, so doing them one after another costs one round trip EACH: a 100-commit history read
/// commit by commit, or a vacuum deleting 100 files one at a time, measured about one round trip per item
/// against Azure Blob. (Scans have their own, memory-bounded pipeline: <c>OrderedScan</c>.)
/// </summary>
internal static class BoundedParallel
{
    /// <summary>
    /// Runs <paramref name="body"/> for every item, at most <paramref name="maxConcurrent"/> at once. The first
    /// failure stops the items not yet started, waits for those in flight, and is rethrown (a cancellation of
    /// <paramref name="cancellationToken"/> as itself). Items start in order; with <paramref name="maxConcurrent"/>
    /// 1 this is a plain serial loop.
    /// </summary>
    public static async ValueTask ForEachAsync<T>(
        IReadOnlyList<T> items,
        int maxConcurrent,
        Func<T, CancellationToken, ValueTask> body,
        CancellationToken cancellationToken)
    {
        if (maxConcurrent < 1)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrent), maxConcurrent, "Must be at least 1.");

        if (maxConcurrent == 1 || items.Count <= 1)
        {
            foreach (var item in items)
                await body(item, cancellationToken).ConfigureAwait(false);
            return;
        }

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        int next = -1;
        Exception? first = null;

        async Task WorkerAsync()
        {
            while (!stop.IsCancellationRequested)
            {
                int i = Interlocked.Increment(ref next);
                if (i >= items.Count)
                    return;
                try
                {
                    await body(items[i], stop.Token).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    // Only the first failure is reported; the others are the cancellation it caused, or
                    // failures of requests already in flight when it happened.
                    Interlocked.CompareExchange(ref first, e, null);
                    stop.Cancel();
                    return;
                }
            }
        }

        var workers = new Task[Math.Min(maxConcurrent, items.Count)];
        for (int w = 0; w < workers.Length; w++)
            workers[w] = WorkerAsync();
        await Task.WhenAll(workers).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        if (first is not null)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(first).Throw();
    }

    /// <summary>
    /// <paramref name="read"/> applied to every item of <paramref name="source"/>, yielded in SOURCE order, with up
    /// to <paramref name="lookahead"/> reads in flight ahead of the consumer. A read's failure surfaces where its
    /// item would have been yielded, after every item before it, as a serial loop would have thrown. However the
    /// enumeration ends (completion, <c>break</c>, an exception, the token), the reads still in flight are
    /// cancelled and awaited before it returns, and their results and failures discarded.
    /// </summary>
    public static async IAsyncEnumerable<TResult> SelectInOrderAsync<TSource, TResult>(
        IAsyncEnumerable<TSource> source,
        int lookahead,
        Func<TSource, CancellationToken, ValueTask<TResult>> read,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (lookahead < 1)
            throw new ArgumentOutOfRangeException(nameof(lookahead), lookahead, "Must be at least 1.");

        if (lookahead == 1)
        {
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
                yield return await read(item, cancellationToken).ConfigureAwait(false);
            yield break;
        }

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pending = new Queue<Task<TResult>>();
        var e = source.GetAsyncEnumerator(stop.Token);
        try
        {
            bool more = true;
            while (true)
            {
                while (more && pending.Count < lookahead)
                {
                    more = await e.MoveNextAsync().ConfigureAwait(false);
                    if (more)
                        pending.Enqueue(read(e.Current, stop.Token).AsTask());
                }
                if (pending.Count == 0)
                    break;
                yield return await pending.Dequeue().ConfigureAwait(false);
            }
        }
        finally
        {
            stop.Cancel();
            while (pending.Count > 0)
            {
                try
                {
                    await pending.Dequeue().ConfigureAwait(false);
                }
                catch
                {
                    // Abandoned: the consumer is no longer reading, so nothing waits for this result or error.
                }
            }
            await e.DisposeAsync().ConfigureAwait(false);
        }
    }
}
