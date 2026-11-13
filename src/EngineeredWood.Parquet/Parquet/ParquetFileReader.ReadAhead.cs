// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.ExceptionServices;
using EngineeredWood.Scans;

namespace EngineeredWood.Parquet;

public sealed partial class ParquetFileReader
{
    /// <summary>
    /// How many row groups a whole-file read (<see cref="ReadFileReservingAsync"/>) has fetched or fetching at once:
    /// the one it decodes and those after it.
    /// </summary>
    internal const int WholeFileReadAhead = 4;

    /// <summary>
    /// Reads the whole file, its row groups one after another, as ONE unit of a parallel read (a change feed's or a
    /// compaction's file), reserving scan memory before each batch is decoded. Batches are as
    /// <see cref="ReadWithPositionsAsync(EngineeredWood.Expressions.Predicate?, IReadOnlyList{string}?, RowSkipPredicate?, CancellationToken)"/>
    /// returns them, and each reservation is the largest raw batch seen, from the caller's first estimate
    /// (<see cref="ScanReservation.SetInitialEstimate"/>).
    /// <para>Row groups are fetched AHEAD of their decode (#846), as a row group of a scan is (#829): a row group's
    /// compressed bytes are held, and its column chunks fetched, before the reservation for its first batch, and up
    /// to <paramref name="readAhead"/> row groups are held or fetched at once. Fetching each row group only once
    /// the batch before it was handed on ran a file at fetch-plus-decode per row group: on an object store, a
    /// change feed spent most of its time waiting on one row group's fetch after another while its decodes, a
    /// sixth of that, waited on them. A whole file is one unit, so fetching only its NEXT row group ahead, as a
    /// scan's unit does, leaves those waits in place.</para>
    /// <list type="bullet">
    /// <item>The row group the read decodes next is held as a scan's row group is
    /// (<see cref="ScanReservation.HoldAsync"/>): the read waits for room for it.</item>
    /// <item>Those after it are held only as <see cref="ScanReservation.TryHoldAhead"/> allows, retried before each
    /// batch: by the front unit, over the budget; by another unit, only from room nobody nearer the front waits for.
    /// A row group not held yet is fetched when it is.</item>
    /// <item>A row group read only in part (a deletion vector's runs cut out) fetches the pages it needs as it
    /// decodes them, and so does one with a chunk too large to fetch in one buffer (<see cref="TryPrefetch"/>). It
    /// still holds its compressed bytes.</item>
    /// </list>
    /// <para>An error planning a row group surfaces where the serial read raises it, after every batch before that row
    /// group (#850 review): planning it ahead only records the error, and stops planning; a read that stops before
    /// that row group never sees it. Preparing one ahead that fails leaves it unfetched, and its own read raises the
    /// failure when it gets there. Cancellation is never deferred.</para>
    /// </summary>
    internal async IAsyncEnumerable<PositionedRecordBatch> ReadFileReservingAsync(
        IReadOnlyList<string>? columnNames,
        RowSkipPredicate? skipRows,
        ScanReservation reservation,
        int readAhead,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        readAhead = Math.Max(1, readAhead);

        // In file order: the head is decoding, the rest are fetching. `next` is planned but not held yet.
        var held = new Queue<(RowGroupPlan Plan, long Compressed, PrefetchedFile? Io)>();
        (RowGroupPlan Plan, long Compressed)? next = null;
        bool planned = false;
        ExceptionDispatchInfo? planningError = null;
        var plans = PlanRowGroupsAsync(metadata, _options.Filter, columnNames, skipRows, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        IAsyncEnumerator<PositionedRecordBatch>? e = null;
        try
        {
            while (await TopUpAsync().ConfigureAwait(false))
            {
                var head = held.Peek();
                e = ReadPlannedAsync(head.Plan, columnNames, cancellationToken, answers: null, io: head.Io)
                    .GetAsyncEnumerator(cancellationToken);
                while (true)
                {
                    await TopUpAsync().ConfigureAwait(false);
                    await reservation.ReserveNextAsync(cancellationToken).ConfigureAwait(false);
                    if (!await e.MoveNextAsync().ConfigureAwait(false))
                        break;
                    // The raw batch is what the next decode will produce (see ReadPlannedAsync).
                    reservation.Observe(EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(e.Current.Batch));
                    yield return e.Current;
                }
                await e.DisposeAsync().ConfigureAwait(false);
                e = null;
                held.Dequeue();
                if (head.Io is not null)
                    await head.Io.DisposeAsync().ConfigureAwait(false);
                reservation.ReleaseHeld(head.Compressed);
            }
            planningError?.Throw();
        }
        finally
        {
            if (e is not null)
                await e.DisposeAsync().ConfigureAwait(false);
            while (held.Count > 0)
            {
                if (held.Dequeue().Io is { } io)
                    await io.DisposeAsync().ConfigureAwait(false);
            }
            reservation.ReleaseHeld();
            await plans.DisposeAsync().ConfigureAwait(false);
        }

        // Holds and starts fetching the row groups after those held, up to `readAhead` in all: waiting for room
        // for the first, which the read decodes next; the others only while they are let in without waiting. False
        // once nothing is left to read.
        async ValueTask<bool> TopUpAsync()
        {
            while (held.Count < readAhead)
            {
                if (next is null)
                {
                    if (planned)
                        break;
                    try
                    {
                        if (!await plans.MoveNextAsync().ConfigureAwait(false))
                        {
                            planned = true;
                            break;
                        }
                        var plan = plans.Current;
                        next = (plan, PlannedBytes(metadata.RowGroups[plan.Index], schema, columnNames, plan.Ranges).Compressed);
                    }
                    catch (Exception error) when (held.Count > 0 && !cancellationToken.IsCancellationRequested)
                    {
                        // Planning ahead: the row groups held come first, so the error waits for them.
                        planningError = ExceptionDispatchInfo.Capture(error);
                        planned = true;
                        break;
                    }
                }

                var (toHold, compressed) = next.Value;
                if (held.Count == 0)
                    await reservation.HoldAsync(compressed, cancellationToken).ConfigureAwait(false);
                else if (!reservation.TryHoldAhead(compressed))
                    break;
                next = null;

                PrefetchedFile? io = null;
                try
                {
                    if (CoversWholeGroup(toHold, metadata.RowGroups[toHold.Index]))
                    {
                        var ctx = await PrepareRowGroupAsync(toHold.Index, columnNames, cancellationToken)
                            .ConfigureAwait(false);
                        io = TryPrefetch(ctx.Ranges, cancellationToken);
                    }
                }
                catch (Exception) when (!cancellationToken.IsCancellationRequested)
                {
                    // Read unfetched: its own read prepares it again, and raises the failure at its row group.
                }
                finally
                {
                    // Queued even when preparing it failed, so that its hold is released with the others.
                    held.Enqueue((toHold, compressed, io));
                }
            }
            return held.Count > 0;
        }
    }
}
