// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading;
using EngineeredWood.Scans;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Reads several files at once and yields their items in FILE order, then item order within a file — the
/// order a serial read would give (#607). A thin layer over <see cref="OrderedScan"/>, whose units here are
/// the file indexes <c>0 .. fileCount-1</c>; see it for the shape, the memory bound, errors and early exit.
/// </summary>
internal static class OrderedFileScan<T>
{
    /// <summary>Items one file may queue ahead of the consumer, on top of the byte budget.</summary>
    internal const int ChannelCapacity = OrderedScan.ChannelCapacity;

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
    internal static IAsyncEnumerable<T> RunAsync(
        int fileCount,
        Func<int, ScanReservation, CancellationToken, IAsyncEnumerable<T>> readFile,
        Func<T, long> sizeOf,
        Action<T> dispose,
        int maxConcurrentFiles,
        ScanMemoryBudget budget,
        CancellationToken cancellationToken = default)
    {
        if (maxConcurrentFiles < 1)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrentFiles), maxConcurrentFiles, "Must be at least 1.");
        return OrderedScan.RunAsync(
            OrderedScan.Range(fileCount), readFile, sizeOf, dispose,
            (int)Math.Min(maxConcurrentFiles, Math.Max(1, fileCount)), budget, cancellationToken);
    }
}
