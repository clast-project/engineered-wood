// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Runtime.InteropServices;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// A local table that reads like an object store (#830): every file it opens reports
/// <see cref="IReadConcurrencyHint.PreferredConcurrentReads"/>, as the S3, Azure and GCS readers do, and every read
/// request waits <see cref="Latency"/> plus its bytes at <see cref="BytesPerSecond"/> before the local read. An
/// <see cref="IRandomAccessFile.ReadRangesAsync"/> goes through <see cref="CoalescingFileReader"/> with its default
/// options, as the S3 reader's does: nearby ranges are merged, and the merged requests wait concurrently, as parallel
/// ranged GETs would. Writes and listing go straight through.
/// </summary>
internal sealed class SimulatedObjectStore(
    ITableFileSystem inner, TimeSpan latency, long bytesPerSecond, int preferredConcurrentReads = 4) : ITableFileSystem
{
    private readonly int _preferred = preferredConcurrentReads;

    public TimeSpan Latency { get; } = latency;

    public long BytesPerSecond { get; } = bytesPerSecond;

    /// <summary>Read requests made, ranges counted one by one.</summary>
    public long Requests => Interlocked.Read(ref _requests);

    private long _requests;

    public PathNameConstraints PathConstraints => inner.PathConstraints;

    public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) =>
        inner.ListAsync(prefix, cancellationToken);

    public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) =>
        new SlowFile(this, await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false));

    public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
        inner.CreateAsync(path, overwrite, cancellationToken);

    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
        inner.DeleteAsync(path, cancellationToken);

    public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
        inner.ExistsAsync(path, cancellationToken);

    public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) =>
        inner.ReadAllBytesAsync(path, cancellationToken);

    public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        inner.TryWriteAllBytesAsync(path, data, cancellationToken);

    public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        inner.WriteAllBytesAsync(path, data, cancellationToken);

    private Task WaitAsync(long bytes, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _requests);
        var wait = Latency + TimeSpan.FromTicks(bytes * TimeSpan.TicksPerSecond / BytesPerSecond);
        return wait > TimeSpan.Zero ? Task.Delay(wait, cancellationToken) : Task.CompletedTask;
    }

    private sealed class SlowFile(SimulatedObjectStore store, IRandomAccessFile file) : IRandomAccessFile, IReadConcurrencyHint
    {
        public int? PreferredConcurrentReads => store._preferred;

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) => file.GetLengthAsync(cancellationToken);

        public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
        {
            await store.WaitAsync(range.Length, cancellationToken).ConfigureAwait(false);
            return await file.ReadAsync(range, cancellationToken).ConfigureAwait(false);
        }

        // As S3RandomAccessFile does: nearby ranges merged into fewer requests, each a ReadAsync (so each waits),
        // the merged requests in parallel (#840 review).
        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default) =>
            new CoalescingFileReader(this).ReadRangesAsync(ranges, cancellationToken);

        public ValueTask DisposeAsync() => file.DisposeAsync();

        public void Dispose() => file.Dispose();
    }

    /// <summary>
    /// Asks Windows for 1 ms timer resolution, so that a few milliseconds of latency is not rounded up to the default
    /// 15.6 ms tick; a no-op elsewhere. Pair with <see cref="EndHighResolutionTimers"/>.
    /// </summary>
    internal static void BeginHighResolutionTimers()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            _ = timeBeginPeriod(1);
    }

    internal static void EndHighResolutionTimers()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            _ = timeEndPeriod(1);
    }

    [DllImport("winmm.dll")]
    private static extern uint timeBeginPeriod(uint period);

    [DllImport("winmm.dll")]
    private static extern uint timeEndPeriod(uint period);
}
