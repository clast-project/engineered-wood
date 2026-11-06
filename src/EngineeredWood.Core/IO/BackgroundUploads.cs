// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;

namespace EngineeredWood.IO;

/// <summary>
/// The spare upload buffers the writers of one file system share (#713). Each writer owns one buffer, and can
/// always write with it alone, uploading each full buffer and waiting for that upload before filling it again. A
/// spare lets a writer hand its full buffer to a background upload and keep writing into the spare instead, so
/// that uploads overlap each other and the encoding that produces the bytes.
/// <para>Spares are borrowed only when one is free, never waited for: an empty pool slows writers to one upload
/// at a time each, and cannot deadlock them. However many writers the file system has open, the memory beyond
/// their own buffers is at most <see cref="MaxBuffers"/> buffers (#719).</para>
/// </summary>
internal sealed class UploadBufferPool
{
    private readonly ConcurrentBag<byte[]> _free = new();
    private int _available;

    public UploadBufferPool(int maxBuffers)
    {
        if (maxBuffers < 1)
            throw new ArgumentOutOfRangeException(nameof(maxBuffers), maxBuffers, "Must be at least 1.");
        MaxBuffers = maxBuffers;
        _available = maxBuffers;
    }

    /// <summary>The most buffers lent out at once.</summary>
    public int MaxBuffers { get; }

    /// <summary>The buffers lent out now.</summary>
    public int InUse => MaxBuffers - Volatile.Read(ref _available);

    /// <summary>
    /// A buffer of <paramref name="size"/> bytes when one may be lent, reusing one returned earlier; false,
    /// at once, when <see cref="MaxBuffers"/> are out.
    /// </summary>
    public bool TryRent(int size, out byte[]? buffer)
    {
        while (true)
        {
            int available = Volatile.Read(ref _available);
            if (available == 0)
            {
                buffer = null;
                return false;
            }
            if (Interlocked.CompareExchange(ref _available, available - 1, available) == available)
                break;
        }

        // Writers of one file system share a buffer size; a returned buffer of another size is dropped.
        while (_free.TryTake(out var free))
        {
            if (free.Length == size)
            {
                buffer = free;
                return true;
            }
        }
        buffer = new byte[size];
        return true;
    }

    /// <summary>Takes back a lent buffer: not necessarily the same array, since writers trade theirs.</summary>
    public void Return(byte[] buffer)
    {
        _free.Add(buffer);
        Interlocked.Increment(ref _available);
    }
}

/// <summary>
/// One writer's uploads of its full buffers: in the background when <see cref="UploadBufferPool"/> lends a spare,
/// otherwise in turn. The upload callback names the block or part itself, so uploads may finish in any order.
/// <para>A background upload that fails is reported by the writer's next <see cref="UploadAsync"/> or by
/// <see cref="DrainAsync"/>, and cancels the others. <see cref="Abort"/> cancels whatever is in flight.</para>
/// <para>Not thread-safe, like the writer it serves: one caller at a time.</para>
/// </summary>
internal sealed class BackgroundUploads
{
    private readonly UploadBufferPool? _pool;
    private readonly List<Task> _inFlight = new();
    private readonly CancellationTokenSource _abort = new();
    private readonly object _gate = new();
    private ExceptionDispatchInfo? _failure;

    public BackgroundUploads(UploadBufferPool? pool) => _pool = pool;

    /// <summary>
    /// Uploads <paramref name="buffer"/>'s first <paramref name="length"/> bytes with <paramref name="upload"/>
    /// and returns the buffer to fill next: a spare, while <paramref name="buffer"/> uploads in the background
    /// (it goes to the pool when its upload ends); or <paramref name="buffer"/> itself, once its upload is done.
    /// <paramref name="cancellationToken"/> already cancelled refuses the buffer. Once handed over, a background
    /// upload runs under its own cancellation (<see cref="Abort"/>); an upload made in turn runs under both, so a
    /// background failure stops it too, and is what it reports.
    /// <para><paramref name="last"/>: nothing is written after this one, so it is made in turn (still alongside
    /// the uploads in flight) rather than taking a spare that would never be filled.</para>
    /// </summary>
    public async ValueTask<byte[]> UploadAsync(
        byte[] buffer, int length, Func<byte[], int, CancellationToken, Task> upload,
        CancellationToken cancellationToken, bool last = false)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfFailed();
        _inFlight.RemoveAll(t => t.IsCompleted);

        if (!last && _pool is not null && _pool.TryRent(buffer.Length, out var spare))
        {
            _inFlight.Add(Task.Run(() => RunAsync(buffer, length, upload)));
            return spare!;
        }

        using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _abort.Token))
        {
            try
            {
                await upload(buffer, length, linked.Token).ConfigureAwait(false);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                // Cancelled by a background failure, or failing alongside one: the background failure came first.
                ThrowIfFailed();
                throw;
            }
        }
        ThrowIfFailed();
        return buffer;
    }

    /// <summary>The uploads in flight now.</summary>
    public int InFlight => _inFlight.Count(t => !t.IsCompleted);

    /// <summary>
    /// Waits for every background upload, then reports the first that failed. Cancelling
    /// <paramref name="cancellationToken"/> cancels them.
    /// </summary>
    public async ValueTask DrainAsync(CancellationToken cancellationToken)
    {
        using (cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), _abort))
        {
            // RunAsync never faults: it records the failure instead.
            await Task.WhenAll(_inFlight).ConfigureAwait(false);
        }
        _inFlight.Clear();
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfFailed();
    }

    /// <summary>
    /// Cancels the background uploads without waiting for them: the write is abandoned. Each returns its buffer
    /// to the pool when it ends.
    /// </summary>
    public void Abort() => _abort.Cancel();

    /// <summary>
    /// <see cref="Abort"/>, then waits for the uploads to end, reporting nothing: for a store where a part still
    /// uploading can outlive the abort of its upload (S3), so the abort is sent after the last of them.
    /// </summary>
    public async Task AbortAndWaitAsync()
    {
        Abort();
        // RunAsync never faults: it records the failure instead.
        await Task.WhenAll(_inFlight).ConfigureAwait(false);
        _inFlight.Clear();
    }

    private async Task RunAsync(byte[] buffer, int length, Func<byte[], int, CancellationToken, Task> upload)
    {
        try
        {
            await upload(buffer, length, _abort.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            lock (_gate)
                _failure ??= ExceptionDispatchInfo.Capture(ex);
            // The write cannot complete now: stop the others rather than finish uploads nothing will commit.
            try { _abort.Cancel(); } catch (ObjectDisposedException) { }
        }
        finally
        {
            _pool!.Return(buffer);
        }
    }

    private void ThrowIfFailed()
    {
        ExceptionDispatchInfo? failure;
        lock (_gate)
            failure = _failure;
        failure?.Throw();
    }
}
