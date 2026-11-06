// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.Core.Tests.IO;

/// <summary>
/// <see cref="BackgroundUploads"/> and the <see cref="UploadBufferPool"/> its writers share (#713): a full buffer
/// uploads in the background only while the pool has a spare to keep writing into, never waiting for one, and the
/// pool bounds what every writer together holds beyond its own buffer.
/// </summary>
public class BackgroundUploadsTests
{
    private const int Size = 16;

    /// <summary>Uploads that each wait for their own release, recording the bytes they were given.</summary>
    private sealed class GatedUploads
    {
        private readonly object _gate = new();
        private readonly Dictionary<int, TaskCompletionSource<bool>> _releases = new();
        public List<(int Id, byte[] Bytes)> Done { get; } = new();

        public TaskCompletionSource<bool> Release(int id)
        {
            lock (_gate)
            {
                if (!_releases.TryGetValue(id, out var tcs))
                    _releases[id] = tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                return tcs;
            }
        }

        public Func<byte[], int, CancellationToken, Task> For(int id) => async (buffer, length, ct) =>
        {
            // The bytes as the upload starts: a writer filling this buffer meanwhile would change them later.
            byte[] atStart = buffer.Take(length).ToArray();
            var release = Release(id).Task;
            var cancelled = Task.Delay(Timeout.Infinite, ct);
            if (await Task.WhenAny(release, cancelled) == cancelled)
                ct.ThrowIfCancellationRequested();
            await release; // propagates a failure set on it
            Assert.Equal(atStart, buffer.Take(length).ToArray());
            lock (_gate)
                Done.Add((id, atStart));
        };
    }

    private static byte[] Filled(byte value) => Enumerable.Repeat(value, Size).ToArray();

    [Fact]
    public async Task WithoutAPool_EachUploadIsAwaited_AndTheBufferComesBack()
    {
        var uploads = new BackgroundUploads(pool: null);
        var gated = new GatedUploads();
        var buffer = Filled(1);

        var pending = uploads.UploadAsync(buffer, Size, gated.For(0), default).AsTask();
        Assert.False(pending.IsCompleted);
        gated.Release(0).SetResult(true);

        Assert.Same(buffer, await pending);
        Assert.Equal(0, uploads.InFlight);
    }

    /// <summary>
    /// With two spares, two full buffers upload in the background, each handing back a spare to fill; the third
    /// finds none and uploads in turn, handing back its own buffer. Once the uploads end every spare is back.
    /// </summary>
    [Fact]
    public async Task WithSpares_UploadsInTheBackground_UpToTheSpares_ThenInTurn()
    {
        var pool = new UploadBufferPool(maxBuffers: 2);
        var uploads = new BackgroundUploads(pool);
        var gated = new GatedUploads();

        var a = Filled(1);
        var next = await uploads.UploadAsync(a, Size, gated.For(0), default);
        Assert.NotSame(a, next);
        Fill(next, 2); // filling the spare must not touch the upload of a

        var b = next;
        next = await uploads.UploadAsync(b, Size, gated.For(1), default);
        Assert.NotSame(b, next);
        Assert.Equal(2, pool.InUse);
        Assert.Equal(2, uploads.InFlight);

        Fill(next, 3);
        var c = next;
        var third = uploads.UploadAsync(c, Size, gated.For(2), default).AsTask();
        Assert.False(third.IsCompleted); // no spare: it waits for its own upload
        gated.Release(2).SetResult(true);
        Assert.Same(c, await third);

        gated.Release(0).SetResult(true);
        gated.Release(1).SetResult(true);
        await uploads.DrainAsync(default);

        Assert.Equal(0, pool.InUse);
        Assert.Equal(
            new[] { (0, Filled(1)), (1, Filled(2)), (2, Filled(3)) },
            gated.Done.OrderBy(d => d.Id).Select(d => (d.Id, d.Bytes)));
    }

    /// <summary>Two writers share one spare: the first takes it, the second uploads in turn.</summary>
    [Fact]
    public async Task ThePool_BoundsEveryWriterTogether()
    {
        var pool = new UploadBufferPool(maxBuffers: 1);
        var first = new BackgroundUploads(pool);
        var second = new BackgroundUploads(pool);
        var gated = new GatedUploads();

        var a = Filled(1);
        Assert.NotSame(a, await first.UploadAsync(a, Size, gated.For(0), default));

        var b = Filled(2);
        var inTurn = second.UploadAsync(b, Size, gated.For(1), default).AsTask();
        Assert.False(inTurn.IsCompleted);
        gated.Release(1).SetResult(true);
        Assert.Same(b, await inTurn);

        // The spare is back once the first writer's upload ends, for either writer.
        gated.Release(0).SetResult(true);
        await first.DrainAsync(default);
        Assert.Equal(0, pool.InUse);
        var c = Filled(3);
        Assert.NotSame(c, await second.UploadAsync(c, Size, gated.For(2), default));
        gated.Release(2).SetResult(true);
        await second.DrainAsync(default);
    }

    /// <summary>
    /// A background upload that fails is reported by the writer's next upload and by the drain, and the others
    /// still in flight are cancelled: nothing will commit them.
    /// </summary>
    [Fact]
    public async Task AFailedBackgroundUpload_IsReported_AndCancelsTheOthers()
    {
        var pool = new UploadBufferPool(maxBuffers: 4);
        var uploads = new BackgroundUploads(pool);
        var gated = new GatedUploads();

        var next = await uploads.UploadAsync(Filled(1), Size, gated.For(0), default);
        next = await uploads.UploadAsync(next, Size, gated.For(1), default);
        gated.Release(0).SetException(new IOException("block 0 failed"));

        // Upload 1 is cancelled by the failure, not left to finish.
        await WaitUntilAsync(() => uploads.InFlight == 0);
        Assert.DoesNotContain(gated.Done, d => d.Id == 1);

        var failure = await Assert.ThrowsAsync<IOException>(() => uploads.UploadAsync(next, Size, gated.For(2), default).AsTask());
        Assert.Equal("block 0 failed", failure.Message);
        failure = await Assert.ThrowsAsync<IOException>(() => uploads.DrainAsync(default).AsTask());
        Assert.Equal("block 0 failed", failure.Message);
        Assert.Equal(0, pool.InUse);
    }

    /// <summary>
    /// An upload made in turn, for want of a spare, is stopped by a background upload failing meanwhile, and
    /// reports that failure rather than its own cancellation: the write cannot succeed now.
    /// </summary>
    [Fact]
    public async Task AnUploadInTurn_IsStoppedByABackgroundFailure_AndReportsIt()
    {
        var pool = new UploadBufferPool(maxBuffers: 1);
        var uploads = new BackgroundUploads(pool);
        var gated = new GatedUploads();

        var next = await uploads.UploadAsync(Filled(1), Size, gated.For(0), default);
        var inTurn = uploads.UploadAsync(next, Size, gated.For(1), default).AsTask();
        Assert.False(inTurn.IsCompleted);

        gated.Release(0).SetException(new IOException("block 0 failed"));

        var failure = await Assert.ThrowsAsync<IOException>(() => inTurn);
        Assert.Equal("block 0 failed", failure.Message);
        Assert.Empty(gated.Done);
        Assert.Equal(0, pool.InUse);
    }

    /// <summary>
    /// A token already cancelled refuses the buffer even with a spare free: it is neither uploaded nor handed
    /// to the background, and no spare is taken.
    /// </summary>
    [Fact]
    public async Task ACancelledToken_RefusesTheBuffer_EvenWithASpare()
    {
        var pool = new UploadBufferPool(maxBuffers: 1);
        var uploads = new BackgroundUploads(pool);
        int calls = 0;
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => uploads.UploadAsync(
            Filled(1), Size, (_, _, _) => { Interlocked.Increment(ref calls); return Task.CompletedTask; }, cts.Token)
            .AsTask());

        Assert.Equal(0, Volatile.Read(ref calls));
        Assert.Equal(0, pool.InUse);
        Assert.Equal(0, uploads.InFlight);
    }

    [Fact]
    public async Task Abort_CancelsTheUploadsInFlight_AndTheirBuffersGoBack()
    {
        var pool = new UploadBufferPool(maxBuffers: 2);
        var uploads = new BackgroundUploads(pool);
        var gated = new GatedUploads();

        var next = await uploads.UploadAsync(Filled(1), Size, gated.For(0), default);
        await uploads.UploadAsync(next, Size, gated.For(1), default);
        Assert.Equal(2, pool.InUse);

        uploads.Abort();

        await WaitUntilAsync(() => pool.InUse == 0);
        Assert.Empty(gated.Done);
    }

    [Fact]
    public async Task CancellingTheDrain_CancelsTheUploads()
    {
        var pool = new UploadBufferPool(maxBuffers: 1);
        var uploads = new BackgroundUploads(pool);
        var gated = new GatedUploads();
        await uploads.UploadAsync(Filled(1), Size, gated.For(0), default);

        using var cts = new CancellationTokenSource();
        var drain = uploads.DrainAsync(cts.Token).AsTask();
        Assert.False(drain.IsCompleted);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => drain);
        Assert.Equal(0, pool.InUse);
    }

    [Fact]
    public void ThePool_LendsAtMostItsBuffers_AndReusesWhatComesBack()
    {
        var pool = new UploadBufferPool(maxBuffers: 2);
        Assert.True(pool.TryRent(Size, out var one));
        Assert.True(pool.TryRent(Size, out var two));
        Assert.False(pool.TryRent(Size, out var none));
        Assert.Null(none);

        pool.Return(one!);
        Assert.True(pool.TryRent(Size, out var again));
        Assert.Same(one, again);

        // A returned buffer of another size is not handed out for this one.
        pool.Return(again!);
        Assert.True(pool.TryRent(Size * 2, out var larger));
        Assert.Equal(Size * 2, larger!.Length);
        pool.Return(larger);
        pool.Return(two!);
        Assert.Equal(0, pool.InUse);
    }

    private static void Fill(byte[] buffer, byte value)
    {
        for (int i = 0; i < buffer.Length; i++)
            buffer[i] = value;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "timed out");
            await Task.Delay(5);
        }
    }
}
