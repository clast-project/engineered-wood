// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// <see cref="PrefetchedFile"/>, the file a two-pass read's second pass reads through: reads inside the ranges it
/// fetched ahead are served from them, others go to the file; a failed fetch is raised by a read through it but
/// not by disposal; disposal cancels a fetch still running.
/// </summary>
public class PrefetchedFileTests
{
    [Fact]
    public async Task ReadsInsideTheFetchedRangesAreServedFromThemAndOthersGoToTheFile()
    {
        var inner = new FakeFile(1_000);
        await using (var file = new PrefetchedFile(inner, [new FileRange(100, 200)], CancellationToken.None))
        {
            var served = await file.ReadRangesAsync([new FileRange(150, 50), new FileRange(600, 10)]);
            Assert.Equal(Enumerable.Range(150, 50).Select(i => (byte)i), served[0].Memory.ToArray());
            Assert.Equal(Enumerable.Range(600, 10).Select(i => (byte)i), served[1].Memory.ToArray());
            using var single = await file.ReadAsync(new FileRange(120, 5));
            Assert.Equal(Enumerable.Range(120, 5).Select(i => (byte)i), single.Memory.ToArray());
        }

        // The prefetch, then the one range it did not hold; the single read was served.
        Assert.Equal([[new FileRange(100, 200)], [new FileRange(600, 10)]], inner.Requests);
    }

    [Fact]
    public async Task AFailedFetchIsRaisedByAReadThroughTheFile()
    {
        var file = new PrefetchedFile(new FakeFile(1_000) { Fail = true }, [new FileRange(0, 100)], CancellationToken.None);
        await Assert.ThrowsAsync<IOException>(() => file.ReadRangesAsync([new FileRange(0, 10)]).AsTask());
        await file.DisposeAsync();
    }

    /// <summary>
    /// A fetch nobody read through is not raised by disposal: no result needed its bytes (the first pass selected
    /// no row), as a two-pass read does not fail on a corrupt page it never decodes.
    /// </summary>
    [Fact]
    public async Task AFailedFetchNobodyReadIsNotRaised()
    {
        var file = new PrefetchedFile(new FakeFile(1_000) { Fail = true }, [new FileRange(0, 100)], CancellationToken.None);
        await file.DisposeAsync();
    }

    /// <summary>A file that throws as it is asked, rather than returning a faulted task: the same.</summary>
    [Fact]
    public async Task AFileThatThrowsAtOnceIsTreatedAsAFailedFetch()
    {
        var file = new PrefetchedFile(new FakeFile(1_000) { FailAtOnce = true }, [new FileRange(0, 100)], CancellationToken.None);
        await Assert.ThrowsAsync<IOException>(() => file.ReadAsync(new FileRange(0, 10)).AsTask());
        await file.DisposeAsync();
        await new PrefetchedFile(new FakeFile(1_000) { FailAtOnce = true }, [new FileRange(0, 100)], CancellationToken.None)
            .DisposeAsync();
    }

    [Fact]
    public async Task DisposalCancelsAFetchStillRunning()
    {
        var inner = new FakeFile(1_000) { Hang = true };
        var file = new PrefetchedFile(inner, [new FileRange(0, 100)], CancellationToken.None);
        var disposal = file.DisposeAsync().AsTask();
        Assert.Same(disposal, await Task.WhenAny(disposal, Task.Delay(TimeSpan.FromSeconds(10))));
        await disposal;
        Assert.True(inner.SawCancellation);
    }

    /// <summary>Bytes equal to their offset (mod 256); can fail, or hang until cancelled.</summary>
    private sealed class FakeFile(int length) : IRandomAccessFile
    {
        public bool Fail { get; init; }

        public bool Hang { get; init; }

        public bool FailAtOnce { get; init; }

        public bool SawCancellation { get; private set; }

        public List<FileRange[]> Requests { get; } = [];

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) => new(length);

        public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default) =>
            (await ReadRangesAsync([range], cancellationToken))[0];

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            Requests.Add(ranges.ToArray());
            return FailAtOnce ? throw new IOException("refused at once") : ReadCoreAsync(ranges, cancellationToken);
        }

        private async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadCoreAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken)
        {
            if (Hang)
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    SawCancellation = true;
                    throw;
                }
            }

            await Task.Yield();
            if (Fail)
                throw new IOException("the store refused the request");

            return ranges.Select(r => (IMemoryOwner<byte>)new Owned(
                Enumerable.Range((int)r.Offset, (int)r.Length).Select(i => (byte)i).ToArray())).ToList();
        }

        public ValueTask DisposeAsync() => default;

        public void Dispose()
        {
        }

        private sealed class Owned(byte[] bytes) : IMemoryOwner<byte>
        {
            public Memory<byte> Memory => bytes;

            public void Dispose()
            {
            }
        }
    }
}
