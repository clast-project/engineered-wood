// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using EngineeredWood.IO;

namespace EngineeredWood.Tests.IO;

public class CoalescingFileReaderTests
{
    [Fact]
    public async Task SingleRange_PassesThroughWithoutCoalescing()
    {
        var inner = new FakeRandomAccessFile(1000);
        var reader = new CoalescingFileReader(inner);

        using var result = await reader.ReadAsync(new FileRange(10, 20));
        Assert.Equal(20, result.Memory.Length);
        Assert.Equal(1, inner.ReadCallCount);
    }

    [Fact]
    public async Task CloseRanges_AreMergedIntoOneRequest()
    {
        var inner = new FakeRandomAccessFile(1000);
        var options = new CoalescingOptions { MaxGapBytes = 100 };
        var reader = new CoalescingFileReader(inner, options);

        // Two ranges 50 bytes apart — should be merged
        var ranges = new FileRange[]
        {
            new(0, 10),
            new(60, 10),
        };

        IReadOnlyList<IMemoryOwner<byte>> results = await reader.ReadRangesAsync(ranges);
        try
        {
            Assert.Equal(2, results.Count);

            // Inner should have received a single merged ReadAsync call
            Assert.Equal(1, inner.ReadCallCount);

            // Verify the merged range covers [0, 70)
            Assert.Equal(0, inner.LastReadRange!.Value.Offset);
            Assert.Equal(70, inner.LastReadRange!.Value.Length);
        }
        finally
        {
            foreach (var buf in results)
                buf.Dispose();
        }
    }

    [Fact]
    public async Task FarApartRanges_AreNotMerged()
    {
        var inner = new FakeRandomAccessFile(10000);
        var options = new CoalescingOptions { MaxGapBytes = 100 };
        var reader = new CoalescingFileReader(inner, options);

        // Two ranges 4890 bytes apart — should NOT be merged
        var ranges = new FileRange[]
        {
            new(0, 10),
            new(4900, 10),
        };

        IReadOnlyList<IMemoryOwner<byte>> results = await reader.ReadRangesAsync(ranges);
        try
        {
            Assert.Equal(2, results.Count);

            // Should have two separate ReadAsync calls (not merged)
            Assert.Equal(2, inner.ReadCallCount);
        }
        finally
        {
            foreach (var buf in results)
                buf.Dispose();
        }
    }

    [Fact]
    public async Task MergedResults_AreSlicedCorrectly()
    {
        var inner = new FakeRandomAccessFile(1000);
        var options = new CoalescingOptions { MaxGapBytes = 100 };
        var reader = new CoalescingFileReader(inner, options);

        var ranges = new FileRange[]
        {
            new(10, 5),
            new(20, 5),
        };

        IReadOnlyList<IMemoryOwner<byte>> results = await reader.ReadRangesAsync(ranges);
        try
        {
            Assert.Equal(2, results.Count);

            // FakeRandomAccessFile fills with (offset % 256) pattern
            Assert.Equal(5, results[0].Memory.Length);
            Assert.Equal(10, results[0].Memory.Span[0]);
            Assert.Equal(14, results[0].Memory.Span[4]);

            Assert.Equal(5, results[1].Memory.Length);
            Assert.Equal(20, results[1].Memory.Span[0]);
            Assert.Equal(24, results[1].Memory.Span[4]);
        }
        finally
        {
            foreach (var buf in results)
                buf.Dispose();
        }
    }

    [Fact]
    public async Task ResultOrder_MatchesInputOrder_NotSortedOrder()
    {
        var inner = new FakeRandomAccessFile(1000);
        var options = new CoalescingOptions { MaxGapBytes = 100 };
        var reader = new CoalescingFileReader(inner, options);

        // Provide ranges in reverse offset order
        var ranges = new FileRange[]
        {
            new(60, 5),
            new(10, 5),
        };

        IReadOnlyList<IMemoryOwner<byte>> results = await reader.ReadRangesAsync(ranges);
        try
        {
            // First result should correspond to offset 60, not 10
            Assert.Equal(60, results[0].Memory.Span[0]);
            Assert.Equal(10, results[1].Memory.Span[0]);
        }
        finally
        {
            foreach (var buf in results)
                buf.Dispose();
        }
    }

    [Fact]
    public async Task MaxRequestBytes_PreventsOversizedMerge()
    {
        var inner = new FakeRandomAccessFile(10000);
        var options = new CoalescingOptions
        {
            MaxGapBytes = 10000,
            MaxRequestBytes = 50, // very small limit
        };
        var reader = new CoalescingFileReader(inner, options);

        var ranges = new FileRange[]
        {
            new(0, 30),
            new(40, 30), // merged would be 70 bytes, exceeds MaxRequestBytes
        };

        IReadOnlyList<IMemoryOwner<byte>> results = await reader.ReadRangesAsync(ranges);
        try
        {
            Assert.Equal(2, results.Count);
            // Should not be merged due to MaxRequestBytes — two separate ReadAsync calls
            Assert.Equal(2, inner.ReadCallCount);
        }
        finally
        {
            foreach (var buf in results)
                buf.Dispose();
        }
    }

    /// <summary>
    /// Fake implementation that fills buffers with an (offset % 256) pattern.
    /// Tracks call counts to verify coalescing behavior.
    /// </summary>
    private sealed class FakeRandomAccessFile : IRandomAccessFile
    {
        private readonly long _length;
        private int _readCallCount;

        public int ReadCallCount => _readCallCount;
        public FileRange? LastReadRange { get; private set; }

        public FakeRandomAccessFile(long length) => _length = length;

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            new(_length);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(
            FileRange range, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _readCallCount);
            LastReadRange = range;
            return new(CreateBuffer(range));
        }

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            var results = new IMemoryOwner<byte>[ranges.Count];
            for (int i = 0; i < ranges.Count; i++)
                results[i] = CreateBuffer(ranges[i]);

            return new(results);
        }

        private static IMemoryOwner<byte> CreateBuffer(FileRange range)
        {
            var buffer = PooledBufferAllocator.Default.Allocate(checked((int)range.Length));
            var span = buffer.Memory.Span;
            for (int i = 0; i < span.Length; i++)
                span[i] = (byte)((range.Offset + i) % 256);
            return buffer;
        }

        public void Dispose() { }
        public ValueTask DisposeAsync() => default;
    }
}
