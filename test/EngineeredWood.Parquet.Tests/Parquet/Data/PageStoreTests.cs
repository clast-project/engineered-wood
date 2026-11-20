// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The segmented page store that holds a column chunk's pages until its row group is written (#936 decision 2).
/// </summary>
public class PageStoreTests
{
    [Fact]
    public void Segments_DoubleFrom4KiBTo1MiB_ThenStayAt1MiB()
    {
        var store = new SegmentedPageStore();
        long expectedResident = 0;
        int segment = SegmentedPageStore.MinSegment;
        long length = 0;

        // Fill exactly one segment at a time, checking that each new one is the next size up.
        for (int i = 0; i < 12; i++)
        {
            store.Write(new byte[segment]);
            length += segment;
            expectedResident += segment;
            Assert.Equal(length, store.Length);
            Assert.Equal(expectedResident, store.Resident);
            segment = Math.Min(SegmentedPageStore.MaxSegment, segment * 2);
        }

        // 4 + 8 + ... + 1024 KiB, then three more 1 MiB segments.
        Assert.Equal((2L << 20) - (4 << 10) + (3L << 20), store.Resident);
    }

    [Fact]
    public void Resident_IsLengthPlusLessThanOneSegment()
    {
        var store = new SegmentedPageStore();
        store.Write(new byte[1]);
        Assert.Equal(1, store.Length);
        Assert.Equal(SegmentedPageStore.MinSegment, store.Resident);

        store.Write(new byte[SegmentedPageStore.MinSegment]);
        Assert.Equal(SegmentedPageStore.MinSegment + 1, store.Length);
        Assert.Equal(SegmentedPageStore.MinSegment * 3, store.Resident);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4095)]
    [InlineData(4096)]
    [InlineData(4097)]
    [InlineData(1 << 20)]
    [InlineData(5_000_017)]
    public async Task Writes_AcrossSegments_ComeBackInOrder(int chunk)
    {
        var rng = new Random(chunk);
        var expected = new byte[5_000_000 + chunk];
        rng.NextBytes(expected);

        var store = new SegmentedPageStore();
        for (int offset = 0; offset < expected.Length; offset += chunk)
            store.Write(expected.AsSpan(offset, Math.Min(chunk, expected.Length - offset)));

        Assert.Equal(expected.Length, store.Length);
        Assert.Equal(expected, store.ToArray());

        var file = new CollectingFile();
        await store.CopyToAsync(file, CancellationToken.None);
        Assert.Equal(expected, file.Bytes.ToArray());

        // Copying leaves the store as it was.
        Assert.Equal(expected, store.ToArray());
    }

    [Fact]
    public async Task Empty_CopiesNothing()
    {
        var store = new SegmentedPageStore();
        store.Write(ReadOnlySpan<byte>.Empty);
        var file = new CollectingFile();
        await store.CopyToAsync(file, CancellationToken.None);

        Assert.Equal(0, store.Length);
        Assert.Equal(0, store.Resident);
        Assert.Empty(file.Bytes.ToArray());
        Assert.Empty(store.ToArray());
    }

    [Fact]
    public void Dispose_ReleasesEverything()
    {
        var store = new SegmentedPageStore();
        store.Write(new byte[3 << 20]);
        store.Dispose();

        Assert.Equal(0, store.Length);
        Assert.Equal(0, store.Resident);
        store.Dispose();
    }

    private sealed class CollectingFile : ISequentialFile
    {
        public MemoryStream Bytes { get; } = new();

        public long Position => Bytes.Length;

        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            Bytes.Write(data.ToArray(), 0, data.Length);
            return default;
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => default;

        public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => default;

        public ValueTask DisposeAsync() => default;

        public void Dispose() { }
    }
}
