// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Benchmarks.Dv618;

/// <summary>What the deletion vector deletes, inside the file's one row group.</summary>
public enum PageDv
{
    /// <summary>One run of 100,000 rows (10%) in the middle.</summary>
    Run10,
    /// <summary>One run of 500,000 rows (50%) in the middle.</summary>
    Run50,
    /// <summary>Ten runs of 50,000 rows (50%), evenly spread.</summary>
    Runs50,
    /// <summary>One run of 900,000 rows (90%).</summary>
    Run90,
    /// <summary>A random 50%, no run long enough to free a page: the prototype finds nothing to cut.</summary>
    Scattered50,
}

/// <summary>Where the file is read from.</summary>
public enum PageStore
{
    /// <summary>The local file, no added latency.</summary>
    Local,
    /// <summary>Object storage as the cloud readers see it: a coalescer over 20 ms per GET and 100 MB/s.</summary>
    Cloud,
}

/// <summary>
/// #618 step 4, MEASURED BEFORE BUILT: what skipping a deletion vector's long deleted runs INSIDE a kept row group
/// would save. One file of 1M rows in ONE row group (EW's default size; Spark writes one per file), 20,000-row
/// pages (the default), so row-group skipping can never help here.
///
/// <para><c>Baseline</c> is what the Delta scan does today: read the row group whole, then drop the vector's rows
/// (<see cref="KeptRows"/> + <see cref="ArrowCompute.Take(RecordBatch, Apache.Arrow.Schema, ReadOnlySpan{int})"/>).
/// <c>PageSkip</c> is the prototype: find the deleted runs of at least a page from the bitmap alone, read only the
/// rows between them with <see cref="ParquetFileReader.ReadRowRangesAsync"/> (which loads the OffsetIndex and
/// fetches only the pages those rows lie in), then drop the vector's remaining rows the same way. Setup checks
/// both return the same rows, and prints each one's requests and bytes. Run with
/// <c>--filter *PageSkipBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
public class PageSkipBenchmarks
{
    private const int Rows = 1_000_000;
    private const int PageRows = 20_000;

    [ParamsAllValues]
    public PageDv Dv { get; set; }

    [ParamsAllValues]
    public PageStore Store { get; set; }

    private string _path = null!;
    private DeletionVectorBitmap _deleted = null!;

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("amount").DataType(DoubleType.Default).Nullable(false))
        .Field(f => f.Name("category").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("payload").DataType(StringType.Default).Nullable(false))
        .Build();

    [GlobalSetup]
    public void Setup()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ew-dv618-pages");
        _path = Path.Combine(dir, "data.parquet");
        if (!File.Exists(_path))
        {
            Directory.CreateDirectory(dir);
            WriteAsync(_path).GetAwaiter().GetResult();
        }

        _deleted = DeletionVectorBitmap.FromPositions(Deleted(Dv));

        var (baseRows, baseSum, baseRequests, baseBytes) = Measure(Baseline);
        var (skipRows, skipSum, skipRequests, skipBytes) = Measure(PageSkip);
        var (preRows, preSum, preRequests, preBytes) = Measure(PageSkipPrefetched);
        if (baseRows != Rows - _deleted.Count || skipRows != baseRows || skipSum != baseSum
            || preRows != baseRows || preSum != baseSum)
            throw new InvalidOperationException(
                $"{Dv}: baseline {baseRows} rows (sum {baseSum}), page skip {skipRows} (sum {skipSum}), "
                + $"prefetched {preRows} (sum {preSum})");
        Console.WriteLine(
            $"// {Dv}: {baseRows} rows; baseline {baseRequests} GETs {baseBytes:N0} B; "
            + $"page skip {skipRequests} GETs {skipBytes:N0} B; prefetched {preRequests} GETs {preBytes:N0} B");

        (long, long, int, long) Measure(Func<IRandomAccessFile, Task<(long, long)>> read)
        {
            var counting = new LatencyCountingFile(new LocalRandomAccessFile(_path), 0);
            var (rows, sum) = read(new CoalescingFileReader(counting)).GetAwaiter().GetResult();
            return (rows, sum, counting.Requests, counting.Bytes);
        }
    }

    private static IEnumerable<long> Deleted(PageDv dv)
    {
        switch (dv)
        {
            case PageDv.Run10:
                return Range(450_000, 100_000);
            case PageDv.Run50:
                return Range(250_000, 500_000);
            case PageDv.Runs50:
                return Enumerable.Range(0, 10).SelectMany(k => Range(k * 100_000 + 25_000, 50_000));
            case PageDv.Run90:
                return Range(50_000, 900_000);
            default:
                var rng = new Random(618);
                return Enumerable.Range(0, Rows).Where(_ => rng.Next(2) == 0).Select(i => (long)i);
        }

        static IEnumerable<long> Range(long start, int count) =>
            Enumerable.Range(0, count).Select(i => start + i);
    }

    private IRandomAccessFile Open() => Store == PageStore.Cloud
        ? new CoalescingFileReader(new LatencyCountingFile(new LocalRandomAccessFile(_path), 20, 100))
        : new LocalRandomAccessFile(_path);

    [Benchmark(Baseline = true)]
    public async Task<long> ReadBaseline() => (await Baseline(Open())).Rows;

    [Benchmark]
    public async Task<long> ReadPageSkip() => (await PageSkip(Open())).Rows;

    /// <summary>Today's scan: the row group whole, then the vector's rows dropped.</summary>
    private async Task<(long Rows, long Sum)> Baseline(IRandomAccessFile file)
    {
        using var reader = new ParquetFileReader(file, ownsFile: true);
        long rows = 0, sum = 0;
        await foreach (var positioned in reader.ReadWithPositionsAsync(null, null, _deleted.ContainsRange, CancellationToken.None))
            (rows, sum) = Keep(positioned.Batch, positioned.FirstRow, rows, sum);
        return (rows, sum);
    }

    /// <summary>The prototype: only the rows between the vector's page-long runs, then the rest dropped.</summary>
    private async Task<(long Rows, long Sum)> PageSkip(IRandomAccessFile file)
    {
        using var reader = new ParquetFileReader(file, ownsFile: true);
        var metadata = await reader.ReadMetadataAsync();
        long rows = 0, sum = 0, groupStart = 0;
        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            long groupRows = metadata.RowGroups[g].NumRows;
            long start = groupStart;
            groupStart += groupRows;
            if (groupRows == 0 || _deleted.ContainsRange(start, groupRows))
                continue;

            var ranges = LiveRanges(_deleted, start, groupRows, PageRows);
            IAsyncEnumerable<RecordBatch> batches = ranges.Count == 1 && ranges[0].Start == 0 && ranges[0].End == groupRows
                ? reader.ReadRowGroupBatchesAsync(g)
                : reader.ReadRowRangesAsync(g, ranges);

            // Batches follow the ranges and never span a gap: a cursor over them places each one.
            int range = 0;
            long cursor = ranges[0].Start;
            await foreach (var batch in batches)
            {
                (rows, sum) = Keep(batch, start + cursor, rows, sum);
                cursor += batch.Length;
                if (cursor == ranges[range].End && range + 1 < ranges.Count)
                    cursor = ranges[++range].Start;
            }
        }
        return (rows, sum);
    }

    [Benchmark]
    public async Task<long> ReadPageSkipPrefetched() => (await PageSkipPrefetched(Open())).Rows;

    /// <summary>
    /// The prototype with its pages fetched up front: <see cref="ParquetFileReader.ReadRowRangesAsync"/> fetches the
    /// pages of each batch as it reaches it, so N ranges cost N round trips one after another. Here the page index
    /// names every page the ranges need (and each chunk's dictionary), one request fetches them all, and the read
    /// decodes from that: the request pattern a production version would want.
    /// </summary>
    private async Task<(long Rows, long Sum)> PageSkipPrefetched(IRandomAccessFile file)
    {
        var cache = new CachingFile(file);
        using var reader = new ParquetFileReader(cache, ownsFile: true);
        var metadata = await reader.ReadMetadataAsync();
        long rows = 0, sum = 0, groupStart = 0;
        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            var rowGroup = metadata.RowGroups[g];
            long groupRows = rowGroup.NumRows;
            long start = groupStart;
            groupStart += groupRows;
            if (groupRows == 0 || _deleted.ContainsRange(start, groupRows))
                continue;

            var ranges = LiveRanges(_deleted, start, groupRows, PageRows);
            if (ranges.Count == 1 && ranges[0].Start == 0 && ranges[0].End == groupRows)
            {
                await foreach (var batch in reader.ReadRowGroupBatchesAsync(g))
                    (rows, sum) = Keep(batch, start, rows, sum);
                continue;
            }

            // Every page the ranges touch, by column, merged where adjacent; and each chunk's dictionary page.
            var indexes = await reader.ReadPageIndexAsync(g);
            var wanted = new List<FileRange>();
            for (int c = 0; c < indexes.Count; c++)
            {
                var pages = indexes[c].OffsetIndex!.PageLocations;
                var meta = rowGroup.Columns[c].MetaData!;
                if (meta.DictionaryPageOffset is { } d && d > 0 && d < pages[0].Offset)
                    wanted.Add(new FileRange(d, pages[0].Offset - d));
                long runOffset = -1, runEnd = -1;
                for (int p = 0; p < pages.Count; p++)
                {
                    long first = pages[p].FirstRowIndex;
                    long last = p + 1 < pages.Count ? pages[p + 1].FirstRowIndex : groupRows;
                    if (!ranges.Any(r => r.Start < last && first < r.End))
                        continue;
                    if (pages[p].Offset == runEnd)
                    {
                        runEnd += pages[p].CompressedPageSize;
                        continue;
                    }
                    if (runOffset >= 0)
                        wanted.Add(new FileRange(runOffset, runEnd - runOffset));
                    runOffset = pages[p].Offset;
                    runEnd = runOffset + pages[p].CompressedPageSize;
                }
                if (runOffset >= 0)
                    wanted.Add(new FileRange(runOffset, runEnd - runOffset));
            }
            await cache.PrefetchAsync(wanted);

            int range = 0;
            long cursor = ranges[0].Start;
            await foreach (var batch in reader.ReadRowRangesAsync(g, ranges))
            {
                (rows, sum) = Keep(batch, start + cursor, rows, sum);
                cursor += batch.Length;
                if (cursor == ranges[range].End && range + 1 < ranges.Count)
                    cursor = ranges[++range].Start;
            }
        }
        return (rows, sum);
    }

    /// <summary>
    /// Keeps every byte range it has read, and serves a later read lying inside one from memory (copied, so each
    /// caller owns its buffer); anything else goes to the file underneath.
    /// </summary>
    private sealed class CachingFile(IRandomAccessFile inner) : IRandomAccessFile
    {
        private readonly List<(long Offset, byte[] Bytes)> _held = new();

        public async Task PrefetchAsync(IReadOnlyList<FileRange> ranges)
        {
            var buffers = await inner.ReadRangesAsync(ranges);
            for (int i = 0; i < ranges.Count; i++)
            {
                using var buffer = buffers[i];
                _held.Add((ranges[i].Offset, buffer.Memory.ToArray()));
            }
        }

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
        {
            if (TryServe(range) is { } held)
                return held;
            var read = await inner.ReadAsync(range, cancellationToken);
            _held.Add((range.Offset, read.Memory.ToArray()));
            return read;
        }

        public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            var result = new IMemoryOwner<byte>[ranges.Count];
            var missing = new List<int>();
            for (int i = 0; i < ranges.Count; i++)
            {
                if (TryServe(ranges[i]) is { } held)
                    result[i] = held;
                else
                    missing.Add(i);
            }
            if (missing.Count > 0)
            {
                var read = await inner.ReadRangesAsync(missing.Select(i => ranges[i]).ToList(), cancellationToken);
                for (int k = 0; k < missing.Count; k++)
                {
                    _held.Add((ranges[missing[k]].Offset, read[k].Memory.ToArray()));
                    result[missing[k]] = read[k];
                }
            }
            return result;
        }

        private IMemoryOwner<byte>? TryServe(FileRange range)
        {
            foreach (var (offset, bytes) in _held)
            {
                if (range.Offset >= offset && range.Offset + range.Length <= offset + bytes.Length)
                    return new HeldBytes(bytes.AsMemory((int)(range.Offset - offset), (int)range.Length).ToArray());
            }
            return null;
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public void Dispose() => inner.Dispose();

        private sealed class HeldBytes(byte[] bytes) : IMemoryOwner<byte>
        {
            public Memory<byte> Memory => bytes;

            public void Dispose() { }
        }
    }

    private (long Rows, long Sum) Keep(RecordBatch batch, long firstRow, long rows, long sum)
    {
        using (batch)
        {
            using var kept = KeptRows.For(_deleted, firstRow, batch.Length);
            if (kept.Count == 0)
                return (rows, sum);
            var live = kept.AllKept ? batch : ArrowCompute.Take(batch, batch.Schema, kept.Indices);
            try
            {
                var ids = (Int64Array)live.Column(0);
                for (int i = 0; i < ids.Length; i++)
                    sum += ids.GetValue(i)!.Value;
                return (rows + live.Length, sum);
            }
            finally
            {
                if (!ReferenceEquals(live, batch))
                    live.Dispose();
            }
        }
    }

    /// <summary>
    /// The rows of a row group outside every deleted run of at least <paramref name="minRun"/> rows, group-relative
    /// and ascending. A run that long wholly holds an aligned block of half its length, so asking
    /// <see cref="DeletionVectorBitmap.ContainsRange"/> of each such block finds every one (100 asks per 1M rows at
    /// 20,000), and a binary search on either side finds its ends. A vector of short runs costs only the asks.
    /// </summary>
    internal static List<RowRange> LiveRanges(DeletionVectorBitmap deleted, long groupStart, long groupRows, int minRun)
    {
        var ranges = new List<RowRange>();
        long half = Math.Max(1, minRun / 2);
        long live = 0; // group-relative start of the live span being built
        for (long block = 0; block + half <= groupRows; block += half)
        {
            if (block < live || !deleted.ContainsRange(groupStart + block, half))
                continue;

            // Widen [block, block + half) to the whole run, within [live, groupRows).
            long lo = live, hi = block; // the run starts in [lo, hi]
            while (lo < hi)
            {
                long mid = lo + (hi - lo) / 2;
                if (deleted.ContainsRange(groupStart + mid, block - mid)) hi = mid; else lo = mid + 1;
            }
            long runStart = lo;
            lo = block + half;
            hi = groupRows; // the run ends in [lo, hi]
            while (lo < hi)
            {
                long mid = lo + (hi - lo + 1) / 2;
                if (deleted.ContainsRange(groupStart + block, mid - block)) lo = mid; else hi = mid - 1;
            }
            long runEnd = lo;

            if (runEnd - runStart >= minRun)
            {
                if (runStart > live)
                    ranges.Add(new RowRange(live, runStart));
                live = runEnd;
            }
            block = Math.Max(block, runEnd - half); // the loop steps on past the run
        }
        if (live < groupRows)
            ranges.Add(new RowRange(live, groupRows));
        return ranges;
    }

    private static async Task WriteAsync(string path)
    {
        var rng = new Random(42);
        var ids = new Int64Array.Builder().Reserve(Rows);
        var amounts = new DoubleArray.Builder().Reserve(Rows);
        var categories = new StringArray.Builder();
        var payloads = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(i);
            amounts.Append(rng.NextDouble() * 1000);
            categories.Append("cat_" + (i % 16).ToString("00"));
            payloads.Append(rng.Next().ToString("x8") + rng.Next().ToString("x8"));
        }
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false);
        await writer.WriteRowGroupAsync(new RecordBatch(Schema,
            [ids.Build(), amounts.Build(), categories.Build(), payloads.Build()], Rows));
        await writer.CloseAsync();
    }

    /// <summary>
    /// Counts GETs and the bytes they ask for, and delays each by <paramref name="latencyMs"/> plus its transfer
    /// time at <paramref name="megabytesPerSecond"/> (a copy of the Parquet benchmarks' helper, which is internal
    /// there). Under a <see cref="CoalescingFileReader"/>, as here, every merged range is one GET, issued
    /// concurrently with the others of its request, as the cloud readers do.
    /// </summary>
    private sealed class LatencyCountingFile(IRandomAccessFile inner, int latencyMs, double megabytesPerSecond = 0)
        : IRandomAccessFile
    {
        private int _requests;
        private long _bytes;

        public int Requests => _requests;

        public long Bytes => Interlocked.Read(ref _bytes);

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _requests);
            Interlocked.Add(ref _bytes, range.Length);
            await DelayAsync(range.Length, cancellationToken);
            return await inner.ReadAsync(range, cancellationToken);
        }

        public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _requests);
            long bytes = 0;
            foreach (var range in ranges)
                bytes += range.Length;
            Interlocked.Add(ref _bytes, bytes);
            await DelayAsync(bytes, cancellationToken);
            return await inner.ReadRangesAsync(ranges, cancellationToken);
        }

        private Task DelayAsync(long bytes, CancellationToken cancellationToken)
        {
            double ms = latencyMs + (megabytesPerSecond > 0 ? bytes / (megabytesPerSecond * 1000) : 0);
            return ms >= 1 ? Task.Delay(TimeSpan.FromMilliseconds(ms), cancellationToken) : Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public void Dispose() => inner.Dispose();
    }
}
#endif
