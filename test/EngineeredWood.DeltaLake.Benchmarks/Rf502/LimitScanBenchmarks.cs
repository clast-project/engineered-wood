// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Benchmarks.Rg612;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// #502 phase 5f: ten rows of a scan, taken by a consumer that skips and stops itself against an offset and a limit
/// pushed into the scan (<see cref="Pushed"/>). The time includes disposing the scan, so work in flight that disposal
/// waits for is counted. Before the timed runs, one untimed run per case prints the GETs and bytes the data files
/// served, which is the waste on object storage. (Phase 5f's gate measured the consumer alone at windows 1, 4 and 8:
/// doc/row-level-filtering.md §29.)
/// <para>On <see cref="RowGroupUnitScanBenchmarks"/>'s <c>FourLarge</c> table: 4 files of 8 row groups x 131,072 rows.
/// <c>Cloud</c> reads each data file through a coalescer over 20 ms per GET and 100 MB/s. Run with
/// <c>--filter *LimitScanBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 12)]
public class LimitScanBenchmarks
{
    /// <summary>Rows taken: 10, from the start or (<see cref="Paged"/>) from the middle.</summary>
    private const int Limit = 10;

    [Params(false, true)]
    public bool Cloud { get; set; }

    /// <summary>The window of row groups read ahead (1 is serial). Phase 5f's measurement used 1, 4 and 8.</summary>
    [Params(8)]
    public int Window { get; set; }

    /// <summary>The rows taken start at an offset: 2,000,000 rows into the table, or 2,000 rows into the filter's
    /// matches (about 4,200 of them).</summary>
    [Params(false, true)]
    public bool Paged { get; set; }

    /// <summary>
    /// The offset and limit go to the scan (<see cref="DeltaReadOptions.Offset"/>, <see cref="DeltaReadOptions.Limit"/>),
    /// or the consumer skips and stops itself (before #502 phase 5f, the only way).
    /// </summary>
    [Params(false, true)]
    public bool Pushed { get; set; }

    private long Offset => Paged ? (Filtered ? 2_000 : 2_000_000) : 0;

    /// <summary>With <see cref="DeltaReadOptions.FilterRows"/> and <c>amount &lt; 1</c> (0.1%, scattered), or no filter.</summary>
    [Params(false, true)]
    public bool Filtered { get; set; }

    private DeltaTable _table = null!;
    private CountingFileSystem _fs = null!;
    private Expressions.Predicate? _filter;

    [GlobalSetup]
    public void Setup()
    {
        string root = Path.Combine(Path.GetTempPath(), "ew-rg612-scan", RgLayout.FourLarge.ToString());
        // A commit, not just the directory: an interrupted build leaves an empty _delta_log behind.
        if (!RowGroupUnitScanBenchmarks.HasCommit(root))
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
            RowGroupUnitScanBenchmarks.Build(root, RgLayout.FourLarge).GetAwaiter().GetResult();
        }

        _fs = new CountingFileSystem(new LocalTableFileSystem(root), Cloud, latencyMs: 20);
        _table = DeltaTable.OpenAsync(_fs, new DeltaTableOptions { MaxConcurrentReads = Window })
            .AsTask().GetAwaiter().GetResult();
        _filter = Filtered ? SparkSqlParser.ParsePredicate("amount < 1") : null;

        // The waste, from one run: what the data files served, against what the rows taken needed.
        _fs.Reset();
        long taken = TakeAsync().GetAwaiter().GetResult();
        Console.WriteLine(
            $"// LIMIT-IO Cloud={Cloud} Window={Window} Filtered={Filtered} Paged={Paged} Pushed={Pushed}: took {taken} rows; " +
            $"{_fs.Gets} GETs, {_fs.Bytes / 1_000_000.0:F1} MB from data files");
    }

    [GlobalCleanup]
    public void Cleanup() => _table.DisposeAsync().AsTask().GetAwaiter().GetResult();

    [Benchmark(Description = "ReadAsync id, amount; 10 rows")]
    public Task<long> Take() => TakeAsync();

    private async Task<long> TakeAsync()
    {
        var options = new DeltaReadOptions
        {
            Columns = ["id", "amount"], Filter = _filter, FilterRows = _filter is not null,
        };
        if (Pushed)
        {
            long taken = 0;
            await foreach (var batch in _table.ReadAsync(options with { Offset = Offset, Limit = Limit }))
            {
                taken += batch.Length;
                batch.Dispose();
            }

            return taken;
        }

        // The consumer skips the offset and stops at the limit itself.
        long skip = Offset, rows = 0;
        await foreach (var batch in _table.ReadAsync(options))
        {
            long kept = Math.Max(0, batch.Length - skip);
            skip = Math.Max(0, skip - batch.Length);
            rows += kept;
            batch.Dispose();
            if (rows >= Limit)
                break;
        }

        return Math.Min(rows, Limit);
    }

    /// <summary>
    /// The local table, each data file optionally read as object storage (a coalescer over 20 ms per GET and 100 MB/s),
    /// counting the reads and bytes the data files serve.
    /// </summary>
    private sealed class CountingFileSystem(ITableFileSystem inner, bool cloud, int latencyMs) : ITableFileSystem
    {
        private long _gets;
        private long _bytes;

        public long Gets => Interlocked.Read(ref _gets);
        public long Bytes => Interlocked.Read(ref _bytes);

        public void Reset()
        {
            Interlocked.Exchange(ref _gets, 0);
            Interlocked.Exchange(ref _bytes, 0);
        }

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            var file = await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            if (!path.EndsWith(".parquet", StringComparison.Ordinal) || path.IndexOf("_delta_log", StringComparison.Ordinal) >= 0)
                return file;
            var counted = new Transport(file, cloud ? latencyMs : 0, this);
            return cloud ? new CoalescingFileReader(counted) : counted;
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);

        /// <summary>Each call is one GET (under the coalescer, one per merged range).</summary>
        private sealed class Transport(IRandomAccessFile inner, int latencyMs, CountingFileSystem counts) : IRandomAccessFile
        {
            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
                => inner.GetLengthAsync(cancellationToken);

            public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
            {
                Interlocked.Increment(ref counts._gets);
                Interlocked.Add(ref counts._bytes, range.Length);
                if (latencyMs > 0)
                {
                    double ms = latencyMs + range.Length / (100.0 * 1000);
                    await Task.Delay(TimeSpan.FromMilliseconds(ms), cancellationToken).ConfigureAwait(false);
                }

                return await inner.ReadAsync(range, cancellationToken).ConfigureAwait(false);
            }

            public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
            {
                var results = new IMemoryOwner<byte>[ranges.Count];
                for (int i = 0; i < ranges.Count; i++)
                    results[i] = await ReadAsync(ranges[i], cancellationToken).ConfigureAwait(false);
                return results;
            }

            public void Dispose() => inner.Dispose();
            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }
}
