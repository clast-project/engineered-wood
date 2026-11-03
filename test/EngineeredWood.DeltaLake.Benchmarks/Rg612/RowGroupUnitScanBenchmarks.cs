// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Benchmarks.Rg612;

/// <summary>The data files of a <see cref="RowGroupUnitScanBenchmarks"/> table.</summary>
public enum RgLayout
{
    /// <summary>4 files of 8 row groups x 131,072 rows: few, large files, as Spark writes them.</summary>
    FourLarge,

    /// <summary>One file of 8 row groups x 131,072 rows, then 16 files of one row group x 16,384 rows: a scan
    /// that splits by file finishes the small files and then waits on the large one.</summary>
    Skewed,
}

/// <summary>
/// #612 step 2: a Delta scan split by <c>(file, row group)</c> rather than by file. Each table's files hold
/// several row groups, which every older fixture lacks (all are one row group per file).
/// <see cref="DeltaTableOptions.MaxConcurrentFileReads"/> is the scan's window, 1 being the serial read; before
/// step 2 it counts files, after it row groups. Both a full read and a one-column read.
///
/// <para><c>Cloud</c> reads each data file through a <see cref="CoalescingFileReader"/> over 20 ms per GET and
/// 100 MB/s. The tables are built ONCE, under <c>%TEMP%\ew-rg612-scan</c>, and reused, so builds before and
/// after a change read identical bytes. Run with <c>--filter *RowGroupUnitScanBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
public class RowGroupUnitScanBenchmarks
{
    private const int RowsPerGroup = 131_072;
    private const int SmallRows = 16_384;

    [ParamsAllValues]
    public RgLayout Layout { get; set; }

    [Params(false, true)]
    public bool Cloud { get; set; }

    [Params(1, 4, 8)]
    public int Window { get; set; }

    private DeltaTable _table = null!;

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("amount").DataType(DoubleType.Default).Nullable(false))
        .Field(f => f.Name("category").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("payload").DataType(StringType.Default).Nullable(false))
        .Build();

    [GlobalSetup]
    public void Setup()
    {
        string root = Path.Combine(Path.GetTempPath(), "ew-rg612-scan", Layout.ToString());
        if (!Directory.Exists(Path.Combine(root, "_delta_log")))
            Build(root, Layout).GetAwaiter().GetResult();

        var options = new DeltaTableOptions { MaxConcurrentFileReads = Window };
        _table = DeltaTable.OpenAsync(
                new SimulatedFileSystem(new LocalTableFileSystem(root), Cloud, latencyMs: 20), options)
            .AsTask().GetAwaiter().GetResult();

        long expected = Layout == RgLayout.FourLarge ? 4L * 8 * RowsPerGroup : 8L * RowsPerGroup + 16L * SmallRows;
        long actual = FullRead().GetAwaiter().GetResult();
        if (actual != expected)
            throw new InvalidOperationException($"{Layout}: read {actual} rows, expected {expected}");
    }

    [GlobalCleanup]
    public void Cleanup() => _table.DisposeAsync().AsTask().GetAwaiter().GetResult();

    private static async Task Build(string root, RgLayout layout)
    {
        Directory.CreateDirectory(root);
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup },
            TargetFileSize = long.MaxValue,
        };
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(root), Schema, options);
        var random = new Random(612);
        long next = 0;

        async Task WriteFileAsync(int rows)
        {
            var ids = new Int64Array.Builder().Reserve(rows);
            var amounts = new DoubleArray.Builder().Reserve(rows);
            var categories = new StringArray.Builder();
            var payloads = new StringArray.Builder();
            for (int i = 0; i < rows; i++, next++)
            {
                ids.Append(next);
                amounts.Append(random.NextDouble() * 1000);
                categories.Append("cat_" + (next % 16).ToString("00"));
                payloads.Append(random.Next().ToString("x8") + random.Next().ToString("x8"));
            }
            // One commit per file, so each WriteAsync is one data file.
            await table.WriteAsync([new RecordBatch(Schema,
                [ids.Build(), amounts.Build(), categories.Build(), payloads.Build()], rows)]);
        }

        if (layout == RgLayout.FourLarge)
        {
            for (int f = 0; f < 4; f++)
                await WriteFileAsync(8 * RowsPerGroup);
        }
        else
        {
            await WriteFileAsync(8 * RowsPerGroup);
            for (int f = 0; f < 16; f++)
                await WriteFileAsync(SmallRows);
        }

        int expectedFiles = layout == RgLayout.FourLarge ? 4 : 17;
        if (table.CurrentSnapshot.ActiveFiles.Count != expectedFiles)
            throw new InvalidOperationException($"expected {expectedFiles} data files, got {table.CurrentSnapshot.ActiveFiles.Count}");
    }

    [Benchmark(Description = "ReadAllAsync")]
    public Task<long> FullRead() => ScanAsync(null);

    [Benchmark(Description = "ReadAsync, amount only")]
    public Task<long> OneColumn() => ScanAsync(["amount"]);

    private async Task<long> ScanAsync(IReadOnlyList<string>? columns)
    {
        long rows = 0;
        await foreach (var batch in _table.ReadAsync(new DeltaReadOptions { Columns = columns }))
        {
            rows += batch.Length;
            batch.Dispose();
        }
        return rows;
    }

    /// <summary>
    /// The local table, with each data file (Parquet outside the log) optionally read as object storage: a
    /// coalescer over a transport that pays <c>latencyMs</c> per GET and 100 MB/s.
    /// </summary>
    private sealed class SimulatedFileSystem(ITableFileSystem inner, bool cloud, int latencyMs) : ITableFileSystem
    {
        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            var file = await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            if (!cloud || !path.EndsWith(".parquet", StringComparison.Ordinal)
                || path.IndexOf("_delta_log", StringComparison.Ordinal) >= 0)
                return file;
            return new CoalescingFileReader(new Transport(file, latencyMs));
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

        /// <summary>Each call is one GET: the coalescer above issues one per merged range.</summary>
        private sealed class Transport(IRandomAccessFile inner, int latencyMs) : IRandomAccessFile
        {
            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
                => inner.GetLengthAsync(cancellationToken);

            public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
            {
                double ms = latencyMs + range.Length / (100.0 * 1000);
                if (latencyMs > 0)
                    await Task.Delay(TimeSpan.FromMilliseconds(ms), cancellationToken).ConfigureAwait(false);
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
