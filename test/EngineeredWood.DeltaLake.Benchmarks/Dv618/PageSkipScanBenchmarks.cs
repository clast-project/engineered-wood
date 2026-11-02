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

namespace EngineeredWood.DeltaLake.Benchmarks.Dv618;

/// <summary>What the deletion vector deletes, inside the table's one row group.</summary>
public enum ScanDv
{
    /// <summary>No deletion vector: the scan must not change.</summary>
    None,
    /// <summary>One run of 100,000 rows (10%) in the middle.</summary>
    Run10,
    /// <summary>One run of 500,000 rows (50%) in the middle.</summary>
    Run50,
    /// <summary>Ten runs of 50,000 rows (50%), evenly spread.</summary>
    Runs50,
    /// <summary>One run of 900,000 rows (90%).</summary>
    Run90,
    /// <summary>A random 50%, no run long enough to cut.</summary>
    Scattered50,
}

/// <summary>
/// #618 step 4 end to end: <see cref="DeltaTable.ReadAsync"/> over a one-file table of 1M rows in ONE row group
/// (EW's default size; Spark writes one per file), 20,000-row pages, with a deletion vector inside that group.
/// Before step 4 the scan reads the whole group and drops the vector's rows; after it, the long deleted runs
/// are cut out of the read. Uses only API that predates step 4, so the same source runs against both builds.
///
/// <para><c>Cloud</c> reads each data file through a <see cref="CoalescingFileReader"/> over 20 ms per GET and
/// 100 MB/s, as the S3, Azure and GCS readers coalesce. The tables are built ONCE, under
/// <c>%TEMP%\ew-dv618-scan</c>, and reused, so both builds read identical bytes. Setup prints the rows, data GETs
/// and data bytes one cloud scan reads. Run with <c>--filter *PageSkipScanBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
public class PageSkipScanBenchmarks
{
    private const int Rows = 1_000_000;

    [ParamsAllValues]
    public ScanDv Dv { get; set; }

    [Params(false, true)]
    public bool Cloud { get; set; }

    private DeltaTable _table = null!;

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("amount").DataType(DoubleType.Default).Nullable(false))
        .Field(f => f.Name("category").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("payload").DataType(StringType.Default).Nullable(false))
        .Build();

    private static bool[] DeletedMask(ScanDv dv)
    {
        var deleted = new bool[Rows];
        void Run(int start, int count) => deleted.AsSpan(start, count).Fill(true);
        switch (dv)
        {
            case ScanDv.Run10: Run(450_000, 100_000); break;
            case ScanDv.Run50: Run(250_000, 500_000); break;
            case ScanDv.Runs50:
                for (int k = 0; k < 10; k++)
                    Run(k * 100_000 + 25_000, 50_000);
                break;
            case ScanDv.Run90: Run(50_000, 900_000); break;
            case ScanDv.Scattered50:
                var random = new Random(618);
                for (int i = 0; i < Rows; i++)
                    deleted[i] = random.Next(2) == 0;
                break;
        }

        return deleted;
    }

    [GlobalSetup]
    public void Setup()
    {
        string root = Path.Combine(Path.GetTempPath(), "ew-dv618-scan", Dv.ToString());
        var deleted = DeletedMask(Dv);
        if (!Directory.Exists(Path.Combine(root, "_delta_log")))
            Build(root, deleted).GetAwaiter().GetResult();

        long expected = deleted.LongCount(d => !d);
        var probe = new SimulatedFileSystem(new LocalTableFileSystem(root), cloud: true, latencyMs: 0);
        long actual;
        using (var counted = DeltaTable.OpenAsync(probe).AsTask().GetAwaiter().GetResult())
            actual = ScanAsync(counted).GetAwaiter().GetResult();
        if (actual != expected)
            throw new InvalidOperationException($"{Dv}: read {actual} rows, expected {expected}");
        Console.WriteLine($"// {Dv}: {actual} rows; cloud scan {probe.Gets} data GETs, {probe.Bytes:N0} data bytes");

        _table = DeltaTable.OpenAsync(new SimulatedFileSystem(new LocalTableFileSystem(root), Cloud, latencyMs: 20))
            .AsTask().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup() => _table.DisposeAsync().AsTask().GetAwaiter().GetResult();

    private static async Task Build(string root, bool[] deleted)
    {
        Directory.CreateDirectory(root);
        var random = new Random(6184);
        var ids = new Int64Array.Builder().Reserve(Rows);
        var amounts = new DoubleArray.Builder().Reserve(Rows);
        var categories = new StringArray.Builder();
        var payloads = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(i);
            amounts.Append(random.NextDouble() * 1000);
            categories.Append("cat_" + (i % 16).ToString("00"));
            payloads.Append(random.Next().ToString("x8") + random.Next().ToString("x8"));
        }

        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = Rows },
        };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(root), Schema, options, enableDeletionVectors: true);
        await table.WriteAsync([new RecordBatch(Schema,
            [ids.Build(), amounts.Build(), categories.Build(), payloads.Build()], Rows)]);
        if (table.CurrentSnapshot.ActiveFiles.Count != 1)
            throw new InvalidOperationException("expected one data file");
        if (!deleted.Any(d => d))
            return;
        await table.DeleteAsync(batch =>
        {
            var col = (Int64Array)batch.Column("id");
            var mask = new BooleanArray.Builder().Reserve(col.Length);
            for (int i = 0; i < col.Length; i++)
                mask.Append(deleted[col.GetValue(i)!.Value]);
            return mask.Build();
        }, RowDeleteMode.DeletionVector);
    }

    [Benchmark]
    public Task<long> Scan() => ScanAsync(_table);

    private static async Task<long> ScanAsync(DeltaTable table)
    {
        long rows = 0;
        await foreach (var batch in table.ReadAsync())
        {
            rows += batch.Length;
            batch.Dispose();
        }
        return rows;
    }

    /// <summary>
    /// The local table, with each data file (Parquet outside the log) optionally read as object storage: a
    /// coalescer over a transport that pays <c>latencyMs</c> per GET and 100 MB/s, counting GETs and bytes.
    /// </summary>
    private sealed class SimulatedFileSystem(ITableFileSystem inner, bool cloud, int latencyMs) : ITableFileSystem
    {
        private int _gets;
        private long _bytes;

        public int Gets => _gets;
        public long Bytes => Interlocked.Read(ref _bytes);

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            var file = await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            if (!cloud || !path.EndsWith(".parquet", StringComparison.Ordinal)
                || path.IndexOf("_delta_log", StringComparison.Ordinal) >= 0)
                return file;
            return new CoalescingFileReader(new Transport(file, this, latencyMs));
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
        private sealed class Transport(IRandomAccessFile inner, SimulatedFileSystem owner, int latencyMs) : IRandomAccessFile
        {
            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
                => inner.GetLengthAsync(cancellationToken);

            public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
            {
                await DelayAsync(range.Length, cancellationToken).ConfigureAwait(false);
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

            private Task DelayAsync(long bytes, CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref owner._gets);
                Interlocked.Add(ref owner._bytes, bytes);
                double ms = latencyMs + bytes / (100.0 * 1000);
                return latencyMs > 0 ? Task.Delay(TimeSpan.FromMilliseconds(ms), cancellationToken) : Task.CompletedTask;
            }

            public void Dispose() => inner.Dispose();
            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }
}
