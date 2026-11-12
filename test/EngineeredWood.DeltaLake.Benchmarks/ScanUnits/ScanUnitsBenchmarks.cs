// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Benchmarks.Rf502;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Benchmarks.ScanUnits;

/// <summary>
/// One ordered <see cref="DeltaTable.ReadAsync"/> drained by one reader against
/// <see cref="DeltaTable.OpenScanUnitsAsync"/> drained by several readers, each taking the next unit (a row group)
/// when it has finished one. The reader is the host: a query engine whose threads each process what they read.
/// <list type="bullet">
/// <item><see cref="Work"/>: the reader's own work per row (an FNV hash over the comment), standing in for the
/// operators above the scan. Without it the comparison measures the scan alone.</item>
/// <item><see cref="Store"/>: <c>local</c>; <c>store</c>, a <see cref="SimulatedObjectStore"/> at 5 ms and 200 MB/s
/// per request; <c>stall</c>, the same store where one read of one early file waits a further 150 ms once per scan,
/// as a request that times out and is retried does. In order, every unit behind it waits; read by units, only the
/// reader that took it does.</item>
/// </list>
/// <para>The table (8 files of 4 row groups of 250,000 rows, orders-shaped) is built under
/// <c>%TEMP%\ew-scan-units</c>. Run with <c>--filter *ScanUnitsBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 8)]
public class ScanUnitsBenchmarks
{
    private const int Files = 8;
    private const int GroupsPerFile = 4;
    private const int RowsPerGroup = 250_000;
    private const long TotalRows = (long)Files * GroupsPerFile * RowsPerGroup;
    private static readonly string TableRoot = Path.Combine(Path.GetTempPath(), "ew-scan-units");

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("orderkey", Apache.Arrow.Types.Int64Type.Default, nullable: false))
        .Field(new Field("custkey", Apache.Arrow.Types.Int64Type.Default, nullable: false))
        .Field(new Field("comment", Apache.Arrow.Types.StringType.Default, nullable: false))
        .Build();

    [Params("ordered", "ordered-8", "ordered-16", "partitions-16", "units-4", "units-16", "units-16-lookahead")]
    public string Mode { get; set; } = "ordered";

    [Params("local", "store", "stall", "datastall")]
    public string Store { get; set; } = "local";

    [Params(false, true)]
    public bool Work { get; set; }

    private DeltaTable _table = null!;
    private StallOnce? _stall;

    [GlobalSetup]
    public void Setup()
    {
        EnsureTable();
        ITableFileSystem fs = new LocalTableFileSystem(TableRoot);
        if (Store != "local")
        {
            SimulatedObjectStore.BeginHighResolutionTimers();
            fs = new SimulatedObjectStore(fs, TimeSpan.FromMilliseconds(5), bytesPerSecond: 200L << 20);
            if (Store is "stall" or "datastall")
            {
                // the second file in path order: early, so an ordered read has most of the scan behind it
                var second = Directory.GetFiles(TableRoot, "*.parquet").Select(Path.GetFileName).OrderBy(n => n, StringComparer.Ordinal).ElementAt(1)!;
                fs = _stall = new StallOnce(fs, second, TimeSpan.FromMilliseconds(150), dataOnly: Store == "datastall");
            }
        }
        // ordered-N: the ordered scan tuned instead, N row groups read at once (and N files opened ahead)
        int? concurrent = Mode.StartsWith("ordered-", StringComparison.Ordinal)
            ? int.Parse(Mode.Substring("ordered-".Length)) : null;
        _table = DeltaTable.OpenAsync(fs, new DeltaTableOptions { MaxConcurrentReads = concurrent })
            .AsTask().GetAwaiter().GetResult();
        long rows = Scan().GetAwaiter().GetResult();
        if (rows != TotalRows)
            throw new InvalidOperationException($"Read {rows} rows, expected {TotalRows}.");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _table.DisposeAsync().AsTask().GetAwaiter().GetResult();
        if (Store != "local")
            SimulatedObjectStore.EndHighResolutionTimers();
    }

    [Benchmark(Description = "scan orderkey, custkey, comment")]
    public async Task<long> Scan()
    {
        _stall?.Arm();
        var options = new DeltaReadOptions { Columns = ["orderkey", "custkey", "comment"] };
        if (Mode.StartsWith("ordered", StringComparison.Ordinal))
        {
            long rows = 0;
            await foreach (var batch in _table.ReadAsync(options))
                rows += Consume(batch);
            return rows;
        }

        if (Mode.StartsWith("partitions-", StringComparison.Ordinal))
        {
            // N streams for the whole scan, each drained by one reader (DeltaTable.OpenPartitionsAsync)
            int n = int.Parse(Mode.Substring("partitions-".Length));
            await using var parts = await _table.OpenPartitionsAsync(options, n);
            var drains = new Task<long>[n];
            for (int i = 0; i < n; i++)
            {
                int index = i;
                drains[i] = Task.Run(async () =>
                {
                    long rows = 0;
                    await foreach (var b in parts.Partition(index))
                        rows += Consume(b.Batch);
                    return rows;
                });
            }
            long sum = 0;
            foreach (var r in await Task.WhenAll(drains))
                sum += r;
            return sum;
        }

        // units-N: N readers; units-16-lookahead: 16 readers, and the planner told so (it opens 16 files ahead
        // instead of the table's read concurrency)
        bool tell = Mode.EndsWith("-lookahead", StringComparison.Ordinal);
        int readers = int.Parse(Mode.Substring("units-".Length).Replace("-lookahead", ""));
        await using var units = await _table.OpenScanUnitsAsync(options, readers: tell ? readers : null);
        var tasks = new Task<long>[readers];
        for (int r = 0; r < readers; r++)
        {
            tasks[r] = Task.Run(async () =>
            {
                long rows = 0;
                while (await units.NextAsync() is { } unit)
                {
                    await foreach (var batch in unit.ReadAsync())
                        rows += Consume(batch);
                }
                return rows;
            });
        }
        long total = 0;
        foreach (var n in await Task.WhenAll(tasks))
            total += n;
        return total;
    }

    private long Consume(RecordBatch batch)
    {
        int n = batch.Length;
        if (Work)
        {
            var comments = (StringArray)batch.Column(2);
            uint hash = 2166136261;
            for (int i = 0; i < n; i++)
            {
                foreach (byte b in comments.GetBytes(i))
                    hash = (hash ^ b) * 16777619;
            }
            if (hash == 42)
                Console.Write(string.Empty); // keeps the loop
        }
        batch.Dispose();
        return n;
    }

    private static string CompleteMarker => Path.Combine(TableRoot, "_build_complete");

    private static void EnsureTable()
    {
        if (File.Exists(CompleteMarker))
            return;
        if (Directory.Exists(TableRoot))
            Directory.Delete(TableRoot, recursive: true);
        Build().GetAwaiter().GetResult();
    }

    private static async Task Build()
    {
        Directory.CreateDirectory(TableRoot);
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup },
            TargetFileSize = long.MaxValue,
        };
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(TableRoot), Schema, options);
        var random = new Random(4242);
        string[] words = ["final", "special", "pending", "requests", "deposits", "accounts", "furiously", "quickly",
            "regular", "express", "ironic", "packages", "theodolites", "carefully", "blithely", "slyly"];
        long next = 0;
        for (int f = 0; f < Files; f++)
        {
            int rows = GroupsPerFile * RowsPerGroup;
            var orderKeys = new Int64Array.Builder().Reserve(rows);
            var custKeys = new Int64Array.Builder().Reserve(rows);
            var comments = new StringArray.Builder();
            var text = new System.Text.StringBuilder();
            for (int i = 0; i < rows; i++, next++)
            {
                orderKeys.Append(next * 4);
                custKeys.Append(random.Next(1_500_000));
                text.Clear();
                int count = 4 + random.Next(6);
                for (int w = 0; w < count; w++)
                    text.Append(words[random.Next(words.Length)]).Append(' ');
                comments.Append(text.ToString());
            }
            await table.WriteAsync([new RecordBatch(Schema, [orderKeys.Build(), custKeys.Build(), comments.Build()], rows)]);
        }
        File.WriteAllText(CompleteMarker, $"{TotalRows} rows");
    }

    /// <summary>
    /// Delays the first read of one file by <paramref name="stall"/> once per <see cref="Arm"/>: a request that waits
    /// out a timeout before its retry answers.
    /// </summary>
    /// <para><paramref name="dataOnly"/>: only a read in the first half of the file (a data page, not the footer the
    /// planner reads when it opens the file) waits.</para>
    private sealed class StallOnce(ITableFileSystem inner, string fileName, TimeSpan stall, bool dataOnly)
        : ITableFileSystem, IReadConcurrencyHint
    {
        private int _armed;

        public void Arm() => Interlocked.Exchange(ref _armed, 1);

        public int? PreferredConcurrentReads => (inner as IReadConcurrencyHint)?.PreferredConcurrentReads;

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, cancellationToken);

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            var file = await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            return path.EndsWith(fileName, StringComparison.Ordinal) ? new StallingFile(this, file) : file;
        }

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

        private async Task WaitIfArmed(IRandomAccessFile file, long offset, CancellationToken cancellationToken)
        {
            if (dataOnly && offset >= await file.GetLengthAsync(cancellationToken).ConfigureAwait(false) / 2)
                return;
            if (Interlocked.Exchange(ref _armed, 0) == 1)
                await Task.Delay(stall, cancellationToken).ConfigureAwait(false);
        }

        private sealed class StallingFile(StallOnce owner, IRandomAccessFile file) : IRandomAccessFile, IReadConcurrencyHint
        {
            public int? PreferredConcurrentReads => (file as IReadConcurrencyHint)?.PreferredConcurrentReads;

            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) => file.GetLengthAsync(cancellationToken);

            public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
            {
                await owner.WaitIfArmed(file, range.Offset, cancellationToken).ConfigureAwait(false);
                return await file.ReadAsync(range, cancellationToken).ConfigureAwait(false);
            }

            public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
            {
                await owner.WaitIfArmed(file, ranges.Count == 0 ? 0 : ranges.Min(r => r.Offset), cancellationToken).ConfigureAwait(false);
                return await file.ReadRangesAsync(ranges, cancellationToken).ConfigureAwait(false);
            }

            public ValueTask DisposeAsync() => file.DisposeAsync();

            public void Dispose() => file.Dispose();
        }
    }
}
