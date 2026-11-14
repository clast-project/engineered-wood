// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers;
using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Benchmarks.Rg652;

/// <summary>
/// #652 experiment (not a BenchmarkDotNet class): the same 12M lineitem-shaped rows (2 files x 6M, ZSTD) written at
/// several row-group sizes, then scanned. Run: <c>dotnet run -c Release -f net10.0 -- rg652 [args]</c>.
/// <c>build</c> writes the tables (and reports write time + peak memory); the default mode scans them, from local disk
/// or (<c>cloud=true</c>) through a simulated object store: <c>latency=</c> ms plus 100 MB/s per request, ranges
/// merged by a <see cref="CoalescingFileReader"/> with <c>gap=</c> and <c>maxreq=</c> bytes (#876). The tables are
/// also the input of the delta-dotnet bench's <c>--rg876</c> sweep on Azure.
/// </summary>
internal static class RowGroupSizeSweep
{
    private const int Files = 2;
    private const int RowsPerFile = 6_000_000;
    private const int Chunk = 1_000_000;

    private static readonly Decimal128Type Money = new(15, 2);

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(f => f.Name("l_orderkey").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("l_partkey").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("l_suppkey").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("l_linenumber").DataType(Int32Type.Default).Nullable(false))
        .Field(f => f.Name("l_quantity").DataType(Money).Nullable(false))
        .Field(f => f.Name("l_extendedprice").DataType(Money).Nullable(false))
        .Field(f => f.Name("l_discount").DataType(Money).Nullable(false))
        .Field(f => f.Name("l_tax").DataType(Money).Nullable(false))
        .Field(f => f.Name("l_returnflag").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("l_linestatus").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("l_shipdate").DataType(Date32Type.Default).Nullable(false))
        .Field(f => f.Name("l_commitdate").DataType(Date32Type.Default).Nullable(false))
        .Field(f => f.Name("l_receiptdate").DataType(Date32Type.Default).Nullable(false))
        .Field(f => f.Name("l_shipinstruct").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("l_shipmode").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("l_comment").DataType(StringType.Default).Nullable(false))
        .Build();

    private static readonly string[] Q1 =
        ["l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate"];

    private const string Q6Filter =
        "l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' " +
        "AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24";

    private static string Root(string layout) => Path.Combine(Path.GetTempPath(), "ew-rg652", layout);

    public static async Task<int> RunAsync(string[] args)
    {
        string Arg(string name, string dflt) =>
            args.FirstOrDefault(a => a.StartsWith(name + "=", StringComparison.Ordinal))?.Substring(name.Length + 1) ?? dflt;

        string[] layouts = Arg("layouts", "w10k,w100k,w1m,w6m").Split(',');
        if (args.Length > 0 && args[0] == "build")
        {
            Console.WriteLine("layout\twrite_s\tprivPeakMiB\tfiles\trowGroups\tbytesMiB");
            foreach (var layout in layouts)
                await BuildLayoutAsync(layout);
            return 0;
        }

        int?[] windows = Arg("windows", "1,d,16").Split(',').Select(w => w == "d" ? (int?)null : int.Parse(w)).ToArray();
        string[] queries = Arg("q", "all,q1,q6").Split(',');
        bool[] clouds = Arg("cloud", "false,true").Split(',').Select(bool.Parse).ToArray();
        string[] batches = Arg("batch", "none,122880").Split(',');
        int reps = int.Parse(Arg("reps", "3"));
        GapBytes = long.Parse(Arg("gap", "1048576"));
        MaxRequestBytes = long.Parse(Arg("maxreq", new CoalescingOptions().MaxRequestBytes.ToString()));
        Latency = int.Parse(Arg("latency", "20"));

        Console.WriteLine($"gap={GapBytes} maxreq={MaxRequestBytes} cores={Environment.ProcessorCount}");
        Console.WriteLine("layout\tquery\tcloud\tbatch\twindow\tbest_s\tmedian_s\tbudgetPeakMiB\tprivPeakMiB\tbatches\tmaxBatchRows\trows\tgets\tgetMiB\tmaxInFlight");
        foreach (var layout in layouts)
        foreach (var query in queries)
        foreach (var cloud in clouds)
        foreach (var batch in batches)
        foreach (var window in windows)
        {
            int? batchSize = batch == "none" ? null : int.Parse(batch);
            var times = new List<double>();
            long peak = 0, privPeak = 0, rows = 0, nBatches = 0, maxRows = 0;
            for (int r = 0; r < reps; r++)
            {
                var options = new DeltaTableOptions
                {
                    MaxConcurrentReads = window,
                    ParquetReadOptions = ParquetReadOptions.Default with { BatchSize = batchSize },
                };
                await using var table = await DeltaTable.OpenAsync(
                    new SimulatedFileSystem(new LocalTableFileSystem(Root(layout)), cloud, latencyMs: Latency), options);
                var read = query switch
                {
                    "q1" => new DeltaReadOptions { Columns = Q1 },
                    "q6" => new DeltaReadOptions
                    {
                        Columns = ["l_extendedprice", "l_discount"],
                        Filter = EngineeredWood.Expressions.Sql.SparkSqlParser.ParsePredicate(Q6Filter),
                        FilterRows = true,
                    },
                    "q6wide" => new DeltaReadOptions
                    {
                        Columns = ["l_extendedprice", "l_discount"],
                        Filter = EngineeredWood.Expressions.Sql.SparkSqlParser.ParsePredicate("l_discount >= 0.02"),
                        FilterRows = true,
                    },
                    _ => new DeltaReadOptions(),
                };
                Gets = 0; GetBytes = 0; MaxInFlight = 0;
                (double secs, long priv) = await MeasureAsync(async () =>
                {
                    rows = 0; nBatches = 0;
                    await foreach (var b in table.ReadAsync(read))
                    {
                        rows += b.Length;
                        nBatches++;
                        maxRows = Math.Max(maxRows, b.Length);
                        b.Dispose();
                    }
                });
                times.Add(secs);
                privPeak = Math.Max(privPeak, priv);
                peak = Math.Max(peak, table.LastScanBudget?.Peak ?? 0);
            }
            times.Sort();
            Console.WriteLine(
                $"{layout}\t{query}\t{cloud}\t{batch}\t{window?.ToString() ?? "d"}\t{times[0]:F2}\t{times[times.Count / 2]:F2}\t{peak >> 20}\t{privPeak >> 20}\t{nBatches}\t{maxRows}\t{rows}\t{Gets}\t{GetBytes >> 20}\t{MaxInFlight}");
        }
        return 0;
    }

    /// <summary>Runs <paramref name="body"/> once; returns its wall time and private-bytes growth (50 ms samples).</summary>
    private static async Task<(double Seconds, long PrivPeak)> MeasureAsync(Func<Task> body)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var proc = Process.GetCurrentProcess();
        proc.Refresh();
        long priv0 = proc.PrivateMemorySize64, privMax = priv0;
        using var sampler = new CancellationTokenSource();
        var sampling = Task.Run(async () =>
        {
            var me = Process.GetCurrentProcess();
            while (!sampler.IsCancellationRequested)
            {
                me.Refresh();
                privMax = Math.Max(privMax, me.PrivateMemorySize64);
                try { await Task.Delay(50, sampler.Token); } catch (OperationCanceledException) { }
            }
        });
        var sw = Stopwatch.StartNew();
        await body();
        sw.Stop();
        sampler.Cancel();
        await sampling;
        return (sw.Elapsed.TotalSeconds, privMax - priv0);
    }

    private static int RowsOf(string size) => size switch
    {
        "10k" => 10_000,
        "100k" => 100_000,
        "1m" => 1_000_000,
        "6m" => 6_000_000,
        _ => int.Parse(size),
    };

    /// <summary>
    /// <c>w&lt;size&gt;</c>: written at that row-group size. <c>c&lt;size&gt;</c>: hard-linked copy of w10k compacted
    /// into one file per source pair at that size. <c>dflt</c>: written with EW's default write options.
    /// RowGroupMaxBytes is lifted for the w/c layouts so the row count alone decides the cut.
    /// </summary>
    private static async Task BuildLayoutAsync(string layout)
    {
        string root = Root(layout);
        if (Directory.Exists(root))
            Directory.Delete(root, recursive: true);
        var write = ParquetWriteOptions.Default with { Compression = CompressionCodec.Zstd };
        if (layout != "dflt")
            write = write with { RowGroupMaxRows = RowsOf(layout.Substring(1)), RowGroupMaxBytes = 1L << 40 };
        var options = new DeltaTableOptions { ParquetWriteOptions = write, TargetFileSize = long.MaxValue };

        double secs;
        long priv;
        if (layout[0] == 'c')
        {
            CloneTable(Root("w10k"), root);
            await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(root), options);
            (secs, priv) = await MeasureAsync(async () =>
                await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue }));
        }
        else
        {
            Directory.CreateDirectory(root);
            await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(root), Schema, options);
            var random = new Random(652);
            long order = 1;
            (secs, priv) = (0, 0);
            for (int f = 0; f < Files; f++)
            {
                var list = new List<RecordBatch>();
                for (int c = 0; c < RowsPerFile / Chunk; c++)
                    list.Add(Generate(random, Chunk, ref order));
                var (s, p) = await MeasureAsync(async () => await table.WriteAsync(list));
                secs += s;
                priv = Math.Max(priv, p);
                foreach (var b in list)
                    b.Dispose();
            }
        }

        int files = 0, groups = 0;
        long bytes = 0;
        foreach (var file in Directory.EnumerateFiles(root, "*.parquet", SearchOption.AllDirectories))
        {
            if (Path.GetFileName(Path.GetDirectoryName(file)!) == "_delta_log")
                continue;
            await using var local = new LocalRandomAccessFile(file);
            using var reader = new ParquetFileReader(local, ownsFile: false);
            var meta = await reader.ReadMetadataAsync();
            files++;
            groups += meta.RowGroups.Count;
            bytes += new FileInfo(file).Length;
        }
        // Compaction leaves the replaced files on disk; count only the live ones.
        await using (var t = await DeltaTable.OpenAsync(new LocalTableFileSystem(root)))
            if (t.CurrentSnapshot.ActiveFiles.Count != files)
                (files, groups, bytes) = await LiveLayoutAsync(root, t);
        Console.WriteLine($"{layout}\t{secs:F2}\t{priv >> 20}\t{files}\t{groups}\t{bytes >> 20}");
    }

    private static async Task<(int, int, long)> LiveLayoutAsync(string root, DeltaTable table)
    {
        int files = 0, groups = 0;
        long bytes = 0;
        foreach (var add in table.CurrentSnapshot.ActiveFiles.Values)
        {
            string file = Path.Combine(root, Uri.UnescapeDataString(add.Path));
            await using var local = new LocalRandomAccessFile(file);
            using var reader = new ParquetFileReader(local, ownsFile: false);
            var meta = await reader.ReadMetadataAsync();
            files++;
            groups += meta.RowGroups.Count;
            bytes += new FileInfo(file).Length;
        }
        return (files, groups, bytes);
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    private static extern bool CreateHardLink(string newFile, string existingFile, IntPtr security);

    private static void CloneTable(string source, string target)
    {
        Directory.CreateDirectory(Path.Combine(target, "_delta_log"));
        foreach (var f in Directory.EnumerateFiles(source, "*.parquet"))
        {
            if (!CreateHardLink(Path.Combine(target, Path.GetFileName(f)), f, IntPtr.Zero))
                throw new IOException($"hard link failed: {System.Runtime.InteropServices.Marshal.GetLastWin32Error()}");
        }
        foreach (var f in Directory.EnumerateFiles(Path.Combine(source, "_delta_log"), "*.json"))
            File.Copy(f, Path.Combine(target, "_delta_log", Path.GetFileName(f)));
    }

    private static readonly string[] Flags = ["A", "N", "R"];
    private static readonly string[] Statuses = ["F", "O"];
    private static readonly string[] Instructs = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"];
    private static readonly string[] Modes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
    private static readonly string[] Words = ["furiously", "carefully", "quickly", "blithely", "slyly", "ironic", "final",
        "regular", "pending", "express", "deposits", "requests", "accounts", "packages", "theodolites", "foxes", "pinto",
        "beans", "instructions", "asymptotes", "dependencies", "excuses", "sleep", "haggle", "nag", "cajole"];

    private static RecordBatch Generate(Random random, int n, ref long order)
    {
        var ok = new Int64Array.Builder().Reserve(n);
        var pk = new Int64Array.Builder().Reserve(n);
        var sk = new Int64Array.Builder().Reserve(n);
        var ln = new Int32Array.Builder().Reserve(n);
        var qty = new Decimal128Array.Builder(Money).Reserve(n);
        var price = new Decimal128Array.Builder(Money).Reserve(n);
        var disc = new Decimal128Array.Builder(Money).Reserve(n);
        var tax = new Decimal128Array.Builder(Money).Reserve(n);
        var rf = new StringArray.Builder();
        var ls = new StringArray.Builder();
        var sd = new Date32Array.Builder().Reserve(n);
        var cd = new Date32Array.Builder().Reserve(n);
        var rd = new Date32Array.Builder().Reserve(n);
        var si = new StringArray.Builder();
        var sm = new StringArray.Builder();
        var cm = new StringArray.Builder();
        var epoch = new DateTime(1992, 1, 1);
        int line = 1;
        var sb = new System.Text.StringBuilder();
        for (int i = 0; i < n; i++)
        {
            if (line > 1 + random.Next(7)) { order += 1 + random.Next(4); line = 1; }
            ok.Append(order);
            pk.Append(random.Next(2_000_000) + 1);
            sk.Append(random.Next(100_000) + 1);
            ln.Append(line++);
            int q = random.Next(50) + 1;
            qty.Append((decimal)q);
            price.Append(Math.Round(q * (900m + random.Next(100_000) / 100m), 2));
            disc.Append(random.Next(11) / 100m);
            tax.Append(random.Next(9) / 100m);
            rf.Append(Flags[random.Next(3)]);
            ls.Append(Statuses[random.Next(2)]);
            var ship = epoch.AddDays(random.Next(2500));
            sd.Append(ship);
            cd.Append(ship.AddDays(random.Next(60) - 30));
            rd.Append(ship.AddDays(random.Next(30) + 1));
            si.Append(Instructs[random.Next(4)]);
            sm.Append(Modes[random.Next(7)]);
            sb.Clear();
            int wc = 2 + random.Next(4);
            for (int w = 0; w < wc; w++)
            {
                if (w > 0) sb.Append(' ');
                sb.Append(Words[random.Next(Words.Length)]);
            }
            cm.Append(sb.ToString());
        }
        return new RecordBatch(Schema,
        [
            ok.Build(), pk.Build(), sk.Build(), ln.Build(), qty.Build(), price.Build(), disc.Build(), tax.Build(),
            rf.Build(), ls.Build(), sd.Build(), cd.Build(), rd.Build(), si.Build(), sm.Build(), cm.Build(),
        ], n);
    }

    /// <summary>As in the #829 harness: data files optionally read as object storage (20 ms per GET, 100 MB/s per
    /// request) through a coalescer, reporting a concurrency hint of 16 as S3/Azure/GCS do.</summary>
    internal static long GapBytes = 1024 * 1024;
    internal static long MaxRequestBytes = new CoalescingOptions().MaxRequestBytes;
    internal static int Latency = 20;
    internal static long Gets, GetBytes;
    internal static int InFlight, MaxInFlight;

    private sealed class SimulatedFileSystem(ITableFileSystem inner, bool cloud, int latencyMs) : ITableFileSystem
    {
        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            var file = await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            if (!cloud || !path.EndsWith(".parquet", StringComparison.Ordinal)
                || path.IndexOf("_delta_log", StringComparison.Ordinal) >= 0)
                return file;
            return new CoalescingFileReader(new Transport(file, latencyMs), new CoalescingOptions { MaxGapBytes = GapBytes, MaxRequestBytes = MaxRequestBytes });
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

        private sealed class Transport(IRandomAccessFile inner, int latencyMs) : IRandomAccessFile, IReadConcurrencyHint
        {
            public int? PreferredConcurrentReads => 16;

            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
                => inner.GetLengthAsync(cancellationToken);

            public async ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
            {
                Interlocked.Increment(ref Gets);
                Interlocked.Add(ref GetBytes, range.Length);
                int now = Interlocked.Increment(ref InFlight);
                int seen;
                while (now > (seen = Volatile.Read(ref MaxInFlight)) && Interlocked.CompareExchange(ref MaxInFlight, now, seen) != seen) { }
                try
                {
                    double ms = latencyMs + range.Length / (100.0 * 1000);
                    if (latencyMs > 0)
                        await Task.Delay(TimeSpan.FromMilliseconds(ms), cancellationToken).ConfigureAwait(false);
                    return await inner.ReadAsync(range, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    Interlocked.Decrement(ref InFlight);
                }
            }

            public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
            {
                var tasks = new Task<IMemoryOwner<byte>>[ranges.Count];
                for (int i = 0; i < ranges.Count; i++)
                    tasks[i] = ReadAsync(ranges[i], cancellationToken).AsTask();
                return await Task.WhenAll(tasks).ConfigureAwait(false);
            }

            public void Dispose() => inner.Dispose();
            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }
}
#endif
