// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// #894: a two-pass read whose planning read no page index (a RowFilter-only scan) plans with its string columns'
/// OffsetIndexes read ahead, one request for a window of row groups, so that the second pass's string slices are sized
/// from their per-page counts rather than doubled. Off is the read without it (<c>ReadsStringIndexesAhead = false</c>).
/// A case marked "prune" also sets a prune the page index narrows nothing by, so its OffsetIndexes come from planning's
/// page-index read instead, and the read-ahead should read nothing.
/// <para>Usage: <c>twopass-index-ab [rounds] [latencyMs] [cloud] [mbps] [concurrency|-] [rowGroups]</c></para>
/// </summary>
internal static class TwoPassIndexReadAb
{
    private static int RowGroups = 8;
    private static int? s_concurrency;
    private static int GroupRows = 200_000;

    private sealed record Case(string Name, Func<long, bool> Keep, int? BatchSize, bool Prune = false);

    private sealed record Result(double Ms, long Requests, long Bytes, long NativePeak, long NativeTotal,
        long Managed, long Rows, long Checksum);

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 7;
        int latencyMs = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 0;
        bool cloud = args.Length > 3 && args[3] == "cloud";
        double mbps = args.Length > 4 ? double.Parse(args[4], CultureInfo.InvariantCulture) : 0;
        s_concurrency = args.Length > 5 && args[5] != "-" ? int.Parse(args[5], CultureInfo.InvariantCulture) : null;
        RowGroups = args.Length > 6 ? int.Parse(args[6], CultureInfo.InvariantCulture) : 8;
        GroupRows = 1_600_000 / RowGroups;

        string dir = Path.Combine(Path.GetTempPath(), "ew-894-ab");
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, $"two-pass-{RowGroups}.parquet");
        if (!File.Exists(path))
            await WriteAsync(path);

        // Within each group: a leading run (clustered, so the second pass reads only some blocks and cuts them by
        // page maps), or rows spread through it (every block selected, so the second pass reads the group whole).
        var cases = new List<Case>();
        foreach (int? batch in new int?[] { null, 65_536 })
        {
            cases.Add(new Case("clustered 5%", k => k % GroupRows < GroupRows / 20, batch));
            cases.Add(new Case("clustered 25%", k => k % GroupRows < GroupRows / 4, batch));
            cases.Add(new Case("spread 1%", k => k % 100 == 0, batch));
            cases.Add(new Case("spread 30%", k => k % 10 < 3, batch));
        }
        cases.Add(new Case("clustered 25%, prune", k => k % GroupRows < GroupRows / 4, null, Prune: true));

        Console.WriteLine($"# {RowGroups} x {GroupRows:N0} rows; latency {latencyMs} ms, cloud {cloud}, {mbps} MB/s; {rounds} rounds, medians; concurrency {s_concurrency?.ToString(CultureInfo.InvariantCulture) ?? "default"}");
        Console.WriteLine("| case | batch | ms off | on | requests off | on | native peak MB off | on | native total MB off | on |");
        Console.WriteLine("|---|---|---|---|---|---|---|---|---|---|");
        foreach (var c in cases)
        {
            var modes = new[] { false, true };
            var samples = modes.ToDictionary(m => m, _ => new List<Result>());
            foreach (var m in modes)
                await RunOnceAsync(c, path, m, latencyMs, mbps, cloud); // warm-up
            for (int r = 0; r < rounds; r++)
            {
                // Rotated, so drift lands on every mode.
                for (int j = 0; j < modes.Length; j++)
                {
                    var m = modes[(r + j) % modes.Length];
                    samples[m].Add(await RunOnceAsync(c, path, m, latencyMs, mbps, cloud));
                }
            }
            var off = samples[false];
            var on = samples[true];
            if (off[0].Rows != on[0].Rows || off[0].Checksum != on[0].Checksum)
            {
                Console.Error.WriteLine($"{c.Name}: results differ");
                return 1;
            }

            static double Med(List<Result> rs, Func<Result, double> f)
            {
                var v = rs.Select(f).OrderBy(x => x).ToArray();
                return v[v.Length / 2];
            }
            string Two(Func<Result, double> f, string fmt) =>
                $"{Med(off, f).ToString(fmt, CultureInfo.InvariantCulture)} | {Med(on, f).ToString(fmt, CultureInfo.InvariantCulture)}";
            Console.WriteLine(
                $"| {c.Name} | {(c.BatchSize is { } b ? b.ToString(CultureInfo.InvariantCulture) : "none")} " +
                $"| {Two(x => x.Ms, "F0")} | {Two(x => x.Requests, "F0")} " +
                $"| {Two(x => x.NativePeak / 1048576.0, "F1")} | {Two(x => x.NativeTotal / 1048576.0, "F1")} |");
        }

        return 0;
    }

    private static async Task<Result> RunOnceAsync(Case c, string path, bool ahead, int latencyMs, double mbps, bool cloud)
    {
        await using var transport = new LatencyCountingFile(new LocalRandomAccessFile(path), latencyMs, mbps);
        await using var coalescer = cloud ? new CoalescingFileReader(transport) : null;
        IRandomAccessFile file = (IRandomAccessFile?)coalescer ?? transport;
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { BatchSize = c.BatchSize, MaxConcurrentRowGroups = s_concurrency })
        {
            TwoPassReads = true,
            ReadsStringIndexesAhead = ahead,
        };
        await reader.ReadMetadataAsync();
        int requestsBefore = transport.Requests;
        long bytesBefore = transport.Bytes;

        var scan = new ParquetScan
        {
            Columns = ["v", "w"],
            // No value is "m", but every page's v spans it: statistics leave each group undecided, so planning reads the
            // page index (and the projection's OffsetIndexes with it), which narrows nothing.
            Prune = c.Prune ? Expressions.Expressions.NotEqual("v", LiteralValue.Of("m")) : null,
            RowFilter = new RowFilter(new RowPredicate(["k"], batch =>
            {
                var k = (Int64Array)batch.Column(0);
                var keep = new BooleanArray.Builder().Reserve(k.Length);
                for (int i = 0; i < k.Length; i++)
                    keep.Append(c.Keep(k.GetValue(i)!.Value));
                return keep.Build();
            })),
        };

        GC.Collect();
        NativeMemoryTracker.Reset();
        long managedBefore = GC.GetTotalAllocatedBytes(precise: true);
        var clock = Stopwatch.StartNew();
        long rows = 0, checksum = 0;
        await foreach (var selected in reader.ReadSelectedAsync(scan))
        {
            using (selected)
            {
                var batch = selected.Batch;
                rows += batch.Length;
                var v = (StringArray)batch.Column(0);
                var w = (StringArray)batch.Column(1);
                checksum += v.ValueOffsets[^1] - v.ValueOffsets[0] + 3 * (w.ValueOffsets[^1] - w.ValueOffsets[0]);
            }
        }
        double ms = clock.Elapsed.TotalMilliseconds;
        return new Result(ms, transport.Requests - requestsBefore, transport.Bytes - bytesBefore,
            NativeMemoryTracker.PeakBytes, NativeMemoryTracker.TotalAllocated,
            GC.GetTotalAllocatedBytes(precise: true) - managedBefore, rows, checksum);
    }

    // k: the row's position in the file. v: dictionary strings, 5,000 distinct of 33 characters. w: DELTA_BYTE_ARRAY
    // strings, random, 20-60 characters. Default page sizes; the page index is written by default.
    private static async Task WriteAsync(string path)
    {
        var random = new Random(894);
        string Random(int length)
        {
            var chars = new char[length];
            for (int c = 0; c < length; c++)
                chars[c] = (char)('a' + random.Next(26));
            return new string(chars);
        }

        var pool = Enumerable.Range(0, 5_000).Select(_ => Random(33)).ToArray();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("k", Int64Type.Default, false))
            .Field(new Field("v", StringType.Default, false))
            .Field(new Field("w", StringType.Default, false))
            .Build();
        await using var output = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(output, ownsFile: false, new ParquetWriteOptions
        {
            ColumnEncodings = new Dictionary<string, ByteArrayEncoding> { ["w"] = ByteArrayEncoding.DeltaByteArray },
        });
        for (int g = 0; g < RowGroups; g++)
        {
            var k = new Int64Array.Builder();
            var v = new StringArray.Builder();
            var w = new StringArray.Builder();
            for (int i = 0; i < GroupRows; i++)
            {
                k.Append((long)g * GroupRows + i);
                v.Append(pool[random.Next(pool.Length)]);
                w.Append(Random(20 + random.Next(41)));
            }
            using var batch = new RecordBatch(schema, [k.Build(), v.Build(), w.Build()], GroupRows);
            await writer.WriteRowGroupAsync(batch);
        }
        await writer.CloseAsync();
    }
}
#endif
