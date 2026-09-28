// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// What <see cref="ParquetReadOptions.FilterUsePageIndex"/> costs and saves on a filtered
/// <see cref="ParquetFileReader.ReadAllAsync"/>, measured to decide whether it can default on
/// (doc/parquet-page-index.md, R-3). Reports the median time of alternating off/on rounds, and the
/// requests, bytes and rows each read returns.
/// </summary>
/// <remarks>
/// <para>The file is 4 row groups of 500,000 rows written with default options, so every column has
/// 25 pages per row group (the 20,000-row page cap). <c>id</c> and <c>ts</c> are sorted;
/// <c>category</c>, <c>amount</c> and <c>note</c> are not. The cases:</para>
/// <list type="bullet">
/// <item><b>id point</b>, <b>id 1%</b>, <b>id 10%</b>: row-group statistics keep one row group, and its
/// pages narrow it to one, one or two, and ten pages.</item>
/// <item><b>category (unclustered)</b>: nothing can be pruned at either level. The worst case: each row
/// group pays one request for its page index and saves nothing.</item>
/// <item><b>id 1% AND category</b>: the sorted column narrows, the other cannot.</item>
/// </list>
/// <para>Local disk hides what matters most on object storage: the index is one more request, issued
/// before the data read. <c>latencyMs</c> delays every request by that much, and <c>cloud</c> reads
/// through <see cref="CoalescingFileReader"/>, as the S3, Azure and GCS readers do, delaying each GET
/// it issues rather than each call. A GET also takes its bytes at <c>MB/s</c> when that is given:
/// latency alone makes the bytes the index saves free, which understates it.</para>
/// Run with: dotnet run -c Release -f net10.0 -- pageindex-read-ab [rounds] [latencyMs] [direct|cloud] [MB/s]
/// </remarks>
internal static class PageIndexReadAb
{
    private const int RowGroups = 4;
    private const int RowsPerGroup = 500_000;

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 15;
        int latencyMs = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 0;
        bool cloud = args.Length > 3 && args[3] == "cloud";
        double mbps = args.Length > 4 ? double.Parse(args[4], CultureInfo.InvariantCulture) : 0;
        string dir = Path.Combine(Path.GetTempPath(), "ew-pageindex-read-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);

        try
        {
            string path = Path.Combine(dir, "data.parquet");
            await WriteAsync(Build(), path);

            const long total = (long)RowGroups * RowsPerGroup;
            var onePct = Ex.And(
                Ex.GreaterThanOrEqual("id", LiteralValue.Of(1_010_000L)),
                Ex.LessThan("id", LiteralValue.Of(1_010_000L + total / 100)));
            var cases = new (string Label, Predicate Filter)[]
            {
                ("id point", Ex.Equal("id", LiteralValue.Of(1_234_567L))),
                ("id 1%", onePct),
                ("id 10%", Ex.And(
                    Ex.GreaterThanOrEqual("id", LiteralValue.Of(1_010_000L)),
                    Ex.LessThan("id", LiteralValue.Of(1_010_000L + total / 10)))),
                ("category (unclustered)", Ex.Equal("category", LiteralValue.Of("cat-07"))),
                ("id 1% AND category", Ex.And(onePct, Ex.Equal("category", LiteralValue.Of("cat-07")))),
            };

            Console.WriteLine(
                $"{RowGroups} row groups x {RowsPerGroup:N0} rows, default options (25 pages per column per row group); " +
                $"{new FileInfo(path).Length:N0} bytes; median of {rounds} alternating rounds after warm-up; {latencyMs} ms added per " +
                (cloud ? "GET, through CoalescingFileReader" : "request") + (mbps > 0 ? $", then {mbps} MB/s." : "."));
            Console.WriteLine("| Case | ms off | ms on | Time | Calls off | on | GETs off | on | Bytes off | on | Rows off | on |");
            Console.WriteLine("|---|---|---|---|---|---|---|---|---|---|---|---|");

            // Unmeasured passes first: the first case measured otherwise runs on tier-0 code
            // (DictionaryPruningAb found the same).
            for (int pass = 0; pass < 5; pass++)
            {
                foreach (var (_, filter) in cases)
                {
                    await ReadAsync(path, filter, pageIndex: false, latencyMs: 0, mbps: 0, cloud);
                    await ReadAsync(path, filter, pageIndex: true, latencyMs: 0, mbps: 0, cloud);
                }
            }

            foreach (var (label, filter) in cases)
            {
                var msOff = new List<double>();
                var msOn = new List<double>();
                var ratios = new List<double>();
                Result a = default, b = default;
                for (int round = -2; round < rounds; round++)
                {
                    bool offFirst = round % 2 == 0;
                    if (offFirst)
                        a = await ReadAsync(path, filter, pageIndex: false, latencyMs, mbps, cloud);
                    b = await ReadAsync(path, filter, pageIndex: true, latencyMs, mbps, cloud);
                    if (!offFirst)
                        a = await ReadAsync(path, filter, pageIndex: false, latencyMs, mbps, cloud);

                    if (round < 0)
                        continue;
                    msOff.Add(a.Ms);
                    msOn.Add(b.Ms);
                    ratios.Add(b.Ms / a.Ms);
                }

                Console.WriteLine(
                    $"| {label} | {Median(msOff):F1} | {Median(msOn):F1} | {(Median(ratios) - 1) * 100:+0;-0;0}% " +
                    $"| {a.Calls} | {b.Calls} | {a.Gets} | {b.Gets} | {a.Bytes:N0} | {b.Bytes:N0} | {a.Rows:N0} | {b.Rows:N0} |");
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }

        return 0;
    }

    private static RecordBatch Build()
    {
        const int rows = RowGroups * RowsPerGroup;
        var random = new Random(433);
        var ids = new Int64Array.Builder();
        var ts = new Int64Array.Builder();
        var categories = new StringArray.Builder();
        var amounts = new DoubleArray.Builder();
        var notes = new StringArray.Builder();
        Span<byte> noteBytes = stackalloc byte[12];
        for (long i = 0; i < rows; i++)
        {
            ids.Append(i);
            ts.Append(1_600_000_000_000_000L + i * 250 + random.Next(250));
            categories.Append("cat-" + random.Next(50).ToString("D2", CultureInfo.InvariantCulture));
            amounts.Append(random.NextDouble());
            random.NextBytes(noteBytes);
            notes.Append(Convert.ToHexString(noteBytes));
        }

        var schema = new Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("ts", Int64Type.Default, false))
            .Field(new Field("category", StringType.Default, false))
            .Field(new Field("amount", DoubleType.Default, false))
            .Field(new Field("note", StringType.Default, false))
            .Build();
        return new RecordBatch(schema,
            [ids.Build(), ts.Build(), categories.Build(), amounts.Build(), notes.Build()], rows);
    }

    private static async Task WriteAsync(RecordBatch batch, string path)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false,
            ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private readonly record struct Result(double Ms, int Calls, int Gets, long Bytes, long Rows);

    /// <summary>
    /// One filtered read with the option set explicitly: every other option is the default, so
    /// only <see cref="ParquetReadOptions.FilterUsePageIndex"/> differs between the sides.
    /// </summary>
    private static async Task<Result> ReadAsync(string path, Predicate filter, bool pageIndex, int latencyMs, double mbps, bool cloud)
    {
        // Direct: every call is one request and pays the delay. Cloud: calls go through the coalescer,
        // and each GET it sends to the transport pays the delay.
        await using var transport = new LatencyCountingFile(new LocalRandomAccessFile(path), latencyMs, mbps);
        await using var calls = cloud ? new LatencyCountingFile(new CoalescingFileReader(transport), 0) : null;
        IRandomAccessFile file = calls ?? transport;
        var options = new ParquetReadOptions { Filter = filter, FilterUsePageIndex = pageIndex };
        var clock = Stopwatch.StartNew();
        long rows = 0;
        using (var reader = new ParquetFileReader(file, ownsFile: false, options))
        {
            await foreach (var batch in reader.ReadAllAsync())
            {
                rows += batch.Length;
                batch.Dispose();
            }
        }

        return new Result(clock.Elapsed.TotalMilliseconds, (calls ?? transport).Requests, transport.Requests,
            transport.Bytes, rows);
    }

    private static double Median(List<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }
}
#endif
