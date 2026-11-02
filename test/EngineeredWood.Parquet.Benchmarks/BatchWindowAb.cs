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
/// What fetching the pages of many batches in one request saves (#622). A read that returns several
/// batches from one row group used to fetch each batch's pages when it reached that batch, one
/// request after another; it now fetches them a window at a time. Reports the median time of
/// alternating rounds of the two, and the requests, GETs and bytes of each.
/// </summary>
/// <remarks>
/// <para>"Per batch" sets <c>BatchPageWindowBudgetBytes</c> to one byte, which makes every window
/// one batch: the requests and bytes of the read before #622, made by the same binary.</para>
/// <para>The file is one row group of 1,000,000 rows written with default options (20,000-row pages),
/// <c>id</c> sorted. The cases: ranges between ten deleted runs of 50,000 rows (11 ranges); 50 ranges
/// of 1,000 rows, one per 20,000; a page-index filter narrowing to five <c>id</c> intervals; a
/// 65,536-row <see cref="ParquetReadOptions.BatchSize"/>; a 4 MiB
/// <see cref="ParquetReadOptions.MaxBatchByteSize"/>; and the whole row group in one batch, which must
/// not change.</para>
/// <para><c>headerscan</c> as the seventh argument measures #625 instead: the file is written without a page
/// index, so every page map comes from a header scan, and "per batch" is a scan budget of one byte (each chunk
/// read in its own request and let go, its pages fetched again by the windows), the read before #625.</para>
/// Run with: dotnet run -c Release -f net10.0 -- batchwindow-ab [rounds] [latencyMs] [direct|cloud] [MB/s] [window MiB, 0 default] [headerscan]
/// </remarks>
internal static class BatchWindowAb
{
    private const int Rows = 1_000_000;

    private sealed record Case(
        string Label, ParquetReadOptions Options, Func<ParquetFileReader, IAsyncEnumerable<RecordBatch>> Read);

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 11;
        int latencyMs = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 0;
        bool cloud = args.Length > 3 && args[3] == "cloud";
        double mbps = args.Length > 4 ? double.Parse(args[4], CultureInfo.InvariantCulture) : 0;
        long windowMiB = args.Length > 5 ? long.Parse(args[5], CultureInfo.InvariantCulture) : 0;
        s_windowBytes = windowMiB > 0 ? windowMiB << 20 : null;
        s_headerScan = args.Length > 6 && args[6] == "headerscan";
        string dir = Path.Combine(Path.GetTempPath(), "ew-batchwindow-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);

        try
        {
            string path = Path.Combine(dir, "data.parquet");
            await WriteAsync(Build(), path);

            RowRange[] runs = [.. Enumerable.Range(0, 11).Select(k =>
                new RowRange(k == 0 ? 0 : k * 100_000 - 25_000, k == 10 ? Rows : k * 100_000 + 25_000))];
            RowRange[] small = [.. Enumerable.Range(0, 50).Select(k => new RowRange(k * 20_000 + 5_000, k * 20_000 + 6_000))];
            var intervals = Ex.Or(
                Enumerable.Range(0, 5).Select(k => (Predicate)Ex.And(
                    Ex.GreaterThanOrEqual("id", LiteralValue.Of(k * 200_000L + 50_000)),
                    Ex.LessThan("id", LiteralValue.Of(k * 200_000L + 90_000)))).ToArray());

            var cases = new Case[]
            {
                new("11 ranges (ten 50K runs out)", ParquetReadOptions.Default, r => r.ReadRowRangesAsync(0, runs)),
                new("50 ranges of 1K", ParquetReadOptions.Default, r => r.ReadRowRangesAsync(0, small)),
                new("FilterUsePageIndex, 5 intervals", new ParquetReadOptions { Filter = intervals }, r => r.ReadAllAsync()),
                new("BatchSize 65,536", new ParquetReadOptions { BatchSize = 65_536 }, r => r.ReadAllAsync()),
                new("MaxBatchByteSize 4 MiB", new ParquetReadOptions { MaxBatchByteSize = 4L << 20 }, r => r.ReadAllAsync()),
                new("whole row group, one batch", ParquetReadOptions.Default, r => r.ReadAllAsync()),
            };

            Console.WriteLine(
                $"1 row group x {Rows:N0} rows, default options; {new FileInfo(path).Length:N0} bytes; median of {rounds} " +
                $"alternating rounds after warm-up; {latencyMs} ms added per " +
                (cloud ? "GET, through CoalescingFileReader" : "request") + (mbps > 0 ? $", then {mbps} MB/s." : "."));
            Console.WriteLine("| Case | ms per batch | ms windowed | Time | Calls per batch | windowed | GETs per batch | windowed | Bytes per batch | windowed | Batches |");
            Console.WriteLine("|---|---|---|---|---|---|---|---|---|---|---|");

            for (int pass = 0; pass < 3; pass++)
            {
                foreach (var c in cases)
                {
                    await ReadAsync(path, c, windowed: false, 0, 0, cloud);
                    await ReadAsync(path, c, windowed: true, 0, 0, cloud);
                }
            }

            foreach (var c in cases)
            {
                var msOld = new List<double>();
                var msNew = new List<double>();
                var ratios = new List<double>();
                Result a = default, b = default;
                for (int round = -1; round < rounds; round++)
                {
                    bool oldFirst = round % 2 == 0;
                    if (oldFirst)
                        a = await ReadAsync(path, c, windowed: false, latencyMs, mbps, cloud);
                    b = await ReadAsync(path, c, windowed: true, latencyMs, mbps, cloud);
                    if (!oldFirst)
                        a = await ReadAsync(path, c, windowed: false, latencyMs, mbps, cloud);

                    if (a.Rows != b.Rows || a.Sum != b.Sum)
                        throw new InvalidOperationException($"{c.Label}: {a.Rows} rows (sum {a.Sum}) per batch, {b.Rows} (sum {b.Sum}) windowed");
                    if (round < 0)
                        continue;
                    msOld.Add(a.Ms);
                    msNew.Add(b.Ms);
                    ratios.Add(b.Ms / a.Ms);
                }

                Console.WriteLine(
                    $"| {c.Label} | {Median(msOld):F1} | {Median(msNew):F1} | {(Median(ratios) - 1) * 100:+0;-0;0}% " +
                    $"| {a.Calls} | {b.Calls} | {a.Gets} | {b.Gets} | {a.Bytes:N0} | {b.Bytes:N0} | {b.Batches} |");
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
        var random = new Random(622);
        var ids = new Int64Array.Builder();
        var categories = new StringArray.Builder();
        var amounts = new DoubleArray.Builder();
        var notes = new StringArray.Builder();
        Span<byte> noteBytes = stackalloc byte[12];
        for (long i = 0; i < Rows; i++)
        {
            ids.Append(i);
            categories.Append("cat-" + random.Next(50).ToString("D2", CultureInfo.InvariantCulture));
            amounts.Append(random.NextDouble());
            random.NextBytes(noteBytes);
            notes.Append(Convert.ToHexString(noteBytes));
        }

        var schema = new Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("category", StringType.Default, false))
            .Field(new Field("amount", DoubleType.Default, false))
            .Field(new Field("note", StringType.Default, false))
            .Build();
        return new RecordBatch(schema, [ids.Build(), categories.Build(), amounts.Build(), notes.Build()], Rows);
    }

    private static async Task WriteAsync(RecordBatch batch, string path)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false,
            ParquetWriteOptions.Default with { RowGroupMaxRows = Rows, WritePageIndex = !s_headerScan });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private readonly record struct Result(double Ms, int Calls, int Gets, long Bytes, long Rows, long Sum, int Batches);

    /// <summary>The windowed side's budget, when not the default.</summary>
    private static long? s_windowBytes;

    /// <summary>Whether this run measures the header scan (#625) rather than the windows (#622).</summary>
    private static bool s_headerScan;

    private static async Task<Result> ReadAsync(string path, Case c, bool windowed, int latencyMs, double mbps, bool cloud)
    {
        await using var transport = new LatencyCountingFile(new LocalRandomAccessFile(path), latencyMs, mbps);
        await using var calls = cloud ? new LatencyCountingFile(new CoalescingFileReader(transport), 0) : null;
        IRandomAccessFile file = calls ?? transport;
        var clock = Stopwatch.StartNew();
        long rows = 0, sum = 0;
        int batches = 0;
        using (var reader = new ParquetFileReader(file, ownsFile: false, c.Options))
        {
            // In headerscan mode both sides use the same windows, and only the scan budget differs.
            if (s_headerScan && !windowed)
                reader.HeaderScanBudgetBytes = 1;
            else if (!s_headerScan && !windowed)
                reader.BatchPageWindowBudgetBytes = 1;
            if ((s_headerScan || windowed) && s_windowBytes is { } budget)
                reader.BatchPageWindowBudgetBytes = budget;
            await foreach (var batch in c.Read(reader))
            {
                rows += batch.Length;
                batches++;
                var id = (Int64Array)batch.Column(0);
                for (int i = 0; i < id.Length; i++)
                    sum += id.GetValue(i)!.Value;
                batch.Dispose();
            }
        }

        return new Result(clock.Elapsed.TotalMilliseconds, (calls ?? transport).Requests, transport.Requests,
            transport.Bytes, rows, sum, batches);
    }

    private static double Median(List<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }
}
#endif
