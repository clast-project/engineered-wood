// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// What <see cref="ParquetWriteOptions.DataPageRowCountLimit"/> costs, measured to decide whether it can
/// default to 20,000 rows: write time, file size, data pages and full-read time, with and without it,
/// over the page-index workloads (<see cref="PageIndexWorkload"/>).
/// </summary>
/// <remarks>
/// Writes are timed into <see cref="CountingSequentialFile"/>, which discards the bytes, so only the
/// writer is measured. Reads are of a file on disk. Both report the median of alternating rounds after
/// three unmeasured ones, and the median of the per-round ratios.
/// Run with: dotnet run -c Release -f net10.0 -- rowcap-ab [rounds] [limit] [schema...]
/// </remarks>
internal static class RowCountLimitAb
{
    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 15;
        int limit = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 20_000;
        string[] schemas = args.Length > 3 ? args[3..] : ["plain", "dictionary", "strings", "nested"];
        string dir = Path.Combine(Path.GetTempPath(), "ew-rowcap-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);

        Console.WriteLine(
            $"{PageIndexWorkload.Rows:N0} rows, one row group, default options with no limit vs a limit of {limit:N0} rows; " +
            $"median of {rounds} alternating rounds.");
        Console.WriteLine("| Schema | Pages off | on | Bytes off | on | Size | Write ms off | on | Write | Read ms off | on | Read |");
        Console.WriteLine("|---|---|---|---|---|---|---|---|---|---|---|---|");
        try
        {
            foreach (string schema in schemas)
            {
                var batch = PageIndexWorkload.Build(schema);
                // Both sides explicit: the limit defaults to 20,000 since #429, so Default is the capped side.
                var off = ParquetWriteOptions.Default with { DataPageRowCountLimit = null };
                var on = off with { DataPageRowCountLimit = limit };
                string offPath = Path.Combine(dir, schema + "-off.parquet");
                string onPath = Path.Combine(dir, schema + "-on.parquet");
                await WriteFileAsync(batch, off, offPath);
                await WriteFileAsync(batch, on, onPath);

                var write = await AlternateAsync(rounds,
                    () => TimeAsync(() => PageIndexWorkload.WriteAsync(batch, off)),
                    () => TimeAsync(() => PageIndexWorkload.WriteAsync(batch, on)));
                var read = await AlternateAsync(rounds, () => TimeAsync(() => ReadAsync(offPath)), () => TimeAsync(() => ReadAsync(onPath)));

                long offBytes = new FileInfo(offPath).Length, onBytes = new FileInfo(onPath).Length;
                Console.WriteLine(
                    $"| {schema} | {await PagesAsync(offPath)} | {await PagesAsync(onPath)} " +
                    $"| {offBytes:N0} | {onBytes:N0} | {Delta((double)onBytes / offBytes)} " +
                    $"| {write.Off:F1} | {write.On:F1} | {Delta(write.Ratio)} " +
                    $"| {read.Off:F1} | {read.On:F1} | {Delta(read.Ratio)} |");
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }

        return 0;
    }

    private static string Delta(double ratio) =>
        ((ratio - 1) * 100).ToString("+0.0;-0.0;0.0", CultureInfo.InvariantCulture) + "%";

    private static async Task<(double Off, double On, double Ratio)> AlternateAsync(
        int rounds, Func<Task<double>> off, Func<Task<double>> on)
    {
        var offs = new List<double>();
        var ons = new List<double>();
        var ratios = new List<double>();
        for (int round = -3; round < rounds; round++)
        {
            double a, b;
            if (round % 2 == 0)
            {
                a = await off();
                b = await on();
            }
            else
            {
                b = await on();
                a = await off();
            }

            if (round < 0)
                continue;
            offs.Add(a);
            ons.Add(b);
            ratios.Add(b / a);
        }

        return (Median(offs), Median(ons), Median(ratios));
    }

    private static async Task<double> TimeAsync(Func<Task> action)
    {
        var clock = Stopwatch.StartNew();
        await action();
        return clock.Elapsed.TotalMilliseconds;
    }

    private static async Task WriteFileAsync(RecordBatch batch, ParquetWriteOptions options, string path)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private static async Task ReadAsync(string path)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
        await foreach (var batch in reader.ReadAllAsync())
            batch.Dispose();
    }

    private static async Task<int> PagesAsync(string path)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
        int pages = 0;
        var metadata = await reader.ReadMetadataAsync();
        for (int g = 0; g < metadata.RowGroups.Count; g++)
            pages += (await reader.ReadPageIndexAsync(g)).Sum(ix => ix.OffsetIndex!.PageLocations.Count);
        return pages;
    }

    private static double Median(List<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }
}
#endif
