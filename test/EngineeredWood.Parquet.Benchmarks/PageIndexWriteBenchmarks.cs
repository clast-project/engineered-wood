// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.IO;
using EngineeredWood.Parquet;
using Field = Apache.Arrow.Field;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// The cost of <see cref="ParquetWriteOptions.WritePageIndex"/>: the same write with and without
/// page indexes, over the schemas and page layouts that decide it (doc/parquet-page-index.md, W-6).
/// </summary>
/// <remarks>
/// <para>Writes into <see cref="CountingSequentialFile"/>, which discards the bytes, so neither disk
/// I/O nor a growing buffer is in the measurement — only the writer is.</para>
/// <para>Two page sizes: the default (1 MiB), which is what the default-on decision is about, and
/// 8 KiB, a worst case with hundreds of pages per chunk, where per-page work is most visible.</para>
/// <para>Peak working set is not something BenchmarkDotNet measures; <see cref="PageIndexOverhead"/>
/// takes it in a fresh process per configuration, along with file size.</para>
/// Run with: dotnet run -c Release -f net10.0 -- --filter "*PageIndexWriteBenchmarks*"
/// </remarks>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 12)]
public class PageIndexWriteBenchmarks
{
    [Params("plain", "dictionary", "strings", "nested")]
    public string Schema { get; set; } = null!;

    [Params(DataPageVersion.V1, DataPageVersion.V2)]
    public DataPageVersion PageVersion { get; set; }

    [Params(PageIndexWorkload.DefaultPageSize, PageIndexWorkload.SmallPageSize)]
    public int DataPageSize { get; set; }

    private RecordBatch _batch = null!;
    private ParquetWriteOptions _without = null!;
    private ParquetWriteOptions _with = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _batch = PageIndexWorkload.Build(Schema);
        _without = PageIndexWorkload.Options(PageVersion, DataPageSize, writePageIndex: false);
        _with = PageIndexWorkload.Options(PageVersion, DataPageSize, writePageIndex: true);
    }

    [Benchmark(Baseline = true)]
    public Task<long> WithoutIndex() => PageIndexWorkload.WriteAsync(_batch, _without);

    [Benchmark]
    public Task<long> WithIndex() => PageIndexWorkload.WriteAsync(_batch, _with);
}

/// <summary>The data and write the page-index measurements share.</summary>
internal static class PageIndexWorkload
{
    public const int DefaultPageSize = 1024 * 1024;
    public const int SmallPageSize = 8 * 1024;
    public const int Rows = 500_000;

    public static ParquetWriteOptions Options(DataPageVersion version, int dataPageSize, bool writePageIndex) =>
        ParquetWriteOptions.Default with
        {
            DataPageVersion = version,
            DataPageSize = dataPageSize,
            WritePageIndex = writePageIndex,
        };

    /// <summary>Writes one row group and returns the file's size.</summary>
    public static async Task<long> WriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        var file = new CountingSequentialFile();
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch).ConfigureAwait(false);
            await writer.CloseAsync().ConfigureAwait(false);
        }

        return file.Position;
    }

    /// <summary>
    /// <list type="bullet">
    /// <item><b>plain</b>: high-cardinality numbers, which the dictionary declines, so page bounds come
    /// from the value scans.</item>
    /// <item><b>dictionary</b>: low-cardinality numbers and strings, so page bounds come from the
    /// dictionary entries each page references.</item>
    /// <item><b>strings</b>: unique strings of 20-200 bytes, most over the 64-byte truncation limit.</item>
    /// <item><b>nested</b>: a list and a struct, whose pages are cut on record boundaries.</item>
    /// </list>
    /// </summary>
    public static RecordBatch Build(string schema)
    {
        // "<schema>:<field>" is one column of a schema, from the same data, to attribute its cost.
        if (schema.IndexOf(':') is var colon and > 0)
        {
            var whole = Build(schema.Substring(0, colon));
            int index = whole.Schema.GetFieldIndex(schema.Substring(colon + 1));
            return new RecordBatch(
                new Apache.Arrow.Schema([whole.Schema.GetFieldByIndex(index)], null), [whole.Column(index)], Rows);
        }

        var rng = new Random(4);
        var fields = new List<Field>();
        var arrays = new List<IArrowArray>();

        void Add(string name, IArrowArray array, bool nullable)
        {
            fields.Add(new Field(name, array.Data.DataType, nullable));
            arrays.Add(array);
        }

        switch (schema)
        {
            case "plain":
            {
                var ids = new Int64Array.Builder();
                var values = new DoubleArray.Builder();
                var counts = new Int32Array.Builder();
                var times = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
                var epoch = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
                for (int i = 0; i < Rows; i++)
                {
                    ids.Append(i);
                    if (i % 20 == 0) values.AppendNull(); else values.Append(rng.NextDouble() * 1e6);
                    counts.Append(rng.Next());
                    times.Append(epoch.AddMilliseconds(i * 37L));
                }

                Add("id", ids.Build(), false);
                Add("value", values.Build(), true);
                Add("count", counts.Build(), false);
                Add("time", times.Build(), false);
                break;
            }
            case "dictionary":
            {
                var category = new Int32Array.Builder();
                var region = new StringArray.Builder();
                var bucket = new Int64Array.Builder();
                var score = new DoubleArray.Builder();
                for (int i = 0; i < Rows; i++)
                {
                    category.Append(rng.Next(100));
                    if (i % 25 == 0) region.AppendNull(); else region.Append("region-" + rng.Next(50));
                    bucket.Append(rng.Next(1000) * 1_000_000L);
                    score.Append(rng.Next(200) / 4.0);
                }

                Add("category", category.Build(), false);
                Add("region", region.Build(), true);
                Add("bucket", bucket.Build(), false);
                Add("score", score.Build(), false);
                break;
            }
            case "strings":
            {
                const string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-_/";
                var builders = new[] { new StringArray.Builder(), new StringArray.Builder(), new StringArray.Builder() };
                var chars = new char[200];
                for (int i = 0; i < Rows; i++)
                {
                    foreach (var builder in builders)
                    {
                        int length = rng.Next(20, 200);
                        for (int c = 0; c < length; c++)
                            chars[c] = alphabet[rng.Next(alphabet.Length)];
                        builder.Append(new string(chars, 0, length));
                    }
                }

                for (int c = 0; c < builders.Length; c++)
                    Add($"s{c}", builders[c].Build(), false);
                break;
            }
            case "nested":
            {
                var list = new ListArray.Builder(Int64Type.Default);
                var listValues = (Int64Array.Builder)list.ValueBuilder;
                var a = new Int32Array.Builder();
                var b = new StringArray.Builder();
                var flat = new Int64Array.Builder();
                for (int i = 0; i < Rows; i++)
                {
                    if (i % 30 == 0) list.AppendNull();
                    else
                    {
                        list.Append();
                        int n = rng.Next(0, 10);
                        for (int j = 0; j < n; j++)
                            listValues.Append(rng.Next());
                    }

                    a.Append(rng.Next());
                    b.Append("tag-" + rng.Next(40));
                    flat.Append(i);
                }

                var structType = new StructType([new Field("a", Int32Type.Default, false), new Field("b", StringType.Default, false)]);
                Add("list", list.Build(), true);
                Add("struct", new StructArray(structType, Rows, [a.Build(), b.Build()], ArrowBuffer.Empty, 0), true);
                Add("id", flat.Build(), false);
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(schema), schema, null);
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, null), arrays, Rows);
    }
}

/// <summary>An output that keeps only the count of bytes written.</summary>
internal sealed class CountingSequentialFile : ISequentialFile
{
    public long Position { get; private set; }

    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        Position += data.Length;
        return default;
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => default;

    public ValueTask DisposeAsync() => default;

    public void Dispose()
    {
    }
}

#if NET8_0_OR_GREATER // Process APIs the harness uses; the measurement is a net10.0 one.
/// <summary>
/// Peak working set, duration and file size with and without page indexes, each configuration in a
/// fresh process so that one's peak does not hide another's.
/// </summary>
/// <remarks>Run with: dotnet run -c Release -f net10.0 -- pageindex-overhead</remarks>
internal static class PageIndexOverhead
{
    private const int Repetitions = 5;

    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length >= 1 && args[0] == "pageindex-ab")
            return await RunInterleavedAsync(
                args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 25,
                args.Length > 2 ? args[2..] : ["plain", "dictionary", "strings", "nested"]);

        if (args.Length == 5 && args[0] == "pageindex-child")
            return await RunChildAsync(args[1], Enum.Parse<DataPageVersion>(args[2]), int.Parse(args[3], CultureInfo.InvariantCulture), bool.Parse(args[4]));

        // Peak working set moves ±20% from run to run with GC timing alone, so each configuration runs
        // several times, off and on alternately, and the medians are compared.
        int repeats = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 5;
        Console.WriteLine($"Medians of {repeats} fresh processes per configuration.");
        Console.WriteLine("| Schema | Pages | Page size | Peak WS off (MB) | Peak WS on (MB) | Δ | File off (bytes) | File on (bytes) | Δ | Best ms off | Best ms on |");
        Console.WriteLine("|---|---|---|---|---|---|---|---|---|---|---|");
        foreach (string schema in new[] { "plain", "dictionary", "strings", "nested" })
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (int pageSize in new[] { PageIndexWorkload.DefaultPageSize, PageIndexWorkload.SmallPageSize })
        {
            var offRuns = new List<Measurement>();
            var onRuns = new List<Measurement>();
            for (int i = 0; i < repeats; i++)
            {
                offRuns.Add(Child(schema, version, pageSize, index: false));
                onRuns.Add(Child(schema, version, pageSize, index: true));
            }

            var off = new Measurement(Median(offRuns.Select(m => m.PeakMb).ToList()), offRuns[0].FileBytes, Median(offRuns.Select(m => m.BestMs).ToList()));
            var on = new Measurement(Median(onRuns.Select(m => m.PeakMb).ToList()), onRuns[0].FileBytes, Median(onRuns.Select(m => m.BestMs).ToList()));
            Console.WriteLine(
                $"| {schema} | {version} | {(pageSize == PageIndexWorkload.DefaultPageSize ? "1 MiB" : "8 KiB")} " +
                $"| {off.PeakMb:F1} | {on.PeakMb:F1} | {Percent(on.PeakMb, off.PeakMb)} " +
                $"| {off.FileBytes:N0} | {on.FileBytes:N0} | {Percent(on.FileBytes, off.FileBytes)} " +
                $"| {off.BestMs:F0} | {on.BestMs:F0} |");
        }

        return 0;
    }

    /// <summary>
    /// Duration and allocation ratios, with and without page indexes, from writes that alternate
    /// round by round in one process. BenchmarkDotNet runs the two methods one after the other, so
    /// load that comes and goes between them skews its ratio; alternating spreads any drift over both.
    /// Reported: the median of the per-round time ratios, and the medians of each side's allocations.
    /// </summary>
    private static async Task<int> RunInterleavedAsync(int rounds, string[] schemas)
    {
        Console.WriteLine($"Median of {rounds} alternating rounds, after 3 warm-up rounds.");
        Console.WriteLine("| Schema | Pages | Page size | ms off | ms on | Time | Allocated off (MB) | on (MB) | Allocated |");
        Console.WriteLine("|---|---|---|---|---|---|---|---|---|");
        foreach (string schema in schemas)
        {
            var batch = PageIndexWorkload.Build(schema);
            foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
            foreach (int pageSize in new[] { PageIndexWorkload.DefaultPageSize, PageIndexWorkload.SmallPageSize })
            {
                var off = PageIndexWorkload.Options(version, pageSize, writePageIndex: false);
                var on = PageIndexWorkload.Options(version, pageSize, writePageIndex: true);
                var msOff = new List<double>();
                var msOn = new List<double>();
                var ratios = new List<double>();
                var allocOff = new List<double>();
                var allocOn = new List<double>();

                for (int round = -3; round < rounds; round++)
                {
                    // Alternate which side goes first, so neither always follows the other's garbage.
                    bool offFirst = round % 2 == 0;
                    var (tOff, aOff) = offFirst ? await TimeAsync(batch, off) : default;
                    var (tOn, aOn) = await TimeAsync(batch, on);
                    if (!offFirst)
                        (tOff, aOff) = await TimeAsync(batch, off);

                    if (round < 0)
                        continue;
                    msOff.Add(tOff);
                    msOn.Add(tOn);
                    ratios.Add(tOn / tOff);
                    allocOff.Add(aOff);
                    allocOn.Add(aOn);
                }

                double medianAllocOff = Median(allocOff);
                double medianAllocOn = Median(allocOn);

                Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"| {schema} | {version} | {(pageSize == PageIndexWorkload.DefaultPageSize ? "1 MiB" : "8 KiB")} " +
                    $"| {Median(msOff):F1} | {Median(msOn):F1} | {(Median(ratios) - 1) * 100:+0.0;-0.0}% " +
                    $"| {medianAllocOff / 1048576.0:F2} | {medianAllocOn / 1048576.0:F2} | {(medianAllocOn - medianAllocOff) * 100.0 / medianAllocOff:+0.00;-0.00}% |"));
            }
        }

        return 0;
    }

    private static async Task<(double Ms, long Allocated)> TimeAsync(Apache.Arrow.RecordBatch batch, ParquetWriteOptions options)
    {
        long before = GC.GetTotalAllocatedBytes(precise: true);
        var watch = Stopwatch.StartNew();
        await PageIndexWorkload.WriteAsync(batch, options).ConfigureAwait(false);
        double ms = watch.Elapsed.TotalMilliseconds;
        return (ms, GC.GetTotalAllocatedBytes(precise: true) - before);
    }

    private static double Median(List<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted.Count % 2 == 1 ? sorted[sorted.Count / 2] : (sorted[sorted.Count / 2 - 1] + sorted[sorted.Count / 2]) / 2;
    }

    private static string Percent(double value, double baseline) =>
        $"{(value - baseline) / baseline * 100:+0.00;-0.00}%";

    private sealed record Measurement(double PeakMb, long FileBytes, double BestMs);

    private static Measurement Child(string schema, DataPageVersion version, int pageSize, bool index)
    {
        string host = Environment.ProcessPath!;
        var start = new ProcessStartInfo(host)
        {
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };

        // Started as `dotnet app.dll` (or with no apphost), this process is the dotnet host, not the app;
        // the child has to be told which app to run.
        if (Path.GetFileNameWithoutExtension(host).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            start.ArgumentList.Add(typeof(PageIndexOverhead).Assembly.Location);

        foreach (string arg in new[] { "pageindex-child", schema, version.ToString(), pageSize.ToString(CultureInfo.InvariantCulture), index.ToString() })
            start.ArgumentList.Add(arg);

        using var process = Process.Start(start)!;
        string output = process.StandardOutput.ReadToEnd().Trim();
        process.WaitForExit();
        var parts = output.Split(' ');
        return new Measurement(
            double.Parse(parts[0], CultureInfo.InvariantCulture),
            long.Parse(parts[1], CultureInfo.InvariantCulture),
            double.Parse(parts[2], CultureInfo.InvariantCulture));
    }

    private static async Task<int> RunChildAsync(string schema, DataPageVersion version, int pageSize, bool index)
    {
        var batch = PageIndexWorkload.Build(schema);
        var options = PageIndexWorkload.Options(version, pageSize, index);

        long size = 0;
        double best = double.MaxValue;
        for (int i = 0; i < Repetitions; i++)
        {
            var watch = Stopwatch.StartNew();
            size = await PageIndexWorkload.WriteAsync(batch, options).ConfigureAwait(false);
            best = Math.Min(best, watch.Elapsed.TotalMilliseconds);
        }

        using var self = Process.GetCurrentProcess();
        self.Refresh();
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"{self.PeakWorkingSet64 / (1024.0 * 1024.0):F2} {size} {best:F2}"));
        return 0;
    }
}
#endif
