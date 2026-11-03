// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// The baseline for reading a file's row groups in parallel (#612), and an upper bound on what that can
/// gain. "Serial" is <see cref="ParquetFileReader.ReadAllAsync"/> with <c>MaxConcurrentRowGroups = 1</c>: one
/// row group after another, columns decoded in parallel within each, the read before #612.
/// "MaxConcurrentRowGroups = W" is the real thing, with its memory budget and its serial pruning.
/// "Upper bound, window W" reads the same row groups through
/// <see cref="ParquetFileReader.ReadRowGroupAsync"/>, up to W at once on ONE reader (safe since #629), and
/// consumes them in order. It has no memory budget and no pruning, so it is an upper bound on the speed
/// of the real thing, not a design for it.
/// </summary>
/// <remarks>
/// <para>The file is <see cref="RowGroups"/> row groups of <see cref="RowsPerGroup"/> rows, written with
/// default options: <c>id</c> (int64, sorted), <c>category</c> (50 distinct strings, dictionary-encoded),
/// <c>amount</c> (double), <c>qty</c> (int32), <c>ts</c> (int64) and <c>note</c> (24-character hex string).
/// Read whole, and projected to <c>amount</c> alone.</para>
/// <para>Each read consumes every batch: it sums the first column (rows and checksum must agree between
/// the sides, or the run fails), then spins for the given consumer time per batch, standing in for the
/// caller's own work, which a parallel read can overlap and a serial one cannot.</para>
/// Run with: dotnet run -c Release -f net10.0 -- rowgroup-parallel-ab [rounds] [latencyMs] [MB/s] [consumer ms per batch] [windows, e.g. 2,4,8]
/// </remarks>
internal static class RowGroupParallelAb
{
    private const int RowGroups = 8;
    private const int RowsPerGroup = 131_072;

    private static readonly string[] Narrow = ["amount"];

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 11;
        int latencyMs = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 0;
        double mbps = args.Length > 3 ? double.Parse(args[3], CultureInfo.InvariantCulture) : 0;
        double consumerMs = args.Length > 4 ? double.Parse(args[4], CultureInfo.InvariantCulture) : 0;
        int[] windows = args.Length > 5
            ? [.. args[5].Split(',').Select(w => int.Parse(w, CultureInfo.InvariantCulture))]
            : [2, 4, 8];

        string dir = Path.Combine(Path.GetTempPath(), "ew-rowgroup-parallel-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            string path = Path.Combine(dir, "data.parquet");
            await WriteAsync(path);

            Console.WriteLine(
                $"{RowGroups} row groups x {RowsPerGroup:N0} rows, default options; {new FileInfo(path).Length:N0} bytes; " +
                $"median of {rounds} rounds after warm-up, sides in rotating order; {latencyMs} ms added per request" +
                (mbps > 0 ? $", then {mbps} MB/s" : "") + $"; consumer {consumerMs} ms per batch; " +
                $"{Environment.ProcessorCount} logical cores.");
            Console.WriteLine();

            foreach (var (label, columns) in new[] { ("all 6 columns", (string[]?)null), ("amount only", Narrow) })
            {
                var sides = new List<Side> { new("serial (MaxConcurrentRowGroups = 1)", 0, 1) };
                foreach (int w in windows)
                {
                    sides.Add(new($"MaxConcurrentRowGroups = {w}", 0, w));
                    sides.Add(new($"upper bound, window {w}", w, 0));
                }

                // Warm-up, and the check that every side reads the same rows.
                Result? expected = null;
                for (int pass = 0; pass < 3; pass++)
                {
                    foreach (var side in sides)
                    {
                        var r = await ReadAsync(path, columns, side, 0, 0, 0);
                        expected ??= r;
                        if (r.Rows != expected.Value.Rows || r.Checksum != expected.Value.Checksum)
                            throw new InvalidOperationException(
                                $"{label}, {side.Name}: {r.Rows} rows (checksum {r.Checksum}), expected " +
                                $"{expected.Value.Rows} ({expected.Value.Checksum})");
                    }
                }

                var times = sides.Select(_ => new List<double>()).ToArray();
                var peaks = new long[sides.Count];
                var ratios = sides.Select(_ => new List<double>()).ToArray();
                for (int round = 0; round < rounds; round++)
                {
                    var ms = new double[sides.Count];
                    for (int k = 0; k < sides.Count; k++)
                    {
                        // Rotate the order so that no side always runs first or last.
                        int s = (k + round) % sides.Count;
                        var r = await ReadAsync(path, columns, sides[s], latencyMs, mbps, consumerMs);
                        ms[s] = r.Ms;
                        peaks[s] = Math.Max(peaks[s], r.PeakBytes);
                    }

                    for (int s = 0; s < sides.Count; s++)
                    {
                        times[s].Add(ms[s]);
                        ratios[s].Add(ms[0] / ms[s]);
                    }
                }

                Console.WriteLine($"**{label}**");
                Console.WriteLine();
                Console.WriteLine("| Read | ms | Speed-up vs serial | Peak budget MiB |");
                Console.WriteLine("|---|---|---|---|");
                for (int s = 0; s < sides.Count; s++)
                {
                    string peak = sides[s].Option > 1 ? (peaks[s] / 1048576.0).ToString("F1", CultureInfo.InvariantCulture) : "";
                    Console.WriteLine($"| {sides[s].Name} | {Median(times[s]):F1} | {Median(ratios[s]):F2}x | {peak} |");
                }
                Console.WriteLine();
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }

        return 0;
    }

    private static async Task WriteAsync(string path)
    {
        var random = new Random(612);
        var schema = new Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("category", StringType.Default, false))
            .Field(new Field("amount", DoubleType.Default, false))
            .Field(new Field("qty", Int32Type.Default, false))
            .Field(new Field("ts", Int64Type.Default, false))
            .Field(new Field("note", StringType.Default, false))
            .Build();

        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false,
            ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup });
        var noteBytes = new byte[12];
        for (int g = 0; g < RowGroups; g++)
        {
            var ids = new Int64Array.Builder();
            var categories = new StringArray.Builder();
            var amounts = new DoubleArray.Builder();
            var qtys = new Int32Array.Builder();
            var tss = new Int64Array.Builder();
            var notes = new StringArray.Builder();
            for (int i = 0; i < RowsPerGroup; i++)
            {
                long id = (long)g * RowsPerGroup + i;
                ids.Append(id);
                categories.Append("cat-" + random.Next(50).ToString("D2", CultureInfo.InvariantCulture));
                amounts.Append(random.NextDouble() * 1000);
                qtys.Append(random.Next(1, 100));
                tss.Append(1_700_000_000_000_000L + id * 1_000 + random.Next(1_000));
                random.NextBytes(noteBytes);
                notes.Append(Convert.ToHexString(noteBytes));
            }

            using var batch = new RecordBatch(schema,
                [ids.Build(), categories.Build(), amounts.Build(), qtys.Build(), tss.Build(), notes.Build()],
                RowsPerGroup);
            await writer.WriteRowGroupAsync(batch);
        }

        await writer.CloseAsync();
    }

    /// <summary>One way to read: <paramref name="Manual"/> above 0 is the hand-rolled window (the upper
    /// bound); otherwise <see cref="ParquetReadOptions.MaxConcurrentRowGroups"/> is <paramref name="Option"/>.</summary>
    private sealed record Side(string Name, int Manual, int Option);

    private readonly record struct Result(double Ms, long Rows, double Checksum, long PeakBytes);

    private static async Task<Result> ReadAsync(
        string path, string[]? columns, Side side, int latencyMs, double mbps, double consumerMs)
    {
        await using var file = new LatencyCountingFile(new LocalRandomAccessFile(path), latencyMs, mbps);
        var clock = Stopwatch.StartNew();
        long rows = 0;
        double checksum = 0;
        long peak = 0;
        int window = side.Manual;
        var options = new ParquetReadOptions { MaxConcurrentRowGroups = Math.Max(1, side.Option) };
        using (var reader = new ParquetFileReader(file, ownsFile: false, options))
        {
            if (window == 0)
            {
                await foreach (var batch in reader.ReadAllAsync(columns))
                    Consume(batch, consumerMs, ref rows, ref checksum);
                peak = reader.LastScanBudget?.Peak ?? 0;
            }
            else
            {
                int groups = (await reader.ReadMetadataAsync()).RowGroups.Count;
                var pending = new Queue<Task<RecordBatch>>();
                int next = 0;
                try
                {
                    while (next < groups || pending.Count > 0)
                    {
                        while (next < groups && pending.Count < window)
                        {
                            int g = next++;
                            // Task.Run because a local read completes synchronously: without it the reads
                            // would run one after another on this thread.
                            pending.Enqueue(Task.Run(() => reader.ReadRowGroupAsync(g, columns).AsTask()));
                        }

                        Consume(await pending.Dequeue(), consumerMs, ref rows, ref checksum);
                    }
                }
                finally
                {
                    while (pending.Count > 0)
                    {
                        try
                        {
                            (await pending.Dequeue()).Dispose();
                        }
                        catch
                        {
                            // The first failure is the one that propagates.
                        }
                    }
                }
            }
        }

        return new Result(clock.Elapsed.TotalMilliseconds, rows, checksum, peak);
    }

    private static void Consume(RecordBatch batch, double consumerMs, ref long rows, ref double checksum)
    {
        using (batch)
        {
            rows += batch.Length;
            switch (batch.Column(0))
            {
                case Int64Array longs:
                    for (int i = 0; i < longs.Length; i++)
                        checksum += longs.GetValue(i)!.Value;
                    break;
                case DoubleArray doubles:
                    for (int i = 0; i < doubles.Length; i++)
                        checksum += doubles.GetValue(i)!.Value;
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected first column {batch.Column(0).GetType().Name}.");
            }

            if (consumerMs > 0)
            {
                var spin = Stopwatch.StartNew();
                while (spin.Elapsed.TotalMilliseconds < consumerMs)
                    Thread.SpinWait(64);
            }
        }
    }

    private static double Median(List<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }
}
#endif
