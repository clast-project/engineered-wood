// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.DeltaLake.Table.Stats;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Benchmarks;

/// <summary>
/// What a Delta write's own per-file statistics cost: <see cref="StatsCollector"/> alone over one batch, and an
/// append of that batch to a table with statistics on (the default) and off
/// (<c>delta.dataSkippingNumIndexedCols = 0</c>, which keeps only numRecords).
/// </summary>
/// <remarks>
/// Run with: dotnet run -c Release -f net10.0 -- delta-stats [rounds] [rows]. One column per table, written to a
/// local temp directory (so the appends include the file system), best round of each.
/// </remarks>
internal static class DeltaStatsProfile
{
    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 0 ? int.Parse(args[0], CultureInfo.InvariantCulture) : 10;
        int rows = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 1_000_000;
        string root = Path.Combine(Path.GetTempPath(), "ew-delta-stats-" + Guid.NewGuid().ToString("N"));

        Console.WriteLine($"{rows:N0} rows, one column; best of {rounds} (ms).");
        Console.WriteLine("| Column | Delta stats alone | Append, stats on | Append, stats off |");
        Console.WriteLine("|---|---|---|---|");
        try
        {
            int n = 0;
            foreach (var (name, batch) in Shapes(rows))
            {
                // Warm for 3 s first, to time the code a long-running writer runs. Tiered compilation swaps in
                // optimised code only after a call-counting delay, and a method called once per batch that loops
                // over thousands of chunks runs as an on-stack-replacement compile for a long while: after 300 ms
                // DOUBLE still read 2.1 ms and INT64 with nulls 2.4, against 0.42 and 0.68 at steady state.
                for (var warm = Stopwatch.StartNew(); warm.ElapsedMilliseconds < 3000;)
                    StatsCollector.Collect(batch);

                double collect = double.MaxValue;
                for (int i = 0; i < rounds; i++)
                {
                    var clock = Stopwatch.StartNew();
                    StatsCollector.Collect(batch);
                    collect = Math.Min(collect, clock.Elapsed.TotalMilliseconds);
                }

                double on = await AppendAsync(Path.Combine(root, $"{n}-on"), batch, rounds, null);
                double off = await AppendAsync(Path.Combine(root, $"{n}-off"), batch, rounds,
                    new Dictionary<string, string> { ["delta.dataSkippingNumIndexedCols"] = "0" });
                n++;
                Console.WriteLine($"| {name} | {collect:F2} | {on:F2} | {off:F2} |");
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch (IOException) { }
        }

        return 0;
    }

    private static async Task<double> AppendAsync(
        string path, RecordBatch batch, int rounds, IReadOnlyDictionary<string, string>? configuration)
    {
        Directory.CreateDirectory(path);
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(path), batch.Schema, configuration: configuration);
        double best = double.MaxValue;
        for (int i = -2; i < rounds; i++)
        {
            var clock = Stopwatch.StartNew();
            await table.WriteAsync([batch]);
            double ms = clock.Elapsed.TotalMilliseconds;
            if (i >= 0) best = Math.Min(best, ms);
        }

        return best;
    }

    private static IEnumerable<(string, RecordBatch)> Shapes(int rows)
    {
        var random = new Random(42);

        var i32 = new Int32Array.Builder();
        for (int i = 0; i < rows; i++) i32.Append(random.Next());
        yield return ("int32 random", Batch(i32.Build()));

        var i64 = new Int64Array.Builder();
        for (int i = 0; i < rows; i++) i64.Append(random.NextInt64());
        yield return ("int64 random", Batch(i64.Build()));

        var i64n = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
        {
            if (random.Next(7) == 0) i64n.AppendNull(); else i64n.Append(random.NextInt64());
        }
        yield return ("int64 random, 14% null", Batch(i64n.Build()));

        var f64 = new DoubleArray.Builder();
        for (int i = 0; i < rows; i++) f64.Append(random.NextDouble());
        yield return ("double random", Batch(f64.Build()));

        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (int i = 0; i < rows; i++) ts.Append(start.AddSeconds(random.Next(31_536_000)));
        yield return ("timestamp random", Batch(ts.Build()));

        var s = new StringArray.Builder();
        var sb = new StringBuilder();
        for (int i = 0; i < rows; i++)
        {
            sb.Clear();
            int n = 8 + random.Next(17);
            for (int c = 0; c < n; c++) sb.Append((char)('a' + random.Next(26)));
            s.Append(sb.ToString());
        }
        yield return ("string random, 8-24", Batch(s.Build()));

        var d = new Decimal128Array.Builder(new Decimal128Type(38, 4));
        for (int i = 0; i < rows; i++) d.Append((decimal)random.NextInt64(-1_000_000_000_000, 1_000_000_000_000) / 10_000m);
        yield return ("decimal(38,4)", Batch(d.Build()));
    }

    private static RecordBatch Batch(IArrowArray array) =>
        new(new Apache.Arrow.Schema([new Field("c", array.Data.DataType, array.NullCount > 0)], null), [array], array.Length);
}
#endif
