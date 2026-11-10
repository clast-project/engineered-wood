// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// #502 phase 4b A/B: a selected read whose predicate is over the dictionary-encoded <c>category</c>, asked row by
/// row (<c>rows</c>) against the same predicate marked <see cref="RowPredicate.ValueWise"/>, so the reader answers
/// each chunk from its dictionary (<c>dict</c>). Over <see cref="RowFilterAb"/>'s file (4 x 500,000 rows, 50
/// categories): <c>category</c> filter-only, projecting <c>id, amount</c> or nothing (positions only, as a DELETE
/// reads), or returned too (<c>id, category</c>, phase 4d). Each in one pass and in two.
/// </summary>
/// <remarks>Run with: dotnet run -c Release -f net10.0 -- dictanswer-ab [rounds]</remarks>
internal static class DictionaryAnswerAb
{
    private static readonly ArrowRowEvaluator Evaluator = new();

    private sealed record Case(string Label, Predicate Filter, string[] Projection);

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 9;
        string dir = Path.Combine(Path.GetTempPath(), "ew-dictanswer-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            string path = Path.Combine(dir, "data.parquet");
            await RowFilterAb.WriteAsync(RowFilterAb.Build(), path);

            var cases = new List<Case>();
            foreach (var (projName, projection) in new[] { ("id, amount", new[] { "id", "amount" }), ("positions", System.Array.Empty<string>()), ("id, category", new[] { "id", "category" }) })
            {
                cases.Add(new Case($"= 2% {projName}", Ex.Equal("category", LiteralValue.Of("cat-07")), projection));
                cases.Add(new Case($"IN 5 (10%) {projName}", Ex.In("category",
                    LiteralValue.Of("cat-01"), LiteralValue.Of("cat-11"), LiteralValue.Of("cat-21"), LiteralValue.Of("cat-31"), LiteralValue.Of("cat-41")),
                    projection));
                cases.Add(new Case($"<> 98% {projName}", Ex.NotEqual("category", LiteralValue.Of("cat-07")), projection));
            }

            var variants = new (bool Dict, bool TwoPass)[] { (false, false), (true, false), (false, true), (true, true) };
            for (int warm = 0; warm < 3; warm++)
            {
                foreach (var c in cases)
                {
                    foreach (var v in variants)
                        await RunOnceAsync(path, c, v.Dict, v.TwoPass);
                }
            }

            Console.WriteLine($"{new FileInfo(path).Length:N0} bytes; median of {rounds} rounds, order rotated; {Environment.ProcessorCount} logical processors.");
            Console.WriteLine();
            Console.WriteLine("| Case | Kept | rows ms | dict ms | dict vs rows | rows MB alloc | dict MB alloc | 2-pass rows ms | 2-pass dict ms | dict vs rows |");
            Console.WriteLine("|---|---|---|---|---|---|---|---|---|---|");
            foreach (var c in cases)
            {
                var samples = variants.ToDictionary(v => v, _ => new List<(double Ms, long Alloc, long Rows, long Sum)>());
                for (int round = 0; round < rounds; round++)
                {
                    for (int k = 0; k < variants.Length; k++)
                    {
                        var v = variants[(round + k) % variants.Length];
                        samples[v].Add(await RunOnceAsync(path, c, v.Dict, v.TwoPass));
                    }
                }

                var reference = samples[variants[0]][0];
                foreach (var v in variants)
                {
                    if (samples[v].Any(s => s.Rows != reference.Rows || s.Sum != reference.Sum))
                        throw new InvalidOperationException($"{c.Label}: {v} disagrees on the rows.");
                }

                double Ms((bool, bool) v) => Median(samples[v].Select(s => s.Ms));
                double Mb((bool, bool) v) => Median(samples[v].Select(s => (double)s.Alloc)) / 1e6;
                double kept = 100.0 * reference.Rows / 2_000_000;
                Console.WriteLine(
                    $"| {c.Label} | {kept:F1}% | {Ms((false, false)):F1} | {Ms((true, false)):F1} | {Pct(Ms((true, false)), Ms((false, false)))} " +
                    $"| {Mb((false, false)):F1} | {Mb((true, false)):F1} " +
                    $"| {Ms((false, true)):F1} | {Ms((true, true)):F1} | {Pct(Ms((true, true)), Ms((false, true)))} |");
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }

        return 0;
    }

    private static async Task<(double Ms, long Alloc, long Rows, long Sum)> RunOnceAsync(
        string path, Case c, bool dictionary, bool twoPass)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions()) { TwoPassReads = twoPass };
        var scan = new ParquetScan
        {
            Columns = c.Projection,
            Prune = c.Filter,
            RowFilter = new RowFilter(new RowPredicate(["category"], b => Evaluator.EvaluatePredicate(c.Filter, b))
            {
                ValueWise = dictionary,
            }),
        };

        long allocBefore = GC.GetTotalAllocatedBytes(precise: true);
        var clock = Stopwatch.StartNew();
        long rows = 0, sum = 0;
        await foreach (var selected in reader.ReadSelectedAsync(scan))
        {
            using (selected)
            {
                rows += selected.Batch.Length;
                for (int i = 0; i < selected.Batch.Length; i++)
                    sum += selected.Positions[i];
            }
        }

        double ms = clock.Elapsed.TotalMilliseconds;
        return (ms, GC.GetTotalAllocatedBytes(precise: true) - allocBefore, rows, sum);
    }

    private static string Pct(double after, double before) =>
        ((after / before - 1) * 100).ToString("+0;-0;0", CultureInfo.InvariantCulture) + "%";

    private static double Median(IEnumerable<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }
}
#endif
