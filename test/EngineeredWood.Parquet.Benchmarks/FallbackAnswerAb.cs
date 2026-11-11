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
using EngineeredWood.Parquet.Metadata;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// #804 A/B: a value-wise predicate over a column whose chunks fall back from the dictionary to PLAIN partway through,
/// as arrow-cpp, parquet-mr and arrow-rs write them once the dictionary passes a size limit. <c>rows</c> asks the
/// predicate row by row, which is all a mixed chunk got before #804. <c>dict</c> marks it value-wise: the
/// dictionary-encoded pages are answered from the dictionary, and the fallback pages' values are asked.
/// </summary>
/// <remarks>
/// <para>arrow-cpp (ParquetSharp) writes 4 row groups x 500,000 rows: <c>id</c>, <c>amount</c>, and <c>category</c>
/// (50 values). In each row group, one filler value larger than the 64 KiB dictionary page limit, at the fallback
/// point, makes the rest of the chunk PLAIN. 0% PLAIN is a wholly dictionary-encoded control.</para>
/// <para>Run with: dotnet run -c Release -f net10.0 -- fallback-ab [rounds]</para>
/// </remarks>
internal static class FallbackAnswerAb
{
    private const int Groups = 4;
    private const int RowsPerGroup = 500_000;
    private const int Rows = Groups * RowsPerGroup;

    private static readonly ArrowRowEvaluator Evaluator = new();

    // Filter is the predicate the evaluator asks, and Prune; Probe, when set, is asked instead, as a host's delegate
    // (a hash-set probe, like a join filter) would be.
    private sealed record Case(string Label, Predicate Filter, string[] Projection, Func<RecordBatch, BooleanArray>? Probe = null);

    private static readonly HashSet<string> ProbeKeys = new(StringComparer.Ordinal)
    {
        "cat-01", "cat-11", "cat-21", "cat-31", "cat-41",
    };

    private static BooleanArray ProbeKeysOf(RecordBatch batch)
    {
        var column = (StringArray)batch.Column(0);
        var builder = new BooleanArray.Builder().Reserve(column.Length);
        for (int i = 0; i < column.Length; i++)
        {
            if (column.GetString(i) is { } value)
                builder.Append(ProbeKeys.Contains(value));
            else
                builder.AppendNull();
        }

        return builder.Build();
    }

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 9;
        string dir = Path.Combine(Path.GetTempPath(), "ew-fallback-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            var cases = new List<Case>();
            foreach (var (projName, projection) in new[] { ("id, amount", new[] { "id", "amount" }), ("id, category", new[] { "id", "category" }) })
            {
                cases.Add(new Case($"= 2% {projName}", Ex.Equal("category", LiteralValue.Of("cat-07")), projection));
                cases.Add(new Case($"IN 5 (10%) {projName}", Ex.In("category",
                    LiteralValue.Of("cat-01"), LiteralValue.Of("cat-11"), LiteralValue.Of("cat-21"), LiteralValue.Of("cat-31"), LiteralValue.Of("cat-41")),
                    projection));
                cases.Add(new Case($"<> 98% {projName}", Ex.NotEqual("category", LiteralValue.Of("cat-07")), projection));
                cases.Add(new Case($"host probe 5 (10%) {projName}", Ex.In("category",
                    LiteralValue.Of("cat-01"), LiteralValue.Of("cat-11"), LiteralValue.Of("cat-21"), LiteralValue.Of("cat-31"), LiteralValue.Of("cat-41")),
                    projection, ProbeKeysOf));
            }

            Console.WriteLine($"median of {rounds} rounds, order rotated; {Environment.ProcessorCount} logical processors.");
            Console.WriteLine();
            Console.WriteLine("| PLAIN | Case | Kept | rows ms | dict ms | dict vs rows | rows MB alloc | dict MB alloc |");
            Console.WriteLine("|---|---|---|---|---|---|---|---|");
            foreach (int plainPercent in new[] { 0, 10, 50, 90 })
            {
                string path = Path.Combine(dir, $"plain-{plainPercent}.parquet");
                Write(path, plainPercent);
                await CheckMixedAsync(path, plainPercent);

                for (int warm = 0; warm < 2; warm++)
                {
                    foreach (var c in cases)
                    {
                        await RunOnceAsync(path, c, dictionary: false);
                        await RunOnceAsync(path, c, dictionary: true);
                    }
                }

                foreach (var c in cases)
                {
                    var samples = new[] { new List<(double Ms, long Alloc, long Rows, long Sum)>(), new() };
                    for (int round = 0; round < rounds; round++)
                    {
                        for (int k = 0; k < 2; k++)
                        {
                            int v = (round + k) % 2;
                            samples[v].Add(await RunOnceAsync(path, c, dictionary: v == 1));
                        }
                    }

                    var reference = samples[0][0];
                    if (samples.SelectMany(s => s).Any(s => s.Rows != reference.Rows || s.Sum != reference.Sum))
                        throw new InvalidOperationException($"{plainPercent}% {c.Label}: the paths disagree on the rows.");

                    double Ms(int v) => Median(samples[v].Select(s => s.Ms));
                    double Mb(int v) => Median(samples[v].Select(s => (double)s.Alloc)) / 1e6;
                    double kept = 100.0 * reference.Rows / Rows;
                    Console.WriteLine(
                        $"| {plainPercent}% | {c.Label} | {kept:F1}% | {Ms(0):F1} | {Ms(1):F1} | {Pct(Ms(1), Ms(0))} | {Mb(0):F1} | {Mb(1):F1} |");
                }
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }

        return 0;
    }

    private static void Write(string path, int plainPercent)
    {
        var random = new Random(804);
        int fallbackRow = plainPercent == 0 ? -1 : RowsPerGroup * (100 - plainPercent) / 100;
        string filler = new('~', 70_000);
        var columns = new ParquetSharp.Column[]
        {
            new ParquetSharp.Column<long>("id"),
            new ParquetSharp.Column<double>("amount"),
            new ParquetSharp.Column<string>("category"),
        };

        using var properties = new ParquetSharp.WriterPropertiesBuilder()
            .DictionaryPagesizeLimit(64 * 1024)
            .Build();
        using var writer = new ParquetSharp.ParquetFileWriter(path, columns, properties);
        for (int g = 0; g < Groups; g++)
        {
            using var rowGroup = writer.AppendRowGroup();
            using (var ids = rowGroup.NextColumn().LogicalWriter<long>())
                ids.WriteBatch(Enumerable.Range(g * RowsPerGroup, RowsPerGroup).Select(r => (long)r).ToArray());
            using (var amounts = rowGroup.NextColumn().LogicalWriter<double>())
                amounts.WriteBatch(Enumerable.Range(0, RowsPerGroup).Select(_ => random.NextDouble()).ToArray());

            var categories = Enumerable.Range(0, RowsPerGroup)
                .Select(r => r == fallbackRow ? filler : "cat-" + random.Next(50).ToString("D2", CultureInfo.InvariantCulture))
                .ToArray();
            using var category = rowGroup.NextColumn().LogicalWriter<string>();

            // The dictionary is measured after each batch written: the filler alone in its own makes the fallback land
            // right after it.
            if (fallbackRow < 0)
            {
                category.WriteBatch(categories);
            }
            else
            {
                category.WriteBatch(categories.AsSpan(0, fallbackRow + 1));
                category.WriteBatch(categories.AsSpan(fallbackRow + 1));
            }
        }

        writer.Close();
    }

    // The category chunks hold PLAIN data pages exactly when the fallback was meant to happen.
    private static async Task CheckMixedAsync(string path, int plainPercent)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        foreach (var group in metadata.RowGroups)
        {
            var stats = group.Columns.Single(c => c.MetaData!.PathInSchema[0] == "category").MetaData!.EncodingStats!;
            int Count(Func<Encoding, bool> encoding) => stats
                .Where(s => s.PageType is PageType.DataPage or PageType.DataPageV2 && encoding(s.Encoding))
                .Sum(s => s.Count);
            int dict = Count(e => e is Encoding.RleDictionary or Encoding.PlainDictionary);
            int plain = Count(e => e == Encoding.Plain);
            if (dict == 0 || (plain > 0) != (plainPercent > 0))
                throw new InvalidOperationException($"{plainPercent}% PLAIN: {dict} dictionary and {plain} PLAIN data pages.");
        }
    }

    private static async Task<(double Ms, long Alloc, long Rows, long Sum)> RunOnceAsync(string path, Case c, bool dictionary)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions());
        var scan = new ParquetScan
        {
            Columns = c.Projection,
            Prune = c.Filter,
            RowFilter = new RowFilter(new RowPredicate(["category"], c.Probe ?? (b => Evaluator.EvaluatePredicate(c.Filter, b)))
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
