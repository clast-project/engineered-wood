// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// What an all-false dictionary table used as a row-group prune could save (doc/row-level-filtering.md §6, §31). An
/// opaque value-wise host probe matches only row group 0 of 8. Today the reader answers every group from its
/// dictionary but still reads it. <c>ideal</c> skips groups 1-7 by statistics on <c>id</c>, an upper bound on the
/// prune, which would read only their dictionary pages.
/// </summary>
/// <remarks>Run with: dotnet run -c Release -f net10.0 -- allfalse-ab [rounds]</remarks>
internal static class AllFalsePruneAb
{
    private const int Groups = 8;
    private const int RowsPerGroup = 250_000;

    private static readonly HashSet<string> Keys = new(StringComparer.Ordinal) { "k0-01", "k0-05", "k0-09" };

    private sealed record Variant(string Label, bool ValueWise, bool TwoPass, bool Ideal);

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 9;
        string dir = Path.Combine(Path.GetTempPath(), "ew-allfalse-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            string path = Path.Combine(dir, "data.parquet");
            await WriteAsync(path);
            await CheckWhollyDictionaryAsync(path);

            var variants = new Variant[]
            {
                new("rows", false, false, false), new("dict", true, false, false),
                new("dict 2-pass", true, true, false), new("ideal", true, false, true), new("ideal 2-pass", true, true, true),
            };
            var projections = new (string Name, string[] Columns)[]
            {
                ("wide", ["id", "amount", "qty", "ts", "note"]), ("id", ["id"]),
            };

            Console.WriteLine($"{new FileInfo(path).Length:N0} bytes, {Groups} x {RowsPerGroup:N0} rows; median of {rounds}; {Environment.ProcessorCount} logical processors.");
            Console.WriteLine();
            Console.WriteLine("| Projection | Variant | ms | vs dict | MB alloc | rows |");
            Console.WriteLine("|---|---|---|---|---|---|");
            foreach (var (name, columns) in projections)
            {
                for (int warm = 0; warm < 3; warm++)
                {
                    foreach (var v in variants)
                        await RunOnceAsync(path, columns, v);
                }

                var samples = variants.ToDictionary(v => v, _ => new List<(double Ms, long Alloc, long Rows, long Sum)>());
                for (int round = 0; round < rounds; round++)
                {
                    for (int k = 0; k < variants.Length; k++)
                    {
                        var v = variants[(round + k) % variants.Length];
                        samples[v].Add(await RunOnceAsync(path, columns, v));
                    }
                }

                var reference = samples[variants[0]][0];
                foreach (var v in variants)
                {
                    if (samples[v].Any(s => s.Rows != reference.Rows || s.Sum != reference.Sum))
                        throw new InvalidOperationException($"{name} {v.Label}: the rows differ.");
                }

                double dict = Median(samples[variants[1]].Select(s => s.Ms));
                foreach (var v in variants)
                {
                    double ms = Median(samples[v].Select(s => s.Ms));
                    double mb = Median(samples[v].Select(s => (double)s.Alloc)) / 1e6;
                    Console.WriteLine($"| {name} | {v.Label} | {ms:F1} | {Pct(ms, dict)} | {mb:F1} | {reference.Rows:N0} |");
                }
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
        var random = new Random(806);
        await using var output = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(output, ownsFile: false,
            ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup });
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("key", StringType.Default, false))
            .Field(new Field("amount", DoubleType.Default, false))
            .Field(new Field("qty", Int32Type.Default, false))
            .Field(new Field("ts", Int64Type.Default, false))
            .Field(new Field("note", StringType.Default, false))
            .Build();
        var noteBytes = new byte[8];
        for (int g = 0; g < Groups; g++)
        {
            var id = new Int64Array.Builder();
            var key = new StringArray.Builder();
            var amount = new DoubleArray.Builder();
            var qty = new Int32Array.Builder();
            var ts = new Int64Array.Builder();
            var note = new StringArray.Builder();
            for (int r = 0; r < RowsPerGroup; r++)
            {
                long row = (long)g * RowsPerGroup + r;
                id.Append(row);

                // 20 keys local to the row group: only group 0 holds the probe's.
                key.Append($"k{g}-{random.Next(20):D2}");
                amount.Append(random.NextDouble());
                qty.Append(random.Next(100));
                ts.Append(1_600_000_000_000_000L + row * 250);
                random.NextBytes(noteBytes);
                note.Append(Convert.ToHexString(noteBytes));
            }

            using var batch = new RecordBatch(schema,
                [id.Build(), key.Build(), amount.Build(), qty.Build(), ts.Build(), note.Build()], RowsPerGroup);
            await writer.WriteRowGroupAsync(batch);
        }

        await writer.CloseAsync();
    }

    // The prune is only sound for wholly dictionary-encoded chunks: check the premise.
    private static async Task CheckWhollyDictionaryAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        if (metadata.RowGroups.Count != Groups)
            throw new InvalidOperationException($"{metadata.RowGroups.Count} row groups.");
        foreach (var group in metadata.RowGroups)
        {
            var chunk = group.Columns.Single(c => c.MetaData!.PathInSchema[0] == "key");
            if (!MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(chunk.MetaData!))
                throw new InvalidOperationException("key is not wholly dictionary-encoded.");
        }
    }

    private static BooleanArray Probe(RecordBatch batch)
    {
        var column = (StringArray)batch.Column(0);
        var builder = new BooleanArray.Builder().Reserve(column.Length);
        for (int i = 0; i < column.Length; i++)
            builder.Append(Keys.Contains(column.GetString(i)!));
        return builder.Build();
    }

    private static async Task<(double Ms, long Alloc, long Rows, long Sum)> RunOnceAsync(string path, string[] columns, Variant v)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions()) { TwoPassReads = v.TwoPass };
        var scan = new ParquetScan
        {
            Columns = columns,
            Prune = v.Ideal ? Ex.LessThan("id", LiteralValue.Of((long)RowsPerGroup)) : null,
            RowFilter = new RowFilter(new RowPredicate(["key"], Probe) { ValueWise = v.ValueWise }),
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
        var sorted = values.OrderBy(x => x).ToList();
        return sorted[sorted.Count / 2];
    }
}
#endif
