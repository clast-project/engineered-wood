// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Open question 3 of doc/parquet-page-index.md: is row-range decode (R-3) worth building? Measures
/// what page pruning (R-2) would keep, and bounds what R-3 could save, without building either.
/// </summary>
/// <remarks>
/// <para><c>write &lt;dir&gt;</c> writes <c>ew-default.parquet</c>: 4 row groups x 1M rows of a sorted id,
/// a sorted timestamp, a clustered user string, and unclustered category/amount/note columns. Re-write
/// it with other writers into the same directory (see the doc) before analysing.</para>
/// <para><c>analyze &lt;dir&gt; [rounds]</c>, per file and predicate: row groups surviving chunk
/// statistics; pages per predicate column; the rows and bytes page pruning keeps, from a prototype of
/// R-2 (elementary row intervals evaluated by the unchanged <see cref="StatisticsEvaluator"/>); the
/// filtered <see cref="ParquetFileReader.ReadAllAsync"/> time today; and the time to read a file holding
/// ONLY the kept rows. That last time is a lower bound on any R-3 read, so today minus it is an upper
/// bound on what R-3 can save.</para>
/// Run with: dotnet run -c Release -f net10.0 -- pageindex-worth write|analyze &lt;dir&gt; [rounds]
/// </remarks>
internal static class PageIndexWorth
{
    private const int RowGroups = 4;
    private const int RowsPerGroup = 1_000_000;

    public static async Task<int> RunAsync(string[] args)
    {
        string mode = args[1];
        string dir = args[2];
        int rounds = args.Length > 3 ? int.Parse(args[3], CultureInfo.InvariantCulture) : 9;
        if (mode == "write")
        {
            Directory.CreateDirectory(dir);
            await WriteAsync(Build(), Path.Combine(dir, "ew-default.parquet"));
            return 0;
        }

        var predicates = new (string Label, Predicate Filter)[]
        {
            ("id = point", Ex.Equal("id", LiteralValue.Of(2_500_000L))),
            ("id 1% range", Ex.And(
                Ex.GreaterThanOrEqual("id", LiteralValue.Of(2_500_000L)),
                Ex.LessThan("id", LiteralValue.Of(2_540_000L)))),
            ("ts 0.1% range", Ex.And(
                Ex.GreaterThanOrEqual("ts", LiteralValue.Of(Ts(1_700_000))),
                Ex.LessThan("ts", LiteralValue.Of(Ts(1_704_000))))),
            ("user = point", Ex.Equal("user", LiteralValue.Of(User(3_300_000)))),
            ("category = (unclustered)", Ex.Equal("category", LiteralValue.Of("cat-07"))),
            ("amount < 0.001 (unclustered)", Ex.LessThan("amount", LiteralValue.Of(0.001))),
        };

        foreach (string path in Directory.GetFiles(dir, "*.parquet").Where(p => !Path.GetFileName(p).StartsWith("kept-")).Order())
        {
            Console.WriteLine();
            Console.WriteLine($"## {Path.GetFileName(path)} ({new FileInfo(path).Length:N0} bytes)");
            await PrintPagesAsync(path);
            Console.WriteLine();
            Console.WriteLine("| Predicate | RGs kept | Rows kept by RG stats | by page index | Bytes kept by page index | ms today | ms kept-rows file | Upper-bound saving |");
            Console.WriteLine("|---|---|---|---|---|---|---|---|");
            foreach (var (label, filter) in predicates)
                await AnalyzeAsync(path, label, filter, rounds);
        }

        return 0;
    }

    private static long Ts(long row) => 1_600_000_000_000_000L + row * 250;

    private static string User(long row) => "user-" + (row / 250).ToString("D6", CultureInfo.InvariantCulture);

    private static RecordBatch Build()
    {
        var random = new Random(3);
        var ids = new Int64Array.Builder();
        var ts = new Int64Array.Builder();
        var users = new StringArray.Builder();
        var categories = new StringArray.Builder();
        var amounts = new DoubleArray.Builder();
        var notes = new StringArray.Builder();
        Span<byte> noteBytes = stackalloc byte[12];
        for (long i = 0; i < (long)RowGroups * RowsPerGroup; i++)
        {
            ids.Append(i);
            ts.Append(Ts(i) + random.Next(1000)); // sorted, neighbours overlap slightly
            users.Append(User(i));
            categories.Append("cat-" + random.Next(50).ToString("D2", CultureInfo.InvariantCulture));
            amounts.Append(random.NextDouble());
            random.NextBytes(noteBytes);
            notes.Append(Convert.ToHexString(noteBytes));
        }

        var schema = new Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("ts", Int64Type.Default, false))
            .Field(new Field("user", StringType.Default, false))
            .Field(new Field("category", StringType.Default, false))
            .Field(new Field("amount", DoubleType.Default, false))
            .Field(new Field("note", StringType.Default, false))
            .Build();
        return new RecordBatch(schema,
            [ids.Build(), ts.Build(), users.Build(), categories.Build(), amounts.Build(), notes.Build()],
            RowGroups * RowsPerGroup);
    }

    private static async Task WriteAsync(RecordBatch batch, string path)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private static async Task PrintPagesAsync(string path)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
        var metadata = await reader.ReadMetadataAsync();
        var schema = new SchemaDescriptor(metadata.Schema);
        var indexes = await reader.ReadPageIndexAsync(0);
        Console.Write($"{metadata.RowGroups.Count} row groups of {metadata.RowGroups[0].NumRows:N0} rows. Data pages per column in row group 0: ");
        Console.WriteLine(string.Join(", ", indexes.Select(ix =>
            $"{schema.Columns[ix.Column].DottedPath} {ix.OffsetIndex?.PageLocations.Count.ToString(CultureInfo.InvariantCulture) ?? "?"}")));
    }

    private readonly record struct RowRange(int RowGroup, long Start, long End);

    private static async Task AnalyzeAsync(string path, string label, Predicate filter, int rounds)
    {
        var ranges = new List<RowRange>();
        long rgRows = 0, totalBytes = 0, keptBytes = 0;
        int rgKept;
        using (var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true))
        {
            var metadata = await reader.ReadMetadataAsync();
            var schema = new SchemaDescriptor(metadata.Schema);
            var accessor = new ParquetStatisticsAccessor(schema);
            var referenced = new HashSet<string>(ReferencedColumns(filter));
            var surviving = Enumerable.Range(0, metadata.RowGroups.Count)
                .Where(g => StatisticsEvaluator.Evaluate(filter, metadata.RowGroups[g], accessor) != FilterResult.AlwaysFalse)
                .ToList();
            rgKept = surviving.Count;

            foreach (int g in surviving)
            {
                var rowGroup = metadata.RowGroups[g];
                rgRows += rowGroup.NumRows;
                var indexes = await reader.ReadPageIndexAsync(g);
                var byName = indexes.ToDictionary(ix => schema.Columns[ix.Column].DottedPath);

                // Elementary intervals: every referenced column's page boundaries, merged.
                var cuts = new SortedSet<long> { 0, rowGroup.NumRows };
                foreach (string column in referenced)
                    foreach (var location in byName[column].OffsetIndex!.PageLocations)
                        cuts.Add(location.FirstRowIndex);
                var bounds = cuts.ToList();
                for (int i = 0; i + 1 < bounds.Count; i++)
                {
                    var interval = IntervalStats(rowGroup, schema, byName, referenced, bounds[i], bounds[i + 1]);
                    if (StatisticsEvaluator.Evaluate(filter, interval, accessor) == FilterResult.AlwaysFalse)
                        continue;
                    if (ranges.Count > 0 && ranges[^1].RowGroup == g && ranges[^1].End == bounds[i])
                        ranges[^1] = ranges[^1] with { End = bounds[i + 1] };
                    else
                        ranges.Add(new RowRange(g, bounds[i], bounds[i + 1]));
                }

                // Bytes an R-3 read fetches: every column's pages overlapping a kept range, plus its
                // dictionary; versus the whole chunks a read fetches today.
                foreach (var ix in indexes)
                {
                    var meta = rowGroup.Columns[ix.Column].MetaData!;
                    totalBytes += meta.TotalCompressedSize;
                    var pages = ix.OffsetIndex!.PageLocations;
                    long prefix = pages[0].Offset - (meta.DictionaryPageOffset ?? pages[0].Offset);
                    bool any = false;
                    for (int p = 0; p < pages.Count; p++)
                    {
                        long start = pages[p].FirstRowIndex;
                        long end = p + 1 < pages.Count ? pages[p + 1].FirstRowIndex : rowGroup.NumRows;
                        if (ranges.Any(r => r.RowGroup == g && r.Start < end && start < r.End))
                        {
                            keptBytes += pages[p].CompressedPageSize;
                            any = true;
                        }
                    }

                    if (any)
                        keptBytes += prefix;
                }
            }
        }

        long keptRows = ranges.Sum(r => r.End - r.Start);
        string keptPath = Path.Combine(Path.GetDirectoryName(path)!, "kept-" + Path.GetFileName(path));
        await WriteKeptRowsAsync(path, ranges, keptPath);

        var today = new List<double>();
        var kept = new List<double>();
        var options = new ParquetReadOptions { Filter = filter };
        for (int round = -3; round < rounds; round++)
        {
            double a = await TimeReadAsync(path, options);
            double b = keptRows == 0 ? 0 : await TimeReadAsync(keptPath, ParquetReadOptions.Default);
            if (round < 0)
                continue;
            today.Add(a);
            kept.Add(b);
        }

        File.Delete(keptPath);
        double msToday = Median(today), msKept = Median(kept);
        Console.WriteLine(
            $"| {label} | {rgKept}/{RowGroups} | {rgRows:N0} | {keptRows:N0} ({Pct(keptRows, rgRows)}) " +
            $"| {keptBytes:N0} of {totalBytes:N0} ({Pct(keptBytes, totalBytes)}) " +
            $"| {msToday:F1} | {msKept:F1} | {(msToday == 0 ? 0 : (1 - msKept / msToday) * 100):F0}% |");
    }

    private static string Pct(long part, long whole) =>
        whole == 0 ? "-" : (100.0 * part / whole).ToString("F2", CultureInfo.InvariantCulture) + "%";

    /// <summary>
    /// A stand-in row group for one elementary interval, carrying the covering page's bounds as the
    /// chunk statistics, so that <see cref="ParquetStatisticsAccessor"/> and the evaluator run unchanged.
    /// </summary>
    private static RowGroup IntervalStats(
        RowGroup rowGroup, SchemaDescriptor schema, Dictionary<string, ColumnChunkPageIndex> byName,
        HashSet<string> referenced, long start, long end)
    {
        var columns = new List<ColumnChunk>();
        for (int c = 0; c < rowGroup.Columns.Count; c++)
        {
            var chunk = rowGroup.Columns[c];
            string name = schema.Columns[c].DottedPath;
            Statistics? stats = null;
            if (referenced.Contains(name) && byName[name].ColumnIndex is { } columnIndex)
            {
                var pages = byName[name].OffsetIndex!.PageLocations;
                int p = pages.Count - 1;
                while (pages[p].FirstRowIndex > start)
                    p--;
                long? nulls = columnIndex.NullPages[p] ? end - start
                    : columnIndex.NullCounts is { } counts && counts[p] == 0 ? 0 : null;
                stats = columnIndex.NullPages[p]
                    ? new Statistics { NullCount = nulls }
                    : new Statistics
                    {
                        MinValue = columnIndex.MinValues[p],
                        MaxValue = columnIndex.MaxValues[p],
                        NullCount = nulls,
                        IsMinValueExact = false,
                        IsMaxValueExact = false,
                    };
            }

            var meta = chunk.MetaData!;
            columns.Add(new ColumnChunk
            {
                FileOffset = chunk.FileOffset,
                MetaData = new ColumnMetaData
                {
                    Type = meta.Type,
                    Encodings = meta.Encodings,
                    PathInSchema = meta.PathInSchema,
                    Codec = meta.Codec,
                    NumValues = end - start,
                    TotalUncompressedSize = 0,
                    TotalCompressedSize = 0,
                    DataPageOffset = meta.DataPageOffset,
                    Statistics = stats,
                },
            });
        }

        return new RowGroup { Columns = columns, TotalByteSize = 0, NumRows = end - start };
    }

    private static IEnumerable<string> ReferencedColumns(Predicate predicate) => predicate switch
    {
        AndPredicate and => and.Children.SelectMany(ReferencedColumns),
        OrPredicate or => or.Children.SelectMany(ReferencedColumns),
        NotPredicate not => ReferencedColumns(not.Child),
        ComparisonPredicate { Left: UnboundReference r } => [r.Name],
        _ => [],
    };

    private static async Task WriteKeptRowsAsync(string path, List<RowRange> ranges, string keptPath)
    {
        if (ranges.Count == 0)
            return;
        var slices = new List<RecordBatch>();
        using (var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true))
        {
            foreach (var group in ranges.GroupBy(r => r.RowGroup))
            {
                var batch = await reader.ReadRowGroupAsync(group.Key);
                foreach (var r in group)
                    slices.Add(batch.Slice((int)r.Start, (int)(r.End - r.Start)));
            }
        }

        var schema = slices[0].Schema;
        var arrays = Enumerable.Range(0, schema.FieldsList.Count)
            .Select(c => ArrowCompute.Concatenate(slices.Select(s => s.Column(c)).ToList()))
            .ToArray();
        await WriteAsync(new RecordBatch(schema, arrays, arrays[0].Length), keptPath);
    }

    private static async Task<double> TimeReadAsync(string path, ParquetReadOptions options)
    {
        var clock = Stopwatch.StartNew();
        using (var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true, options))
        {
            await foreach (var batch in reader.ReadAllAsync())
                batch.Dispose();
        }

        return clock.Elapsed.TotalMilliseconds;
    }

    private static double Median(List<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }
}
#endif
