// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Phase 0 of exact row-level filtering (#502, doc/row-level-filtering.md): what an exact filtered read
/// costs today, and how much a two-pass read could save, before any reader change. Reports, per case,
/// the median of alternating rounds for each strategy below.
/// </summary>
/// <remarks>
/// <para>The strategies all return the same rows (checked by count and a checksum of
/// <c>id</c>, in an untimed run after each case's timed rounds), except <c>none</c>:</para>
/// <list type="bullet">
/// <item><b>none</b>: the projection read with no filter at all. A reference point, not a filtered read.</item>
/// <item><b>post</b>: today's exact read, as a table layer must do it. Prune with the predicate
/// (<see cref="ParquetFileReader.ReadWithPositionsAsync(Predicate?, IReadOnlyList{string}?, RowSkipPredicate?, CancellationToken)"/>),
/// reading the projection plus any filter-only column. Then, per batch, clear excluded rows, AND in
/// <see cref="ArrowRowEvaluator.EvaluatePredicate"/>'s mask, gather with <see cref="ArrowCompute.Take(RecordBatch, Schema, ReadOnlySpan{int})"/>,
/// and drop the filter-only columns. This is the baseline every later phase is measured against.</item>
/// <item><b>pass1</b>: the same, reading only the filter columns and counting the selected rows. The floor
/// for any two-pass read: what remains if decoding the other columns cost nothing.</item>
/// <item><b>late</b>: an emulation of phase 1 using public APIs. Pass 1 as above builds each row group's
/// selection. Pass 2 then reads the projection with <see cref="ParquetFileReader.ReadRowRangesAsync"/>,
/// only over the PAGES holding a selected row (from the OffsetIndex), and keeps the selected rows. It
/// over-states phase 1's cost in two ways: a projected filter column is decoded twice, and each row group's
/// pass 2 reads its OffsetIndexes again (one more request), where the reader would reuse those planning read.
/// The page boundaries it plans with are read before timing starts, standing for that planning.</item>
/// </list>
/// <para>The file is 4 row groups of 500,000 rows, written with default options (20,000-row pages, page
/// indexes). <c>id</c> is sorted, so a range on it is CLUSTERED: the page index narrows it, and its
/// matches are contiguous. <c>r</c> is uniform over [0, 1,000,000), so a range on it is SCATTERED: no page
/// can be pruned and matches are everywhere. <c>category</c> has 50 values and is dictionary-encoded. The
/// narrow projection is <c>id, amount</c>; the wide one is every column but the scattered key.</para>
/// <para>Arguments, all optional and positional:
/// <c>rowfilter-ab [rounds] [latencyMs] [direct|cloud] [MB/s] [excluded fraction] [case substring]</c>.
/// <c>latencyMs</c>, <c>cloud</c> and <c>MB/s</c> are as in <c>pageindex-read-ab</c>. The excluded fraction
/// (e.g. 0.01) excludes that share of rows at random positions, as a deletion vector would, applied by every
/// strategy. The case substring runs only the cases whose label contains it.</para>
/// Run with: dotnet run -c Release -f net10.0 -- rowfilter-ab 7
/// </remarks>
internal static class RowFilterAb
{
    private const int RowGroups = 4;
    private const int RowsPerGroup = 500_000;
    private const long TotalRows = (long)RowGroups * RowsPerGroup;
    private const int KeySpace = 1_000_000;

    private static readonly string[] Narrow = ["id", "amount"];
    private static readonly string[] Wide = ["id", "ts", "category", "amount", "qty", "note"];

    private static readonly ArrowRowEvaluator Evaluator = new();

    /// <summary>Whether reads checksum their output: only in the untimed validation runs.</summary>
    private static volatile bool s_validate;

    private enum Strategy { None, Post, Pass1, Late }

    private sealed record Case(string Label, Predicate Filter, string[] FilterColumns, string[] Projection);

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 7;
        int latencyMs = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 0;
        bool cloud = args.Length > 3 && args[3] == "cloud";
        double mbps = args.Length > 4 ? double.Parse(args[4], CultureInfo.InvariantCulture) : 0;
        double excludedFraction = args.Length > 5 ? double.Parse(args[5], CultureInfo.InvariantCulture) : 0;
        string? only = args.Length > 6 ? args[6] : null;

        string dir = Path.Combine(Path.GetTempPath(), "ew-rowfilter-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            string path = Path.Combine(dir, "data.parquet");
            await WriteAsync(Build(), path);
            var excluded = excludedFraction > 0 ? Exclusions.Random(TotalRows, excludedFraction, seed: 502) : null;
            var layout = await Layout.LoadAsync(path);

            var cases = Cases().Where(c => only is null || c.Label.Contains(only, StringComparison.OrdinalIgnoreCase)).ToList();

            Console.WriteLine(
                $"{RowGroups} row groups x {RowsPerGroup:N0} rows, default options; {new FileInfo(path).Length:N0} bytes; " +
                $"{(excluded is null ? "no rows excluded" : $"{excluded.Count:N0} rows excluded at random")}; " +
                $"median of {rounds} rounds (strategy order rotated) after warm-up; {latencyMs} ms added per " +
                (cloud ? "GET, through CoalescingFileReader" : "request") + (mbps > 0 ? $", then {mbps} MB/s." : ".") +
                $" {Environment.ProcessorCount} logical processors.");
            Console.WriteLine();
            Console.WriteLine("| Case | Kept | none ms | post ms | pass1 ms | late ms | late vs post | eval ms | gather ms | post MB decoded | late MB decoded | post MB managed | late MB managed | post MB read | late MB read | post GETs | late GETs |");
            Console.WriteLine("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");

            var strategies = new[] { Strategy.None, Strategy.Post, Strategy.Pass1, Strategy.Late };

            // Unmeasured passes first, so that no case is measured on tier-0 code.
            for (int pass = 0; pass < 3; pass++)
            {
                foreach (var c in cases)
                {
                    foreach (var s in strategies)
                        await RunOnceAsync(s, c, path, layout, excluded, 0, 0, cloud);
                }
            }

            foreach (var c in cases)
            {
                var samples = strategies.ToDictionary(s => s, _ => new List<Result>());
                for (int round = 0; round < rounds; round++)
                {
                    for (int k = 0; k < strategies.Length; k++)
                    {
                        var s = strategies[(round + k) % strategies.Length];
                        samples[s].Add(await RunOnceAsync(s, c, path, layout, excluded, latencyMs, mbps, cloud));
                    }
                }

                // The strategies must agree on the rows, or the comparison means nothing. Checked in a run of
                // its own, outside every timing: the checksum is a pass over the output, and it would run on
                // the consumer's thread for post but on the row-group workers for late.
                s_validate = true;
                try
                {
                    var post = await RunOnceAsync(Strategy.Post, c, path, layout, excluded, 0, 0, cloud);
                    var pass1 = await RunOnceAsync(Strategy.Pass1, c, path, layout, excluded, 0, 0, cloud);
                    var late = await RunOnceAsync(Strategy.Late, c, path, layout, excluded, 0, 0, cloud);
                    if (pass1.Rows != post.Rows || late.Rows != post.Rows || late.Checksum != post.Checksum)
                    {
                        throw new InvalidOperationException(
                            $"{c.Label}: post kept {post.Rows} rows (checksum {post.Checksum}), pass1 {pass1.Rows}, " +
                            $"late {late.Rows} (checksum {late.Checksum}).");
                    }
                }
                finally
                {
                    s_validate = false;
                }

                var p = samples[Strategy.Post];
                var l = samples[Strategy.Late];
                var ratios = p.Zip(l, (a, b) => b.Ms / a.Ms).ToList();
                double kept = 100.0 * p[0].Rows / TotalRows;
                Console.WriteLine(
                    $"| {c.Label} | {kept.ToString(kept < 1 ? "0.###" : "0.#", CultureInfo.InvariantCulture)}% " +
                    $"| {Median(samples[Strategy.None], r => r.Ms):F1} | {Median(p, r => r.Ms):F1} " +
                    $"| {Median(samples[Strategy.Pass1], r => r.Ms):F1} | {Median(l, r => r.Ms):F1} " +
                    $"| {(Median(ratios) - 1) * 100:+0;-0;0}% | {Median(p, r => r.EvalMs):F1} | {Median(p, r => r.GatherMs):F1} " +
                    $"| {p[0].DecodedBytes / 1e6:F1} | {l[0].DecodedBytes / 1e6:F1} " +
                    $"| {Median(p, r => r.AllocBytes) / 1e6:F1} | {Median(l, r => r.AllocBytes) / 1e6:F1} " +
                    $"| {p[0].Bytes / 1e6:F1} | {l[0].Bytes / 1e6:F1} | {p[0].Gets} | {l[0].Gets} |");
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }

        return 0;
    }

    private static IEnumerable<Case> Cases()
    {
        foreach (var (projName, projection) in new[] { ("narrow", Narrow), ("wide", Wide) })
        {
            // Clustered: a range of the sorted id.
            foreach (var (label, share) in new[] { ("point", 0.0), ("0.1%", 0.001), ("1%", 0.01), ("10%", 0.1), ("50%", 0.5), ("90%", 0.9) })
            {
                // Small ranges start mid-file, inside the third row group; 90% cannot, so it starts at row 0.
                const long from = 1_010_000L;
                Predicate filter = share switch
                {
                    0 => Ex.Equal("id", LiteralValue.Of(1_234_567L)),
                    > 0.5 => Ex.LessThan("id", LiteralValue.Of((long)(TotalRows * share))),
                    _ => Ex.And(
                        Ex.GreaterThanOrEqual("id", LiteralValue.Of(from)),
                        Ex.LessThan("id", LiteralValue.Of(from + (long)(TotalRows * share)))),
                };

                yield return new Case($"clustered {label} {projName}", filter, ["id"], projection);
            }

            // Scattered: a range of the uniform key, which no statistics can narrow.
            foreach (var (label, bound) in new[] { ("point", -1L), ("0.1%", 1_000L), ("1%", 10_000L), ("10%", 100_000L), ("50%", 500_000L), ("90%", 900_000L), ("99%", 990_000L) })
            {
                Predicate filter = bound < 0
                    ? Ex.Equal("r", LiteralValue.Of(123_456L))
                    : Ex.LessThan("r", LiteralValue.Of(bound));
                yield return new Case($"scattered {label} {projName}", filter, ["r"], projection);
            }

            // Dictionary-encoded strings: part 3's case (#608's OneCategory).
            yield return new Case($"category = 2% {projName}",
                Ex.Equal("category", LiteralValue.Of("cat-07")), ["category"], projection);
            yield return new Case($"category <> 98% {projName}",
                Ex.NotEqual("category", LiteralValue.Of("cat-07")), ["category"], projection);
        }
    }

    private static RecordBatch Build()
    {
        const int rows = (int)TotalRows;
        var random = new Random(502);
        var ids = new Int64Array.Builder();
        var ts = new Int64Array.Builder();
        var keys = new Int64Array.Builder();
        var categories = new StringArray.Builder();
        var amounts = new DoubleArray.Builder();
        var qtys = new Int32Array.Builder();
        var notes = new StringArray.Builder();
        Span<byte> noteBytes = stackalloc byte[12];
        for (long i = 0; i < rows; i++)
        {
            ids.Append(i);
            ts.Append(1_600_000_000_000_000L + i * 250 + random.Next(250));
            keys.Append(random.Next(KeySpace));
            categories.Append("cat-" + random.Next(50).ToString("D2", CultureInfo.InvariantCulture));
            amounts.Append(random.NextDouble());
            qtys.Append(random.Next(100));
            random.NextBytes(noteBytes);
            notes.Append(Convert.ToHexString(noteBytes));
        }

        var schema = new Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("ts", Int64Type.Default, false))
            .Field(new Field("r", Int64Type.Default, false))
            .Field(new Field("category", StringType.Default, false))
            .Field(new Field("amount", DoubleType.Default, false))
            .Field(new Field("qty", Int32Type.Default, false))
            .Field(new Field("note", StringType.Default, false))
            .Build();
        return new RecordBatch(schema,
            [ids.Build(), ts.Build(), keys.Build(), categories.Build(), amounts.Build(), qtys.Build(), notes.Build()], rows);
    }

    private static async Task WriteAsync(RecordBatch batch, string path)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false,
            ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private readonly record struct Result(
        double Ms, double EvalMs, double GatherMs, long AllocBytes, long DecodedBytes, int Gets, long Bytes, long Rows, long Checksum);

    private static async Task<Result> RunOnceAsync(
        Strategy strategy, Case c, string path, Layout layout, Exclusions? excluded, int latencyMs, double mbps, bool cloud)
    {
        await using var transport = new LatencyCountingFile(new LocalRandomAccessFile(path), latencyMs, mbps);
        await using var coalescer = cloud ? new CoalescingFileReader(transport) : null;
        IRandomAccessFile file = (IRandomAccessFile?)coalescer ?? transport;

        // Every option at its default: Filter stays unset, each read passes its predicate itself.
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions());
        long allocBefore = GC.GetTotalAllocatedBytes(precise: true);
        var clock = Stopwatch.StartNew();
        var tally = strategy switch
        {
            Strategy.None => await ReadNoneAsync(reader, c),
            Strategy.Post => await ReadPostAsync(reader, c, excluded, includeOutput: true),
            Strategy.Pass1 => await ReadPostAsync(reader, c, excluded, includeOutput: false),
            _ => await ReadLateAsync(reader, c, layout, excluded),
        };
        double ms = clock.Elapsed.TotalMilliseconds;
        long alloc = GC.GetTotalAllocatedBytes(precise: true) - allocBefore;
        return new Result(ms, tally.EvalMs, tally.GatherMs, alloc, tally.DecodedBytes, transport.Requests, transport.Bytes, tally.Rows, tally.Checksum);
    }

    private sealed class Tally
    {
        public long Rows;
        public long Checksum;
        public double EvalMs;

        /// <summary>Time spent gathering the selected rows (index list, Take, projection), on the consumer's thread.</summary>
        public double GatherMs;

        /// <summary>The Arrow size of every batch the reader decoded: the decode work, which the managed
        /// allocation count misses, since Arrow buffers are native memory.</summary>
        public long DecodedBytes;

        public void Decoded(RecordBatch batch) => DecodedBytes += ArrowSizes.EstimatedBytes(batch);

        public void Add(RecordBatch output)
        {
            Rows += output.Length;
            if (s_validate && output.Schema.GetFieldIndex("id") is var i and >= 0)
            {
                var ids = (Int64Array)output.Column(i);
                for (int r = 0; r < ids.Length; r++)
                    Checksum += ids.GetValue(r)!.Value;
            }
        }
    }

    private static async Task<Tally> ReadNoneAsync(ParquetFileReader reader, Case c)
    {
        var tally = new Tally();
        await foreach (var positioned in reader.ReadWithPositionsAsync(null, c.Projection))
        {
            tally.Rows += positioned.Batch.Length;
            tally.Decoded(positioned.Batch);
            positioned.Batch.Dispose();
        }

        return tally;
    }

    /// <summary>
    /// The exact read a table layer does today: prune, decode the projection plus filter-only columns, mask,
    /// gather. With <paramref name="includeOutput"/> false, only the filter columns are read and the selected
    /// rows counted (pass 1 alone).
    /// </summary>
    private static async Task<Tally> ReadPostAsync(ParquetFileReader reader, Case c, Exclusions? excluded, bool includeOutput)
    {
        var columns = includeOutput ? c.Projection.Union(c.FilterColumns).ToArray() : c.FilterColumns;
        var tally = new Tally();
        var evalClock = new Stopwatch();
        var gatherClock = new Stopwatch();
        ulong[] words = [];
        int[] indices = [];
        await foreach (var positioned in reader.ReadWithPositionsAsync(c.Filter, columns, excluded?.SkipRows, CancellationToken.None))
        {
            using var batch = positioned.Batch;
            tally.Decoded(batch);
            int rows = batch.Length;
            int wordCount = SelectionMask.WordCount(rows);
            if (words.Length < wordCount)
                words = new ulong[wordCount];
            var selection = words.AsSpan(0, wordCount);
            SelectionMask.SelectAll(selection, rows);
            excluded?.Clear(positioned.FirstRow, rows, selection);

            evalClock.Start();
            using (var mask = Evaluator.EvaluatePredicate(c.Filter, batch))
                SelectionMask.And(selection, rows, mask);
            evalClock.Stop();

            if (!includeOutput)
            {
                tally.Rows += SelectionMask.Count(selection);
                continue;
            }

            gatherClock.Start();
            if (indices.Length < rows)
                indices = new int[rows];
            int kept = SelectionMask.ToIndices(selection, indices);
            using var output = Project(ArrowCompute.Take(batch, batch.Schema, indices.AsSpan(0, kept)), c.Projection);
            gatherClock.Stop();
            tally.Add(output);
        }

        tally.EvalMs = evalClock.Elapsed.TotalMilliseconds;
        tally.GatherMs = gatherClock.Elapsed.TotalMilliseconds;
        return tally;
    }

    /// <summary>
    /// Phase 1, emulated: pass 1 builds each row group's selection from the filter columns; pass 2 reads the
    /// projection over only the pages that hold a selected row, row groups four at a time, and keeps the
    /// selected rows.
    /// </summary>
    private static async Task<Tally> ReadLateAsync(ParquetFileReader reader, Case c, Layout layout, Exclusions? excluded)
    {
        // Pass 1: the selection of every row group, one bit per row.
        var selections = new ulong[RowGroups][];
        var tally = new Tally();
        var evalClock = new Stopwatch();
        await foreach (var positioned in reader.ReadWithPositionsAsync(c.Filter, c.FilterColumns, excluded?.SkipRows, CancellationToken.None))
        {
            using var batch = positioned.Batch;
            tally.Decoded(batch);
            int rows = batch.Length;
            int group = (int)(positioned.FirstRow / RowsPerGroup);
            int offset = (int)(positioned.FirstRow - (long)group * RowsPerGroup);
            var words = new ulong[SelectionMask.WordCount(rows)];
            SelectionMask.SelectAll(words, rows);
            excluded?.Clear(positioned.FirstRow, rows, words);
            evalClock.Start();
            using (var mask = Evaluator.EvaluatePredicate(c.Filter, batch))
                SelectionMask.And(words, rows, mask);
            evalClock.Stop();

            var groupWords = selections[group] ??= new ulong[SelectionMask.WordCount(RowsPerGroup)];
            OrInto(groupWords, offset, words, rows);
        }

        tally.EvalMs = evalClock.Elapsed.TotalMilliseconds;

        // Pass 2: each row group's pages with a selected row, decoded and cut down to the selected rows.
        var results = new Tally[RowGroups];
        await Parallel.ForEachAsync(
            Enumerable.Range(0, RowGroups),
            new ParallelOptions { MaxDegreeOfParallelism = Math.Min(4, Environment.ProcessorCount) },
            async (group, token) =>
            {
                var local = new Tally();
                results[group] = local;
                if (selections[group] is not { } selected || SelectionMask.Count(selected) == 0)
                    return;

                var ranges = layout.PagesWithSelectedRows(group, selected);
                int range = 0;
                long cursor = ranges[0].Start;
                ulong[] words = [];
                int[] indices = [];
                await foreach (var batch in reader.ReadRowRangesAsync(group, ranges, c.Projection, token))
                {
                    using (batch)
                    {
                        local.Decoded(batch);
                        int rows = batch.Length;
                        int wordCount = SelectionMask.WordCount(rows);
                        if (words.Length < wordCount)
                            words = new ulong[wordCount];
                        var batchWords = words.AsSpan(0, wordCount);
                        CopyOut(selected, (int)cursor, batchWords, rows);
                        if (indices.Length < rows)
                            indices = new int[rows];
                        int kept = SelectionMask.ToIndices(batchWords, indices);
                        using var output = ArrowCompute.Take(batch, batch.Schema, indices.AsSpan(0, kept));
                        local.Add(output);
                    }

                    cursor += batch.Length;
                    if (cursor == ranges[range].End && range + 1 < ranges.Count)
                        cursor = ranges[++range].Start;
                }
            });

        foreach (var r in results)
        {
            tally.Rows += r.Rows;
            tally.Checksum += r.Checksum;
            tally.DecodedBytes += r.DecodedBytes;
        }

        return tally;
    }

    /// <summary>ORs the first <paramref name="rows"/> bits of <paramref name="source"/> into
    /// <paramref name="target"/> starting at bit <paramref name="offset"/>, a word at a time.</summary>
    private static void OrInto(Span<ulong> target, int offset, ReadOnlySpan<ulong> source, int rows)
    {
        int shift = offset & 63, baseWord = offset >> 6;
        for (int j = 0; j < SelectionMask.WordCount(rows); j++)
        {
            ulong v = source[j];
            int tail = rows - 64 * j;
            if (tail < 64)
                v &= (1UL << tail) - 1;
            target[baseWord + j] |= v << shift;
            if (shift != 0 && baseWord + j + 1 < target.Length)
                target[baseWord + j + 1] |= v >> (64 - shift);
        }
    }

    /// <summary>Sets <paramref name="target"/> to <paramref name="rows"/> bits of <paramref name="source"/>
    /// read from bit <paramref name="offset"/>, a word at a time.</summary>
    private static void CopyOut(ReadOnlySpan<ulong> source, int offset, Span<ulong> target, int rows)
    {
        int shift = offset & 63, baseWord = offset >> 6;
        for (int i = 0; i < SelectionMask.WordCount(rows); i++)
        {
            int w = baseWord + i;
            ulong v = source[w] >> shift;
            if (shift != 0 && w + 1 < source.Length)
                v |= source[w + 1] << (64 - shift);
            int tail = rows - 64 * i;
            target[i] = tail < 64 ? v & ((1UL << tail) - 1) : v;
        }
    }

    private static RecordBatch Project(RecordBatch batch, string[] projection)
    {
        if (batch.ColumnCount == projection.Length)
            return batch;

        var fields = new List<Field>(projection.Length);
        var arrays = new List<IArrowArray>(projection.Length);
        foreach (var name in projection)
        {
            int i = batch.Schema.GetFieldIndex(name);
            fields.Add(batch.Schema.GetFieldByIndex(i));
            arrays.Add(batch.Column(i));
        }

        // The dropped columns are disposed with the full batch; the kept ones move to the projection.
        var projected = new RecordBatch(new Schema(fields, null), arrays, batch.Length);
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (!projection.Contains(batch.Schema.GetFieldByIndex(i).Name))
                batch.Column(i).Dispose();
        }

        return projected;
    }

    /// <summary>Each row group's page boundaries, from the OffsetIndex, read once before timing.</summary>
    private sealed class Layout(long[][] pageStarts)
    {
        public static async Task<Layout> LoadAsync(string path)
        {
            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var starts = new long[RowGroups][];
            for (int g = 0; g < RowGroups; g++)
            {
                // Every column's pages are cut at the same rows in an EW-written file (the 20,000-row cap), so
                // one column's boundaries serve the whole projection. The union is taken anyway.
                var indexes = await reader.ReadPageIndexAsync(g, Wide);
                starts[g] = indexes
                    .SelectMany(ix => ix.OffsetIndex!.PageLocations.Select(p => p.FirstRowIndex))
                    .Distinct()
                    .OrderBy(s => s)
                    .ToArray();
            }

            return new Layout(starts);
        }

        /// <summary>The row ranges of the pages of <paramref name="group"/> that hold a selected row,
        /// adjacent pages merged.</summary>
        public List<RowRange> PagesWithSelectedRows(int group, ulong[] selected)
        {
            var starts = pageStarts[group];
            var ranges = new List<RowRange>();
            for (int p = 0; p < starts.Length; p++)
            {
                long start = starts[p];
                long end = p + 1 < starts.Length ? starts[p + 1] : RowsPerGroup;
                if (!AnySet(selected, (int)start, (int)end))
                    continue;
                if (ranges.Count > 0 && ranges[^1].End == start)
                    ranges[^1] = ranges[^1] with { End = end };
                else
                    ranges.Add(new RowRange(start, end));
            }

            return ranges;
        }

        private static bool AnySet(ulong[] words, int from, int to)
        {
            for (int i = from; i < to; i++)
            {
                if ((i & 63) == 0 && i + 64 <= to)
                {
                    if (words[i >> 6] != 0)
                        return true;
                    i += 63;
                    continue;
                }

                if ((words[i >> 6] & (1UL << (i & 63))) != 0)
                    return true;
            }

            return false;
        }
    }

    /// <summary>Rows excluded by file position, as a deletion vector excludes them.</summary>
    private sealed class Exclusions(ulong[] words, long count)
    {
        public long Count => count;

        public static Exclusions Random(long rows, double fraction, int seed)
        {
            var random = new Random(seed);
            var words = new ulong[(rows + 63) >> 6];
            long count = 0;
            for (long i = 0; i < rows; i++)
            {
                if (random.NextDouble() < fraction)
                {
                    words[i >> 6] |= 1UL << (int)(i & 63);
                    count++;
                }
            }

            return new Exclusions(words, count);
        }

        public RowSkipPredicate SkipRows => (first, rowCount) =>
        {
            for (long i = first; i < first + rowCount; i++)
            {
                if ((words[i >> 6] & (1UL << (int)(i & 63))) == 0)
                    return false;
            }

            return true;
        };

        public void Clear(long firstRow, int rows, Span<ulong> selection)
        {
            for (int i = 0; i < rows; i++)
            {
                long p = firstRow + i;
                if ((words[p >> 6] & (1UL << (int)(p & 63))) != 0)
                    selection[i >> 6] &= ~(1UL << (i & 63));
            }
        }
    }

    private static double Median<T>(List<T> values, Func<T, double> selector)
    {
        var sorted = values.Select(selector).OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }

    private static double Median(List<double> values) => Median(values, v => v);
}
#endif
