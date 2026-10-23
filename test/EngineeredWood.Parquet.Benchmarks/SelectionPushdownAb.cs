// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Compression;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// EXPERIMENT (perf/selection-pushdown-ab): does handing the column decoder the rows to keep beat
/// decoding every row and filtering the Arrow arrays afterwards?
/// </summary>
/// <remarks>
/// Two scenarios. A FILTER (<c>k &gt; t</c>, <c>n IS NOT NULL</c>): the predicate column is decoded
/// whole, the predicate yields the selection, and the remaining columns decode under it. A DELETION
/// VECTOR: the selection is known before any column decodes. Variants:
/// <list type="bullet">
///   <item><c>full</c>: decode every row, no filter (reference cost of the read itself).</item>
///   <item><c>take</c>: decode every row, hand-written mask, <see cref="ArrowCompute.Take(IArrowArray, ReadOnlySpan{int})"/> per column.</item>
///   <item><c>evaluator</c> (filter): as <c>take</c> but the mask comes from <see cref="ArrowRowEvaluator"/>, the piece EW has today.</item>
///   <item><c>hashset</c> (DV): <c>DeletionVectorFilter</c>'s loop: probe a <c>HashSet&lt;long&gt;</c> per row, then Take.</item>
///   <item><c>compact</c>: selection pushed in; each page decodes whole and is compacted in place.</item>
///   <item><c>skip</c>: selection pushed in; PLAIN and dictionary decoders never copy a dropped value.</item>
/// </list>
/// One row group, decoded single-threaded from chunks already in memory: CPU and allocation only, no I/O.
/// Allocation is managed (this thread) plus native (<see cref="NativeMemoryTracker"/>), excluding pooled rentals.
/// Run with: dotnet run -c Release -f net10.0 -- selection-ab [rounds] [rows] [snappy|none] [columns|all]
/// </remarks>
internal static class SelectionPushdownAb
{
    private static readonly string[] AllColumns = ["k", "n", "id", "d", "s", "c"];

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 11;
        int rows = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1 << 20;
        var codec = args.Length > 3 && args[3].StartsWith("none", StringComparison.Ordinal) ? CompressionCodec.Uncompressed : CompressionCodec.Snappy;
        // "snappy-plain" / "none-plain": PLAIN integers, as a file from pyarrow or Spark would have once its dictionary overflows.
        bool plainInts = args.Length > 3 && args[3].EndsWith("-plain", StringComparison.Ordinal);
        string[] output = args.Length > 4 && args[4] != "all" ? args[4].Split(',') : AllColumns;
        string? only = args.Length > 5 ? args[5] : null;

        string path = Path.Combine(Path.GetTempPath(), "ew-selection-ab-" + Guid.NewGuid().ToString("N")[..8] + ".parquet");
        try
        {
            await WriteAsync(path, rows, codec, plainInts);
            using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
            var raw = await reader.ReadRawRowGroupAsync(0);
            var meta = await reader.ReadMetadataAsync();

            Console.WriteLine($"{rows:N0} rows written in {meta.RowGroups.Count} row group(s); measuring row group 0 ({raw.RowCount:N0} rows), {codec}; output columns: {string.Join(",", output)}");
            for (int i = 0; i < raw.Columns.Count; i++)
            {
                var cm = raw.Meta[i].MetaData!;
                Console.WriteLine($"  {raw.Fields[i].Name,-3} {raw.Fields[i].DataType.Name,-7} nullable={raw.Fields[i].IsNullable,-5} " +
                    $"encodings={string.Join("/", cm.Encodings)} compressed={cm.TotalCompressedSize:N0}");
            }
            Console.WriteLine($"median of {rounds} rotated rounds after 10 warm-up passes; Δ is against `take`");
            Console.WriteLine();

            var k = ColumnIndex(raw, "k");
            var outIdx = output.Select(n => ColumnIndex(raw, n)).ToArray();

            var scenarios = new List<Scenario>();
            foreach (double keep in new[] { 0.01, 0.1, 0.5, 0.9, 0.99 })
            {
                int t = (int)((1 - keep) * 1_000_000);
                scenarios.Add(Scenario.Filter($"k > {t:N0} (keep {keep:P0})", k, Union(outIdx, k),
                    col => GreaterThan((Int32Array)col, t), Expressions.Expressions.GreaterThan("k", LiteralValue.Of(t))));
            }
            int n = ColumnIndex(raw, "n");
            scenarios.Add(Scenario.Filter("n IS NOT NULL (keep ~50%)", n, Union(outIdx, n),
                col => NotNull(col), Expressions.Expressions.IsNotNull("n")));
            foreach (double deleted in new[] { 0.001, 0.01, 0.1, 0.5 })
                scenarios.Add(Scenario.Dv($"DV deletes {deleted:P1}", outIdx, Deleted(raw.RowCount, deleted)));

            Console.WriteLine("| Scenario | Variant | ms | Δ ms | MB alloc | Δ MB |");
            Console.WriteLine("|---|---|---|---|---|---|");
            foreach (var scenario in scenarios.Where(s => only is null || s.Name.Contains(only, StringComparison.Ordinal)))
                Run(raw, scenario, rounds);
        }
        finally
        {
            File.Delete(path);
        }

        return 0;
    }

    private static void Run(ParquetFileReader.RawRowGroup raw, Scenario scenario, int rounds)
    {
        var variants = scenario.Variants(raw);

        // Every variant must produce the rows `take` does.
        var expected = variants.Single(v => v.Name == "take").Run();
        foreach (var v in variants.Where(v => v.Name is not ("take" or "full")))
        {
            var actual = v.Run();
            Verify(scenario.Name, v.Name, expected, actual);
            Dispose(actual);
        }
        Dispose(expected);

        for (int i = 0; i < 10; i++)
            foreach (var v in variants)
                Dispose(v.Run());

        var ms = variants.ToDictionary(v => v.Name, _ => new List<double>());
        var mb = variants.ToDictionary(v => v.Name, _ => new List<double>());
        for (int r = 0; r < rounds; r++)
        {
            for (int j = 0; j < variants.Count; j++)
            {
                var v = variants[(j + r) % variants.Count];
                GC.Collect();
                GC.WaitForPendingFinalizers();
                long managed = GC.GetAllocatedBytesForCurrentThread();
                long native = NativeMemoryTracker.TotalAllocated;
                var sw = Stopwatch.StartNew();
                var result = v.Run();
                sw.Stop();
                long bytes = GC.GetAllocatedBytesForCurrentThread() - managed + NativeMemoryTracker.TotalAllocated - native;
                Dispose(result);
                ms[v.Name].Add(sw.Elapsed.TotalMilliseconds);
                mb[v.Name].Add(bytes / 1048576.0);
            }
        }

        double baseMs = Median(ms["take"]), baseMb = Median(mb["take"]);
        foreach (var v in variants)
        {
            double m = Median(ms[v.Name]), a = Median(mb[v.Name]);
            Console.WriteLine($"| {scenario.Name} | {v.Name} | {m:F2} | {Delta(m / baseMs)} | {a:F1} | {Delta(a / baseMb)} |");
        }
    }

    private sealed record Variant(string Name, Func<IArrowArray[]> Run);

    private sealed class Scenario
    {
        public required string Name { get; init; }
        public required Func<ParquetFileReader.RawRowGroup, List<Variant>> Variants { get; init; }

        public static Scenario Filter(
            string name, int predicateColumn, int[] output, Func<IArrowArray, byte[]> mask, Predicate predicate) => new()
            {
                Name = name,
                Variants = raw =>
                [
                    new("full", () => output.Select(c => Decode(raw, c)).ToArray()),
                    new("take", () =>
                    {
                        var cols = output.Select(c => Decode(raw, c)).ToArray();
                        var keep = mask(cols[System.Array.IndexOf(output, predicateColumn)]);
                        return TakeAll(cols, Indices(keep));
                    }),
                    new("runcopy", () =>
                    {
                        var cols = output.Select(c => Decode(raw, c)).ToArray();
                        var keep = mask(cols[System.Array.IndexOf(output, predicateColumn)]);
                        return RunCopyAll(cols, keep);
                    }),
                    new("evaluator", () =>
                    {
                        var cols = output.Select(c => Decode(raw, c)).ToArray();
                        var schema = new Schema(output.Select(c => raw.Fields[c]), null);
                        var result = new ArrowRowEvaluator().EvaluatePredicate(predicate, new RecordBatch(schema, cols, raw.RowCount));
                        var indices = new List<int>();
                        for (int i = 0; i < result.Length; i++)
                            if (result.GetValue(i) == true)
                                indices.Add(i);
                        result.Dispose();
                        return TakeAll(cols, indices.ToArray());
                    }),
                    new("compact", () => Pushed(raw, output, predicateColumn, mask, skip: false)),
                    new("skip", () => Pushed(raw, output, predicateColumn, mask, skip: true)),
                ],
            };

        public static Scenario Dv(string name, int[] output, long[] deleted) => new()
        {
            Name = name,
            Variants = raw =>
            [
                new("full", () => output.Select(c => Decode(raw, c)).ToArray()),
                new("take", () =>
                {
                    var cols = output.Select(c => Decode(raw, c)).ToArray();
                    return TakeAll(cols, Indices(RowSelection.FromDeleted(deleted, raw.RowCount).Keep.ToArray()));
                }),
                new("runcopy", () =>
                {
                    var cols = output.Select(c => Decode(raw, c)).ToArray();
                    return RunCopyAll(cols, RowSelection.FromDeleted(deleted, raw.RowCount).Keep.ToArray());
                }),
                new("hashset", () =>
                {
                    // DeletionVectorFilter.Filter, minus the RecordBatch wrapper.
                    var cols = output.Select(c => Decode(raw, c)).ToArray();
                    var set = new HashSet<long>(deleted);
                    var keepRows = new List<int>();
                    for (int i = 0; i < raw.RowCount; i++)
                        if (!set.Contains(i))
                            keepRows.Add(i);
                    var taken = cols.Select(c => ArrowCompute.Take(c, keepRows)).ToArray();
                    Dispose(cols);
                    return taken;
                }),
                new("compact", () =>
                {
                    var selection = RowSelection.FromDeleted(deleted, raw.RowCount);
                    return output.Select(c => Decode(raw, c, selection, skip: false)).ToArray();
                }),
                new("skip", () =>
                {
                    var selection = RowSelection.FromDeleted(deleted, raw.RowCount);
                    return output.Select(c => Decode(raw, c, selection, skip: true)).ToArray();
                }),
            ],
        };
    }

    private static IArrowArray[] Pushed(
        ParquetFileReader.RawRowGroup raw, int[] output, int predicateColumn, Func<IArrowArray, byte[]> mask, bool skip)
    {
        var predicate = Decode(raw, predicateColumn);
        var keep = mask(predicate);
        var selection = new RowSelection(keep, raw.RowCount);
        var result = new IArrowArray[output.Length];
        for (int i = 0; i < output.Length; i++)
        {
            result[i] = output[i] == predicateColumn
                ? ArrowCompute.Take(predicate, Indices(keep))
                : Decode(raw, output[i], selection, skip);
        }
        predicate.Dispose();
        return result;
    }

    private static IArrowArray Decode(ParquetFileReader.RawRowGroup raw, int c, RowSelection? selection = null, bool skip = true) =>
        ColumnChunkReader.ReadColumn(raw.Chunks[c], raw.Columns[c], raw.Meta[c].MetaData!, raw.RowCount, raw.Fields[c],
            selection: selection, skipInDecoder: skip).Array;

    private static IArrowArray[] TakeAll(IArrowArray[] cols, int[] indices)
    {
        var taken = cols.Select(c => ArrowCompute.Take(c, indices)).ToArray();
        Dispose(cols);
        return taken;
    }

    /// <summary>
    /// The best a filter outside the decoder can do: a new buffer per column, filled one run of kept
    /// rows at a time (one copy per run, offsets rebased per run).
    /// </summary>
    private static IArrowArray[] RunCopyAll(IArrowArray[] cols, byte[] keep)
    {
        var runs = new List<(int Start, int End)>();
        int kept = 0;
        for (int i = 0; i < keep.Length;)
        {
            if (keep[i] == 0) { i++; continue; }
            int start = i;
            while (i < keep.Length && keep[i] != 0) i++;
            runs.Add((start, i));
            kept += i - start;
        }

        var result = cols.Select(c => RunCopy(c, runs, kept)).ToArray();
        Dispose(cols);
        return result;
    }

    private static IArrowArray RunCopy(IArrowArray array, List<(int Start, int End)> runs, int kept)
    {
        var data = array.Data;
        ArrowBuffer validity = ArrowBuffer.Empty;
        int nullCount = 0;
        if (array.NullCount > 0)
        {
            var source = data.Buffers[0].Span;
            var bits = new byte[(kept + 7) / 8];
            int w = 0;
            foreach (var (start, end) in runs)
            {
                for (int i = start; i < end; i++, w++)
                {
                    if ((source[i >> 3] & (1 << (i & 7))) != 0)
                        bits[w >> 3] |= (byte)(1 << (w & 7));
                    else
                        nullCount++;
                }
            }
            validity = new ArrowBuffer(bits);
        }

        if (data.DataType is StringType)
        {
            var offsets = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
            var bytes = data.Buffers[2].Span;
            int total = 0;
            foreach (var (start, end) in runs)
                total += offsets[end] - offsets[start];
            var newOffsets = new int[kept + 1];
            var newBytes = new byte[total];
            int w = 0, pos = 0;
            foreach (var (start, end) in runs)
            {
                int len = offsets[end] - offsets[start];
                bytes.Slice(offsets[start], len).CopyTo(newBytes.AsSpan(pos));
                int shift = pos - offsets[start];
                for (int i = start; i < end; i++)
                    newOffsets[++w] = offsets[i + 1] + shift;
                pos += len;
            }
            var offsetBytes = new byte[newOffsets.Length * 4];
            Buffer.BlockCopy(newOffsets, 0, offsetBytes, 0, offsetBytes.Length);
            return Apache.Arrow.ArrowArrayFactory.BuildArray(new ArrayData(data.DataType, kept, nullCount, 0,
                [validity, new ArrowBuffer(offsetBytes), new ArrowBuffer(newBytes)]));
        }

        int width = ((FixedWidthType)data.DataType).BitWidth / 8;
        var values = data.Buffers[1].Span;
        var newValues = new byte[kept * width];
        int at = 0;
        foreach (var (start, end) in runs)
        {
            int len = (end - start) * width;
            values.Slice(start * width, len).CopyTo(newValues.AsSpan(at));
            at += len;
        }
        return Apache.Arrow.ArrowArrayFactory.BuildArray(new ArrayData(data.DataType, kept, nullCount, 0,
            [validity, new ArrowBuffer(newValues)]));
    }

    private static int[] Indices(byte[] keep)
    {
        int count = 0;
        foreach (byte b in keep) count += b;
        var indices = new int[count];
        int w = 0;
        for (int i = 0; i < keep.Length; i++)
            if (keep[i] != 0) indices[w++] = i;
        return indices;
    }

    private static byte[] GreaterThan(Int32Array col, int t)
    {
        var values = col.Values;
        var keep = new byte[col.Length];
        for (int i = 0; i < keep.Length; i++)
            keep[i] = (byte)(values[i] > t && col.IsValid(i) ? 1 : 0);
        return keep;
    }

    private static byte[] NotNull(IArrowArray col)
    {
        var keep = new byte[col.Length];
        for (int i = 0; i < keep.Length; i++)
            keep[i] = (byte)(col.IsValid(i) ? 1 : 0);
        return keep;
    }

    private static long[] Deleted(int rows, double fraction)
    {
        var rng = new Random(17);
        var set = new HashSet<long>();
        int target = (int)(rows * fraction);
        while (set.Count < target)
            set.Add(rng.Next(rows));
        var sorted = set.ToArray();
        System.Array.Sort(sorted);
        return sorted;
    }

    private static async Task WriteAsync(string path, int rows, CompressionCodec codec, bool plainInts)
    {
        var rng = new Random(42);
        var k = new Int32Array.Builder();
        var n = new Int32Array.Builder();
        var id = new Int64Array.Builder();
        var d = new DoubleArray.Builder();
        var s = new StringArray.Builder();
        var c = new StringArray.Builder();
        var categories = Enumerable.Range(0, 200).Select(i => RandomString(rng, 10 + rng.Next(20))).ToArray();
        for (int i = 0; i < rows; i++)
        {
            k.Append(rng.Next(1_000_000));
            if (rng.Next(2) == 0) n.AppendNull(); else n.Append(rng.Next());
            id.Append(rng.NextInt64());
            if (rng.Next(10) == 0) d.AppendNull(); else d.Append(rng.NextDouble());
            s.Append(RandomString(rng, 8 + rng.Next(25)));
            c.Append(categories[rng.Next(categories.Length)]);
        }

        var schema = new Schema.Builder()
            .Field(new Field("k", Int32Type.Default, nullable: false))
            .Field(new Field("n", Int32Type.Default, nullable: true))
            .Field(new Field("id", Int64Type.Default, nullable: false))
            .Field(new Field("d", DoubleType.Default, nullable: true))
            .Field(new Field("s", StringType.Default, nullable: false))
            .Field(new Field("c", StringType.Default, nullable: false))
            .Build();
        using var batch = new RecordBatch(schema,
            [k.Build(), n.Build(), id.Build(), d.Build(), s.Build(), c.Build()], rows);

        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default with
        {
            Compression = codec,
            IntegerEncoding = plainInts ? IntegerEncoding.Plain : IntegerEncoding.DeltaBinaryPacked,
        });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private static string RandomString(Random rng, int length)
    {
        var chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char)('a' + rng.Next(26));
        return new string(chars);
    }

    private static void Verify(string scenario, string variant, IArrowArray[] expected, IArrowArray[] actual)
    {
        for (int c = 0; c < expected.Length; c++)
        {
            var e = expected[c];
            var a = actual[c];
            if (e.Length != a.Length || e.NullCount != a.NullCount)
                throw new InvalidOperationException($"{scenario}/{variant} column {c}: {a.Length} rows ({a.NullCount} null), expected {e.Length} ({e.NullCount} null).");
            for (int i = 0; i < e.Length; i++)
            {
                bool same = (e, a) switch
                {
                    _ when e.IsNull(i) || a.IsNull(i) => e.IsNull(i) == a.IsNull(i),
                    (Int32Array x, Int32Array y) => x.GetValue(i) == y.GetValue(i),
                    (Int64Array x, Int64Array y) => x.GetValue(i) == y.GetValue(i),
                    (DoubleArray x, DoubleArray y) => x.GetValue(i).Equals(y.GetValue(i)),
                    (StringArray x, StringArray y) => x.GetString(i) == y.GetString(i),
                    _ => throw new InvalidOperationException($"No comparison for {e.GetType().Name}."),
                };
                if (!same)
                    throw new InvalidOperationException($"{scenario}/{variant} column {c} differs at row {i}.");
            }
        }
    }

    private static int ColumnIndex(ParquetFileReader.RawRowGroup raw, string name) =>
        System.Array.FindIndex(raw.Fields, f => f.Name == name) is var i and >= 0 ? i : throw new ArgumentException($"No column '{name}'.");

    private static int[] Union(int[] output, int column) => output.Contains(column) ? output : [column, .. output];

    private static void Dispose(IArrowArray[] arrays)
    {
        foreach (var a in arrays) a.Dispose();
    }

    private static double Median(List<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }

    private static string Delta(double ratio) =>
        ((ratio - 1) * 100).ToString("+0;-0;0", CultureInfo.InvariantCulture) + "%";
}
#endif
