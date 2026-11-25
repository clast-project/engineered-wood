// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// <see cref="BufferedParquetWriter"/> against the streaming <see cref="ParquetFileWriter.WriteBatchAsync"/>, fed
/// the same slices into one row group, per column shape and for a mixed table.
/// </summary>
/// <remarks>
/// Run with: dotnet run -c Release -f net10.0 -- buffered-ab [rounds] [rows] [batch] [filter]. Uncompressed, in
/// memory, bytes discarded. Each case is warmed for 3 s first.
/// </remarks>
internal static class BufferedWriteAb
{
    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 10;
        int rows = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1_000_000;
        int batchRows = args.Length > 3 ? int.Parse(args[3], CultureInfo.InvariantCulture) : 65_536;
        string? filter = args.Length > 4 ? args[4] : null;

        if (Environment.GetEnvironmentVariable("EW_BUFFERED_HASH") == "1")
            return await HashAsync(rows);

        var options = ParquetWriteOptions.Default with
        {
            Compression = CompressionCodec.Uncompressed,
            RowGroupMaxRows = rows,
        };

        Console.WriteLine($"{rows:N0} rows in {batchRows:N0}-row appends, one row group, uncompressed; best of {rounds} after 3 s warm-up (ms).");
        Console.WriteLine("| Shape | Streaming | Buffered | Ratio |");
        Console.WriteLine("|---|---|---|---|");

        foreach (var (name, batch) in Shapes(rows))
        {
            if (filter is not null && !name.Contains(filter, StringComparison.OrdinalIgnoreCase))
                continue;
            // EW_BUFFERED_ONLY=1 skips the streaming side, for a profiler attached to the buffered one.
            double streaming = Environment.GetEnvironmentVariable("EW_BUFFERED_ONLY") == "1"
                ? double.NaN
                : Time(rounds, () => StreamingAsync(batch, batchRows, options).GetAwaiter().GetResult());
            double buffered = Time(rounds, () => BufferedAsync(batch, batchRows, options).GetAwaiter().GetResult());
            Console.WriteLine($"| {name} | {streaming:F2} | {buffered:F2} | {buffered / streaming:F2}x |");
        }

        return 0;
    }

    /// <summary>
    /// EW_BUFFERED_HASH=1: a SHA-256 of each shape written by the buffered writer under several options and append
    /// sizes, so a change meant to leave its output alone can be diffed against a build from before it.
    /// </summary>
    private static async Task<int> HashAsync(int rows)
    {
        var variants = new (string Name, ParquetWriteOptions Options)[]
        {
            ("default", ParquetWriteOptions.Default with { CreatedBy = "x" }),
            ("snappy-v1", ParquetWriteOptions.Default with { CreatedBy = "x", Compression = CompressionCodec.Snappy, DataPageVersion = DataPageVersion.V1 }),
            ("dict-off", ParquetWriteOptions.Default with { CreatedBy = "x", DictionaryEnabled = false }),
            ("dict-limit-64k", ParquetWriteOptions.Default with { CreatedBy = "x", DictionaryPageSizeLimit = 64 * 1024 }),
            ("dict-limit-4m", ParquetWriteOptions.Default with { CreatedBy = "x", DictionaryPageSizeLimit = 4 << 20 }),
            ("rg-300k-bloom-index", ParquetWriteOptions.Default with { CreatedBy = "x", RowGroupMaxRows = 300_000, BloomFilterColumns = ["c0", "c1", "c4"], WritePageIndex = true }),
        };

        foreach (var (shape, batch) in Shapes(rows))
        foreach (var (name, options) in variants)
        foreach (int batchRows in new[] { 1_000, 65_536, rows })
        {
            var sink = new MemorySink();
            await using (var writer = new BufferedParquetWriter(sink, ownsFile: false, options))
            {
                for (int start = 0; start < batch.Length; start += batchRows)
                    await writer.AppendAsync(batch.Slice(start, Math.Min(batchRows, batch.Length - start)));
                await writer.CloseAsync();
            }

            string hash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(sink.ToArray()));
            Console.WriteLine($"{hash[..16]}  {shape} / {name} / {batchRows}");
        }

        return 0;
    }

    private sealed class MemorySink : EngineeredWood.IO.ISequentialFile
    {
        private readonly MemoryStream _stream = new();

        public long Position => _stream.Position;

        public byte[] ToArray() => _stream.ToArray();

        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            _stream.Write(data.Span);
            return default;
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => default;

        public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => default;

        public ValueTask DisposeAsync() => default;

        public void Dispose() { }
    }

    private static async Task StreamingAsync(RecordBatch batch, int batchRows, ParquetWriteOptions options)
    {
        await using var writer = new ParquetFileWriter(new CountingSequentialFile(), ownsFile: true, options);
        for (int start = 0; start < batch.Length; start += batchRows)
            await writer.WriteBatchAsync(batch.Slice(start, Math.Min(batchRows, batch.Length - start)));
        await writer.CloseAsync();
    }

    private static async Task BufferedAsync(RecordBatch batch, int batchRows, ParquetWriteOptions options)
    {
        await using var writer = new BufferedParquetWriter(new CountingSequentialFile(), ownsFile: true, options);
        for (int start = 0; start < batch.Length; start += batchRows)
            await writer.AppendAsync(batch.Slice(start, Math.Min(batchRows, batch.Length - start)));
        await writer.CloseAsync();
    }

    private static double Time(int rounds, Action action)
    {
        for (var warm = Stopwatch.StartNew(); warm.ElapsedMilliseconds < 3000;)
            action();

        double best = double.MaxValue;
        for (int i = 0; i < rounds; i++)
        {
            var clock = Stopwatch.StartNew();
            action();
            best = Math.Min(best, clock.Elapsed.TotalMilliseconds);
        }

        return best;
    }

    private static IEnumerable<(string, RecordBatch)> Shapes(int rows)
    {
        var columns = new List<(string Name, IArrowArray Array, bool Nullable)>();
        var random = new Random(42);

        var i64 = new Int64Array.Builder();
        for (int i = 0; i < rows; i++) i64.Append(random.NextInt64());
        columns.Add(("int64 random", i64.Build(), false));

        var i64c = new Int64Array.Builder();
        for (int i = 0; i < rows; i++) i64c.Append(1_000_000 + random.Next(1000));
        columns.Add(("int64 1k distinct", i64c.Build(), false));

        var i32n = new Int32Array.Builder();
        for (int i = 0; i < rows; i++)
        {
            if (random.Next(10) == 0) i32n.AppendNull();
            else i32n.Append(random.Next(50_000));
        }
        columns.Add(("int32 50k distinct 10% null", i32n.Build(), true));

        var f64 = new DoubleArray.Builder();
        for (int i = 0; i < rows; i++) f64.Append(random.NextDouble());
        columns.Add(("double random", f64.Build(), false));

        var s = new StringArray.Builder();
        for (int i = 0; i < rows; i++) s.Append("value-" + random.NextInt64().ToString(CultureInfo.InvariantCulture));
        columns.Add(("string random", s.Build(), false));

        var c = new StringArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            if (random.Next(20) == 0) c.AppendNull();
            else c.Append("category-" + random.Next(1000).ToString(CultureInfo.InvariantCulture));
        }
        columns.Add(("string 1k distinct 5% null", c.Build(), true));

        var b = new BooleanArray.Builder();
        for (int i = 0; i < rows; i++) b.Append(random.Next(2) == 0);
        columns.Add(("bool", b.Build(), false));

        var d = new Decimal128Array.Builder(new Decimal128Type(18, 2));
        for (int i = 0; i < rows; i++) d.Append(random.Next(1_000_000) / 100m);
        columns.Add(("decimal(18,2)", d.Build(), false));

        var list = new ListArray.Builder(Int32Type.Default);
        var listValues = (Int32Array.Builder)list.ValueBuilder;
        for (int i = 0; i < rows; i++)
        {
            list.Append();
            int n = random.Next(4);
            for (int k = 0; k < n; k++) listValues.Append(random.Next(100));
        }
        columns.Add(("list<int32>", list.Build(), true));

        foreach (var (name, array, nullable) in columns)
            yield return (name, Batch([(name, array, nullable)]));

        yield return ("mixed table", Batch(columns));
    }

    private static RecordBatch Batch(IEnumerable<(string Name, IArrowArray Array, bool Nullable)> columns)
    {
        var list = columns.ToList();
        var fields = list.Select((c, i) => new Field("c" + i, c.Array.Data.DataType, c.Nullable)).ToList();
        return new RecordBatch(new Apache.Arrow.Schema(fields, null), list.Select(c => c.Array), list[0].Array.Length);
    }
}
#endif
