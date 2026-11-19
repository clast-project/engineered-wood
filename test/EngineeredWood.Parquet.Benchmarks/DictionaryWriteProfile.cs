// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Compression;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Where a dictionary-encoded write's time goes: each column written with the dictionary on (the default) and
/// off, <see cref="DictionaryEncoder.TryEncode"/> alone (building the dictionary, or giving up on it), and for
/// strings the per-value FNV-1a hash the dictionary's table computes, alone.
/// </summary>
/// <remarks>
/// Run with: dotnet run -c Release -f net10.0 -- dict-profile [rounds] [rows]. One column, uncompressed, in
/// memory. Each case is warmed for 3 s first: shorter harnesses here timed tier-0 code.
/// </remarks>
internal static class DictionaryWriteProfile
{
    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 15;
        int rows = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1_000_000;

        Console.WriteLine($"{rows:N0} rows, one column, uncompressed, in memory; best of {rounds} after a 3 s warm-up (ms).");
        Console.WriteLine("| Column | Dictionary? | Write, dict on | Write, dict off | TryEncode alone | Hash alone | Bytes, dict on |");
        Console.WriteLine("|---|---|---|---|---|---|---|");

        foreach (var (name, batch, physical) in Shapes(rows))
        {
            var on = ParquetWriteOptions.Default with { Compression = CompressionCodec.Uncompressed };
            var off = on with { DictionaryEnabled = false };
            var array = batch.Column(0);
            bool built = DictionaryEncoder.TryEncode(array, physical, 0, null, array.Length, on) is not null;
            byte[] file = await NullableColumnProfile.WriteAsync(batch, on);

            double wOn = Time(rounds, () => NullableColumnProfile.DiscardWriteAsync(batch, on).GetAwaiter().GetResult());
            double wOff = Time(rounds, () => NullableColumnProfile.DiscardWriteAsync(batch, off).GetAwaiter().GetResult());
            double encode = Time(rounds, () => DictionaryEncoder.TryEncode(array, physical, 0, null, array.Length, on));
            string hash = physical == PhysicalType.ByteArray ? Time(rounds, () => HashAll(array)).ToString("F2", CultureInfo.InvariantCulture) : "-";

            Console.WriteLine($"| {name} | {(built ? "kept" : "abandoned")} | {wOn:F2} | {wOff:F2} | {encode:F2} | {hash} | {file.Length:N0} |");
        }

        return 0;
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

    private static uint _sink;

    /// <summary>FNV-1a over every value, as the dictionary's hash table computes it.</summary>
    private static void HashAll(IArrowArray array)
    {
        var offsets = MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span);
        var data = array.Data.Buffers[2].Span;
        uint acc = 0;
        for (int i = 0; i < array.Length; i++)
        {
            var value = data.Slice(offsets[i], offsets[i + 1] - offsets[i]);
            uint h = 2166136261;
            foreach (byte b in value)
            {
                h ^= b;
                h *= 16777619;
            }

            acc += h;
        }

        _sink += acc;
    }

    private static IEnumerable<(string, RecordBatch, PhysicalType)> Shapes(int rows)
    {
        var random = new Random(42);

        var i64 = new Int64Array.Builder();
        for (int i = 0; i < rows; i++) i64.Append(random.NextInt64());
        yield return ("int64 random", Batch(i64.Build()), PhysicalType.Int64);

        var i64c = new Int64Array.Builder();
        for (int i = 0; i < rows; i++) i64c.Append(1_000_000 + random.Next(1000));
        yield return ("int64, 1,000 distinct", Batch(i64c.Build()), PhysicalType.Int64);

        var f64 = new DoubleArray.Builder();
        for (int i = 0; i < rows; i++) f64.Append(random.NextDouble());
        yield return ("double random", Batch(f64.Build()), PhysicalType.Double);

        var s = new StringArray.Builder();
        for (int i = 0; i < rows; i++) s.Append("value-" + random.NextInt64().ToString(CultureInfo.InvariantCulture));
        yield return ("string random", Batch(s.Build()), PhysicalType.ByteArray);

        var c = new StringArray.Builder();
        for (int i = 0; i < rows; i++) c.Append("category-" + random.Next(1000).ToString(CultureInfo.InvariantCulture));
        yield return ("string, 1,000 distinct", Batch(c.Build()), PhysicalType.ByteArray);

        var m = new StringArray.Builder();
        for (int i = 0; i < rows; i++) m.Append("customer-" + random.Next(100_000).ToString("D6", CultureInfo.InvariantCulture));
        yield return ("string, 100,000 distinct", Batch(m.Build()), PhysicalType.ByteArray);
    }

    private static RecordBatch Batch(IArrowArray array) =>
        new(new Apache.Arrow.Schema([new Field("c", array.Data.DataType, false)], null), [array], array.Length);
}
#endif
