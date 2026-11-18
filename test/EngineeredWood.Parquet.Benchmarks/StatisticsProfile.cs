// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// What column statistics cost a write: each column written with statistics on (the default) and off,
/// with the dictionary on (the default) and off, and the statistics scan alone over the same values.
/// </summary>
/// <remarks>
/// Run with: dotnet run -c Release -f net10.0 -- stats-profile [rounds] [rows]. One column, uncompressed,
/// in memory. Statistics off also drops the ColumnIndex, whose page bounds are the scan that, with the
/// page index on (the default), also gives the chunk statistics. Each cell is the best round.
/// </remarks>
internal static class StatisticsProfile
{
    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 15;
        int rows = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1_000_000;

        Console.WriteLine($"{rows:N0} rows, one column, uncompressed, in memory; best of {rounds} (ms).");
        Console.WriteLine("| Column | Dict, stats on | Dict, stats off | No dict, stats on | No dict, stats off | No dict, no page index, stats on | No dict, no page index, stats off | Scan alone | Page bounds, 20,000 rows |");
        Console.WriteLine("|---|---|---|---|---|---|---|---|---|");

        foreach (var (name, batch, physical, typeLength, order) in Shapes(rows))
        {
            var on = ParquetWriteOptions.Default with { Compression = CompressionCodec.Uncompressed };
            var off = on with { WriteStatistics = false };
            var plainOn = on with { DictionaryEnabled = false };
            var plainOff = off with { DictionaryEnabled = false };
            var noIndexOn = plainOn with { WritePageIndex = false };
            var noIndexOff = plainOff with { WritePageIndex = false };

            // The writer scans a wide decimal in its big-endian form, so the scans here do too.
            var array = physical == PhysicalType.FixedLenByteArray && order == StatisticsOrder.SignedBigEndian
                ? DecimalNarrowing.ToBigEndian(batch.Column(0), typeLength)
                : batch.Column(0);
            int[]? levels = null;
            if (array.NullCount > 0)
            {
                levels = new int[array.Length];
                for (int i = 0; i < levels.Length; i++)
                    levels[i] = array.IsNull(i) ? 0 : 1;
            }

            double dOn = double.MaxValue, dOff = double.MaxValue, pOn = double.MaxValue, pOff = double.MaxValue, nOn = double.MaxValue, nOff = double.MaxValue;
            double scan = double.MaxValue, pages = double.MaxValue;
            for (int round = 0; round < rounds; round++)
            {
                dOn = Math.Min(dOn, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, on)));
                dOff = Math.Min(dOff, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, off)));
                pOn = Math.Min(pOn, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, plainOn)));
                pOff = Math.Min(pOff, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, plainOff)));
                nOn = Math.Min(nOn, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, noIndexOn)));
                nOff = Math.Min(nOff, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, noIndexOff)));
                scan = Math.Min(scan, Scan(array, physical, typeLength, levels, order));
                pages = Math.Min(pages, Pages(array, physical, typeLength, levels, order));
            }

            Console.WriteLine($"| {name} | {dOn:F2} | {dOff:F2} | {pOn:F2} | {pOff:F2} | {nOn:F2} | {nOff:F2} | {scan:F2} | {pages:F2} |");
        }

        return 0;
    }

    private static double Scan(IArrowArray array, PhysicalType physical, int typeLength, int[]? levels, StatisticsOrder order)
    {
        int nonNull = array.Length - array.NullCount;
        double best = double.MaxValue;
        for (int i = -3; i < 5; i++)
        {
            var clock = Stopwatch.StartNew();
            StatisticsCollector.Compute(array, physical, typeLength, levels, nonNull, array.Length, false, order);
            double ms = clock.Elapsed.TotalMilliseconds;
            if (i >= 0) best = Math.Min(best, ms);
        }

        return best;
    }

    private static double Pages(IArrowArray array, PhysicalType physical, int typeLength, int[]? levels, StatisticsOrder order)
    {
        double best = double.MaxValue;
        for (int i = -3; i < 5; i++)
        {
            var clock = Stopwatch.StartNew();
            for (int start = 0; start < array.Length; start += 20_000)
            {
                int count = Math.Min(20_000, array.Length - start);
                StatisticsCollector.ComputePageBounds(array, physical, typeLength, levels, start, count, false, order);
            }

            double ms = clock.Elapsed.TotalMilliseconds;
            if (i >= 0) best = Math.Min(best, ms);
        }

        return best;
    }

    private static IEnumerable<(string, RecordBatch, PhysicalType, int, StatisticsOrder)> Shapes(int rows)
    {
        var random = new Random(42);

        var i32 = new Int32Array.Builder();
        for (int i = 0; i < rows; i++) i32.Append(random.Next());
        yield return ("int32 random", Batch(i32.Build()), PhysicalType.Int32, 0, StatisticsOrder.Default);

        var i64 = new Int64Array.Builder();
        for (int i = 0; i < rows; i++) i64.Append(random.NextInt64());
        yield return ("int64 random", Batch(i64.Build()), PhysicalType.Int64, 0, StatisticsOrder.Default);

        var i64n = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
        {
            if (random.Next(7) == 0) i64n.AppendNull(); else i64n.Append(random.NextInt64());
        }
        yield return ("int64 random, 14% null", Batch(i64n.Build()), PhysicalType.Int64, 0, StatisticsOrder.Default);

        var f32 = new FloatArray.Builder();
        for (int i = 0; i < rows; i++) f32.Append((float)random.NextDouble());
        yield return ("float random", Batch(f32.Build()), PhysicalType.Float, 0, StatisticsOrder.Default);

        var f64 = new DoubleArray.Builder();
        for (int i = 0; i < rows; i++) f64.Append(random.NextDouble());
        yield return ("double random", Batch(f64.Build()), PhysicalType.Double, 0, StatisticsOrder.Default);

        var s = new StringArray.Builder();
        var sb = new StringBuilder();
        for (int i = 0; i < rows; i++)
        {
            sb.Clear();
            int n = 8 + random.Next(17);
            for (int c = 0; c < n; c++) sb.Append((char)('a' + random.Next(26)));
            s.Append(sb.ToString());
        }
        yield return ("string random, 8-24", Batch(s.Build()), PhysicalType.ByteArray, 0, StatisticsOrder.Default);

        var u = new StringArray.Builder();
        for (int i = 0; i < rows; i++)
            u.Append($"https://example.com/catalog/items/{random.Next(1_000_000):D7}/detail");
        yield return ("string urls, shared prefix", Batch(u.Build()), PhysicalType.ByteArray, 0, StatisticsOrder.Default);

        var d = new Decimal128Array.Builder(new Decimal128Type(38, 4));
        for (int i = 0; i < rows; i++) d.Append((decimal)random.NextInt64(-1_000_000_000_000, 1_000_000_000_000) / 10_000m);
        yield return ("decimal(38,4)", Batch(d.Build()), PhysicalType.FixedLenByteArray, 16, StatisticsOrder.SignedBigEndian);

        var uuids = new byte[rows * 16];
        random.NextBytes(uuids);
        var fsb = new Apache.Arrow.Arrays.FixedSizeBinaryArray(new ArrayData(
            new FixedSizeBinaryType(16), rows, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(uuids)]));
        yield return ("fixed_size_binary(16) random", Batch(fsb), PhysicalType.FixedLenByteArray, 16, StatisticsOrder.Default);
    }

    private static RecordBatch Batch(IArrowArray array) =>
        new(new Apache.Arrow.Schema([new Field("c", array.Data.DataType, array.NullCount > 0)], null), [array], array.Length);
}
#endif
