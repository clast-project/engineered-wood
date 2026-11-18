// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Write time, read time and file size of the same data with <see cref="ParquetWriteOptions.BatchBitPackedRuns"/>
/// off and on, over the streams the RLE/bit-packed encoder writes: dictionary indices (random and clustered, at
/// two widths, with and without nulls), repetition levels (fixed and ragged lists), booleans, and the
/// definition levels of a plain nullable column.
/// </summary>
/// <remarks>
/// Run with: dotnet run -c Release -f net10.0 -- batched-write [rounds] [rows]. The two settings are timed
/// alternately, round by round, so drift hits both alike; each cell is the best round. Uncompressed and in
/// memory, so the encoder's share is as large as it gets.
/// </remarks>
internal static class BatchedRunsWriteProfile
{
    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 15;
        int rows = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1_000_000;

        Console.WriteLine($"{rows:N0} rows, one column, uncompressed, in memory; best of {rounds} alternating rounds (ms).");
        Console.WriteLine("| Column | Write off | Write on | Read off | Read on | Bytes off | Bytes on | Size |");
        Console.WriteLine("|---|---|---|---|---|---|---|---|");

        foreach (var (name, build, dictionary) in Shapes())
        {
            var batch = build(rows);
            var off = ParquetWriteOptions.Default with
            {
                Compression = CompressionCodec.Uncompressed,
                DictionaryEnabled = dictionary,
                BatchBitPackedRuns = false,
            };
            var on = off with { BatchBitPackedRuns = true };
            byte[] fileOff = await NullableColumnProfile.WriteAsync(batch, off);
            byte[] fileOn = await NullableColumnProfile.WriteAsync(batch, on);

            double wOff = double.MaxValue, wOn = double.MaxValue, rOff = double.MaxValue, rOn = double.MaxValue;
            for (int round = 0; round < rounds; round++)
            {
                wOff = Math.Min(wOff, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, off)));
                wOn = Math.Min(wOn, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, on)));
                rOff = Math.Min(rOff, NullableColumnProfile.Best(1, () => NullableColumnProfile.ReadAsync(fileOff)));
                rOn = Math.Min(rOn, NullableColumnProfile.Best(1, () => NullableColumnProfile.ReadAsync(fileOn)));
            }

            Console.WriteLine(
                $"| {name} | {wOff:F2} | {wOn:F2} | {rOff:F2} | {rOn:F2} | {fileOff.Length:N0} | {fileOn.Length:N0} | {100.0 * fileOn.Length / fileOff.Length:F1}% |");
        }

        return 0;
    }

    private static IEnumerable<(string Name, Func<int, RecordBatch> Build, bool Dictionary)> Shapes()
    {
        yield return ("dict 1000, random", rows => Strings(rows, 1000, run: 1, nullShare: 0), true);
        yield return ("dict 1000, random, 14% null", rows => Strings(rows, 1000, run: 1, nullShare: 1.0 / 7), true);
        yield return ("dict 16, random", rows => Strings(rows, 16, run: 1, nullShare: 0), true);
        yield return ("dict 1000, runs of 1-20", rows => Strings(rows, 1000, run: 20, nullShare: 0), true);
        yield return ("list<int64> length 3", rows => Lists(rows, fixedLength: 3), false);
        yield return ("list<int64> length 0-5", rows => Lists(rows, fixedLength: -1), false);
        yield return ("bool, 14% null", rows => Booleans(rows, 1.0 / 7), false);
        yield return ("double, 14% null", rows => NullableColumnProfile.Build("double", rows, 1.0 / 7), false);
    }

    private static RecordBatch Strings(int rows, int cardinality, int run, double nullShare)
    {
        var random = new Random(42);
        var b = new StringArray.Builder();
        int i = 0;
        while (i < rows)
        {
            int length = Math.Min(rows - i, random.Next(1, run + 1));
            string value = "category-" + random.Next(cardinality).ToString(CultureInfo.InvariantCulture);
            for (int j = 0; j < length; j++, i++)
            {
                if (random.NextDouble() < nullShare) b.AppendNull();
                else b.Append(value);
            }
        }

        return Batch(b.Build(), nullable: nullShare > 0);
    }

    private static RecordBatch Lists(int rows, int fixedLength)
    {
        var random = new Random(42);
        var b = new ListArray.Builder(Int64Type.Default);
        var values = (Int64Array.Builder)b.ValueBuilder;
        for (int i = 0; i < rows; i++)
        {
            b.Append();
            int n = fixedLength >= 0 ? fixedLength : random.Next(0, 6);
            for (int j = 0; j < n; j++)
                values.Append(random.Next(1_000_000));
        }

        return Batch(b.Build(), nullable: true);
    }

    private static RecordBatch Booleans(int rows, double nullShare)
    {
        var random = new Random(42);
        var b = new BooleanArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            if (random.NextDouble() < nullShare) b.AppendNull();
            else b.Append(random.Next(2) == 0);
        }

        return Batch(b.Build(), nullable: true);
    }

    private static RecordBatch Batch(IArrowArray array, bool nullable) =>
        new(new Apache.Arrow.Schema([new Field("c", array.Data.DataType, nullable)], null), [array], array.Length);
}
#endif
