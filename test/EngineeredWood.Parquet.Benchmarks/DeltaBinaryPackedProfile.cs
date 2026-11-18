// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using EngineeredWood.Compression;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Where the time goes reading a DELTA_BINARY_PACKED integer column (EW's default integer encoding): the
/// same data read as DELTA_BINARY_PACKED and as PLAIN (the floor without decoding), and the decoder alone
/// over the same values.
/// </summary>
/// <remarks>
/// Run with: dotnet run -c Release -f net10.0 -- delta-profile [rounds] [rows]. One column, not nullable,
/// uncompressed, dictionary off, in memory. The two encodings are read alternately round by round; each
/// cell is the best round.
/// </remarks>
internal static class DeltaBinaryPackedProfile
{
    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 15;
        int rows = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1_000_000;

        Console.WriteLine($"{rows:N0} rows, one column, uncompressed, dictionary off, in memory; best of {rounds} (ms).");
        Console.WriteLine("| Column | Read PLAIN | Read DELTA | Decoder alone | Write PLAIN | Write DELTA | Encoder alone | Bytes PLAIN | Bytes DELTA |");
        Console.WriteLine("|---|---|---|---|---|---|---|---|---|");

        foreach (var (name, long64, values) in Shapes(rows))
        {
            var batch = Batch(values, long64);
            var plain = ParquetWriteOptions.Default with
            {
                Compression = CompressionCodec.Uncompressed,
                DictionaryEnabled = false,
                IntegerEncoding = IntegerEncoding.Plain,
            };
            var delta = plain with { IntegerEncoding = IntegerEncoding.DeltaBinaryPacked };
            byte[] filePlain = await NullableColumnProfile.WriteAsync(batch, plain);
            byte[] fileDelta = await NullableColumnProfile.WriteAsync(batch, delta);

            // The decoder alone, over one stream of every value: no pages, no Arrow build.
            var encoder = new DeltaBinaryPackedEncoder();
            if (long64) encoder.EncodeInt64s(values);
            else encoder.EncodeInt32s(System.Array.ConvertAll(values, v => (int)v));
            byte[] stream = encoder.ToArray();
            var longs = new long[values.Length];
            var ints = new int[values.Length];
            Verify(stream, values, long64, longs, ints);

            double rPlain = double.MaxValue, rDelta = double.MaxValue, decode = double.MaxValue;
            double wPlain = double.MaxValue, wDelta = double.MaxValue, encode = double.MaxValue;
            var narrow = System.Array.ConvertAll(values, v => (int)v);
            for (int round = 0; round < rounds; round++)
            {
                rPlain = Math.Min(rPlain, NullableColumnProfile.Best(1, () => NullableColumnProfile.ReadAsync(filePlain)));
                rDelta = Math.Min(rDelta, NullableColumnProfile.Best(1, () => NullableColumnProfile.ReadAsync(fileDelta)));
                decode = Math.Min(decode, Decode(stream, long64, longs, ints));
                wPlain = Math.Min(wPlain, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, plain)));
                wDelta = Math.Min(wDelta, NullableColumnProfile.Best(1, () => NullableColumnProfile.DiscardWriteAsync(batch, delta)));
                encode = Math.Min(encode, Encode(encoder, values, narrow, long64));
            }

            Console.WriteLine(
                $"| {name} | {rPlain:F2} | {rDelta:F2} | {decode:F2} | {wPlain:F2} | {wDelta:F2} | {encode:F2} | {filePlain.Length:N0} | {fileDelta.Length:N0} |");
        }

        Console.WriteLine();
        Console.WriteLine("Prototype kernels, ms per stream (int64 full width left out: widths over 56 take another path).");
        Console.WriteLine("| Column | Encode shipped | Encode by word | Decode shipped | Unpack only | Unpack + sum fused |");
        Console.WriteLine("|---|---|---|---|---|---|");
        foreach (var (name, long64, values) in Shapes(rows))
        {
            if (!(long64 && name.Contains("full width")))
                DeltaBinaryPackedKernels.Run(name, values, long64);
        }

        return 0;
    }

    private static double Encode(DeltaBinaryPackedEncoder encoder, long[] values, int[] narrow, bool long64)
    {
        double best = double.MaxValue;
        for (int i = -3; i < 5; i++)
        {
            encoder.Reset();
            var clock = Stopwatch.StartNew();
            if (long64) encoder.EncodeInt64s(values);
            else encoder.EncodeInt32s(narrow);
            double ms = clock.Elapsed.TotalMilliseconds;
            if (i >= 0) best = Math.Min(best, ms);
        }

        return best;
    }

    private static double Decode(byte[] stream, bool long64, long[] longs, int[] ints)
    {
        double best = double.MaxValue;
        for (int i = -3; i < 5; i++)
        {
            var clock = Stopwatch.StartNew();
            var decoder = new DeltaBinaryPackedDecoder(stream);
            if (long64) decoder.DecodeInt64s(longs);
            else decoder.DecodeInt32s(ints);
            double ms = clock.Elapsed.TotalMilliseconds;
            if (i >= 0) best = Math.Min(best, ms);
        }

        return best;
    }

    private static void Verify(byte[] stream, long[] values, bool long64, long[] longs, int[] ints)
    {
        Decode(stream, long64, longs, ints);
        for (int i = 0; i < values.Length; i++)
        {
            if ((long64 ? longs[i] : ints[i]) != (long64 ? values[i] : (int)values[i]))
                throw new InvalidOperationException($"decode mismatch at {i}");
        }
    }

    private static IEnumerable<(string Name, bool Long64, long[] Values)> Shapes(int rows)
    {
        foreach (bool long64 in new[] { false, true })
        {
            string t = long64 ? "int64" : "int32";
            var random = new Random(42);

            yield return ($"{t} ids, step 1 (width 0)", long64, Fill(rows, i => 1_000 + i));
            yield return ($"{t} timestamps, jitter 0-255", long64, Running(rows, () => 1_000 + random.Next(256), long64));
            yield return ($"{t} random deltas, 24-bit", long64, Running(rows, () => random.Next(-8_000_000, 8_000_000), long64));
            yield return ($"{t} random values, full width", long64,
                Fill(rows, _ => long64 ? random.NextInt64() : random.Next(int.MinValue, int.MaxValue)));
        }
    }

    private static long[] Fill(int rows, Func<int, long> value)
    {
        var values = new long[rows];
        for (int i = 0; i < rows; i++)
            values[i] = value(i);
        return values;
    }

    private static long[] Running(int rows, Func<long> delta, bool long64)
    {
        var values = new long[rows];
        long v = long64 ? 1_700_000_000_000_000L : 0;
        for (int i = 0; i < rows; i++)
        {
            v += delta();
            values[i] = long64 ? v : (int)v;
        }

        return values;
    }

    private static RecordBatch Batch(long[] values, bool long64)
    {
        IArrowArray array;
        if (long64)
        {
            var b = new Int64Array.Builder();
            b.AppendRange(values);
            array = b.Build();
        }
        else
        {
            var b = new Int32Array.Builder();
            foreach (long v in values) b.Append((int)v);
            array = b.Build();
        }

        return new RecordBatch(new Apache.Arrow.Schema([new Field("c", array.Data.DataType, false)], null), [array], array.Length);
    }
}
#endif
