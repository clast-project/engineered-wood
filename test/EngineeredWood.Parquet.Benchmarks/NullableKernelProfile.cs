// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// The per-value null loops of a nullable flat column, each timed alone over the same data: what the
/// writer and reader do today (the real internals where they are reachable, faithful copies where they
/// are private) against branchless and bitmap-driven alternatives.
/// </summary>
/// <remarks>Run with: dotnet run -c Release -f net10.0 -- nullable-kernels [rows] [rounds]</remarks>
internal static class NullableKernelProfile
{
    public static int Run(string[] args)
    {
        int rows = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 1_000_000;
        int rounds = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 30;
        Console.WriteLine($"{rows:N0} doubles, random nulls; best of {rounds} warm runs, ms.");
        Console.WriteLine("| Kernel | 0% | 14% | 50% |");
        Console.WriteLine("|---|---|---|---|");

        var shares = new[] { 0.0, 1.0 / 7, 0.5 };
        var cases = shares.Select(s => new Case(NullableColumnProfile.Build("double", rows, s).Column(0))).ToArray();
        var lines = new List<(string Name, double[] Ms)>();

        void Add(string name, Func<Case, Action> body)
        {
            var ms = new double[cases.Length];
            for (int c = 0; c < cases.Length; c++)
                ms[c] = Best(rounds, body(cases[c]));
            lines.Add((name, ms));
        }

        // ---- write ----
        Add("W levels: IsNull(i) -> int[] (today)", k => () => k.Count = LevelsFromIsNull(k.Array, k.IntLevels));
        Add("W levels: bitmap -> int[]", k => () => k.Count = LevelsFromBitmap(k.Bitmap, k.Rows, k.IntLevels));
        Add("W levels: bitmap -> byte[]", k => () => k.Count = ByteLevelsFromBitmap(k.Bitmap, k.Rows, k.ByteLevels));
        Add("W gather: branchy on int[] (today)", k => () => GatherBranchy(k.Values, k.IntLevels, k.Dense));
        Add("W gather: branchless on int[]", k => () => GatherBranchless(k.Values, k.IntLevels, k.Dense));
        Add("W gather: by bitmap byte", k => () => GatherByBitmap(k.Values, k.Bitmap, k.Rows, k.Dense));
        Add("W stats: skip nulls by int[] (today)", k => () => k.Sink = MinMaxSkipping(k.Values, k.IntLevels));
        Add("W stats: over the gathered values", k => () => k.Sink = MinMaxDense(k.Dense.AsSpan(0, k.NonNull)));
        // The writer's default (BatchBitPackedRuns = false) heads every 8-value literal group; batching puts up
        // to 63 groups under one header. Both sides are timed at both settings.
        Add("W def RLE: encoder over int[] (today, 1 group/header)", k => () => EncodeLevels(k.IntLevels, 1));
        Add("W def RLE: bitmap bytes as bit-packed, 1 group/header", k => () => k.Count = EncodeFromBitmap(k.Bitmap, k.Rows, k.Encoded, 1));
        Add("W def RLE: encoder over int[], batched 63", k => () => EncodeLevels(k.IntLevels, RleBitPackedEncoder.MaxLiteralGroups));
        Add("W def RLE: bitmap bytes as bit-packed, batched 63", k => () => k.Count = EncodeFromBitmap(k.Bitmap, k.Rows, k.Encoded, RleBitPackedEncoder.MaxLiteralGroups));

        // ---- read ----
        Add("R def decode: LevelDecoder (today, 1 group/header)", k => () => LevelDecoder.DecodeV2(k.EncodedLevels, 1, k.Rows, k.ByteLevels, out k.Count));
        Add("R def decode: LevelDecoder, batched 63", k => () => LevelDecoder.DecodeV2(k.EncodedLevelsBatched, 1, k.Rows, k.ByteLevels, out k.Count));
        Add("R bitmap: BuildFromEquality (today)", k => () => BitmapHelper.BuildFromEquality(k.ByteLevels, k.OutBitmap, k.Rows, 1));
        Add("R scatter: per bit, reverse (today)", k => () => { k.ResetScatter(); ScatterPerBit(k.ScatterValues, k.Bitmap, k.Rows, k.NonNull); });
        Add("R scatter: per byte, early exit", k => () => { k.ResetScatter(); ScatterByByte(k.ScatterValues, k.Bitmap, k.Rows, k.NonNull); });
        Add("R scatter: reset copy alone", k => () => k.ResetScatter());

        foreach (var (name, ms) in lines)
            Console.WriteLine($"| {name} | {ms[0]:F3} | {ms[1]:F3} | {ms[2]:F3} |");

        Verify(cases);
        return 0;
    }

    private sealed class Case
    {
        public readonly IArrowArray Array;
        public readonly int Rows;
        public readonly int NonNull;
        public readonly byte[] Bitmap;
        public readonly double[] Values;
        public readonly int[] IntLevels;
        public readonly byte[] ByteLevels;
        public readonly double[] Dense;
        public readonly double[] ScatterValues;
        public readonly byte[] OutBitmap;
        public readonly byte[] EncodedLevels;
        public readonly byte[] EncodedLevelsBatched;
        public readonly byte[] Encoded;
        public int Count;
        public double Sink;

        public Case(IArrowArray array)
        {
            Array = array;
            Rows = array.Length;
            // A builder that saw no nulls leaves the validity buffer empty.
            var validity = array.Data.Buffers[0].Span;
            Bitmap = new byte[(Rows + 7) / 8];
            if (validity.IsEmpty)
                Bitmap.AsSpan().Fill(0xFF);
            else
                validity.Slice(0, Bitmap.Length).CopyTo(Bitmap);
            Values = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span).Slice(0, Rows).ToArray();
            IntLevels = new int[Rows];
            ByteLevels = new byte[Rows];
            Dense = new double[Rows + 1];
            ScatterValues = new double[Rows];
            OutBitmap = new byte[(Rows + 7) / 8 + 8];
            Encoded = new byte[Rows / 4 + 1024];
            NonNull = LevelsFromIsNull(array, IntLevels);
            EncodedLevels = EncodeLevels(IntLevels, 1);
            EncodedLevelsBatched = EncodeLevels(IntLevels, RleBitPackedEncoder.MaxLiteralGroups);
            GatherBranchy(Values, IntLevels, Dense);
        }

        public void ResetScatter() => Dense.AsSpan(0, NonNull).CopyTo(ScatterValues);
    }

    private static void Verify(Case[] cases)
    {
        foreach (var k in cases)
        {
            var expectInts = new int[k.Rows];
            int n = LevelsFromIsNull(k.Array, expectInts);
            Check(LevelsFromBitmap(k.Bitmap, k.Rows, k.IntLevels) == n && k.IntLevels.AsSpan().SequenceEqual(expectInts), "levels int");
            Check(ByteLevelsFromBitmap(k.Bitmap, k.Rows, k.ByteLevels) == n, "levels byte count");
            for (int i = 0; i < k.Rows; i++)
                Check(k.ByteLevels[i] == expectInts[i], "levels byte");

            var expectDense = new double[k.Rows + 1];
            GatherBranchy(k.Values, expectInts, expectDense);
            var d = new double[k.Rows + 1];
            GatherBranchless(k.Values, expectInts, d);
            Check(d.AsSpan(0, n).SequenceEqual(expectDense.AsSpan(0, n)), "gather branchless");
            d = new double[k.Rows + 1];
            GatherByBitmap(k.Values, k.Bitmap, k.Rows, d);
            Check(d.AsSpan(0, n).SequenceEqual(expectDense.AsSpan(0, n)), "gather bitmap");

            var a = new double[k.Rows];
            var b = new double[k.Rows];
            expectDense.AsSpan(0, n).CopyTo(a);
            expectDense.AsSpan(0, n).CopyTo(b);
            ScatterPerBit(a, k.Bitmap, k.Rows, n);
            ScatterByByte(b, k.Bitmap, k.Rows, n);
            for (int i = 0; i < k.Rows; i++)
            {
                if (expectInts[i] != 0)
                    Check(a[i] == k.Values[i] && b[i] == k.Values[i], "scatter");
            }

            // The bit-packed copy must decode to the same levels, at either literal batching.
            foreach (int groups in new[] { 1, RleBitPackedEncoder.MaxLiteralGroups })
            {
                int len = EncodeFromBitmap(k.Bitmap, k.Rows, k.Encoded, groups);
                var decoded = new byte[k.Rows];
                LevelDecoder.DecodeV2(k.Encoded.AsSpan(0, len), 1, k.Rows, decoded, out int m);
                Check(m == n, "encode count");
                for (int i = 0; i < k.Rows; i++)
                    Check(decoded[i] == expectInts[i], "encode levels");
                int today = EncodeLevels(expectInts, groups).Length;
                Console.WriteLine($"  {k.Rows - n} nulls, {groups} group(s)/header: encoder {today:N0} bytes, bitmap copy {len:N0} bytes");
            }
        }

        Console.WriteLine("All alternatives verified against today's output.");
    }

    private static void Check(bool ok, string what)
    {
        if (!ok)
            throw new InvalidOperationException("Mismatch: " + what);
    }

    private static double Best(int rounds, Action action)
    {
        double best = double.MaxValue;
        for (int i = -5; i < rounds; i++)
        {
            long t = Stopwatch.GetTimestamp();
            action();
            double ms = Stopwatch.GetElapsedTime(t).TotalMilliseconds;
            if (i >= 0)
                best = Math.Min(best, ms);
        }

        return best;
    }

    // ColumnChunkWriter.WriteColumn's loop.
    private static int LevelsFromIsNull(IArrowArray array, int[] levels)
    {
        int nonNull = 0;
        for (int i = 0; i < array.Length; i++)
        {
            bool isNull = array.IsNull(i);
            levels[i] = isNull ? 0 : 1;
            if (!isNull) nonNull++;
        }

        return nonNull;
    }

    private static int LevelsFromBitmap(ReadOnlySpan<byte> bitmap, int rows, int[] levels)
    {
        int nonNull = 0;
        ref int dst = ref MemoryMarshal.GetArrayDataReference(levels);
        int full = rows >> 3;
        for (int b = 0; b < full; b++)
        {
            uint bits = bitmap[b];
            nonNull += BitOperations.PopCount(bits);
            ref int d = ref Unsafe.Add(ref dst, b << 3);
            d = (int)(bits & 1);
            Unsafe.Add(ref d, 1) = (int)((bits >> 1) & 1);
            Unsafe.Add(ref d, 2) = (int)((bits >> 2) & 1);
            Unsafe.Add(ref d, 3) = (int)((bits >> 3) & 1);
            Unsafe.Add(ref d, 4) = (int)((bits >> 4) & 1);
            Unsafe.Add(ref d, 5) = (int)((bits >> 5) & 1);
            Unsafe.Add(ref d, 6) = (int)((bits >> 6) & 1);
            Unsafe.Add(ref d, 7) = (int)(bits >> 7);
        }

        for (int i = full << 3; i < rows; i++)
        {
            int v = (bitmap[i >> 3] >> (i & 7)) & 1;
            levels[i] = v;
            nonNull += v;
        }

        return nonNull;
    }

    private static readonly ulong[] SpreadBits = BuildSpreadBits();

    private static ulong[] BuildSpreadBits()
    {
        var table = new ulong[256];
        for (int b = 0; b < 256; b++)
        {
            for (int j = 0; j < 8; j++)
                table[b] |= (ulong)((b >> j) & 1) << (8 * j);
        }

        return table;
    }

    private static int ByteLevelsFromBitmap(ReadOnlySpan<byte> bitmap, int rows, byte[] levels)
    {
        // One bitmap byte -> eight level bytes as one 64-bit store from a 256-entry table.
        int nonNull = 0;
        ref byte dst = ref MemoryMarshal.GetArrayDataReference(levels);
        int full = rows >> 3;
        for (int b = 0; b < full; b++)
        {
            ulong bits = bitmap[b];
            nonNull += BitOperations.PopCount(bits);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref dst, b << 3), SpreadBits[(int)bits]);
        }

        for (int i = full << 3; i < rows; i++)
        {
            int v = (bitmap[i >> 3] >> (i & 7)) & 1;
            levels[i] = (byte)v;
            nonNull += v;
        }

        return nonNull;
    }

    // ColumnChunkWriter.ExtractNonNullValues.
    private static void GatherBranchy(double[] values, int[] levels, double[] dense)
    {
        int idx = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (levels[i] != 0)
                dense[idx++] = values[i];
        }
    }

    private static void GatherBranchless(double[] values, int[] levels, double[] dense)
    {
        ref double dst = ref MemoryMarshal.GetArrayDataReference(dense);
        int idx = 0;
        for (int i = 0; i < values.Length; i++)
        {
            Unsafe.Add(ref dst, idx) = values[i];
            idx += levels[i] != 0 ? 1 : 0;
        }
    }

    private static void GatherByBitmap(double[] values, ReadOnlySpan<byte> bitmap, int rows, double[] dense)
    {
        ref double src = ref MemoryMarshal.GetArrayDataReference(values);
        ref double dst = ref MemoryMarshal.GetArrayDataReference(dense);
        int idx = 0;
        int full = rows >> 3;
        for (int b = 0; b < full; b++)
        {
            uint bits = bitmap[b];
            ref double s = ref Unsafe.Add(ref src, b << 3);
            if (bits == 0xFF)
            {
                Unsafe.CopyBlockUnaligned(ref Unsafe.As<double, byte>(ref Unsafe.Add(ref dst, idx)), ref Unsafe.As<double, byte>(ref s), 64);
                idx += 8;
            }
            else if (bits != 0)
            {
                for (int j = 0; j < 8; j++)
                {
                    Unsafe.Add(ref dst, idx) = Unsafe.Add(ref s, j);
                    idx += (int)((bits >> j) & 1);
                }
            }
        }

        for (int i = full << 3; i < rows; i++)
        {
            Unsafe.Add(ref dst, idx) = values[i];
            idx += (bitmap[i >> 3] >> (i & 7)) & 1;
        }
    }

    // StatisticsCollector.ComputeDoubleMinMax without the NaN and signed-zero handling, which both sides keep.
    private static double MinMaxSkipping(double[] values, int[] levels)
    {
        bool first = true;
        double min = 0, max = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (levels[i] == 0) continue;
            double v = values[i];
            if (double.IsNaN(v)) continue;
            if (first) { min = max = v; first = false; }
            else
            {
                if (v < min || (v == 0.0 && min == 0.0 && double.IsNegative(v))) min = v;
                if (v > max || (v == 0.0 && max == 0.0 && !double.IsNegative(v))) max = v;
            }
        }

        return min + max;
    }

    private static double MinMaxDense(ReadOnlySpan<double> values)
    {
        bool first = true;
        double min = 0, max = 0;
        for (int i = 0; i < values.Length; i++)
        {
            double v = values[i];
            if (double.IsNaN(v)) continue;
            if (first) { min = max = v; first = false; }
            else
            {
                if (v < min || (v == 0.0 && min == 0.0 && double.IsNegative(v))) min = v;
                if (v > max || (v == 0.0 && max == 0.0 && !double.IsNegative(v))) max = v;
            }
        }

        return min + max;
    }

    private static byte[] EncodeLevels(int[] levels, int maxLiteralGroups)
    {
        var encoder = new RleBitPackedEncoder(1, maxLiteralGroups: maxLiteralGroups);
        encoder.Encode(levels);
        return encoder.ToArray();
    }

    /// <summary>
    /// Width-1 levels straight from the validity bitmap: Parquet bit-packs LSB first, as Arrow's bitmap is laid
    /// out, so a literal run is the bitmap's bytes. Runs of 8+ all-set or all-clear bytes become RLE runs.
    /// </summary>
    private static int EncodeFromBitmap(ReadOnlySpan<byte> bitmap, int rows, Span<byte> output, int maxLiteralGroups)
    {
        const int MinRleBytes = 8;
        int pos = 0;
        int full = rows >> 3;
        int b = 0;
        int literalStart = 0;
        while (b < full)
        {
            byte v = bitmap[b];
            if (v is 0 or 0xFF)
            {
                int e = b + 1;
                while (e < full && bitmap[e] == v) e++;
                if (e - b >= MinRleBytes)
                {
                    pos = EmitLiteral(bitmap, literalStart, b, output, pos, maxLiteralGroups);
                    pos = WriteVarint(output, pos, (uint)((e - b) * 8) << 1);
                    output[pos++] = (byte)(v & 1);
                    literalStart = e;
                }

                b = e;
            }
            else
            {
                b++;
            }
        }

        // The tail (and any partial byte) goes in the last literal run; its padding bits are past the count.
        int end = (rows + 7) >> 3;
        pos = EmitLiteral(bitmap, literalStart, end, output, pos, maxLiteralGroups);
        return pos;
    }

    private static int EmitLiteral(ReadOnlySpan<byte> bitmap, int start, int end, Span<byte> output, int pos, int maxLiteralGroups)
    {
        while (start < end)
        {
            int groups = Math.Min(end - start, maxLiteralGroups);
            output[pos++] = (byte)((groups << 1) | 1);
            bitmap.Slice(start, groups).CopyTo(output.Slice(pos));
            pos += groups;
            start += groups;
        }

        return pos;
    }

    private static int WriteVarint(Span<byte> output, int pos, uint value)
    {
        while (value >= 0x80)
        {
            output[pos++] = (byte)(value | 0x80);
            value >>= 7;
        }

        output[pos++] = (byte)value;
        return pos;
    }

    // ArrowArrayBuilder.BuildFixedArray's loop.
    private static void ScatterPerBit(double[] values, ReadOnlySpan<byte> bitmap, int rows, int nonNull)
    {
        int readIdx = nonNull - 1;
        for (int i = rows - 1; i >= 0 && readIdx >= 0; i--)
        {
            if ((bitmap[i >> 3] & (1 << (i & 7))) != 0)
                values[i] = values[readIdx--];
        }
    }

    private static void ScatterByByte(double[] values, ReadOnlySpan<byte> bitmap, int rows, int nonNull)
    {
        ref double v = ref MemoryMarshal.GetArrayDataReference(values);
        int readIdx = nonNull - 1;
        int i = rows - 1;

        // Partial last byte, bit by bit.
        for (; i >= 0 && (i & 7) != 7 && readIdx < i; i--)
        {
            if ((bitmap[i >> 3] & (1 << (i & 7))) != 0)
                Unsafe.Add(ref v, i) = Unsafe.Add(ref v, readIdx--);
        }

        // Whole bytes, highest first. Once readIdx == i every remaining value is already in place.
        while (i >= 7 && readIdx < i)
        {
            uint bits = bitmap[i >> 3];
            int baseRow = i - 7;
            if (bits == 0xFF)
            {
                // Destination is above the source, so a backward-safe move.
                values.AsSpan(readIdx - 7, 8).CopyTo(values.AsSpan(baseRow, 8));
                readIdx -= 8;
            }
            else if (bits != 0)
            {
                for (int j = 7; j >= 0; j--)
                {
                    uint bit = (bits >> j) & 1;
                    Unsafe.Add(ref v, baseRow + j) = Unsafe.Add(ref v, Math.Max(readIdx, 0));
                    readIdx -= (int)bit;
                }
            }

            i -= 8;
        }
    }
}
#endif
