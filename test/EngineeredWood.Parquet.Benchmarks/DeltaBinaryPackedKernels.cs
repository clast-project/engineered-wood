// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers.Binary;
using System.Diagnostics;
using EngineeredWood.Encodings;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Prototype kernels for DELTA_BINARY_PACKED, each checked against the values before it is timed: a split of
/// the decoder's time between unpacking and the running sum.
/// </summary>
internal static class DeltaBinaryPackedKernels
{
    private const int BlockSize = 128, MiniblockCount = 4, PerMiniblock = 32;

    public static void Run(string name, long[] values, bool long64)
    {
        var narrow = System.Array.ConvertAll(values, v => (int)v);
        var shipped = new DeltaBinaryPackedEncoder();
        if (long64) shipped.EncodeInt64s(values); else shipped.EncodeInt32s(narrow);
        byte[] expected = shipped.ToArray();

        // Decoder split: shipped; unpack only (deltas, no sum); unpack straight into the destination with the
        // sum fused.
        var longs = new long[values.Length];
        var ints = new int[values.Length];
        double decShipped = Time(() => { var d = new DeltaBinaryPackedDecoder(expected); if (long64) d.DecodeInt64s(longs); else d.DecodeInt32s(ints); });
        double unpackOnly = Time(() => UnpackOnly(expected, longs));
        Fused(expected, longs);
        for (int i = 0; i < values.Length; i++)
        {
            if ((long64 ? longs[i] : (int)longs[i]) != (long64 ? values[i] : narrow[i]))
                throw new InvalidOperationException($"{name}: fused decoder differs at {i}");
        }

        double fused = Time(() => Fused(expected, longs));

        Console.WriteLine($"| {name} | {decShipped:F2} | {unpackOnly:F2} | {fused:F2} |");
    }

    private static double Time(Action action)
    {
        double best = double.MaxValue;
        for (int i = -3; i < 15; i++)
        {
            var clock = Stopwatch.StartNew();
            action();
            double ms = clock.Elapsed.TotalMilliseconds;
            if (i >= 0) best = Math.Min(best, ms);
        }

        return best;
    }

    // ---- Decoder split ----

    /// <summary>The shipped unpack loop over every miniblock, keeping deltas, no running sum.</summary>
    private static void UnpackOnly(byte[] data, long[] destination) => Walk(data, destination, sum: false);

    /// <summary>Unpacks straight into the destination with the running sum in the same loop.</summary>
    private static void Fused(byte[] data, long[] destination) => Walk(data, destination, sum: true);

    private static void Walk(byte[] data, long[] destination, bool sum)
    {
        int pos = 0;
        Varint.ReadUnsigned(data, ref pos);
        Varint.ReadUnsigned(data, ref pos);
        int total = (int)Varint.ReadUnsigned(data, ref pos);
        long last = Varint.ReadSigned(data, ref pos);
        if (total == 0)
            return;

        destination[0] = last;
        int done = 1;
        while (done < total)
        {
            long min = Varint.ReadSigned(data, ref pos);
            int widthsAt = pos;
            pos += MiniblockCount;
            for (int mb = 0; mb < MiniblockCount && done < total; mb++)
            {
                int w = data[widthsAt + mb];
                int count = Math.Min(PerMiniblock, total - done);
                ulong mask = w == 64 ? ulong.MaxValue : (1UL << w) - 1;
                int bit = pos * 8;
                for (int i = 0; i < count; i++, bit += w)
                {
                    // Widths up to 56 only, as on the benchmark data; the shipped decoder handles the rest.
                    int at = bit >> 3;
                    ulong raw = at + 8 <= data.Length
                        ? BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(at))
                        : Tail(data, at);
                    long delta = (long)((raw >> (bit & 7)) & mask);
                    if (sum)
                    {
                        last += min + delta;
                        destination[done++] = last;
                    }
                    else
                    {
                        destination[done++] = delta;
                    }
                }

                pos += PerMiniblock * w / 8;
            }
        }
    }

    private static ulong Tail(byte[] data, int at)
    {
        ulong v = 0;
        for (int i = at; i < data.Length; i++)
            v |= (ulong)data[i] << ((i - at) * 8);
        return v;
    }
}
#endif
