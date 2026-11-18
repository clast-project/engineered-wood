// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using EngineeredWood.Encodings;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Prototype kernels for DELTA_BINARY_PACKED, each checked against the shipped encoder or decoder before it is
/// timed: an encoder that packs a 64-bit word at a time (byte-identical output), and a split of the decoder's
/// time between unpacking and the running sum.
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

        var output = new byte[expected.Length + 64];
        int length = long64 ? EncodeWords64(values, output) : EncodeWords32(narrow, output);
        if (!output.AsSpan(0, length).SequenceEqual(expected))
            throw new InvalidOperationException($"{name}: word encoder differs from the shipped one");

        double encShipped = Time(() => { shipped.Reset(); if (long64) shipped.EncodeInt64s(values); else shipped.EncodeInt32s(narrow); });
        double encWords = Time(() => { if (long64) EncodeWords64(values, output); else EncodeWords32(narrow, output); });

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

        Console.WriteLine($"| {name} | {encShipped:F2} | {encWords:F2} | {decShipped:F2} | {unpackOnly:F2} | {fused:F2} |");
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

    // ---- Encoder: the shipped format, packed a 64-bit word at a time ----

    private static int EncodeWords32(int[] values, byte[] output)
    {
        int pos = Header(output, values.Length, values.Length == 0 ? 0 : values[0]);
        Span<long> deltas = stackalloc long[BlockSize];
        int previous = values.Length == 0 ? 0 : values[0];
        for (int s = 1; s < values.Length;)
        {
            int n = Math.Min(BlockSize, values.Length - s);
            for (int i = 0; i < n; i++)
            {
                int v = values[s + i];
                deltas[i] = unchecked(v - previous);
                previous = v;
            }

            pos = Block(deltas.Slice(0, n), output, pos);
            s += n;
        }

        return pos;
    }

    private static int EncodeWords64(long[] values, byte[] output)
    {
        int pos = Header(output, values.Length, values.Length == 0 ? 0 : values[0]);
        Span<long> deltas = stackalloc long[BlockSize];
        long previous = values.Length == 0 ? 0 : values[0];
        for (int s = 1; s < values.Length;)
        {
            int n = Math.Min(BlockSize, values.Length - s);
            for (int i = 0; i < n; i++)
            {
                long v = values[s + i];
                deltas[i] = unchecked(v - previous);
                previous = v;
            }

            pos = Block(deltas.Slice(0, n), output, pos);
            s += n;
        }

        return pos;
    }

    private static int Header(byte[] output, int count, long first)
    {
        int pos = 0;
        pos += Varint.WriteUnsigned(output.AsSpan(pos), BlockSize);
        pos += Varint.WriteUnsigned(output.AsSpan(pos), MiniblockCount);
        pos += Varint.WriteUnsigned(output.AsSpan(pos), (ulong)count);
        pos += Varint.WriteSigned(output.AsSpan(pos), first);
        return pos;
    }

    private static int Block(ReadOnlySpan<long> deltas, byte[] output, int pos)
    {
        int n = deltas.Length;
        long min = long.MaxValue;
        for (int i = 0; i < n; i++)
            min = Math.Min(min, deltas[i]);
        pos += Varint.WriteSigned(output.AsSpan(pos), min);

        // A miniblock's width is the bit length of its largest remainder, which is that of their OR.
        Span<int> widths = stackalloc int[MiniblockCount];
        for (int mb = 0; mb < MiniblockCount; mb++)
        {
            ulong or = 0;
            int end = Math.Min(n, (mb + 1) * PerMiniblock);
            for (int i = mb * PerMiniblock; i < end; i++)
                or |= unchecked((ulong)(deltas[i] - min));
            widths[mb] = 64 - BitOperations.LeadingZeroCount(or);
            output[pos + mb] = (byte)widths[mb];
        }

        pos += MiniblockCount;
        for (int mb = 0; mb < MiniblockCount; mb++)
        {
            int w = widths[mb];
            if (w == 0)
                continue;

            ulong acc = 0;
            int bits = 0;
            int start = mb * PerMiniblock;
            for (int i = 0; i < PerMiniblock; i++)
            {
                ulong v = start + i < n ? unchecked((ulong)(deltas[start + i] - min)) : 0;
                acc |= v << bits;
                bits += w;
                if (bits >= 64)
                {
                    BinaryPrimitives.WriteUInt64LittleEndian(output.AsSpan(pos), acc);
                    pos += 8;
                    bits -= 64;
                    acc = bits == 0 ? 0 : v >> (w - bits);
                }
            }

            // 32 values of w bits are 4w bytes: whatever is left is a whole number of bytes.
            for (; bits > 0; bits -= 8, acc >>= 8)
                output[pos++] = (byte)acc;
        }

        return pos;
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
