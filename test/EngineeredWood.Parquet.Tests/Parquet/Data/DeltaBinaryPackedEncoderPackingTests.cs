// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Encodings;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The DELTA_BINARY_PACKED encoder packs a 64-bit word at a time. Its output must be byte for byte what
/// packing a value at a time, a byte at a time, wrote before: checked here against a straightforward
/// reference encoder at every bit width, with miniblocks of different widths and partial last blocks,
/// and against the decoder.
/// </summary>
public class DeltaBinaryPackedEncoderPackingTests
{
    private const int BlockSize = 128, MiniblockCount = 4, PerMiniblock = 32;

    [Fact]
    public void ExactBytes_OneBitMiniblock()
    {
        // Deltas 1 and 2: min delta 1, remainders 0 and 1, so the first miniblock is 1 bit wide and
        // packs to 32 bits = 4 bytes, the second remainder in bit 1.
        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt32s(new[] { 0, 1, 3 });

        byte[] expected =
        [
            0x80, 0x01, 0x04, 0x03, 0x00, // block size 128, 4 miniblocks, 3 values, first value 0
            0x02, 0x01, 0x00, 0x00, 0x00, // min delta 1 (zigzag), widths 1, 0, 0, 0
            0x02, 0x00, 0x00, 0x00,       // remainders 0, 1, then 30 zeros of padding
        ];
        Assert.Equal(expected, encoder.ToArray());
    }

    [Fact]
    public void ExactBytes_ValueStraddlesAWord()
    {
        // Remainders 0, 1, ..., 7 repeating, so width 3: remainder 21 (5, binary 101) takes bits 63-65,
        // its low bit in the first word and its high bits in the next.
        var values = new long[1 + PerMiniblock];
        for (int i = 1; i < values.Length; i++)
            values[i] = values[i - 1] + 10 + (i - 1) % 8;

        var encoder = new DeltaBinaryPackedEncoder();
        encoder.EncodeInt64s(values);
        byte[] bytes = encoder.ToArray();

        // Header 5 bytes (first value 0), min delta 10 (zigzag 20), widths 3, 0, 0, 0.
        Assert.Equal(new byte[] { 0x14, 0x03, 0x00, 0x00, 0x00 }, bytes.AsSpan(5, 5).ToArray());

        var packed = bytes.AsSpan(10).ToArray();
        Assert.Equal(12, packed.Length);
        for (int i = 0; i < PerMiniblock; i++)
        {
            int expected = i % 8;
            int actual = 0;
            for (int b = 0; b < 3; b++)
            {
                int bit = i * 3 + b;
                actual |= ((packed[bit >> 3] >> (bit & 7)) & 1) << b;
            }

            Assert.Equal(expected, actual);
        }
    }

    public static TheoryData<int> Widths64()
    {
        var data = new TheoryData<int>();
        for (int w = 0; w <= 64; w++)
            data.Add(w);
        return data;
    }

    public static TheoryData<int> Widths32()
    {
        var data = new TheoryData<int>();
        for (int w = 0; w <= 32; w++)
            data.Add(w);
        return data;
    }

    [Theory]
    [MemberData(nameof(Widths64))]
    public void Int64_EveryWidth_MatchesReference(int width)
    {
        var random = new Random(width);
        foreach (int count in new[] { 1, 2, 33, 64, 97, 128, 129, 130, 161, 257, 1000 })
        {
            long[] values = Int64s(random, count, () => width);
            AssertMatchesReference(values, long64: true);
        }
    }

    [Theory]
    [MemberData(nameof(Widths32))]
    public void Int32_EveryWidth_MatchesReference(int width)
    {
        var random = new Random(1000 + width);
        foreach (int count in new[] { 1, 2, 33, 64, 97, 128, 129, 130, 161, 257, 1000 })
        {
            long[] values = Int32s(random, count, () => width);
            AssertMatchesReference(values, long64: false);
        }
    }

    [Fact]
    public void MixedWidthsPerMiniblock_MatchReference()
    {
        // A width drawn afresh every 32 deltas, so the miniblocks of a block differ, including 0 and
        // the widths whose last word is partial.
        var random = new Random(7);
        for (int round = 0; round < 50; round++)
        {
            int count = random.Next(1, 2000);
            int width = 0, n = 0;
            int Next64() { if (n++ % PerMiniblock == 0) width = random.Next(65); return width; }
            int Next32() { if (n++ % PerMiniblock == 0) width = random.Next(33); return width; }

            AssertMatchesReference(Int64s(random, count, Next64), long64: true);
            n = 0;
            AssertMatchesReference(Int32s(random, count, Next32), long64: false);
        }
    }

    [Fact]
    public void ReusedEncoder_MatchesFresh()
    {
        // Packing writes every byte it covers rather than ORing into cleared ones, so the stale bytes of
        // an earlier, wider encode must not leak into a reused buffer.
        var random = new Random(11);
        var reused = new DeltaBinaryPackedEncoder();
        reused.EncodeInt64s(Int64s(random, 500, () => 64));

        long[] values = Int64s(random, 500, () => 5);
        reused.Reset();
        reused.EncodeInt64s(values);

        var fresh = new DeltaBinaryPackedEncoder();
        fresh.EncodeInt64s(values);
        Assert.Equal(fresh.ToArray(), reused.ToArray());
    }

    private static void AssertMatchesReference(long[] values, bool long64)
    {
        var encoder = new DeltaBinaryPackedEncoder(16);
        if (long64)
            encoder.EncodeInt64s(values);
        else
            encoder.EncodeInt32s(Array.ConvertAll(values, v => (int)v));

        byte[] actual = encoder.ToArray();
        Assert.Equal(Reference(values, long64), actual);

        if (long64)
        {
            var decoded = new long[values.Length];
            new DeltaBinaryPackedDecoder(actual).DecodeInt64s(decoded);
            Assert.Equal(values, decoded);
        }
        else
        {
            var decoded = new int[values.Length];
            new DeltaBinaryPackedDecoder(actual).DecodeInt32s(decoded);
            Assert.Equal(Array.ConvertAll(values, v => (int)v), decoded);
        }
    }

    /// <summary>Values whose deltas, less the block's minimum, are up to <paramref name="width"/> bits.</summary>
    private static long[] Int64s(Random random, int count, Func<int> width)
    {
        var values = new long[count];
        values[0] = (long)Bits(random, 64);
        for (int i = 1; i < count; i++)
            values[i] = unchecked(values[i - 1] + (long)Bits(random, width()));
        return values;
    }

    /// <summary>As <see cref="Int64s"/>, but 32-bit: deltas wrap at 32 bits.</summary>
    private static long[] Int32s(Random random, int count, Func<int> width)
    {
        var values = new long[count];
        values[0] = random.Next(int.MinValue, int.MaxValue);
        for (int i = 1; i < count; i++)
            values[i] = unchecked((int)values[i - 1] + (int)Bits(random, Math.Min(32, width())));
        return values;
    }

    /// <summary>A random value of exactly <paramref name="width"/> bits, its top bit set.</summary>
    private static ulong Bits(Random random, int width)
    {
        if (width == 0)
            return 0;
        var bytes = new byte[8];
        random.NextBytes(bytes);
        ulong v = BitConverter.ToUInt64(bytes, 0);
        if (width < 64)
            v &= (1UL << width) - 1;
        return v | 1UL << (width - 1);
    }

    /// <summary>
    /// The encoding as written before word packing: each remainder ORed into cleared bytes one byte at a
    /// time. Deltas wrap at 32 bits for INT32 and at 64 for INT64.
    /// </summary>
    private static byte[] Reference(long[] values, bool long64)
    {
        var output = new List<byte>();
        void Varint(ulong v) { for (; v >= 0x80; v >>= 7) output.Add((byte)(v | 0x80)); output.Add((byte)v); }
        void ZigZag(long v) => Varint(unchecked((ulong)((v << 1) ^ (v >> 63))));

        Varint(BlockSize);
        Varint(MiniblockCount);
        Varint((ulong)values.Length);
        ZigZag(values[0]);

        var deltas = new long[values.Length - 1];
        for (int i = 1; i < values.Length; i++)
            deltas[i - 1] = long64 ? unchecked(values[i] - values[i - 1]) : unchecked((int)values[i] - (int)values[i - 1]);

        for (int s = 0; s < deltas.Length; s += BlockSize)
        {
            int n = Math.Min(BlockSize, deltas.Length - s);
            long min = long.MaxValue;
            for (int i = 0; i < n; i++)
                min = Math.Min(min, deltas[s + i]);
            ZigZag(min);

            var widths = new int[MiniblockCount];
            for (int mb = 0; mb < MiniblockCount; mb++)
            {
                ulong max = 0;
                for (int i = mb * PerMiniblock; i < Math.Min(n, (mb + 1) * PerMiniblock); i++)
                    max = Math.Max(max, unchecked((ulong)(deltas[s + i] - min)));
                while (widths[mb] < 64 && max >> widths[mb] != 0)
                    widths[mb]++;
                output.Add((byte)widths[mb]);
            }

            for (int mb = 0; mb < MiniblockCount; mb++)
            {
                int w = widths[mb];
                var packed = new byte[PerMiniblock * w / 8];
                for (int i = 0; i < PerMiniblock; i++)
                {
                    int at = mb * PerMiniblock + i;
                    ulong v = at < n ? unchecked((ulong)(deltas[s + at] - min)) : 0;
                    for (int b = 0; b < w; b++)
                    {
                        int bit = i * w + b;
                        if ((v >> b & 1) != 0)
                            packed[bit >> 3] |= (byte)(1 << (bit & 7));
                    }
                }

                output.AddRange(packed);
            }
        }

        return output.ToArray();
    }
}
