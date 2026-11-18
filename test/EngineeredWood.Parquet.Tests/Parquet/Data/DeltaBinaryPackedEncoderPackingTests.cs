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
            AssertWidths(values, long64: true, width);
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
            AssertWidths(values, long64: false, width);
        }
    }

    public static TheoryData<int, int, int> Layouts()
    {
        // Block size, miniblocks, width: miniblocks of 32, 64 and 128 values, at widths either side of
        // the decoder's one-read limit (56) and its full-width two-read path.
        var data = new TheoryData<int, int, int>();
        foreach (var (block, miniblocks) in new[] { (128, 4), (64, 2), (96, 3), (256, 4), (128, 1), (512, 16) })
        {
            foreach (int width in new[] { 0, 1, 7, 8, 13, 31, 32, 33, 55, 56, 57, 63, 64 })
                data.Add(block, miniblocks, width);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void Decoder_OtherBlockLayouts(int blockSize, int miniblockCount, int width)
    {
        // Streams in layouts other writers may choose, each followed by 0 to 20 bytes of whatever data
        // comes next in the page: the last miniblock is then read with and without room for whole
        // 8- and 16-byte reads, and the decoder must stop where the stream ends.
        var random = new Random(blockSize * 100 + miniblockCount * 10 + width);
        foreach (int count in new[] { 1, 2, 33, 200, 1000 })
        {
            long[] values = Int64s(random, count, () => width);
            int[] ints = Array.ConvertAll(Int32s(random, count, () => Math.Min(32, width)), v => (int)v);
            byte[] stream64 = Reference(values, long64: true, blockSize, miniblockCount);
            byte[] stream32 = Reference(Array.ConvertAll(ints, v => (long)v), long64: false, blockSize, miniblockCount);

            foreach (int trailing in new[] { 0, 1, 7, 8, 9, 15, 16, 20 })
            {
                var data64 = new byte[stream64.Length + trailing];
                stream64.CopyTo(data64, 0);
                data64.AsSpan(stream64.Length).Fill(0xA5);
                var decoder64 = new DeltaBinaryPackedDecoder(data64);
                var longs = new long[count];
                decoder64.DecodeInt64s(longs);
                Assert.Equal(values, longs);
                Assert.Equal(stream64.Length, decoder64.BytesConsumed);

                var data32 = new byte[stream32.Length + trailing];
                stream32.CopyTo(data32, 0);
                data32.AsSpan(stream32.Length).Fill(0xA5);
                var decoder32 = new DeltaBinaryPackedDecoder(data32);
                var decoded = new int[count];
                decoder32.DecodeInt32s(decoded);
                Assert.Equal(ints, decoded);
                Assert.Equal(stream32.Length, decoder32.BytesConsumed);
            }
        }
    }

    /// <summary>
    /// A stream claiming one miniblock of 2^30 values at width 32, with 10 values and only the 8 bytes
    /// of the first two deltas (1 and 2). Its size, 2^32 bytes, wraps to 0 in 32 bits: the decoder then
    /// took the miniblock as fitting and read the other deltas from past the end of the data. Past the
    /// end here is 0xFF, outside the span.
    /// </summary>
    [Fact]
    public void Decoder_MiniblockSizeOverflowing32Bits_IsRefused()
    {
        byte[] stream = HugeMiniblock(blockSizeVarint: [0x80, 0x80, 0x80, 0x80, 0x04]); // 1 << 30
        var backing = new byte[stream.Length + 64];
        stream.CopyTo(backing, 0);
        backing.AsSpan(stream.Length).Fill(0xFF);

        Assert.Throws<InvalidDataException>(() =>
            new DeltaBinaryPackedDecoder(backing.AsSpan(0, stream.Length)).DecodeInt32s(new int[10]));
        Assert.Throws<InvalidDataException>(() =>
            new DeltaBinaryPackedDecoder(backing.AsSpan(0, stream.Length)).DecodeInt64s(new long[10]));
    }

    /// <summary>
    /// As above but 2^20 values, a 4 MiB miniblock that fits 32 bits and not the data: the bytes past
    /// the end read as zeros, as they always have, so the deltas after the first two are 0.
    /// </summary>
    [Fact]
    public void Decoder_TruncatedMiniblock_ReadsZerosPastTheEnd()
    {
        byte[] stream = HugeMiniblock(blockSizeVarint: [0x80, 0x80, 0x40]); // 1 << 20
        var backing = new byte[stream.Length + 64];
        stream.CopyTo(backing, 0);
        backing.AsSpan(stream.Length).Fill(0xFF);

        var ints = new int[10];
        new DeltaBinaryPackedDecoder(backing.AsSpan(0, stream.Length)).DecodeInt32s(ints);
        Assert.Equal(new[] { 0, 1, 3, 3, 3, 3, 3, 3, 3, 3 }, ints);

        var longs = new long[10];
        new DeltaBinaryPackedDecoder(backing.AsSpan(0, stream.Length)).DecodeInt64s(longs);
        Assert.Equal(new long[] { 0, 1, 3, 3, 3, 3, 3, 3, 3, 3 }, longs);
    }

    /// <summary>
    /// One block of one miniblock of the given size, 10 values from 0, min delta 0, width 32, and the
    /// first two deltas only: 1 and 2.
    /// </summary>
    private static byte[] HugeMiniblock(byte[] blockSizeVarint) =>
    [
        .. blockSizeVarint, 0x01, 0x0A, 0x00, // block size, 1 miniblock, 10 values, first value 0
        0x00, 0x20,                           // min delta 0, width 32
        0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
    ];

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

    /// <summary>
    /// Values whose deltas come from <see cref="Delta"/>, so that with one width throughout, every
    /// miniblock of two or more deltas packs at exactly that width.
    /// </summary>
    private static long[] Int64s(Random random, int count, Func<int> width)
    {
        var values = new long[count];
        values[0] = (long)Random64(random);
        for (int i = 1; i < count; i++)
            values[i] = unchecked(values[i - 1] + Delta(random, i - 1, width()));
        return values;
    }

    /// <summary>As <see cref="Int64s"/>, but 32-bit: widths are capped at 32.</summary>
    private static long[] Int32s(Random random, int count, Func<int> width)
    {
        var values = new long[count];
        values[0] = (int)Random64(random);
        for (int i = 1; i < count; i++)
            values[i] = unchecked((int)values[i - 1] + (int)Delta(random, i - 1, Math.Min(32, width())));
        return values;
    }

    /// <summary>
    /// A delta of <paramref name="width"/> bits, signed: the remainder r, from 0 to 2^width - 1, less
    /// 2^(width - 1). Each miniblock's first delta has r = 0, the least possible, and its second the
    /// greatest, so the block minimum is the low end of the range and those two remainders span all
    /// <paramref name="width"/> bits. Width 64 covers the whole of <see cref="long"/> (and 32 the whole
    /// of <see cref="int"/>), so the deltas reach both signed extremes.
    /// </summary>
    private static long Delta(Random random, int index, int width)
    {
        if (width == 0)
            return 0;
        ulong top = width == 64 ? ulong.MaxValue : (1UL << width) - 1;
        ulong r = (index % PerMiniblock) switch
        {
            0 => 0,
            1 => top,
            _ => Random64(random) & top,
        };
        return unchecked((long)r - (long)(1UL << (width - 1)));
    }

    private static ulong Random64(Random random)
    {
        var bytes = new byte[8];
        random.NextBytes(bytes);
        return BitConverter.ToUInt64(bytes, 0);
    }

    /// <summary>
    /// Asserts the widths the encoder wrote: <paramref name="width"/> for each miniblock of two or more
    /// deltas, 0 for one of a single delta (its remainder is 0) or none.
    /// </summary>
    private static void AssertWidths(long[] values, bool long64, int width)
    {
        var encoder = new DeltaBinaryPackedEncoder();
        if (long64)
            encoder.EncodeInt64s(values);
        else
            encoder.EncodeInt32s(Array.ConvertAll(values, v => (int)v));
        byte[] data = encoder.ToArray();

        int pos = 0;
        Varint.ReadUnsigned(data, ref pos);
        Varint.ReadUnsigned(data, ref pos);
        Varint.ReadUnsigned(data, ref pos);
        Varint.ReadSigned(data, ref pos);
        int deltas = values.Length - 1;
        for (int s = 0; s < deltas; s += BlockSize)
        {
            Varint.ReadSigned(data, ref pos);
            int packed = 0;
            for (int mb = 0; mb < MiniblockCount; mb++)
            {
                int inMiniblock = Math.Max(0, Math.Min(PerMiniblock, deltas - s - mb * PerMiniblock));
                Assert.Equal(inMiniblock >= 2 ? width : 0, data[pos + mb]);
                packed += PerMiniblock * data[pos + mb] / 8;
            }

            pos += MiniblockCount + packed;
        }

        Assert.Equal(data.Length, pos);
    }

    /// <summary>
    /// The encoding as written before word packing: each remainder ORed into cleared bytes one byte at a
    /// time. Deltas wrap at 32 bits for INT32 and at 64 for INT64. The block layout defaults to the
    /// encoder's, 128 values in 4 miniblocks; the decoder tests vary it as other writers may.
    /// </summary>
    private static byte[] Reference(long[] values, bool long64, int blockSize = BlockSize, int miniblockCount = MiniblockCount)
    {
        int perMiniblock = blockSize / miniblockCount;
        var output = new List<byte>();
        void Varint(ulong v) { for (; v >= 0x80; v >>= 7) output.Add((byte)(v | 0x80)); output.Add((byte)v); }
        void ZigZag(long v) => Varint(unchecked((ulong)((v << 1) ^ (v >> 63))));

        Varint((ulong)blockSize);
        Varint((ulong)miniblockCount);
        Varint((ulong)values.Length);
        ZigZag(values[0]);

        var deltas = new long[values.Length - 1];
        for (int i = 1; i < values.Length; i++)
            deltas[i - 1] = long64 ? unchecked(values[i] - values[i - 1]) : unchecked((int)values[i] - (int)values[i - 1]);

        for (int s = 0; s < deltas.Length; s += blockSize)
        {
            int n = Math.Min(blockSize, deltas.Length - s);
            long min = long.MaxValue;
            for (int i = 0; i < n; i++)
                min = Math.Min(min, deltas[s + i]);
            ZigZag(min);

            var widths = new int[miniblockCount];
            for (int mb = 0; mb < miniblockCount; mb++)
            {
                ulong max = 0;
                for (int i = mb * perMiniblock; i < Math.Min(n, (mb + 1) * perMiniblock); i++)
                    max = Math.Max(max, unchecked((ulong)(deltas[s + i] - min)));
                while (widths[mb] < 64 && max >> widths[mb] != 0)
                    widths[mb]++;
                output.Add((byte)widths[mb]);
            }

            for (int mb = 0; mb < miniblockCount; mb++)
            {
                int w = widths[mb];
                var packed = new byte[perMiniblock * w / 8];
                for (int i = 0; i < perMiniblock; i++)
                {
                    int at = mb * perMiniblock + i;
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
