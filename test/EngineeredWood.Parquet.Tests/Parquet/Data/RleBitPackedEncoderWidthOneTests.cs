// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The packed width-1 path of <see cref="RleBitPackedEncoder.Encode"/> (definition levels of flat columns,
/// RLE booleans), which batched encoders take: values packed to bits, literal groups copied as bytes, RLE only
/// where it costs less. Checked against the decoder and against the group-by-group path it replaces.
/// </summary>
public class RleBitPackedEncoderWidthOneTests
{
    private static readonly int[] Depths = [2, 8, RleBitPackedEncoder.MaxLiteralGroups];

    private static byte[] Encode(int[] values, int maxLiteralGroups, bool packed = true)
    {
        var encoder = new RleBitPackedEncoder(1, maxLiteralGroups: maxLiteralGroups) { PackWidthOne = packed };
        encoder.Encode(values);
        return encoder.ToArray();
    }

    private static int[] Decode(byte[] encoded, int count)
    {
        var decoded = new int[count];
        new RleBitPackedDecoder(encoded, 1).ReadBatch(decoded.AsSpan());
        return decoded;
    }

    /// <summary>Runs of 0s and 1s, each 1 to <paramref name="maxRun"/> long.</summary>
    private static int[] RandomRuns(Random random, int count, int maxRun)
    {
        var values = new int[count];
        int i = 0;
        while (i < count)
        {
            int length = Math.Min(count - i, random.Next(1, maxRun + 1));
            int bit = random.Next(2);
            for (int j = 0; j < length; j++)
                values[i++] = bit;
        }

        return values;
    }

    private static int[] Filled(int count, int bit)
    {
        var values = new int[count];
        values.AsSpan().Fill(bit);
        return values;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(9)]
    [InlineData(17)]
    [InlineData(30)]
    [InlineData(70)]
    [InlineData(300)]
    public void RoundTrips_RandomRuns(int maxRun)
    {
        for (int seed = 0; seed < 40; seed++)
        {
            // Lengths ending at every bit of a byte and of a 64-bit word.
            var values = RandomRuns(new Random(seed), 1_000 + seed * 3, maxRun);
            foreach (int depth in Depths)
            {
                Assert.True(values.AsSpan().SequenceEqual(Decode(Encode(values, depth), values.Length)),
                    $"seed {seed}, depth {depth} did not round-trip");
            }
        }
    }

    [Fact]
    public void RoundTrips_EveryShortLength()
    {
        var random = new Random(7);
        for (int n = 0; n <= 200; n++)
        {
            foreach (var values in new[] { Filled(n, 0), Filled(n, 1), RandomRuns(random, n, 3), RandomRuns(random, n, 40) })
            {
                foreach (int depth in Depths)
                {
                    Assert.True(values.AsSpan().SequenceEqual(Decode(Encode(values, depth), n)),
                        $"length {n}, depth {depth} did not round-trip");
                }
            }
        }
    }

    [Fact]
    public void RoundTrips_LongRunsAndLongLiterals()
    {
        // A run long enough for a three-byte RLE header, literal runs that fill to 63 groups and spill, and
        // runs starting at every offset within a byte.
        var values = new List<int>();
        values.AddRange(Filled(100_000, 1));
        values.AddRange(RandomRuns(new Random(1), 5_000, 3));
        for (int shift = 0; shift < 8; shift++)
        {
            values.AddRange(Filled(shift + 1, shift & 1));
            values.AddRange(Filled(40, 1 - (shift & 1)));
        }

        values.AddRange(Filled(70_001, 0));
        var array = values.ToArray();
        foreach (int depth in Depths)
            Assert.True(array.AsSpan().SequenceEqual(Decode(Encode(array, depth), array.Length)), $"depth {depth}");
    }

    [Fact]
    public void Literals_AreThePackedBits()
    {
        // 0,1,0,1... packs LSB first to 0xAA; 64 values are 8 groups under one header.
        var values = new int[64];
        for (int i = 0; i < values.Length; i++)
            values[i] = i & 1;

        byte[] expected = [(8 << 1) | 1, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA];
        Assert.Equal(expected, Encode(values, RleBitPackedEncoder.MaxLiteralGroups));
    }

    [Fact]
    public void LongRun_IsOneRleRun()
    {
        // 1000 << 1 = 2000 = 0xD0 0x0F as a varint, then the value byte.
        byte[] expected = [0xD0, 0x0F, 0x01];
        Assert.Equal(expected, Encode(Filled(1_000, 1), RleBitPackedEncoder.MaxLiteralGroups));
    }

    [Fact]
    public void ShortRun_AmongLiterals_StaysLiteral()
    {
        // 8 mixed values, 16 ones, 8 mixed: as RLE the ones cost a header, a value byte and a second literal
        // header (3 bytes), left in they cost 2. The group-by-group path takes the RLE: the first group's last
        // 1 joins the run, which it flushes, then 2 bytes of RLE and a second literal run, 6 bytes in all.
        int[] mixed = [0, 1, 1, 0, 1, 0, 0, 1];
        var values = mixed.Concat(Filled(16, 1)).Concat(mixed).ToArray();

        byte[] packed = Encode(values, RleBitPackedEncoder.MaxLiteralGroups);
        Assert.Equal(new byte[] { (4 << 1) | 1, 0x96, 0xFF, 0xFF, 0x96 }, packed);
        Assert.Equal(6, Encode(values, RleBitPackedEncoder.MaxLiteralGroups, packed: false).Length);
    }

    [Fact]
    public void RunsBetweenRleRuns_ChainAsRle()
    {
        // Long, short (9), long: each run starts where the last ended, so RLE needs no literal header and
        // leaving the 9 in would open one.
        var values = Filled(100, 0).Concat(Filled(9, 1)).Concat(Filled(100, 0)).ToArray();
        // 100 << 1 = 200 = 0xC8 0x01 as a varint.
        byte[] expected = [0xC8, 0x01, 0x00, 9 << 1, 0x01, 0xC8, 0x01, 0x00];
        Assert.Equal(expected, Encode(values, RleBitPackedEncoder.MaxLiteralGroups));
    }

    [Fact]
    public void ReusedEncoder_MatchesAFreshOne()
    {
        // Shorter streams after a long one of 1s must not pick up its packed bits. Long runs shift the group
        // grid off byte boundaries, so the final group reads past the last packed byte.
        var encoder = new RleBitPackedEncoder(1, maxLiteralGroups: RleBitPackedEncoder.MaxLiteralGroups);
        for (int seed = 0; seed < 50; seed++)
        {
            encoder.Reset();
            encoder.Encode(Filled(2_003, 1));
            var shorter = RandomRuns(new Random(seed), 500 + seed, 40);
            encoder.Reset();
            encoder.Encode(shorter);
            Assert.Equal(Encode(shorter, RleBitPackedEncoder.MaxLiteralGroups), encoder.ToArray());
        }
    }

    [Fact]
    public void Unbatched_KeepsTheGroupPath()
    {
        // At one group per header the output stays byte for byte what it always was.
        var values = RandomRuns(new Random(5), 3_000, 20);
        Assert.Equal(Encode(values, 1, packed: false), Encode(values, 1));
    }

    /// <summary>
    /// The cost rule is a heuristic, so a single stream can come out a few bytes larger than the group path or
    /// an unbatched encode; summed over many, it must not. Checked at the writer's depth and at 8. At depth 2
    /// a header comes every other byte, which the rule's even share per byte models too coarsely: measured
    /// 0.07% over the group path at runs up to 64, so depth 2 is only held to round-tripping.
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(12)]
    [InlineData(20)]
    [InlineData(40)]
    [InlineData(64)]
    [InlineData(200)]
    public void Packed_IsNoLargerInAggregate(int maxRun)
    {
        foreach (int depth in new[] { 8, RleBitPackedEncoder.MaxLiteralGroups })
        {
            long unbatched = 0, byGroup = 0, packed = 0;
            for (int seed = 0; seed < 100; seed++)
            {
                var values = RandomRuns(new Random(seed), 2_000 + seed, maxRun);
                unbatched += Encode(values, 1).Length;
                byGroup += Encode(values, depth, packed: false).Length;
                packed += Encode(values, depth).Length;
            }

            Assert.True(packed <= byGroup, $"depth {depth}: packed {packed} > by group {byGroup}");
            Assert.True(packed <= unbatched, $"depth {depth}: packed {packed} > unbatched {unbatched}");
        }
    }
}
