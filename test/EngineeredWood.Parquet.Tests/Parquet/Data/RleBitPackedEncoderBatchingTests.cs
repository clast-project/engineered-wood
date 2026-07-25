// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;
using Xunit.Abstractions;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Covers batching consecutive bit-packed groups into a single literal run
/// (<see cref="RleBitPackedEncoder"/>'s <c>maxLiteralGroups</c>). The contract is that batching
/// changes only the framing: any batching depth must decode back to exactly the input.
/// </summary>
public class RleBitPackedEncoderBatchingTests
{
    private readonly ITestOutputHelper _output;

    public RleBitPackedEncoderBatchingTests(ITestOutputHelper output) => _output = output;

    private static byte[] Encode(int[] values, int bitWidth, int maxLiteralGroups)
    {
        var encoder = new RleBitPackedEncoder(bitWidth, maxLiteralGroups: maxLiteralGroups);
        encoder.Encode(values);
        return encoder.ToArray();
    }

    private static int[] Decode(byte[] encoded, int bitWidth, int count)
    {
        var decoded = new int[count];
        var decoder = new RleBitPackedDecoder(encoded, bitWidth);
        decoder.ReadBatch(decoded.AsSpan());
        return decoded;
    }

    // ---- Data shapes that exercise the literal/RLE boundary ----

    public static TheoryData<string, int, int[]> Shapes()
    {
        var data = new TheoryData<string, int, int[]>();

        // All-literal: no run ever reaches 8, so everything is bit-packed.
        var alternating = new int[4096];
        for (int i = 0; i < alternating.Length; i++) alternating[i] = i & 1;
        data.Add("alternating", 1, alternating);

        // Fixed-length list repetition pattern (0,1,1) — the small-n shape.
        var repPattern = new int[4098];
        for (int i = 0; i < repPattern.Length; i++) repPattern[i] = i % 3 == 0 ? 0 : 1;
        data.Add("rep-n3", 1, repPattern);

        // Pure long run: must stay RLE regardless of batching.
        var longRun = new int[4096];
        Array.Fill(longRun, 5);
        data.Add("long-run", 3, longRun);

        // Runs straddling the RLE threshold (7, 8, 9 identical values).
        var straddle = new List<int>();
        for (int i = 0; i < 300; i++)
        {
            int len = 7 + (i % 3);
            for (int j = 0; j < len; j++) straddle.Add(i % 8);
        }
        data.Add("straddle-7-8-9", 3, straddle.ToArray());

        // Dictionary-index-like: high cardinality, mostly distinct.
        var indices = new int[4096];
        var rng = new Random(1234);
        for (int i = 0; i < indices.Length; i++) indices[i] = rng.Next(0, 1024);
        data.Add("dict-indices", 10, indices);

        // Mixed literal and run stretches.
        var mixed = new List<int>();
        var rng2 = new Random(99);
        while (mixed.Count < 4096)
        {
            if (rng2.Next(2) == 0)
            {
                int runLen = rng2.Next(1, 40);
                int v = rng2.Next(0, 16);
                for (int j = 0; j < runLen && mixed.Count < 4096; j++) mixed.Add(v);
            }
            else
            {
                for (int j = 0; j < 12 && mixed.Count < 4096; j++) mixed.Add(rng2.Next(0, 16));
            }
        }
        data.Add("mixed", 4, mixed.ToArray());

        // Not a multiple of 8 — exercises the padded final group.
        var ragged = new int[1003];
        for (int i = 0; i < ragged.Length; i++) ragged[i] = i % 5;
        data.Add("ragged-length", 3, ragged);

        return data;
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void Batching_RoundTripsIdentically(string name, int bitWidth, int[] values)
    {
        foreach (int groups in new[] { 1, 2, 8, 63 })
        {
            var encoded = Encode(values, bitWidth, groups);
            var decoded = Decode(encoded, bitWidth, values.Length);
            Assert.True(values.AsSpan().SequenceEqual(decoded),
                $"shape '{name}' mismatched at maxLiteralGroups={groups}");
        }
    }

    [Fact]
    public void DefaultDepth_IsByteIdenticalToExplicitOne()
    {
        foreach (var (name, bitWidth, values) in Shapes().Select(r => ((string)r[0], (int)r[1], (int[])r[2])))
        {
            var byDefault = new RleBitPackedEncoder(bitWidth);
            byDefault.Encode(values);

            var explicitOne = Encode(values, bitWidth, maxLiteralGroups: 1);
            Assert.True(byDefault.ToArray().AsSpan().SequenceEqual(explicitOne),
                $"shape '{name}' changed under the default depth");
        }
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void Batching_NeverGrowsTheStream(string name, int bitWidth, int[] values)
    {
        int unbatched = Encode(values, bitWidth, 1).Length;
        int batched = Encode(values, bitWidth, RleBitPackedEncoder.MaxLiteralGroups).Length;

        _output.WriteLine(
            $"{name,-16} bitWidth={bitWidth,2}  unbatched={unbatched,7}  batched={batched,7}  " +
            $"delta={(batched - unbatched),7}  ({100.0 * batched / unbatched:F1}%)");

        Assert.True(batched <= unbatched,
            $"shape '{name}' grew from {unbatched} to {batched} bytes under batching");
    }

    [Fact]
    public void Batching_RejectsOutOfRangeDepth()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RleBitPackedEncoder(1, maxLiteralGroups: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RleBitPackedEncoder(1, maxLiteralGroups: 64));
    }

    [Fact]
    public void Batching_ReusesEncoderAcrossResets()
    {
        // The writer resets and re-encodes per page; buffered literals must not leak between pages.
        var encoder = new RleBitPackedEncoder(2, maxLiteralGroups: 63);
        var pageA = new int[100];
        var pageB = new int[137];
        for (int i = 0; i < pageA.Length; i++) pageA[i] = i % 3;
        for (int i = 0; i < pageB.Length; i++) pageB[i] = (i + 1) % 3;

        encoder.Reset();
        encoder.Encode(pageA);
        var encodedA = encoder.ToArray();

        encoder.Reset();
        encoder.Encode(pageB);
        var encodedB = encoder.ToArray();

        Assert.True(pageA.AsSpan().SequenceEqual(Decode(encodedA, 2, pageA.Length)));
        Assert.True(pageB.AsSpan().SequenceEqual(Decode(encodedB, 2, pageB.Length)));
    }
}
