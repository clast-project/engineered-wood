// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Isolates the repetition-level scan in <see cref="FixedListDetector.MatchesFixedPattern"/> for
/// small lists (<c>length &lt; 8</c>), the only case whose bit-packed portion is a real scalar loop,
/// and compares the scalar scan against the adaptive (tiled <c>SequenceEqual</c>) strategy.
/// </summary>
/// <remarks>
/// Two encodings, because the answer depends entirely on the producer:
/// <list type="bullet">
///   <item><description><c>EwEncoder</c> — EngineeredWood's own RLE encoder, which flushes a
///   bit-packed group every eight values, producing a header-interleaved stream of single bytes.
///   There is no contiguous block, so tiling has nothing to vectorise.</description></item>
///   <item><description><c>DenseRun</c> — one large bit-packed run (as parquet-mr / arrow emit for
///   small lists), a long contiguous block where <c>SequenceEqual</c> can actually work.</description></item>
/// </list>
/// </remarks>
[MemoryDiagnoser]
public class FixedListDetectorBenchmarks
{
    /// <summary>Repetition levels per invocation, held constant across list lengths.</summary>
    public const int NumValues = 2_000_000;

    public enum Encoding
    {
        EwEncoder,
        DenseRun,
    }

    [Params(3, 5, 7)]
    public int Length { get; set; }

    [Params(Encoding.EwEncoder, Encoding.DenseRun)]
    public Encoding Layout { get; set; }

    private byte[] _encoded = null!;
    private int _numValues;

    [GlobalSetup]
    public void Setup()
    {
        // A whole number of records that lands near NumValues.
        int rows = NumValues / Length;
        _numValues = rows * Length;

        var rep = new int[_numValues];
        for (int i = 0; i < rows; i++)
        {
            rep[i * Length] = 0;
            for (int j = 1; j < Length; j++)
                rep[i * Length + j] = 1;
        }

        _encoded = Layout == Encoding.EwEncoder
            ? EncodeWithEwEncoder(rep)
            : EncodeSingleBitPackedRun(rep);

        // Sanity: both strategies must accept this stream, or the benchmark is measuring nothing.
        if (!FixedListDetector.MatchesFixedPattern(_encoded, _numValues, Length, 0, RepScanStrategy.Scalar) ||
            !FixedListDetector.MatchesFixedPattern(_encoded, _numValues, Length, 0, RepScanStrategy.Adaptive))
        {
            throw new InvalidOperationException("Encoded stream was not accepted by both strategies.");
        }
    }

    [Benchmark(Baseline = true)]
    public bool Scalar() =>
        FixedListDetector.MatchesFixedPattern(_encoded, _numValues, Length, 0, RepScanStrategy.Scalar);

    [Benchmark]
    public bool Adaptive() =>
        FixedListDetector.MatchesFixedPattern(_encoded, _numValues, Length, 0, RepScanStrategy.Adaptive);

    private static byte[] EncodeWithEwEncoder(int[] rep)
    {
        var encoder = new RleBitPackedEncoder(bitWidth: 1);
        encoder.Encode(rep);
        return encoder.ToArray();
    }

    private static byte[] EncodeSingleBitPackedRun(int[] levels)
    {
        int numGroups = (levels.Length + 7) / 8;
        var bytes = new byte[numGroups];
        for (int i = 0; i < levels.Length; i++)
        {
            if (levels[i] != 0)
                bytes[i >> 3] |= (byte)(1 << (i & 7));
        }

        var header = new List<byte>();
        uint h = (uint)((numGroups << 1) | 1);
        while (h > 0x7F) { header.Add((byte)(h | 0x80)); h >>= 7; }
        header.Add((byte)h);

        var result = new byte[header.Count + bytes.Length];
        header.CopyTo(result);
        bytes.CopyTo(result, header.Count);
        return result;
    }
}
