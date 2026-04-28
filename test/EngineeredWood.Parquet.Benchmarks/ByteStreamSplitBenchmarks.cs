// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Encodings;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Benchmarks BYTE_STREAM_SPLIT decoding at the decoder level (no file I/O).
/// </summary>
[MemoryDiagnoser]
public class ByteStreamSplitBenchmarks
{
    private const int ValueCount = 1_000_000;

    private byte[] _float32Data = null!;
    private byte[] _float64Data = null!;
    private byte[] _int32Data = null!;
    private byte[] _int64Data = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var random = new Random(42);

        _float32Data = EncodeSplit(GenerateFloats(random, ValueCount), sizeof(float));
        _float64Data = EncodeSplit(GenerateDoubles(random, ValueCount), sizeof(double));
        _int32Data = EncodeSplit(GenerateInt32s(random, ValueCount), sizeof(int));
        _int64Data = EncodeSplit(GenerateInt64s(random, ValueCount), sizeof(long));
    }

    [Benchmark(Description = "Float32")]
    public float DecodeFloat32()
    {
        var output = new float[ValueCount];
        ByteStreamSplitDecoder.DecodeFloats(_float32Data, output, ValueCount);
        return output[^1];
    }

    [Benchmark(Description = "Float64")]
    public double DecodeFloat64()
    {
        var output = new double[ValueCount];
        ByteStreamSplitDecoder.DecodeDoubles(_float64Data, output, ValueCount);
        return output[^1];
    }

    [Benchmark(Description = "Int32")]
    public int DecodeInt32()
    {
        var output = new int[ValueCount];
        ByteStreamSplitDecoder.DecodeInt32s(_int32Data, output, ValueCount);
        return output[^1];
    }

    [Benchmark(Description = "Int64")]
    public long DecodeInt64()
    {
        var output = new long[ValueCount];
        ByteStreamSplitDecoder.DecodeInt64s(_int64Data, output, ValueCount);
        return output[^1];
    }

    // --- Encoding helpers ---

    private static byte[] EncodeSplit<T>(T[] values, int width) where T : unmanaged
    {
        var bytes = MemoryMarshal.AsBytes(values.AsSpan());
        int count = values.Length;
        var result = new byte[count * width];

        for (int stream = 0; stream < width; stream++)
        {
            for (int i = 0; i < count; i++)
                result[stream * count + i] = bytes[i * width + stream];
        }

        return result;
    }

    private static float[] GenerateFloats(Random random, int count)
    {
        var values = new float[count];
        for (int i = 0; i < count; i++)
            values[i] = (float)(random.NextDouble() * 1000 - 500);
        return values;
    }

    private static double[] GenerateDoubles(Random random, int count)
    {
        var values = new double[count];
        for (int i = 0; i < count; i++)
            values[i] = random.NextDouble() * 1000 - 500;
        return values;
    }

    private static int[] GenerateInt32s(Random random, int count)
    {
        var values = new int[count];
        for (int i = 0; i < count; i++)
            values[i] = random.Next();
        return values;
    }

    private static long[] GenerateInt64s(Random random, int count)
    {
        var values = new long[count];
        for (int i = 0; i < count; i++)
            values[i] = NextInt64(random);
        return values;
    }

    private static long NextInt64(Random random)
    {
#if NET8_0_OR_GREATER
        return random.NextInt64();
#else
        return ((long)random.Next() << 32) | (uint)random.Next();
#endif
    }
}
