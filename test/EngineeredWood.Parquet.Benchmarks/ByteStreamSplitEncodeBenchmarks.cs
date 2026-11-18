// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Benchmarks BYTE_STREAM_SPLIT encoding at the encoder level (no file I/O), through the two entry points
/// the column writer calls: the dense one (no nulls) and the sparse one that skips nulls by definition level.
/// 131,072 values is a typical page and stays in cache; 1,000,000 does not.
/// </summary>
[MemoryDiagnoser]
public class ByteStreamSplitEncodeBenchmarks
{
    [Params(131_072, 1_000_000)]
    public int Count { get; set; }

    private float[] _floats = null!;
    private double[] _doubles = null!;
    private int[] _defLevels = null!;
    private int _nonNullCount;
    private byte[] _dest = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var random = new Random(42);
        _floats = new float[Count];
        _doubles = new double[Count];
        _defLevels = new int[Count];
        for (int i = 0; i < Count; i++)
        {
            _doubles[i] = random.NextDouble() * 1000 - 500;
            _floats[i] = (float)_doubles[i];
            // About 10% nulls.
            _defLevels[i] = random.Next(10) == 0 ? 0 : 1;
            _nonNullCount += _defLevels[i];
        }

        _dest = new byte[Count * sizeof(double)];
    }

    [Benchmark(Description = "Float32")]
    public byte DenseFloat32()
    {
        ByteStreamSplitEncoder.EncodeFloatsTo(_floats, _dest);
        return _dest[0];
    }

    [Benchmark(Description = "Float64")]
    public byte DenseFloat64()
    {
        ByteStreamSplitEncoder.EncodeDoublesTo(_doubles, _dest);
        return _dest[0];
    }

    [Benchmark(Description = "Float32 10% null")]
    public byte SparseFloat32()
    {
        ByteStreamSplitEncoder.EncodeFloatsTo(_floats, 0, Count, _nonNullCount, _defLevels, _dest);
        return _dest[0];
    }

    [Benchmark(Description = "Float64 10% null")]
    public byte SparseFloat64()
    {
        ByteStreamSplitEncoder.EncodeDoublesTo(_doubles, 0, Count, _nonNullCount, _defLevels, _dest);
        return _dest[0];
    }
}
