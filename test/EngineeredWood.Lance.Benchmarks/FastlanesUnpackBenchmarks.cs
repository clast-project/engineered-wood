// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;

namespace EngineeredWood.Lance.Benchmarks;

/// <summary>
/// Microbenchmark for the Fastlanes unpack path used by both
/// <c>BitpackedForNonNeg</c> (v2.0) and v2.1 miniblock
/// <c>InlineBitpacking</c>. Measures pure Clast.FastLanes throughput so
/// regressions are visible separately from the surrounding miniblock
/// envelope cost.
/// </summary>
[MemoryDiagnoser]
public class FastlanesUnpackBenchmarks
{
    private const int Chunk = Clast.FastLanes.BitPacking.ElementsPerChunk;

    private byte[] _packed8 = null!;
    private byte[] _packed16 = null!;
    private byte[] _packed20 = null!;

    [GlobalSetup]
    public void Setup()
    {
        // Build three packed buffers at distinct bit widths to capture how
        // throughput scales with output bytes per chunk.
        var src = new uint[Chunk];
        for (int i = 0; i < Chunk; i++) src[i] = (uint)(i & 0xFFFFF); // 20-bit-friendly

        _packed8 = Pack(src, 8);
        _packed16 = Pack(src, 16);
        _packed20 = Pack(src, 20);
    }

    private static byte[] Pack(uint[] src, int bitWidth)
    {
        int bytes = Clast.FastLanes.BitPacking.PackedByteCount<uint>(bitWidth);
        var dest = new byte[bytes];
        Clast.FastLanes.BitPacking.PackChunk<uint>(bitWidth, src, dest);
        return dest;
    }

    [Benchmark]
    public uint Unpack_UInt32_BitWidth8()
    {
        var output = new uint[Chunk];
        Clast.FastLanes.BitPacking.UnpackChunk<uint>(8, _packed8, output);
        return output[Chunk - 1];
    }

    [Benchmark]
    public uint Unpack_UInt32_BitWidth16()
    {
        var output = new uint[Chunk];
        Clast.FastLanes.BitPacking.UnpackChunk<uint>(16, _packed16, output);
        return output[Chunk - 1];
    }

    [Benchmark]
    public uint Unpack_UInt32_BitWidth20()
    {
        var output = new uint[Chunk];
        Clast.FastLanes.BitPacking.UnpackChunk<uint>(20, _packed20, output);
        return output[Chunk - 1];
    }
}
