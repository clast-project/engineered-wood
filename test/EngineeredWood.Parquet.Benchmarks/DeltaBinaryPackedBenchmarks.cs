// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Benchmarks DELTA_BINARY_PACKED decoding at the decoder level (no file I/O).
/// Tests various bit widths and data patterns to measure decode throughput.
/// </summary>
[MemoryDiagnoser]
public class DeltaBinaryPackedBenchmarks
{
    private const int ValueCount = 1_000_000;
    private const int BlockSize = 128;
    private const int MiniblockCount = 4;
    private const int ValuesPerMiniblock = BlockSize / MiniblockCount;

    private byte[] _int32SmallDeltas = null!;
    private byte[] _int32LargeDeltas = null!;
    private byte[] _int32Constant = null!;
    private byte[] _int64SmallDeltas = null!;
    private byte[] _int64LargeDeltas = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var random = new Random(42);

        // Small deltas: 8-bit range (common for timestamps, sequential IDs)
        _int32SmallDeltas = Encode(GenerateInt32WithDeltas(random, ValueCount, -128, 127));

        // Large deltas: 24-bit range (more bit-unpacking work)
        _int32LargeDeltas = Encode(GenerateInt32WithDeltas(random, ValueCount, -8_000_000, 8_000_000));

        // Constant values: bitwidth 0 (pure RLE fast path)
        _int32Constant = Encode(Enumerable.Repeat(42, ValueCount).ToArray());

        // INT64 variants
        _int64SmallDeltas = Encode(GenerateInt64WithDeltas(random, ValueCount, -128, 127));
        _int64LargeDeltas = Encode(GenerateInt64WithDeltas(random, ValueCount, -8_000_000, 8_000_000));
    }

    [Benchmark(Description = "Int32_SmallDelta")]
    public int DecodeInt32SmallDeltas()
    {
        var decoder = new DeltaBinaryPackedDecoder(_int32SmallDeltas);
        var output = new int[ValueCount];
        decoder.DecodeInt32s(output);
        return output[^1];
    }

    [Benchmark(Description = "Int32_LargeDelta")]
    public int DecodeInt32LargeDeltas()
    {
        var decoder = new DeltaBinaryPackedDecoder(_int32LargeDeltas);
        var output = new int[ValueCount];
        decoder.DecodeInt32s(output);
        return output[^1];
    }

    [Benchmark(Description = "Int32_Constant")]
    public int DecodeInt32Constant()
    {
        var decoder = new DeltaBinaryPackedDecoder(_int32Constant);
        var output = new int[ValueCount];
        decoder.DecodeInt32s(output);
        return output[^1];
    }

    [Benchmark(Description = "Int64_SmallDelta")]
    public long DecodeInt64SmallDeltas()
    {
        var decoder = new DeltaBinaryPackedDecoder(_int64SmallDeltas);
        var output = new long[ValueCount];
        decoder.DecodeInt64s(output);
        return output[^1];
    }

    [Benchmark(Description = "Int64_LargeDelta")]
    public long DecodeInt64LargeDeltas()
    {
        var decoder = new DeltaBinaryPackedDecoder(_int64LargeDeltas);
        var output = new long[ValueCount];
        decoder.DecodeInt64s(output);
        return output[^1];
    }

    // --- Data generation helpers ---

    private static int[] GenerateInt32WithDeltas(Random random, int count, int minDelta, int maxDelta)
    {
        var values = new int[count];
        values[0] = random.Next();
        for (int i = 1; i < count; i++)
            values[i] = values[i - 1] + random.Next(minDelta, maxDelta + 1);
        return values;
    }

    private static long[] GenerateInt64WithDeltas(Random random, int count, int minDelta, int maxDelta)
    {
        var values = new long[count];
        values[0] = NextInt64(random);
        for (int i = 1; i < count; i++)
            values[i] = values[i - 1] + random.Next(minDelta, maxDelta + 1);
        return values;
    }

    // --- DELTA_BINARY_PACKED encoder (for generating test data) ---

    private static byte[] Encode(int[] values)
    {
        var deltas = new long[values.Length - 1];
        for (int i = 0; i < deltas.Length; i++)
            deltas[i] = (long)values[i + 1] - values[i];
        return EncodeCore(values[0], deltas, values.Length);
    }

    private static byte[] Encode(long[] values)
    {
        var deltas = new long[values.Length - 1];
        for (int i = 0; i < deltas.Length; i++)
            deltas[i] = values[i + 1] - values[i];
        return EncodeCore(values[0], deltas, values.Length);
    }

    private static byte[] EncodeCore(long firstValue, long[] deltas, int totalCount)
    {
        using var ms = new MemoryStream();

        WriteUnsignedVarInt(ms, BlockSize);
        WriteUnsignedVarInt(ms, MiniblockCount);
        WriteUnsignedVarInt(ms, totalCount);
        WriteZigZagVarInt(ms, firstValue);

        int deltaIdx = 0;
        while (deltaIdx < deltas.Length)
        {
            int blockCount = Math.Min(BlockSize, deltas.Length - deltaIdx);

            // Find min delta in this block
            long minDelta = long.MaxValue;
            for (int i = 0; i < blockCount; i++)
                minDelta = Math.Min(minDelta, deltas[deltaIdx + i]);
            if (blockCount == 0)
                minDelta = 0;

            WriteZigZagVarInt(ms, minDelta);

            // Compute bit widths per miniblock
            var bitWidths = new byte[MiniblockCount];
            for (int mb = 0; mb < MiniblockCount; mb++)
            {
                int mbStart = deltaIdx + mb * ValuesPerMiniblock;
                int mbEnd = Math.Min(mbStart + ValuesPerMiniblock, deltaIdx + blockCount);
                long maxVal = 0;
                for (int i = mbStart; i < mbEnd; i++)
                    maxVal = Math.Max(maxVal, deltas[i] - minDelta);

                bitWidths[mb] = maxVal == 0 ? (byte)0 : (byte)(64 - LeadingZeroCount(maxVal));
            }

            ms.Write(bitWidths, 0, bitWidths.Length);

            // Write bit-packed deltas per miniblock
            for (int mb = 0; mb < MiniblockCount; mb++)
            {
                int bitWidth = bitWidths[mb];
                if (bitWidth == 0)
                    continue;

                int mbStart = deltaIdx + mb * ValuesPerMiniblock;
                // Always encode a full miniblock (pad with zeros)
                int totalBits = ValuesPerMiniblock * bitWidth;
                var packed = new byte[(totalBits + 7) / 8];

                int bitOffset = 0;
                for (int i = 0; i < ValuesPerMiniblock; i++)
                {
                    long val = (mbStart + i < deltaIdx + blockCount)
                        ? deltas[mbStart + i] - minDelta
                        : 0;
                    WriteBitPacked(packed, bitOffset, val, bitWidth);
                    bitOffset += bitWidth;
                }

                ms.Write(packed, 0, packed.Length);
            }

            deltaIdx += blockCount;
        }

        return ms.ToArray();
    }

    private static int LeadingZeroCount(long value)
    {
#if NET8_0_OR_GREATER
        return (int)long.LeadingZeroCount(value);
#else
        ulong v = (ulong)value;
        if (v == 0) { return 64; }
        int n = 0;
        if ((v & 0xFFFFFFFF00000000UL) == 0) { n += 32; v <<= 32; }
        if ((v & 0xFFFF000000000000UL) == 0) { n += 16; v <<= 16; }
        if ((v & 0xFF00000000000000UL) == 0) { n += 8; v <<= 8; }
        if ((v & 0xF000000000000000UL) == 0) { n += 4; v <<= 4; }
        if ((v & 0xC000000000000000UL) == 0) { n += 2; v <<= 2; }
        if ((v & 0x8000000000000000UL) == 0) { n += 1; }
        return n;
#endif
    }

    private static long NextInt64(Random random)
    {
#if NET8_0_OR_GREATER
        return random.NextInt64();
#else
        return ((long)random.Next() << 32) | (uint)random.Next();
#endif
    }

    private static void WriteBitPacked(byte[] buffer, int bitOffset, long value, int bitWidth)
    {
        for (int i = 0; i < bitWidth; i++)
        {
            if (((value >> i) & 1) == 1)
                buffer[(bitOffset + i) / 8] |= (byte)(1 << ((bitOffset + i) % 8));
        }
    }

    private static void WriteUnsignedVarInt(MemoryStream ms, long value)
    {
        while (value > 0x7F)
        {
            ms.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        ms.WriteByte((byte)value);
    }

    private static void WriteZigZagVarInt(MemoryStream ms, long value)
    {
        WriteUnsignedVarInt(ms, (value << 1) ^ (value >> 63));
    }
}
