// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Benchmarks DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY decoding at the decoder level.
/// </summary>
[MemoryDiagnoser]
public class DeltaByteArrayBenchmarks
{
    private const int ValueCount = 100_000;
    private const int BlockSize = 128;
    private const int MiniblockCount = 4;
    private const int ValuesPerMiniblock = BlockSize / MiniblockCount;

    private byte[] _deltaLengthShortStrings = null!;
    private byte[] _deltaLengthLongStrings = null!;
    private byte[] _deltaByteArrayHighPrefix = null!;
    private byte[] _deltaByteArrayLowPrefix = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var random = new Random(42);

        // DELTA_LENGTH_BYTE_ARRAY: short strings (~10 chars)
        var shortStrings = GenerateStrings(random, ValueCount, 5, 15);
        _deltaLengthShortStrings = EncodeDeltaLengthByteArray(shortStrings);

        // DELTA_LENGTH_BYTE_ARRAY: long strings (~100 chars)
        var longStrings = GenerateStrings(random, ValueCount, 80, 120);
        _deltaLengthLongStrings = EncodeDeltaLengthByteArray(longStrings);

        // DELTA_BYTE_ARRAY: high prefix sharing (e.g., URLs with common prefix)
        var highPrefixStrings = GeneratePrefixStrings(random, ValueCount, "https://example.com/path/to/resource/", 5, 15);
        _deltaByteArrayHighPrefix = EncodeDeltaByteArray(highPrefixStrings);

        // DELTA_BYTE_ARRAY: low prefix sharing (random strings)
        _deltaByteArrayLowPrefix = EncodeDeltaByteArray(shortStrings);
    }

    [Benchmark(Description = "DeltaLength_Short")]
    public int DecodeDeltaLengthShortStrings()
    {
        using var state = new ColumnBuildState(PhysicalType.ByteArray, 0, 0, ValueCount);
        DeltaLengthByteArrayDecoder.Decode(_deltaLengthShortStrings, ValueCount, state);
        return state.ValueCount;
    }

    [Benchmark(Description = "DeltaLength_Long")]
    public int DecodeDeltaLengthLongStrings()
    {
        using var state = new ColumnBuildState(PhysicalType.ByteArray, 0, 0, ValueCount);
        DeltaLengthByteArrayDecoder.Decode(_deltaLengthLongStrings, ValueCount, state);
        return state.ValueCount;
    }

    [Benchmark(Description = "DeltaByteArray_HighPrefix")]
    public int DecodeDeltaByteArrayHighPrefix()
    {
        using var state = new ColumnBuildState(PhysicalType.ByteArray, 0, 0, ValueCount);
        DeltaByteArrayDecoder.Decode(_deltaByteArrayHighPrefix, ValueCount, state);
        return state.ValueCount;
    }

    [Benchmark(Description = "DeltaByteArray_LowPrefix")]
    public int DecodeDeltaByteArrayLowPrefix()
    {
        using var state = new ColumnBuildState(PhysicalType.ByteArray, 0, 0, ValueCount);
        DeltaByteArrayDecoder.Decode(_deltaByteArrayLowPrefix, ValueCount, state);
        return state.ValueCount;
    }

    // --- Data generation ---

    private static byte[][] GenerateStrings(Random random, int count, int minLen, int maxLen)
    {
        var result = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            int len = random.Next(minLen, maxLen + 1);
            var bytes = new byte[len];
            for (int j = 0; j < len; j++)
                bytes[j] = (byte)random.Next('a', 'z' + 1);
            result[i] = bytes;
        }
        return result;
    }

    private static byte[][] GeneratePrefixStrings(Random random, int count, string prefix, int suffixMin, int suffixMax)
    {
        var prefixBytes = System.Text.Encoding.UTF8.GetBytes(prefix);
        var result = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            int suffixLen = random.Next(suffixMin, suffixMax + 1);
            var bytes = new byte[prefixBytes.Length + suffixLen];
            prefixBytes.CopyTo(bytes, 0);
            for (int j = prefixBytes.Length; j < bytes.Length; j++)
                bytes[j] = (byte)random.Next('a', 'z' + 1);
            result[i] = bytes;
        }
        return result;
    }

    // --- DELTA_LENGTH_BYTE_ARRAY encoder ---

    private static byte[] EncodeDeltaLengthByteArray(byte[][] values)
    {
        // Lengths as DELTA_BINARY_PACKED + concatenated raw bytes
        var lengths = new int[values.Length];
        int totalBytes = 0;
        for (int i = 0; i < values.Length; i++)
        {
            lengths[i] = values[i].Length;
            totalBytes += values[i].Length;
        }

        var lengthBlock = EncodeDeltaBinaryPacked(lengths);

        var result = new byte[lengthBlock.Length + totalBytes];
        lengthBlock.CopyTo(result, 0);
        int pos = lengthBlock.Length;
        for (int i = 0; i < values.Length; i++)
        {
            values[i].CopyTo(result, pos);
            pos += values[i].Length;
        }
        return result;
    }

    // --- DELTA_BYTE_ARRAY encoder ---

    private static byte[] EncodeDeltaByteArray(byte[][] values)
    {
        // Compute prefix lengths and suffixes
        var prefixLengths = new int[values.Length];
        var suffixes = new byte[values.Length][];
        prefixLengths[0] = 0;
        suffixes[0] = values[0];

        for (int i = 1; i < values.Length; i++)
        {
            var prev = values[i - 1];
            var curr = values[i];
            int commonLen = 0;
            int maxCommon = Math.Min(prev.Length, curr.Length);
            while (commonLen < maxCommon && prev[commonLen] == curr[commonLen])
                commonLen++;
            prefixLengths[i] = commonLen;
            suffixes[i] = new byte[curr.Length - commonLen];
            Array.Copy(curr, commonLen, suffixes[i], 0, suffixes[i].Length);
        }

        // Encode: prefix lengths as DELTA_BINARY_PACKED + suffixes as DELTA_LENGTH_BYTE_ARRAY
        var prefixBlock = EncodeDeltaBinaryPacked(prefixLengths);
        var suffixBlock = EncodeDeltaLengthByteArray(suffixes);

        var result = new byte[prefixBlock.Length + suffixBlock.Length];
        prefixBlock.CopyTo(result, 0);
        suffixBlock.CopyTo(result, prefixBlock.Length);
        return result;
    }

    // --- DELTA_BINARY_PACKED encoder (reused from DeltaBinaryPackedBenchmarks) ---

    private static byte[] EncodeDeltaBinaryPacked(int[] values)
    {
        var deltas = new long[values.Length - 1];
        for (int i = 0; i < deltas.Length; i++)
            deltas[i] = (long)values[i + 1] - values[i];
        return EncodeDeltaBinaryPackedCore(values[0], deltas, values.Length);
    }

    private static byte[] EncodeDeltaBinaryPackedCore(long firstValue, long[] deltas, int totalCount)
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

            long minDelta = long.MaxValue;
            for (int i = 0; i < blockCount; i++)
                minDelta = Math.Min(minDelta, deltas[deltaIdx + i]);
            if (blockCount == 0)
                minDelta = 0;

            WriteZigZagVarInt(ms, minDelta);

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

            for (int mb = 0; mb < MiniblockCount; mb++)
            {
                int bitWidth = bitWidths[mb];
                if (bitWidth == 0) continue;

                int mbStart = deltaIdx + mb * ValuesPerMiniblock;
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
