// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Compression;

namespace EngineeredWood.Tests.Parquet.Compression;

public class CompressorTests
{
    private static readonly byte[] TestData = GenerateTestData();

    private static byte[] GenerateTestData()
    {
        var rng = new Random(42);
        // Mix of compressible and random data
        var data = new byte[10_000];
        for (int i = 0; i < data.Length; i++)
        {
            // Repeat patterns for compressibility
            data[i] = (byte)(i % 256 == 0 ? rng.Next(256) : data[Math.Max(0, i - 1)]);
        }
        return data;
    }

    [Fact]
    public void Uncompressed_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Uncompressed);
    }

    [Fact]
    public void Snappy_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Snappy);
    }

    [Fact]
    public void Gzip_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Gzip);
    }

    [Fact]
    public void Brotli_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Brotli);
    }

    [Fact]
    public void Lz4Raw_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Lz4);
    }

    [Fact]
    public void Zstd_RoundTrips()
    {
        AssertRoundTrip(CompressionCodec.Zstd);
    }

    [Fact]
    public void Snappy_CompressesSmallerThanOriginal()
    {
        var compressed = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Snappy, TestData.Length)];
        int compressedLength = Compressor.Compress(CompressionCodec.Snappy, TestData, compressed);
        Assert.True(compressedLength < TestData.Length,
            $"Snappy compressed {TestData.Length} → {compressedLength} (expected smaller)");
    }

    [Fact]
    public void Zstd_CompressesSmallerThanOriginal()
    {
        var compressed = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Zstd, TestData.Length)];
        int compressedLength = Compressor.Compress(CompressionCodec.Zstd, TestData, compressed);
        Assert.True(compressedLength < TestData.Length,
            $"Zstd compressed {TestData.Length} → {compressedLength} (expected smaller)");
    }

    [Fact]
    public void Uncompressed_SameSize()
    {
        int maxLen = Compressor.GetMaxCompressedLength(CompressionCodec.Uncompressed, TestData.Length);
        Assert.Equal(TestData.Length, maxLen);

        var output = new byte[maxLen];
        int written = Compressor.Compress(CompressionCodec.Uncompressed, TestData, output);
        Assert.Equal(TestData.Length, written);
    }

    [Fact]
    public void Uncompressed_EmptyInput()
    {
        int maxLen = Compressor.GetMaxCompressedLength(CompressionCodec.Uncompressed, 0);
        Assert.Equal(0, maxLen);

        var output = new byte[1];
        int written = Compressor.Compress(CompressionCodec.Uncompressed, ReadOnlySpan<byte>.Empty, output);
        Assert.Equal(0, written);
    }

    [Fact]
    public void Lzo_NotSupported()
    {
        Assert.Throws<NotSupportedException>(() =>
            Compressor.GetMaxCompressedLength(CompressionCodec.Lzo, 100));
        Assert.Throws<NotSupportedException>(() =>
            Compressor.Compress(CompressionCodec.Lzo, TestData, new byte[TestData.Length]));
    }

    private static void AssertRoundTrip(CompressionCodec codec)
    {
        int maxLen = Compressor.GetMaxCompressedLength(codec, TestData.Length);
        var compressed = new byte[maxLen];
        int compressedLength = Compressor.Compress(codec, TestData, compressed);

        var decompressed = new byte[TestData.Length];
        int decompressedLength = Decompressor.Decompress(codec, compressed.AsSpan(0, compressedLength), decompressed);

        Assert.Equal(TestData.Length, decompressedLength);
        Assert.Equal(TestData, decompressed);
    }

    public static IEnumerable<object[]> TunableCodecsAndLevels()
    {
        var codecs = new[]
        {
            CompressionCodec.Gzip, CompressionCodec.Deflate, CompressionCodec.Brotli,
            CompressionCodec.Lz4, CompressionCodec.Zstd,
        };
        var levels = new[]
        {
            BlockCompressionLevel.Fastest, BlockCompressionLevel.Optimal, BlockCompressionLevel.SmallestSize,
        };
        foreach (var c in codecs)
            foreach (var l in levels)
                yield return new object[] { c, l };
    }

    [Theory]
    [MemberData(nameof(TunableCodecsAndLevels))]
    public void RoundTrips_AtLevel(CompressionCodec codec, BlockCompressionLevel level)
    {
        int maxLen = Compressor.GetMaxCompressedLength(codec, TestData.Length);
        var compressed = new byte[maxLen];
        int compressedLength = Compressor.Compress(codec, TestData, compressed, level);

        var decompressed = new byte[TestData.Length];
        int decompressedLength = Decompressor.Decompress(
            codec, compressed.AsSpan(0, compressedLength), decompressed);

        Assert.Equal(TestData.Length, decompressedLength);
        Assert.Equal(TestData, decompressed);
    }

    [Theory]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Deflate)]
    [InlineData(CompressionCodec.Brotli)]
    [InlineData(CompressionCodec.Lz4)]
    [InlineData(CompressionCodec.Zstd)]
    public void SmallestSize_NotLargerThan_Fastest(CompressionCodec codec)
    {
        // Use a large, highly redundant payload so size differences across levels exceed framing
        // overhead. The default TestData is too short to demonstrate monotonicity for Gzip/Deflate
        // (especially on netstandard2.0, where SmallestSize falls back to Optimal).
        var payload = MakeRedundantPayload(64 * 1024);
        int fastestLen = CompressLength(codec, payload, BlockCompressionLevel.Fastest);
        int smallestLen = CompressLength(codec, payload, BlockCompressionLevel.SmallestSize);
        Assert.True(smallestLen <= fastestLen,
            $"{codec}: SmallestSize ({smallestLen}) > Fastest ({fastestLen})");
    }

    private static byte[] MakeRedundantPayload(int length)
    {
        var data = new byte[length];
        var pattern = "the quick brown fox jumps over the lazy dog. "u8;
        for (int i = 0; i < length; i++)
            data[i] = pattern[i % pattern.Length];
        return data;
    }

    [Fact]
    public void Zstd_CustomLevel_RoundTripsAndShrinks()
    {
        // Zstd level 22 is the maximum; should produce output no larger than default.
        int maxLen = Compressor.GetMaxCompressedLength(CompressionCodec.Zstd, TestData.Length);
        var compressed = new byte[maxLen];
        int compressedLength = Compressor.Compress(
            CompressionCodec.Zstd, TestData, compressed, level: null, customLevel: 22);

        var decompressed = new byte[TestData.Length];
        int decompressedLength = Decompressor.Decompress(
            CompressionCodec.Zstd, compressed.AsSpan(0, compressedLength), decompressed);

        Assert.Equal(TestData.Length, decompressedLength);
        Assert.Equal(TestData, decompressed);

        int defaultLen = CompressLength(CompressionCodec.Zstd, level: null);
        Assert.True(compressedLength <= defaultLen,
            $"Zstd custom level 22 ({compressedLength}) > default ({defaultLen})");
    }

    private static int CompressLength(CompressionCodec codec, BlockCompressionLevel? level)
        => CompressLength(codec, TestData, level);

    private static int CompressLength(CompressionCodec codec, byte[] payload, BlockCompressionLevel? level)
    {
        int maxLen = Compressor.GetMaxCompressedLength(codec, payload.Length);
        var compressed = new byte[maxLen];
        return Compressor.Compress(codec, payload, compressed, level);
    }
}
