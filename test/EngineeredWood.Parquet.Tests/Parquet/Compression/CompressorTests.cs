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

    /// <summary>
    /// Concatenated Gzip members (RFC 1952), the first carrying an FEXTRA field that holds what a
    /// member boundary looks like: its own trailer (CRC-32 and length) followed by a member's magic.
    /// .NET Framework finds member ends by searching for that, so the search must skip the header,
    /// whose optional fields can hold any bytes, and take a boundary only where a member starts (#426).
    /// </summary>
    [Fact]
    public void ConcatenatedGzipMembers_WithABoundaryLookalikeInAHeaderField_Decompress()
    {
        byte[] first = TestData.AsSpan(0, 3_000).ToArray();
        byte[] second = TestData.AsSpan(3_000).ToArray();

        var crc = new System.IO.Hashing.Crc32();
        crc.Append(first);
        byte[] lookalike = [.. crc.GetCurrentHash(), .. BitConverter.GetBytes(first.Length), 0x1F, 0x8B, 0x08, 0x00];

        byte[] member = GzipMember(first);
        byte[] withExtra =
        [
            .. member.AsSpan(0, 3), (byte)(member[3] | 0x04), .. member.AsSpan(4, 6),
            (byte)lookalike.Length, 0, .. lookalike,
            .. member.AsSpan(10),
        ];
        byte[] source = [.. withExtra, .. GzipMember(second)];

        // One byte to spare, as a page is read, so that a short or long result shows.
        byte[] destination = new byte[TestData.Length + 1];
        int written = Decompressor.Decompress(CompressionCodec.Gzip, source, destination);

        Assert.Equal(TestData.Length, written);
        Assert.Equal(TestData, destination.AsSpan(0, written).ToArray());

    }

    /// <summary>
    /// An empty Gzip member, first, between two others or last: every member is read. On .NET Framework the
    /// member search takes a boundary only where the next member produces bytes, which an empty one
    /// does not; GZipStream steps over it into the member after, so nothing is lost.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void ConcatenatedGzipMembers_WithAnEmptyOne_Decompress(int emptyAt)
    {
        var members = new List<byte[]>
        {
            GzipMember(TestData.AsSpan(0, 3_000).ToArray()),
            GzipMember(TestData.AsSpan(3_000).ToArray()),
        };
        members.Insert(emptyAt, GzipMember([]));
        byte[] source = members.SelectMany(m => m).ToArray();

        byte[] destination = new byte[TestData.Length + 1];
        int written = Decompressor.Decompress(CompressionCodec.Gzip, source, destination);

        Assert.Equal(TestData.Length, written);
        Assert.Equal(TestData, destination.AsSpan(0, written).ToArray());
    }

    private static byte[] GzipMember(byte[] data)
    {
        using var output = new MemoryStream();
        using (var gzip = new System.IO.Compression.GZipStream(output, System.IO.Compression.CompressionLevel.Optimal, leaveOpen: true))
            gzip.Write(data, 0, data.Length);
        return output.ToArray();
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
