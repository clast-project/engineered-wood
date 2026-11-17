// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.IO.Compression;
using EngineeredWood.Compression;
using ZstdSharpCompressor = ZstdSharp.Compressor;

namespace EngineeredWood.Tests.Parquet.Compression;

/// <summary>
/// What the codecs must keep doing whichever implementation is behind them.
/// </summary>
/// <remarks>
/// On .NET 11, Core compresses and decompresses Gzip and Deflate with the span-based
/// GZipEncoder/Decoder and DeflateEncoder/Decoder, and compresses Zstandard pages of 64 KiB and up
/// with the native ZstandardEncoder. Elsewhere it uses GZipStream, DeflateStream and ZstdSharp.
/// These tests run on every target, so the two are held to the same answers.
/// </remarks>
public class CodecBehaviourTests
{
    private static readonly byte[] EmptyGzip =
    [
        0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF,
        0x03, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    private static byte[] Pattern(int length, int period, int offset = 0)
    {
        var data = new byte[length];
        for (int i = 0; i < length; i++)
            data[i] = (byte)(i % period + offset);
        return data;
    }

    private static byte[] Gz(byte[] data)
    {
        var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
            gzip.Write(data, 0, data.Length);
        return output.ToArray();
    }

    private static byte[] Df(byte[] data)
    {
        var output = new MemoryStream();
        using (var deflate = new DeflateStream(output, CompressionLevel.Fastest, leaveOpen: true))
            deflate.Write(data, 0, data.Length);
        return output.ToArray();
    }

    private static byte[] Concat(params byte[][] parts) => parts.SelectMany(p => p).ToArray();

    private static readonly byte[] A = Pattern(5000, 13);
    private static readonly byte[] B = Pattern(3000, 7, 100);

    /// <summary>
    /// The edge cases a Gzip decode can meet, each with its destination size. They are the cases
    /// the .NET 11 decoder was checked against GZipStream with, kept as a standing comparison.
    /// </summary>
    public static IEnumerable<object[]> GzipCases()
    {
        var ga = Gz(A);
        var corruptCrc = ga.ToArray(); corruptCrc[corruptCrc.Length - 6] ^= 0xFF;
        var corruptLength = ga.ToArray(); corruptLength[corruptLength.Length - 2] ^= 0x01;
        var corruptBody = ga.ToArray(); corruptBody[20] ^= 0x55;
        var random = new byte[200_000];
        new Random(3).NextBytes(random);

        yield return new object[] { "one member, exact", ga, A.Length };
        yield return new object[] { "one member, size+1", ga, A.Length + 1 };
        yield return new object[] { "one member, size-1", ga, A.Length - 1 };
        yield return new object[] { "two members", Concat(ga, Gz(B)), A.Length + B.Length + 1 };
        yield return new object[] { "two members, cut in the second", Concat(ga, Gz(B)), A.Length + 100 };
        yield return new object[] { "three members, exact", Concat(ga, Gz(B), ga), 2 * A.Length + B.Length };
        yield return new object[] { "an empty member between", Concat(ga, EmptyGzip, Gz(B)), A.Length + B.Length + 1 };
        yield return new object[] { "an empty member alone", EmptyGzip, 1 };
        yield return new object[] { "an empty member first", Concat(EmptyGzip, ga), A.Length + 1 };
        yield return new object[] { "trailing zeros", Concat(ga, new byte[8]), A.Length + 1 };
        yield return new object[] { "trailing junk", Concat(ga, new byte[] { 1, 2, 3 }), A.Length + 1 };
        yield return new object[] { "trailing magic only", Concat(ga, new byte[] { 0x1F, 0x8B }), A.Length + 1 };
        yield return new object[] { "a trailing lone 1F", Concat(ga, new byte[] { 0x1F }), A.Length + 1 };
        yield return new object[] { "trailing 1F 00", Concat(ga, new byte[] { 0x1F, 0x00 }), A.Length + 1 };
        yield return new object[] { "a trailing partial header", Concat(ga, new byte[] { 0x1F, 0x8B, 0x08, 0x00 }), A.Length + 1 };
        yield return new object[] { "a trailing bad header", Concat(ga, new byte[] { 0x1F, 0x8B, 0x07, 0, 0, 0, 0, 0, 0, 0xFF }), A.Length + 1 };
        yield return new object[] { "junk between members", Concat(ga, new byte[] { 9 }, Gz(B)), A.Length + B.Length + 1 };
        yield return new object[] { "truncated in the body", ga.Take(40).ToArray(), A.Length + 1 };
        yield return new object[] { "truncated in the trailer", ga.Take(ga.Length - 3).ToArray(), A.Length + 1 };
        yield return new object[] { "a header only", ga.Take(10).ToArray(), A.Length + 1 };
        yield return new object[] { "a corrupt CRC", corruptCrc, A.Length + 1 };
        yield return new object[] { "a corrupt length", corruptLength, A.Length + 1 };
        yield return new object[] { "a corrupt body", corruptBody, A.Length + 1 };
        yield return new object[] { "not Gzip", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, 100 };
        yield return new object[] { "random data", Gz(random), random.Length + 1 };
        yield return new object[] { "more than the destination", Gz(random), 1000 };
    }

    public static IEnumerable<object[]> DeflateCases()
    {
        var da = Df(A);
        yield return new object[] { "one stream, size+1", da, A.Length + 1 };
        yield return new object[] { "one stream, size-1", da, A.Length - 1 };
        yield return new object[] { "trailing junk", Concat(da, new byte[] { 1, 2, 3 }), A.Length + 1 };
        yield return new object[] { "two streams", Concat(da, Df(B)), A.Length + B.Length + 1 };
        yield return new object[] { "truncated", da.Take(20).ToArray(), A.Length + 1 };
        yield return new object[] { "an empty final block", new byte[] { 0x03, 0x00 }, 1 };
        yield return new object[] { "not Deflate", new byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, 100 };
    }

    /// <summary>A decode's answer: the bytes, or the exception type.</summary>
    private static string Answer(Func<byte[], byte[], int> decode, byte[] source, int destinationSize)
    {
        var destination = new byte[destinationSize];
        try
        {
            int n = decode(source, destination);
            return $"{n} bytes: {Convert.ToBase64String(destination, 0, n).GetHashCode():X8}";
        }
        catch (Exception e)
        {
            return $"throws {e.GetType().Name}";
        }
    }

#if NET
    /// <summary>The Gzip decode Core used before .NET 11: GZipStream, read until the destination is full.</summary>
    private static int StreamGzip(byte[] source, byte[] destination)
    {
        using var gzip = new GZipStream(new MemoryStream(source), CompressionMode.Decompress);
        int total = 0;
        while (total < destination.Length)
        {
            int read = gzip.Read(destination, total, destination.Length - total);
            if (read == 0) break;
            total += read;
        }
        return total;
    }

    private static int StreamDeflate(byte[] source, byte[] destination)
    {
        using var deflate = new DeflateStream(new MemoryStream(source), CompressionMode.Decompress);
        int total = 0;
        while (total < destination.Length)
        {
            int read = deflate.Read(destination, total, destination.Length - total);
            if (read == 0) break;
            total += read;
        }
        return total;
    }

    /// <summary>
    /// Gzip decodes exactly as GZipStream does: concatenated and empty members, what is ignored
    /// after a member, truncation and corruption. On .NET 11 this compares GZipDecoder against
    /// GZipStream; on the other modern targets Core still uses GZipStream, and it pins that.
    /// </summary>
    [Theory]
    [MemberData(nameof(GzipCases))]
    public void Gzip_DecodesAsGZipStreamDoes(string name, byte[] source, int destinationSize)
    {
        Assert.Equal(
            Answer(StreamGzip, source, destinationSize),
            Answer((s, d) => Decompressor.Decompress(CompressionCodec.Gzip, s, d), source, destinationSize));
        _ = name;
    }

    [Theory]
    [MemberData(nameof(DeflateCases))]
    public void Deflate_DecodesAsDeflateStreamDoes(string name, byte[] source, int destinationSize)
    {
        Assert.Equal(
            Answer(StreamDeflate, source, destinationSize),
            Answer((s, d) => Decompressor.Decompress(CompressionCodec.Deflate, s, d), source, destinationSize));
        _ = name;
    }
#endif

    /// <summary>
    /// A decode that threw leaves the per-thread decoder mid-member; the next call must not see it.
    /// </summary>
    [Theory]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Deflate)]
    public void ADecodeAfterAFailedOne_IsUnaffected(CompressionCodec codec)
    {
        var good = codec == CompressionCodec.Gzip ? Gz(A) : Df(A);
        var corrupt = codec == CompressionCodec.Gzip ? good.ToArray() : new byte[] { 0xFF, 0xFF, 0xFF, 0xFF };
        if (codec == CompressionCodec.Gzip)
            corrupt[20] ^= 0x55;

        Assert.ThrowsAny<InvalidDataException>(() => Decompressor.Decompress(codec, corrupt, new byte[A.Length + 1]));

        // And one cut short, which leaves the decoder waiting for more input rather than failed.
        Decompressor.Decompress(codec, good.Take(good.Length / 2).ToArray(), new byte[A.Length + 1]);

        var destination = new byte[A.Length + 1];
        Assert.Equal(A.Length, Decompressor.Decompress(codec, good, destination));
        Assert.Equal(A, destination.Take(A.Length).ToArray());
    }

    /// <summary>
    /// An encode that threw (a destination too short) leaves the per-thread encoder mid-stream; the
    /// next call must write exactly what a first call writes.
    /// </summary>
    [Theory]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Deflate)]
    [InlineData(CompressionCodec.Zstd)]
    public void AnEncodeAfterAFailedOne_IsUnaffected(CompressionCodec codec)
    {
        var data = MakeMixed(200_000);
        var first = Compress(codec, data, level: null);

        // ArgumentException from the Gzip and Deflate paths and the native Zstandard encoder; ZstdSharp
        // throws its own exception type for a too-short destination, as it always has.
        Assert.ThrowsAny<Exception>(() => Compressor.Compress(codec, data, new byte[first.Length / 2]));

        Assert.Equal(first, Compress(codec, data, level: null));
    }

    /// <summary>
    /// Each level gets its own encoder state; moving between them on one thread must give each
    /// level's own output every time.
    /// </summary>
    [Theory]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Deflate)]
    [InlineData(CompressionCodec.Zstd)]
    public void SwitchingLevels_GivesEachLevelsOwnOutput(CompressionCodec codec)
    {
        var data = MakeMixed(200_000);
        var fastest = Compress(codec, data, BlockCompressionLevel.Fastest);
        var smallest = Compress(codec, data, BlockCompressionLevel.SmallestSize);

        Assert.True(smallest.Length <= fastest.Length, $"{smallest.Length} at SmallestSize, {fastest.Length} at Fastest");
        Assert.Equal(fastest, Compress(codec, data, BlockCompressionLevel.Fastest));
        Assert.Equal(smallest, Compress(codec, data, BlockCompressionLevel.SmallestSize));
        Assert.Equal(fastest, Compress(codec, data, BlockCompressionLevel.Fastest));
    }

    public static IEnumerable<object?[]> ZstdLevels()
    {
        foreach (int length in new[] { 8 * 1024, 64 * 1024 - 1, 64 * 1024, 1 << 20 })
        {
            yield return new object?[] { length, null, null };
            yield return new object?[] { length, BlockCompressionLevel.Fastest, null };
            yield return new object?[] { length, BlockCompressionLevel.Optimal, null };
            yield return new object?[] { length, null, -5 };
            yield return new object?[] { length, null, 10 };
            yield return new object?[] { length, null, 19 };
        }
    }

    /// <summary>
    /// Zstandard frames are the same bytes whichever encoder wrote them: on .NET 11 a page of
    /// 64 KiB or more goes through the native encoder and a smaller one through ZstdSharp, and
    /// either way it matches ZstdSharp. The frame records its content size, which readers size
    /// their output from.
    /// </summary>
    [Theory]
    [MemberData(nameof(ZstdLevels))]
    public void Zstd_WritesZstdSharpsFrame(int length, BlockCompressionLevel? level, int? customLevel)
    {
        var data = MakeMixed(length);

        int zstdLevel = customLevel ?? level switch
        {
            BlockCompressionLevel.Fastest => 1,
            BlockCompressionLevel.SmallestSize => 22,
            _ => ZstdSharpCompressor.DefaultCompressionLevel,
        };
        byte[] expected;
        using (var reference = new ZstdSharpCompressor(zstdLevel))
            expected = reference.Wrap(data).ToArray();

        var destination = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Zstd, length)];
        int written = Compressor.Compress(CompressionCodec.Zstd, data, destination, level, customLevel);

        Assert.Equal(expected, destination.Take(written).ToArray());
        Assert.Equal(length, Decompressor.GetDecompressedLength(CompressionCodec.Zstd, destination.AsSpan(0, written)));
    }

    private static byte[] Compress(CompressionCodec codec, byte[] data, BlockCompressionLevel? level)
    {
        var destination = new byte[Compressor.GetMaxCompressedLength(codec, data.Length)];
        int written = Compressor.Compress(codec, data, destination, level);
        return destination.Take(written).ToArray();
    }

    /// <summary>A repeated phrase broken up by random stretches: compresses, but not trivially.</summary>
    private static byte[] MakeMixed(int length)
    {
        var data = new byte[length];
        var phrase = "the quick brown fox jumps over the lazy dog. "u8;
        for (int i = 0; i < length; i++)
            data[i] = phrase[i % phrase.Length];

        var random = new Random(length);
        for (int start = 0; start < length; start += 8192)
        {
            var noise = new byte[Math.Min(1024, length - start)];
            random.NextBytes(noise);
            noise.CopyTo(data, start);
        }
        return data;
    }
}
