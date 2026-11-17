// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
#if NET8_0_OR_GREATER
using System.Runtime.Intrinsics.X86;
#endif
using EngineeredWood.Encodings;

namespace EngineeredWood.Core.Tests.Encodings;

/// <summary>
/// BYTE_STREAM_SPLIT decoding. The dispatcher only reaches a kernel the CPU supports, and on an AVX2
/// machine it gives the 128-bit kernel just counts 16-31. So each SIMD kernel is also checked directly
/// against the scalar loop, over counts that cover empty input, partial vectors and scalar tails.
/// </summary>
public class ByteStreamSplitDecoderTests
{
    public static readonly TheoryData<int> Counts = new()
    {
        0, 1, 7, 15, 16, 17, 31, 32, 33, 47, 48, 63, 64, 65, 100, 255, 1000, 4097,
    };

    /// <summary>The reference layout: W streams of N bytes, stream s holding byte s of every value.</summary>
    private static byte[] Split(byte[] values, int count, int width)
    {
        var split = new byte[values.Length];
        for (int s = 0; s < width; s++)
            for (int i = 0; i < count; i++)
                split[s * count + i] = values[i * width + s];
        return split;
    }

    private static byte[] RandomValues(int count, int width)
    {
        var values = new byte[count * width];
        new Random(count * 31 + width).NextBytes(values);
        return values;
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void Scalar_RestoresValues(int count)
    {
        foreach (int width in new[] { 1, 2, 3, 4, 8, 12, 16 })
        {
            var values = RandomValues(count, width);
            var decoded = new byte[values.Length];
            ByteStreamSplitDecoder.Unsplit(Split(values, count, width), decoded, count, width);
            Assert.Equal(values, decoded);
        }
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void PublicDecoders_RestoreValues(int count)
    {
        var floats = RandomValues(count, 4);
        var decodedFloats = new float[count];
        ByteStreamSplitDecoder.DecodeFloats(Split(floats, count, 4), decodedFloats, count);
        Assert.Equal(floats, MemoryMarshal.AsBytes(decodedFloats.AsSpan()).ToArray());

        var ints = RandomValues(count, 4);
        var decodedInts = new int[count];
        ByteStreamSplitDecoder.DecodeInt32s(Split(ints, count, 4), decodedInts, count);
        Assert.Equal(ints, MemoryMarshal.AsBytes(decodedInts.AsSpan()).ToArray());

        var doubles = RandomValues(count, 8);
        var decodedDoubles = new double[count];
        ByteStreamSplitDecoder.DecodeDoubles(Split(doubles, count, 8), decodedDoubles, count);
        Assert.Equal(doubles, MemoryMarshal.AsBytes(decodedDoubles.AsSpan()).ToArray());

        var longs = RandomValues(count, 8);
        var decodedLongs = new long[count];
        ByteStreamSplitDecoder.DecodeInt64s(Split(longs, count, 8), decodedLongs, count);
        Assert.Equal(longs, MemoryMarshal.AsBytes(decodedLongs.AsSpan()).ToArray());

        foreach (int width in new[] { 2, 4, 8, 16 })
        {
            var flba = RandomValues(count, width);
            var decodedFlba = new byte[flba.Length];
            ByteStreamSplitDecoder.DecodeFixedLenByteArrays(Split(flba, count, width), decodedFlba, count, width);
            Assert.Equal(flba, decodedFlba);
        }
    }

    /// <summary>
    /// The count and the payload both come from the file. The SIMD kernels load without bounds checks,
    /// so a payload one byte short of count x width must be refused before any kernel runs, at every
    /// count that reaches a SIMD path (16+ for the 128-bit kernel, 32+ for AVX2).
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(1000)]
    public void TruncatedPayload_IsRefused(int count)
    {
        Assert.Throws<InvalidDataException>(() =>
            ByteStreamSplitDecoder.DecodeFloats(new byte[count * 4 - 1], new float[count], count));
        Assert.Throws<InvalidDataException>(() =>
            ByteStreamSplitDecoder.DecodeInt32s(new byte[count * 4 - 1], new int[count], count));
        Assert.Throws<InvalidDataException>(() =>
            ByteStreamSplitDecoder.DecodeDoubles(new byte[count * 8 - 1], new double[count], count));
        Assert.Throws<InvalidDataException>(() =>
            ByteStreamSplitDecoder.DecodeInt64s(new byte[count * 8 - 1], new long[count], count));
        foreach (int width in new[] { 2, 4, 8, 16 })
        {
            Assert.Throws<InvalidDataException>(() => ByteStreamSplitDecoder.DecodeFixedLenByteArrays(
                new byte[count * width - 1], new byte[count * width], count, width));
        }
    }

    [Theory]
    [InlineData(16)]
    [InlineData(33)]
    public void ShortDestination_IsRefused(int count)
    {
        Assert.Throws<ArgumentException>(() =>
            ByteStreamSplitDecoder.DecodeFloats(new byte[count * 4], new float[count - 1], count));
        Assert.Throws<ArgumentException>(() =>
            ByteStreamSplitDecoder.DecodeInt64s(new byte[count * 8], new long[count - 1], count));
        Assert.Throws<ArgumentException>(() => ByteStreamSplitDecoder.DecodeFixedLenByteArrays(
            new byte[count * 8], new byte[count * 8 - 1], count, 8));
    }

    [Fact]
    public void LongerPayload_IsAccepted()
    {
        // Only a short payload is corrupt; trailing bytes past count x width are ignored, as before.
        const int count = 40;
        var values = RandomValues(count, 4);
        var payload = Split(values, count, 4).Concat(new byte[] { 0xAA, 0xBB }).ToArray();
        // Split lays the streams out at stride count, so the extra bytes sit after the last stream.
        var decoded = new int[count];
        ByteStreamSplitDecoder.DecodeInt32s(payload, decoded, count);
        Assert.Equal(values, MemoryMarshal.AsBytes(decoded.AsSpan()).ToArray());
    }

#if NET8_0_OR_GREATER
    [SkippableTheory]
    [MemberData(nameof(Counts))]
    public void Vector128Kernels_MatchScalar(int count)
    {
        Skip.IfNot(ByteStreamSplitDecoder.IsVector128ZipSupported, "needs SSE2 or ARM64 AdvSimd");
        AssertKernel(ByteStreamSplitDecoder.UnsplitVector128_4, count, 4);
        AssertKernel(ByteStreamSplitDecoder.UnsplitVector128_8, count, 8);
    }

    [SkippableTheory]
    [MemberData(nameof(Counts))]
    public void Avx2Kernels_MatchScalar(int count)
    {
        Skip.IfNot(Avx2.IsSupported, "needs AVX2");
        AssertKernel(ByteStreamSplitDecoder.UnsplitAvx2_4, count, 4);
        AssertKernel(ByteStreamSplitDecoder.UnsplitAvx2_8, count, 8);
    }

    private delegate void Kernel(ReadOnlySpan<byte> data, Span<byte> destination, int count);

    private static void AssertKernel(Kernel kernel, int count, int width)
    {
        var values = RandomValues(count, width);
        var decoded = new byte[values.Length];
        kernel(Split(values, count, width), decoded, count);
        Assert.Equal(values, decoded);
    }
#endif
}
