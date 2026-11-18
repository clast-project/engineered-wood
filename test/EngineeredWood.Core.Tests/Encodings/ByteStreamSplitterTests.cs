// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Runtime.Intrinsics.X86;
#endif
using EngineeredWood.Encodings;

namespace EngineeredWood.Core.Tests.Encodings;

/// <summary>
/// BYTE_STREAM_SPLIT encoding. As with the decoder, the dispatcher only reaches a kernel the CPU supports
/// and gives the 128-bit kernel just counts 16-31 on an AVX2 machine, so each SIMD kernel is also checked
/// directly against the reference layout.
/// </summary>
public class ByteStreamSplitterTests
{
    public static readonly TheoryData<int> Counts = new()
    {
        0, 1, 7, 15, 16, 17, 31, 32, 33, 47, 48, 63, 64, 65, 100, 255, 1000, 4097,
    };

    /// <summary>The reference layout: W streams of N bytes, stream s holding byte s of every value.</summary>
    private static byte[] ReferenceSplit(byte[] values, int count, int width)
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
    public void Split_MatchesReference(int count)
    {
        foreach (int width in new[] { 1, 2, 3, 4, 8, 12, 16 })
        {
            var values = RandomValues(count, width);
            var split = new byte[values.Length];
            ByteStreamSplitter.Split(values, split, count, width);
            Assert.Equal(ReferenceSplit(values, count, width), split);

            var scalar = new byte[values.Length];
            ByteStreamSplitter.SplitScalar(values, scalar, count, width);
            Assert.Equal(split, scalar);
        }
    }

    [Theory]
    [MemberData(nameof(Counts))]
    public void Split_RoundTripsThroughDecoder(int count)
    {
        foreach (int width in new[] { 2, 4, 8, 16 })
        {
            var values = RandomValues(count, width);
            var split = new byte[values.Length];
            ByteStreamSplitter.Split(values, split, count, width);

            var decoded = new byte[values.Length];
            ByteStreamSplitDecoder.DecodeFixedLenByteArrays(split, decoded, count, width);
            Assert.Equal(values, decoded);
        }
    }

    [Fact]
    public void Split_LeavesTheRestOfALargerDestinationAlone()
    {
        // The column writer reuses a buffer bigger than one page; stream s starts at s x count regardless.
        const int count = 70;
        var values = RandomValues(count, 8);
        var dest = Enumerable.Repeat((byte)0xEE, count * 8 + 9).ToArray();
        ByteStreamSplitter.Split(values, dest, count, 8);

        Assert.Equal(ReferenceSplit(values, count, 8), dest.Take(count * 8).ToArray());
        Assert.All(dest.Skip(count * 8), b => Assert.Equal(0xEE, b));
    }

    [Theory]
    [InlineData(16)]
    [InlineData(33)]
    public void ShortSpans_AreRefused(int count)
    {
        foreach (int width in new[] { 4, 8 })
        {
            Assert.Throws<ArgumentException>(() => ByteStreamSplitter.Split(
                new byte[count * width - 1], new byte[count * width], count, width));
            Assert.Throws<ArgumentException>(() => ByteStreamSplitter.Split(
                new byte[count * width], new byte[count * width - 1], count, width));
        }

        Assert.Throws<ArgumentOutOfRangeException>(() => ByteStreamSplitter.Split(
            new byte[64], new byte[64], -1, 4));
    }

#if NET8_0_OR_GREATER
    [SkippableTheory]
    [MemberData(nameof(Counts))]
    public void Vector128Kernels_MatchReference(int count)
    {
        Skip.IfNot(ByteStreamSplitter.IsVector128Supported, "needs SSSE3 or ARM64 AdvSimd");
        AssertKernel(ByteStreamSplitter.SplitVector128_4, count, 4);
        AssertKernel(ByteStreamSplitter.SplitVector128_8, count, 8);
    }

    [SkippableTheory]
    [MemberData(nameof(Counts))]
    public void Avx2Kernels_MatchReference(int count)
    {
        Skip.IfNot(Avx2.IsSupported, "needs AVX2");
        AssertKernel(ByteStreamSplitter.SplitAvx2_4, count, 4);
        AssertKernel(ByteStreamSplitter.SplitAvx2_8, count, 8);
    }

    private delegate void Kernel(ReadOnlySpan<byte> source, Span<byte> destination, int count);

    private static void AssertKernel(Kernel kernel, int count, int width)
    {
        var values = RandomValues(count, width);
        var split = new byte[values.Length];
        kernel(values, split, count);
        Assert.Equal(ReferenceSplit(values, count, width), split);
    }
#endif
}
