// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
#if NET8_0_OR_GREATER
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
#endif

namespace EngineeredWood.Encodings;

/// <summary>
/// The BYTE_STREAM_SPLIT transform on the encode side: N values of W bytes become W consecutive
/// streams of N bytes, stream s holding byte s of every value. <see cref="ByteStreamSplitDecoder"/>
/// is the inverse.
/// </summary>
/// <remarks>
/// <para>Widths 4 and 8 have SIMD kernels, chosen like the decoder's: AVX2 where available, otherwise a
/// 128-bit kernel on SSSE3 or ARM64 AdvSimd, otherwise the scalar loop. Each kernel is a byte shuffle
/// that groups every value's bytes by stream inside a register, followed by a transpose built from
/// 16-, 32- and 64-bit interleaves (the same unpack/zip instructions the decoder uses).</para>
/// <para>The kernels live in Core, next to the decoder, so that the arm64 CI job (which runs only
/// Core.Tests) exercises the AdvSimd path.</para>
/// </remarks>
internal static class ByteStreamSplitter
{
    /// <summary>
    /// Splits <paramref name="count"/> values of <paramref name="width"/> bytes from
    /// <paramref name="source"/> into <paramref name="destination"/>, stream s starting at s x count.
    /// </summary>
    public static void Split(ReadOnlySpan<byte> source, Span<byte> destination, int count, int width)
    {
        if (width < 0)
            throw new ArgumentOutOfRangeException(nameof(width), width, "The width cannot be negative.");
        CheckLengths(source, destination, count, width);

        if (width == 4)
            Split4(source, destination, count);
        else if (width == 8)
            Split8(source, destination, count);
        else
            SplitScalar(source, destination, count, width);
    }

    /// <summary>
    /// The SIMD kernels load and store without bounds checks, so the entry point validates the spans.
    /// Both come from the caller, so a short span is a caller bug.
    /// </summary>
    private static void CheckLengths(ReadOnlySpan<byte> source, Span<byte> destination, int count, int width)
    {
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), count, "The value count cannot be negative.");

        long needed = (long)count * width;
        if (source.Length < needed)
        {
            throw new ArgumentException(
                $"The source holds {source.Length} bytes, but {count} values of {width} bytes need {needed}.",
                nameof(source));
        }

        if (destination.Length < needed)
        {
            throw new ArgumentException(
                $"The destination holds {destination.Length} bytes, but {count} values of {width} bytes need {needed}.",
                nameof(destination));
        }
    }

    private static void Split4(ReadOnlySpan<byte> source, Span<byte> destination, int count)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported && count >= Vector256<byte>.Count)
            SplitAvx2_4(source, destination, count);
        else if (IsVector128Supported && count >= Vector128<byte>.Count)
            SplitVector128_4(source, destination, count);
        else
#endif
            SplitScalar(source, destination, count, 4);
    }

    private static void Split8(ReadOnlySpan<byte> source, Span<byte> destination, int count)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported && count >= Vector256<byte>.Count)
            SplitAvx2_8(source, destination, count);
        else if (IsVector128Supported && count >= Vector128<byte>.Count)
            SplitVector128_8(source, destination, count);
        else
#endif
            SplitScalar(source, destination, count, 8);
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// For a 16-byte block of four 4-byte values, gathers byte s of each value into 32-bit lane s.
    /// </summary>
    private static ReadOnlySpan<byte> GroupBy4 => [0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15];

    /// <summary>
    /// For a 16-byte block of two 8-byte values, gathers byte s of each value into 16-bit lane s.
    /// </summary>
    private static ReadOnlySpan<byte> GroupBy8 => [0, 8, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15];

    /// <summary>
    /// AVX2 split for 4-byte types: 32 values per iteration. Each register takes values i..i+3 in its low
    /// lane and i+16..i+19 in its high lane, so after the in-lane shuffle and a 4x4 transpose of 32-bit
    /// groups, register s holds stream s for values 0-15 (low lane) and 16-31 (high lane), in order.
    /// </summary>
    internal static void SplitAvx2_4(ReadOnlySpan<byte> source, Span<byte> destination, int count)
    {
        const int width = 4;
        const int vecLen = 32; // Vector256<byte>.Count

        ref byte src = ref MemoryMarshal.GetReference(source);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        nuint stride = (nuint)count;
        var lane = Vector128.Create(GroupBy4);
        var mask = Vector256.Create(lane, lane);

        int i = 0;
        int aligned = count - (count % vecLen);

        for (; i < aligned; i += vecLen)
        {
            nuint inOff = (nuint)i * width;
            var z0 = Avx2.Shuffle(Load2x128(ref src, inOff, inOff + 64), mask).AsUInt32();
            var z1 = Avx2.Shuffle(Load2x128(ref src, inOff + 16, inOff + 80), mask).AsUInt32();
            var z2 = Avx2.Shuffle(Load2x128(ref src, inOff + 32, inOff + 96), mask).AsUInt32();
            var z3 = Avx2.Shuffle(Load2x128(ref src, inOff + 48, inOff + 112), mask).AsUInt32();

            var a0 = Avx2.UnpackLow(z0, z1).AsUInt64();
            var a1 = Avx2.UnpackHigh(z0, z1).AsUInt64();
            var a2 = Avx2.UnpackLow(z2, z3).AsUInt64();
            var a3 = Avx2.UnpackHigh(z2, z3).AsUInt64();

            nuint at = (nuint)i;
            Avx2.UnpackLow(a0, a2).AsByte().StoreUnsafe(ref dst, at);
            Avx2.UnpackHigh(a0, a2).AsByte().StoreUnsafe(ref dst, at + stride);
            Avx2.UnpackLow(a1, a3).AsByte().StoreUnsafe(ref dst, at + 2 * stride);
            Avx2.UnpackHigh(a1, a3).AsByte().StoreUnsafe(ref dst, at + 3 * stride);
        }

        SplitTail(source, destination, count, width, i);
    }

    /// <summary>
    /// AVX2 split for 8-byte types: 32 values per iteration. Register j takes values 2j and 2j+1 in its
    /// low lane and 16+2j and 17+2j in its high lane; after the in-lane shuffle and an 8x8 transpose of
    /// 16-bit pairs, register s holds stream s for values 0-15 (low lane) and 16-31 (high lane), in order.
    /// </summary>
    internal static void SplitAvx2_8(ReadOnlySpan<byte> source, Span<byte> destination, int count)
    {
        const int width = 8;
        const int vecLen = 32; // Vector256<byte>.Count

        ref byte src = ref MemoryMarshal.GetReference(source);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        nuint stride = (nuint)count;
        var lane = Vector128.Create(GroupBy8);
        var mask = Vector256.Create(lane, lane);

        int i = 0;
        int aligned = count - (count % vecLen);

        for (; i < aligned; i += vecLen)
        {
            nuint inOff = (nuint)i * width;
            var z0 = Avx2.Shuffle(Load2x128(ref src, inOff, inOff + 128), mask).AsUInt16();
            var z1 = Avx2.Shuffle(Load2x128(ref src, inOff + 16, inOff + 144), mask).AsUInt16();
            var z2 = Avx2.Shuffle(Load2x128(ref src, inOff + 32, inOff + 160), mask).AsUInt16();
            var z3 = Avx2.Shuffle(Load2x128(ref src, inOff + 48, inOff + 176), mask).AsUInt16();
            var z4 = Avx2.Shuffle(Load2x128(ref src, inOff + 64, inOff + 192), mask).AsUInt16();
            var z5 = Avx2.Shuffle(Load2x128(ref src, inOff + 80, inOff + 208), mask).AsUInt16();
            var z6 = Avx2.Shuffle(Load2x128(ref src, inOff + 96, inOff + 224), mask).AsUInt16();
            var z7 = Avx2.Shuffle(Load2x128(ref src, inOff + 112, inOff + 240), mask).AsUInt16();

            var a0 = Avx2.UnpackLow(z0, z1).AsUInt32();
            var a1 = Avx2.UnpackHigh(z0, z1).AsUInt32();
            var a2 = Avx2.UnpackLow(z2, z3).AsUInt32();
            var a3 = Avx2.UnpackHigh(z2, z3).AsUInt32();
            var a4 = Avx2.UnpackLow(z4, z5).AsUInt32();
            var a5 = Avx2.UnpackHigh(z4, z5).AsUInt32();
            var a6 = Avx2.UnpackLow(z6, z7).AsUInt32();
            var a7 = Avx2.UnpackHigh(z6, z7).AsUInt32();

            var b0 = Avx2.UnpackLow(a0, a2).AsUInt64();
            var b1 = Avx2.UnpackHigh(a0, a2).AsUInt64();
            var b2 = Avx2.UnpackLow(a1, a3).AsUInt64();
            var b3 = Avx2.UnpackHigh(a1, a3).AsUInt64();
            var b4 = Avx2.UnpackLow(a4, a6).AsUInt64();
            var b5 = Avx2.UnpackHigh(a4, a6).AsUInt64();
            var b6 = Avx2.UnpackLow(a5, a7).AsUInt64();
            var b7 = Avx2.UnpackHigh(a5, a7).AsUInt64();

            nuint at = (nuint)i;
            Avx2.UnpackLow(b0, b4).AsByte().StoreUnsafe(ref dst, at);
            Avx2.UnpackHigh(b0, b4).AsByte().StoreUnsafe(ref dst, at + stride);
            Avx2.UnpackLow(b1, b5).AsByte().StoreUnsafe(ref dst, at + 2 * stride);
            Avx2.UnpackHigh(b1, b5).AsByte().StoreUnsafe(ref dst, at + 3 * stride);
            Avx2.UnpackLow(b2, b6).AsByte().StoreUnsafe(ref dst, at + 4 * stride);
            Avx2.UnpackHigh(b2, b6).AsByte().StoreUnsafe(ref dst, at + 5 * stride);
            Avx2.UnpackLow(b3, b7).AsByte().StoreUnsafe(ref dst, at + 6 * stride);
            Avx2.UnpackHigh(b3, b7).AsByte().StoreUnsafe(ref dst, at + 7 * stride);
        }

        SplitTail(source, destination, count, width, i);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<byte> Load2x128(ref byte src, nuint low, nuint high)
        => Vector256.Create(Vector128.LoadUnsafe(ref src, low), Vector128.LoadUnsafe(ref src, high));

    /// <summary>
    /// True when the 128-bit kernels can run: SSSE3 (for the byte shuffle) or ARM64 AdvSimd.
    /// </summary>
    internal static bool IsVector128Supported => Ssse3.IsSupported || AdvSimd.Arm64.IsSupported;

    // The 128-bit kernels share one shape across x64 and ARM64: a byte shuffle (PSHUFB / TBL), then
    // interleaves of the lower or upper halves (PUNPCKL*/PUNPCKH* / ZIP1/ZIP2). Callers check
    // IsVector128Supported first.

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<byte> Shuffle(Vector128<byte> value, Vector128<byte> indices)
        => AdvSimd.Arm64.IsSupported ? AdvSimd.Arm64.VectorTableLookup(value, indices) : Ssse3.Shuffle(value, indices);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<ushort> ZipLow(Vector128<ushort> a, Vector128<ushort> b)
        => AdvSimd.Arm64.IsSupported ? AdvSimd.Arm64.ZipLow(a, b) : Sse2.UnpackLow(a, b);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<ushort> ZipHigh(Vector128<ushort> a, Vector128<ushort> b)
        => AdvSimd.Arm64.IsSupported ? AdvSimd.Arm64.ZipHigh(a, b) : Sse2.UnpackHigh(a, b);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<uint> ZipLow(Vector128<uint> a, Vector128<uint> b)
        => AdvSimd.Arm64.IsSupported ? AdvSimd.Arm64.ZipLow(a, b) : Sse2.UnpackLow(a, b);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<uint> ZipHigh(Vector128<uint> a, Vector128<uint> b)
        => AdvSimd.Arm64.IsSupported ? AdvSimd.Arm64.ZipHigh(a, b) : Sse2.UnpackHigh(a, b);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<ulong> ZipLow(Vector128<ulong> a, Vector128<ulong> b)
        => AdvSimd.Arm64.IsSupported ? AdvSimd.Arm64.ZipLow(a, b) : Sse2.UnpackLow(a, b);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<ulong> ZipHigh(Vector128<ulong> a, Vector128<ulong> b)
        => AdvSimd.Arm64.IsSupported ? AdvSimd.Arm64.ZipHigh(a, b) : Sse2.UnpackHigh(a, b);

    /// <summary>
    /// 128-bit split for 4-byte types: 16 values per iteration. The shuffle groups each 4-value block
    /// by stream into 32-bit lanes; a 4x4 transpose of those lanes (two rounds of zips) leaves stream s
    /// for all 16 values in register s.
    /// </summary>
    internal static void SplitVector128_4(ReadOnlySpan<byte> source, Span<byte> destination, int count)
    {
        const int width = 4;
        const int vecLen = 16; // Vector128<byte>.Count

        ref byte src = ref MemoryMarshal.GetReference(source);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        nuint stride = (nuint)count;
        var mask = Vector128.Create(GroupBy4);

        int i = 0;
        int aligned = count - (count % vecLen);

        for (; i < aligned; i += vecLen)
        {
            nuint inOff = (nuint)i * width;
            var z0 = Shuffle(Vector128.LoadUnsafe(ref src, inOff), mask).AsUInt32();
            var z1 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 16), mask).AsUInt32();
            var z2 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 32), mask).AsUInt32();
            var z3 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 48), mask).AsUInt32();

            var a0 = ZipLow(z0, z1).AsUInt64();
            var a1 = ZipHigh(z0, z1).AsUInt64();
            var a2 = ZipLow(z2, z3).AsUInt64();
            var a3 = ZipHigh(z2, z3).AsUInt64();

            nuint at = (nuint)i;
            ZipLow(a0, a2).AsByte().StoreUnsafe(ref dst, at);
            ZipHigh(a0, a2).AsByte().StoreUnsafe(ref dst, at + stride);
            ZipLow(a1, a3).AsByte().StoreUnsafe(ref dst, at + 2 * stride);
            ZipHigh(a1, a3).AsByte().StoreUnsafe(ref dst, at + 3 * stride);
        }

        SplitTail(source, destination, count, width, i);
    }

    /// <summary>
    /// 128-bit split for 8-byte types: 16 values per iteration. The shuffle groups each 2-value block
    /// by stream into 16-bit lanes; an 8x8 transpose of those lanes (three rounds of zips) leaves stream s
    /// for all 16 values in register s.
    /// </summary>
    internal static void SplitVector128_8(ReadOnlySpan<byte> source, Span<byte> destination, int count)
    {
        const int width = 8;
        const int vecLen = 16; // Vector128<byte>.Count

        ref byte src = ref MemoryMarshal.GetReference(source);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        nuint stride = (nuint)count;
        var mask = Vector128.Create(GroupBy8);

        int i = 0;
        int aligned = count - (count % vecLen);

        for (; i < aligned; i += vecLen)
        {
            nuint inOff = (nuint)i * width;
            var z0 = Shuffle(Vector128.LoadUnsafe(ref src, inOff), mask).AsUInt16();
            var z1 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 16), mask).AsUInt16();
            var z2 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 32), mask).AsUInt16();
            var z3 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 48), mask).AsUInt16();
            var z4 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 64), mask).AsUInt16();
            var z5 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 80), mask).AsUInt16();
            var z6 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 96), mask).AsUInt16();
            var z7 = Shuffle(Vector128.LoadUnsafe(ref src, inOff + 112), mask).AsUInt16();

            var a0 = ZipLow(z0, z1).AsUInt32();
            var a1 = ZipHigh(z0, z1).AsUInt32();
            var a2 = ZipLow(z2, z3).AsUInt32();
            var a3 = ZipHigh(z2, z3).AsUInt32();
            var a4 = ZipLow(z4, z5).AsUInt32();
            var a5 = ZipHigh(z4, z5).AsUInt32();
            var a6 = ZipLow(z6, z7).AsUInt32();
            var a7 = ZipHigh(z6, z7).AsUInt32();

            var b0 = ZipLow(a0, a2).AsUInt64();
            var b1 = ZipHigh(a0, a2).AsUInt64();
            var b2 = ZipLow(a1, a3).AsUInt64();
            var b3 = ZipHigh(a1, a3).AsUInt64();
            var b4 = ZipLow(a4, a6).AsUInt64();
            var b5 = ZipHigh(a4, a6).AsUInt64();
            var b6 = ZipLow(a5, a7).AsUInt64();
            var b7 = ZipHigh(a5, a7).AsUInt64();

            nuint at = (nuint)i;
            ZipLow(b0, b4).AsByte().StoreUnsafe(ref dst, at);
            ZipHigh(b0, b4).AsByte().StoreUnsafe(ref dst, at + stride);
            ZipLow(b1, b5).AsByte().StoreUnsafe(ref dst, at + 2 * stride);
            ZipHigh(b1, b5).AsByte().StoreUnsafe(ref dst, at + 3 * stride);
            ZipLow(b2, b6).AsByte().StoreUnsafe(ref dst, at + 4 * stride);
            ZipHigh(b2, b6).AsByte().StoreUnsafe(ref dst, at + 5 * stride);
            ZipLow(b3, b7).AsByte().StoreUnsafe(ref dst, at + 6 * stride);
            ZipHigh(b3, b7).AsByte().StoreUnsafe(ref dst, at + 7 * stride);
        }

        SplitTail(source, destination, count, width, i);
    }

    /// <summary>The scalar remainder after a kernel's last full block, values <paramref name="start"/> on.</summary>
    private static void SplitTail(ReadOnlySpan<byte> source, Span<byte> destination, int count, int width, int start)
    {
        for (int i = start; i < count; i++)
        {
            int off = i * width;
            for (int s = 0; s < width; s++)
                destination[s * count + i] = source[off + s];
        }
    }
#endif

    /// <summary>
    /// Scalar fallback and the only path for widths other than 4 and 8.
    /// </summary>
    internal static void SplitScalar(ReadOnlySpan<byte> source, Span<byte> destination, int count, int width)
    {
        for (int stream = 0; stream < width; stream++)
        {
            var streamData = destination.Slice(stream * count, count);
            for (int i = 0; i < count; i++)
                streamData[i] = source[i * width + stream];
        }
    }
}
