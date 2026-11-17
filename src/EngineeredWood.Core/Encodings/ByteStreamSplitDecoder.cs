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
/// Decodes BYTE_STREAM_SPLIT encoded values — used by both Parquet and Lance.
/// </summary>
/// <remarks>
/// The encoding splits each value's bytes across interleaved streams for better compression.
/// For N values of W-byte width, the data is W consecutive streams of N bytes each:
/// stream 0 has byte 0 of every value, stream 1 has byte 1 of every value, etc.
///
/// <para>Widths 4 and 8 have SIMD kernels: AVX2 where available, otherwise a 128-bit kernel on
/// SSE2 or ARM64 AdvSimd. Measured on an Apple M4 Pro, the 128-bit kernel decodes a page-sized
/// run (131,072 values) 12-20x faster than the scalar loop; on x64 without AVX2 it is 9-20x faster.</para>
/// </remarks>
internal static class ByteStreamSplitDecoder
{
    public static void DecodeFloats(ReadOnlySpan<byte> data, Span<float> destination, int count)
        => Unsplit4(data, MemoryMarshal.AsBytes(destination), count);

    public static void DecodeDoubles(ReadOnlySpan<byte> data, Span<double> destination, int count)
        => Unsplit8(data, MemoryMarshal.AsBytes(destination), count);

    public static void DecodeInt32s(ReadOnlySpan<byte> data, Span<int> destination, int count)
        => Unsplit4(data, MemoryMarshal.AsBytes(destination), count);

    public static void DecodeInt64s(ReadOnlySpan<byte> data, Span<long> destination, int count)
        => Unsplit8(data, MemoryMarshal.AsBytes(destination), count);

    public static void DecodeFixedLenByteArrays(ReadOnlySpan<byte> data, Span<byte> destination, int count, int typeLength)
    {
        if (typeLength == 4)
            Unsplit4(data, destination, count);
        else if (typeLength == 8)
            Unsplit8(data, destination, count);
        else
            Unsplit(data, destination, count, typeLength);
    }

    private static void Unsplit4(ReadOnlySpan<byte> data, Span<byte> destination, int count)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported && count >= Vector256<byte>.Count)
            UnsplitAvx2_4(data, destination, count);
        else if (IsVector128ZipSupported && count >= Vector128<byte>.Count)
            UnsplitVector128_4(data, destination, count);
        else
#endif
            Unsplit(data, destination, count, 4);
    }

    private static void Unsplit8(ReadOnlySpan<byte> data, Span<byte> destination, int count)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported && count >= Vector256<byte>.Count)
            UnsplitAvx2_8(data, destination, count);
        else if (IsVector128ZipSupported && count >= Vector128<byte>.Count)
            UnsplitVector128_8(data, destination, count);
        else
#endif
            Unsplit(data, destination, count, 8);
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// AVX2 unsplit for 4-byte types (float, int32). Processes 32 values per iteration
    /// by loading 32 bytes from each of 4 streams, then interleaving with unpack instructions.
    /// </summary>
    internal static void UnsplitAvx2_4(ReadOnlySpan<byte> data, Span<byte> destination, int count)
    {
        const int width = 4;
        const int vecLen = 32; // Vector256<byte>.Count

        ref byte src = ref MemoryMarshal.GetReference(data);
        ref byte dst = ref MemoryMarshal.GetReference(destination);

        int s0 = 0;
        int s1 = count;
        int s2 = count * 2;
        int s3 = count * 3;

        int i = 0;
        int aligned = count - (count % vecLen);

        for (; i < aligned; i += vecLen)
        {
            var v0 = Vector256.LoadUnsafe(ref src, (nuint)(s0 + i));
            var v1 = Vector256.LoadUnsafe(ref src, (nuint)(s1 + i));
            var v2 = Vector256.LoadUnsafe(ref src, (nuint)(s2 + i));
            var v3 = Vector256.LoadUnsafe(ref src, (nuint)(s3 + i));

            var lo01 = Avx2.UnpackLow(v0, v1);
            var hi01 = Avx2.UnpackHigh(v0, v1);
            var lo23 = Avx2.UnpackLow(v2, v3);
            var hi23 = Avx2.UnpackHigh(v2, v3);

            var a = Avx2.UnpackLow(lo01.AsInt16(), lo23.AsInt16()).AsByte();
            var b = Avx2.UnpackHigh(lo01.AsInt16(), lo23.AsInt16()).AsByte();
            var c = Avx2.UnpackLow(hi01.AsInt16(), hi23.AsInt16()).AsByte();
            var d = Avx2.UnpackHigh(hi01.AsInt16(), hi23.AsInt16()).AsByte();

            var r0 = Avx2.Permute2x128(a, b, 0x20);
            var r1 = Avx2.Permute2x128(c, d, 0x20);
            var r2 = Avx2.Permute2x128(a, b, 0x31);
            var r3 = Avx2.Permute2x128(c, d, 0x31);

            int outOff = i * width;
            r0.StoreUnsafe(ref dst, (nuint)outOff);
            r1.StoreUnsafe(ref dst, (nuint)(outOff + 32));
            r2.StoreUnsafe(ref dst, (nuint)(outOff + 64));
            r3.StoreUnsafe(ref dst, (nuint)(outOff + 96));
        }

        for (; i < count; i++)
        {
            int off = i * width;
            destination[off] = data[s0 + i];
            destination[off + 1] = data[s1 + i];
            destination[off + 2] = data[s2 + i];
            destination[off + 3] = data[s3 + i];
        }
    }

    /// <summary>
    /// AVX2 unsplit for 8-byte types (double, int64). Processes 32 values per iteration
    /// by loading 32 bytes from each of 8 streams, then interleaving.
    /// </summary>
    internal static void UnsplitAvx2_8(ReadOnlySpan<byte> data, Span<byte> destination, int count)
    {
        const int width = 8;
        const int vecLen = 32;

        ref byte src = ref MemoryMarshal.GetReference(data);
        ref byte dst = ref MemoryMarshal.GetReference(destination);

        int[] streamOffsets = new int[8];
        for (int s = 0; s < 8; s++)
            streamOffsets[s] = s * count;

        int i = 0;
        int aligned = count - (count % vecLen);

        for (; i < aligned; i += vecLen)
        {
            var v0 = Vector256.LoadUnsafe(ref src, (nuint)(streamOffsets[0] + i));
            var v1 = Vector256.LoadUnsafe(ref src, (nuint)(streamOffsets[1] + i));
            var v2 = Vector256.LoadUnsafe(ref src, (nuint)(streamOffsets[2] + i));
            var v3 = Vector256.LoadUnsafe(ref src, (nuint)(streamOffsets[3] + i));
            var v4 = Vector256.LoadUnsafe(ref src, (nuint)(streamOffsets[4] + i));
            var v5 = Vector256.LoadUnsafe(ref src, (nuint)(streamOffsets[5] + i));
            var v6 = Vector256.LoadUnsafe(ref src, (nuint)(streamOffsets[6] + i));
            var v7 = Vector256.LoadUnsafe(ref src, (nuint)(streamOffsets[7] + i));

            var lo01 = Avx2.UnpackLow(v0, v1);
            var hi01 = Avx2.UnpackHigh(v0, v1);
            var lo23 = Avx2.UnpackLow(v2, v3);
            var hi23 = Avx2.UnpackHigh(v2, v3);
            var lo45 = Avx2.UnpackLow(v4, v5);
            var hi45 = Avx2.UnpackHigh(v4, v5);
            var lo67 = Avx2.UnpackLow(v6, v7);
            var hi67 = Avx2.UnpackHigh(v6, v7);

            var a0 = Avx2.UnpackLow(lo01.AsInt16(), lo23.AsInt16()).AsByte();
            var a1 = Avx2.UnpackHigh(lo01.AsInt16(), lo23.AsInt16()).AsByte();
            var a2 = Avx2.UnpackLow(hi01.AsInt16(), hi23.AsInt16()).AsByte();
            var a3 = Avx2.UnpackHigh(hi01.AsInt16(), hi23.AsInt16()).AsByte();
            var b0 = Avx2.UnpackLow(lo45.AsInt16(), lo67.AsInt16()).AsByte();
            var b1 = Avx2.UnpackHigh(lo45.AsInt16(), lo67.AsInt16()).AsByte();
            var b2 = Avx2.UnpackLow(hi45.AsInt16(), hi67.AsInt16()).AsByte();
            var b3 = Avx2.UnpackHigh(hi45.AsInt16(), hi67.AsInt16()).AsByte();

            var c0 = Avx2.UnpackLow(a0.AsInt32(), b0.AsInt32()).AsByte();
            var c1 = Avx2.UnpackHigh(a0.AsInt32(), b0.AsInt32()).AsByte();
            var c2 = Avx2.UnpackLow(a1.AsInt32(), b1.AsInt32()).AsByte();
            var c3 = Avx2.UnpackHigh(a1.AsInt32(), b1.AsInt32()).AsByte();
            var c4 = Avx2.UnpackLow(a2.AsInt32(), b2.AsInt32()).AsByte();
            var c5 = Avx2.UnpackHigh(a2.AsInt32(), b2.AsInt32()).AsByte();
            var c6 = Avx2.UnpackLow(a3.AsInt32(), b3.AsInt32()).AsByte();
            var c7 = Avx2.UnpackHigh(a3.AsInt32(), b3.AsInt32()).AsByte();

            var r0 = Avx2.Permute2x128(c0, c1, 0x20);
            var r1 = Avx2.Permute2x128(c2, c3, 0x20);
            var r2 = Avx2.Permute2x128(c4, c5, 0x20);
            var r3 = Avx2.Permute2x128(c6, c7, 0x20);
            var r4 = Avx2.Permute2x128(c0, c1, 0x31);
            var r5 = Avx2.Permute2x128(c2, c3, 0x31);
            var r6 = Avx2.Permute2x128(c4, c5, 0x31);
            var r7 = Avx2.Permute2x128(c6, c7, 0x31);

            int outOff = i * width;
            r0.StoreUnsafe(ref dst, (nuint)outOff);
            r1.StoreUnsafe(ref dst, (nuint)(outOff + 32));
            r2.StoreUnsafe(ref dst, (nuint)(outOff + 64));
            r3.StoreUnsafe(ref dst, (nuint)(outOff + 96));
            r4.StoreUnsafe(ref dst, (nuint)(outOff + 128));
            r5.StoreUnsafe(ref dst, (nuint)(outOff + 160));
            r6.StoreUnsafe(ref dst, (nuint)(outOff + 192));
            r7.StoreUnsafe(ref dst, (nuint)(outOff + 224));
        }

        for (; i < count; i++)
        {
            int off = i * width;
            for (int s = 0; s < 8; s++)
                destination[off + s] = data[streamOffsets[s] + i];
        }
    }

    /// <summary>
    /// True when the 128-bit kernels can run: SSE2 (every x64 CPU) or ARM64 AdvSimd.
    /// </summary>
    internal static bool IsVector128ZipSupported => Sse2.IsSupported || AdvSimd.Arm64.IsSupported;

    // Full-width 128-bit interleaves: ZipLow(a, b) = a0 b0 a1 b1 ... from the lower halves, ZipHigh from
    // the upper halves. Sse2.UnpackLow/High and AdvSimd ZIP1/ZIP2 are exactly these operations, and they
    // are also what .NET 11's Vector128.ZipLower/ZipUpper compile to, so the explicit forms let net8.0
    // and net10.0 share the kernel. Callers check IsVector128ZipSupported first.

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<byte> ZipLow(Vector128<byte> a, Vector128<byte> b)
        => AdvSimd.Arm64.IsSupported ? AdvSimd.Arm64.ZipLow(a, b) : Sse2.UnpackLow(a, b);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<byte> ZipHigh(Vector128<byte> a, Vector128<byte> b)
        => AdvSimd.Arm64.IsSupported ? AdvSimd.Arm64.ZipHigh(a, b) : Sse2.UnpackHigh(a, b);

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

    /// <summary>
    /// 128-bit unsplit for 4-byte types: 16 values per iteration. Two rounds of zips
    /// (bytes, then 16-bit pairs) turn four 16-byte stream slices into 64 output bytes in order.
    /// </summary>
    internal static void UnsplitVector128_4(ReadOnlySpan<byte> data, Span<byte> destination, int count)
    {
        const int width = 4;
        const int vecLen = 16; // Vector128<byte>.Count

        ref byte src = ref MemoryMarshal.GetReference(data);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        nuint stride = (nuint)count;

        int i = 0;
        int aligned = count - (count % vecLen);

        for (; i < aligned; i += vecLen)
        {
            nuint at = (nuint)i;
            var v0 = Vector128.LoadUnsafe(ref src, at);
            var v1 = Vector128.LoadUnsafe(ref src, at + stride);
            var v2 = Vector128.LoadUnsafe(ref src, at + 2 * stride);
            var v3 = Vector128.LoadUnsafe(ref src, at + 3 * stride);

            var l01 = ZipLow(v0, v1).AsUInt16();
            var h01 = ZipHigh(v0, v1).AsUInt16();
            var l23 = ZipLow(v2, v3).AsUInt16();
            var h23 = ZipHigh(v2, v3).AsUInt16();

            nuint outOff = at * width;
            ZipLow(l01, l23).AsByte().StoreUnsafe(ref dst, outOff);
            ZipHigh(l01, l23).AsByte().StoreUnsafe(ref dst, outOff + 16);
            ZipLow(h01, h23).AsByte().StoreUnsafe(ref dst, outOff + 32);
            ZipHigh(h01, h23).AsByte().StoreUnsafe(ref dst, outOff + 48);
        }

        for (; i < count; i++)
        {
            int off = i * width;
            destination[off] = data[i];
            destination[off + 1] = data[count + i];
            destination[off + 2] = data[2 * count + i];
            destination[off + 3] = data[3 * count + i];
        }
    }

    /// <summary>
    /// 128-bit unsplit for 8-byte types: 16 values per iteration. Three rounds of zips
    /// (bytes, 16-bit pairs, 32-bit quads) turn eight 16-byte stream slices into 128 output bytes in order.
    /// </summary>
    internal static void UnsplitVector128_8(ReadOnlySpan<byte> data, Span<byte> destination, int count)
    {
        const int width = 8;
        const int vecLen = 16; // Vector128<byte>.Count

        ref byte src = ref MemoryMarshal.GetReference(data);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        nuint stride = (nuint)count;

        int i = 0;
        int aligned = count - (count % vecLen);

        for (; i < aligned; i += vecLen)
        {
            nuint at = (nuint)i;
            var v0 = Vector128.LoadUnsafe(ref src, at);
            var v1 = Vector128.LoadUnsafe(ref src, at + stride);
            var v2 = Vector128.LoadUnsafe(ref src, at + 2 * stride);
            var v3 = Vector128.LoadUnsafe(ref src, at + 3 * stride);
            var v4 = Vector128.LoadUnsafe(ref src, at + 4 * stride);
            var v5 = Vector128.LoadUnsafe(ref src, at + 5 * stride);
            var v6 = Vector128.LoadUnsafe(ref src, at + 6 * stride);
            var v7 = Vector128.LoadUnsafe(ref src, at + 7 * stride);

            var l01 = ZipLow(v0, v1).AsUInt16();
            var h01 = ZipHigh(v0, v1).AsUInt16();
            var l23 = ZipLow(v2, v3).AsUInt16();
            var h23 = ZipHigh(v2, v3).AsUInt16();
            var l45 = ZipLow(v4, v5).AsUInt16();
            var h45 = ZipHigh(v4, v5).AsUInt16();
            var l67 = ZipLow(v6, v7).AsUInt16();
            var h67 = ZipHigh(v6, v7).AsUInt16();

            var a0 = ZipLow(l01, l23).AsUInt32();
            var a1 = ZipHigh(l01, l23).AsUInt32();
            var a2 = ZipLow(h01, h23).AsUInt32();
            var a3 = ZipHigh(h01, h23).AsUInt32();
            var b0 = ZipLow(l45, l67).AsUInt32();
            var b1 = ZipHigh(l45, l67).AsUInt32();
            var b2 = ZipLow(h45, h67).AsUInt32();
            var b3 = ZipHigh(h45, h67).AsUInt32();

            nuint outOff = at * width;
            ZipLow(a0, b0).AsByte().StoreUnsafe(ref dst, outOff);
            ZipHigh(a0, b0).AsByte().StoreUnsafe(ref dst, outOff + 16);
            ZipLow(a1, b1).AsByte().StoreUnsafe(ref dst, outOff + 32);
            ZipHigh(a1, b1).AsByte().StoreUnsafe(ref dst, outOff + 48);
            ZipLow(a2, b2).AsByte().StoreUnsafe(ref dst, outOff + 64);
            ZipHigh(a2, b2).AsByte().StoreUnsafe(ref dst, outOff + 80);
            ZipLow(a3, b3).AsByte().StoreUnsafe(ref dst, outOff + 96);
            ZipHigh(a3, b3).AsByte().StoreUnsafe(ref dst, outOff + 112);
        }

        for (; i < count; i++)
        {
            int off = i * width;
            for (int s = 0; s < width; s++)
                destination[off + s] = data[s * count + i];
        }
    }
#endif

    /// <summary>
    /// Scalar fallback and the only path for arbitrary type widths.
    /// </summary>
    internal static void Unsplit(ReadOnlySpan<byte> data, Span<byte> destination, int count, int width)
    {
        for (int stream = 0; stream < width; stream++)
        {
            var streamData = data.Slice(stream * count, count);
            for (int i = 0; i < count; i++)
                destination[i * width + stream] = streamData[i];
        }
    }
}
