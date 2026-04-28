// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
#if NET8_0_OR_GREATER
using System.Runtime.Intrinsics;
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
/// </remarks>
internal static class ByteStreamSplitDecoder
{
    public static void DecodeFloats(ReadOnlySpan<byte> data, Span<float> destination, int count)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported && count >= Vector256<byte>.Count)
            UnsplitAvx2_4(data, MemoryMarshal.AsBytes(destination), count);
        else
#endif
            Unsplit(data, MemoryMarshal.AsBytes(destination), count, sizeof(float));
    }

    public static void DecodeDoubles(ReadOnlySpan<byte> data, Span<double> destination, int count)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported && count >= Vector256<byte>.Count)
            UnsplitAvx2_8(data, MemoryMarshal.AsBytes(destination), count);
        else
#endif
            Unsplit(data, MemoryMarshal.AsBytes(destination), count, sizeof(double));
    }

    public static void DecodeInt32s(ReadOnlySpan<byte> data, Span<int> destination, int count)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported && count >= Vector256<byte>.Count)
            UnsplitAvx2_4(data, MemoryMarshal.AsBytes(destination), count);
        else
#endif
            Unsplit(data, MemoryMarshal.AsBytes(destination), count, sizeof(int));
    }

    public static void DecodeInt64s(ReadOnlySpan<byte> data, Span<long> destination, int count)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported && count >= Vector256<byte>.Count)
            UnsplitAvx2_8(data, MemoryMarshal.AsBytes(destination), count);
        else
#endif
            Unsplit(data, MemoryMarshal.AsBytes(destination), count, sizeof(long));
    }

    public static void DecodeFixedLenByteArrays(ReadOnlySpan<byte> data, Span<byte> destination, int count, int typeLength)
    {
#if NET8_0_OR_GREATER
        if (Avx2.IsSupported && count >= Vector256<byte>.Count && (typeLength == 4 || typeLength == 8))
        {
            if (typeLength == 4)
                UnsplitAvx2_4(data, destination, count);
            else
                UnsplitAvx2_8(data, destination, count);
        }
        else
#endif
        {
            Unsplit(data, destination, count, typeLength);
        }
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// AVX2 unsplit for 4-byte types (float, int32). Processes 32 values per iteration
    /// by loading 32 bytes from each of 4 streams, then interleaving with unpack instructions.
    /// </summary>
    private static void UnsplitAvx2_4(ReadOnlySpan<byte> data, Span<byte> destination, int count)
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
    private static void UnsplitAvx2_8(ReadOnlySpan<byte> data, Span<byte> destination, int count)
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
#endif

    /// <summary>
    /// Scalar fallback and the only path for arbitrary type widths.
    /// </summary>
    private static void Unsplit(ReadOnlySpan<byte> data, Span<byte> destination, int count, int width)
    {
        for (int stream = 0; stream < width; stream++)
        {
            var streamData = data.Slice(stream * count, count);
            for (int i = 0; i < count; i++)
                destination[i * width + stream] = streamData[i];
        }
    }
}
