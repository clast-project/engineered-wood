// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#if NET8_0_OR_GREATER
using System.Runtime.Intrinsics;
#endif

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes ALP (Adaptive Lossless floating-Point, encoding number 10) data pages
/// for FLOAT and DOUBLE columns.
/// </summary>
/// <remarks>
/// Page layout (see parquet-format Encodings.md §ALP):
///
/// <code>
/// +-------------+-----------------------------+--------------------------------------+
/// |   Header    |        Offset Array         |            Vector Data               |
/// |  (7 bytes)  |   (num_vectors * 4 bytes)   |            (variable)                |
/// +-------------+-----------------------------+--------------------------------------+
/// </code>
///
/// Each vector contains an AlpInfo header (exponent, factor, num_exceptions),
/// a ForInfo header (frame_of_reference int32/int64 + bit_width), bit-packed
/// FOR-encoded deltas, then exception positions (uint16) and exception values.
/// </remarks>
internal static class AlpDecoder
{
    private const int PageHeaderSize = 7;
    private const int AlpInfoSize = 4;
    private const int FloatForInfoSize = 5;
    private const int DoubleForInfoSize = 9;
    private const int FloatExceptionValueSize = 4;
    private const int DoubleExceptionValueSize = 8;
    private const int ExceptionPositionSize = 2;

    /// <summary>
    /// Values unpacked into scratch in one pass before being transformed. One canonical ALP vector,
    /// which keeps the scratch inside L1; larger vectors are unpacked a tile at a time.
    /// </summary>
    private const int UnpackTile = 1024;

    /// <summary>
    /// Reused scratch for the bulk unpack, one per thread, matching how the writer holds its own
    /// staging buffers. A <c>stackalloc</c> would work too, but 8 KB is a large frame to take on
    /// every page decode and the runtime zeroes it on each call; this costs one allocation per
    /// thread for the life of the process instead.
    /// </summary>
    [ThreadStatic]
    private static ulong[]? t_unpackScratch;

    /// <summary>
    /// Magnitude bound for the branch-free integer-to-double conversion: it is exact only while the
    /// integer fits in 51 bits, because the mantissa trick below biases by 2^51 into the 52 bits a
    /// double's mantissa holds exactly.
    /// </summary>
    private const long MagicBias = 1L << 51;

    /// <summary>The bit pattern of 2^52 as a double — OR an integer into it and the mantissa is the integer.</summary>
    private const ulong MagicMantissa = 0x4330000000000000UL;

    /// <summary>2^52 + 2^51: the bias to subtract back off after the mantissa trick.</summary>
    private const double MagicOffset = 4503599627370496.0 + 2251799813685248.0;

    /// <summary>
    /// Widest bit width for which one 64-bit read always covers a whole value. A value starts at
    /// most 7 bits into its first byte, so 7 + width must fit in 64. Above this a value can straddle
    /// two words and the per-value path handles it.
    /// </summary>
    private const int MaxSingleReadBitWidth = 57;

    /// <summary>Decodes an ALP-encoded page of FLOAT values.</summary>
    public static void DecodeFloats(ReadOnlySpan<byte> data, Span<float> destination, int count)
    {
        var (logVectorSize, numElements) = ReadPageHeader(data);
        if (numElements != count)
            throw new ParquetFormatException(
                $"ALP page header num_elements ({numElements}) does not match expected count ({count}).");

        int vectorSize = 1 << logVectorSize;
        int numVectors = (numElements + vectorSize - 1) / vectorSize;

        var offsetArray = data.Slice(PageHeaderSize, numVectors * 4);
        var vectorsBase = data.Slice(PageHeaderSize);

        // One scratch buffer for the whole page: the deltas are unpacked into it in bulk and then
        // transformed, rather than each value being extracted and transformed on its own.
        ulong[] scratch = t_unpackScratch ??= new ulong[UnpackTile];

        int produced = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int vectorOffset = (int)BinaryPrimitives.ReadUInt32LittleEndian(offsetArray.Slice(v * 4, 4));
            int valuesInVector = (v == numVectors - 1)
                ? numElements - produced
                : vectorSize;

            DecodeFloatVector(vectorsBase.Slice(vectorOffset), valuesInVector,
                destination.Slice(produced, valuesInVector), scratch);
            produced += valuesInVector;
        }
    }

    /// <summary>Decodes an ALP-encoded page of DOUBLE values.</summary>
    public static void DecodeDoubles(ReadOnlySpan<byte> data, Span<double> destination, int count)
    {
        var (logVectorSize, numElements) = ReadPageHeader(data);
        if (numElements != count)
            throw new ParquetFormatException(
                $"ALP page header num_elements ({numElements}) does not match expected count ({count}).");

        int vectorSize = 1 << logVectorSize;
        int numVectors = (numElements + vectorSize - 1) / vectorSize;

        var offsetArray = data.Slice(PageHeaderSize, numVectors * 4);
        var vectorsBase = data.Slice(PageHeaderSize);

        // One scratch buffer for the whole page: the deltas are unpacked into it in bulk and then
        // transformed, rather than each value being extracted and transformed on its own.
        ulong[] scratch = t_unpackScratch ??= new ulong[UnpackTile];

        int produced = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int vectorOffset = (int)BinaryPrimitives.ReadUInt32LittleEndian(offsetArray.Slice(v * 4, 4));
            int valuesInVector = (v == numVectors - 1)
                ? numElements - produced
                : vectorSize;

            DecodeDoubleVector(vectorsBase.Slice(vectorOffset), valuesInVector,
                destination.Slice(produced, valuesInVector), scratch);
            produced += valuesInVector;
        }
    }

    private static (int LogVectorSize, int NumElements) ReadPageHeader(ReadOnlySpan<byte> data)
    {
        if (data.Length < PageHeaderSize)
            throw new ParquetFormatException(
                $"ALP page is too small to contain a header ({data.Length} bytes).");

        byte compressionMode = data[0];
        if (compressionMode != 0)
            throw new ParquetFormatException(
                $"Unsupported ALP compression_mode {compressionMode} (only 0 = ALP is supported).");

        byte integerEncoding = data[1];
        if (integerEncoding != 0)
            throw new ParquetFormatException(
                $"Unsupported ALP integer_encoding {integerEncoding} (only 0 = FOR + bit-packing is supported).");

        byte logVectorSize = data[2];
        if (logVectorSize < 3 || logVectorSize > 15)
            throw new ParquetFormatException(
                $"ALP log_vector_size {logVectorSize} is out of the allowed range [3, 15].");

        int numElements = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(3, 4));
        if (numElements < 0)
            throw new ParquetFormatException(
                $"ALP num_elements {numElements} is negative.");

        return (logVectorSize, numElements);
    }

#if NET8_0_OR_GREATER
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
#endif
    private static void DecodeFloatVector(
        ReadOnlySpan<byte> vector, int n, Span<float> destination, Span<ulong> scratch)
    {
        if (vector.Length < AlpInfoSize + FloatForInfoSize)
            throw new ParquetFormatException("ALP FLOAT vector is too small to contain its header.");

        int exponent = vector[0];
        int factor = vector[1];
        int numExceptions = BinaryPrimitives.ReadUInt16LittleEndian(vector.Slice(2, 2));
        ValidateExponentFactor(exponent, factor, isFloat: true);

        int frameOfReference = BinaryPrimitives.ReadInt32LittleEndian(vector.Slice(AlpInfoSize, 4));
        int bitWidth = vector[AlpInfoSize + 4];
        if (bitWidth > 32)
            throw new ParquetFormatException(
                $"ALP FLOAT bit_width {bitWidth} exceeds 32.");

        int packedSize = (n * bitWidth + 7) / 8;
        int headerSize = AlpInfoSize + FloatForInfoSize;
        if (vector.Length < headerSize + packedSize +
            numExceptions * (ExceptionPositionSize + FloatExceptionValueSize))
            throw new ParquetFormatException("ALP FLOAT vector is truncated.");

        var packed = vector.Slice(headerSize, packedSize);

        // Decoding formula: value = (float)((float)encoded * 10^factor * 10^(-exponent)).
        // The spec requires float-precision arithmetic for FLOAT to match the encoder's
        // rounding bit-exactly across languages.
        float factMulF = FloatPow10[factor];
        float fracEF = FloatNegPow10[exponent];

        if (bitWidth == 0)
        {
            float value = (float)frameOfReference * factMulF * fracEF;
            destination.Slice(0, n).Fill(value);
        }
        else
        {
            int produced = 0;
            while (produced < n)
            {
                // It is the tile's START that has to be byte-aligned, not its length: `produced`
                // only ever advances by whole tiles and UnpackTile is a multiple of eight, so the
                // offset below is always an exact number of bytes. The last tile can be any size,
                // and UnpackDeltas leaves whatever does not fill a group of eight to the loop after
                // it.
                int tile = Math.Min(UnpackTile, n - produced);
                int unpacked = UnpackDeltas(
                    packed.Slice((produced * bitWidth) >> 3), bitWidth, tile, scratch);

                for (int i = 0; i < unpacked; i++)
                {
                    long encoded = unchecked((long)(uint)scratch[i] + frameOfReference);
                    destination[produced + i] = (float)encoded * factMulF * fracEF;
                }

                for (int i = unpacked; i < tile; i++)
                {
                    uint delta = ExtractBitsUInt32(packed, produced + i, bitWidth);
                    long encoded = unchecked((long)delta + frameOfReference);
                    destination[produced + i] = (float)encoded * factMulF * fracEF;
                }

                produced += tile;
            }
        }

        if (numExceptions > 0)
        {
            int positionsOffset = headerSize + packedSize;
            int valuesOffset = positionsOffset + numExceptions * ExceptionPositionSize;
            var positions = vector.Slice(positionsOffset, numExceptions * ExceptionPositionSize);
            var values = vector.Slice(valuesOffset, numExceptions * FloatExceptionValueSize);

            for (int i = 0; i < numExceptions; i++)
            {
                int pos = BinaryPrimitives.ReadUInt16LittleEndian(positions.Slice(i * 2, 2));
                if ((uint)pos >= (uint)n)
                    throw new ParquetFormatException(
                        $"ALP FLOAT exception position {pos} is out of range for vector of size {n}.");
                destination[pos] = MemoryMarshal.Cast<byte, float>(values.Slice(i * 4, 4))[0];
            }
        }
    }

#if NET8_0_OR_GREATER
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
#endif
    private static void DecodeDoubleVector(
        ReadOnlySpan<byte> vector, int n, Span<double> destination, Span<ulong> scratch)
    {
        if (vector.Length < AlpInfoSize + DoubleForInfoSize)
            throw new ParquetFormatException("ALP DOUBLE vector is too small to contain its header.");

        int exponent = vector[0];
        int factor = vector[1];
        int numExceptions = BinaryPrimitives.ReadUInt16LittleEndian(vector.Slice(2, 2));
        ValidateExponentFactor(exponent, factor, isFloat: false);

        long frameOfReference = BinaryPrimitives.ReadInt64LittleEndian(vector.Slice(AlpInfoSize, 8));
        int bitWidth = vector[AlpInfoSize + 8];
        if (bitWidth > 64)
            throw new ParquetFormatException(
                $"ALP DOUBLE bit_width {bitWidth} exceeds 64.");

        int packedSize = (n * bitWidth + 7) / 8;
        int headerSize = AlpInfoSize + DoubleForInfoSize;
        if (vector.Length < headerSize + packedSize +
            numExceptions * (ExceptionPositionSize + DoubleExceptionValueSize))
            throw new ParquetFormatException("ALP DOUBLE vector is truncated.");

        var packed = vector.Slice(headerSize, packedSize);

        // Decoding formula: value = (double)(encoded * 10^factor) * 10^(-exponent), scaling as an
        // int64 first. Written out here rather than called per value through Clast.Alp, which is
        // what the FLOAT path above already does: MEASURED, the call does not inline, and writing
        // it out took decode from 5.6 to 7.4 GB/s over the CWI ALP corpus.
        //
        // The int64 scale is not incidental. It is what makes this bit-identical to
        // Clast.Alp.AlpDecoder.DecodeValue on every input — including the overflow a corrupt page
        // can reach but legitimately encoded data cannot, where the same expression in double
        // arithmetic diverges. AlpDecoderTests pins that across all 190 (exponent, factor) pairs.
        long factMul = DoubleIntPow10[factor];
        double fracE = DoubleNegPow10[exponent];

        if (bitWidth == 0)
        {
            double value = (double)unchecked(frameOfReference * factMul) * fracE;
            destination.Slice(0, n).Fill(value);
        }
        else
        {
            // Decided once for the vector: every delta in it shares one frame of reference and one
            // bit width, so both halves of the guard — the encoded values fitting the range where
            // converting to double is exact, and scaling them not overflowing int64 — are
            // properties of the vector header rather than of any individual value.
            bool convertible = EncodedValuesFitConversion(frameOfReference, bitWidth, factMul);

            int produced = 0;
            while (produced < n)
            {
                // It is the tile's START that has to be byte-aligned, not its length: `produced`
                // only ever advances by whole tiles and UnpackTile is a multiple of eight, so the
                // offset below is always an exact number of bytes. The last tile can be any size,
                // and UnpackDeltas leaves whatever does not fill a group of eight to the loop after
                // it.
                int tile = Math.Min(UnpackTile, n - produced);
                int unpacked = UnpackDeltas(
                    packed.Slice((produced * bitWidth) >> 3), bitWidth, tile, scratch);

                Transform(
                    scratch.Slice(0, unpacked), destination.Slice(produced, unpacked),
                    frameOfReference, factMul, fracE, convertible);

                // Whatever the bulk path declined — a width that can straddle two words, or the
                // groups whose reads would run past the buffer — one value at a time.
                for (int i = unpacked; i < tile; i++)
                {
                    ulong delta = ExtractBitsUInt64(packed, produced + i, bitWidth);
                    long encoded = unchecked((long)delta + frameOfReference);
                    destination[produced + i] = (double)unchecked(encoded * factMul) * fracE;
                }

                produced += tile;
            }
        }

        if (numExceptions > 0)
        {
            int positionsOffset = headerSize + packedSize;
            int valuesOffset = positionsOffset + numExceptions * ExceptionPositionSize;
            var positions = vector.Slice(positionsOffset, numExceptions * ExceptionPositionSize);
            var values = vector.Slice(valuesOffset, numExceptions * DoubleExceptionValueSize);

            for (int i = 0; i < numExceptions; i++)
            {
                int pos = BinaryPrimitives.ReadUInt16LittleEndian(positions.Slice(i * 2, 2));
                if ((uint)pos >= (uint)n)
                    throw new ParquetFormatException(
                        $"ALP DOUBLE exception position {pos} is out of range for vector of size {n}.");
                destination[pos] = MemoryMarshal.Cast<byte, double>(values.Slice(i * 8, 8))[0];
            }
        }
    }

    private static void ValidateExponentFactor(int exponent, int factor, bool isFloat)
    {
        int maxExponent = isFloat ? 10 : 18;
        if ((uint)exponent > (uint)maxExponent)
            throw new ParquetFormatException(
                $"ALP exponent {exponent} is out of range [0, {maxExponent}].");
        if ((uint)factor > (uint)exponent)
            throw new ParquetFormatException(
                $"ALP factor {factor} must be in [0, exponent={exponent}].");
    }

    /// <summary>
    /// Turns unpacked deltas into values: add the frame of reference, scale by 10^factor as an
    /// int64, convert, and scale by 10^-exponent.
    /// </summary>
    /// <remarks>
    /// <para>The vectorized path converts without any int64-to-double instruction, which AVX2 does
    /// not have — biasing the integer into the mantissa of 2^52 and subtracting the bias back off
    /// costs an add, an OR and a subtract. MEASURED at 2.7x the scalar loop over an L1-resident
    /// tile; <c>Vector256.ConvertToDouble</c>, which is the obvious thing to reach for, is
    /// emulated without AVX-512DQ and comes out at 0.57x instead.</para>
    /// <para>It also multiplies by 10^factor <b>after</b> converting rather than before, because
    /// AVX2 has no 64-bit integer multiply either. That reorder costs no accuracy: both operands are
    /// exactly representable, so the multiply rounds the same exact real product that converting the
    /// int64 product would have rounded. What it cannot reproduce is the scalar form's wrap when
    /// that int64 product overflows, so the guard declines there.</para>
    /// </remarks>
#if NET8_0_OR_GREATER
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
#endif
    private static void Transform(
        ReadOnlySpan<ulong> deltas, Span<double> destination,
        long frameOfReference, long factMul, double fracE, bool convertible)
    {
        int i = 0;

#if NET8_0_OR_GREATER
        if (convertible && Vector256.IsHardwareAccelerated && deltas.Length >= Vector256<ulong>.Count)
        {
            // The frame of reference and the conversion's bias fold into one addend.
            var frame = Vector256.Create(frameOfReference + MagicBias);
            var mantissa = Vector256.Create(MagicMantissa);
            var offset = Vector256.Create(MagicOffset);
            var factor = Vector256.Create((double)factMul);
            var scale = Vector256.Create(fracE);

            ref ulong source = ref MemoryMarshal.GetReference(deltas);
            ref double target = ref MemoryMarshal.GetReference(destination);

            for (; i <= deltas.Length - Vector256<ulong>.Count; i += Vector256<ulong>.Count)
            {
                var biased = (Vector256.LoadUnsafe(ref source, (nuint)i).AsInt64() + frame).AsUInt64();
                var encoded = (biased | mantissa).AsDouble() - offset;
                ((encoded * factor) * scale).StoreUnsafe(ref target, (nuint)i);
            }
        }
#endif

        for (; i < deltas.Length; i++)
        {
            long encoded = unchecked((long)deltas[i] + frameOfReference);
            destination[i] = (double)unchecked(encoded * factMul) * fracE;
        }
    }

    /// <summary>
    /// Whether this vector can take the vectorized transform. Decided from the header alone: deltas
    /// run from 0 to 2^bitWidth - 1, so the encoded values run from the frame of reference to that
    /// plus the span.
    /// </summary>
    /// <remarks>
    /// Two conditions, and it matters which quantity each one is about. The conversion is exact only
    /// for encoded values inside 2^51 — that is a bound on the <b>encoded value</b>, not on the
    /// scaled one. The scaled one only has to avoid overflowing int64, because that is the single
    /// case where the scalar form's wrapping multiply and the reordered double multiply part company.
    /// Bounding the scaled value by 2^51 instead would be correct but nearly useless: MEASURED, it
    /// turns the fast path off for every dataset in the CWI corpus, since a factor of 10^13 against
    /// a three-digit encoded value already lands past 2^51.
    /// </remarks>
    private static bool EncodedValuesFitConversion(long frameOfReference, int bitWidth, long factMul)
    {
        // Above this the bulk unpacker declines anyway, and the shift below would be undefined.
        if (bitWidth > MaxSingleReadBitWidth)
            return false;

        long span = (1L << bitWidth) - 1;
        const long Limit = MagicBias - 1;

        // Written so nothing overflows: the second test is only reached once the frame of reference
        // is known to be at least -Limit, which keeps the subtraction inside 2^52.
        if (frameOfReference < -Limit || span > Limit - frameOfReference)
            return false;

        long widest = Math.Max(Math.Abs(frameOfReference), Math.Abs(frameOfReference + span));
        return widest <= long.MaxValue / factMul;
    }

    /// <summary>
    /// Unpacks whole groups of eight values from the front of an LSB-first bit-packed stream,
    /// returning how many it produced. The caller finishes anything left over.
    /// </summary>
    /// <remarks>
    /// <para>Eight values at <paramref name="bitWidth"/> bits occupy exactly <paramref name="bitWidth"/>
    /// bytes, so every group of eight realigns to a byte boundary and the eight byte offsets and
    /// shifts within a group depend only on the width. Hoisting them out of the loop turns each
    /// value into one unaligned 64-bit read, one shift and one mask, with no per-value arithmetic,
    /// no bounds check and no branch.</para>
    /// <para>The methods on this path are marked <c>AggressiveOptimization</c>. MEASURED: without
    /// it the staged decode sits at 1.7 GB/s until it has been called a few thousand times — worse
    /// than the 5.9 GB/s of the per-value code it replaced — because the unrolled kernel is far
    /// more sensitive to tier-0 and instrumented codegen than a simple loop was. A reader touching
    /// only a handful of pages would otherwise have come out three times slower.</para>
    /// <para>MEASURED: this is worth about 2.2x over extracting values one at a time, and it beat a
    /// version specialised into one kernel per bit width — the variable shifts cost the same as
    /// immediate ones on anything with BMI2, and one small method stays in cache where fifty-seven
    /// do not. An AVX2 kernel measured a further 1.25x on the widths it can handle, which did not
    /// justify a shuffle-mask table and a second code path.</para>
    /// </remarks>
#if NET8_0_OR_GREATER
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
#endif
    private static int UnpackDeltas(
        ReadOnlySpan<byte> packed, int bitWidth, int count, Span<ulong> destination)
    {
        // Above this a value can straddle two 64-bit words, which this kernel does not handle.
        if (bitWidth > MaxSingleReadBitWidth)
            return 0;

        int o1 = bitWidth >> 3, o2 = (2 * bitWidth) >> 3, o3 = (3 * bitWidth) >> 3;
        int o4 = (4 * bitWidth) >> 3, o5 = (5 * bitWidth) >> 3;
        int o6 = (6 * bitWidth) >> 3, o7 = (7 * bitWidth) >> 3;

        // Every group reads a whole word at o7, which for narrow widths reaches past the group's
        // own bytes. Groups whose read would leave the buffer are left to the caller.
        if (packed.Length < o7 + 8)
            return 0;

        int groups = Math.Min(count / 8, (packed.Length - o7 - 8) / bitWidth + 1);
        if (groups <= 0)
            return 0;

        int s1 = bitWidth & 7, s2 = (2 * bitWidth) & 7, s3 = (3 * bitWidth) & 7;
        int s4 = (4 * bitWidth) & 7, s5 = (5 * bitWidth) & 7;
        int s6 = (6 * bitWidth) & 7, s7 = (7 * bitWidth) & 7;
        ulong mask = (1UL << bitWidth) - 1UL;

        ref byte source = ref MemoryMarshal.GetReference(packed);

        for (int g = 0; g < groups; g++)
        {
            int b = g * bitWidth;
            int o = g * 8;

            destination[o] = ReadWord(ref source, b) & mask;
            destination[o + 1] = (ReadWord(ref source, b + o1) >> s1) & mask;
            destination[o + 2] = (ReadWord(ref source, b + o2) >> s2) & mask;
            destination[o + 3] = (ReadWord(ref source, b + o3) >> s3) & mask;
            destination[o + 4] = (ReadWord(ref source, b + o4) >> s4) & mask;
            destination[o + 5] = (ReadWord(ref source, b + o5) >> s5) & mask;
            destination[o + 6] = (ReadWord(ref source, b + o6) >> s6) & mask;
            destination[o + 7] = (ReadWord(ref source, b + o7) >> s7) & mask;
        }

        return groups * 8;
    }

    /// <summary>Reads a little-endian 64-bit word at a byte offset, aligned or not.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadWord(ref byte source, int byteOffset)
    {
        ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref source, byteOffset));
        return BitConverter.IsLittleEndian ? value : BinaryPrimitives.ReverseEndianness(value);
    }

    /// <summary>
    /// Extracts <paramref name="bitWidth"/> bits at value index <paramref name="i"/>
    /// from an LSB-first bit-packed stream. <paramref name="bitWidth"/> must be in [1, 32].
    /// </summary>
    private static uint ExtractBitsUInt32(ReadOnlySpan<byte> packed, int i, int bitWidth)
    {
        long bitOffset = (long)i * bitWidth;
        int byteIndex = (int)(bitOffset >> 3);
        int bitIndex = (int)(bitOffset & 7);

        // Read up to 8 bytes to safely span a 32-bit value at any bit alignment.
        ulong raw = 0;
        int rem = packed.Length - byteIndex;
        int toRead = rem >= 8 ? 8 : rem;
        if (toRead == 8)
        {
            raw = BinaryPrimitives.ReadUInt64LittleEndian(packed.Slice(byteIndex, 8));
        }
        else
        {
            for (int k = 0; k < toRead; k++)
                raw |= (ulong)packed[byteIndex + k] << (k * 8);
        }

        uint mask = bitWidth == 32 ? uint.MaxValue : (uint)((1u << bitWidth) - 1);
        return (uint)((raw >> bitIndex) & mask);
    }

    /// <summary>
    /// Extracts <paramref name="bitWidth"/> bits at value index <paramref name="i"/>
    /// from an LSB-first bit-packed stream. <paramref name="bitWidth"/> must be in [1, 64].
    /// </summary>
    private static ulong ExtractBitsUInt64(ReadOnlySpan<byte> packed, int i, int bitWidth)
    {
        long bitOffset = (long)i * bitWidth;
        int byteIndex = (int)(bitOffset >> 3);
        int bitIndex = (int)(bitOffset & 7);

        // Need up to two 64-bit words: low word covers bits [byteIndex .. byteIndex+8),
        // high word covers the spillover when bitIndex + bitWidth > 64.
        ulong low = ReadUInt64Padded(packed, byteIndex);
        ulong value = bitIndex == 0 ? low : (low >> bitIndex);

        int spill = bitIndex + bitWidth - 64;
        if (spill > 0)
        {
            ulong high = ReadUInt64Padded(packed, byteIndex + 8);
            value |= high << (64 - bitIndex);
        }

        ulong mask = bitWidth == 64 ? ulong.MaxValue : ((1UL << bitWidth) - 1UL);
        return value & mask;
    }

    private static ulong ReadUInt64Padded(ReadOnlySpan<byte> packed, int byteIndex)
    {
        if (byteIndex < 0 || byteIndex >= packed.Length)
            return 0UL;
        int rem = packed.Length - byteIndex;
        if (rem >= 8)
            return BinaryPrimitives.ReadUInt64LittleEndian(packed.Slice(byteIndex, 8));

        ulong result = 0;
        for (int k = 0; k < rem; k++)
            result |= (ulong)packed[byteIndex + k] << (k * 8);
        return result;
    }

    /// <summary>
    /// Powers of 10 as int64: index f holds <c>10^f</c>. Indices 0..18 cover the DOUBLE factor
    /// range allowed by the spec, and 10^18 is the largest power of ten an int64 holds.
    /// </summary>
    private static readonly long[] DoubleIntPow10 =
    [
        1L, 10L, 100L, 1_000L, 10_000L, 100_000L,
        1_000_000L, 10_000_000L, 100_000_000L, 1_000_000_000L,
        10_000_000_000L, 100_000_000_000L, 1_000_000_000_000L, 10_000_000_000_000L,
        100_000_000_000_000L, 1_000_000_000_000_000L, 10_000_000_000_000_000L,
        100_000_000_000_000_000L, 1_000_000_000_000_000_000L,
    ];

    /// <summary>
    /// Negative powers of 10 in double precision: index e holds <c>10^(-e)</c>.
    /// Indices 0..18 cover the DOUBLE exponent range allowed by the spec.
    /// </summary>
    private static readonly double[] DoubleNegPow10 =
    [
        1e0,   1e-1,  1e-2,  1e-3,  1e-4,  1e-5,  1e-6,
        1e-7,  1e-8,  1e-9,  1e-10, 1e-11, 1e-12,
        1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18,
    ];

    /// <summary>
    /// Powers of 10 in single precision: index e holds <c>(float)10^e</c>.
    /// Indices 0..10 cover the FLOAT factor range allowed by the spec.
    /// </summary>
    private static readonly float[] FloatPow10 =
    [
        1e0f,  1e1f,  1e2f,  1e3f,  1e4f,  1e5f,
        1e6f,  1e7f,  1e8f,  1e9f,  1e10f,
    ];

    /// <summary>
    /// Negative powers of 10 in single precision: index e holds <c>(float)10^(-e)</c>.
    /// Indices 0..10 cover the FLOAT exponent range allowed by the spec.
    /// </summary>
    private static readonly float[] FloatNegPow10 =
    [
        1e0f,  1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f,
        1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f,
    ];
}
