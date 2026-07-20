// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;

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

        int produced = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int vectorOffset = (int)BinaryPrimitives.ReadUInt32LittleEndian(offsetArray.Slice(v * 4, 4));
            int valuesInVector = (v == numVectors - 1)
                ? numElements - produced
                : vectorSize;

            DecodeFloatVector(vectorsBase.Slice(vectorOffset), valuesInVector,
                destination.Slice(produced, valuesInVector));
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

        int produced = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int vectorOffset = (int)BinaryPrimitives.ReadUInt32LittleEndian(offsetArray.Slice(v * 4, 4));
            int valuesInVector = (v == numVectors - 1)
                ? numElements - produced
                : vectorSize;

            DecodeDoubleVector(vectorsBase.Slice(vectorOffset), valuesInVector,
                destination.Slice(produced, valuesInVector));
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

    private static void DecodeFloatVector(
        ReadOnlySpan<byte> vector, int n, Span<float> destination)
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
            for (int i = 0; i < n; i++)
            {
                uint delta = ExtractBitsUInt32(packed, i, bitWidth);
                long encoded = unchecked((long)delta + frameOfReference);
                destination[i] = (float)encoded * factMulF * fracEF;
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

    private static void DecodeDoubleVector(
        ReadOnlySpan<byte> vector, int n, Span<double> destination)
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

        if (bitWidth == 0)
        {
            double value = Clast.Alp.AlpDecoder.DecodeValue(frameOfReference, exponent, factor);
            destination.Slice(0, n).Fill(value);
        }
        else
        {
            for (int i = 0; i < n; i++)
            {
                ulong delta = ExtractBitsUInt64(packed, i, bitWidth);
                long encoded = unchecked((long)delta + frameOfReference);
                destination[i] = Clast.Alp.AlpDecoder.DecodeValue(encoded, exponent, factor);
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
