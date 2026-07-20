// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes ALP (Adaptive Lossless floating-Point, encoding number 10) data pages
/// for FLOAT and DOUBLE columns. See parquet-format Encodings.md §ALP for the layout.
/// </summary>
internal static class AlpEncoder
{
    /// <summary>log2 of the default vector size (1024 elements).</summary>
    public const int DefaultLogVectorSize = 10;

    private const int MaxFloatExponent = 10;
    private const int MaxDoubleExponent = 18;

    private const float FloatMagic = (1u << 22) + (1u << 23);
    private const double DoubleMagic = (1L << 51) + (1L << 52);

    private static readonly float[] FloatPow10 =
    [
        1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f,
    ];

    private static readonly float[] FloatNegPow10 =
    [
        1e0f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f,
    ];

    /// <summary>Encodes a span of DOUBLE values into a single ALP-encoded page byte sequence.</summary>
    public static byte[] EncodeDoubles(ReadOnlySpan<double> values, int logVectorSize = DefaultLogVectorSize)
    {
        if (logVectorSize < 3 || logVectorSize > 15)
            throw new ArgumentOutOfRangeException(nameof(logVectorSize), logVectorSize,
                "log_vector_size must be in [3, 15].");

        int vectorSize = 1 << logVectorSize;
        int numVectors = (values.Length + vectorSize - 1) / vectorSize;

        var vectorBytes = new byte[numVectors][];
        int totalVectorSize = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int start = v * vectorSize;
            int n = Math.Min(vectorSize, values.Length - start);
            vectorBytes[v] = EncodeDoubleVector(values.Slice(start, n));
            totalVectorSize += vectorBytes[v].Length;
        }

        return AssemblePage(numVectors, values.Length, logVectorSize, vectorBytes, totalVectorSize);
    }

    /// <summary>Encodes a span of FLOAT values into a single ALP-encoded page byte sequence.</summary>
    public static byte[] EncodeFloats(ReadOnlySpan<float> values, int logVectorSize = DefaultLogVectorSize)
    {
        if (logVectorSize < 3 || logVectorSize > 15)
            throw new ArgumentOutOfRangeException(nameof(logVectorSize), logVectorSize,
                "log_vector_size must be in [3, 15].");

        int vectorSize = 1 << logVectorSize;
        int numVectors = (values.Length + vectorSize - 1) / vectorSize;

        var vectorBytes = new byte[numVectors][];
        int totalVectorSize = 0;
        for (int v = 0; v < numVectors; v++)
        {
            int start = v * vectorSize;
            int n = Math.Min(vectorSize, values.Length - start);
            vectorBytes[v] = EncodeFloatVector(values.Slice(start, n));
            totalVectorSize += vectorBytes[v].Length;
        }

        return AssemblePage(numVectors, values.Length, logVectorSize, vectorBytes, totalVectorSize);
    }

    private static byte[] AssemblePage(
        int numVectors, int numElements, int logVectorSize, byte[][] vectorBytes, int totalVectorSize)
    {
        int headerSize = 7;
        int offsetArraySize = numVectors * 4;
        int totalSize = headerSize + offsetArraySize + totalVectorSize;

        var page = new byte[totalSize];
        page[0] = 0; // compression_mode = ALP
        page[1] = 0; // integer_encoding = FOR + bit-packing
        page[2] = (byte)logVectorSize;
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(3, 4), numElements);

        // Offsets are measured from the start of the offset array; the first offset
        // points just past the offset array.
        uint runningOffset = (uint)offsetArraySize;
        int writePos = headerSize + offsetArraySize;
        for (int v = 0; v < numVectors; v++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(headerSize + v * 4, 4), runningOffset);
            vectorBytes[v].CopyTo(page.AsSpan(writePos));
            writePos += vectorBytes[v].Length;
            runningOffset += (uint)vectorBytes[v].Length;
        }

        return page;
    }

    // ───── DOUBLE vector encoding ─────

    private static byte[] EncodeDoubleVector(ReadOnlySpan<double> values)
    {
        int n = values.Length;

        // The Parquet spec restricts DOUBLE to exponent ∈ [0, 18]. Clast.Alp's
        // FindBestFactorExponent searches up to e=23, so we use our own
        // constrained version that calls into the same shared primitives.
        var (exponent, factor) = FindBestDoubleFactorExponent(values);

        // Encode each value, recording exceptions and substituting placeholders so the
        // FOR range stays tight.
        long[] encoded = new long[n];
        EncodeDoubleVectorWithParams(values, exponent, factor, encoded,
            out int[] exceptionPositions, out double[] exceptionValues);

        long min = encoded.Length > 0 ? encoded[0] : 0;
        long max = min;
        for (int i = 1; i < n; i++)
        {
            long e = encoded[i];
            if (e < min) min = e;
            if (e > max) max = e;
        }

        ulong range = (n == 0) ? 0UL : unchecked((ulong)(max - min));
        int bitWidth = BitsRequired64(range);

        int packedSize = (n * bitWidth + 7) / 8;
        int totalSize = 4 + 9 + packedSize + exceptionPositions.Length * 2 + exceptionValues.Length * 8;
        var buf = new byte[totalSize];

        buf[0] = (byte)exponent;
        buf[1] = (byte)factor;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2, 2), checked((ushort)exceptionPositions.Length));
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(4, 8), min);
        buf[12] = (byte)bitWidth;

        if (bitWidth > 0)
        {
            var packDest = buf.AsSpan(13, packedSize);
            for (int i = 0; i < n; i++)
                PackBits64(packDest, i, bitWidth, unchecked((ulong)(encoded[i] - min)));
        }

        int posOffset = 13 + packedSize;
        int valOffset = posOffset + exceptionPositions.Length * 2;
        for (int i = 0; i < exceptionPositions.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(
                buf.AsSpan(posOffset + i * 2, 2), checked((ushort)exceptionPositions[i]));
        for (int i = 0; i < exceptionValues.Length; i++)
        {
            long bits = BitConverter.DoubleToInt64Bits(exceptionValues[i]);
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(valOffset + i * 8, 8), bits);
        }

        return buf;
    }

    private static (int Exponent, int Factor) FindBestDoubleFactorExponent(ReadOnlySpan<double> samples)
    {
        int bestExponent = 0;
        int bestFactor = 0;
        int bestExceptions = samples.Length + 1;

        for (int e = 0; e <= MaxDoubleExponent; e++)
        {
            for (int f = 0; f <= e; f++)
            {
                int exceptions = 0;
                for (int i = 0; i < samples.Length; i++)
                {
                    double v = samples[i];

                    if (!IsFiniteDouble(v) || IsNegativeZero(v))
                    {
                        if (++exceptions >= bestExceptions) break;
                        continue;
                    }

                    long encoded = Clast.Alp.AlpEncoder.EncodeValue(v, e, f);
                    double decoded = Clast.Alp.AlpDecoder.DecodeValue(encoded, e, f);
                    if (decoded != v)
                    {
                        if (++exceptions >= bestExceptions) break;
                    }
                }

                if (exceptions < bestExceptions)
                {
                    bestExceptions = exceptions;
                    bestExponent = e;
                    bestFactor = f;
                    if (bestExceptions == 0) return (bestExponent, bestFactor);
                }
            }
        }

        return (bestExponent, bestFactor);
    }

    private static void EncodeDoubleVectorWithParams(
        ReadOnlySpan<double> values, int exponent, int factor,
        Span<long> destination,
        out int[] exceptionPositions, out double[] exceptionValues)
    {
        long fillValue = 0;
        bool hasFill = false;
        int exceptionCount = 0;

        // First pass: encode round-tripping values, count exceptions.
        for (int i = 0; i < values.Length; i++)
        {
            double v = values[i];

            if (!IsFiniteDouble(v) || IsNegativeZero(v))
            {
                exceptionCount++;
                continue;
            }

            long encoded = Clast.Alp.AlpEncoder.EncodeValue(v, exponent, factor);
            double decoded = Clast.Alp.AlpDecoder.DecodeValue(encoded, exponent, factor);
            if (decoded != v)
            {
                exceptionCount++;
            }
            else
            {
                destination[i] = encoded;
                if (!hasFill)
                {
                    fillValue = encoded;
                    hasFill = true;
                }
            }
        }

        if (exceptionCount == 0)
        {
            exceptionPositions = [];
            exceptionValues = [];
            return;
        }

        exceptionPositions = new int[exceptionCount];
        exceptionValues = new double[exceptionCount];
        int ei = 0;

        // Second pass: substitute placeholders for exception slots and record their values.
        for (int i = 0; i < values.Length; i++)
        {
            double v = values[i];
            bool special = !IsFiniteDouble(v) || IsNegativeZero(v);
            if (!special)
            {
                long encoded = Clast.Alp.AlpEncoder.EncodeValue(v, exponent, factor);
                double decoded = Clast.Alp.AlpDecoder.DecodeValue(encoded, exponent, factor);
                if (decoded != v) special = true;
            }

            if (special)
            {
                exceptionPositions[ei] = i;
                exceptionValues[ei] = v;
                destination[i] = fillValue;
                ei++;
            }
        }
    }

    private static bool IsFiniteDouble(double v) => !double.IsNaN(v) && !double.IsInfinity(v);

    // ───── FLOAT vector encoding ─────

    private static byte[] EncodeFloatVector(ReadOnlySpan<float> values)
    {
        int n = values.Length;

        var (exponent, factor) = FindBestFloatFactorExponent(values);

        int[] encoded = new int[n];
        EncodeFloatVectorWithParams(values, exponent, factor, encoded,
            out int[] exceptionPositions, out float[] exceptionValues);

        long min = n > 0 ? encoded[0] : 0;
        long max = min;
        for (int i = 1; i < n; i++)
        {
            long e = encoded[i];
            if (e < min) min = e;
            if (e > max) max = e;
        }

        ulong range = (n == 0) ? 0UL : unchecked((ulong)(max - min));
        int bitWidth = BitsRequired32(range);

        int packedSize = (n * bitWidth + 7) / 8;
        int totalSize = 4 + 5 + packedSize + exceptionPositions.Length * 2 + exceptionValues.Length * 4;
        var buf = new byte[totalSize];

        buf[0] = (byte)exponent;
        buf[1] = (byte)factor;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2, 2), checked((ushort)exceptionPositions.Length));
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(4, 4), checked((int)min));
        buf[8] = (byte)bitWidth;

        if (bitWidth > 0)
        {
            var packDest = buf.AsSpan(9, packedSize);
            for (int i = 0; i < n; i++)
                PackBits64(packDest, i, bitWidth, unchecked((ulong)(encoded[i] - min)));
        }

        int posOffset = 9 + packedSize;
        int valOffset = posOffset + exceptionPositions.Length * 2;
        for (int i = 0; i < exceptionPositions.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(
                buf.AsSpan(posOffset + i * 2, 2), checked((ushort)exceptionPositions[i]));
        for (int i = 0; i < exceptionValues.Length; i++)
        {
            byte[] fb = BitConverter.GetBytes(exceptionValues[i]);
            fb.CopyTo(buf.AsSpan(valOffset + i * 4, 4));
        }

        return buf;
    }

    private static (int Exponent, int Factor) FindBestFloatFactorExponent(ReadOnlySpan<float> samples)
    {
        int bestExponent = 0;
        int bestFactor = 0;
        int bestExceptions = samples.Length + 1;

        for (int e = 0; e <= MaxFloatExponent; e++)
        {
            float expMul = FloatPow10[e];
            float fracE = FloatNegPow10[e];

            for (int f = 0; f <= e; f++)
            {
                float fracMul = FloatNegPow10[f];
                float factMulF = FloatPow10[f];

                int exceptions = 0;
                for (int i = 0; i < samples.Length; i++)
                {
                    float v = samples[i];

                    if (!IsFiniteFloat(v) || IsNegativeZero(v))
                    {
                        if (++exceptions >= bestExceptions) break;
                        continue;
                    }

                    float scaled = v * expMul * fracMul;
                    if (scaled <= int.MinValue + 512 || scaled >= int.MaxValue - 512)
                    {
                        if (++exceptions >= bestExceptions) break;
                        continue;
                    }

                    float rounded = scaled + FloatMagic - FloatMagic;
                    int enc = (int)rounded;
                    float decoded = enc * factMulF * fracE;
                    if (decoded != v)
                    {
                        if (++exceptions >= bestExceptions) break;
                    }
                }

                if (exceptions < bestExceptions)
                {
                    bestExceptions = exceptions;
                    bestExponent = e;
                    bestFactor = f;
                    if (bestExceptions == 0) return (bestExponent, bestFactor);
                }
            }
        }

        return (bestExponent, bestFactor);
    }

    private static void EncodeFloatVectorWithParams(
        ReadOnlySpan<float> values, int exponent, int factor,
        Span<int> destination,
        out int[] exceptionPositions, out float[] exceptionValues)
    {
        float expMul = FloatPow10[exponent];
        float fracMul = FloatNegPow10[factor];
        float factMulF = FloatPow10[factor];
        float fracE = FloatNegPow10[exponent];

        int fillValue = 0;
        bool hasFill = false;
        int exceptionCount = 0;

        for (int i = 0; i < values.Length; i++)
        {
            float v = values[i];

            if (!IsFiniteFloat(v) || IsNegativeZero(v))
            {
                exceptionCount++;
                continue;
            }

            float scaled = v * expMul * fracMul;
            if (scaled <= int.MinValue + 512 || scaled >= int.MaxValue - 512)
            {
                exceptionCount++;
                continue;
            }

            float rounded = scaled + FloatMagic - FloatMagic;
            int enc = (int)rounded;
            float decoded = enc * factMulF * fracE;
            if (decoded != v)
            {
                exceptionCount++;
            }
            else
            {
                destination[i] = enc;
                if (!hasFill)
                {
                    fillValue = enc;
                    hasFill = true;
                }
            }
        }

        if (exceptionCount == 0)
        {
            exceptionPositions = [];
            exceptionValues = [];
            return;
        }

        exceptionPositions = new int[exceptionCount];
        exceptionValues = new float[exceptionCount];
        int ei = 0;

        for (int i = 0; i < values.Length; i++)
        {
            float v = values[i];

            bool special = !IsFiniteFloat(v) || IsNegativeZero(v);
            if (!special)
            {
                float scaled = v * expMul * fracMul;
                if (scaled <= int.MinValue + 512 || scaled >= int.MaxValue - 512)
                {
                    special = true;
                }
                else
                {
                    float rounded = scaled + FloatMagic - FloatMagic;
                    int enc = (int)rounded;
                    float decoded = enc * factMulF * fracE;
                    if (decoded != v)
                        special = true;
                }
            }

            if (special)
            {
                exceptionPositions[ei] = i;
                exceptionValues[ei] = v;
                destination[i] = fillValue;
                ei++;
            }
        }
    }

    // ───── Bit packing ─────

    /// <summary>
    /// Writes <paramref name="value"/> at value index <paramref name="i"/> in an LSB-first
    /// bit-packed stream of <paramref name="bitWidth"/>-bit values.
    /// </summary>
    private static void PackBits64(Span<byte> dest, int i, int bitWidth, ulong value)
    {
        long bitOffset = (long)i * bitWidth;
        int byteIdx = (int)(bitOffset >> 3);
        int bitIdx = (int)(bitOffset & 7);

        ulong low = ReadLE(dest, byteIdx);
        low |= value << bitIdx;
        WriteLE(dest, byteIdx, low);

        int spill = bitIdx + bitWidth - 64;
        if (spill > 0)
        {
            ulong high = ReadLE(dest, byteIdx + 8);
            high |= value >> (64 - bitIdx);
            WriteLE(dest, byteIdx + 8, high);
        }
    }

    private static ulong ReadLE(Span<byte> dest, int idx)
    {
        if (idx >= dest.Length) return 0;
        int rem = dest.Length - idx;
        if (rem >= 8) return BinaryPrimitives.ReadUInt64LittleEndian(dest.Slice(idx, 8));
        ulong r = 0;
        for (int k = 0; k < rem; k++) r |= (ulong)dest[idx + k] << (k * 8);
        return r;
    }

    private static void WriteLE(Span<byte> dest, int idx, ulong v)
    {
        if (idx >= dest.Length) return;
        int rem = dest.Length - idx;
        if (rem >= 8)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(idx, 8), v);
            return;
        }
        for (int k = 0; k < rem; k++) dest[idx + k] = (byte)(v >> (k * 8));
    }

    private static int BitsRequired64(ulong v)
    {
        int n = 0;
        while (v != 0)
        {
            v >>= 1;
            n++;
        }
        return n;
    }

    private static int BitsRequired32(ulong v)
    {
        int n = BitsRequired64(v);
        return n > 32 ? 32 : n;
    }

    private static bool IsFiniteFloat(float v) => !float.IsNaN(v) && !float.IsInfinity(v);

    private static bool IsNegativeZero(float v) =>
        v == 0f && BitConverter.GetBytes(v)[3] != 0; // sign bit set

    private static bool IsNegativeZero(double v) =>
        v == 0.0 && BitConverter.DoubleToInt64Bits(v) != 0L;
}
