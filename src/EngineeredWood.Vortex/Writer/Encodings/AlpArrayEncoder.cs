// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.AlpArrayDecoder"/>:
/// emits a <c>vortex.alp</c> ArrayNode subtree for floating-point columns.
///
/// <para>ALP (Adaptive Lossless floating Point) encodes each value as
/// <c>round(value * 10^e * 10^-f)</c> cast to an integer (i32 for f32, i64 for
/// f64) where (e, f) are picked per-column to maximize how many values
/// round-trip bit-exactly. Values that don't roundtrip become patches
/// (out-of-band index + original-bit-pattern value). Decode is
/// <c>encoded * 10^f * 10^-e</c>; the reader overwrites patched indices with
/// their stored originals. See "ALP: Adaptive Lossless floating-Point
/// Compression" (Afroozeh & Boncz, VLDB 2024).</para>
///
/// <para>Wire shape: 0 buffers, 1 child (encoded primitive) or 3 children
/// (encoded + patch_indices + patch_values), metadata
/// <c>ALPMetadata { exp_e, exp_f, patches? }</c>. The float column's validity
/// bitmap rides with the encoded child (the decoder reuses it on the float
/// output).</para>
///
/// <para>Scope: <see cref="FloatArray"/> / <see cref="DoubleArray"/>,
/// nullable + non-nullable, sliced + non-sliced. Caller (dispatch) gates on
/// a probe in <see cref="IsApplicable"/> that runs the full
/// encode-and-measure pipeline once and only accepts when the result is
/// meaningfully smaller than raw.</para>
/// </summary>
internal static class AlpArrayEncoder
{
    private const int ElementsPerChunk = 1024;

    // f32: SWEET = 2^23 + 2^22, MAX_EXPONENT = 10.
    private const float SweetF32 = (1 << 23) + (1 << 22);
    private const int MaxExponentF32 = 10;
    private static readonly float[] F10F32 = {
        1f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f,
    };
    private static readonly float[] If10F32 = {
        1f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f,
    };

    // f64: SWEET = 2^52 + 2^51, MAX_EXPONENT = 18.
    private const double SweetF64 = (1L << 52) + (1L << 51);
    private const int MaxExponentF64 = 18;
    private static readonly double[] F10F64 = {
        1d, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
        1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
    };
    private static readonly double[] If10F64 = {
        1d, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9,
        1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18,
    };

    /// <summary>
    /// True iff the column is float, non-sliced, and the best (e, f) gives
    /// a meaningfully smaller encoding than raw float storage. Runs the full
    /// pipeline once; the result is recomputed in <see cref="Emit"/> (cost is
    /// bounded — O(n × MAX_EXPONENT²) but we sample large columns).
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is null) return false;
        if (array is not FloatArray && array is not DoubleArray) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n == 0) return false;
        // All-null column: nothing meaningful to encode.
        if (data.GetNullCount() == n) return false;

        // Pick best exponents on a sample, then estimate full size.
        var (e, f, encodedBits, patchCount) = FindBestExponentsAndEstimate(array);
        if (e < 0) return false;

        // Cost: encoded_bits × n / 8 + patches × (sizeof(T) + 8 [u64 index]).
        long encodedBytes = ((long)encodedBits * n + 7) / 8;
        int valByteWidth = array is FloatArray ? 4 : 8;
        long patchBytes = (long)patchCount * (valByteWidth + 8);
        long alpBytes = encodedBytes + patchBytes;
        long rawBytes = (long)n * valByteWidth;
        // Require ≥ 1.5x compression to justify the encoding overhead.
        return alpBytes * 3 / 2 < rawBytes;
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        if (array is not FloatArray && array is not DoubleArray)
            throw new NotSupportedException(
                $"vortex.alp writer requires FloatArray or DoubleArray, got {array.GetType().Name}.");
        var data = ((Apache.Arrow.Array)array).Data;

        // 1. Pick best exponents and run the full encode pipeline.
        var (expE, expF, _, _) = FindBestExponentsAndEstimate(array);
        if (expE < 0)
            throw new InvalidOperationException(
                $"vortex.alp couldn't find exponents that compress {array.GetType().Name}.");

        var (encodedArray, patchIndicesArray, patchValuesArray) =
            EncodeAtExponents(array, expE, expF);

        // 2. Children. The encoded ints typically have a narrow range — for
        //    a 5-distinct floats column they might fit in 3 bits if min ≥ 0,
        //    or be FoR-friendly when negatives are involved. Pass compress=true
        //    so the recursive dispatch picks bitpacked / FoR / etc. — without
        //    this, the encoded child would stay at native i32/i64 width and
        //    ALP would actually inflate vs raw float storage.
        var childTickets = new List<int>(3);
        childTickets.Add(ArrayEncoderDispatch.Emit(sb, encodedArray, idx, statsTicket: null, compress: true));
        bool hasPatches = patchIndicesArray is not null;
        byte patchIndicesPtype = 3; // U64
        if (hasPatches)
        {
            childTickets.Add(ArrayEncoderDispatch.Emit(sb, patchIndicesArray!, idx));
            childTickets.Add(ArrayEncoderDispatch.Emit(sb, patchValuesArray!, idx));
        }

        // 3. Metadata.
        var metadataBytes = SerializeAlpMetadata(
            expE, expF, hasPatches, hasPatches ? patchIndicesArray!.Length : 0, patchIndicesPtype);
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var children = childTickets.ToArray();
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, idx.Alp, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, idx.Alp, metadataTicket, children, statsTicket.Value);
    }

    /// <summary>
    /// Searches all (e, f) with <c>0 ≤ f &lt; e &lt; MAX_EXPONENT</c>, picking
    /// the pair that minimizes estimated encoded size. Returns
    /// <c>(expE, expF, encodedBits, patchCount)</c> for the winner; expE = -1
    /// signals "no profitable encoding found".
    /// </summary>
    private static (int E, int F, int EncodedBits, int PatchCount)
        FindBestExponentsAndEstimate(IArrowArray array)
    {
        return array switch
        {
            FloatArray fa => FindBestF32(fa),
            DoubleArray da => FindBestF64(da),
            _ => (-1, -1, 0, 0),
        };
    }

    private static (int E, int F, int EncodedBits, int PatchCount) FindBestF32(FloatArray array)
    {
        var data = array.Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        var src = MemoryMarshal.Cast<byte, float>(data.Buffers[1].Span.Slice(off * 4, n * 4));

        int bestE = -1, bestF = 0;
        long bestSize = long.MaxValue;
        int bestEncodedBits = 0, bestPatchCount = 0;

        for (int e = MaxExponentF32 - 1; e >= 0; e--)
        {
            for (int f = 0; f < e; f++)
            {
                int patchCount = 0;
                long minVal = long.MaxValue, maxVal = long.MinValue;
                float fe = F10F32[e];
                float ife = If10F32[f];
                // Decode uses double-precision intermediate to match the
                // reader's `(float)(rawInts[i] * F10D[f] * IF10D[e])`. Using
                // float-precision decode here would cause some values to
                // pass the bit-equality check on the writer side but produce
                // a different bit pattern at read time — silent corruption.
                double fdec = F10F64[f];
                double idec = If10F64[e];

                for (int i = 0; i < n; i++)
                {
                    if (hasNulls && !IsValidAt(validity, off + i)) continue;
                    float v = src[i];
                    int encoded = EncodeF32(v, fe, ife);
                    float decoded = DecodeF32(encoded, fdec, idec);
                    if (SingleToInt32Bits(decoded) != SingleToInt32Bits(v))
                    {
                        patchCount++;
                        continue;
                    }
                    if (encoded < minVal) minVal = encoded;
                    if (encoded > maxVal) maxVal = encoded;
                }

                int encodedBits = EstimateBitsPerValue(minVal, maxVal, 32);
                long size = ((long)encodedBits * n + 7) / 8 + (long)patchCount * (4 + 8);
                if (size < bestSize)
                {
                    bestSize = size;
                    bestE = e;
                    bestF = f;
                    bestEncodedBits = encodedBits;
                    bestPatchCount = patchCount;
                }
            }
        }
        return (bestE, bestF, bestEncodedBits, bestPatchCount);
    }

    private static (int E, int F, int EncodedBits, int PatchCount) FindBestF64(DoubleArray array)
    {
        var data = array.Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        var src = MemoryMarshal.Cast<byte, double>(data.Buffers[1].Span.Slice(off * 8, n * 8));

        int bestE = -1, bestF = 0;
        long bestSize = long.MaxValue;
        int bestEncodedBits = 0, bestPatchCount = 0;

        for (int e = MaxExponentF64 - 1; e >= 0; e--)
        {
            for (int f = 0; f < e; f++)
            {
                int patchCount = 0;
                long minVal = long.MaxValue, maxVal = long.MinValue;
                double fe = F10F64[e];
                double ife = If10F64[f];
                double fdec = F10F64[f];
                double idec = If10F64[e];

                for (int i = 0; i < n; i++)
                {
                    if (hasNulls && !IsValidAt(validity, off + i)) continue;
                    double v = src[i];
                    long encoded = EncodeF64(v, fe, ife);
                    double decoded = DecodeF64(encoded, fdec, idec);
                    if (BitConverter.DoubleToInt64Bits(decoded) != BitConverter.DoubleToInt64Bits(v))
                    {
                        patchCount++;
                        continue;
                    }
                    if (encoded < minVal) minVal = encoded;
                    if (encoded > maxVal) maxVal = encoded;
                }

                int encodedBits = EstimateBitsPerValue(minVal, maxVal, 64);
                long size = ((long)encodedBits * n + 7) / 8 + (long)patchCount * (8 + 8);
                if (size < bestSize)
                {
                    bestSize = size;
                    bestE = e;
                    bestF = f;
                    bestEncodedBits = encodedBits;
                    bestPatchCount = patchCount;
                }
            }
        }
        return (bestE, bestF, bestEncodedBits, bestPatchCount);
    }

    /// <summary>
    /// Encodes the column at the chosen (e, f). Returns the encoded primitive
    /// array (with the input's validity bitmap if nullable) and optional
    /// patches arrays (indices: u64, values: parent type).
    /// </summary>
    private static (IArrowArray Encoded, IArrowArray? PatchIndices, IArrowArray? PatchValues)
        EncodeAtExponents(IArrowArray array, int expE, int expF)
    {
        return array switch
        {
            FloatArray fa => EncodeAtF32(fa, expE, expF),
            DoubleArray da => EncodeAtF64(da, expE, expF),
            _ => throw new NotSupportedException(),
        };
    }

    private static (IArrowArray Encoded, IArrowArray? PatchIndices, IArrowArray? PatchValues)
        EncodeAtF32(FloatArray array, int expE, int expF)
    {
        var data = array.Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        int nullCount = hasNulls ? data.GetNullCount() : 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        // Rebase validity into a fresh offset-0 bitmap so the encoded child
        // (which is densely packed at logical positions [0..n)) has a
        // matching null-bitmap layout.
        var validityBuf = hasNulls
            ? new ArrowBuffer(EncoderHelpers.ExtractValidityBitmap(validity, srcBitOffset: off, rowCount: n))
            : ArrowBuffer.Empty;
        var src = MemoryMarshal.Cast<byte, float>(data.Buffers[1].Span.Slice(off * 4, n * 4));

        var encodedBytes = new byte[n * 4];
        var encodedSpan = MemoryMarshal.Cast<byte, int>(encodedBytes.AsSpan());
        var patchIdxList = new List<ulong>();
        var patchValList = new List<float>();

        float fe = F10F32[expE], ife = If10F32[expF];
        // Decode uses double-precision intermediate to match the reader.
        // See comment in FindBestF32.
        double fdec = F10F64[expF], idec = If10F64[expE];
        for (int i = 0; i < n; i++)
        {
            if (hasNulls && !IsValidAt(validity, off + i))
            {
                encodedSpan[i] = 0;
                continue;
            }
            float v = src[i];
            int encoded = EncodeF32(v, fe, ife);
            float decoded = DecodeF32(encoded, fdec, idec);
            if (SingleToInt32Bits(decoded) == SingleToInt32Bits(v))
            {
                encodedSpan[i] = encoded;
            }
            else
            {
                // Patch: encoded slot can be anything (reader overwrites it).
                encodedSpan[i] = 0;
                patchIdxList.Add((ulong)i);
                patchValList.Add(v);
            }
        }

        var encodedArr = new Int32Array(new ArrowBuffer(encodedBytes), validityBuf, n, nullCount, 0);
        if (patchIdxList.Count == 0) return (encodedArr, null, null);
        return (encodedArr, BuildU64Indices(patchIdxList), BuildF32Values(patchValList));
    }

    private static (IArrowArray Encoded, IArrowArray? PatchIndices, IArrowArray? PatchValues)
        EncodeAtF64(DoubleArray array, int expE, int expF)
    {
        var data = array.Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        int nullCount = hasNulls ? data.GetNullCount() : 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        var validityBuf = hasNulls
            ? new ArrowBuffer(EncoderHelpers.ExtractValidityBitmap(validity, srcBitOffset: off, rowCount: n))
            : ArrowBuffer.Empty;
        var src = MemoryMarshal.Cast<byte, double>(data.Buffers[1].Span.Slice(off * 8, n * 8));

        var encodedBytes = new byte[n * 8];
        var encodedSpan = MemoryMarshal.Cast<byte, long>(encodedBytes.AsSpan());
        var patchIdxList = new List<ulong>();
        var patchValList = new List<double>();

        double fe = F10F64[expE], ife = If10F64[expF];
        double fdec = F10F64[expF], idec = If10F64[expE];
        for (int i = 0; i < n; i++)
        {
            if (hasNulls && !IsValidAt(validity, off + i))
            {
                encodedSpan[i] = 0;
                continue;
            }
            double v = src[i];
            long encoded = EncodeF64(v, fe, ife);
            double decoded = DecodeF64(encoded, fdec, idec);
            if (BitConverter.DoubleToInt64Bits(decoded) == BitConverter.DoubleToInt64Bits(v))
            {
                encodedSpan[i] = encoded;
            }
            else
            {
                encodedSpan[i] = 0;
                patchIdxList.Add((ulong)i);
                patchValList.Add(v);
            }
        }

        var encodedArr = new Int64Array(new ArrowBuffer(encodedBytes), validityBuf, n, nullCount, 0);
        if (patchIdxList.Count == 0) return (encodedArr, null, null);
        return (encodedArr, BuildU64Indices(patchIdxList), BuildF64Values(patchValList));
    }

    private static int EncodeF32(float value, float fe, float ife)
    {
        // round(value * 10^e * 10^-f). fast_round = (x + SWEET) - SWEET.
        float scaled = value * fe * ife;
        float rounded = (scaled + SweetF32) - SweetF32;
        return unchecked((int)rounded);
    }

    private static float DecodeF32(int encoded, double fdec, double idec) =>
        (float)(encoded * fdec * idec);

    private static long EncodeF64(double value, double fe, double ife)
    {
        double scaled = value * fe * ife;
        double rounded = (scaled + SweetF64) - SweetF64;
        return unchecked((long)rounded);
    }

    private static double DecodeF64(long encoded, double fdec, double idec) =>
        encoded * fdec * idec;

    /// <summary>
    /// Estimated bits-per-value via FoR-style "subtract min, count bits". Used
    /// in find_best to compare candidate (e, f) pairs.
    /// </summary>
    private static int EstimateBitsPerValue(long minVal, long maxVal, int nativeBits)
    {
        if (minVal == long.MaxValue) return 0; // all-null
        ulong range = unchecked((ulong)(maxVal - minVal));
        if (range == 0) return 0;
        return 64 - System.Numerics.BitOperations.LeadingZeroCount(range);
    }

    private static bool IsValidAt(ReadOnlySpan<byte> bitmap, int i) =>
        (bitmap[i >> 3] & (1 << (i & 7))) != 0;

    /// <summary>
    /// <c>BitConverter.SingleToInt32Bits</c> is .NET 6+; on netstandard2.0 we
    /// reinterpret via the 4-byte buffer round-trip.
    /// </summary>
    private static int SingleToInt32Bits(float f)
    {
#if NET6_0_OR_GREATER
        return BitConverter.SingleToInt32Bits(f);
#else
        return BitConverter.ToInt32(BitConverter.GetBytes(f), 0);
#endif
    }

    private static UInt64Array BuildU64Indices(List<ulong> indices)
    {
        var bytes = new byte[indices.Count * 8];
        for (int i = 0; i < indices.Count; i++)
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(i * 8, 8), indices[i]);
        return new UInt64Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, indices.Count, 0, 0);
    }

    private static FloatArray BuildF32Values(List<float> values)
    {
        var bytes = new byte[values.Count * 4];
        for (int i = 0; i < values.Count; i++)
            BinaryPrimitives.WriteInt32LittleEndian(
                bytes.AsSpan(i * 4, 4), SingleToInt32Bits(values[i]));
        return new FloatArray(new ArrowBuffer(bytes), ArrowBuffer.Empty, values.Count, 0, 0);
    }

    private static DoubleArray BuildF64Values(List<double> values)
    {
        var bytes = new byte[values.Count * 8];
        for (int i = 0; i < values.Count; i++)
            BinaryPrimitives.WriteInt64LittleEndian(
                bytes.AsSpan(i * 8, 8), BitConverter.DoubleToInt64Bits(values[i]));
        return new DoubleArray(new ArrowBuffer(bytes), ArrowBuffer.Empty, values.Count, 0, 0);
    }

    /// <summary>
    /// Inline ALPMetadata proto bytes:
    ///   field 1 (varint): exp_e (u32)
    ///   field 2 (varint): exp_f (u32)
    ///   field 3 (length-delim, optional): patches submessage with
    ///     <c>{len: u64 (tag 1), offset: u64 (tag 2, omitted=0), indices_ptype: PType (tag 3)}</c>.
    /// </summary>
    private static byte[] SerializeAlpMetadata(
        int expE, int expF, bool hasPatches, int patchesLen, byte indicesPtype)
    {
        Span<byte> tmp = stackalloc byte[32];
        int pos = 0;
        tmp[pos++] = 0x08; // tag 1
        pos += Varint.WriteUnsigned(tmp.Slice(pos), (ulong)expE);
        tmp[pos++] = 0x10; // tag 2
        pos += Varint.WriteUnsigned(tmp.Slice(pos), (ulong)expF);
        if (hasPatches)
        {
            // Patches submessage: field 1 (len) + field 3 (indices_ptype).
            Span<byte> sub = stackalloc byte[16];
            int subPos = 0;
            sub[subPos++] = 0x08; // tag 1
            subPos += Varint.WriteUnsigned(sub.Slice(subPos), (ulong)patchesLen);
            sub[subPos++] = 0x18; // tag 3
            sub[subPos++] = indicesPtype;
            tmp[pos++] = 0x1A; // tag 3, wire-type 2
            pos += Varint.WriteUnsigned(tmp.Slice(pos), (ulong)subPos);
            sub.Slice(0, subPos).CopyTo(tmp.Slice(pos));
            pos += subPos;
        }
        return tmp.Slice(0, pos).ToArray();
    }
}
