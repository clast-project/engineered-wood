// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.AlpRdArrayDecoder"/>:
/// emits a <c>vortex.alprd</c> ArrayNode subtree for floating-point columns
/// where the front bits cluster into a small dictionary of patterns. ALP-RD
/// ("real doubles") splits each value's bit pattern at a chosen cut point
/// <c>right_bw</c> into:
/// <list type="bullet">
///   <item><c>right_parts[i]</c> — the lower <c>right_bw</c> bits, stored as
///     the float's UINT companion type (u64 for f64, u32 for f32) and
///     bitpacked.</item>
///   <item><c>left_parts[i]</c> — the upper bits cast to u16, then
///     dictionary-encoded into a small code (≤ 8 entries → 1–3 bits per
///     value). Front bits that don't fit the dictionary become patches
///     (out-of-band index + raw 16-bit value).</item>
/// </list>
///
/// <para>Wire shape: 0 buffers, 2 children (left_parts, right_parts) when
/// patch-free; 4 children (left_parts, right_parts, patch_indices,
/// patch_values) with patches. Metadata
/// <c>ALPRDMetadata { right_bit_width, dict_len, dict[u32 packed], left_parts_ptype, patches? }</c>.</para>
///
/// <para>Scope: f32 + f64; nullable + non-nullable; sliced + non-sliced.
/// right_parts is u32 for FloatArray, u64 for DoubleArray; the dictionary
/// stores u16 left patterns either way (CUT_LIMIT caps the left part
/// width at 16 bits regardless of float type). For nullable inputs the
/// validity bitmap rides on the <c>left_parts</c> inner child; null rows
/// write zero into both left/right (any bits work since validity masks
/// them) and contribute no patches.</para>
///
/// <para>See "ALP: Adaptive Lossless floating-Point Compression" (Afroozeh &amp;
/// Boncz, VLDB 2024), Section 3.4 (RD variant), and the upstream Rust
/// implementation in <c>vortex-alp/src/alp_rd/</c> for the algorithm.</para>
/// </summary>
internal static class AlpRdArrayEncoder
{
    private const int MaxDictSize = 8;
    private const int CutLimit = 16;

    /// <summary>
    /// Compacts the non-null values from a sliced f32 column into a
    /// fresh array of length <paramref name="validCount"/>. Used by the
    /// dict-probe step so null-row garbage bits don't pollute the
    /// histogram of left bit patterns.
    /// </summary>
    private static float[] BuildDenseFloats(
        ReadOnlySpan<float> src, ReadOnlySpan<byte> validityBitmap, int srcOffset, int n, int validCount)
    {
        var dense = new float[validCount];
        int j = 0;
        for (int i = 0; i < n; i++)
        {
            int abs = srcOffset + i;
            if ((validityBitmap[abs >> 3] & (1 << (abs & 7))) != 0)
                dense[j++] = src[i];
        }
        return dense;
    }

    /// <summary>f64 sibling of <see cref="BuildDenseFloats"/>.</summary>
    private static double[] BuildDenseDoubles(
        ReadOnlySpan<double> src, ReadOnlySpan<byte> validityBitmap, int srcOffset, int n, int validCount)
    {
        var dense = new double[validCount];
        int j = 0;
        for (int i = 0; i < n; i++)
        {
            int abs = srcOffset + i;
            if ((validityBitmap[abs >> 3] & (1 << (abs & 7))) != 0)
                dense[j++] = src[i];
        }
        return dense;
    }

    public static bool IsApplicable(IArrowArray array)
    {
        if (array is not (FloatArray or DoubleArray)) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int off = data.Offset;
        // Need enough rows to amortize FastLanes' 1024-element chunk padding
        // (which forces left/right payloads up to a multiple of 128 × W bytes).
        if (n < 1024) return false;
        // All-null columns produce no useful dict; degrade to the next
        // encoder rather than emit a degenerate ALP-RD.
        if ((int)data.GetNullCount() >= n) return false;

        // Train and probe profitability against the ACTUAL padded byte cost.
        // ALP-RD's intrinsic ratio is ~85% of raw for ideal data
        // (e.g. 49 bits / 64 for f64), so a bits-per-value × 1.5 gate (used
        // elsewhere) can't ever fire. Instead, model the bitpacked children's
        // padded byte size and accept whenever ALP-RD beats 90% of raw.
        // For nullable inputs we feed a dense-valid-values buffer to the
        // dict probe so null-row garbage bits don't skew the histogram —
        // the chunk-padded cost still uses the full row count n.
        bool isF32 = array is FloatArray;
        int floatBytes = isF32 ? 4 : 8;
        bool hasNulls = data.GetNullCount() > 0;
        int validCount = n - (int)data.GetNullCount();
        int rightBw;
        ushort[] dict;
        int exceptionCount;
        if (isF32)
        {
            var src = MemoryMarshal.Cast<byte, float>(data.Buffers[1].Span.Slice(off * 4, n * 4));
            (rightBw, dict, exceptionCount) = hasNulls
                ? FindBestDictionaryF32(BuildDenseFloats(src, data.Buffers[0].Span, off, n, validCount))
                : FindBestDictionaryF32(src);
        }
        else
        {
            var src = MemoryMarshal.Cast<byte, double>(data.Buffers[1].Span.Slice(off * 8, n * 8));
            (rightBw, dict, exceptionCount) = hasNulls
                ? FindBestDictionary(BuildDenseDoubles(src, data.Buffers[0].Span, off, n, validCount))
                : FindBestDictionary(src);
        }
        if (rightBw == 0) return false;

        int leftBw = BitWidth(dict.Length == 0 ? 0 : dict.Length - 1);
        int numChunks = (n + 1023) / 1024;
        // FastLanes packs 1024 rows per chunk into `128 × W` bytes. Both
        // left_parts (left_bw) and right_parts (right_bw) bitpack this way.
        long leftPayload = (long)numChunks * 128 * leftBw;
        long rightPayload = (long)numChunks * 128 * rightBw;
        // Patches: bounded index + u16 value ≈ 8 bytes per exception.
        long patchPayload = (long)exceptionCount * 8;
        long compressedBytes = leftPayload + rightPayload + patchPayload;
        long rawBytes = (long)n * floatBytes;
        return compressedBytes < rawBytes * 9 / 10;
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is not (FloatArray or DoubleArray))
            throw new NotSupportedException(
                $"vortex.alprd writer requires FloatArray or DoubleArray, got {array.GetType().Name}.");
        var data = ((Apache.Arrow.Array)array).Data;

        bool isF32 = array is FloatArray;
        int n = array.Length;
        int off = data.Offset;
        int nullCount = (int)data.GetNullCount();
        bool hasNulls = nullCount > 0;
        var validityBitmap = hasNulls ? data.Buffers[0].Span : default;
        int validCount = n - nullCount;

        int rightBw;
        ushort[] dict;
        if (isF32)
        {
            var src = MemoryMarshal.Cast<byte, float>(data.Buffers[1].Span.Slice(off * 4, n * 4));
            (rightBw, dict, _) = hasNulls
                ? FindBestDictionaryF32(BuildDenseFloats(src, validityBitmap, off, n, validCount))
                : FindBestDictionaryF32(src);
        }
        else
        {
            var src = MemoryMarshal.Cast<byte, double>(data.Buffers[1].Span.Slice(off * 8, n * 8));
            (rightBw, dict, _) = hasNulls
                ? FindBestDictionary(BuildDenseDoubles(src, validityBitmap, off, n, validCount))
                : FindBestDictionary(src);
        }
        if (rightBw == 0)
            throw new InvalidOperationException(
                "vortex.alprd: no profitable dictionary found (caller should gate via IsApplicable).");

        // Encode each row: split bits, dict-encode the left half, gather patches.
        // left_parts MUST be u16 (matching upstream): the dict-encoded codes
        // fit in 3 bits but PATCH values carry the raw up-to-16-bit left
        // pattern, and patch_values share the left_parts ptype on the wire.
        // A u8 left_parts would silently truncate patches to their low byte.
        int leftBw = BitWidth(dict.Length == 0 ? 0 : dict.Length - 1);
        var dictLookup = BuildReverseDictionary(dict);

        var leftCodesBytes = new byte[(long)n * 2]; // U16
        var leftCodesSpan = MemoryMarshal.Cast<byte, ushort>(leftCodesBytes.AsSpan());
        // right_parts width follows the float type: u32 for f32, u64 for f64.
        int rightElemSize = isF32 ? 4 : 8;
        var rightPartsBytes = new byte[(long)n * rightElemSize];
        var patchIdxList = new List<long>();
        var patchValList = new List<ushort>();
        if (isF32)
        {
            uint rightMask = (uint)((1UL << rightBw) - 1);
            var src = MemoryMarshal.Cast<byte, float>(data.Buffers[1].Span.Slice(off * 4, n * 4));
            var rightPartsSpan = MemoryMarshal.Cast<byte, uint>(rightPartsBytes.AsSpan());
            for (int i = 0; i < n; i++)
            {
                if (hasNulls && (validityBitmap[(off + i) >> 3] & (1 << ((off + i) & 7))) == 0)
                {
                    // Null row: write zero into both children (the validity
                    // bitmap on left_parts masks them); skip patch generation.
                    rightPartsSpan[i] = 0;
                    leftCodesSpan[i] = 0;
                    continue;
                }
                uint bits = unchecked((uint)SingleToInt32Bits(src[i]));
                uint right = bits & rightMask;
                ushort left = (ushort)(bits >> rightBw);
                rightPartsSpan[i] = right;
                if (dictLookup.TryGetValue(left, out byte code)) leftCodesSpan[i] = code;
                else { leftCodesSpan[i] = 0; patchIdxList.Add(i); patchValList.Add(left); }
            }
        }
        else
        {
            ulong rightMask = (1UL << rightBw) - 1;
            var src = MemoryMarshal.Cast<byte, double>(data.Buffers[1].Span.Slice(off * 8, n * 8));
            var rightPartsSpan = MemoryMarshal.Cast<byte, ulong>(rightPartsBytes.AsSpan());
            for (int i = 0; i < n; i++)
            {
                if (hasNulls && (validityBitmap[(off + i) >> 3] & (1 << ((off + i) & 7))) == 0)
                {
                    rightPartsSpan[i] = 0;
                    leftCodesSpan[i] = 0;
                    continue;
                }
                ulong bits = unchecked((ulong)BitConverter.DoubleToInt64Bits(src[i]));
                ulong right = bits & rightMask;
                ushort left = (ushort)(bits >> rightBw);
                rightPartsSpan[i] = right;
                if (dictLookup.TryGetValue(left, out byte code)) leftCodesSpan[i] = code;
                else { leftCodesSpan[i] = 0; patchIdxList.Add(i); patchValList.Add(left); }
            }
        }

        // Children: left_parts and right_parts go through the dispatcher with
        // compress=true so they pick up bitpacked at left_bw / right_bw. The
        // column's validity bitmap rides on left_parts (rebased to offset 0
        // to match the densely-packed encoded values); the reader extracts
        // it from there. right_parts stays non-nullable — both children
        // share validity semantically but only one needs to carry the
        // bitmap on the wire.
        ArrowBuffer leftValidityBuf = ArrowBuffer.Empty;
        if (hasNulls)
        {
            var bitmap = EncoderHelpers.ExtractValidityBitmap(
                validityBitmap, srcBitOffset: off, rowCount: n);
            leftValidityBuf = new ArrowBuffer(bitmap);
        }
        var leftPartsArr = new UInt16Array(
            new ArrowBuffer(leftCodesBytes), leftValidityBuf, n, nullCount, 0);
        IArrowArray rightPartsArr = isF32
            ? new UInt32Array(new ArrowBuffer(rightPartsBytes), ArrowBuffer.Empty, n, 0, 0)
            : new UInt64Array(new ArrowBuffer(rightPartsBytes), ArrowBuffer.Empty, n, 0, 0);

        int leftTicket = ArrayEncoderDispatch.Emit(
            sb, leftPartsArr, idx, statsTicket: null, compress: true);
        int rightTicket = ArrayEncoderDispatch.Emit(
            sb, rightPartsArr, idx, statsTicket: null, compress: true);

        var childTickets = new List<int>(4) { leftTicket, rightTicket };

        bool hasPatches = patchIdxList.Count > 0;
        byte patchIndicesPtype = 0; // U8 default
        if (hasPatches)
        {
            // Patch indices: smallest unsigned int that holds the largest index.
            int maxIdx = (int)patchIdxList[patchIdxList.Count - 1]; // patch_idx_list is appended in row order
            patchIndicesPtype = SmallestUIntPtypeFor(maxIdx);
            int idxElemSize = ElemSizeForPtype(patchIndicesPtype);
            var idxBytes = new byte[(long)patchIdxList.Count * idxElemSize];
            for (int k = 0; k < patchIdxList.Count; k++)
                WriteUnsigned(idxBytes.AsSpan(k * idxElemSize, idxElemSize),
                    (ulong)patchIdxList[k], idxElemSize);
            var idxArr = BuildUnsignedArray(idxBytes, patchIdxList.Count, idxElemSize);

            // Patch values: u16 (the raw left bit pattern, matching left_parts ptype).
            var valBytes = new byte[(long)patchValList.Count * 2];
            for (int k = 0; k < patchValList.Count; k++)
                BinaryPrimitives.WriteUInt16LittleEndian(
                    valBytes.AsSpan(k * 2, 2), patchValList[k]);
            var valArr = new UInt16Array(
                new ArrowBuffer(valBytes), ArrowBuffer.Empty, patchValList.Count, 0, 0);

            childTickets.Add(ArrayEncoderDispatch.Emit(
                sb, idxArr, idx, statsTicket: null, compress: true));
            childTickets.Add(ArrayEncoderDispatch.Emit(
                sb, valArr, idx, statsTicket: null, compress: false));
        }

        var metadataBytes = SerializeMetadata(
            rightBitWidth: rightBw,
            dict: dict,
            leftPartsPtype: 1, // U16 — matches the left_parts and patch_values arrays
            hasPatches: hasPatches,
            patchesLen: patchIdxList.Count,
            patchIndicesPtype: patchIndicesPtype);
        int metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var children = childTickets.ToArray();
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, idx.AlpRd, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, idx.AlpRd, metadataTicket, children, statsTicket.Value);
    }

    /// <summary>
    /// Searches all <c>p ∈ [1, 16]</c> (where <c>right_bw = floatBits - p</c>),
    /// builds a dictionary of the most-frequent left bit patterns at each
    /// cut, and picks the cut minimizing
    /// <c>right_bw + left_bw + exception_overhead/n</c>. Returns the winning
    /// <c>(rightBw, dict, exceptionCount)</c>; <c>dict[code]</c> is the
    /// original 16-bit pattern that <c>code</c> dictionary-decodes to.
    /// </summary>
    private static (int RightBw, ushort[] Dict, int ExceptionCount)
        FindBestDictionary(ReadOnlySpan<double> values)
    {
        double bestSize = double.MaxValue;
        int bestRightBw = 0;
        ushort[] bestDict = System.Array.Empty<ushort>();
        int bestExceptions = 0;

        for (int p = 1; p <= CutLimit; p++)
        {
            int rightBw = 64 - p;
            var counts = new Dictionary<ushort, int>();
            for (int i = 0; i < values.Length; i++)
            {
                ulong bits = unchecked((ulong)BitConverter.DoubleToInt64Bits(values[i]));
                ushort left = (ushort)(bits >> rightBw);
                counts.TryGetValue(left, out int c);
                counts[left] = c + 1;
            }
            EvaluateCut(counts, rightBw, values.Length,
                ref bestSize, ref bestRightBw, ref bestDict, ref bestExceptions);
        }
        return (bestRightBw, bestDict, bestExceptions);
    }

    /// <summary>
    /// f32 variant of <see cref="FindBestDictionary(ReadOnlySpan{double})"/>:
    /// shift width is 32 − p and the bit pattern comes from
    /// <see cref="SingleToInt32Bits"/>.
    /// </summary>
    private static (int RightBw, ushort[] Dict, int ExceptionCount)
        FindBestDictionaryF32(ReadOnlySpan<float> values)
    {
        double bestSize = double.MaxValue;
        int bestRightBw = 0;
        ushort[] bestDict = System.Array.Empty<ushort>();
        int bestExceptions = 0;

        for (int p = 1; p <= CutLimit; p++)
        {
            int rightBw = 32 - p;
            var counts = new Dictionary<ushort, int>();
            for (int i = 0; i < values.Length; i++)
            {
                uint bits = unchecked((uint)SingleToInt32Bits(values[i]));
                ushort left = (ushort)(bits >> rightBw);
                counts.TryGetValue(left, out int c);
                counts[left] = c + 1;
            }
            EvaluateCut(counts, rightBw, values.Length,
                ref bestSize, ref bestRightBw, ref bestDict, ref bestExceptions);
        }
        return (bestRightBw, bestDict, bestExceptions);
    }

    /// <summary>
    /// Common cost-evaluation step shared by the f32 and f64 cut searches.
    /// Sorts the histogram by frequency, picks the top
    /// <see cref="MaxDictSize"/> as the dictionary, counts exceptions, and
    /// updates the running best if this cut beats it.
    /// </summary>
    private static void EvaluateCut(
        Dictionary<ushort, int> counts, int rightBw, int sampleN,
        ref double bestSize, ref int bestRightBw, ref ushort[] bestDict, ref int bestExceptions)
    {
        var sorted = counts.OrderByDescending(kv => kv.Value).ToList();
        int dictLen = Math.Min(sorted.Count, MaxDictSize);
        var dict = new ushort[dictLen];
        for (int k = 0; k < dictLen; k++) dict[k] = sorted[k].Key;

        int exceptionCount = 0;
        for (int k = dictLen; k < sorted.Count; k++) exceptionCount += sorted[k].Value;

        int leftBw = BitWidth(dictLen == 0 ? 0 : dictLen - 1);
        // Same cost model as upstream: right_bw + left_bw +
        // exceptions * (POSITION_BITS + VALUE_BITS) / n.
        double bitsPerVal = leftBw + rightBw + ((double)exceptionCount * 32 / sampleN);
        if (bitsPerVal < bestSize)
        {
            bestSize = bitsPerVal;
            bestRightBw = rightBw;
            bestDict = dict;
            bestExceptions = exceptionCount;
        }
    }

    private static Dictionary<ushort, byte> BuildReverseDictionary(ushort[] dict)
    {
        var lookup = new Dictionary<ushort, byte>(dict.Length);
        for (int i = 0; i < dict.Length; i++) lookup[dict[i]] = (byte)i;
        return lookup;
    }

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

    /// <summary>Number of bits to represent <paramref name="value"/> (0 → 1).</summary>
    private static int BitWidth(int value)
    {
        if (value <= 0) return 1;
        return 32 - System.Numerics.BitOperations.LeadingZeroCount((uint)value);
    }

    private static byte SmallestUIntPtypeFor(int max)
    {
        if (max <= byte.MaxValue) return 0;
        if (max <= ushort.MaxValue) return 1;
        if ((uint)max <= uint.MaxValue) return 2;
        return 3;
    }

    private static int ElemSizeForPtype(byte ptype) => ptype switch
    {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => throw new NotSupportedException(),
    };

    private static void WriteUnsigned(Span<byte> dest, ulong value, int elemSize)
    {
        switch (elemSize)
        {
            case 1: dest[0] = (byte)value; break;
            case 2: BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)value); break;
            case 4: BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)value); break;
            case 8: BinaryPrimitives.WriteUInt64LittleEndian(dest, value); break;
            default: throw new NotSupportedException();
        }
    }

    private static IArrowArray BuildUnsignedArray(byte[] bytes, int len, int elemSize)
    {
        var buf = new ArrowBuffer(bytes);
        return elemSize switch
        {
            1 => new UInt8Array(buf, ArrowBuffer.Empty, len, 0, 0),
            2 => new UInt16Array(buf, ArrowBuffer.Empty, len, 0, 0),
            4 => new UInt32Array(buf, ArrowBuffer.Empty, len, 0, 0),
            8 => new UInt64Array(buf, ArrowBuffer.Empty, len, 0, 0),
            _ => throw new NotSupportedException(),
        };
    }

    /// <summary>
    /// Inline ALPRDMetadata proto bytes:
    ///   field 1 (varint): right_bit_width (u32)
    ///   field 2 (varint): dict_len (u32)
    ///   field 3 (length-delim, packed varints u32): dict[]
    ///   field 4 (varint): left_parts_ptype (PType enum)
    ///   field 5 (length-delim, optional): patches (PatchesMetadata)
    /// </summary>
    private static byte[] SerializeMetadata(
        int rightBitWidth, ushort[] dict, byte leftPartsPtype,
        bool hasPatches, int patchesLen, byte patchIndicesPtype)
    {
        // Outer scratch buffer is bounded: tags+varints for fields 1,2,4 (≤ 12 bytes)
        // + field 3 (length-delim with up to 8 varint values, each ≤ 3 bytes — 26 bytes)
        // + field 5 (length-delim ≤ 16 bytes when patches set). 64 is comfortable.
        var tmp = new byte[64];
        int pos = 0;

        tmp[pos++] = 0x08; // tag 1, varint
        pos += Varint.WriteUnsigned(tmp.AsSpan(pos), (ulong)rightBitWidth);

        tmp[pos++] = 0x10; // tag 2, varint
        pos += Varint.WriteUnsigned(tmp.AsSpan(pos), (ulong)dict.Length);

        // Field 3: packed varint vector of u32 values.
        Span<byte> dictPayload = stackalloc byte[dict.Length * 5]; // worst case 5 bytes per varint
        int dictPos = 0;
        for (int i = 0; i < dict.Length; i++)
            dictPos += Varint.WriteUnsigned(dictPayload.Slice(dictPos), dict[i]);
        tmp[pos++] = 0x1A; // tag 3, length-delim
        pos += Varint.WriteUnsigned(tmp.AsSpan(pos), (ulong)dictPos);
        dictPayload.Slice(0, dictPos).CopyTo(tmp.AsSpan(pos));
        pos += dictPos;

        tmp[pos++] = 0x20; // tag 4, varint
        pos += Varint.WriteUnsigned(tmp.AsSpan(pos), leftPartsPtype);

        if (hasPatches)
        {
            // Inner PatchesMetadata: field 1 = len, field 3 = indices_ptype.
            // (Field 2 = offset is omitted since we don't slice; proto3 default = 0.)
            Span<byte> inner = stackalloc byte[16];
            int innerPos = 0;
            inner[innerPos++] = 0x08; // field 1, varint
            innerPos += Varint.WriteUnsigned(inner.Slice(innerPos), (ulong)patchesLen);
            inner[innerPos++] = 0x18; // field 3, varint
            inner[innerPos++] = patchIndicesPtype;

            tmp[pos++] = 0x2A; // tag 5, length-delim
            pos += Varint.WriteUnsigned(tmp.AsSpan(pos), (ulong)innerPos);
            inner.Slice(0, innerPos).CopyTo(tmp.AsSpan(pos));
            pos += innerPos;
        }

        var result = new byte[pos];
        System.Array.Copy(tmp, result, pos);
        return result;
    }
}
