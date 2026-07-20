// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.BitPackedArrayDecoder"/>:
/// emits a <c>fastlanes.bitpacked</c> ArrayNode subtree using
/// <c>Clast.FastLanes.BitPacking</c>.
///
/// <para>Scope:
/// <list type="bullet">
///   <item>Unsigned integer columns (UInt8..UInt64), nullable + non-nullable.</item>
///   <item>Signed integer columns (Int8..Int64) when ALL non-null values are
///     non-negative — bit pattern is identical to unsigned for non-negative
///     values, so the bytes flow through the same packing path. (Vortex's own
///     compressor does the same: cast to unsigned + bitpack.)</item>
///   <item>No patches. <c>bit_width</c> is chosen as <c>MaxBits</c> across the
///     non-null values; null positions are zero-filled before packing so they
///     never inflate the bit width.</item>
///   <item>No slicing offset (offset = 0).</item>
/// </list></para>
///
/// <para>Wire shape:
/// <list type="bullet">
///   <item>No patches, no nulls: 1 buffer (packed), 0 children, metadata
///     <c>{bit_width}</c>. (slots 0+1+3)</item>
///   <item>No patches, nullable: 1 packed buffer, 1 child (vortex.bool
///     validity), metadata <c>{bit_width}</c>. (slots 0+1+2+3)</item>
///   <item>With patches: 1 packed buffer, children = [patch_indices,
///     patch_values, chunk_offsets, optional validity], metadata
///     <c>{bit_width, patches}</c>. (slots 0+1+2+3)</item>
/// </list></para>
///
/// <para>Bit-width selection follows vortex's <c>find_best_bit_width</c>:
/// build a histogram of per-value bit widths, then for each candidate W,
/// total cost = ceil(W * n / 8) + (rows_above_W) * (sizeof(T) + 4). Pick W
/// minimizing total cost. When the chosen W is below the column's actual
/// MaxBits, we emit patches for the rows that don't fit.</para>
/// </summary>
internal static class BitPackedArrayEncoder
{
    private const int ElementsPerChunk = 1024;

    /// <summary>
    /// Returns true iff <paramref name="array"/> is a supported shape AND
    /// the chosen bit width is strictly less than native — i.e., the encoding
    /// actually saves space. Sliced inputs are supported; signed columns with
    /// any non-null negative value are rejected (FoR handles those).
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        int? nativeBits = NativeBits(array);
        if (nativeBits is not int native) return false;
        if (IsSigned(array) && HasNegative(array)) return false;

        var freq = BuildBitWidthHistogram(array);
        int bestW = FindBestBitWidth(freq, native, BytesPerException(array));
        return bestW < native;
    }

    public static int Emit(SegmentBuilder sb, IArrowArray array, ushort bitpackedEncodingIdx,
        ushort primitiveEncodingIdx, ushort boolEncodingIdx, int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        var data = ((Apache.Arrow.Array)array).Data;

        int? nativeBits = NativeBits(array);
        if (nativeBits is not int native)
            throw new NotSupportedException(
                $"fastlanes.bitpacked writer doesn't support Arrow array {array.GetType().Name}.");

        if (IsSigned(array) && HasNegative(array))
            throw new InvalidOperationException(
                $"fastlanes.bitpacked requires non-negative values; {array.GetType().Name} contains negatives.");

        int rowCount = array.Length;
        var freq = BuildBitWidthHistogram(array);
        int bitWidth = FindBestBitWidth(freq, native, BytesPerException(array));
        if (bitWidth >= native)
            throw new InvalidOperationException(
                $"BitPacked best bit_width {bitWidth} >= native {native} for {array.GetType().Name}.");

        // Pack ALL values at chosen bit_width — outliers will be truncated and
        // overwritten by the reader from patches.
        var packedBytes = PackToBytes(array, bitWidth, rowCount);
        ushort packedBufIdx = sb.AddBuffer(packedBytes, alignmentExponent: 0);

        // Determine if patches are needed: any non-null value with bits > bitWidth.
        // freq[w] counts non-null values with exactly w bits.
        int patchCount = 0;
        for (int w = bitWidth + 1; w < freq.Length; w++) patchCount += freq[w];

        // Gather child tickets — patches first, then validity if present.
        var childTickets = new List<int>(4);
        byte patchIndicesPtype = 0;
        if (patchCount > 0)
        {
            var (indicesArray, valuesArray, chunkOffsetsArray, indicesPtype) =
                GatherPatches(array, bitWidth, patchCount);
            patchIndicesPtype = indicesPtype;
            childTickets.Add(PrimitiveArrayEncoder.Emit(sb, indicesArray, primitiveEncodingIdx, boolEncodingIdx));
            childTickets.Add(PrimitiveArrayEncoder.Emit(sb, valuesArray, primitiveEncodingIdx, boolEncodingIdx));
            childTickets.Add(PrimitiveArrayEncoder.Emit(sb, chunkOffsetsArray, primitiveEncodingIdx, boolEncodingIdx));
        }

        // Optional trailing validity child.
        if (data.GetNullCount() > 0)
        {
            var bitmap = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: rowCount);
            ushort bitmapBufIdx = sb.AddBuffer(bitmap, alignmentExponent: 0);
            int validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, boolEncodingIdx, bitmapBufIdx);
            childTickets.Add(validityNodeTicket);
        }

        var metadataBytes = SerializeBitPackedMetadata(
            bitWidth, patchCount, patchIndicesPtype, rowCount);
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        if (childTickets.Count == 0)
        {
            return statsTicket is null
                ? ArrayNodeEmitter.EmitWithMetadataAndBuffer(
                    sb.Builder, bitpackedEncodingIdx, packedBufIdx, metadataTicket)
                : ArrayNodeEmitter.EmitWithMetadataBufferAndStats(
                    sb.Builder, bitpackedEncodingIdx, packedBufIdx, metadataTicket, statsTicket.Value);
        }

        var children = childTickets.ToArray();
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataBufferAndChildren(
                sb.Builder, bitpackedEncodingIdx, packedBufIdx, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataBufferChildrenAndStats(
                sb.Builder, bitpackedEncodingIdx, packedBufIdx, metadataTicket,
                children, statsTicket.Value);
    }

    /// <summary>Convenience: encode one column's segment in isolation.</summary>
    public static byte[] Encode(IArrowArray array, ushort bitpackedEncodingIdx,
        ushort primitiveEncodingIdx, ushort boolEncodingIdx)
    {
        var sb = new SegmentBuilder();
        var rootTicket = Emit(sb, array, bitpackedEncodingIdx, primitiveEncodingIdx, boolEncodingIdx);
        return sb.FinishSegment(rootTicket);
    }

    /// <summary>Bytes per exception (parent value width + u32 index width).</summary>
    private static int BytesPerException(IArrowArray array) => array switch
    {
        UInt8Array or Int8Array => 1 + 4,
        UInt16Array or Int16Array => 2 + 4,
        UInt32Array or Int32Array or FloatArray => 4 + 4,
        UInt64Array or Int64Array or DoubleArray => 8 + 4,
        _ => 4 + 4,
    };

    /// <summary>
    /// Builds a histogram <c>freq[w]</c> = count of non-null values whose
    /// minimum representation requires exactly <c>w</c> bits. Length = native+1.
    /// For sliced inputs, walks <c>data.Offset + i</c>.
    /// </summary>
    private static int[] BuildBitWidthHistogram(IArrowArray array)
    {
        int native = NativeBits(array)!.Value;
        var freq = new int[native + 1];
        if (array.Length == 0) return freq;

        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;

        switch (array)
        {
            case UInt8Array or Int8Array:
                {
                    var src = data.Buffers[1].Span.Slice(off, n);
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) continue;
                        uint v = src[i];
                        int bits = v == 0 ? 0 : 32 - System.Numerics.BitOperations.LeadingZeroCount(v);
                        freq[bits]++;
                    }
                    break;
                }
            case UInt16Array or Int16Array:
                {
                    var src = MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) continue;
                        uint v = src[i];
                        int bits = v == 0 ? 0 : 32 - System.Numerics.BitOperations.LeadingZeroCount(v);
                        freq[bits]++;
                    }
                    break;
                }
            case UInt32Array or Int32Array:
                {
                    var src = MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) continue;
                        uint v = src[i];
                        int bits = v == 0 ? 0 : 32 - System.Numerics.BitOperations.LeadingZeroCount(v);
                        freq[bits]++;
                    }
                    break;
                }
            case UInt64Array or Int64Array:
                {
                    var src = MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) continue;
                        ulong v = src[i];
                        int bits = v == 0 ? 0 : 64 - System.Numerics.BitOperations.LeadingZeroCount(v);
                        freq[bits]++;
                    }
                    break;
                }
            default: throw new NotSupportedException();
        }
        return freq;
    }

    /// <summary>
    /// Vortex's find_best_bit_width: picks W in [0, native] minimizing
    /// <c>ceil(W * n / 8) + (n - num_packed) * bytesPerException</c> where
    /// <c>num_packed = sum(freq[0..=W])</c>. Returns native if no W beats the
    /// all-exceptions baseline (effectively "don't bitpack").
    /// </summary>
    private static int FindBestBitWidth(int[] freq, int native, int bytesPerException)
    {
        long totalLen = 0;
        for (int i = 0; i < freq.Length; i++) totalLen += freq[i];
        if (totalLen == 0) return 0; // empty / all-null column

        // Match vortex's find_best_bit_width semantics: pick the smallest W
        // that minimizes packed_cost(W) + exceptions_cost(W). Costs are tied
        // at W=bestW vs W=native when the rounded byte count is identical
        // (small arrays with no exceptions); ties don't update bestWidth, so
        // bestWidth stays at the smaller W and FoR-style callers can rely on
        // a strict `bestWidth < native` outcome whenever the histogram allows.
        long bestCost = totalLen * bytesPerException; // baseline = all exceptions
        int bestWidth = 0;
        long numPacked = 0;
        for (int w = 0; w < freq.Length; w++)
        {
            long packedCost = (w * totalLen + 7) / 8;
            numPacked += freq[w];
            long exceptionsCost = (totalLen - numPacked) * bytesPerException;
            long cost = exceptionsCost + packedCost;
            if (cost < bestCost)
            {
                bestCost = cost;
                bestWidth = w;
            }
        }
        return bestWidth;
    }

    /// <summary>
    /// Walks the column once and gathers patches: for each non-null value
    /// requiring more than <paramref name="bitWidth"/> bits, record its
    /// (logical row index, value) pair. Also records <c>chunk_offsets[c]</c> =
    /// patch count accumulated before chunk c (length = numChunks). Indices
    /// type is u8/u16/u32 based on rowCount.
    /// </summary>
    private static (IArrowArray Indices, IArrowArray Values, IArrowArray ChunkOffsets, byte IndicesPtype)
        GatherPatches(IArrowArray array, int bitWidth, int patchCount)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;

        // Pick indices ptype.
        byte indicesPtype = n <= byte.MaxValue + 1 ? (byte)0
                          : n <= ushort.MaxValue + 1 ? (byte)1
                          : (byte)2;

        // Allocate index buffer at the chosen width.
        int idxByteWidth = indicesPtype == 0 ? 1 : indicesPtype == 1 ? 2 : 4;
        var indexBytes = new byte[patchCount * idxByteWidth];

        // Allocate values buffer at the parent's element size.
        int valByteWidth = ElementByteWidth(array);
        var valueBytes = new byte[(long)patchCount * valByteWidth];

        // Chunk offsets — one u64 per 1024-row chunk.
        int numChunks = (n + ElementsPerChunk - 1) / ElementsPerChunk;
        var chunkOffsetsBytes = new byte[(long)numChunks * 8];

        int patchPos = 0;
        for (int i = 0; i < n; i++)
        {
            if ((i & (ElementsPerChunk - 1)) == 0)
            {
                int c = i / ElementsPerChunk;
                System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(
                    chunkOffsetsBytes.AsSpan(c * 8, 8), (ulong)patchPos);
            }
            if (hasNulls && IsNullAt(validity, off + i)) continue;
            ulong v = ReadValueAsULong(array, data, off + i);
            int bits = v == 0 ? 0 : 64 - System.Numerics.BitOperations.LeadingZeroCount(v);
            if (bits <= bitWidth) continue;

            // Write index at the chosen width.
            switch (indicesPtype)
            {
                case 0: indexBytes[patchPos] = (byte)i; break;
                case 1: System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(
                    indexBytes.AsSpan(patchPos * 2, 2), (ushort)i); break;
                default: System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
                    indexBytes.AsSpan(patchPos * 4, 4), (uint)i); break;
            }
            // Write the original value at its native width.
            WriteValueAtNativeWidth(valueBytes, patchPos, v, valByteWidth);
            patchPos++;
        }

        IArrowArray indicesArray = indicesPtype switch
        {
            0 => new UInt8Array(new ArrowBuffer(indexBytes), ArrowBuffer.Empty, patchCount, 0, 0),
            1 => new UInt16Array(new ArrowBuffer(indexBytes), ArrowBuffer.Empty, patchCount, 0, 0),
            _ => new UInt32Array(new ArrowBuffer(indexBytes), ArrowBuffer.Empty, patchCount, 0, 0),
        };
        IArrowArray valuesArray = BuildValuesArray(array, valueBytes, patchCount);
        IArrowArray chunkOffsetsArray = new UInt64Array(
            new ArrowBuffer(chunkOffsetsBytes), ArrowBuffer.Empty, numChunks, 0, 0);
        return (indicesArray, valuesArray, chunkOffsetsArray, indicesPtype);
    }

    private static int ElementByteWidth(IArrowArray array) => array switch
    {
        UInt8Array or Int8Array => 1,
        UInt16Array or Int16Array => 2,
        UInt32Array or Int32Array => 4,
        UInt64Array or Int64Array => 8,
        _ => throw new NotSupportedException(),
    };

    private static ulong ReadValueAsULong(IArrowArray array, ArrayData data, int globalRow)
    {
        var src = data.Buffers[1].Span;
        return array switch
        {
            UInt8Array or Int8Array => src[globalRow],
            UInt16Array or Int16Array => System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(globalRow * 2, 2)),
            UInt32Array or Int32Array => System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(globalRow * 4, 4)),
            UInt64Array or Int64Array => System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(src.Slice(globalRow * 8, 8)),
            _ => throw new NotSupportedException(),
        };
    }

    private static void WriteValueAtNativeWidth(byte[] dst, int patchPos, ulong v, int byteWidth)
    {
        switch (byteWidth)
        {
            case 1: dst[patchPos] = (byte)v; break;
            case 2: System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(
                dst.AsSpan(patchPos * 2, 2), (ushort)v); break;
            case 4: System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
                dst.AsSpan(patchPos * 4, 4), (uint)v); break;
            default: System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(
                dst.AsSpan(patchPos * 8, 8), v); break;
        }
    }

    private static IArrowArray BuildValuesArray(IArrowArray template, byte[] bytes, int n)
    {
        var buf = new ArrowBuffer(bytes);
        return template switch
        {
            UInt8Array => new UInt8Array(buf, ArrowBuffer.Empty, n, 0, 0),
            Int8Array => new Int8Array(buf, ArrowBuffer.Empty, n, 0, 0),
            UInt16Array => new UInt16Array(buf, ArrowBuffer.Empty, n, 0, 0),
            Int16Array => new Int16Array(buf, ArrowBuffer.Empty, n, 0, 0),
            UInt32Array => new UInt32Array(buf, ArrowBuffer.Empty, n, 0, 0),
            Int32Array => new Int32Array(buf, ArrowBuffer.Empty, n, 0, 0),
            UInt64Array => new UInt64Array(buf, ArrowBuffer.Empty, n, 0, 0),
            Int64Array => new Int64Array(buf, ArrowBuffer.Empty, n, 0, 0),
            _ => throw new NotSupportedException(),
        };
    }

    private static int? NativeBits(IArrowArray array) => array switch
    {
        UInt8Array or Int8Array => 8,
        UInt16Array or Int16Array => 16,
        UInt32Array or Int32Array => 32,
        UInt64Array or Int64Array => 64,
        _ => null,
    };

    private static bool IsSigned(IArrowArray array) => array switch
    {
        Int8Array or Int16Array or Int32Array or Int64Array => true,
        _ => false,
    };

    /// <summary>True if any non-null value in a signed array is negative.</summary>
    private static bool HasNegative(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n == 0) return false;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;

        switch (array)
        {
            case Int8Array:
                {
                    var span = MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(off, n));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) continue;
                        if (span[i] < 0) return true;
                    }
                    return false;
                }
            case Int16Array:
                {
                    var span = MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) continue;
                        if (span[i] < 0) return true;
                    }
                    return false;
                }
            case Int32Array:
                {
                    var span = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) continue;
                        if (span[i] < 0) return true;
                    }
                    return false;
                }
            case Int64Array:
                {
                    var span = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) continue;
                        if (span[i] < 0) return true;
                    }
                    return false;
                }
            default: return false;
        }
    }

    private static bool IsNullAt(ReadOnlySpan<byte> validity, int i) =>
        (validity[i >> 3] & (1 << (i & 7))) == 0;

    /// <summary>
    /// Computes <c>MaxBits</c> over the column's non-null values. For signed
    /// arrays we know all values are non-negative (caller verified via
    /// <see cref="HasNegative"/>) so the byte view's high bit is always 0 — we
    /// can dispatch directly to the unsigned MaxBits kernel.
    /// </summary>
    private static int ComputeMaxBits(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n == 0) return 0;
        int off = data.Offset;

        // For nullable inputs, MaxBits over the raw buffer might pick up
        // garbage at null positions. Build a clean view first when needed.
        bool hasNulls = data.GetNullCount() > 0;
        if (hasNulls)
            return MaxBitsCleaned(array, data, n);

        // Non-nullable fast path: pass the raw buffer slice straight through.
        return array switch
        {
            UInt8Array or Int8Array => Clast.FastLanes.BitPacking.MaxBits<byte>(
                data.Buffers[1].Span.Slice(off, n)),
            UInt16Array or Int16Array => Clast.FastLanes.BitPacking.MaxBits<ushort>(
                MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(off * 2, n * 2))),
            UInt32Array or Int32Array => Clast.FastLanes.BitPacking.MaxBits<uint>(
                MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(off * 4, n * 4))),
            UInt64Array or Int64Array => Clast.FastLanes.BitPacking.MaxBits<ulong>(
                MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(off * 8, n * 8))),
            _ => throw new NotSupportedException(),
        };
    }

    private static int MaxBitsCleaned(IArrowArray array, ArrayData data, int n)
    {
        int off = data.Offset;
        var validity = data.Buffers[0].Span;
        switch (array)
        {
            case UInt8Array or Int8Array:
                {
                    var src = data.Buffers[1].Span.Slice(off, n);
                    var clean = new byte[n];
                    for (int i = 0; i < n; i++)
                        clean[i] = IsNullAt(validity, off + i) ? (byte)0 : src[i];
                    return Clast.FastLanes.BitPacking.MaxBits<byte>(clean);
                }
            case UInt16Array or Int16Array:
                {
                    var src = MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    var clean = new ushort[n];
                    for (int i = 0; i < n; i++)
                        clean[i] = IsNullAt(validity, off + i) ? (ushort)0 : src[i];
                    return Clast.FastLanes.BitPacking.MaxBits<ushort>(clean);
                }
            case UInt32Array or Int32Array:
                {
                    var src = MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    var clean = new uint[n];
                    for (int i = 0; i < n; i++)
                        clean[i] = IsNullAt(validity, off + i) ? 0u : src[i];
                    return Clast.FastLanes.BitPacking.MaxBits<uint>(clean);
                }
            case UInt64Array or Int64Array:
                {
                    var src = MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    var clean = new ulong[n];
                    for (int i = 0; i < n; i++)
                        clean[i] = IsNullAt(validity, off + i) ? 0UL : src[i];
                    return Clast.FastLanes.BitPacking.MaxBits<ulong>(clean);
                }
            default: throw new NotSupportedException();
        }
    }

    /// <summary>
    /// Packs <paramref name="array"/> to <c>numChunks × packedBytesPerChunk</c>
    /// bytes via Clast.FastLanes. Pads partial trailing chunks with zeros and
    /// replaces null positions with zeros.
    /// </summary>
    private static byte[] PackToBytes(IArrowArray array, int bitWidth, int rowCount)
    {
        int numChunks = (rowCount + ElementsPerChunk - 1) / ElementsPerChunk;
        if (bitWidth == 0)
        {
            // Per FastLanes: bit_width=0 means all values are 0 and the packed
            // buffer is zero bytes. Reader handles bit_width=0 directly.
            return System.Array.Empty<byte>();
        }

        var data = ((Apache.Arrow.Array)array).Data;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;

        return array switch
        {
            UInt8Array or Int8Array => PackTyped<byte>(
                data.Buffers[1].Span, off, validity, hasNulls, rowCount, bitWidth, numChunks),
            UInt16Array or Int16Array => PackTyped<ushort>(
                data.Buffers[1].Span, off, validity, hasNulls, rowCount, bitWidth, numChunks),
            UInt32Array or Int32Array => PackTyped<uint>(
                data.Buffers[1].Span, off, validity, hasNulls, rowCount, bitWidth, numChunks),
            UInt64Array or Int64Array => PackTyped<ulong>(
                data.Buffers[1].Span, off, validity, hasNulls, rowCount, bitWidth, numChunks),
            _ => throw new NotSupportedException($"PackToBytes doesn't support {array.GetType().Name}."),
        };
    }

    private static byte[] PackTyped<T>(
        ReadOnlySpan<byte> rawBytes, int elementOffset,
        ReadOnlySpan<byte> validity, bool hasNulls,
        int rowCount, int bitWidth, int numChunks)
        where T : unmanaged
    {
        int packedBytesPerChunk = Clast.FastLanes.BitPacking.PackedByteCount<T>(bitWidth);
        var output = new byte[(long)numChunks * packedBytesPerChunk];
        int elemSize = Marshal.SizeOf<T>();
        // Slice value bytes by elementOffset so values[i] corresponds to logical row i.
        var values = MemoryMarshal.Cast<byte, T>(rawBytes.Slice(elementOffset * elemSize, rowCount * elemSize));

        var chunkBuf = new T[ElementsPerChunk];
        for (int c = 0; c < numChunks; c++)
        {
            System.Array.Clear(chunkBuf, 0, chunkBuf.Length);
            int rowsInChunk = Math.Min(ElementsPerChunk, rowCount - c * ElementsPerChunk);
            int globalBase = c * ElementsPerChunk;
            if (hasNulls)
            {
                // Per-element: replace null positions with default(T)=0. Validity
                // bits live at absolute index elementOffset + logical_row.
                for (int i = 0; i < rowsInChunk; i++)
                {
                    int logicalRow = globalBase + i;
                    chunkBuf[i] = IsNullAt(validity, elementOffset + logicalRow) ? default : values[logicalRow];
                }
            }
            else
            {
                values.Slice(globalBase, rowsInChunk).CopyTo(chunkBuf);
            }
            PackChunk<T>(bitWidth, chunkBuf, output.AsSpan(c * packedBytesPerChunk, packedBytesPerChunk));
        }
        return output;
    }

    private static void PackChunk<T>(int bitWidth, ReadOnlySpan<T> input, Span<byte> packed)
        where T : unmanaged
    {
        if (typeof(T) == typeof(byte))
            Clast.FastLanes.BitPacking.PackChunk<byte>(bitWidth, MemoryMarshal.Cast<T, byte>(input), packed);
        else if (typeof(T) == typeof(ushort))
            Clast.FastLanes.BitPacking.PackChunk<ushort>(bitWidth, MemoryMarshal.Cast<T, ushort>(input), packed);
        else if (typeof(T) == typeof(uint))
            Clast.FastLanes.BitPacking.PackChunk<uint>(bitWidth, MemoryMarshal.Cast<T, uint>(input), packed);
        else if (typeof(T) == typeof(ulong))
            Clast.FastLanes.BitPacking.PackChunk<ulong>(bitWidth, MemoryMarshal.Cast<T, ulong>(input), packed);
        else
            throw new NotSupportedException($"PackChunk doesn't support element type {typeof(T)}.");
    }

    /// <summary>
    /// Inline BitPackedMetadata proto bytes:
    ///   field 1 (varint): bit_width (u32)
    ///   field 2 (varint): offset (u32, omitted when 0 per proto3 default)
    ///   field 3 (length-delim, optional): patches (PatchesMetadata embedded)
    /// PatchesMetadata fields used:
    ///   field 1 (varint): len (u64)              — patch count
    ///   field 2 (varint): offset (u64)           — omitted (0)
    ///   field 3 (varint): indices_ptype (PType)
    ///   field 4 (varint): chunk_offsets_len (u64)
    ///   field 5 (varint): chunk_offsets_ptype (PType, U64=3)
    /// </summary>
    private static byte[] SerializeBitPackedMetadata(
        int bitWidth, int patchCount, byte patchIndicesPtype, int rowCount)
    {
        // Worst case bytes for metadata + nested patches submessage.
        Span<byte> tmp = stackalloc byte[64];
        int pos = 0;
        tmp[pos++] = 0x08;
        pos += Varint.WriteUnsigned(tmp.Slice(pos), (ulong)bitWidth);

        if (patchCount > 0)
        {
            // Serialize PatchesMetadata into a temporary buffer first so we
            // know its length to emit the length prefix.
            int numChunks = (rowCount + ElementsPerChunk - 1) / ElementsPerChunk;
            Span<byte> sub = stackalloc byte[32];
            int subPos = 0;
            sub[subPos++] = 0x08; // tag 1: len
            subPos += Varint.WriteUnsigned(sub.Slice(subPos), (ulong)patchCount);
            sub[subPos++] = 0x18; // tag 3: indices_ptype
            sub[subPos++] = patchIndicesPtype;
            sub[subPos++] = 0x20; // tag 4: chunk_offsets_len
            subPos += Varint.WriteUnsigned(sub.Slice(subPos), (ulong)numChunks);
            sub[subPos++] = 0x28; // tag 5: chunk_offsets_ptype = U64 (3)
            sub[subPos++] = 3;

            tmp[pos++] = 0x1A; // tag 3, wire-type 2 (length-delim)
            pos += Varint.WriteUnsigned(tmp.Slice(pos), (ulong)subPos);
            sub.Slice(0, subPos).CopyTo(tmp.Slice(pos));
            pos += subPos;
        }

        return tmp.Slice(0, pos).ToArray();
    }
}
