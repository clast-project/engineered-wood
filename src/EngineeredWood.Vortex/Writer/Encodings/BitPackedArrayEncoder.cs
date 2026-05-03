// Copyright (c) Curt Hagenlocher. All rights reserved.
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
/// <para>Wire shape (non-nullable): 1 buffer (packed bytes), 0 children,
/// metadata <c>{bit_width}</c>. Nullable: 1 packed buffer, 1 child (a
/// <c>vortex.bool</c> ArrayNode carrying the validity bitmap).</para>
/// </summary>
internal static class BitPackedArrayEncoder
{
    private const int ElementsPerChunk = 1024;

    /// <summary>
    /// Returns true iff <paramref name="array"/> is a supported shape AND
    /// bitpacking would actually save space (max bit width &lt; native).
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        if (data.Offset != 0) return false;
        int? nativeBits = NativeBits(array);
        if (nativeBits is not int native) return false;

        if (IsSigned(array) && HasNegative(array)) return false;

        int maxBits = ComputeMaxBits(array);
        return maxBits < native;
    }

    public static int Emit(SegmentBuilder sb, IArrowArray array, ushort bitpackedEncodingIdx,
        ushort boolEncodingIdx, int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        var data = ((Apache.Arrow.Array)array).Data;
        if (data.Offset != 0)
            throw new NotSupportedException("fastlanes.bitpacked writer doesn't yet support sliced inputs.");

        int? nativeBits = NativeBits(array);
        if (nativeBits is not int native)
            throw new NotSupportedException(
                $"fastlanes.bitpacked writer doesn't support Arrow array {array.GetType().Name}.");

        if (IsSigned(array) && HasNegative(array))
            throw new InvalidOperationException(
                $"fastlanes.bitpacked requires non-negative values; {array.GetType().Name} contains negatives.");

        int rowCount = array.Length;
        int bitWidth = ComputeMaxBits(array);
        if (bitWidth > native)
            throw new InvalidOperationException(
                $"MaxBits {bitWidth} exceeds native {native} for {array.GetType().Name}.");

        var packedBytes = PackToBytes(array, bitWidth, rowCount);
        ushort packedBufIdx = sb.AddBuffer(packedBytes, alignmentExponent: 0);

        var metadataBytes = SerializeBitPackedMetadata(bitWidth);
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        // Emit optional validity child as a vortex.bool ArrayNode.
        if (data.GetNullCount() > 0)
        {
            var bitmap = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: rowCount);
            ushort bitmapBufIdx = sb.AddBuffer(bitmap, alignmentExponent: 0);
            int validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, boolEncodingIdx, bitmapBufIdx);
            var children = new[] { validityNodeTicket };
            return statsTicket is null
                ? ArrayNodeEmitter.EmitWithMetadataBufferAndChildren(
                    sb.Builder, bitpackedEncodingIdx, packedBufIdx, metadataTicket, children)
                : ArrayNodeEmitter.EmitWithMetadataBufferChildrenAndStats(
                    sb.Builder, bitpackedEncodingIdx, packedBufIdx, metadataTicket,
                    children, statsTicket.Value);
        }

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndBuffer(
                sb.Builder, bitpackedEncodingIdx, packedBufIdx, metadataTicket)
            : ArrayNodeEmitter.EmitWithMetadataBufferAndStats(
                sb.Builder, bitpackedEncodingIdx, packedBufIdx, metadataTicket, statsTicket.Value);
    }

    /// <summary>Convenience: encode one column's segment in isolation.</summary>
    public static byte[] Encode(IArrowArray array, ushort bitpackedEncodingIdx, ushort boolEncodingIdx)
    {
        var sb = new SegmentBuilder();
        var rootTicket = Emit(sb, array, bitpackedEncodingIdx, boolEncodingIdx);
        return sb.FinishSegment(rootTicket);
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
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;

        switch (array)
        {
            case Int8Array:
                {
                    var span = MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(0, n));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, i)) continue;
                        if (span[i] < 0) return true;
                    }
                    return false;
                }
            case Int16Array:
                {
                    var span = MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(0, n * 2));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, i)) continue;
                        if (span[i] < 0) return true;
                    }
                    return false;
                }
            case Int32Array:
                {
                    var span = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(0, n * 4));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, i)) continue;
                        if (span[i] < 0) return true;
                    }
                    return false;
                }
            case Int64Array:
                {
                    var span = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(0, n * 8));
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, i)) continue;
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

        // For nullable inputs, MaxBits over the raw buffer might pick up
        // garbage at null positions. Build a clean view first when needed.
        bool hasNulls = data.GetNullCount() > 0;
        if (hasNulls)
            return MaxBitsCleaned(array, data, n);

        // Non-nullable fast path: pass the raw buffer slice straight through.
        return array switch
        {
            UInt8Array or Int8Array => Clast.FastLanes.BitPacking.MaxBits<byte>(
                data.Buffers[1].Span.Slice(0, n)),
            UInt16Array or Int16Array => Clast.FastLanes.BitPacking.MaxBits<ushort>(
                MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(0, n * 2))),
            UInt32Array or Int32Array => Clast.FastLanes.BitPacking.MaxBits<uint>(
                MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(0, n * 4))),
            UInt64Array or Int64Array => Clast.FastLanes.BitPacking.MaxBits<ulong>(
                MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(0, n * 8))),
            _ => throw new NotSupportedException(),
        };
    }

    private static int MaxBitsCleaned(IArrowArray array, ArrayData data, int n)
    {
        var validity = data.Buffers[0].Span;
        switch (array)
        {
            case UInt8Array or Int8Array:
                {
                    var src = data.Buffers[1].Span.Slice(0, n);
                    var clean = new byte[n];
                    for (int i = 0; i < n; i++)
                        clean[i] = IsNullAt(validity, i) ? (byte)0 : src[i];
                    return Clast.FastLanes.BitPacking.MaxBits<byte>(clean);
                }
            case UInt16Array or Int16Array:
                {
                    var src = MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(0, n * 2));
                    var clean = new ushort[n];
                    for (int i = 0; i < n; i++)
                        clean[i] = IsNullAt(validity, i) ? (ushort)0 : src[i];
                    return Clast.FastLanes.BitPacking.MaxBits<ushort>(clean);
                }
            case UInt32Array or Int32Array:
                {
                    var src = MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(0, n * 4));
                    var clean = new uint[n];
                    for (int i = 0; i < n; i++)
                        clean[i] = IsNullAt(validity, i) ? 0u : src[i];
                    return Clast.FastLanes.BitPacking.MaxBits<uint>(clean);
                }
            case UInt64Array or Int64Array:
                {
                    var src = MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(0, n * 8));
                    var clean = new ulong[n];
                    for (int i = 0; i < n; i++)
                        clean[i] = IsNullAt(validity, i) ? 0UL : src[i];
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
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;

        return array switch
        {
            UInt8Array or Int8Array => PackTyped<byte>(
                data.Buffers[1].Span, validity, hasNulls, rowCount, bitWidth, numChunks),
            UInt16Array or Int16Array => PackTyped<ushort>(
                data.Buffers[1].Span, validity, hasNulls, rowCount, bitWidth, numChunks),
            UInt32Array or Int32Array => PackTyped<uint>(
                data.Buffers[1].Span, validity, hasNulls, rowCount, bitWidth, numChunks),
            UInt64Array or Int64Array => PackTyped<ulong>(
                data.Buffers[1].Span, validity, hasNulls, rowCount, bitWidth, numChunks),
            _ => throw new NotSupportedException($"PackToBytes doesn't support {array.GetType().Name}."),
        };
    }

    private static byte[] PackTyped<T>(
        ReadOnlySpan<byte> rawBytes, ReadOnlySpan<byte> validity, bool hasNulls,
        int rowCount, int bitWidth, int numChunks)
        where T : unmanaged
    {
        int packedBytesPerChunk = Clast.FastLanes.BitPacking.PackedByteCount<T>(bitWidth);
        var output = new byte[(long)numChunks * packedBytesPerChunk];
        var values = MemoryMarshal.Cast<byte, T>(rawBytes.Slice(0, rowCount * Marshal.SizeOf<T>()));

        var chunkBuf = new T[ElementsPerChunk];
        for (int c = 0; c < numChunks; c++)
        {
            System.Array.Clear(chunkBuf, 0, chunkBuf.Length);
            int rowsInChunk = Math.Min(ElementsPerChunk, rowCount - c * ElementsPerChunk);
            int globalBase = c * ElementsPerChunk;
            if (hasNulls)
            {
                // Per-element: replace null positions with default(T)=0.
                for (int i = 0; i < rowsInChunk; i++)
                {
                    int gi = globalBase + i;
                    chunkBuf[i] = IsNullAt(validity, gi) ? default : values[gi];
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
    /// </summary>
    private static byte[] SerializeBitPackedMetadata(int bitWidth)
    {
        Span<byte> tmp = stackalloc byte[1 + 5];
        int pos = 0;
        tmp[pos++] = 0x08;
        pos += Varint.WriteUnsigned(tmp.Slice(pos), (ulong)bitWidth);
        return tmp.Slice(0, pos).ToArray();
    }
}
