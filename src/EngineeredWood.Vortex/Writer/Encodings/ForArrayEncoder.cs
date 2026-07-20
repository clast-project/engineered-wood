// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.ForArrayDecoder"/>:
/// emits a <c>fastlanes.for</c> (Frame of Reference) ArrayNode subtree.
/// Subtracts a reference scalar (the column's min) from every value; the
/// (always non-negative) residuals are then encoded as a child via
/// <see cref="BitPackedArrayEncoder"/>.
///
/// <para>Wire shape: 0 buffers, 1 child (the residuals — same Arrow dtype as
/// the parent — typically <c>fastlanes.bitpacked</c>), metadata = vortex
/// <c>ScalarValue</c> protobuf of the reference. Reader: <c>output[i] = ref + residuals[i]</c>.</para>
///
/// <para>Scope: non-sliced integer columns (Int8..Int64, UInt8..UInt64),
/// nullable + non-nullable. Min is computed over non-null values; null-position
/// residuals are zero-filled by the inner bitpacked encoder, so they never
/// inflate the bit width. The validity bitmap is preserved as the bitpacked
/// child's validity, which the reader propagates to the output.</para>
///
/// <para>FoR is profitable when EITHER (a) the column has negative values
/// that <see cref="BitPackedArrayEncoder"/> alone can't handle, OR (b)
/// <c>min != 0</c> AND <c>MaxBits(values - min) &lt; MaxBits(values)</c>, i.e.
/// shifting tightens the bit width. When neither holds, plain bitpacked wins
/// and dispatch falls through to it.</para>
/// </summary>
internal static class ForArrayEncoder
{
    /// <summary>
    /// True iff FoR can encode <paramref name="array"/> AND would compress
    /// strictly better than plain bitpacked (or where plain bitpacked doesn't
    /// even apply because of negative values).
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is null) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        if (array.Length == 0) return false;
        int? nativeBits = NativeBits(array);
        if (nativeBits is not int native) return false;
        // All-null columns: no min to subtract — let plain bitpacked handle it
        // (bit_width=0, single zero-padded chunk, validity child carries the nulls).
        if (data.GetNullCount() == array.Length) return false;

        int residualBits = ComputeResidualBits(array);
        if (residualBits >= native) return false; // FoR child wouldn't compress.

        // FoR is profitable when either:
        //   (a) bitpacked alone can't apply (signed column with any non-null negative).
        //   (b) min != 0 — residuals strictly fewer bits than direct values.
        if (IsSigned(array) && HasNegative(array)) return true;
        return MinIsNonZero(array);
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array,
        ushort forEncodingIdx, ushort bitpackedEncodingIdx, ushort primitiveEncodingIdx, ushort boolEncodingIdx,
        int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        var data = ((Apache.Arrow.Array)array).Data;

        // 1. Build residuals array (same Arrow dtype as parent) + serialize the
        //    reference scalar's protobuf bytes for the metadata field.
        var (residualArray, metadataBytes) = BuildResidualsAndMetadata(array);

        // 2. Emit residuals as a child via bitpacked. Child gets no stats —
        //    statsTicket only attaches at the top of the column.
        int residualNodeTicket = BitPackedArrayEncoder.Emit(
            sb, residualArray, bitpackedEncodingIdx, primitiveEncodingIdx, boolEncodingIdx);

        // 3. Emit FoR metadata as a byte vector (ScalarValue protobuf bytes).
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        // 4. Emit FoR ArrayNode: same vtable shape as vortex.list (0 buffers,
        //    metadata + children).
        var children = new[] { residualNodeTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, forEncodingIdx, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, forEncodingIdx, metadataTicket, children, statsTicket.Value);
    }

    /// <summary>Convenience: encode one column's segment in isolation.</summary>
    public static byte[] Encode(
        IArrowArray array, ushort forEncodingIdx, ushort bitpackedEncodingIdx,
        ushort primitiveEncodingIdx, ushort boolEncodingIdx)
    {
        var sb = new SegmentBuilder();
        var rootTicket = Emit(sb, array, forEncodingIdx, bitpackedEncodingIdx, primitiveEncodingIdx, boolEncodingIdx);
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

    private static bool IsNullAt(ReadOnlySpan<byte> validity, int i) =>
        (validity[i >> 3] & (1 << (i & 7))) == 0;

    /// <summary>True if any non-null value in a signed column is negative.</summary>
    private static bool HasNegative(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        switch (array)
        {
            case Int8Array:
                {
                    var s = MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(off, n));
                    for (int i = 0; i < n; i++)
                        if (!(hasNulls && IsNullAt(validity, off + i)) && s[i] < 0) return true;
                    return false;
                }
            case Int16Array:
                {
                    var s = MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    for (int i = 0; i < n; i++)
                        if (!(hasNulls && IsNullAt(validity, off + i)) && s[i] < 0) return true;
                    return false;
                }
            case Int32Array:
                {
                    var s = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    for (int i = 0; i < n; i++)
                        if (!(hasNulls && IsNullAt(validity, off + i)) && s[i] < 0) return true;
                    return false;
                }
            case Int64Array:
                {
                    var s = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    for (int i = 0; i < n; i++)
                        if (!(hasNulls && IsNullAt(validity, off + i)) && s[i] < 0) return true;
                    return false;
                }
            default: return false;
        }
    }

    /// <summary>True if the column's minimum value (over non-null slots) is non-zero.</summary>
    private static bool MinIsNonZero(IArrowArray array) => ComputeMinSignedOrUnsigned(array) != 0;

    /// <summary>
    /// Returns the column min as a 64-bit value (sign-extended for signed
    /// types, zero-extended for unsigned). Skips null slots. Caller has
    /// already ensured at least one non-null value exists.
    /// </summary>
    private static long ComputeMinSignedOrUnsigned(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        switch (array)
        {
            case Int8Array:
                {
                    var s = MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(off, n));
                    return ScanMinSigned<sbyte>(s, n, off, hasNulls, validity, v => v);
                }
            case Int16Array:
                {
                    var s = MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    return ScanMinSigned<short>(s, n, off, hasNulls, validity, v => v);
                }
            case Int32Array:
                {
                    var s = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    return ScanMinSigned<int>(s, n, off, hasNulls, validity, v => v);
                }
            case Int64Array:
                {
                    var s = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    return ScanMinSigned<long>(s, n, off, hasNulls, validity, v => v);
                }
            case UInt8Array:
                {
                    var s = data.Buffers[1].Span.Slice(off, n);
                    return ScanMinUnsigned(s, n, off, hasNulls, validity, v => (long)v);
                }
            case UInt16Array:
                {
                    var s = MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    return (long)ScanMinUnsignedSpan<ushort>(s, n, off, hasNulls, validity);
                }
            case UInt32Array:
                {
                    var s = MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    return (long)ScanMinUnsignedSpan<uint>(s, n, off, hasNulls, validity);
                }
            case UInt64Array:
                {
                    var s = MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    var min = ScanMinUnsignedSpan<ulong>(s, n, off, hasNulls, validity);
                    // Caller only uses == 0 comparison; cast preserves that semantically.
                    return unchecked((long)min);
                }
            default: throw new NotSupportedException();
        }
    }

    private static long ScanMinSigned<T>(ReadOnlySpan<T> span, int n, int off, bool hasNulls, ReadOnlySpan<byte> validity, Func<T, long> toLong)
        where T : unmanaged, IComparable<T>
    {
        long min = long.MaxValue;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls && IsNullAt(validity, off + i)) continue;
            long v = toLong(span[i]);
            if (v < min) min = v;
        }
        return min;
    }

    private static long ScanMinUnsigned(ReadOnlySpan<byte> span, int n, int off, bool hasNulls, ReadOnlySpan<byte> validity, Func<byte, long> toLong)
    {
        long min = long.MaxValue;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls && IsNullAt(validity, off + i)) continue;
            long v = toLong(span[i]);
            if (v < min) min = v;
        }
        return min;
    }

    private static T ScanMinUnsignedSpan<T>(ReadOnlySpan<T> span, int n, int off, bool hasNulls, ReadOnlySpan<byte> validity)
        where T : unmanaged, IComparable<T>
    {
        bool found = false;
        T min = default;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls && IsNullAt(validity, off + i)) continue;
            if (!found) { min = span[i]; found = true; }
            else if (span[i].CompareTo(min) < 0) min = span[i];
        }
        return min;
    }

    /// <summary>
    /// Computes <c>MaxBits(values - min)</c> over non-null slots — the bit
    /// width required by the FoR-shifted residuals. Caller has already ensured
    /// at least one non-null value exists.
    /// </summary>
    private static int ComputeResidualBits(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        ulong maxResidual = 0;
        switch (array)
        {
            case Int8Array:
                {
                    var s = MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(off, n));
                    sbyte min = sbyte.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (s[i] < min) min = s[i]; }
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; ulong r = (ulong)(s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case Int16Array:
                {
                    var s = MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    short min = short.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (s[i] < min) min = s[i]; }
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; ulong r = (ulong)(s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case Int32Array:
                {
                    var s = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    int min = int.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (s[i] < min) min = s[i]; }
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; ulong r = (ulong)((long)s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case Int64Array:
                {
                    var s = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    long min = long.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (s[i] < min) min = s[i]; }
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; ulong r = unchecked((ulong)(s[i] - min)); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case UInt8Array:
                {
                    var s = data.Buffers[1].Span.Slice(off, n);
                    byte min = byte.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (s[i] < min) min = s[i]; }
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; ulong r = (ulong)(s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case UInt16Array:
                {
                    var s = MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    ushort min = ushort.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (s[i] < min) min = s[i]; }
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; ulong r = (ulong)(s[i] - min); if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case UInt32Array:
                {
                    var s = MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    uint min = uint.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (s[i] < min) min = s[i]; }
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; ulong r = s[i] - min; if (r > maxResidual) maxResidual = r; }
                    break;
                }
            case UInt64Array:
                {
                    var s = MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    ulong min = ulong.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (s[i] < min) min = s[i]; }
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; ulong r = s[i] - min; if (r > maxResidual) maxResidual = r; }
                    break;
                }
            default: throw new NotSupportedException();
        }
        return maxResidual == 0 ? 0 : 64 - System.Numerics.BitOperations.LeadingZeroCount(maxResidual);
    }

    /// <summary>
    /// Constructs the residuals Arrow array (same dtype as parent) and the
    /// reference-scalar's ScalarValue protobuf bytes. Min is computed over
    /// non-null slots only; null-position residual bytes can hold anything
    /// (the inner bitpacked encoder zero-fills nulls before packing). The
    /// residuals array shares the input's validity bitmap so the bitpacked
    /// encoder emits a validity child and the reader recovers nullability.
    /// </summary>
    private static (IArrowArray Residuals, byte[] MetadataBytes) BuildResidualsAndMetadata(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        // For sliced inputs, extract a fresh bit-aligned validity bitmap so the
        // residuals array can have offset=0 with its own values buffer of
        // length n. Re-using the source's validity buffer at offset=0 would
        // misindex when off > 0; passing it with offset would require a
        // matching values buffer of length off+n, which is wasteful.
        ArrowBuffer validityBuf = hasNulls
            ? new ArrowBuffer(EncoderHelpers.ExtractValidityBitmap(validity, srcBitOffset: off, rowCount: n))
            : ArrowBuffer.Empty;
        int nullCount = hasNulls ? data.GetNullCount() : 0;

        switch (array)
        {
            case Int8Array:
                {
                    var src = MemoryMarshal.Cast<byte, sbyte>(data.Buffers[1].Span.Slice(off, n));
                    sbyte min = sbyte.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (src[i] < min) min = src[i]; }
                    var bytes = new byte[n];
                    var dst = MemoryMarshal.Cast<byte, sbyte>(bytes.AsSpan());
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) { dst[i] = 0; continue; }
                        dst[i] = (sbyte)(src[i] - min);
                    }
                    return (new Int8Array(new ArrowBuffer(bytes), validityBuf, n, nullCount, 0),
                            ScalarValueSerializer.FromSignedInt(min));
                }
            case Int16Array:
                {
                    var src = MemoryMarshal.Cast<byte, short>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    short min = short.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (src[i] < min) min = src[i]; }
                    var bytes = new byte[n * 2];
                    var dst = MemoryMarshal.Cast<byte, short>(bytes.AsSpan());
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) { dst[i] = 0; continue; }
                        dst[i] = (short)(src[i] - min);
                    }
                    return (new Int16Array(new ArrowBuffer(bytes), validityBuf, n, nullCount, 0),
                            ScalarValueSerializer.FromSignedInt(min));
                }
            case Int32Array:
                {
                    var src = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    int min = int.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (src[i] < min) min = src[i]; }
                    var bytes = new byte[n * 4];
                    var dst = MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) { dst[i] = 0; continue; }
                        dst[i] = src[i] - min;
                    }
                    return (new Int32Array(new ArrowBuffer(bytes), validityBuf, n, nullCount, 0),
                            ScalarValueSerializer.FromSignedInt(min));
                }
            case Int64Array:
                {
                    var src = MemoryMarshal.Cast<byte, long>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    long min = long.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (src[i] < min) min = src[i]; }
                    var bytes = new byte[n * 8];
                    var dst = MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) { dst[i] = 0; continue; }
                        dst[i] = src[i] - min;
                    }
                    return (new Int64Array(new ArrowBuffer(bytes), validityBuf, n, nullCount, 0),
                            ScalarValueSerializer.FromSignedInt(min));
                }
            case UInt8Array:
                {
                    var src = data.Buffers[1].Span.Slice(off, n);
                    byte min = byte.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (src[i] < min) min = src[i]; }
                    var bytes = new byte[n];
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) { bytes[i] = 0; continue; }
                        bytes[i] = (byte)(src[i] - min);
                    }
                    return (new UInt8Array(new ArrowBuffer(bytes), validityBuf, n, nullCount, 0),
                            ScalarValueSerializer.FromUnsignedInt(min));
                }
            case UInt16Array:
                {
                    var src = MemoryMarshal.Cast<byte, ushort>(data.Buffers[1].Span.Slice(off * 2, n * 2));
                    ushort min = ushort.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (src[i] < min) min = src[i]; }
                    var bytes = new byte[n * 2];
                    var dst = MemoryMarshal.Cast<byte, ushort>(bytes.AsSpan());
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) { dst[i] = 0; continue; }
                        dst[i] = (ushort)(src[i] - min);
                    }
                    return (new UInt16Array(new ArrowBuffer(bytes), validityBuf, n, nullCount, 0),
                            ScalarValueSerializer.FromUnsignedInt(min));
                }
            case UInt32Array:
                {
                    var src = MemoryMarshal.Cast<byte, uint>(data.Buffers[1].Span.Slice(off * 4, n * 4));
                    uint min = uint.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (src[i] < min) min = src[i]; }
                    var bytes = new byte[n * 4];
                    var dst = MemoryMarshal.Cast<byte, uint>(bytes.AsSpan());
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) { dst[i] = 0; continue; }
                        dst[i] = src[i] - min;
                    }
                    return (new UInt32Array(new ArrowBuffer(bytes), validityBuf, n, nullCount, 0),
                            ScalarValueSerializer.FromUnsignedInt(min));
                }
            case UInt64Array:
                {
                    var src = MemoryMarshal.Cast<byte, ulong>(data.Buffers[1].Span.Slice(off * 8, n * 8));
                    ulong min = ulong.MaxValue;
                    for (int i = 0; i < n; i++) { if (hasNulls && IsNullAt(validity, off + i)) continue; if (src[i] < min) min = src[i]; }
                    var bytes = new byte[n * 8];
                    var dst = MemoryMarshal.Cast<byte, ulong>(bytes.AsSpan());
                    for (int i = 0; i < n; i++)
                    {
                        if (hasNulls && IsNullAt(validity, off + i)) { dst[i] = 0; continue; }
                        dst[i] = src[i] - min;
                    }
                    return (new UInt64Array(new ArrowBuffer(bytes), validityBuf, n, nullCount, 0),
                            ScalarValueSerializer.FromUnsignedInt(min));
                }
            default:
                throw new NotSupportedException(
                    $"fastlanes.for doesn't support Arrow {array.GetType().Name}.");
        }
    }
}
