// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.SparseArrayDecoder"/>:
/// emits a <c>vortex.sparse</c> ArrayNode subtree for primitive integer columns
/// where one value (the mode) covers the bulk of the rows. Stores that value
/// once as a fill scalar plus (index, value) patches for the rows where the
/// column deviates.
///
/// <para>Wire shape: 1 buffer (fill_value as ScalarValue protobuf bytes), 2
/// children (patch_indices, patch_values), metadata
/// <c>SparseMetadata { patches: PatchesMetadata { len, offset = 0, indices_ptype } }</c>.
/// vortex's <c>SparseMetadata</c> wraps the patches descriptor as a
/// length-delimited submessage at field 1.</para>
///
/// <para>Phase 1 scope: nullable + non-nullable, sliced + non-sliced
/// primitive integer columns (Int8..Int64, UInt8..UInt64). Float fill values
/// are deferred until ALP + RLE stop being the dominant float-compression
/// paths.</para>
/// </summary>
internal static class SparseArrayEncoder
{
    /// <summary>
    /// Profitability gate: accept only when sparse storage (fill scalar +
    /// numPatches × (sizeof(index) + sizeof(value))) is at least 1.5× smaller
    /// than raw. Picks the column's mode in O(n) before deciding.
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is null) return false;
        if (ElementSize(array) is not int elemSize) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n < 2) return false; // constant catches the 1-row case.
        int nullCount = data.GetNullCount();
        if (nullCount == n) return false; // all-null — constant claims with null fill.

        var (modeKey, modeFreq) = FindMode(array, n, elemSize);
        int numPatches = n - modeFreq;
        if (numPatches == 0) return false; // constant should claim this column.

        int indicesElemSize = SmallestUIntElemSize(n);
        // Approximate fill scalar overhead — varint-encoded sint64 / uint64
        // / fixed-width float — bounded by ~10 bytes incl. tag.
        const int fillOverheadBytes = 10;
        // Patch values inherit the parent's nullability — when the input is
        // nullable, the patch values array carries a validity bitmap (one
        // bit per patch, packed). Approximate as numPatches/8 bytes.
        long sparseBytes = fillOverheadBytes
            + (long)numPatches * (indicesElemSize + elemSize)
            + (nullCount > 0 ? (long)(numPatches + 7) / 8 : 0);
        long rawBytes = (long)n * elemSize;
        _ = modeKey; // mode value isn't needed in the gate; recomputed in Emit.
        return sparseBytes * 3 / 2 < rawBytes;
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        if (ElementSize(array) is not int elemSize)
            throw new NotSupportedException(
                $"vortex.sparse writer doesn't support Arrow {array.GetType().Name}.");
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        int off = data.Offset;
        int nullCount = data.GetNullCount();
        bool hasNulls = nullCount > 0;
        var src = data.Buffers[1].Span;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        var (modeKey, _) = FindMode(array, n, elemSize);

        // Walk a second time gathering patches. A row is a patch if it's
        // null OR has a value other than the mode. The mode is computed
        // from non-null values only, so null rows always become patches
        // (the fill scalar is non-null in the case-A nullable strategy).
        int patchCount = 0;
        for (int i = 0; i < n; i++)
        {
            bool isNull = hasNulls && (validity[(off + i) >> 3] & (1 << ((off + i) & 7))) == 0;
            if (isNull || ReadKey(src, (off + i) * elemSize, elemSize) != modeKey) patchCount++;
        }

        byte indicesPtype = SmallestUIntPtypeFor(n);
        int indicesElemSize = SmallestUIntElemSize(n);
        var indicesBytes = new byte[(long)patchCount * indicesElemSize];
        var valuesBytes = new byte[(long)patchCount * elemSize];
        // Patch values' validity bitmap (one bit per patch). Allocated only
        // for nullable inputs; null rows propagate as cleared bits.
        byte[]? patchValuesValidityBytes = hasNulls ? new byte[(patchCount + 7) / 8] : null;
        int patchValuesNullCount = 0;

        int writeIdx = 0;
        for (int i = 0; i < n; i++)
        {
            bool isNull = hasNulls && (validity[(off + i) >> 3] & (1 << ((off + i) & 7))) == 0;
            if (!isNull && ReadKey(src, (off + i) * elemSize, elemSize) == modeKey) continue;

            // Patch indices are logical positions within the slice, NOT
            // absolute rows in the underlying buffer.
            WriteIndex(indicesBytes.AsSpan(writeIdx * indicesElemSize, indicesElemSize), (ulong)i, indicesElemSize);
            if (!isNull)
            {
                src.Slice((off + i) * elemSize, elemSize)
                    .CopyTo(valuesBytes.AsSpan(writeIdx * elemSize, elemSize));
            }
            // (else the value slot stays zeroed — the patch's validity bit masks it)
            if (patchValuesValidityBytes is not null)
            {
                if (isNull) patchValuesNullCount++;
                else patchValuesValidityBytes[writeIdx >> 3] |= (byte)(1 << (writeIdx & 7));
            }
            writeIdx++;
        }

        var indicesArr = BuildUnsignedArray(indicesBytes, patchCount, indicesElemSize);
        var patchValidityBuf = patchValuesValidityBytes is null
            ? ArrowBuffer.Empty
            : new ArrowBuffer(patchValuesValidityBytes);
        var valuesArr = BuildPrimitiveArray(
            array, valuesBytes, patchCount, patchValidityBuf, patchValuesNullCount);
        var fillValueBytes = SerializeFillScalar(array, modeKey);

        // Children: patch_indices, patch_values. Both go through dispatch
        // with compress=true since indices are strictly-sorted unsigned ints
        // (often small range) and values may have their own structure.
        ushort fillBufIdx = sb.AddBuffer(fillValueBytes, alignmentExponent: 0);
        int indicesTicket = ArrayEncoderDispatch.Emit(sb, indicesArr, idx, statsTicket: null, compress: true);
        int valuesTicket = ArrayEncoderDispatch.Emit(sb, valuesArr, idx, statsTicket: null, compress: true);

        var metadataBytes = SerializeMetadata((ulong)patchCount, indicesPtype);
        int metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var children = new[] { indicesTicket, valuesTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataBufferAndChildren(
                sb.Builder, idx.Sparse, fillBufIdx, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataBufferChildrenAndStats(
                sb.Builder, idx.Sparse, fillBufIdx, metadataTicket, children, statsTicket.Value);
    }

    /// <summary>
    /// O(n) mode discovery via a Dictionary keyed on the value's 64-bit bit
    /// pattern (works for any primitive ≤ 8 bytes; signed-vs-unsigned
    /// irrelevant since we compare bit patterns). Null rows are skipped —
    /// the mode is the most-frequent **non-null** value, which becomes the
    /// fill scalar; null rows always land in patches.
    /// </summary>
    private static (long ModeKey, int ModeFreq) FindMode(
        IArrowArray array, int n, int elemSize)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        var src = data.Buffers[1].Span;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        var counts = new Dictionary<long, int>();
        for (int i = 0; i < n; i++)
        {
            if (hasNulls && (validity[(off + i) >> 3] & (1 << ((off + i) & 7))) == 0) continue;
            long key = ReadKey(src, (off + i) * elemSize, elemSize);
            counts.TryGetValue(key, out int c);
            counts[key] = c + 1;
        }
        long bestKey = 0;
        int bestFreq = 0;
        foreach (var kv in counts)
        {
            if (kv.Value > bestFreq)
            {
                bestKey = kv.Key;
                bestFreq = kv.Value;
            }
        }
        return (bestKey, bestFreq);
    }

    private static int? ElementSize(IArrowArray array) => array switch
    {
        Int8Array or UInt8Array => 1,
        Int16Array or UInt16Array => 2,
        Int32Array or UInt32Array => 4,
        Int64Array or UInt64Array => 8,
        _ => null,
    };

    private static int SmallestUIntElemSize(int n)
    {
        if (n <= byte.MaxValue) return 1;
        if (n <= ushort.MaxValue) return 2;
        if ((uint)n <= uint.MaxValue) return 4;
        return 8;
    }

    private static byte SmallestUIntPtypeFor(int n) => SmallestUIntElemSize(n) switch
    {
        1 => 0, // U8
        2 => 1, // U16
        4 => 2, // U32
        _ => 3, // U64
    };

    private static long ReadKey(ReadOnlySpan<byte> src, int byteOffset, int elemSize) => elemSize switch
    {
        1 => src[byteOffset],
        2 => BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(byteOffset, 2)),
        4 => BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(byteOffset, 4)),
        8 => unchecked((long)BinaryPrimitives.ReadUInt64LittleEndian(src.Slice(byteOffset, 8))),
        _ => throw new NotSupportedException(),
    };

    private static void WriteIndex(Span<byte> dest, ulong value, int elemSize)
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

    private static IArrowArray BuildPrimitiveArray(
        IArrowArray template, byte[] valuesBytes, int len,
        ArrowBuffer validity, int nullCount)
    {
        var buf = new ArrowBuffer(valuesBytes);
        return template switch
        {
            Int8Array => new Int8Array(buf, validity, len, nullCount, 0),
            UInt8Array => new UInt8Array(buf, validity, len, nullCount, 0),
            Int16Array => new Int16Array(buf, validity, len, nullCount, 0),
            UInt16Array => new UInt16Array(buf, validity, len, nullCount, 0),
            Int32Array => new Int32Array(buf, validity, len, nullCount, 0),
            UInt32Array => new UInt32Array(buf, validity, len, nullCount, 0),
            Int64Array => new Int64Array(buf, validity, len, nullCount, 0),
            UInt64Array => new UInt64Array(buf, validity, len, nullCount, 0),
            _ => throw new NotSupportedException(),
        };
    }

    /// <summary>
    /// Serializes the fill scalar to <c>ScalarValue</c> proto bytes. Tag
    /// numbers come from <c>vortex-proto/proto/scalar.proto</c>: 3 = sint64
    /// for signed integers, 4 = uint64 for unsigned. The Rust reader consumes
    /// these bytes directly via <c>ScalarValue::from_proto_bytes</c>.
    /// </summary>
    private static byte[] SerializeFillScalar(IArrowArray array, long modeKey)
    {
        return array switch
        {
            Int8Array or Int16Array or Int32Array or Int64Array =>
                ScalarValueSerializer.FromSignedInt(SignExtendSigned(array, modeKey)),
            UInt8Array or UInt16Array or UInt32Array or UInt64Array =>
                ScalarValueSerializer.FromUnsignedInt((ulong)modeKey),
            _ => throw new NotSupportedException(),
        };
    }

    /// <summary>
    /// <see cref="ReadKey"/> zero-extends the source bytes to long, but the
    /// Rust reader expects sint64-style scalars sign-extended from the
    /// column's native width. Re-extend here so a UInt8 mode of 0xFF encodes
    /// as 255 (matches our Int8 reader's <c>(sbyte)fill.Int64Value</c> cast)
    /// and an Int8 mode of −1 encodes as the sint64 −1 (bit pattern with all
    /// high bits set after sign extension).
    /// </summary>
    private static long SignExtendSigned(IArrowArray array, long modeKey) => array switch
    {
        Int8Array => (sbyte)(byte)modeKey,
        Int16Array => (short)(ushort)modeKey,
        Int32Array => (int)(uint)modeKey,
        Int64Array => modeKey,
        _ => throw new NotSupportedException(),
    };

    /// <summary>
    /// <c>SparseMetadata { patches: PatchesMetadata }</c>. PatchesMetadata
    /// fields used: 1=len (varint), 2=offset (varint, omitted when 0),
    /// 3=indices_ptype (varint). Outer SparseMetadata wraps PatchesMetadata
    /// at field 1 (length-delim, wire-type 2 → tag 0x0A).
    /// </summary>
    private static byte[] SerializeMetadata(ulong patchesLen, byte indicesPtype)
    {
        // Inner PatchesMetadata.
        Span<byte> inner = stackalloc byte[16];
        int innerPos = 0;
        inner[innerPos++] = 0x08; // field 1, varint — len
        innerPos += Varint.WriteUnsigned(inner.Slice(innerPos), patchesLen);
        // field 2 (offset) omitted: proto3 default of 0 matches our
        // non-sliced constraint.
        inner[innerPos++] = 0x18; // field 3, varint — indices_ptype
        inner[innerPos++] = indicesPtype;

        Span<byte> outer = stackalloc byte[24];
        int outerPos = 0;
        outer[outerPos++] = 0x0A; // field 1, wire-type 2 — patches submessage
        outerPos += Varint.WriteUnsigned(outer.Slice(outerPos), (ulong)innerPos);
        inner.Slice(0, innerPos).CopyTo(outer.Slice(outerPos));
        outerPos += innerPos;
        return outer.Slice(0, outerPos).ToArray();
    }
}
