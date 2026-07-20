// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.DictArrayDecoder"/>:
/// emits a <c>vortex.dict</c> ArrayNode subtree for repetitive
/// <see cref="StringArray"/> columns. Builds a deduplicated dictionary of
/// distinct values and a per-row codes array; reader does
/// <c>output[i] = values[codes[i]]</c>.
///
/// <para>Wire shape: 0 buffers, 2 children (codes, values), metadata
/// <c>DictMetadata { codes_ptype, values_len }</c>. Same vtable as vortex.list
/// (slots 0+1+2, with optional slot 4 for stats). Codes child is encoded as
/// vortex.primitive (smallest fitting unsigned width); values child is
/// vortex.varbin.</para>
///
/// <para>Scope: Arrow <see cref="StringArray"/>, nullable + non-nullable.
/// For nullable inputs the dict values are non-nullable (only distinct
/// non-null strings) and the codes child carries the validity bitmap.
/// <c>is_nullable_codes</c> in the metadata is set accordingly.</para>
/// </summary>
internal static class DictArrayEncoder
{
    /// <summary>
    /// Returns true iff the column is a StringArray with at least one non-null
    /// value AND the distinct count is small enough that the codes + dict-values
    /// payload is meaningfully smaller than the raw varbin encoding. Heuristic:
    /// at least 4× repetition (<c>K × 4 ≤ n</c>) AND at least 8 rows.
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is not StringArray s) return false;
        var data = s.Data;
        int n = s.Length;
        if (n < 8) return false;
        int nullCount = data.GetNullCount();
        if (nullCount == n) return false; // all-null — nothing to dict

        // Probe distinct count over non-null values, capped early.
        var seen = new HashSet<string>(StringComparer.Ordinal);
        int cap = n / 4;
        bool hasNulls = nullCount > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        int off = data.Offset;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls && !IsValidAt(validity, off + i)) continue;
            seen.Add(s.GetString(i));
            if (seen.Count > cap) return false;
        }
        return true;
    }

    private static bool IsValidAt(ReadOnlySpan<byte> bitmap, int i) =>
        (bitmap[i >> 3] & (1 << (i & 7))) != 0;

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is not StringArray s)
            throw new NotSupportedException(
                $"vortex.dict writer requires StringArray, got {array.GetType().Name}.");
        var data = s.Data;

        int n = s.Length;
        int nullCount = data.GetNullCount();
        bool hasNulls = nullCount > 0;
        int off = data.Offset;
        var validity = hasNulls ? data.Buffers[0].Span : default;

        // 1. Build dictionary in input order — first occurrence (of a non-null
        //    value) assigns the index. Null positions get code = 0; the codes
        //    child's validity bit at that row is 0 so the value is masked.
        //    StringArray.GetString(i) already resolves through data.Offset, but
        //    the validity bitmap is bit-addressed at data.Offset and needs to
        //    be read with that offset added to i.
        var lookup = new Dictionary<string, int>(StringComparer.Ordinal);
        var distinct = new List<string>();
        var codes = new int[n];
        for (int i = 0; i < n; i++)
        {
            if (hasNulls && !IsValidAt(validity, off + i))
            {
                codes[i] = 0;
                continue;
            }
            var v = s.GetString(i);
            if (!lookup.TryGetValue(v, out int idx_))
            {
                idx_ = distinct.Count;
                lookup.Add(v, idx_);
                distinct.Add(v);
            }
            codes[i] = idx_;
        }
        int k = distinct.Count;

        // 2. Pick the smallest unsigned codes width. PType enum: U8=0, U16=1, U32=2, U64=3.
        //    For nullable input, the codes array carries a fresh row-0-aligned
        //    copy of the visible validity bits — Apache.Arrow's bitmap is bit-
        //    addressed at data.Offset, so we can't just hand it through
        //    untouched when the input is sliced.
        ArrowBuffer codesValidityBuf = hasNulls
            ? new ArrowBuffer(EncoderHelpers.ExtractValidityBitmap(validity, srcBitOffset: off, rowCount: n))
            : ArrowBuffer.Empty;
        IArrowArray codesArray;
        byte codesPtype;
        if (k <= byte.MaxValue + 1) // K up to 256 fits in u8.
        {
            codesArray = BuildCodesU8(codes, codesValidityBuf, nullCount);
            codesPtype = 0;
        }
        else if (k <= ushort.MaxValue + 1) // up to 65536 fits in u16.
        {
            codesArray = BuildCodesU16(codes, codesValidityBuf, nullCount);
            codesPtype = 1;
        }
        else
        {
            codesArray = BuildCodesU32(codes, codesValidityBuf, nullCount);
            codesPtype = 2;
        }

        // 3. Build the values StringArray (dictionary itself — non-nullable).
        var valuesArray = BuildStringArray(distinct);

        // 4. Encode children recursively. Codes go through dispatch with
        //    compress=false so they don't accidentally pick up another encoding
        //    layer; same for values.
        int codesNodeTicket = ArrayEncoderDispatch.Emit(sb, codesArray, idx);
        int valuesNodeTicket = ArrayEncoderDispatch.Emit(sb, valuesArray, idx);

        // 5. Metadata. Set is_nullable_codes when the input had nulls.
        var metadataBytes = SerializeDictMetadata(codesPtype, (uint)k, isNullableCodes: hasNulls);
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var childTickets = new[] { codesNodeTicket, valuesNodeTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, idx.Dict, metadataTicket, childTickets)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, idx.Dict, metadataTicket, childTickets, statsTicket.Value);
    }

    private static UInt8Array BuildCodesU8(int[] codes, ArrowBuffer validity, int nullCount)
    {
        var bytes = new byte[codes.Length];
        for (int i = 0; i < codes.Length; i++) bytes[i] = checked((byte)codes[i]);
        return new UInt8Array(new ArrowBuffer(bytes), validity, codes.Length, nullCount, 0);
    }

    private static UInt16Array BuildCodesU16(int[] codes, ArrowBuffer validity, int nullCount)
    {
        var bytes = new byte[codes.Length * 2];
        for (int i = 0; i < codes.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(i * 2, 2), checked((ushort)codes[i]));
        return new UInt16Array(new ArrowBuffer(bytes), validity, codes.Length, nullCount, 0);
    }

    private static UInt32Array BuildCodesU32(int[] codes, ArrowBuffer validity, int nullCount)
    {
        var bytes = new byte[codes.Length * 4];
        for (int i = 0; i < codes.Length; i++)
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(i * 4, 4), checked((uint)codes[i]));
        return new UInt32Array(new ArrowBuffer(bytes), validity, codes.Length, nullCount, 0);
    }

    private static StringArray BuildStringArray(List<string> values)
    {
        // offsets[k+1] + concatenated UTF-8 bytes; no validity (non-nullable).
        int total = 0;
        var encodedSegments = new byte[values.Count][];
        for (int i = 0; i < values.Count; i++)
        {
            encodedSegments[i] = System.Text.Encoding.UTF8.GetBytes(values[i]);
            total += encodedSegments[i].Length;
        }
        var offsetBytes = new byte[(values.Count + 1) * 4];
        var dataBytes = new byte[total];
        int pos = 0;
        for (int i = 0; i < values.Count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4, 4), pos);
            encodedSegments[i].CopyTo(dataBytes, pos);
            pos += encodedSegments[i].Length;
        }
        BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(values.Count * 4, 4), pos);

        return new StringArray(
            values.Count,
            new ArrowBuffer(offsetBytes),
            new ArrowBuffer(dataBytes),
            ArrowBuffer.Empty,
            nullCount: 0,
            offset: 0);
    }

    /// <summary>
    /// Inline DictMetadata proto bytes (per vortex-array's <c>arrays/dict/array.rs</c>):
    ///   field 1 (varint, u32): values_len
    ///   field 2 (varint, PType enum): codes_ptype
    ///   field 3 (varint, optional bool): is_nullable_codes — emitted as <c>true</c>
    ///     when the input column had any null rows; otherwise omitted (proto3
    ///     default = false).
    ///   field 4 (optional, all_values_referenced) is absent.
    /// </summary>
    private static byte[] SerializeDictMetadata(byte codesPtype, uint valuesLen, bool isNullableCodes)
    {
        // Worst case: 1 + 5 + 1 + 1 + 1 + 1 = 10 bytes.
        Span<byte> tmp = stackalloc byte[10];
        int pos = 0;
        tmp[pos++] = 0x08; // tag: field 1, wire-type 0 — values_len
        pos += Varint.WriteUnsigned(tmp.Slice(pos), valuesLen);
        tmp[pos++] = 0x10; // tag: field 2, wire-type 0 — codes_ptype
        tmp[pos++] = codesPtype;
        if (isNullableCodes)
        {
            tmp[pos++] = 0x18; // tag: field 3, wire-type 0 — is_nullable_codes
            tmp[pos++] = 1;
        }
        return tmp.Slice(0, pos).ToArray();
    }
}
