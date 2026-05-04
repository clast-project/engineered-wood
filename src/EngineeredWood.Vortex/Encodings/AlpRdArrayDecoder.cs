// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.alprd</c> ("real doubles"). Splits each float's bit
/// pattern into left (high bits, dictionary-encoded) and right (low bits, raw)
/// parts. Reconstructs:
/// <c>bits[i] = (dict[left_parts[i]] &lt;&lt; right_bit_width) | right_parts[i]</c>,
/// then reinterprets as the float type.
///
/// <para>Wire format (per <c>encodings/alp/src/alp_rd/array.rs</c>):
/// <list type="bullet">
///   <item>0 buffers</item>
///   <item>2-4 children: left_parts (codes, ptype from metadata),
///     right_parts (u32 for f32 / u64 for f64), optional patch indices, optional patch values</item>
///   <item>Metadata <c>ALPRDMetadata { right_bit_width, dict_len, dict[u32 packed], left_parts_ptype, patches }</c></item>
/// </list></para>
///
/// <para>Scope: f32 + f64. right_parts is u32 for FloatType, u64 for DoubleType;
/// the dictionary stores u16 left patterns either way (CUT_LIMIT=16 caps the
/// left part width). Other dtypes throw <see cref="NotSupportedException"/>.</para>
/// </summary>
internal static class AlpRdArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not (FloatType or DoubleType))
            throw new NotSupportedException(
                $"vortex.alprd supports FloatType / DoubleType, got {expectedType}.");

        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.alprd expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount < 2)
            throw new VortexFormatException(
                $"vortex.alprd expects at least 2 children, got {node.ChildCount}.");

        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException("vortex.alprd ArrayNode has empty metadata.");
        var meta = ParseAlpRdMetadata(metaVec.RawBytes(metaVec.Length));

        var leftPartsType = PtypeIntToArrowType(meta.LeftPartsPtype);
        var leftParts = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, leftPartsType, expectedRowCount);

        // right_parts width is determined by the parent dtype: u32 for f32, u64 for f64.
        bool isF32 = expectedType is FloatType;
        var rightPartsType = isF32 ? (IArrowType)UInt32Type.Default : UInt64Type.Default;
        var rightParts = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs, rightPartsType, expectedRowCount);

        // Patch path (overwrites some left_parts with raw left bit patterns).
        IArrowArray? patchIndices = null;
        IArrowArray? patchValues = null;
        if (meta.HasPatches)
        {
            if (node.ChildCount < 4)
                throw new VortexFormatException(
                    $"vortex.alprd with patches requires 4 children, got {node.ChildCount}.");
            var indicesType = PtypeIntToArrowType(meta.PatchIndicesPtype);
            patchIndices = ArrayDecoder.DecodeNode(
                node.Child(2), serialized, arraySpecs, indicesType, (long)meta.PatchesLen);
            // Patch values are direct left bit patterns of the same ptype as left_parts.
            patchValues = ArrayDecoder.DecodeNode(
                node.Child(3), serialized, arraySpecs, leftPartsType, (long)meta.PatchesLen);
        }

        var rowCount = checked((int)expectedRowCount);
        var resolvedLeft = ResolveLeftBits(leftParts, meta.Dict, rowCount);
        if (patchIndices is not null && patchValues is not null)
            ApplyPatches(resolvedLeft, patchIndices, patchValues, (int)meta.PatchesOffset);

        // Combine left << shift | right → float bit pattern.
        var shift = meta.RightBitWidth;
        if (isF32)
        {
            var bytes = new byte[(long)rowCount * sizeof(float)];
            var floats = MemoryMarshal.Cast<byte, float>(bytes.AsSpan());
            var rightArr = (UInt32Array)rightParts;
            for (int i = 0; i < rowCount; i++)
            {
                uint leftBits = (uint)resolvedLeft[i];
                uint combined = (leftBits << shift) | rightArr.GetValue(i)!.Value;
                floats[i] = Int32BitsToSingle(unchecked((int)combined));
            }
            return new FloatArray(new ArrowBuffer(bytes), ArrowBuffer.Empty, rowCount, 0, 0);
        }
        else
        {
            var bytes = new byte[(long)rowCount * sizeof(double)];
            var doubles = MemoryMarshal.Cast<byte, double>(bytes.AsSpan());
            var rightArr = (UInt64Array)rightParts;
            for (int i = 0; i < rowCount; i++)
            {
                ulong leftBits = resolvedLeft[i];
                ulong combined = (leftBits << shift) | rightArr.GetValue(i)!.Value;
                doubles[i] = BitConverter.Int64BitsToDouble(unchecked((long)combined));
            }
            return new DoubleArray(new ArrowBuffer(bytes), ArrowBuffer.Empty, rowCount, 0, 0);
        }
    }

    /// <summary>
    /// Materializes the per-row left bit patterns: for each row, look up
    /// <c>dict[left_parts[i]]</c>. Patch values overwrite specific positions
    /// later via <see cref="ApplyPatches"/>.
    /// </summary>
    private static ulong[] ResolveLeftBits(IArrowArray leftParts, ushort[] dict, int rowCount)
    {
        var result = new ulong[rowCount];
        switch (leftParts)
        {
            case UInt8Array u8:
                for (int i = 0; i < rowCount; i++) result[i] = dict[u8.GetValue(i)!.Value];
                break;
            case UInt16Array u16:
                for (int i = 0; i < rowCount; i++) result[i] = dict[u16.GetValue(i)!.Value];
                break;
            case UInt32Array u32:
                for (int i = 0; i < rowCount; i++) result[i] = dict[checked((int)u32.GetValue(i)!.Value)];
                break;
            default:
                throw new NotSupportedException(
                    $"vortex.alprd: unexpected left_parts type {leftParts.GetType().Name}.");
        }
        return result;
    }

    /// <summary>
    /// Overwrites <c>resolved[indices[k] - patchesOffset] = values[k]</c>. Patch
    /// values are raw left bit patterns (NOT dict codes), so they replace the
    /// dictionary-decoded high bits at the indicated positions.
    /// </summary>
    private static void ApplyPatches(
        ulong[] resolved, IArrowArray indices, IArrowArray values, int patchesOffset)
    {
        int patchCount = indices.Length;
        for (int k = 0; k < patchCount; k++)
        {
            int rowIdx = GetIntAtIndex(indices, k) - patchesOffset;
            if ((uint)rowIdx >= (uint)resolved.Length)
                throw new VortexFormatException(
                    $"vortex.alprd patch indices[{k}]={rowIdx} out of range [0, {resolved.Length}).");
            resolved[rowIdx] = (ulong)GetIntAtIndex(values, k);
        }
    }

    internal readonly struct AlpRdMeta
    {
        public int RightBitWidth { get; init; }
        public ushort[] Dict { get; init; }
        public int LeftPartsPtype { get; init; }
        public bool HasPatches { get; init; }
        public ulong PatchesLen { get; init; }
        public ulong PatchesOffset { get; init; }
        public int PatchIndicesPtype { get; init; }
    }

    /// <summary>
    /// Parses <c>ALPRDMetadata</c>:
    ///   field 1 (varint): right_bit_width
    ///   field 2 (varint): dict_len
    ///   field 3 (length-delim, packed varints): dict — array of u32 codes
    ///   field 4 (varint): left_parts_ptype (PType enum)
    ///   field 5 (length-delim, optional): patches (PatchesMetadata)
    /// </summary>
    private static AlpRdMeta ParseAlpRdMetadata(ReadOnlySpan<byte> bytes)
    {
        int rightBitWidth = 0, dictLen = 0, leftPtype = 0;
        ushort[] dict = System.Array.Empty<ushort>();
        bool hasPatches = false;
        ulong patchesLen = 0, patchesOffset = 0;
        int patchIndicesPtype = 2; // default U32

        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                rightBitWidth = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                dictLen = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 2)
            {
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                var dictBytes = bytes.Slice(pos, len);
                pos += len;
                // Packed varint vector.
                var list = new List<ushort>();
                int dpos = 0;
                while (dpos < dictBytes.Length)
                    list.Add(checked((ushort)Varint.ReadUnsigned(dictBytes, ref dpos)));
                dict = list.ToArray();
            }
            else if (fieldNum == 4 && wireType == 0)
                leftPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 5 && wireType == 2)
            {
                hasPatches = true;
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                var patchBytes = bytes.Slice(pos, len);
                pos += len;
                ParsePatchesMetadata(patchBytes, out patchesLen, out patchesOffset, out patchIndicesPtype);
            }
            else SkipField(bytes, ref pos, wireType);
        }

        if (dict.Length != dictLen && dict.Length > 0)
        {
            // dict_len may be the explicitly-stored count; the packed vector encodes the same number.
            // Truncate or extend defensively.
            if (dict.Length > dictLen) System.Array.Resize(ref dict, dictLen);
        }

        return new AlpRdMeta
        {
            RightBitWidth = rightBitWidth,
            Dict = dict,
            LeftPartsPtype = leftPtype,
            HasPatches = hasPatches,
            PatchesLen = patchesLen,
            PatchesOffset = patchesOffset,
            PatchIndicesPtype = patchIndicesPtype,
        };
    }

    private static void ParsePatchesMetadata(
        ReadOnlySpan<byte> bytes, out ulong len, out ulong offset, out int indicesPtype)
    {
        len = 0; offset = 0; indicesPtype = 2; // proto3 default = U32 (PType=2)
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                len = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                offset = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 0)
                indicesPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else SkipField(bytes, ref pos, wireType);
        }
    }

    private static void SkipField(ReadOnlySpan<byte> bytes, ref int pos, uint wireType)
    {
        switch (wireType)
        {
            case 0: Varint.ReadUnsigned(bytes, ref pos); break;
            case 1: pos += 8; break;
            case 2:
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                pos += len;
                break;
            case 5: pos += 4; break;
            default:
                throw new VortexFormatException(
                    $"Unsupported protobuf wire type {wireType} in ALPRDMetadata.");
        }
    }

    /// <summary>
    /// <c>BitConverter.Int32BitsToSingle</c> is .NET 6+; on netstandard2.0
    /// we reinterpret via the 4-byte buffer round-trip.
    /// </summary>
    private static float Int32BitsToSingle(int bits)
    {
#if NET6_0_OR_GREATER
        return BitConverter.Int32BitsToSingle(bits);
#else
        return BitConverter.ToSingle(BitConverter.GetBytes(bits), 0);
#endif
    }

    private static IArrowType PtypeIntToArrowType(int ptype) => ptype switch
    {
        0 => UInt8Type.Default,
        1 => UInt16Type.Default,
        2 => UInt32Type.Default,
        3 => UInt64Type.Default,
        4 => Int8Type.Default,
        5 => Int16Type.Default,
        6 => Int32Type.Default,
        7 => Int64Type.Default,
        _ => throw new VortexFormatException($"Unsupported ptype {ptype} in ALPRDMetadata."),
    };

    private static int GetIntAtIndex(IArrowArray array, int i) => array switch
    {
        UInt8Array u8 => u8.GetValue(i)!.Value,
        UInt16Array u16 => u16.GetValue(i)!.Value,
        UInt32Array u32 => checked((int)u32.GetValue(i)!.Value),
        UInt64Array u64 => checked((int)u64.GetValue(i)!.Value),
        Int8Array i8 => i8.GetValue(i)!.Value,
        Int16Array i16 => i16.GetValue(i)!.Value,
        Int32Array i32 => i32.GetValue(i)!.Value,
        Int64Array i64 => checked((int)i64.GetValue(i)!.Value),
        _ => throw new VortexFormatException(
            $"vortex.alprd: int array type {array.GetType().Name} not supported."),
    };
}
