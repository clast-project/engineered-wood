// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Layouts;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for the <c>vortex.dict</c> ARRAY encoding (distinct from the
/// layout-level <c>vortex.dict</c> we already handle in the planner). Same
/// reconstruction semantics: <c>result[i] = values[codes[i]]</c>, but applied
/// at array-decode time inside a single segment.
///
/// <para>Wire format: 0 buffers, 2 children (codes, values).
/// Metadata <c>DictMetadata { values_len: u32 (tag 1), codes_ptype: PType (tag 2),
/// is_nullable_codes: optional bool (tag 3), all_values_referenced: optional bool (tag 4) }</c>.</para>
/// </summary>
internal static class DictArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.dict (array) expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount != 2)
            throw new VortexFormatException(
                $"vortex.dict (array) expects 2 children (codes, values), got {node.ChildCount}.");

        var metaVec = node.Metadata;
        var (codesPtype, valuesLen) = ParseDictMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));

        var codesArrowType = PtypeIntToArrowType(codesPtype);
        var codes = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, codesArrowType, expectedRowCount);
        var values = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs, expectedType, (long)valuesLen);

        return DictReconstructor.Reconstruct(expectedType, values, codes);
    }

    /// <summary>
    /// Parses <c>DictMetadata</c> per vortex-array's <c>arrays/dict/array.rs</c>:
    ///   field 1 (varint, u32): values_len
    ///   field 2 (varint, PType enum): codes_ptype
    ///   field 3 (varint, optional bool): is_nullable_codes
    ///   field 4 (varint, optional bool): all_values_referenced
    /// </summary>
    private static (int CodesPtype, ulong ValuesLen) ParseDictMetadata(ReadOnlySpan<byte> bytes)
    {
        int codesPtype = 0;
        ulong valuesLen = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                valuesLen = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                codesPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else
            {
                switch (wireType)
                {
                    case 0: Varint.ReadUnsigned(bytes, ref pos); break;
                    case 1: pos += 8; break;
                    case 2:
                        var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                        pos += len; break;
                    case 5: pos += 4; break;
                    default:
                        throw new VortexFormatException(
                            $"Unsupported wire type {wireType} in DictMetadata.");
                }
            }
        }
        return (codesPtype, valuesLen);
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
        _ => throw new VortexFormatException($"Unsupported ptype {ptype} in DictMetadata."),
    };
}
