// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using Clast.Fsst;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.fsst</c>: FSST-compressed string arrays. Wire format
/// (per <c>encodings/fsst/src/array.rs</c>):
/// <list type="bullet">
///   <item>3 buffers: symbols (N×u64 packed), symbol_lengths (N×u8), compressed_codes_bytes</item>
///   <item>2-3 children: uncompressed_lengths (primitive int), codes_offsets (primitive int), optional codes_validity (bool)</item>
///   <item>metadata: protobuf <c>FSSTMetadata { uncompressed_lengths_ptype, codes_offsets_ptype }</c></item>
/// </list>
///
/// <para>Decoding strategy mirrors the upstream <c>canonicalize_fsst</c>: bulk-decompress
/// the entire <c>compressed_codes_bytes</c> in one shot, then build Arrow string offsets
/// from the per-row <c>uncompressed_lengths</c> child. FSST is a stateless byte-by-byte
/// expansion (each input byte either expands to a symbol or, if 0xFF, escapes the next
/// byte literally), so concatenated rows can be decompressed together without losing
/// row boundaries.</para>
/// </summary>
internal static class FsstArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not StringType and not BinaryType)
            throw new NotSupportedException(
                $"vortex.fsst decoder only supports StringType / BinaryType, got {expectedType}.");

        if (node.BufferRefCount != 3)
            throw new VortexFormatException(
                $"vortex.fsst expects 3 buffers, got {node.BufferRefCount}. " +
                "Legacy 2-buffer format is not supported (re-emit fixture with current vortex).");
        if (node.ChildCount < 2 || node.ChildCount > 3)
            throw new VortexFormatException(
                $"vortex.fsst expects 2 or 3 children, got {node.ChildCount}.");

        // Buffers.
        AssertNoBufferCompression(node, serialized, 0);
        AssertNoBufferCompression(node, serialized, 1);
        AssertNoBufferCompression(node, serialized, 2);

        var symbolsBuf = serialized.BufferBytes(node.BufferRef(0));
        var symbolLengthsBuf = serialized.BufferBytes(node.BufferRef(1));
        var compressedBuf = serialized.BufferBytes(node.BufferRef(2));

        var numSymbols = symbolLengthsBuf.Length;
        if (symbolsBuf.Length != numSymbols * 8)
            throw new VortexFormatException(
                $"vortex.fsst symbols buffer is {symbolsBuf.Length} bytes but symbol_lengths has " +
                $"{numSymbols} entries (expected {numSymbols * 8} bytes for 8-byte packed symbols).");

        // Force code 255 to length 0 — Clast.Fsst (cwida convention) reserves it as the escape code.
        var lengths = symbolLengthsBuf.ToArray();
        if (lengths.Length > 255) lengths[255] = 0;

        // Resolve uncompressed_lengths Arrow type from FSSTMetadata, then decode child[0].
        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException("vortex.fsst ArrayNode has empty metadata; expected FSSTMetadata proto.");
        var metaBytes = metaVec.RawBytes(metaVec.Length);
        var (uncompressedLensPtype, _codesOffsetsPtype) = ParseFsstMetadata(metaBytes);
        _ = _codesOffsetsPtype; // unused: we don't need codes_offsets for bulk decompress

        var uncompressedLensType = PtypeIntToArrowType(uncompressedLensPtype);
        var uncompressedLensArr = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, uncompressedLensType, expectedRowCount);

        // Validity (optional child[2]).
        ArrowBuffer nullBuffer;
        int nullCount;
        var rowCount = checked((int)expectedRowCount);
        if (node.ChildCount == 3)
        {
            // Per upstream: child[2] is the codes_validity (a vortex.bool array).
            // BoolArrayDecoder.ReadBitmap is exactly what we want.
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(2), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }
        else
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }

        // Bulk FSST decompress: pass the full compressed buffer as a single batch.
        // FSST has no inter-row state, so concatenated row bytes decompress correctly.
        var decoder = FsstDecoder.FromSymbols(lengths, symbolsBuf);
        int destCap = FsstDecoder.MaxDecompressedLength(compressedBuf.Length);
        var dest = new byte[destCap];
        var compressedLengths = new[] { compressedBuf.Length };
        var destinationOffsets = new int[2];
        if (!decoder.TryDecompressBatch(
                compressedBuf, compressedLengths,
                dest, destinationOffsets,
                out int totalWritten))
        {
            throw new VortexFormatException(
                $"vortex.fsst decompress failed " +
                $"(compressedBytes={compressedBuf.Length}, destCap={destCap}, rows={rowCount}).");
        }

        // Compact dest if it over-allocated.
        var arrowData = totalWritten == dest.Length ? dest : dest.AsSpan(0, totalWritten).ToArray();

        // Build Arrow offsets from prefix sum of uncompressed_lengths.
        var offsetBytes = new byte[(rowCount + 1) * 4];
        int cumulative = 0;
        for (int i = 0; i < rowCount; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4), cumulative);
            cumulative += GetIntAtIndex(uncompressedLensArr, i);
        }
        BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(rowCount * 4), cumulative);

        if (cumulative != totalWritten)
            throw new VortexFormatException(
                $"vortex.fsst: sum of uncompressed_lengths is {cumulative} but FSST wrote {totalWritten} bytes.");

        var offsetsArrowBuf = new ArrowBuffer(offsetBytes);
        var valuesArrowBuf = new ArrowBuffer(arrowData);
        // FSST trains on raw bytes — its symbol substitution is dtype-agnostic,
        // so the decompressed output is valid for either Utf8 or Binary
        // semantically (the writer's input determines the dtype).
        return expectedType is StringType
            ? new StringArray(rowCount, offsetsArrowBuf, valuesArrowBuf, nullBuffer, nullCount, offset: 0)
            : new BinaryArray(BinaryType.Default, rowCount, offsetsArrowBuf, valuesArrowBuf, nullBuffer, nullCount, offset: 0);
    }

    private static void AssertNoBufferCompression(
        ArrayNode node, SerializedArray serialized, int slot)
    {
        var desc = serialized.Message.Buffer(node.BufferRef(slot));
        if (desc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.fsst buffer {slot} compression {desc.Compression} not yet implemented.");
    }

    /// <summary>
    /// Parses <c>FSSTMetadata</c> proto:
    ///   field 1 (varint): uncompressed_lengths_ptype (PType enum)
    ///   field 2 (varint): codes_offsets_ptype (PType enum)
    /// </summary>
    private static (int UncompressedLensPtype, int CodesOffsetsPtype) ParseFsstMetadata(ReadOnlySpan<byte> bytes)
    {
        int? uncomp = null, codes = null;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                uncomp = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                codes = (int)Varint.ReadUnsigned(bytes, ref pos);
            else SkipField(bytes, ref pos, wireType);
        }
        // Proto3 default for enum fields is 0 (= PType.U8). Missing fields are
        // not serialized, so we treat absence as U8.
        return (uncomp ?? 0, codes ?? 0);
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
                    $"Unsupported protobuf wire type {wireType} in FSSTMetadata.");
        }
    }

    /// <summary>PType enum values from dtype.fbs: U8=0, U16=1, U32=2, U64=3, I8=4, I16=5, I32=6, I64=7.</summary>
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
        _ => throw new VortexFormatException(
            $"vortex.fsst: ptype {ptype} is not a supported integer type."),
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
            $"vortex.fsst uncompressed_lengths array type {array.GetType().Name} not supported."),
    };
}
