// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.onpair</c>: OnPair short-string compression (vortex 0.84+, edition
/// <c>core2026.08.1</c>), which the default compressor picks for strings when it beats FSST.
/// Wire format (per <c>encodings/onpair/src/array.rs</c>):
/// <list type="bullet">
///   <item>1 buffer: <c>dict_bytes</c>, the dictionary tokens concatenated, followed by read padding</item>
///   <item>4-5 children, each a primitive integer array of the ptype the metadata names:
///     <c>dict_offsets</c> (<c>dict_size + 1</c>), <c>codes</c> (<c>codes_len</c>),
///     <c>codes_offsets</c> (rows + 1), <c>uncompressed_lengths</c> (rows), and an optional
///     validity (bool)</item>
///   <item>metadata: protobuf <c>OnPairMetadata { 1: uncompressed_lengths_ptype, 3: dict_size,
///     4: codes_len, 5: dict_offsets_ptype, 6: codes_ptype, 7: codes_offsets_ptype }</c></item>
/// </list>
///
/// <para>The dictionary is Arrow-binary shaped: token <c>t</c> is
/// <c>dict_bytes[dict_offsets[t]..dict_offsets[t + 1]]</c>, 1 to 16 bytes long, with at most
/// 2^16 tokens so every code fits a u16 (the <c>onpair</c> crate's <c>CompactDictionary</c>).
/// A row is the concatenation of its codes' tokens. As upstream's <c>canonicalize_onpair</c>
/// does, the code window <c>codes[codes_offsets[0]..codes_offsets[rows]]</c> is decoded in
/// order (a sliced array keeps the full <c>codes</c> child and narrows only
/// <c>codes_offsets</c>). Each row is the run of codes between its two
/// <c>codes_offsets</c>, and must decode to its <c>uncompressed_lengths</c> entry, which is
/// zero for null rows.</para>
/// </summary>
internal static class OnPairArrayDecoder
{
    /// <summary>The longest token the dictionary may hold (<c>onpair::MAX_TOKEN_SIZE</c>).</summary>
    private const int MaxTokenSize = 16;

    /// <summary>The most tokens a dictionary may hold, so every code fits a u16.</summary>
    private const int MaxTokens = 1 << 16;

    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not StringType and not BinaryType)
            throw new NotSupportedException(
                $"vortex.onpair decoder only supports StringType / BinaryType, got {expectedType}.");
        if (node.BufferRefCount != 1)
            throw new VortexFormatException(
                $"vortex.onpair expects 1 buffer (dict_bytes), got {node.BufferRefCount}.");
        if (node.ChildCount is not 4 and not 5)
            throw new VortexFormatException(
                $"vortex.onpair expects 4 or 5 children, got {node.ChildCount}.");

        var bufferDesc = serialized.Message.Buffer(node.BufferRef(0));
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.onpair buffer compression {bufferDesc.Compression} not yet implemented.");
        var dictBytes = serialized.BufferBytes(node.BufferRef(0));

        var metaVec = node.Metadata;
        var meta = ParseMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));

        int rowCount = checked((int)expectedRowCount);
        // Bounded before anything is allocated from it.
        if (meta.DictSize > MaxTokens)
            throw new VortexFormatException(
                $"vortex.onpair dictionary has {meta.DictSize} tokens; at most {MaxTokens} are addressable.");
        if (meta.CodesLen > int.MaxValue)
            throw new NotSupportedException(
                $"vortex.onpair codes_len {meta.CodesLen} exceeds what this reader can index.");

        var dictOffsets = ToInt64s(
            DecodeChild(node, serialized, arraySpecs, 0, meta.DictOffsetsPtype, (long)meta.DictSize + 1, "dict_offsets"));
        var codes = DecodeChild(node, serialized, arraySpecs, 1, meta.CodesPtype, (long)meta.CodesLen, "codes");
        var codesOffsets = ToInt64s(
            DecodeChild(node, serialized, arraySpecs, 2, meta.CodesOffsetsPtype, (long)rowCount + 1, "codes_offsets"));
        var lengths = ToInt64s(
            DecodeChild(node, serialized, arraySpecs, 3, meta.UncompressedLengthsPtype, rowCount, "uncompressed_lengths"));

        ValidateDictionary(dictOffsets, dictBytes.Length);
        int numTokens = dictOffsets.Length - 1;

        // Arrow offsets from the per-row decoded lengths.
        var offsetBytes = new byte[((long)rowCount + 1) * 4];
        long total = 0;
        for (int i = 0; i < rowCount; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4), (int)total);
            if (lengths[i] < 0)
                throw new VortexFormatException($"vortex.onpair uncompressed length {lengths[i]} at row {i} is negative.");
            total += lengths[i];
            if (total > int.MaxValue)
                throw new NotSupportedException(
                    "vortex.onpair column decodes to more than 2 GiB, which a StringArray / BinaryArray cannot hold.");
        }
        BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(rowCount * 4), (int)total);

        long codeStart = codesOffsets[0];
        long codeEnd = codesOffsets[rowCount];
        if (codeStart < 0 || codeStart > codeEnd || codeEnd > codes.Length)
            throw new VortexFormatException(
                $"vortex.onpair code window [{codeStart}, {codeEnd}) is not within the {codes.Length} codes.");
        var window = CodeWindow(codes, (int)codeStart, (int)codeEnd, numTokens);

        // Row by row, so every boundary is checked: nondecreasing and within the
        // window, and decoding to exactly the row's recorded length.
        var values = new byte[total];
        int written = 0;
        for (int i = 0; i < rowCount; i++)
        {
            long runStart = codesOffsets[i], runEnd = codesOffsets[i + 1];
            if (runEnd < runStart || runEnd > codeEnd)
                throw new VortexFormatException(
                    $"vortex.onpair codes_offsets at row {i} ({runStart} to {runEnd}) decrease or pass the window end {codeEnd}.");
            int rowEnd = checked(written + (int)lengths[i]);
            for (long c = runStart; c < runEnd; c++)
            {
                var code = window[c - codeStart];
                int begin = (int)dictOffsets[code];
                int len = (int)dictOffsets[code + 1] - begin;
                if (written + len > rowEnd)
                    throw new VortexFormatException(
                        $"vortex.onpair row {i} decodes to more than its {lengths[i]} recorded bytes.");
                dictBytes.Slice(begin, len).CopyTo(values.AsSpan(written));
                written += len;
            }
            if (written != rowEnd)
                throw new VortexFormatException(
                    $"vortex.onpair row {i} decodes to {lengths[i] - (rowEnd - written)} bytes but records {lengths[i]}.");
        }

        ArrowBuffer nullBuffer;
        int nullCount;
        if (node.ChildCount == 5)
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(4), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }
        else
        {
            nullBuffer = ArrowBuffer.Empty;
            nullCount = 0;
        }

        var offsetsBuf = new ArrowBuffer(offsetBytes);
        var valuesBuf = new ArrowBuffer(values);
        return expectedType is StringType
            ? new StringArray(rowCount, offsetsBuf, valuesBuf, nullBuffer, nullCount, offset: 0)
            : new BinaryArray(BinaryType.Default, rowCount, offsetsBuf, valuesBuf, nullBuffer, nullCount, offset: 0);
    }

    /// <summary>
    /// The structural checks of <c>onpair::CompactDictionary::validate_safety</c>, minus the read
    /// padding, which only its over-copying decoder needs: at least one and at most 2^16 tokens,
    /// offsets starting at 0 and strictly increasing by at most 16, within <c>dict_bytes</c>.
    /// </summary>
    private static void ValidateDictionary(long[] offsets, int dictBytesLength)
    {
        int numTokens = offsets.Length - 1;
        if (numTokens < 1)
            throw new VortexFormatException("vortex.onpair dictionary is empty.");
        if (numTokens > MaxTokens)
            throw new VortexFormatException(
                $"vortex.onpair dictionary has {numTokens} tokens; at most {MaxTokens} are addressable.");
        if (offsets[0] != 0)
            throw new VortexFormatException($"vortex.onpair dict_offsets starts at {offsets[0]}, not 0.");
        for (int t = 0; t < numTokens; t++)
        {
            long len = offsets[t + 1] - offsets[t];
            if (len < 1 || len > MaxTokenSize)
                throw new VortexFormatException(
                    $"vortex.onpair token {t} is {len} bytes long; tokens are 1 to {MaxTokenSize} bytes.");
        }
        if (offsets[numTokens] > dictBytesLength)
            throw new VortexFormatException(
                $"vortex.onpair dict_offsets end {offsets[numTokens]} exceeds dict_bytes length {dictBytesLength}.");
    }

    private static IArrowArray DecodeChild(
        ArrayNode node, SerializedArray serialized, IReadOnlyList<string> arraySpecs,
        int child, int ptype, long length, string name)
    {
        var array = ArrayDecoder.DecodeNode(
            node.Child(child), serialized, arraySpecs, PtypeIntToArrowType(ptype, name), length);
        if (array.Length != length)
            throw new VortexFormatException(
                $"vortex.onpair {name} has {array.Length} values, expected {length}.");
        if (array.NullCount != 0)
            throw new VortexFormatException($"vortex.onpair {name} must not contain nulls.");
        return array;
    }

    private static long[] ToInt64s(IArrowArray array)
    {
        var result = new long[array.Length];
        for (int i = 0; i < result.Length; i++)
            result[i] = ValueAt(array, i);
        return result;
    }

    /// <summary>
    /// Codes <c>[start, end)</c> as token ids, each checked against the dictionary. Only the
    /// window is copied, at two bytes a code; the child decoders materialize a child whole, so
    /// the full <c>codes</c> array has already been decoded.
    /// </summary>
    private static ushort[] CodeWindow(IArrowArray codes, int start, int end, int numTokens)
    {
        var window = new ushort[end - start];
        switch (codes)
        {
            case UInt16Array u16:
                {
                    var src = u16.Values.Slice(start, end - start);
                    for (int i = 0; i < src.Length; i++)
                        window[i] = CheckCode(src[i], start + i, numTokens);
                    break;
                }
            case UInt8Array u8:
                {
                    var src = u8.Values.Slice(start, end - start);
                    for (int i = 0; i < src.Length; i++)
                        window[i] = CheckCode(src[i], start + i, numTokens);
                    break;
                }
            default:
                for (int i = 0; i < window.Length; i++)
                    window[i] = CheckCode(ValueAt(codes, start + i), start + i, numTokens);
                break;
        }
        return window;
    }

    private static ushort CheckCode(long code, long position, int numTokens) =>
        (ulong)code < (ulong)numTokens
            ? (ushort)code
            : throw new VortexFormatException(
                $"vortex.onpair code {code} at position {position} is out of range (dictionary has {numTokens} tokens).");

    private static long ValueAt(IArrowArray array, int i) => array switch
    {
        UInt8Array u8 => u8.GetValue(i)!.Value,
        UInt16Array u16 => u16.GetValue(i)!.Value,
        UInt32Array u32 => u32.GetValue(i)!.Value,
        UInt64Array u64 => u64.GetValue(i)!.Value <= long.MaxValue
            ? (long)u64.GetValue(i)!.Value
            : throw new VortexFormatException($"vortex.onpair value {u64.GetValue(i)} does not fit a signed 64-bit integer."),
        Int8Array i8 => i8.GetValue(i)!.Value,
        Int16Array i16 => i16.GetValue(i)!.Value,
        Int32Array i32 => i32.GetValue(i)!.Value,
        Int64Array i64 => i64.GetValue(i)!.Value,
        _ => throw new VortexFormatException(
            $"vortex.onpair child decoded to unsupported array type {array.GetType().Name}."),
    };

    private readonly record struct Metadata(
        int UncompressedLengthsPtype,
        uint DictSize,
        ulong CodesLen,
        int DictOffsetsPtype,
        int CodesPtype,
        int CodesOffsetsPtype);

    /// <summary>
    /// Parses <c>OnPairMetadata</c>. Absent fields hold their proto3 defaults: 0, which for the
    /// ptype enums is U8.
    /// </summary>
    private static Metadata ParseMetadata(ReadOnlySpan<byte> bytes)
    {
        int uncompressedLengths = 0, dictOffsets = 0, codes = 0, codesOffsets = 0;
        uint dictSize = 0;
        ulong codesLen = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (wireType == 0)
            {
                var value = (ulong)Varint.ReadUnsigned(bytes, ref pos);
                switch (fieldNum)
                {
                    case 1: uncompressedLengths = checked((int)value); break;
                    case 3: dictSize = checked((uint)value); break;
                    case 4: codesLen = value; break;
                    case 5: dictOffsets = checked((int)value); break;
                    case 6: codes = checked((int)value); break;
                    case 7: codesOffsets = checked((int)value); break;
                }
                continue;
            }
            switch (wireType)
            {
                case 1: pos += 8; break;
                case 2: pos += checked((int)Varint.ReadUnsigned(bytes, ref pos)); break;
                case 5: pos += 4; break;
                default:
                    throw new VortexFormatException(
                        $"Unsupported protobuf wire type {wireType} in OnPairMetadata.");
            }
        }
        return new Metadata(uncompressedLengths, dictSize, codesLen, dictOffsets, codes, codesOffsets);
    }

    /// <summary>PType enum values from dtype.fbs: U8=0, U16=1, U32=2, U64=3, I8=4, I16=5, I32=6, I64=7.</summary>
    private static IArrowType PtypeIntToArrowType(int ptype, string name) => ptype switch
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
            $"vortex.onpair {name} ptype {ptype} is not an integer type."),
    };
}
