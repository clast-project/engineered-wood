// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.MapArrayDecoder"/>: emits a
/// <c>vortex.map</c> ArrayNode, which joined the <c>core2026.08.2</c> edition (vortex 0.85).
///
/// <para>Wire shape: the map itself has 0 buffers and empty metadata, and exactly one child: a
/// <c>vortex.listview</c> of the non-nullable <c>{key, value}</c> entry struct, which carries the
/// map's validity. Upstream refuses any other encoding for that child, so the entries cannot reuse
/// <see cref="ListArrayEncoder"/>'s <c>vortex.list</c>.</para>
///
/// <para>The list-view is written from Arrow's contiguous offsets: row <c>i</c>'s view is
/// <c>offsets[i] - offsets[0]</c> with size <c>offsets[i + 1] - offsets[i]</c>, both i32, and the
/// entries child sees only the visible range, as <see cref="ListArrayEncoder"/> does.</para>
/// </summary>
internal static class MapArrayEncoder
{
    private const byte PtypeI32 = 6;

    public static int Emit(SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is not MapArray map)
            throw new NotSupportedException(
                $"vortex.map writer requires Apache.Arrow.MapArray, got {array.GetType().Name}.");

        int entriesTicket = EmitEntries(sb, map, idx);
        var children = new[] { entriesTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithChildrenOnly(sb.Builder, idx.Map, children)
            : ArrayNodeEmitter.EmitWithChildrenAndStats(sb.Builder, idx.Map, children, statsTicket.Value);
    }

    /// <summary>Emits the <c>vortex.listview</c> of entry structs.</summary>
    private static int EmitEntries(SegmentBuilder sb, MapArray map, EncodingIndices idx)
    {
        var data = map.Data;
        int rowCount = map.Length;

        // An empty array may carry an empty offsets buffer rather than a single 0.
        var offsetsAll = data.Buffers[1].Span;
        int offsetsStart = data.Offset * 4;
        int offsetsByteLen = rowCount == 0 ? 0 : (rowCount + 1) * 4;
        if (offsetsAll.Length < offsetsStart + offsetsByteLen)
            throw new InvalidOperationException(
                $"MapArray offsets buffer is {offsetsAll.Length} bytes; need {offsetsStart + offsetsByteLen}.");
        var visibleOffsets = offsetsAll.Slice(offsetsStart, offsetsByteLen);

        int firstOffset = rowCount == 0 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets);
        int lastOffset = rowCount == 0 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets.Slice(rowCount * 4, 4));
        int visibleEntriesLen = lastOffset - firstOffset;

        var offsetBytes = new byte[rowCount * 4];
        var sizeBytes = new byte[rowCount * 4];
        for (int i = 0; i < rowCount; i++)
        {
            int start = BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets.Slice(i * 4, 4));
            int end = BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets.Slice((i + 1) * 4, 4));
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4, 4), start - firstOffset);
            BinaryPrimitives.WriteInt32LittleEndian(sizeBytes.AsSpan(i * 4, 4), end - start);
        }

        // KeyValues, not Values: MapArray hides ListArray.Values with the value column alone.
        var entries = (StructArray)((Apache.Arrow.Array)map.KeyValues).Slice(firstOffset, visibleEntriesLen);
        // Vortex's entry struct is non-nullable, and so is Arrow's; a null entry has no encoding.
        if (entries.Data.GetNullCount() > 0)
            throw new NotSupportedException(
                "vortex.map entries cannot be null; the MapArray's entries struct has null slots.");
        // The key dtype is always non-nullable, but Apache.Arrow's key builder still accepts a
        // null. Written under that dtype, vortex refuses the file ("incorrect validity ... for
        // dtype utf8") while EW's own reader would hand the null key back, so refuse it here.
        // Only the visible entries count: a slice may leave a null key behind.
        if (entries.Fields[0].Data.GetNullCount() > 0)
            throw new NotSupportedException(
                "vortex.map keys cannot be null; the MapArray has a null key among the entries being written.");
        int elementsTicket = ArrayEncoderDispatch.Emit(sb, entries, idx);

        ushort offsetsBufIdx = sb.AddBuffer(offsetBytes, alignmentExponent: 2);
        int offsetsTicket = ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, idx.Primitive, offsetsBufIdx);
        ushort sizesBufIdx = sb.AddBuffer(sizeBytes, alignmentExponent: 2);
        int sizesTicket = ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, idx.Primitive, sizesBufIdx);

        int? validityTicket = null;
        if (data.GetNullCount() > 0)
        {
            var bitmapBytes = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: rowCount);
            ushort bitmapBufIdx = sb.AddBuffer(bitmapBytes, 0);
            validityTicket = ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, idx.Bool, bitmapBufIdx);
        }

        var childTickets = validityTicket is null
            ? new[] { elementsTicket, offsetsTicket, sizesTicket }
            : new[] { elementsTicket, offsetsTicket, sizesTicket, validityTicket.Value };

        var metadataTicket = sb.Builder.WriteByteVector(
            SerializeListViewMetadata((ulong)visibleEntriesLen, PtypeI32, PtypeI32));
        return ArrayNodeEmitter.EmitWithMetadataAndChildren(
            sb.Builder, idx.ListView, metadataTicket, childTickets);
    }

    /// <summary>
    /// Inline ListViewMetadata proto bytes:
    ///   field 1 (varint, u64):  elements_len
    ///   field 2 (varint, enum): offset_ptype
    ///   field 3 (varint, enum): size_ptype
    /// </summary>
    private static byte[] SerializeListViewMetadata(ulong elementsLen, byte offsetPtype, byte sizePtype)
    {
        // Worst case: 1-byte tag + 10-byte u64 varint + two (1-byte tag + 1-byte value) = 15.
        Span<byte> tmp = stackalloc byte[15];
        int pos = 0;
        tmp[pos++] = 0x08; // tag: field 1, wire-type 0 (varint)
        pos += Varint.WriteUnsigned(tmp.Slice(pos), elementsLen);
        tmp[pos++] = 0x10; // tag: field 2, wire-type 0 (varint)
        pos += Varint.WriteUnsigned(tmp.Slice(pos), offsetPtype);
        tmp[pos++] = 0x18; // tag: field 3, wire-type 0 (varint)
        pos += Varint.WriteUnsigned(tmp.Slice(pos), sizePtype);
        return tmp.Slice(0, pos).ToArray();
    }
}
