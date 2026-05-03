// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.ListArrayDecoder"/>:
/// emits a <c>vortex.list</c> ArrayNode subtree. Element child is encoded
/// recursively via <see cref="ArrayEncoderDispatch"/>; offsets are an i32
/// vortex.primitive child.
///
/// <para>Wire shape: 0 buffers on the list itself; children = [elements,
/// offsets, validity?]; metadata = ListMetadata { elements_len, offset_ptype:I32 }.
/// Visible offsets are rebased so output offsets[0] = 0 and the elements
/// child sees only the visible range.</para>
/// </summary>
internal static class ListArrayEncoder
{
    private const byte OffsetsPtypeI32 = 6;

    public static int Emit(SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is not ListArray listArr)
            throw new NotSupportedException(
                $"vortex.list writer requires Apache.Arrow.ListArray, got {array.GetType().Name}.");

        var data = listArr.Data;
        int rowCount = listArr.Length;

        // Read sliced offsets. Buffers[1] = i32 offsets; visible window starts
        // at data.Offset entries in. Rebase so output offsets[0] = 0, and slice
        // the elements array to the corresponding [firstOffset, lastOffset)
        // range.
        var offsetsAll = data.Buffers[1].Span;
        int offsetsStart = data.Offset * 4;
        int offsetsByteLen = (rowCount + 1) * 4;
        if (offsetsAll.Length < offsetsStart + offsetsByteLen)
            throw new InvalidOperationException(
                $"ListArray offsets buffer is {offsetsAll.Length} bytes; need {offsetsStart + offsetsByteLen}.");
        var visibleOffsets = offsetsAll.Slice(offsetsStart, offsetsByteLen);

        int firstOffset = rowCount == 0 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets);
        int lastOffset = rowCount == 0 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets.Slice(rowCount * 4, 4));
        int visibleElementsLen = lastOffset - firstOffset;

        var offsetBytes = new byte[offsetsByteLen];
        for (int i = 0; i <= rowCount; i++)
        {
            int orig = BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets.Slice(i * 4, 4));
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4, 4), orig - firstOffset);
        }

        // Slice the FULL elements array to the visible range so the recursive
        // encode sees only the rows actually referenced.
        var fullElements = listArr.Values;
        var slicedElements = ((Apache.Arrow.Array)fullElements).Slice(firstOffset, visibleElementsLen);

        // Recursive: encode the elements subtree. This may register more buffers.
        int elementsTicket = ArrayEncoderDispatch.Emit(sb, slicedElements, idx);

        // Offsets child: a vortex.primitive node referencing one buffer.
        ushort offsetsBufIdx = sb.AddBuffer(offsetBytes, alignmentExponent: 2);
        int offsetsNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
            sb.Builder, idx.Primitive, offsetsBufIdx);

        int? validityNodeTicket = null;
        if (data.GetNullCount() > 0)
        {
            var bitmapBytes = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: rowCount);
            ushort bitmapBufIdx = sb.AddBuffer(bitmapBytes, 0);
            validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, idx.Bool, bitmapBufIdx);
        }

        var childTickets = validityNodeTicket is null
            ? new[] { elementsTicket, offsetsNodeTicket }
            : new[] { elementsTicket, offsetsNodeTicket, validityNodeTicket.Value };

        // ListMetadata: { elements_len: u64 (field 1), offset_ptype: I32 (field 2) }.
        var metadataBytes = SerializeListMetadata((ulong)visibleElementsLen, OffsetsPtypeI32);
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, idx.List, metadataTicket, childTickets)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, idx.List, metadataTicket, childTickets, statsTicket.Value);
    }

    /// <summary>
    /// Inline ListMetadata proto bytes:
    ///   field 1 (varint, u64): elements_len
    ///   field 2 (varint, enum):  offset_ptype = I32 (6)
    /// </summary>
    private static byte[] SerializeListMetadata(ulong elementsLen, byte offsetPtype)
    {
        // Worst case: 1-byte tag + 10-byte u64 varint + 1-byte tag + 1-byte value = 13.
        Span<byte> tmp = stackalloc byte[13];
        int pos = 0;
        tmp[pos++] = 0x08; // tag: field 1, wire-type 0 (varint)
        pos += Varint.WriteUnsigned(tmp.Slice(pos), elementsLen);
        tmp[pos++] = 0x10; // tag: field 2, wire-type 0 (varint)
        pos += Varint.WriteUnsigned(tmp.Slice(pos), offsetPtype);
        return tmp.Slice(0, pos).ToArray();
    }
}
