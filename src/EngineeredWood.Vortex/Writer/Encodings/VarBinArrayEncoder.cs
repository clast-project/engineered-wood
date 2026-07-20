// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.VarBinArrayDecoder"/>:
/// emits a <c>vortex.varbin</c> ArrayNode subtree for an Arrow
/// <see cref="StringArray"/> or <see cref="BinaryArray"/> (i32 offsets only).
///
/// <para>Buffers (in registration order): bytes, offsets, optional bitmap.
/// Root ArrayNode { encoding=varbin, buffer_indices=[bytes_buf],
/// metadata={offsets_ptype:I32}, children=[offsets_node, validity_node?] }.</para>
/// </summary>
internal static class VarBinArrayEncoder
{
    /// <summary>PType.I32 = 6. Used in the inline VarBinMetadata proto bytes.</summary>
    private const byte OffsetsPtypeI32 = 6;

    public static int Emit(
        SegmentBuilder sb, IArrowArray array,
        ushort varbinEncodingIdx, ushort primitiveEncodingIdx, ushort boolEncodingIdx,
        int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        if (array is not StringArray && array is not BinaryArray)
            throw new NotSupportedException(
                $"vortex.varbin writer requires StringArray or BinaryArray, got {array.GetType().Name}.");

        var data = ((Apache.Arrow.Array)array).Data;
        int rowCount = array.Length;

        // Read sliced offsets and rebase so output offsets[0] = 0.
        var offsetsAll = data.Buffers[1].Span;
        int offsetsStart = data.Offset * 4;
        int offsetsEnd = offsetsStart + (rowCount + 1) * 4;
        if (offsetsAll.Length < offsetsEnd)
            throw new InvalidOperationException(
                $"Arrow offsets buffer is {offsetsAll.Length} bytes; need at least {offsetsEnd}.");
        var visibleOffsets = offsetsAll.Slice(offsetsStart, (rowCount + 1) * 4);
        int firstOffset = rowCount == 0 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets);
        int lastOffset = rowCount == 0 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets.Slice(rowCount * 4, 4));
        int totalDataLen = lastOffset - firstOffset;

        var offsetBytes = new byte[(rowCount + 1) * 4];
        for (int i = 0; i <= rowCount; i++)
        {
            int orig = BinaryPrimitives.ReadInt32LittleEndian(visibleOffsets.Slice(i * 4, 4));
            BinaryPrimitives.WriteInt32LittleEndian(offsetBytes.AsSpan(i * 4, 4), orig - firstOffset);
        }

        var dataBytes = new byte[totalDataLen];
        if (totalDataLen > 0)
            data.Buffers[2].Span.Slice(firstOffset, totalDataLen).CopyTo(dataBytes);

        // Register buffers in order: bytes (0), offsets (1), bitmap? (2).
        var bytesBufIdx = sb.AddBuffer(dataBytes, 0);
        var offsetsBufIdx = sb.AddBuffer(offsetBytes, 2);
        ushort? bitmapBufIdx = null;
        if (data.GetNullCount() > 0)
        {
            var bitmapBytes = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: rowCount);
            bitmapBufIdx = sb.AddBuffer(bitmapBytes, 0);
        }

        // Emit child nodes in the FB.
        int offsetsNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
            sb.Builder, primitiveEncodingIdx, offsetsBufIdx);
        int? validityNodeTicket = null;
        if (bitmapBufIdx is not null)
            validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, boolEncodingIdx, bitmapBufIdx.Value);

        // Build the children-vector tickets list.
        var childTickets = validityNodeTicket is null
            ? new[] { offsetsNodeTicket }
            : new[] { offsetsNodeTicket, validityNodeTicket.Value };

        // Inline VarBinMetadata: proto field 1 (offsets_ptype = I32). 2 bytes total.
        // Wire: tag (field=1, wire-type=0/varint) = 0x08; value = 6 (single-byte varint).
        var metadataTicket = sb.Builder.WriteByteVector(new byte[] { 0x08, OffsetsPtypeI32 });

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataBufferAndChildren(
                sb.Builder, varbinEncodingIdx, bytesBufIdx, metadataTicket, childTickets)
            : ArrayNodeEmitter.EmitWithMetadataBufferChildrenAndStats(
                sb.Builder, varbinEncodingIdx, bytesBufIdx, metadataTicket, childTickets, statsTicket.Value);
    }

    /// <summary>Convenience: encode one column's segment in isolation.</summary>
    public static byte[] Encode(
        IArrowArray array, ushort varbinEncodingIdx, ushort primitiveEncodingIdx, ushort boolEncodingIdx)
    {
        var sb = new SegmentBuilder();
        var rootTicket = Emit(sb, array, varbinEncodingIdx, primitiveEncodingIdx, boolEncodingIdx);
        return sb.FinishSegment(rootTicket);
    }
}
