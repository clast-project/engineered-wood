// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.StructArrayDecoder"/>:
/// emits a <c>vortex.struct</c> ARRAY-level subtree (NOT the layout-level
/// struct at the file root). Recursively encodes each field via
/// <see cref="ArrayEncoderDispatch.Emit"/>, so any field can itself be a
/// nested struct, list, primitive, etc.
///
/// <para>Wire shape: 0 buffers, empty metadata, N children when non-nullable
/// (one ArrayNode per field) or N+1 children when nullable (validity bitmap
/// at child[0], fields at [1..]). Same vtable as vortex.fixed_size_list —
/// slots 0+2 (encoding + children), or 0+2+4 with stats.</para>
///
/// <para>Sliced inputs (parent <c>data.Offset != 0</c>) are honored by
/// slicing each field child to the parent's logical window before recursive
/// encoding. The validity bitmap is extracted with
/// <see cref="EncoderHelpers.ExtractValidityBitmap"/> using the parent's
/// bit offset.</para>
/// </summary>
internal static class StructArrayEncoder
{
    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is not StructArray sArr)
            throw new NotSupportedException(
                $"vortex.struct writer requires Apache.Arrow.StructArray, got {array.GetType().Name}.");

        var data = sArr.Data;
        int rowCount = sArr.Length;
        int parentOffset = data.Offset;
        int nfields = sArr.Fields.Count;

        var childTickets = new List<int>(nfields + 1);

        // Optional struct-level validity child (placed FIRST per vortex's slot
        // convention — child[0] is validity when present).
        if (data.GetNullCount() > 0)
        {
            var bitmap = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: parentOffset, rowCount: rowCount);
            ushort bitmapBufIdx = sb.AddBuffer(bitmap, alignmentExponent: 0);
            int validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, idx.Bool, bitmapBufIdx);
            childTickets.Add(validityNodeTicket);
        }

        // Per-field children. Apache.Arrow's StructArray.Fields[i] auto-
        // propagates the parent's offset and length to each child — so
        // sliced.Fields[0].Data.Offset == sliced.Offset, no manual Slice
        // needed. Don't double-slice; use the fields as-is.
        for (int i = 0; i < nfields; i++)
            childTickets.Add(ArrayEncoderDispatch.Emit(sb, sArr.Fields[i], idx));

        var children = childTickets.ToArray();
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithChildrenOnly(sb.Builder, idx.Struct_, children)
            : ArrayNodeEmitter.EmitWithChildrenAndStats(
                sb.Builder, idx.Struct_, children, statsTicket.Value);
    }
}
