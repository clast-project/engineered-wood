// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.VarBinViewArrayDecoder"/>:
/// emits a <c>vortex.varbinview</c> ArrayNode subtree using Arrow's
/// BinaryView layout — a 16-byte view per row, with strings ≤ 12 bytes
/// inlined directly in the view and longer strings stored in a separate
/// data buffer with the view holding (length, prefix, buf_idx, offset).
///
/// <para>Wire shape: <c>1 + ndata</c> buffers — data buffers come first,
/// the views buffer last (per upstream's <c>buffer(idx)</c>: indices
/// <c>0..ndata</c> are data buffers, <c>ndata</c> is views). 0 children
/// for non-nullable, 1 child (vortex.bool validity) for nullable. Empty
/// metadata.</para>
///
/// <para>View layout (16 bytes per row, little-endian):
/// <list type="bullet">
///   <item>bytes 0..3: length (u32)</item>
///   <item>length ≤ 12 (inline): bytes 4..15 are the string bytes,
///     zero-padded.</item>
///   <item>length &gt; 12 (referenced): bytes 4..7 = first 4 bytes of the
///     string (prefix used for fast filter compares), bytes 8..11 = data
///     buffer index (u32), bytes 12..15 = offset within buffer (u32).</item>
/// </list></para>
///
/// <para>Scope: <see cref="StringArray"/> + <see cref="BinaryArray"/>,
/// nullable + non-nullable, sliced + non-sliced. Single data buffer (all
/// long values concatenated). Apache.Arrow's <c>StringArray</c> inherits
/// from <c>BinaryArray</c> and shares its byte-level accessors
/// (<c>GetBytes</c>, <c>GetValueLength</c>, <c>IsValid</c>), so the encode
/// loop is identical for both — only the schema-level dtype distinguishes
/// them. The encoder is exposed as an opt-in
/// (<c>VortexFileWriter</c>'s <c>preferVarBinView</c> flag) rather than
/// auto-dispatched — for short-value columns vortex.varbin is more
/// compact (4 + len bytes/row vs varbinview's 16 + (len if &gt; 12) bytes/row).</para>
/// </summary>
internal static class VarBinViewArrayEncoder
{
    /// <summary>Inline cutoff. Values with length ≤ this go in the view.</summary>
    private const int InlineLimit = 12;

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        // BinaryArray is the base type; StringArray inherits from it. Both
        // expose the same byte-level accessors we use below.
        if (array is not BinaryArray s)
            throw new NotSupportedException(
                $"vortex.varbinview writer requires StringArray or BinaryArray, got {array.GetType().Name}.");
        var data = s.Data;

        int n = s.Length;
        int nullCount = data.GetNullCount();
        bool hasNulls = nullCount > 0;

        // Pass 1: compute total long-string byte size to size the data buffer.
        // For nullable columns we count nulls as zero-length (the validity child
        // masks them on read).
        int dataLen = 0;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls && !s.IsValid(i)) continue;
            int len = s.GetValueLength(i);
            if (len > InlineLimit) dataLen += len;
        }

        // Pass 2: lay out the views and the (single) data buffer.
        var viewsBytes = new byte[(long)n * 16];
        var dataBytes = dataLen == 0 ? System.Array.Empty<byte>() : new byte[dataLen];
        int dataPos = 0;
        for (int i = 0; i < n; i++)
        {
            var view = viewsBytes.AsSpan(i * 16, 16);
            if (hasNulls && !s.IsValid(i))
            {
                // Null row: zeroed view. The validity child masks the value.
                continue;
            }
            int len = s.GetValueLength(i);
            BinaryPrimitives.WriteUInt32LittleEndian(view, (uint)len);
            if (len == 0) continue;

            var bytes = s.GetBytes(i);
            // GetBytes can over-read into padding; trim to the typed length.
            if (bytes.Length > len) bytes = bytes.Slice(0, len);

            if (len <= InlineLimit)
            {
                bytes.CopyTo(view.Slice(4));
            }
            else
            {
                // Prefix (first 4 bytes) is duplicated in the view to enable
                // prefix-only filter compares without dereferencing.
                bytes.Slice(0, 4).CopyTo(view.Slice(4));
                BinaryPrimitives.WriteUInt32LittleEndian(view.Slice(8), 0); // buf_idx — single data buffer
                BinaryPrimitives.WriteUInt32LittleEndian(view.Slice(12), (uint)dataPos);
                bytes.CopyTo(dataBytes.AsSpan(dataPos, len));
                dataPos += len;
            }
        }

        // Register buffers — data buffer FIRST, views buffer LAST (per the
        // upstream buffer(idx) convention). The views buffer is an array of
        // 16-byte BinaryView structs and MUST be 16-byte aligned (= 2^4);
        // vortex's Rust reader Buffer<BinaryView> reinterpret-casts the bytes
        // and panics with "Bytes alignment must align to the requested
        // alignment 16" if it isn't.
        var bufIdxs = dataLen == 0
            ? new[] { sb.AddBuffer(viewsBytes, alignmentExponent: 4) }
            : new[]
            {
                sb.AddBuffer(dataBytes, alignmentExponent: 0),
                sb.AddBuffer(viewsBytes, alignmentExponent: 4),
            };

        // Empty metadata — vortex.varbinview rejects non-empty.
        int metadataTicket = sb.Builder.WriteByteVector(System.Array.Empty<byte>());

        // Optional validity child (vortex.bool with the column's bitmap).
        int[] children;
        if (hasNulls)
        {
            var bitmap = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: n);
            ushort bitmapBufIdx = sb.AddBuffer(bitmap, alignmentExponent: 0);
            int validityTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, idx.Bool, bitmapBufIdx);
            children = new[] { validityTicket };
        }
        else
        {
            children = System.Array.Empty<int>();
        }

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataBuffersAndChildren(
                sb.Builder, idx.VarBinView, bufIdxs, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataBuffersChildrenAndStats(
                sb.Builder, idx.VarBinView, bufIdxs, metadataTicket, children, statsTicket.Value);
    }
}
