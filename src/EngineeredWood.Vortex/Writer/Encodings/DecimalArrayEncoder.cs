// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.DecimalArrayDecoder"/>:
/// emits a <c>vortex.decimal</c> ArrayNode subtree for an Arrow
/// <see cref="Decimal128Array"/> or <see cref="Decimal256Array"/>.
///
/// <para>MVP scope: stores values at full Arrow width — Decimal128 →
/// <c>values_type = I128</c> (16 bytes/row), Decimal256 → <c>values_type = I256</c>
/// (32 bytes/row). Vortex's own writer narrows to the smallest signed integer
/// width that fits all values; we skip that optimization for now since it's
/// not required for correctness — the reader sign-extends back to the schema's
/// declared precision regardless.</para>
///
/// <para>Wire shape: 1 buffer (raw 16- or 32-byte little-endian values), 0–1
/// children (optional <c>vortex.bool</c> validity node), metadata
/// <c>DecimalMetadata { values_type }</c>.</para>
/// </summary>
internal static class DecimalArrayEncoder
{
    /// <summary>DecimalType enum: 4 = I128, 5 = I256.</summary>
    private const byte ValuesTypeI128 = 4;
    private const byte ValuesTypeI256 = 5;

    public static int Emit(
        SegmentBuilder sb, IArrowArray array,
        ushort decimalEncodingIdx, ushort boolEncodingIdx,
        int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        if (array is not Decimal128Array && array is not Decimal256Array)
            throw new NotSupportedException(
                $"vortex.decimal writer requires Decimal128Array or Decimal256Array, got {array.GetType().Name}.");

        var data = ((Apache.Arrow.Array)array).Data;
        int rowCount = array.Length;
        bool isD128 = array is Decimal128Array;
        int byteWidth = isD128 ? 16 : 32;
        byte valuesType = isD128 ? ValuesTypeI128 : ValuesTypeI256;
        // Alignment exponent: log2(byteWidth) — 4 for 16, 5 for 32.
        byte alignmentExp = isD128 ? (byte)4 : (byte)5;

        // Slice value bytes by element offset. Apache.Arrow stores Decimal*
        // values at Buffers[1] in native LE form, byteWidth bytes per element.
        int byteOffset = data.Offset * byteWidth;
        var valueBytes = new byte[rowCount * byteWidth];
        if (rowCount > 0)
            data.Buffers[1].Span.Slice(byteOffset, valueBytes.Length).CopyTo(valueBytes);
        ushort valueBufIdx = sb.AddBuffer(valueBytes, alignmentExp);

        // Inline DecimalMetadata: proto field 1 (values_type, varint enum). 2 bytes total.
        // Wire: tag (field=1, wire-type=0/varint) = 0x08; value = valuesType (single-byte varint).
        var metadataTicket = sb.Builder.WriteByteVector(new byte[] { 0x08, valuesType });

        // Optional validity child as a vortex.bool ArrayNode.
        if (data.GetNullCount() > 0)
        {
            var bitmap = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: rowCount);
            ushort bitmapBufIdx = sb.AddBuffer(bitmap, alignmentExponent: 0);
            int validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, boolEncodingIdx, bitmapBufIdx);
            var children = new[] { validityNodeTicket };
            return statsTicket is null
                ? ArrayNodeEmitter.EmitWithMetadataBufferAndChildren(
                    sb.Builder, decimalEncodingIdx, valueBufIdx, metadataTicket, children)
                : ArrayNodeEmitter.EmitWithMetadataBufferChildrenAndStats(
                    sb.Builder, decimalEncodingIdx, valueBufIdx, metadataTicket,
                    children, statsTicket.Value);
        }

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndBuffer(
                sb.Builder, decimalEncodingIdx, valueBufIdx, metadataTicket)
            : ArrayNodeEmitter.EmitWithMetadataBufferAndStats(
                sb.Builder, decimalEncodingIdx, valueBufIdx, metadataTicket, statsTicket.Value);
    }

    /// <summary>Convenience: encode one column's segment in isolation.</summary>
    public static byte[] Encode(IArrowArray array, ushort decimalEncodingIdx, ushort boolEncodingIdx)
    {
        var sb = new SegmentBuilder();
        var rootTicket = Emit(sb, array, decimalEncodingIdx, boolEncodingIdx);
        return sb.FinishSegment(rootTicket);
    }
}
