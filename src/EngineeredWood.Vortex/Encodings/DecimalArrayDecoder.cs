// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.decimal</c>: decimal values stored as a primitive
/// integer of varying width (I8/I16/I32/I64/I128/I256), depending on the
/// minimum width that fits all values.
///
/// <para>Wire format: 1 buffer (raw integer values, sized per <c>values_type</c>),
/// 0-1 children (optional validity). Metadata <c>DecimalMetadata { values_type }</c>
/// (DecimalType enum: 0=I8, 1=I16, 2=I32, 3=I64, 4=I128, 5=I256).</para>
///
/// <para>Output: Apache Arrow <see cref="Decimal128Type"/> (precision ≤ 38) or
/// <see cref="Decimal256Type"/> (precision &gt; 38). The schema converter chooses
/// based on the dtype's precision; this decoder respects whichever was picked
/// and sign-extends the values_type buffer into the matching slot width
/// (16 bytes for Decimal128, 32 bytes for Decimal256). I256 → Decimal128 is
/// rejected because it cannot fit.</para>
/// </summary>
internal static class DecimalArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not Decimal128Type and not Decimal256Type)
            throw new NotSupportedException(
                $"vortex.decimal produces Decimal128 or Decimal256, got {expectedType}.");
        if (node.BufferRefCount != 1)
            throw new VortexFormatException(
                $"vortex.decimal expects 1 buffer, got {node.BufferRefCount}.");

        var metaVec = node.Metadata;
        var valuesType = ParseDecimalMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));

        var bufferRef = node.BufferRef(0);
        var bufferDesc = serialized.Message.Buffer(bufferRef);
        if (bufferDesc.Compression != BufferCompression.None)
            throw new NotSupportedException(
                $"vortex.decimal buffer compression {bufferDesc.Compression} not yet implemented.");
        var data = serialized.BufferBytes(bufferRef);

        var rowCount = checked((int)expectedRowCount);

        // Optional validity child.
        ArrowBuffer nullBuffer; int nullCount;
        if (node.ChildCount == 0) { nullBuffer = ArrowBuffer.Empty; nullCount = 0; }
        else if (node.ChildCount == 1)
        {
            nullBuffer = BoolArrayDecoder.ReadBitmap(node.Child(0), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(nullBuffer.Span, rowCount);
        }
        else throw new NotSupportedException(
            $"vortex.decimal with {node.ChildCount} children is not supported.");

        int byteWidth = expectedType is Decimal128Type ? 16 : 32;
        var widenedBytes = new byte[(long)rowCount * byteWidth];
        Widen(valuesType, byteWidth, data, widenedBytes, rowCount);

        var arrayData = new ArrayData(
            expectedType, rowCount, nullCount, offset: 0,
            new[] { nullBuffer, new ArrowBuffer(widenedBytes) });
        return expectedType is Decimal128Type
            ? new Decimal128Array(arrayData)
            : new Decimal256Array(arrayData);
    }

    /// <summary>
    /// Sign-extends <paramref name="src"/> integers (per <paramref name="valuesType"/>)
    /// into <paramref name="byteWidth"/>-byte little-endian slots in <paramref name="dst"/>.
    /// </summary>
    private static void Widen(int valuesType, int byteWidth, ReadOnlySpan<byte> src, byte[] dst, int rowCount)
    {
        switch (valuesType)
        {
            case 0: // I8
                for (int i = 0; i < rowCount; i++)
                    WriteSignExtendedLE(dst, i, byteWidth, (sbyte)src[i]);
                break;
            case 1: // I16
                {
                    var s = MemoryMarshal.Cast<byte, short>(src);
                    for (int i = 0; i < rowCount; i++) WriteSignExtendedLE(dst, i, byteWidth, s[i]);
                    break;
                }
            case 2: // I32
                {
                    var s = MemoryMarshal.Cast<byte, int>(src);
                    for (int i = 0; i < rowCount; i++) WriteSignExtendedLE(dst, i, byteWidth, s[i]);
                    break;
                }
            case 3: // I64
                {
                    var s = MemoryMarshal.Cast<byte, long>(src);
                    for (int i = 0; i < rowCount; i++) WriteSignExtendedLE(dst, i, byteWidth, s[i]);
                    break;
                }
            case 4: // I128
                if (byteWidth == 16)
                {
                    src.Slice(0, rowCount * 16).CopyTo(dst.AsSpan());
                }
                else
                {
                    // Copy 16 LE bytes per row, sign-extend the upper 16 bytes from the
                    // top bit of byte 15.
                    for (int i = 0; i < rowCount; i++)
                    {
                        var srcRow = src.Slice(i * 16, 16);
                        var dstRow = dst.AsSpan(i * 32, 32);
                        srcRow.CopyTo(dstRow);
                        byte fill = (srcRow[15] & 0x80) != 0 ? (byte)0xFF : (byte)0;
                        dstRow.Slice(16, 16).Fill(fill);
                    }
                }
                break;
            case 5: // I256
                if (byteWidth == 16)
                    throw new NotSupportedException(
                        "vortex.decimal with I256 values cannot fit in Decimal128; "
                        + "schema must declare precision > 38 (→ Decimal256Type) for I256 storage.");
                src.Slice(0, rowCount * 32).CopyTo(dst.AsSpan());
                break;
            default:
                throw new VortexFormatException(
                    $"Unknown DecimalType enum value {valuesType}.");
        }
    }

    /// <summary>Writes a signed 64-bit value as a sign-extended LE int of width <paramref name="byteWidth"/> (16 or 32).</summary>
    private static void WriteSignExtendedLE(byte[] dst, int row, int byteWidth, long value)
    {
        var span = dst.AsSpan(row * byteWidth, byteWidth);
        BinaryPrimitives.WriteInt64LittleEndian(span, value);
        long fill = value < 0 ? -1L : 0L;
        for (int i = 8; i < byteWidth; i += 8)
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(i), fill);
    }

    private static int ParseDecimalMetadata(ReadOnlySpan<byte> bytes)
    {
        // DecimalMetadata { values_type: DecimalType at field 1 }
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                return (int)Varint.ReadUnsigned(bytes, ref pos);
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
                        $"Unsupported wire type {wireType} in DecimalMetadata.");
            }
        }
        return 0; // proto3 default = I8
    }
}
