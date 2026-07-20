// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.decimal_byte_parts</c>: decimal values stored as a
/// "most significant part" (msp) integer plus zero or more lower parts.
///
/// <para>Wire format: 0 buffers, 1+ children (msp + optional lower parts).
/// Metadata <c>DecimalBytesPartsMetadata { zeroth_child_ptype, lower_part_count }</c>.</para>
///
/// <para>Phase 1 scope: <c>lower_part_count == 0</c> only (the current upstream
/// limitation as well). msp child is decoded as the resolved ptype, then
/// sign-extended into a 128-bit LE slot per row.</para>
/// </summary>
internal static class DecimalBytePartsArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not Decimal128Type and not Decimal256Type)
            throw new NotSupportedException(
                $"vortex.decimal_byte_parts produces Decimal128 or Decimal256, got {expectedType}.");
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.decimal_byte_parts expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount < 1)
            throw new VortexFormatException(
                $"vortex.decimal_byte_parts expects at least 1 child (msp), got {node.ChildCount}.");

        var metaVec = node.Metadata;
        var (zerothPtype, lowerCount) = ParseMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));
        if (lowerCount != 0)
            throw new NotSupportedException(
                $"vortex.decimal_byte_parts with lower_part_count={lowerCount} not yet supported.");

        var rowCount = checked((int)expectedRowCount);
        var msp = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, PtypeIntToArrowType(zerothPtype), expectedRowCount);

        var (nullBuffer, nullCount) = ExtractValidity(msp);

        var byteWidth = expectedType is Decimal128Type ? 16 : 32;
        var widenedBytes = new byte[(long)rowCount * byteWidth];
        WidenToWide(msp, rowCount, widenedBytes, byteWidth);

        var data = new ArrayData(
            expectedType, rowCount, nullCount, offset: 0,
            new[] { nullBuffer, new ArrowBuffer(widenedBytes) });
        return expectedType is Decimal128Type
            ? new Decimal128Array(data)
            : new Decimal256Array(data);
    }

    private static (ArrowBuffer NullBuffer, int NullCount) ExtractValidity(IArrowArray array)
    {
        var data = (array as Apache.Arrow.Array)?.Data;
        if (data is null || data.NullCount == 0 || data.Buffers.Length == 0)
            return (ArrowBuffer.Empty, 0);
        return (data.Buffers[0], data.NullCount);
    }

    private static void WidenToWide(IArrowArray msp, int rowCount, byte[] dst, int byteWidth)
    {
        // Sign-extend a 64-bit signed value into byteWidth LE bytes.
        switch (msp)
        {
            case Int8Array a:
                for (int i = 0; i < rowCount; i++) WriteSignedLE(dst, i, byteWidth, a.GetValue(i)!.Value);
                break;
            case Int16Array a:
                for (int i = 0; i < rowCount; i++) WriteSignedLE(dst, i, byteWidth, a.GetValue(i)!.Value);
                break;
            case Int32Array a:
                for (int i = 0; i < rowCount; i++) WriteSignedLE(dst, i, byteWidth, a.GetValue(i)!.Value);
                break;
            case Int64Array a:
                for (int i = 0; i < rowCount; i++) WriteSignedLE(dst, i, byteWidth, a.GetValue(i)!.Value);
                break;
            case UInt8Array u:
                for (int i = 0; i < rowCount; i++) WriteUnsignedLE(dst, i, byteWidth, u.GetValue(i)!.Value);
                break;
            case UInt16Array u:
                for (int i = 0; i < rowCount; i++) WriteUnsignedLE(dst, i, byteWidth, u.GetValue(i)!.Value);
                break;
            case UInt32Array u:
                for (int i = 0; i < rowCount; i++) WriteUnsignedLE(dst, i, byteWidth, u.GetValue(i)!.Value);
                break;
            case UInt64Array u:
                for (int i = 0; i < rowCount; i++) WriteUnsignedLE(dst, i, byteWidth, u.GetValue(i)!.Value);
                break;
            default:
                throw new NotSupportedException(
                    $"vortex.decimal_byte_parts: unexpected msp array type {msp.GetType().Name}.");
        }
    }

    /// <summary>Writes a signed 64-bit value as a sign-extended LE int of width <paramref name="byteWidth"/> (16 or 32).</summary>
    private static void WriteSignedLE(byte[] dst, int row, int byteWidth, long value)
    {
        var span = dst.AsSpan(row * byteWidth, byteWidth);
        BinaryPrimitives.WriteInt64LittleEndian(span, value);
        long fill = value < 0 ? -1L : 0L;
        for (int i = 8; i < byteWidth; i += 8)
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(i), fill);
    }

    /// <summary>Writes an unsigned 64-bit value as a zero-extended LE int.</summary>
    private static void WriteUnsignedLE(byte[] dst, int row, int byteWidth, ulong value)
    {
        var span = dst.AsSpan(row * byteWidth, byteWidth);
        BinaryPrimitives.WriteUInt64LittleEndian(span, value);
        for (int i = 8; i < byteWidth; i += 8)
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(i), 0L);
    }

    private static (int ZerothPtype, int LowerCount) ParseMetadata(ReadOnlySpan<byte> bytes)
    {
        int zerothPtype = 0, lowerCount = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                zerothPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                lowerCount = (int)Varint.ReadUnsigned(bytes, ref pos);
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
                            $"Unsupported wire type {wireType} in DecimalBytesPartsMetadata.");
                }
            }
        }
        return (zerothPtype, lowerCount);
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
        _ => throw new VortexFormatException($"Unsupported ptype {ptype} in DecimalBytesPartsMetadata."),
    };
}
