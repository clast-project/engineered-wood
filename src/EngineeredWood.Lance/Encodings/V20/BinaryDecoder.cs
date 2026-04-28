// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Decodes a <see cref="Binary"/> v2.0 ArrayEncoding — the representation
/// pylance emits for <c>string</c>, <c>large_string</c>, <c>binary</c>, and
/// <c>large_binary</c> columns.
///
/// <para>Layout, per <c>protos/encodings_v2_0.proto</c> and
/// <c>protos/file.proto</c>:</para>
/// <list type="bullet">
///   <item>
///     <term>indices</term>
///     <description>
///       An <see cref="ArrayEncoding"/> producing one offset per row (no leading zero).
///       If row <c>i</c> is non-null, <c>offsets[i] = base + len(value_i)</c>, where
///       <c>base = 0</c> for <c>i = 0</c> and <c>base = offsets[i-1] % null_adjustment</c>
///       otherwise.
///       If row <c>i</c> is null, <c>offsets[i] = base + len + null_adjustment</c>
///       (so any offset ≥ <c>null_adjustment</c> signals a null).
///     </description>
///   </item>
///   <item>
///     <term>bytes</term>
///     <description>An <see cref="ArrayEncoding"/> producing a flat byte payload.</description>
///   </item>
///   <item>
///     <term>null_adjustment</term>
///     <description>A sentinel strictly greater than the total byte length.</description>
///   </item>
/// </list>
/// </summary>
internal sealed class BinaryDecoder : IArrayDecoder
{
    private readonly Binary _encoding;

    public BinaryDecoder(Binary encoding) => _encoding = encoding;

    public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
    {
        if (_encoding.Indices is null || _encoding.Bytes is null)
            throw new LanceFormatException("Binary encoding is missing indices or bytes.");

        ReadOnlyMemory<byte> offsetBytes = FlatDecoder.ResolveFlatBuffer(
            _encoding.Indices, context, out ulong bitsPerOffset);
        ReadOnlyMemory<byte> dataBytes = FlatDecoder.ResolveFlatBuffer(
            _encoding.Bytes, context, out _);

        int length = checked((int)numRows);
        ulong nullAdjustment = _encoding.NullAdjustment;

        bool useLarge = targetType is LargeStringType or LargeBinaryType;
        bool isString = targetType is StringType or LargeStringType;
        bool isBinary = targetType is BinaryType or LargeBinaryType;
        if (!isString && !isBinary)
            throw new LanceFormatException(
                $"Binary encoding used with non-binary target type {targetType}.");

        if (bitsPerOffset == 32)
            return BuildArray(ParseOffsetsAndBuildArrow<uint>(
                offsetBytes.Span, length, nullAdjustment, dataBytes.Span,
                useLarge, out int nullCount, out byte[]? bitmap, out byte[] outOffsets, out byte[] outData),
                targetType, length, nullCount, bitmap, outOffsets, outData);

        if (bitsPerOffset == 64)
            return BuildArray(ParseOffsetsAndBuildArrow<ulong>(
                offsetBytes.Span, length, nullAdjustment, dataBytes.Span,
                useLarge, out int nullCount, out byte[]? bitmap, out byte[] outOffsets, out byte[] outData),
                targetType, length, nullCount, bitmap, outOffsets, outData);

        throw new NotImplementedException(
            $"Binary encoding with bits_per_offset={bitsPerOffset} is not supported.");
    }

    // The generic parameter indicates how offsets are stored on disk; the
    // output Arrow offsets are either 32-bit (StringType/BinaryType) or
    // 64-bit (LargeStringType/LargeBinaryType), chosen by `useLarge`.
    private static bool ParseOffsetsAndBuildArrow<TDiskOffset>(
        ReadOnlySpan<byte> offsetBytes,
        int length,
        ulong nullAdjustment,
        ReadOnlySpan<byte> dataBytes,
        bool useLarge,
        out int nullCount,
        out byte[]? bitmap,
        out byte[] arrowOffsets,
        out byte[] arrowData)
        where TDiskOffset : unmanaged
    {
        int diskWidth = System.Runtime.InteropServices.Marshal.SizeOf<TDiskOffset>();
        int expectedOffsetBytes = diskWidth * length;
        if (offsetBytes.Length < expectedOffsetBytes)
            throw new LanceFormatException(
                $"Binary offsets buffer too small: got {offsetBytes.Length}, need {expectedOffsetBytes}.");

        int arrowOffsetWidth = useLarge ? sizeof(long) : sizeof(int);
        arrowOffsets = new byte[arrowOffsetWidth * (length + 1)];

        nullCount = 0;

        // Allocate the validity bitmap up-front. We set a bit for every
        // non-null row and drop the buffer at the end if no nulls appeared.
        var bitmapArr = new byte[(length + 7) / 8];

        ulong diskBase = 0;
        ulong totalDataLen = 0;
        Span<byte> arrowOffsetsSpan = arrowOffsets;
        if (useLarge)
            BinaryPrimitives.WriteInt64LittleEndian(arrowOffsetsSpan.Slice(0, 8), 0);
        else
            BinaryPrimitives.WriteInt32LittleEndian(arrowOffsetsSpan.Slice(0, 4), 0);

        ulong arrowCumulative = 0;
        var sourceRanges = new (int Start, int Length)[length];

        for (int i = 0; i < length; i++)
        {
            ulong diskOffset = ReadDiskOffset<TDiskOffset>(offsetBytes, i, diskWidth);
            bool isNull = diskOffset >= nullAdjustment;
            ulong endModulo = isNull ? diskOffset - nullAdjustment : diskOffset;
            ulong rowLen = endModulo - diskBase;

            if (isNull)
            {
                sourceRanges[i] = (0, 0);
                nullCount++;
            }
            else
            {
                if (rowLen > int.MaxValue)
                    throw new LanceFormatException($"Row {i} length exceeds Int32.MaxValue.");
                int startInt = checked((int)diskBase);
                int lenInt = checked((int)rowLen);
                sourceRanges[i] = (startInt, lenInt);
                arrowCumulative += rowLen;
                bitmapArr[i >> 3] |= (byte)(1 << (i & 7));
            }

            if (useLarge)
                BinaryPrimitives.WriteInt64LittleEndian(
                    arrowOffsetsSpan.Slice((i + 1) * 8, 8),
                    checked((long)arrowCumulative));
            else
                BinaryPrimitives.WriteInt32LittleEndian(
                    arrowOffsetsSpan.Slice((i + 1) * 4, 4),
                    checked((int)arrowCumulative));

            diskBase = endModulo;
            totalDataLen = endModulo;
        }

        bitmap = nullCount == 0 ? null : bitmapArr;

        // Build the compact data buffer: concatenate non-null source slices in order.
        if (nullCount == 0)
        {
            if (dataBytes.Length < (int)totalDataLen)
                throw new LanceFormatException(
                    $"Binary bytes buffer too small: got {dataBytes.Length}, need {totalDataLen}.");
            arrowData = dataBytes.Slice(0, (int)totalDataLen).ToArray();
        }
        else
        {
            arrowData = new byte[arrowCumulative];
            int writeOffset = 0;
            for (int i = 0; i < length; i++)
            {
                var (start, len) = sourceRanges[i];
                if (len > 0)
                {
                    if (start + len > dataBytes.Length)
                        throw new LanceFormatException(
                            $"Binary row {i} range [{start},{start + len}) exceeds data buffer size {dataBytes.Length}.");
                    dataBytes.Slice(start, len).CopyTo(arrowData.AsSpan(writeOffset, len));
                    writeOffset += len;
                }
            }
        }

        return true;
    }

    private static IArrowArray BuildArray(
        bool _unused,
        IArrowType targetType, int length, int nullCount,
        byte[]? bitmap, byte[] arrowOffsets, byte[] arrowData)
    {
        var validityBuffer = nullCount == 0 || bitmap is null
            ? ArrowBuffer.Empty
            : new ArrowBuffer(bitmap);
        var offsetsBuffer = new ArrowBuffer(arrowOffsets);
        var dataBuffer = new ArrowBuffer(arrowData);

        var data = new ArrayData(
            targetType, length, nullCount, offset: 0,
            new[] { validityBuffer, offsetsBuffer, dataBuffer });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static ulong ReadDiskOffset<T>(ReadOnlySpan<byte> buffer, int index, int width)
        where T : unmanaged
    {
        return width switch
        {
            4 => BinaryPrimitives.ReadUInt32LittleEndian(buffer.Slice(index * 4, 4)),
            8 => BinaryPrimitives.ReadUInt64LittleEndian(buffer.Slice(index * 8, 8)),
            _ => throw new NotImplementedException($"Offset width {width} bytes is not supported."),
        };
    }
}
