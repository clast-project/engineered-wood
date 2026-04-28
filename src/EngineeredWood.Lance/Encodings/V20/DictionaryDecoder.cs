// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Decodes a <see cref="Dictionary"/> v2.0 ArrayEncoding by materializing
/// the result: the <c>indices</c> sub-encoding produces one integer per row,
/// the <c>items</c> sub-encoding produces <c>num_dictionary_items</c> values
/// of the target type, and the output array is
/// <c>items[indices[i]]</c> for each row <c>i</c>.
///
/// <para>Phase 3 materializes into the plain Arrow type named by the schema
/// rather than an <c>Apache.Arrow.DictionaryArray</c>. The Lance schema
/// expresses dictionaries only at encoding level (the value type is what
/// surfaces in the Arrow schema), so materialization matches user
/// expectations.</para>
///
/// <para>Supported target types: fixed-width primitives and
/// <see cref="BooleanType"/>, <see cref="StringType"/>, <see cref="BinaryType"/>,
/// <see cref="LargeStringType"/>, <see cref="LargeBinaryType"/>.</para>
/// </summary>
internal sealed class DictionaryDecoder : IArrayDecoder
{
    private readonly Dictionary _encoding;

    public DictionaryDecoder(Dictionary encoding) => _encoding = encoding;

    public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
    {
        if (_encoding.Indices is null || _encoding.Items is null)
            throw new LanceFormatException("Dictionary encoding is missing indices or items.");
        if (_encoding.NumDictionaryItems == 0 && numRows > 0)
            throw new LanceFormatException(
                $"Dictionary encoding has num_dictionary_items=0 but {numRows} rows to decode.");

        int length = checked((int)numRows);
        int dictCount = checked((int)_encoding.NumDictionaryItems);

        // Indices are stored at their natural bit width (typically 8/16/32),
        // which may be narrower than UInt32. Read raw bytes from the flat
        // buffer so we don't force a width expansion through the Arrow layer.
        //
        // Lance uses 1-based dictionary indices with 0 reserved for null:
        // raw index 0 → null, raw index N ≥ 1 → items[N - 1]. This is built
        // in regardless of any outer Nullable wrapping.
        var indicesFlat = FlatDecoder.ResolveFlatBuffer(_encoding.Indices, context, out ulong bitsPerIndex);
        int[] indices = ReadIndices(indicesFlat.Span, length, bitsPerIndex, dictCount, out int nullCount, out byte[]? validity);

        // Dictionary items themselves are decoded via the normal path.
        var itemsDecoder = V20ArrayEncodingDispatcher.Create(_encoding.Items);
        var itemsArray = itemsDecoder.Decode(dictCount, targetType, context);

        return Materialize(indices, itemsArray, length, targetType, nullCount, validity);
    }

    private static int[] ReadIndices(
        ReadOnlySpan<byte> flat, int numRows, ulong bitsPerValue, int dictCount,
        out int nullCount, out byte[]? validity)
    {
        if (bitsPerValue % 8 != 0)
            throw new NotImplementedException(
                $"Dictionary indices with bits_per_value={bitsPerValue} (non-byte-aligned) are not supported yet.");

        int bytesPerValue = checked((int)(bitsPerValue / 8));
        int required = bytesPerValue * numRows;
        if (flat.Length < required)
            throw new LanceFormatException(
                $"Dictionary indices buffer too small: need {required} bytes, have {flat.Length}.");

        // Per Lance semantics, index 0 is null; items are 1-based. We translate
        // to 0-based item indices here and fabricate a validity bitmap if we
        // encounter any 0. `result[i]` is only meaningful for non-null rows.
        var result = new int[numRows];
        var bitmap = new byte[(numRows + 7) / 8];
        nullCount = 0;
        for (int i = 0; i < numRows; i++)
        {
            long raw = bytesPerValue switch
            {
                1 => flat[i],
                2 => System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(flat.Slice(i * 2, 2)),
                4 => checked((long)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(flat.Slice(i * 4, 4))),
                8 => checked((long)System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(flat.Slice(i * 8, 8))),
                _ => throw new NotImplementedException(
                    $"Dictionary indices of {bytesPerValue}-byte width are not supported."),
            };

            if (raw == 0)
            {
                nullCount++;
                result[i] = 0; // placeholder — row is null
                continue;
            }

            long adjusted = raw - 1;
            if (adjusted < 0 || adjusted >= dictCount)
                throw new LanceFormatException(
                    $"Dictionary raw index {raw} at row {i} is out of range (dict size = {dictCount}).");
            result[i] = (int)adjusted;
            bitmap[i >> 3] |= (byte)(1 << (i & 7));
        }

        validity = nullCount == 0 ? null : bitmap;
        return result;
    }

    private static IArrowArray Materialize(
        int[] indices, IArrowArray items, int numRows, IArrowType targetType,
        int nullCount, byte[]? validity)
    {
        if (targetType is StringType or BinaryType)
            return MaterializeVariable<int>(indices, items, numRows, targetType, nullCount, validity);
        if (targetType is LargeStringType or LargeBinaryType)
            return MaterializeVariable<long>(indices, items, numRows, targetType, nullCount, validity);
        if (targetType is BooleanType)
            return MaterializeBoolean(indices, (BooleanArray)items, numRows, nullCount, validity);
        if (targetType is FixedWidthType fw)
            return MaterializeFixedWidth(indices, items, numRows, targetType, fw.BitWidth / 8, nullCount, validity);

        throw new NotImplementedException(
            $"Dictionary materialization for Arrow type {targetType} is not yet supported.");
    }

    private static IArrowArray MaterializeFixedWidth(
        int[] indices, IArrowArray items, int numRows, IArrowType targetType, int bytesPerValue,
        int nullCount, byte[]? validity)
    {
        if (items.NullCount > 0)
            throw new NotImplementedException("Dictionary items with nulls are not yet supported.");

        ReadOnlySpan<byte> itemBytes = items.Data.Buffers[1].Span;
        var output = new byte[numRows * bytesPerValue];

        for (int i = 0; i < numRows; i++)
        {
            // For null rows (validity bit = 0) we leave the value bytes at zero —
            // consumers should consult the validity bitmap, not the value.
            if (validity is not null && (validity[i >> 3] & (1 << (i & 7))) == 0)
                continue;
            int idx = indices[i];
            itemBytes
                .Slice(idx * bytesPerValue, bytesPerValue)
                .CopyTo(output.AsSpan(i * bytesPerValue, bytesPerValue));
        }

        var validityBuffer = validity is null ? ArrowBuffer.Empty : new ArrowBuffer(validity);
        var data = new ArrayData(
            targetType, numRows, nullCount, offset: 0,
            new[] { validityBuffer, new ArrowBuffer(output) });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray MaterializeBoolean(
        int[] indices, BooleanArray items, int numRows, int nullCount, byte[]? validity)
    {
        int bitmapBytes = (numRows + 7) / 8;
        var output = new byte[bitmapBytes];

        for (int i = 0; i < numRows; i++)
        {
            if (validity is not null && (validity[i >> 3] & (1 << (i & 7))) == 0)
                continue;
            if (items.GetValue(indices[i]) == true)
                output[i >> 3] |= (byte)(1 << (i & 7));
        }

        var validityBuffer = validity is null ? ArrowBuffer.Empty : new ArrowBuffer(validity);
        return new BooleanArray(new ArrayData(
            BooleanType.Default, numRows, nullCount, offset: 0,
            new[] { validityBuffer, new ArrowBuffer(output) }));
    }

    private static IArrowArray MaterializeVariable<TArrowOffset>(
        int[] indices, IArrowArray items, int numRows, IArrowType targetType,
        int nullCount, byte[]? validity)
        where TArrowOffset : unmanaged
    {
        if (items.NullCount > 0)
            throw new NotImplementedException("Dictionary items with nulls are not yet supported.");

        int offsetWidth = System.Runtime.InteropServices.Marshal.SizeOf<TArrowOffset>();
        ReadOnlySpan<byte> itemOffsetsBytes = items.Data.Buffers[1].Span;
        ReadOnlySpan<byte> itemValueBytes = items.Data.Buffers[2].Span;

        var lengths = new int[numRows];
        long totalBytes = 0;
        for (int i = 0; i < numRows; i++)
        {
            if (validity is not null && (validity[i >> 3] & (1 << (i & 7))) == 0)
            {
                lengths[i] = 0;
                continue;
            }
            int idx = indices[i];
            long start = ReadOffset<TArrowOffset>(itemOffsetsBytes, idx, offsetWidth);
            long end = ReadOffset<TArrowOffset>(itemOffsetsBytes, idx + 1, offsetWidth);
            int len = checked((int)(end - start));
            lengths[i] = len;
            totalBytes = checked(totalBytes + len);
        }

        int outOffsetBytes = offsetWidth * (numRows + 1);
        var outOffsets = new byte[outOffsetBytes];
        var outData = new byte[totalBytes];

        long cumulative = 0;
        WriteOffset<TArrowOffset>(outOffsets, 0, offsetWidth, 0);
        int writePos = 0;
        for (int i = 0; i < numRows; i++)
        {
            int len = lengths[i];
            if (len > 0)
            {
                int idx = indices[i];
                long start = ReadOffset<TArrowOffset>(itemOffsetsBytes, idx, offsetWidth);
                itemValueBytes.Slice(checked((int)start), len)
                    .CopyTo(outData.AsSpan(writePos, len));
                writePos += len;
                cumulative += len;
            }
            WriteOffset<TArrowOffset>(outOffsets, i + 1, offsetWidth, cumulative);
        }

        var validityBuffer = validity is null ? ArrowBuffer.Empty : new ArrowBuffer(validity);
        var data = new ArrayData(
            targetType, numRows, nullCount, offset: 0,
            new[] { validityBuffer, new ArrowBuffer(outOffsets), new ArrowBuffer(outData) });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static long ReadOffset<T>(ReadOnlySpan<byte> buffer, int index, int width)
        where T : unmanaged =>
        width switch
        {
            4 => System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(index * 4, 4)),
            8 => System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(index * 8, 8)),
            _ => throw new NotSupportedException($"Offset width {width} not supported."),
        };

    private static void WriteOffset<T>(Span<byte> buffer, int index, int width, long value)
        where T : unmanaged
    {
        switch (width)
        {
            case 4:
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                    buffer.Slice(index * 4, 4), checked((int)value));
                break;
            case 8:
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(
                    buffer.Slice(index * 8, 8), value);
                break;
            default:
                throw new NotSupportedException($"Offset width {width} not supported.");
        }
    }
}
