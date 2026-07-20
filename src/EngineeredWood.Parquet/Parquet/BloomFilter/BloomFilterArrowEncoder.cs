// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using Clast.BloomFilter;

namespace EngineeredWood.Parquet.BloomFilter;

/// <summary>
/// Encodes Arrow array values into a <see cref="SplitBlockBloomFilterBuilder"/>
/// using Parquet plain-encoding byte representations, without boxing.
/// </summary>
internal static class BloomFilterArrowEncoder
{
    /// <summary>
    /// Adds all non-null values from an Arrow array to the bloom filter builder.
    /// </summary>
    public static void AddArrowValues(
        SplitBlockBloomFilterBuilder builder,
        IArrowArray array,
        PhysicalType physicalType,
        int typeLength)
    {
        switch (physicalType)
        {
            case PhysicalType.Boolean:
                AddBooleanValues(builder, (BooleanArray)array);
                break;
            case PhysicalType.Int32:
                AddFixedWidthValues(builder, array, 4);
                break;
            case PhysicalType.Int64:
                AddFixedWidthValues(builder, array, 8);
                break;
            case PhysicalType.Float:
                AddFixedWidthValues(builder, array, 4);
                break;
            case PhysicalType.Double:
                AddFixedWidthValues(builder, array, 8);
                break;
            case PhysicalType.ByteArray:
                AddVariableLengthValues(builder, array);
                break;
            case PhysicalType.FixedLenByteArray:
                AddFixedWidthValues(builder, array, typeLength);
                break;
        }
    }

    private static void AddBooleanValues(SplitBlockBloomFilterBuilder builder, BooleanArray array)
    {
        Span<byte> buf = stackalloc byte[1];
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            buf[0] = array.GetValue(i) == true ? (byte)1 : (byte)0;
            builder.Add(buf);
        }
    }

    /// <summary>
    /// Adds values from fixed-width primitive arrays by reading directly from the values buffer.
    /// Works for Int32, Int64, Float, Double, FixedSizeBinary, Date32, etc.
    /// </summary>
    private static void AddFixedWidthValues(
        SplitBlockBloomFilterBuilder builder, IArrowArray array, int valueSize)
    {
        var data = array.Data;
        // Buffer 1 is the values buffer for primitive arrays.
        if (data.Buffers.Length < 2 || data.Buffers[1].Length == 0) return;

        var values = data.Buffers[1].Span;
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            int offset = (data.Offset + i) * valueSize;
            builder.Add(values.Slice(offset, valueSize));
        }
    }

    /// <summary>
    /// Adds values from variable-length arrays (StringArray, BinaryArray) using offset buffers.
    /// </summary>
    private static void AddVariableLengthValues(
        SplitBlockBloomFilterBuilder builder, IArrowArray array)
    {
        switch (array)
        {
            case StringArray typed:
                for (int i = 0; i < typed.Length; i++)
                {
                    if (typed.IsNull(i)) continue;
                    builder.Add(typed.GetBytes(i));
                }
                break;
            case BinaryArray typed:
                for (int i = 0; i < typed.Length; i++)
                {
                    if (typed.IsNull(i)) continue;
                    builder.Add(typed.GetBytes(i));
                }
                break;
            default:
                // Fallback for LargeString/LargeBinary: use offsets buffer.
                AddLargeVariableLengthValues(builder, array);
                break;
        }
    }

    private static void AddLargeVariableLengthValues(
        SplitBlockBloomFilterBuilder builder, IArrowArray array)
    {
        var data = array.Data;
        if (data.Buffers.Length < 3) return;

        // Buffer 1 = offsets, Buffer 2 = values data.
        var offsets = data.Buffers[1].Span;
        var values = data.Buffers[2].Span;
        int offsetSize = data.DataType is LargeStringType or LargeBinaryType ? 8 : 4;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            int idx = data.Offset + i;

            long start, end;
            if (offsetSize == 8)
            {
                start = BinaryPrimitives.ReadInt64LittleEndian(offsets.Slice(idx * 8));
                end = BinaryPrimitives.ReadInt64LittleEndian(offsets.Slice((idx + 1) * 8));
            }
            else
            {
                start = BinaryPrimitives.ReadInt32LittleEndian(offsets.Slice(idx * 4));
                end = BinaryPrimitives.ReadInt32LittleEndian(offsets.Slice((idx + 1) * 4));
            }

            if (end > start)
                builder.Add(values.Slice((int)start, (int)(end - start)));
        }
    }
}
