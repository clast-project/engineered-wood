// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Builds Apache Arrow arrays from decoded Parquet column data, inserting nulls
/// based on definition levels. Uses native memory buffers to avoid managed heap copies.
/// </summary>
internal static class ArrowArrayBuilder
{
    /// <summary>
    /// Builds a dense flat array for repeated columns. Elements where defLevel &lt; maxDefLevel
    /// are null; all others are present. Used by the list/map assembler to get the inner element array.
    /// </summary>
    public static IArrowArray BuildDense(ColumnBuildState state, Field field, int numValues)
    {
        // For repeated columns, the leaf field type may be wrapped in ListType/MapType.
        // We need the element type for building the flat array.
        var arrowType = field.DataType;

        // The number of non-null values is state.ValueCount.
        // If there are no def levels (required leaf), all values are present — build directly.
        if (!state.IsNullable)
        {
            return BuildNonNullableDense(state, arrowType, numValues);
        }

        // Nullable: build array of numValues length, inserting nulls for elements
        // where defLevel < maxDefLevel
        return arrowType switch
        {
            BooleanType => BuildDenseBooleanArray(state, numValues),
            Int8Type => BuildDenseNarrowIntArray<sbyte>(state, arrowType, numValues),
            UInt8Type => BuildDenseNarrowIntArray<byte>(state, arrowType, numValues),
            Int16Type => BuildDenseNarrowIntArray<short>(state, arrowType, numValues),
            UInt16Type => BuildDenseNarrowIntArray<ushort>(state, arrowType, numValues),
            Int32Type or Date32Type or Time32Type => BuildDenseFixedArray<int>(state, arrowType, numValues),
            UInt32Type => BuildDenseFixedArray<uint>(state, arrowType, numValues),
            Int64Type or TimestampType or Time64Type => BuildDenseFixedArray<long>(state, arrowType, numValues),
            UInt64Type => BuildDenseFixedArray<ulong>(state, arrowType, numValues),
#if NET6_0_OR_GREATER
            HalfFloatType => BuildDenseFixedArray<Half>(state, arrowType, numValues),
#endif
            FloatType => BuildDenseFixedArray<float>(state, arrowType, numValues),
            DoubleType => BuildDenseFixedArray<double>(state, arrowType, numValues),
            StringType => BuildDenseVarBinaryArray(state, arrowType, numValues),
            BinaryType => BuildDenseVarBinaryArray(state, arrowType, numValues),
            StringViewType => BuildDenseStringViewArray(state, arrowType, numValues),
            BinaryViewType => BuildDenseStringViewArray(state, arrowType, numValues),
            LargeStringType => BuildDenseLargeVarBinaryArray(state, arrowType, numValues),
            LargeBinaryType => BuildDenseLargeVarBinaryArray(state, arrowType, numValues),
            Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type
                => BuildDecimalArray(state, arrowType, numValues, dense: true),
            // Any ExtensionType whose storage is FixedSizeBinary (e.g. GuidType
            // wrapping FixedSizeBinaryType(16)): build the storage array with the
            // existing FLBA path, then wrap it via the extension's CreateArray.
            ExtensionType ext when ext.StorageType is FixedSizeBinaryType fsbStorage
                => ext.CreateArray(BuildDenseFixedSizeBinaryArray(state, numValues, fsbStorage)),
            FixedSizeBinaryType fsb => BuildDenseFixedSizeBinaryArray(state, numValues, fsb),
            NullType => BuildNullArray(numValues),
            _ => throw new NotSupportedException(
                $"Arrow type '{arrowType.Name}' is not supported for dense array building."),
        };
    }

    private static IArrowArray BuildNonNullableDense(ColumnBuildState state, IArrowType arrowType, int numValues)
    {
        // Non-nullable: all numValues values are present, buffer is already dense
        if (arrowType is BooleanType)
        {
            var valueBuffer = state.BuildValueBuffer();
            var arrayData = new ArrayData(BooleanType.Default, numValues, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return new BooleanArray(arrayData);
        }

        if (arrowType is StringType or BinaryType)
        {
            var offsetsBuffer = state.BuildOffsetsBuffer();
            var dataBuffer = state.BuildDataBuffer();
            var arrayData = new ArrayData(arrowType, numValues, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, offsetsBuffer, dataBuffer });
            return ArrowArrayFactory.BuildArray(arrayData);
        }

        if (arrowType is LargeStringType or LargeBinaryType)
        {
            var offsetsBuffer = state.BuildLargeOffsetsBuffer();
            var dataBuffer = state.BuildDataBuffer();
            var arrayData = new ArrayData(arrowType, numValues, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, offsetsBuffer, dataBuffer });
            return ArrowArrayFactory.BuildArray(arrayData);
        }

        {
            var valueBuffer = state.BuildValueBuffer();
            var arrayData = new ArrayData(arrowType, numValues, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return ArrowArrayFactory.BuildArray(arrayData);
        }
    }

    private static IArrowArray BuildDenseFixedArray<T>(ColumnBuildState state, IArrowType arrowType, int numValues)
        where T : unmanaged
    {
        var defLevels = state.DefLevelSpan;
        int nullCount = numValues - state.ValueCount;
        int nonNullCount = state.ValueCount;

        int bitmapBytes = (numValues + 7) / 8;
        using var bitmapBuf = new NativeBuffer<byte>(bitmapBytes);
        var bitmap = bitmapBuf.ByteSpan;

        // In-place reverse scatter: reuse the dense buffer, walking right-to-left
        // so each value moves rightward (read index always <= write index).
        var values = state.GetWritableValueSpan<T>();
        int readIdx = nonNullCount - 1;
        for (int i = numValues - 1; i >= 0 && readIdx >= 0; i--)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                values[i] = values[readIdx--];
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        var valueArrow = state.BuildValueBuffer();
        var bitmapArrow = bitmapBuf.Build();

        var data = new ArrayData(arrowType, numValues, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildDenseNarrowIntArray<TNarrow>(ColumnBuildState state, IArrowType arrowType, int numValues)
        where TNarrow : unmanaged
    {
        var sourceValues = state.GetValueSpan<int>();
        var defLevels = state.DefLevelSpan;
        int nullCount = numValues - state.ValueCount;

        using var scatteredBuf = new NativeBuffer<TNarrow>(numValues, zeroFill: false);
        using var bitmapBuf = new NativeBuffer<byte>((numValues + 7) / 8);

        var scattered = scatteredBuf.Span;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                scattered[i] = CastNarrow<TNarrow>(sourceValues[valueIdx++]);
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(arrowType, numValues, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildDenseBooleanArray(ColumnBuildState state, int numValues)
    {
        var defLevels = state.DefLevelSpan;
        var denseBits = state.ValueByteSpan;
        int nullCount = numValues - state.ValueCount;

        using var scatteredBuf = new NativeBuffer<byte>((numValues + 7) / 8);
        using var bitmapBuf = new NativeBuffer<byte>((numValues + 7) / 8);

        var scattered = scatteredBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                bool val = ((denseBits[valueIdx >> 3] >> (valueIdx & 7)) & 1) == 1;
                if (val)
                    scattered[i >> 3] |= (byte)(1 << (i & 7));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(BooleanType.Default, numValues, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return new BooleanArray(data);
    }

    private static IArrowArray BuildDenseVarBinaryArray(ColumnBuildState state, IArrowType arrowType, int numValues)
    {
        var defLevels = state.DefLevelSpan;
        var denseOffsets = state.GetOffsetsSpan();
        int nonNullCount = state.ValueCount;
        int nullCount = numValues - nonNullCount;

        using var scatteredOffsets = new NativeBuffer<int>(numValues + 1, zeroFill: false);
        using var bitmapBuf = new NativeBuffer<byte>((numValues + 7) / 8);

        var offsets = scatteredOffsets.Span;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                offsets[i] = denseOffsets[valueIdx];
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
            else
            {
                offsets[i] = valueIdx < denseOffsets.Length ? denseOffsets[valueIdx] : (nonNullCount > 0 ? denseOffsets[nonNullCount] : 0);
            }
        }
        offsets[numValues] = nonNullCount > 0 ? denseOffsets[nonNullCount] : 0;

        var offsetsArrow = scatteredOffsets.Build();
        var bitmapArrow = bitmapBuf.Build();
        var dataArrow = state.BuildDataBuffer();
        state.DisposeOffsetsBuffer();

        var data = new ArrayData(arrowType, numValues, nullCount, offset: 0,
            new[] { bitmapArrow, offsetsArrow, dataArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildDenseFixedSizeBinaryArray(
        ColumnBuildState state, int numValues, FixedSizeBinaryType fixedType)
    {
        int byteWidth = fixedType.ByteWidth;
        var defLevels = state.DefLevelSpan;
        var denseBytes = state.ValueByteSpan;
        int nullCount = numValues - state.ValueCount;

        using var scatteredBuf = new NativeBuffer<byte>(numValues * byteWidth, zeroFill: false);
        using var bitmapBuf = new NativeBuffer<byte>((numValues + 7) / 8);

        var scattered = scatteredBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                denseBytes.Slice(valueIdx * byteWidth, byteWidth)
                    .CopyTo(scattered.Slice(i * byteWidth, byteWidth));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(fixedType, numValues, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return new FixedSizeBinaryArray(data);
    }

    /// <summary>
    /// Creates an Arrow <see cref="IArrowArray"/> from accumulated column data.
    /// </summary>
    public static IArrowArray Build(ColumnBuildState state, Field field, int rowCount)
    {
        var arrowType = field.DataType;

        return arrowType switch
        {
            BooleanType => BuildBooleanArray(state, rowCount),
            Int8Type => BuildNarrowIntArray<sbyte>(state, arrowType, rowCount),
            UInt8Type => BuildNarrowIntArray<byte>(state, arrowType, rowCount),
            Int16Type => BuildNarrowIntArray<short>(state, arrowType, rowCount),
            UInt16Type => BuildNarrowIntArray<ushort>(state, arrowType, rowCount),
            Int32Type or Date32Type or Time32Type => BuildFixedArray<int>(state, arrowType, rowCount),
            UInt32Type => BuildFixedArray<uint>(state, arrowType, rowCount),
            Int64Type or TimestampType or Time64Type => BuildFixedArray<long>(state, arrowType, rowCount),
            UInt64Type => BuildFixedArray<ulong>(state, arrowType, rowCount),
#if NET6_0_OR_GREATER
            HalfFloatType => BuildFixedArray<Half>(state, arrowType, rowCount),
#endif
            FloatType => BuildFixedArray<float>(state, arrowType, rowCount),
            DoubleType => BuildFixedArray<double>(state, arrowType, rowCount),
            StringType => BuildStringArray(state, rowCount),
            BinaryType => BuildBinaryArray(state, rowCount),
            StringViewType => BuildStringViewArray(state, rowCount),
            BinaryViewType => BuildBinaryViewArray(state, rowCount),
            LargeStringType => BuildLargeStringArray(state, rowCount),
            LargeBinaryType => BuildLargeBinaryArray(state, rowCount),
            Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type
                => BuildDecimalArray(state, arrowType, rowCount, dense: false),
            ExtensionType ext when ext.StorageType is FixedSizeBinaryType fsbStorage
                => ext.CreateArray(BuildFixedSizeBinaryArray(state, rowCount, fsbStorage)),
            FixedSizeBinaryType fsb => BuildFixedSizeBinaryArray(state, rowCount, fsb),
            NullType => BuildNullArray(rowCount),
            _ => throw new NotSupportedException(
                $"Arrow type '{arrowType.Name}' is not supported for array building."),
        };
    }

    private static IArrowArray BuildNullArray(int length)
    {
        return new NullArray(length);
    }

    /// <summary>
    /// Builds a validity bitmap from byte definition levels using SIMD where available.
    /// Bit i is set if defLevels[i] == maxDefLevel.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BuildBitmapFromDefLevels(
        ReadOnlySpan<byte> defLevels, Span<byte> bitmap, int count, byte maxDefLevel)
    {
        BitmapHelper.BuildFromEquality(defLevels, bitmap, count, maxDefLevel);
    }

    private static IArrowArray BuildFixedArray<T>(ColumnBuildState state, IArrowType arrowType, int rowCount)
        where T : unmanaged
    {
        if (!state.IsNullable)
        {
            // Non-nullable: the value buffer is already dense and complete
            var valueBuffer = state.BuildValueBuffer();
            var arrayData = new ArrayData(arrowType, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return ArrowArrayFactory.BuildArray(arrayData);
        }

        int nonNullCount = state.ValueCount;
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;

        int bitmapBytes = (rowCount + 7) / 8;
        using var bitmapBuf = new NativeBuffer<byte>(bitmapBytes);
        var bitmap = bitmapBuf.ByteSpan;

        // Build bitmap using SIMD, then scatter values using the bitmap
        BuildBitmapFromDefLevels(defLevels, bitmap, rowCount, (byte)state.MaxDefLevel);

        var values = state.GetWritableValueSpan<T>();
        int readIdx = nonNullCount - 1;
        for (int i = rowCount - 1; i >= 0 && readIdx >= 0; i--)
        {
            if ((bitmap[i >> 3] & (1 << (i & 7))) != 0)
                values[i] = values[readIdx--];
        }

        var valueArrow = state.BuildValueBuffer();
        var bitmapArrow = bitmapBuf.Build();

        var data = new ArrayData(arrowType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    /// <summary>
    /// Builds arrays for types narrower than their Parquet physical type (Int8/UInt8/Int16/UInt16
    /// are stored as Int32 in Parquet).
    /// </summary>
    private static IArrowArray BuildNarrowIntArray<TNarrow>(ColumnBuildState state, IArrowType arrowType, int rowCount)
        where TNarrow : unmanaged
    {
        var sourceValues = state.GetValueSpan<int>();
        int nonNullCount = state.ValueCount;

        if (!state.IsNullable)
        {
            using var narrowBuf = new NativeBuffer<TNarrow>(rowCount, zeroFill: false);
            var dest = narrowBuf.Span;
            for (int i = 0; i < nonNullCount; i++)
                dest[i] = CastNarrow<TNarrow>(sourceValues[i]);

            var valueBuffer = narrowBuf.Build();
            state.DisposeValueBuffer();
            var arrayData = new ArrayData(arrowType, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return ArrowArrayFactory.BuildArray(arrayData);
        }

        // Nullable narrow
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;

        using var scatteredBuf = new NativeBuffer<TNarrow>(rowCount, zeroFill: false);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var scattered = scatteredBuf.Span;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                scattered[i] = CastNarrow<TNarrow>(sourceValues[valueIdx++]);
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(arrowType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TNarrow CastNarrow<TNarrow>(int value) where TNarrow : unmanaged
    {
        if (typeof(TNarrow) == typeof(sbyte)) return (TNarrow)(object)checked((sbyte)value);
        if (typeof(TNarrow) == typeof(byte)) return (TNarrow)(object)checked((byte)value);
        if (typeof(TNarrow) == typeof(short)) return (TNarrow)(object)checked((short)value);
        if (typeof(TNarrow) == typeof(ushort)) return (TNarrow)(object)checked((ushort)value);
        throw new NotSupportedException($"CastNarrow does not support {typeof(TNarrow).Name}");
    }

    private static IArrowArray BuildBooleanArray(ColumnBuildState state, int rowCount)
    {
        // Boolean values are already bit-packed in the value buffer
        if (!state.IsNullable)
        {
            var valueBuffer = state.BuildValueBuffer();
            var arrayData = new ArrayData(BooleanType.Default, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return new BooleanArray(arrayData);
        }

        // Nullable: scatter bits and build bitmap
        int nonNullCount = state.ValueCount;
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;
        var denseBits = state.ValueByteSpan;

        // Both buffers need zeroing: bits are set via |=
        using var scatteredBuf = new NativeBuffer<byte>((rowCount + 7) / 8);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var scattered = scatteredBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                // Read bit from dense packed buffer
                bool val = ((denseBits[valueIdx >> 3] >> (valueIdx & 7)) & 1) == 1;
                if (val)
                    scattered[i >> 3] |= (byte)(1 << (i & 7));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(BooleanType.Default, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return new BooleanArray(data);
    }

    private static IArrowArray BuildStringArray(ColumnBuildState state, int rowCount)
    {
        if (!state.IsNullable)
        {
            var offsetsBuffer = state.BuildOffsetsBuffer();
            var dataBuffer = state.BuildDataBuffer();
            var arrayData = new ArrayData(Apache.Arrow.Types.StringType.Default, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, offsetsBuffer, dataBuffer });
            return new StringArray(arrayData);
        }

        // Nullable: scatter offsets with null gaps, build bitmap
        return BuildNullableVarBinaryArray(state, Apache.Arrow.Types.StringType.Default, rowCount);
    }

    private static IArrowArray BuildBinaryArray(ColumnBuildState state, int rowCount)
    {
        if (!state.IsNullable)
        {
            var offsetsBuffer = state.BuildOffsetsBuffer();
            var dataBuffer = state.BuildDataBuffer();
            var arrayData = new ArrayData(BinaryType.Default, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, offsetsBuffer, dataBuffer });
            return new BinaryArray(arrayData);
        }

        return BuildNullableVarBinaryArray(state, BinaryType.Default, rowCount);
    }

    private static IArrowArray BuildNullableVarBinaryArray(ColumnBuildState state, IArrowType arrowType, int rowCount)
    {
        int nonNullCount = state.ValueCount;
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;
        var denseOffsets = state.GetOffsetsSpan();
        var denseData = state.GetDataSpan();

        using var scatteredOffsets = new NativeBuffer<int>(rowCount + 1, zeroFill: false);
        int bitmapBytes = (rowCount + 7) / 8;
        using var bitmapBuf = new NativeBuffer<byte>(bitmapBytes);

        var offsets = scatteredOffsets.Span;
        var bitmap = bitmapBuf.ByteSpan;

        BuildBitmapFromDefLevels(defLevels, bitmap, rowCount, (byte)state.MaxDefLevel);

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if ((bitmap[i >> 3] & (1 << (i & 7))) != 0)
            {
                offsets[i] = denseOffsets[valueIdx++];
            }
            else
            {
                offsets[i] = valueIdx < denseOffsets.Length ? denseOffsets[valueIdx] : (nonNullCount > 0 ? denseOffsets[nonNullCount] : 0);
            }
        }
        offsets[rowCount] = nonNullCount > 0 ? denseOffsets[nonNullCount] : 0;

        var offsetsArrow = scatteredOffsets.Build();
        var bitmapArrow = bitmapBuf.Build();
        var dataArrow = state.BuildDataBuffer();
        state.DisposeOffsetsBuffer();

        var data = new ArrayData(arrowType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, offsetsArrow, dataArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildStringViewArray(ColumnBuildState state, int rowCount) =>
        BuildVarViewArray(state, StringViewType.Default, rowCount);

    private static IArrowArray BuildBinaryViewArray(ColumnBuildState state, int rowCount) =>
        BuildVarViewArray(state, BinaryViewType.Default, rowCount);

    private static IArrowArray BuildVarViewArray(ColumnBuildState state, IArrowType arrowType, int rowCount)
    {
        int nonNullCount = state.ValueCount;

        if (!state.IsNullable)
        {
            var viewsArrow = state.BuildViewsBuffer();
            var dataArrow = state.BuildDataBuffer();
            var arrayData = new ArrayData(arrowType, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, viewsArrow, dataArrow });
            return ArrowArrayFactory.BuildArray(arrayData);
        }

        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;
        var denseViews = state.GetDenseViewsSpan();

        // Scatter dense views → sparse views; null rows get 16 zero bytes (valid null view)
        using var sparseViews = new NativeBuffer<byte>(
            rowCount * ColumnBuildState.ViewEntrySize, zeroFill: true);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var outViews = sparseViews.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                denseViews.Slice(valueIdx * ColumnBuildState.ViewEntrySize, ColumnBuildState.ViewEntrySize)
                    .CopyTo(outViews.Slice(i * ColumnBuildState.ViewEntrySize));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
            // null row: 16 zero bytes already there from zeroFill
        }

        var viewsArrowBuf = sparseViews.Build();
        var bitmapArrow = bitmapBuf.Build();
        var dataArrowBuf = state.BuildDataBuffer();

        var data = new ArrayData(arrowType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, viewsArrowBuf, dataArrowBuf });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildDenseStringViewArray(
        ColumnBuildState state, IArrowType arrowType, int numValues)
    {
        // Dense view arrays (repeated columns): all numValues entries present, no nulls/scatter
        int nonNullCount = state.ValueCount;

        if (!state.IsNullable)
        {
            var viewsArrow = state.BuildViewsBuffer();
            var dataArrow = state.BuildDataBuffer();
            var arrayData = new ArrayData(arrowType, numValues, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, viewsArrow, dataArrow });
            return ArrowArrayFactory.BuildArray(arrayData);
        }

        // Nullable dense: same scatter logic
        int nullCount = numValues - nonNullCount;
        var defLevels = state.DefLevelSpan;
        var denseViews = state.GetDenseViewsSpan();

        using var sparseViews = new NativeBuffer<byte>(
            numValues * ColumnBuildState.ViewEntrySize, zeroFill: true);
        using var bitmapBuf = new NativeBuffer<byte>((numValues + 7) / 8);

        var outViews = sparseViews.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                denseViews.Slice(valueIdx * ColumnBuildState.ViewEntrySize, ColumnBuildState.ViewEntrySize)
                    .CopyTo(outViews.Slice(i * ColumnBuildState.ViewEntrySize));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
        }

        var viewsArrowBuf = sparseViews.Build();
        var bitmapArrow = bitmapBuf.Build();
        var dataArrowBuf = state.BuildDataBuffer();

        var data = new ArrayData(arrowType, numValues, nullCount, offset: 0,
            new[] { bitmapArrow, viewsArrowBuf, dataArrowBuf });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildLargeStringArray(ColumnBuildState state, int rowCount) =>
        BuildLargeVarBinaryArray(state, LargeStringType.Default, rowCount);

    private static IArrowArray BuildLargeBinaryArray(ColumnBuildState state, int rowCount) =>
        BuildLargeVarBinaryArray(state, LargeBinaryType.Default, rowCount);

    private static IArrowArray BuildLargeVarBinaryArray(ColumnBuildState state, IArrowType arrowType, int rowCount)
    {
        if (!state.IsNullable)
        {
            var offsetsBuffer = state.BuildLargeOffsetsBuffer();
            var dataBuffer = state.BuildDataBuffer();
            var arrayData = new ArrayData(arrowType, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, offsetsBuffer, dataBuffer });
            return ArrowArrayFactory.BuildArray(arrayData);
        }

        return BuildNullableLargeVarBinaryArray(state, arrowType, rowCount);
    }

    private static IArrowArray BuildNullableLargeVarBinaryArray(
        ColumnBuildState state, IArrowType arrowType, int rowCount)
    {
        int nonNullCount = state.ValueCount;
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;
        var denseOffsets = state.GetLargeOffsetsSpan();
        var denseData = state.GetDataSpan();

        using var scatteredOffsets = new NativeBuffer<long>(rowCount + 1, zeroFill: false);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var offsets = scatteredOffsets.Span;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                offsets[i] = denseOffsets[valueIdx];
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
            else
            {
                offsets[i] = valueIdx < denseOffsets.Length ? denseOffsets[valueIdx] : (nonNullCount > 0 ? denseOffsets[nonNullCount] : 0L);
            }
        }
        offsets[rowCount] = nonNullCount > 0 ? denseOffsets[nonNullCount] : 0L;

        var offsetsArrow = scatteredOffsets.Build();
        var bitmapArrow = bitmapBuf.Build();
        var dataArrow = state.BuildDataBuffer();
        state.DisposeLargeOffsetsBuffer();

        var data = new ArrayData(arrowType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, offsetsArrow, dataArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildDenseLargeVarBinaryArray(
        ColumnBuildState state, IArrowType arrowType, int numValues)
    {
        var defLevels = state.DefLevelSpan;
        var denseOffsets = state.GetLargeOffsetsSpan();
        int nonNullCount = state.ValueCount;
        int nullCount = numValues - nonNullCount;

        using var scatteredOffsets = new NativeBuffer<long>(numValues + 1, zeroFill: false);
        using var bitmapBuf = new NativeBuffer<byte>((numValues + 7) / 8);

        var offsets = scatteredOffsets.Span;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                offsets[i] = denseOffsets[valueIdx];
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
            else
            {
                offsets[i] = valueIdx < denseOffsets.Length ? denseOffsets[valueIdx] : (nonNullCount > 0 ? denseOffsets[nonNullCount] : 0L);
            }
        }
        offsets[numValues] = nonNullCount > 0 ? denseOffsets[nonNullCount] : 0L;

        var offsetsArrow = scatteredOffsets.Build();
        var bitmapArrow = bitmapBuf.Build();
        var dataArrow = state.BuildDataBuffer();
        state.DisposeLargeOffsetsBuffer();

        var data = new ArrayData(arrowType, numValues, nullCount, offset: 0,
            new[] { bitmapArrow, offsetsArrow, dataArrow });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static IArrowArray BuildFixedSizeBinaryArray(
        ColumnBuildState state, int rowCount, FixedSizeBinaryType fixedType)
    {
        int byteWidth = fixedType.ByteWidth;

        if (!state.IsNullable)
        {
            var valueBuffer = state.BuildValueBuffer();
            var arrayData = new ArrayData(fixedType, rowCount, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return new FixedSizeBinaryArray(arrayData);
        }

        // Nullable: scatter fixed-size values
        int nonNullCount = state.ValueCount;
        int nullCount = rowCount - nonNullCount;
        var defLevels = state.DefLevelSpan;
        var denseBytes = state.ValueByteSpan;

        using var scatteredBuf = new NativeBuffer<byte>(rowCount * byteWidth, zeroFill: false);
        using var bitmapBuf = new NativeBuffer<byte>((rowCount + 7) / 8);

        var scattered = scatteredBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels[i] == state.MaxDefLevel)
            {
                denseBytes.Slice(valueIdx * byteWidth, byteWidth)
                    .CopyTo(scattered.Slice(i * byteWidth, byteWidth));
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
        }

        var valueArrow = scatteredBuf.Build();
        var bitmapArrow = bitmapBuf.Build();
        state.DisposeValueBuffer();

        var data = new ArrayData(fixedType, rowCount, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return new FixedSizeBinaryArray(data);
    }

    /// <summary>
    /// Builds a decimal Arrow array from column data. Handles all physical type sources:
    /// INT32/INT64 (reinterpret as little-endian bytes) and FLBA/ByteArray (reverse from big-endian + sign-extend).
    /// </summary>
    private static IArrowArray BuildDecimalArray(ColumnBuildState state, IArrowType arrowType, int count, bool dense)
    {
        int byteWidth = arrowType switch
        {
            Decimal32Type => 4,
            Decimal64Type => 8,
            Decimal128Type => 16,
            Decimal256Type => 32,
            _ => throw new NotSupportedException($"Unexpected decimal type: {arrowType.Name}"),
        };

        var physicalType = state.PhysicalType;

        if (physicalType == PhysicalType.Int32)
            return BuildDecimalFromInt32(state, arrowType, count, byteWidth, dense);
        if (physicalType == PhysicalType.Int64)
            return BuildDecimalFromInt64(state, arrowType, count, byteWidth, dense);

        // FLBA or ByteArray: raw bytes, big-endian → little-endian + sign-extend to target width
        return BuildDecimalFromBytes(state, arrowType, count, byteWidth, dense);
    }

    private static IArrowArray BuildDecimalFromInt32(
        ColumnBuildState state, IArrowType arrowType, int count, int byteWidth, bool dense)
    {
        var denseValues = state.GetValueSpan<int>();
        var defLevels = state.IsNullable ? state.DefLevelSpan : default;
        int nullCount = state.IsNullable ? count - state.ValueCount : 0;

        using var valueBuf = new NativeBuffer<byte>(count * byteWidth);
        using var bitmapBuf = new NativeBuffer<byte>((count + 7) / 8);

        var values = valueBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < count; i++)
        {
            if (!state.IsNullable || defLevels[i] == state.MaxDefLevel)
            {
                int val = denseValues[valueIdx++];
                var slot = values.Slice(i * byteWidth, byteWidth);
                byte fill = val < 0 ? (byte)0xFF : (byte)0x00;
                slot.Fill(fill);
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(slot, val);
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        var valueArrow = valueBuf.Build();
        var bitmapArrow = state.IsNullable ? bitmapBuf.Build() : ArrowBuffer.Empty;
        state.DisposeValueBuffer();

        var data = new ArrayData(arrowType, count, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return BuildDecimalArrayFromData(data);
    }

    private static IArrowArray BuildDecimalFromInt64(
        ColumnBuildState state, IArrowType arrowType, int count, int byteWidth, bool dense)
    {
        var denseValues = state.GetValueSpan<long>();
        var defLevels = state.IsNullable ? state.DefLevelSpan : default;
        int nullCount = state.IsNullable ? count - state.ValueCount : 0;

        using var valueBuf = new NativeBuffer<byte>(count * byteWidth);
        using var bitmapBuf = new NativeBuffer<byte>((count + 7) / 8);

        var values = valueBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < count; i++)
        {
            if (!state.IsNullable || defLevels[i] == state.MaxDefLevel)
            {
                long val = denseValues[valueIdx++];
                var slot = values.Slice(i * byteWidth, byteWidth);
                byte fill = val < 0 ? (byte)0xFF : (byte)0x00;
                slot.Fill(fill);
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(slot, val);
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        var valueArrow = valueBuf.Build();
        var bitmapArrow = state.IsNullable ? bitmapBuf.Build() : ArrowBuffer.Empty;
        state.DisposeValueBuffer();

        var data = new ArrayData(arrowType, count, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return BuildDecimalArrayFromData(data);
    }

    private static IArrowArray BuildDecimalFromBytes(
        ColumnBuildState state, IArrowType arrowType, int count, int byteWidth, bool dense)
    {
        if (state.PhysicalType == PhysicalType.ByteArray)
            return BuildDecimalFromVarBytes(state, arrowType, count, byteWidth, dense);

        // FLBA: fixed source width
        var denseBytes = state.ValueByteSpan;
        int sourceByteWidth = state.ValueCount > 0 ? denseBytes.Length / state.ValueCount : 0;

        var defLevels = state.IsNullable ? state.DefLevelSpan : default;
        int nullCount = state.IsNullable ? count - state.ValueCount : 0;

        using var valueBuf = new NativeBuffer<byte>(count * byteWidth);
        using var bitmapBuf = new NativeBuffer<byte>((count + 7) / 8);

        var values = valueBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < count; i++)
        {
            if (!state.IsNullable || defLevels[i] == state.MaxDefLevel)
            {
                var src = denseBytes.Slice(valueIdx * sourceByteWidth, sourceByteWidth);
                var dst = values.Slice(i * byteWidth, byteWidth);
                ReverseAndSignExtend(src, dst);
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
        }

        var valueArrow = valueBuf.Build();
        var bitmapArrow = state.IsNullable ? bitmapBuf.Build() : ArrowBuffer.Empty;
        state.DisposeValueBuffer();

        var data = new ArrayData(arrowType, count, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return BuildDecimalArrayFromData(data);
    }

    private static IArrowArray BuildDecimalFromVarBytes(
        ColumnBuildState state, IArrowType arrowType, int count, int byteWidth, bool dense)
    {
        var offsets = state.GetOffsetsSpan();
        var dataSpan = state.GetDataSpan();
        var defLevels = state.IsNullable ? state.DefLevelSpan : default;
        int nullCount = state.IsNullable ? count - state.ValueCount : 0;

        using var valueBuf = new NativeBuffer<byte>(count * byteWidth);
        using var bitmapBuf = new NativeBuffer<byte>((count + 7) / 8);

        var values = valueBuf.ByteSpan;
        var bitmap = bitmapBuf.ByteSpan;

        int valueIdx = 0;
        for (int i = 0; i < count; i++)
        {
            if (!state.IsNullable || defLevels[i] == state.MaxDefLevel)
            {
                int start = offsets[valueIdx];
                int end = offsets[valueIdx + 1];
                var src = dataSpan.Slice(start, end - start);
                var dst = values.Slice(i * byteWidth, byteWidth);
                ReverseAndSignExtend(src, dst);
                bitmap[i >> 3] |= (byte)(1 << (i & 7));
                valueIdx++;
            }
        }

        var valueArrow = valueBuf.Build();
        var bitmapArrow = state.IsNullable ? bitmapBuf.Build() : ArrowBuffer.Empty;
        state.DisposeOffsetsBuffer();

        var data = new ArrayData(arrowType, count, nullCount, offset: 0,
            new[] { bitmapArrow, valueArrow });
        return BuildDecimalArrayFromData(data);
    }

    /// <summary>
    /// Reverses big-endian Parquet decimal bytes to little-endian and sign-extends to the target width.
    /// </summary>
    private static void ReverseAndSignExtend(ReadOnlySpan<byte> source, Span<byte> target)
    {
        // Sign-extend: fill with 0xFF if negative (high bit of big-endian first byte), else 0x00
        byte fill = (source.Length > 0 && (source[0] & 0x80) != 0) ? (byte)0xFF : (byte)0x00;
        target.Fill(fill);

        // Reverse big-endian source into little-endian target
        int len = Math.Min(source.Length, target.Length);
        for (int j = 0; j < len; j++)
            target[j] = source[source.Length - 1 - j];
    }

    private static IArrowArray BuildDecimalArrayFromData(ArrayData data) =>
        data.DataType switch
        {
            Decimal32Type => new Decimal32Array(data),
            Decimal64Type => new Decimal64Array(data),
            Decimal128Type => new Decimal128Array(data),
            Decimal256Type => new Decimal256Array(data),
            _ => throw new NotSupportedException($"Unexpected decimal type: {data.DataType.Name}"),
        };
}

/// <summary>
/// Accumulates decoded values and definition levels across multiple data pages for one column,
/// using native memory buffers to avoid managed heap allocations.
/// </summary>
internal sealed class ColumnBuildState : IDisposable
{
    private readonly PhysicalType _physicalType;
    internal readonly int MaxDefLevel;
    internal readonly int MaxRepLevel;

    // Definition levels (nullable columns only) — pooled managed array, never transferred to Arrow
    private byte[]? _defLevels;
    private int _defLevelCount;

    // Repetition levels (repeated columns only) — pooled managed array, never transferred to Arrow
    private byte[]? _repLevels;
    private int _repLevelCount;

    // Value buffer: stores dense non-null values for fixed-width types and fixed-len byte arrays
    private NativeBuffer<byte>? _valueBuffer;
    private int _valueByteOffset;
    private int _valueCount;

    // For Boolean: track bit offset since values are bit-packed
    private int _boolBitOffset;

    // For ByteArray/String (offset mode): separate offsets and data buffers
    private NativeBuffer<int>? _offsetsBuffer;
    private NativeBuffer<long>? _largeOffsetsBuffer; // used when ByteArrayOutput == LargeOffsets
    private int _offsetsCount; // number of offset entries written (= _valueCount + 1 after first page)
    private NativeBuffer<byte>? _dataBuffer;
    private int _dataByteOffset;

    // For ByteArray/String (view mode): 16-byte views buffer + overflow data buffer
    private readonly ByteArrayOutputKind _byteArrayOutput;
    private NativeBuffer<byte>? _viewsBuffer;   // 16 bytes per non-null value (dense)
    private int _viewsCount;

    private readonly int _capacity;
    private readonly int _elementSize; // bytes per element for fixed-width types

    // View entry layout (16 bytes):
    //   [0..3]  int32 Length
    //   Short (Length <= 12): [4..15] inline data (padded with 0s)
    //   Long  (Length >  12): [4..7] 4-byte prefix, [8..11] int32 buf_index=0, [12..15] int32 buf_offset
    private const int MaxInlineLength = 12;
    internal const int ViewEntrySize = 16;

    /// <summary>The Parquet physical type for this column.</summary>
    public PhysicalType PhysicalType => _physicalType;

    /// <summary>Whether this column is nullable (has def levels).</summary>
    public bool IsNullable => MaxDefLevel > 0;

    /// <summary>Number of non-null values accumulated.</summary>
    public int ValueCount => _valueCount;

    /// <summary>Whether this column is in view mode (produces StringViewArray/BinaryViewArray).</summary>
    public bool IsViewMode => _byteArrayOutput == ByteArrayOutputKind.ViewType;

    /// <summary>
    /// Creates a new column build state.
    /// </summary>
    /// <param name="physicalType">Parquet physical type.</param>
    /// <param name="maxDefLevel">Maximum definition level.</param>
    /// <param name="maxRepLevel">Maximum repetition level (0 for flat/struct-only columns).</param>
    /// <param name="capacity">Buffer capacity: rowCount for flat columns, numValues for repeated.</param>
    /// <param name="byteArrayOutput">Controls the Arrow output type for BYTE_ARRAY columns.</param>
    public ColumnBuildState(PhysicalType physicalType, int maxDefLevel, int maxRepLevel, int capacity,
        ByteArrayOutputKind byteArrayOutput = ByteArrayOutputKind.Default)
    {
        _physicalType = physicalType;
        MaxDefLevel = maxDefLevel;
        MaxRepLevel = maxRepLevel;
        _capacity = capacity;
        _byteArrayOutput = byteArrayOutput;

        if (maxDefLevel > 0)
        {
            _defLevels = ArrayPool<byte>.Shared.Rent(capacity);
            _defLevelCount = 0;
        }

        if (maxRepLevel > 0)
        {
            _repLevels = ArrayPool<byte>.Shared.Rent(capacity);
            _repLevelCount = 0;
        }

        switch (physicalType)
        {
            case PhysicalType.Boolean:
                _elementSize = 0; // bit-packed, special handling
                // Boolean bits are set via |=, so buffer must start zeroed
                _valueBuffer = new NativeBuffer<byte>((capacity + 7) / 8, zeroFill: true);
                break;
            case PhysicalType.Int32:
                _elementSize = sizeof(int);
                _valueBuffer = new NativeBuffer<byte>(capacity * _elementSize, zeroFill: false);
                break;
            case PhysicalType.Int64:
                _elementSize = sizeof(long);
                _valueBuffer = new NativeBuffer<byte>(capacity * _elementSize, zeroFill: false);
                break;
            case PhysicalType.Float:
                _elementSize = sizeof(float);
                _valueBuffer = new NativeBuffer<byte>(capacity * _elementSize, zeroFill: false);
                break;
            case PhysicalType.Double:
                _elementSize = sizeof(double);
                _valueBuffer = new NativeBuffer<byte>(capacity * _elementSize, zeroFill: false);
                break;
            case PhysicalType.Int96:
                _elementSize = 12;
                _valueBuffer = new NativeBuffer<byte>(capacity * 12, zeroFill: false);
                break;
            case PhysicalType.FixedLenByteArray:
                // elementSize will be set on first decode (needs TypeLength from column)
                _elementSize = 0;
                break;
            case PhysicalType.ByteArray:
                _elementSize = 0;
                if (byteArrayOutput == ByteArrayOutputKind.ViewType)
                {
                    _viewsBuffer = new NativeBuffer<byte>(capacity * ViewEntrySize, zeroFill: false);
                    _dataBuffer = new NativeBuffer<byte>(capacity * 32, zeroFill: false); // overflow
                }
                else if (byteArrayOutput == ByteArrayOutputKind.LargeOffsets)
                {
                    _largeOffsetsBuffer = new NativeBuffer<long>(capacity + 1, zeroFill: false);
                    _largeOffsetsBuffer.Span[0] = 0;
                    _offsetsCount = 1;
                    _dataBuffer = new NativeBuffer<byte>(capacity * 32, zeroFill: false);
                }
                else
                {
                    _offsetsBuffer = new NativeBuffer<int>(capacity + 1, zeroFill: false);
                    _offsetsBuffer.Span[0] = 0;
                    _offsetsCount = 1;
                    _dataBuffer = new NativeBuffer<byte>(capacity * 32, zeroFill: false);
                }
                break;
        }
    }

    /// <summary>Returns true if the value at position <paramref name="rowIndex"/> is null.</summary>
    public bool IsNull(int rowIndex) =>
        MaxDefLevel > 0 && _defLevels![rowIndex] < (byte)MaxDefLevel;

    /// <summary>Gets the definition levels span (for the build phase).</summary>
    public ReadOnlySpan<byte> DefLevelSpan => _defLevels.AsSpan(0, _defLevelCount);

    /// <summary>Gets the repetition levels span (for the build phase).</summary>
    public ReadOnlySpan<byte> RepLevelSpan => _repLevels.AsSpan(0, _repLevelCount);

    /// <summary>
    /// Reserves space for <paramref name="count"/> definition levels and returns a writable span.
    /// </summary>
    public Span<byte> ReserveDefLevels(int count)
    {
        if (_defLevels == null) return Span<byte>.Empty;
        var span = _defLevels.AsSpan(_defLevelCount, count);
        _defLevelCount += count;
        return span;
    }

    /// <summary>
    /// Reserves space for <paramref name="count"/> repetition levels and returns a writable span.
    /// </summary>
    public Span<byte> ReserveRepLevels(int count)
    {
        if (_repLevels == null) return Span<byte>.Empty;
        var span = _repLevels.AsSpan(_repLevelCount, count);
        _repLevelCount += count;
        return span;
    }

    /// <summary>
    /// Reserves space for <paramref name="count"/> values of type <typeparamref name="T"/>
    /// and returns a writable span into the native value buffer.
    /// </summary>
    public Span<T> ReserveValues<T>(int count) where T : unmanaged
    {
        int byteSize = count * Unsafe.SizeOf<T>();
        var byteSpan = _valueBuffer!.ByteSpan.Slice(_valueByteOffset, byteSize);
        _valueByteOffset += byteSize;
        _valueCount += count;
        return MemoryMarshal.Cast<byte, T>(byteSpan);
    }

    /// <summary>
    /// Reserves space for <paramref name="count"/> boolean values (bit-packed).
    /// Writes the decoded booleans into the native bit buffer.
    /// </summary>
    public void AddBoolValues(ReadOnlySpan<bool> values)
    {
        var buf = _valueBuffer!.ByteSpan;
        for (int i = 0; i < values.Length; i++)
        {
            int bitPos = _boolBitOffset + i;
            if (values[i])
                buf[bitPos >> 3] |= (byte)(1 << (bitPos & 7));
        }
        _boolBitOffset += values.Length;
        _valueCount += values.Length;
    }

    /// <summary>
    /// Reserves space for <paramref name="count"/> fixed-length byte values and returns a writable span.
    /// </summary>
    public Span<byte> ReserveFixedBytes(int count, int typeLength)
    {
        // Lazy init for FixedLenByteArray (needs typeLength from column descriptor)
        if (_valueBuffer == null)
            _valueBuffer = new NativeBuffer<byte>(_capacity * typeLength, zeroFill: false);

        int byteSize = count * typeLength;
        var span = _valueBuffer.ByteSpan.Slice(_valueByteOffset, byteSize);
        _valueByteOffset += byteSize;
        _valueCount += count;
        return span;
    }

    /// <summary>
    /// Adds BYTE_ARRAY values: writes offsets and copies data into native buffers,
    /// or writes view/large-offset entries when in the appropriate mode.
    /// </summary>
    public void AddByteArrayValues(ReadOnlySpan<int> sourceOffsets, ReadOnlySpan<byte> sourceData, int count)
    {
        if (_byteArrayOutput == ByteArrayOutputKind.ViewType)
        {
            for (int i = 0; i < count; i++)
            {
                int start = sourceOffsets[i];
                int end = sourceOffsets[i + 1];
                WriteOneStringView(sourceData.Slice(start, end - start));
            }
            _valueCount += count;
            return;
        }

        // Ensure data buffer has enough space
        int dataNeeded = _dataByteOffset + sourceData.Length;
        if (dataNeeded > _dataBuffer!.ByteSpan.Length)
            _dataBuffer.Grow(dataNeeded);

        // Copy data
        sourceData.CopyTo(_dataBuffer.ByteSpan.Slice(_dataByteOffset));

        // Write offsets (shifted by current data offset)
        if (_byteArrayOutput == ByteArrayOutputKind.LargeOffsets)
        {
            var largeOffsets = _largeOffsetsBuffer!.Span;
            for (int i = 0; i < count; i++)
                largeOffsets[_offsetsCount + i] = _dataByteOffset + sourceOffsets[i + 1];
        }
        else
        {
            var offsets = _offsetsBuffer!.Span;
            for (int i = 0; i < count; i++)
                offsets[_offsetsCount + i] = _dataByteOffset + sourceOffsets[i + 1];
        }
        _offsetsCount += count;
        _dataByteOffset += sourceData.Length;
        _valueCount += count;
    }

    /// <summary>
    /// Reserves <paramref name="byteCount"/> bytes in the data buffer and returns a writable
    /// span for the caller to fill directly. Must be followed by
    /// <see cref="CommitByteArrayData"/> to write offsets and advance counters.
    /// </summary>
    internal Span<byte> ReserveByteArrayData(int byteCount)
    {
        int needed = _dataByteOffset + byteCount;
        if (needed > _dataBuffer!.ByteSpan.Length)
            _dataBuffer.Grow(needed);
        return _dataBuffer.ByteSpan.Slice(_dataByteOffset, byteCount);
    }

    /// <summary>
    /// Commits <paramref name="count"/> byte-array offsets and advances the data write
    /// position by <paramref name="byteCount"/>. Call after filling the span returned
    /// by <see cref="ReserveByteArrayData"/>.
    /// </summary>
    internal void CommitByteArrayData(ReadOnlySpan<int> valueOffsets, int count, int byteCount)
    {
        if (_byteArrayOutput == ByteArrayOutputKind.LargeOffsets)
        {
            var largeOffsets = _largeOffsetsBuffer!.Span;
            for (int i = 0; i < count; i++)
                largeOffsets[_offsetsCount + i] = _dataByteOffset + valueOffsets[i + 1];
        }
        else
        {
            var offsets = _offsetsBuffer!.Span;
            for (int i = 0; i < count; i++)
                offsets[_offsetsCount + i] = _dataByteOffset + valueOffsets[i + 1];
        }
        _offsetsCount += count;
        _dataByteOffset += byteCount;
        _valueCount += count;
    }

    /// <summary>
    /// Writes a single string/binary view entry into the views buffer.
    /// Values ≤12 bytes are stored inline; longer values are appended to the
    /// overflow data buffer and referenced by (buffer_index=0, buffer_offset).
    /// Call only when <see cref="IsViewMode"/> is true.
    /// </summary>
    internal void WriteOneStringView(ReadOnlySpan<byte> value)
    {
        int len = value.Length;
        Span<byte> viewsBuf = _viewsBuffer!.ByteSpan;
        int viewBase = _viewsCount * ViewEntrySize;


#if NET8_0_OR_GREATER
        MemoryMarshal.Write(viewsBuf.Slice(viewBase), in len);
#else
        MemoryMarshal.Write(viewsBuf.Slice(viewBase), ref len);
#endif

        if (len <= MaxInlineLength)
        {
            // Inline: zero the 12-byte data area then copy value bytes
            viewsBuf.Slice(viewBase + 4, MaxInlineLength).Clear();
            value.CopyTo(viewsBuf.Slice(viewBase + 4));
        }
        else
        {
            // Reference: write 4-byte prefix, then buf_index=0, buf_offset
            value.Slice(0, 4).CopyTo(viewsBuf.Slice(viewBase + 4));
            int bufIdx = 0;
#if NET8_0_OR_GREATER
            MemoryMarshal.Write(viewsBuf.Slice(viewBase + 8), in bufIdx);
#else
            MemoryMarshal.Write(viewsBuf.Slice(viewBase + 8), ref bufIdx);
#endif
            int bufOff = _dataByteOffset;
#if NET8_0_OR_GREATER
            MemoryMarshal.Write(viewsBuf.Slice(viewBase + 12), in bufOff);
#else
            MemoryMarshal.Write(viewsBuf.Slice(viewBase + 12), ref bufOff);
#endif

            // Append to overflow buffer
            int needed = _dataByteOffset + len;
            if (needed > _dataBuffer!.ByteSpan.Length)
                _dataBuffer.Grow(needed);
            value.CopyTo(_dataBuffer.ByteSpan.Slice(_dataByteOffset));
            _dataByteOffset += len;
        }

        _viewsCount++;
        _valueCount++;
    }

    /// <summary>
    /// Batch-writes PLAIN-encoded BYTE_ARRAY values as string/binary views.
    /// Pre-reserves overflow space to avoid per-value growth checks.
    /// </summary>
    internal void WritePlainByteArrayViews(ReadOnlySpan<byte> data, int count)
    {
        // First pass: measure total overflow (values > 12 bytes)
        int totalOverflow = 0;
        int srcPos = 0;
        for (int i = 0; i < count; i++)
        {
            int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(srcPos));
            srcPos += 4 + len;
            if (len > MaxInlineLength)
                totalOverflow += len;
        }

        // Pre-reserve overflow space once
        if (totalOverflow > 0)
        {
            int needed = _dataByteOffset + totalOverflow;
            if (needed > _dataBuffer!.ByteSpan.Length)
                _dataBuffer.Grow(needed);
        }

        // Second pass: write views with no growth checks
        Span<byte> viewsBuf = _viewsBuffer!.ByteSpan;
        Span<byte> dataBuf = totalOverflow > 0 ? _dataBuffer!.ByteSpan : Span<byte>.Empty;
        srcPos = 0;
        for (int i = 0; i < count; i++)
        {
            int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(srcPos));
            srcPos += 4;
            var value = data.Slice(srcPos, len);
            srcPos += len;

            int viewBase = _viewsCount * ViewEntrySize;
#if NET8_0_OR_GREATER
            MemoryMarshal.Write(viewsBuf.Slice(viewBase), in len);
#else
            MemoryMarshal.Write(viewsBuf.Slice(viewBase), ref len);
#endif

            if (len <= MaxInlineLength)
            {
                viewsBuf.Slice(viewBase + 4, MaxInlineLength).Clear();
                value.CopyTo(viewsBuf.Slice(viewBase + 4));
            }
            else
            {
                value.Slice(0, 4).CopyTo(viewsBuf.Slice(viewBase + 4));
                int bufIdx = 0;
#if NET8_0_OR_GREATER
                MemoryMarshal.Write(viewsBuf.Slice(viewBase + 8), in bufIdx);
#else
                MemoryMarshal.Write(viewsBuf.Slice(viewBase + 8), ref bufIdx);
#endif
                int bufOff = _dataByteOffset;
#if NET8_0_OR_GREATER
                MemoryMarshal.Write(viewsBuf.Slice(viewBase + 12), in bufOff);
#else
                MemoryMarshal.Write(viewsBuf.Slice(viewBase + 12), ref bufOff);
#endif
                value.CopyTo(dataBuf.Slice(_dataByteOffset));
                _dataByteOffset += len;
            }

            _viewsCount++;
            _valueCount++;
        }
    }

    /// <summary>Gets the dense views buffer as a read-only byte span (for the build phase).</summary>
    internal ReadOnlySpan<byte> GetDenseViewsSpan() =>
        _viewsBuffer!.ByteSpan.Slice(0, _viewsCount * ViewEntrySize);

    /// <summary>Transfers the views buffer to an ArrowBuffer.</summary>
    internal ArrowBuffer BuildViewsBuffer() => _viewsBuffer!.Build();

    // --- Build methods: transfer ownership to ArrowBuffer ---

    /// <summary>Gets a typed span over the dense value data (for the build phase).</summary>
    public ReadOnlySpan<T> GetValueSpan<T>() where T : unmanaged
    {
        if (_valueBuffer == null) return ReadOnlySpan<T>.Empty;
        var bytes = _valueBuffer.ByteSpan.Slice(0, _valueByteOffset);
        return MemoryMarshal.Cast<byte, T>(bytes);
    }

    /// <summary>
    /// Gets a writable span over the FULL value buffer capacity (rowCount elements).
    /// Used for in-place scatter during nullable array assembly — the dense values
    /// at [0, ValueCount) are scattered rightward into their sparse row positions.
    /// </summary>
    internal Span<T> GetWritableValueSpan<T>() where T : unmanaged
    {
        if (_valueBuffer == null) return Span<T>.Empty;
        return MemoryMarshal.Cast<byte, T>(_valueBuffer.ByteSpan);
    }

    /// <summary>Gets a span over the raw value bytes (for boolean and fixed-size binary build).</summary>
    public ReadOnlySpan<byte> ValueByteSpan =>
        _valueBuffer == null ? ReadOnlySpan<byte>.Empty
        : _valueBuffer.ByteSpan.Slice(0, _physicalType == PhysicalType.Boolean
            ? (_boolBitOffset + 7) / 8
            : _valueByteOffset);

    /// <summary>Transfers the value buffer to an ArrowBuffer.</summary>
    public ArrowBuffer BuildValueBuffer()
    {
        return _valueBuffer?.Build() ?? ArrowBuffer.Empty;
    }

    /// <summary>Disposes the value buffer (after data has been copied elsewhere).</summary>
    public void DisposeValueBuffer()
    {
        _valueBuffer?.Dispose();
        _valueBuffer = null;
    }

    /// <summary>Gets the offsets span (for byte array build).</summary>
    public ReadOnlySpan<int> GetOffsetsSpan() =>
        _offsetsBuffer!.Span.Slice(0, _offsetsCount);

    /// <summary>Gets the large offsets span (for large byte array build).</summary>
    public ReadOnlySpan<long> GetLargeOffsetsSpan() =>
        _largeOffsetsBuffer!.Span.Slice(0, _offsetsCount);

    /// <summary>Gets the data span (for byte array build).</summary>
    public ReadOnlySpan<byte> GetDataSpan() =>
        _dataBuffer!.ByteSpan.Slice(0, _dataByteOffset);

    /// <summary>Transfers the offsets buffer to an ArrowBuffer.</summary>
    public ArrowBuffer BuildOffsetsBuffer()
    {
        return _offsetsBuffer!.Build();
    }

    /// <summary>Disposes the offsets buffer.</summary>
    public void DisposeOffsetsBuffer()
    {
        _offsetsBuffer?.Dispose();
        _offsetsBuffer = null;
    }

    /// <summary>Transfers the large offsets buffer to an ArrowBuffer.</summary>
    public ArrowBuffer BuildLargeOffsetsBuffer()
    {
        return _largeOffsetsBuffer!.Build();
    }

    /// <summary>Disposes the large offsets buffer.</summary>
    public void DisposeLargeOffsetsBuffer()
    {
        _largeOffsetsBuffer?.Dispose();
        _largeOffsetsBuffer = null;
    }

    /// <summary>Transfers the data buffer to an ArrowBuffer.</summary>
    public ArrowBuffer BuildDataBuffer()
    {
        return _dataBuffer!.Build();
    }

    /// <summary>
    /// Trims the accumulated state so that only the first <paramref name="maxRows"/> rows
    /// are retained. For flat columns (MaxRepLevel == 0) this limits def levels to
    /// <paramref name="maxRows"/> entries. For repeated columns it scans rep levels to
    /// find the value cutoff corresponding to <paramref name="maxRows"/> row boundaries.
    /// </summary>
    internal void LimitToRows(int maxRows)
    {
        if (MaxRepLevel > 0)
        {
            // Repeated: find value count for maxRows rows by counting rep-level-zero entries.
            var reps = _repLevels.AsSpan(0, _repLevelCount);
            int rowsSeen = 0;
            int valueCutoff = 0;
            for (int i = 0; i < reps.Length; i++)
            {
                if (reps[i] == 0)
                    rowsSeen++;
                if (rowsSeen > maxRows)
                    break;
                valueCutoff = i + 1;
            }
            _repLevelCount = valueCutoff;
            LimitValues(valueCutoff);
        }
        else
        {
            LimitValues(maxRows);
        }
    }

    /// <summary>
    /// Trims def levels and value counters to exactly <paramref name="count"/> entries.
    /// </summary>
    private void LimitValues(int count)
    {
        if (_defLevels != null && _defLevelCount > count)
        {
            // Nullable: recount non-nulls from the first 'count' def levels.
            int nonNulls = 0;
            for (int i = 0; i < count; i++)
            {
                if (_defLevels[i] == MaxDefLevel)
                    nonNulls++;
            }
            _defLevelCount = count;
            _valueCount = nonNulls;
            AdjustValueCounters(nonNulls);
        }
        else if (_defLevels == null && _valueCount > count)
        {
            // Non-nullable: value count == row count.
            _valueCount = count;
            AdjustValueCounters(count);
        }
    }

    private void AdjustValueCounters(int newValueCount)
    {
        switch (_physicalType)
        {
            case PhysicalType.Boolean:
                _boolBitOffset = newValueCount;
                break;
            case PhysicalType.ByteArray:
                if (_byteArrayOutput == ByteArrayOutputKind.ViewType)
                    _viewsCount = newValueCount;
                else
                    _offsetsCount = newValueCount + 1;
                // _dataByteOffset is left unchanged — extra data bytes are harmless.
                break;
            default:
                _valueByteOffset = newValueCount * _elementSize;
                break;
        }
    }

    public void Dispose()
    {
        if (_defLevels != null)
        {
            ArrayPool<byte>.Shared.Return(_defLevels);
            _defLevels = null;
        }
        if (_repLevels != null)
        {
            ArrayPool<byte>.Shared.Return(_repLevels);
            _repLevels = null;
        }
        _valueBuffer?.Dispose();
        _offsetsBuffer?.Dispose();
        _largeOffsetsBuffer?.Dispose();
        _viewsBuffer?.Dispose();
        _dataBuffer?.Dispose();
    }

}

/// <summary>
/// Factory to construct the correct Arrow array type from <see cref="ArrayData"/>.
/// </summary>
internal static class ArrowArrayFactory
{
    public static IArrowArray BuildArray(ArrayData data) =>
        data.DataType switch
        {
            BooleanType => new BooleanArray(data),
            Int8Type => new Int8Array(data),
            UInt8Type => new UInt8Array(data),
            Int16Type => new Int16Array(data),
            UInt16Type => new UInt16Array(data),
            Int32Type => new Int32Array(data),
            UInt32Type => new UInt32Array(data),
            Int64Type => new Int64Array(data),
            UInt64Type => new UInt64Array(data),
#if NET6_0_OR_GREATER
            HalfFloatType => new HalfFloatArray(data),
#endif
            FloatType => new FloatArray(data),
            DoubleType => new DoubleArray(data),
            Date32Type => new Date32Array(data),
            Time32Type => new Time32Array(data),
            Time64Type => new Time64Array(data),
            TimestampType => new TimestampArray(data),
            Apache.Arrow.Types.StringType => new StringArray(data),
            BinaryType => new BinaryArray(data),
            StringViewType => new StringViewArray(data),
            BinaryViewType => new BinaryViewArray(data),
            LargeStringType => new LargeStringArray(data),
            LargeBinaryType => new LargeBinaryArray(data),
            Decimal32Type => new Decimal32Array(data),
            Decimal64Type => new Decimal64Array(data),
            Decimal128Type => new Decimal128Array(data),
            Decimal256Type => new Decimal256Array(data),
            FixedSizeBinaryType => new FixedSizeBinaryArray(data),
            // ExtensionType: rebuild the storage array with the inner storage
            // type, then wrap via CreateArray. This covers GuidType today and
            // any future extension whose storage type we already know how to
            // build.
            ExtensionType ext => ext.CreateArray(
                BuildArray(new ArrayData(ext.StorageType, data.Length, data.NullCount, data.Offset, data.Buffers, data.Children, data.Dictionary))),
            _ => throw new NotSupportedException($"Cannot construct Arrow array for type '{data.DataType.Name}'."),
        };
}
