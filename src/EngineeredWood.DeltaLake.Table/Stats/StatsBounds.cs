// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.DeltaLake.Table.Stats;

/// <summary>
/// The bounds of one Arrow array for <see cref="StatsCollector"/>, found with the Parquet writer's statistics
/// kernels (<see cref="IntegerBounds"/>, <see cref="FloatBounds"/>, <see cref="ByteArrayBounds"/>) rather than a
/// value at a time through <c>IsNull</c>, <c>GetValue</c> and <c>CompareTo</c>.
/// </summary>
/// <remarks>
/// <para>The kernels take a definition level per value (0 = null). An array with nulls has its validity bitmap
/// expanded into levels a <see cref="Chunk"/> of rows at a time, into one small rented buffer, so the levels stay
/// in cache beside the values they describe; an array without one passes none.</para>
/// <para>Every method honours the array's offset: <see cref="PrimitiveArray{T}.Values"/> and
/// a string array's <c>ValueOffsets</c> are already sliced, and the bitmap is read from bit
/// <c>Data.Offset</c>.</para>
/// </remarks>
internal static class StatsBounds
{
    /// <summary>Rows whose levels are expanded at a time.</summary>
    internal const int Chunk = 4096;

    /// <summary>The least and greatest present INT32 values (signed); false when every value is null.</summary>
    public static bool TryInt32(PrimitiveArray<int> array, out int min, out int max)
    {
        var values = array.Values;
        min = max = 0;
        if (array.NullCount == 0)
            return IntegerBounds.TryInt32(values, default, unsigned: false, out min, out max);

        bool any = false;
        int[] levels = ArrayPool<int>.Shared.Rent(Chunk);
        try
        {
            for (int s = 0; s < values.Length; s += Chunk)
            {
                int n = Math.Min(Chunk, values.Length - s);
                ExpandValidity(array, s, levels.AsSpan(0, n));
                if (!IntegerBounds.TryInt32(values.Slice(s, n), levels.AsSpan(0, n), false, out int lo, out int hi))
                    continue;
                (min, max) = any ? (Math.Min(min, lo), Math.Max(max, hi)) : (lo, hi);
                any = true;
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(levels);
        }

        return any;
    }

    /// <summary>The least and greatest present INT64 values (signed); false when every value is null.</summary>
    public static bool TryInt64(PrimitiveArray<long> array, out long min, out long max)
    {
        var values = array.Values;
        min = max = 0;
        if (array.NullCount == 0)
            return IntegerBounds.TryInt64(values, default, unsigned: false, out min, out max);

        bool any = false;
        int[] levels = ArrayPool<int>.Shared.Rent(Chunk);
        try
        {
            for (int s = 0; s < values.Length; s += Chunk)
            {
                int n = Math.Min(Chunk, values.Length - s);
                ExpandValidity(array, s, levels.AsSpan(0, n));
                if (!IntegerBounds.TryInt64(values.Slice(s, n), levels.AsSpan(0, n), false, out long lo, out long hi))
                    continue;
                (min, max) = any ? (Math.Min(min, lo), Math.Max(max, hi)) : (lo, hi);
                any = true;
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(levels);
        }

        return any;
    }

    /// <summary>
    /// The bounds of a DOUBLE array in Delta's order, where NaN is the greatest value (as Spark writes it): the
    /// least non-NaN value, or NaN when every present value is NaN; and NaN when any present value is, else the
    /// greatest. A zero bound is -0 for the least and +0 for the greatest when the array holds both zeros.
    /// False when every value is null.
    /// </summary>
    public static bool TryDouble(DoubleArray array, out double min, out double max)
    {
        var values = array.Values;
        long minKey = long.MaxValue, maxKey = long.MinValue, nans = 0, present = 0;
        double[]? dense = array.NullCount == 0 ? null : ArrayPool<double>.Shared.Rent(Chunk);
        int[]? levels = dense is null ? null : ArrayPool<int>.Shared.Rent(Chunk);
        try
        {
            for (int s = 0; s < values.Length; s += Chunk)
            {
                int n = Math.Min(Chunk, values.Length - s);
                ReadOnlySpan<double> chunk = values.Slice(s, n);
                if (dense is not null)
                {
                    ExpandValidity(array, s, levels.AsSpan(0, n));
                    n = PresentValues.Gather(chunk, levels.AsSpan(0, n), dense.AsSpan(0, n));
                    chunk = dense.AsSpan(0, n);
                }

                present += n;
                if (FloatBounds.TryDouble(chunk, out long lo, out long hi, out long count))
                {
                    minKey = Math.Min(minKey, DoubleKey(lo));
                    maxKey = Math.Max(maxKey, DoubleKey(hi));
                }

                nans += count;
            }
        }
        finally
        {
            if (dense is not null)
            {
                ArrayPool<double>.Shared.Return(dense);
                ArrayPool<int>.Shared.Return(levels!);
            }
        }

        min = max = 0;
        if (present == 0)
            return false;
        min = nans == present ? double.NaN : BitConverter.Int64BitsToDouble(DoubleKey(minKey));
        max = nans > 0 ? double.NaN : BitConverter.Int64BitsToDouble(DoubleKey(maxKey));
        return true;
    }

    /// <summary>The FLOAT counterpart of <see cref="TryDouble"/>.</summary>
    public static bool TryFloat(FloatArray array, out float min, out float max)
    {
        var values = array.Values;
        int minKey = int.MaxValue, maxKey = int.MinValue;
        long nans = 0, present = 0;
        float[]? dense = array.NullCount == 0 ? null : ArrayPool<float>.Shared.Rent(Chunk);
        int[]? levels = dense is null ? null : ArrayPool<int>.Shared.Rent(Chunk);
        try
        {
            for (int s = 0; s < values.Length; s += Chunk)
            {
                int n = Math.Min(Chunk, values.Length - s);
                ReadOnlySpan<float> chunk = values.Slice(s, n);
                if (dense is not null)
                {
                    ExpandValidity(array, s, levels.AsSpan(0, n));
                    n = PresentValues.Gather(chunk, levels.AsSpan(0, n), dense.AsSpan(0, n));
                    chunk = dense.AsSpan(0, n);
                }

                present += n;
                if (FloatBounds.TryFloat(chunk, out int lo, out int hi, out long count))
                {
                    minKey = Math.Min(minKey, FloatKey(lo));
                    maxKey = Math.Max(maxKey, FloatKey(hi));
                }

                nans += count;
            }
        }
        finally
        {
            if (dense is not null)
            {
                ArrayPool<float>.Shared.Return(dense);
                ArrayPool<int>.Shared.Return(levels!);
            }
        }

        min = max = 0;
        if (present == 0)
            return false;
        min = nans == present ? float.NaN : Int32BitsToSingle(FloatKey(minKey));
        max = nans > 0 ? float.NaN : Int32BitsToSingle(FloatKey(maxKey));
        return true;
    }

    /// <summary>
    /// The positions of the least and greatest present values of a string array, by their UTF-8 bytes; -1 for
    /// both when every value is null. Ties keep the first position.
    /// </summary>
    public static (int MinIdx, int MaxIdx) FindString(StringArray array)
    {
        ReadOnlySpan<int> offsets = array.ValueOffsets;
        ReadOnlySpan<byte> data = array.ValueBuffer.Span;
        if (array.NullCount == 0)
            return ByteArrayBounds.Find(offsets, data, null, 0, array.Length);

        int minIdx = -1, maxIdx = -1;
        int[] levels = ArrayPool<int>.Shared.Rent(Chunk);
        try
        {
            for (int s = 0; s < array.Length; s += Chunk)
            {
                int n = Math.Min(Chunk, array.Length - s);
                ExpandValidity(array, s, levels.AsSpan(0, n));
                var (lo, hi) = ByteArrayBounds.Find(offsets.Slice(s, n + 1), data, levels, 0, n);
                if (lo < 0)
                    continue;
                lo += s;
                hi += s;
                if (minIdx < 0)
                {
                    (minIdx, maxIdx) = (lo, hi);
                    continue;
                }

                if (Bytes(offsets, data, lo).SequenceCompareTo(Bytes(offsets, data, minIdx)) < 0) minIdx = lo;
                if (Bytes(offsets, data, hi).SequenceCompareTo(Bytes(offsets, data, maxIdx)) > 0) maxIdx = hi;
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(levels);
        }

        return (minIdx, maxIdx);
    }

    /// <summary>
    /// The positions of the least and greatest present values of a DECIMAL128 array; -1 for both when every
    /// value is null. Ties keep the first position.
    /// </summary>
    /// <remarks>
    /// A value is a 16-byte little-endian two's-complement integer (the scale is the column's), so it compares as
    /// its high word signed and then its low word unsigned. Whether the high words tie is a coin toss for small
    /// numbers (the high word is all sign), so the comparison does not short-circuit: one branch, rarely taken.
    /// </remarks>
    public static (int MinIdx, int MaxIdx) FindDecimal128(Decimal128Array array)
    {
        ReadOnlySpan<byte> buffer = array.ValueBuffer.Span;
        int offset = array.Data.Offset;
        bool dense = array.NullCount == 0;
        int minIdx = -1, maxIdx = -1;
        long minHi = 0, maxHi = 0;
        ulong minLo = 0, maxLo = 0;
        for (int i = 0; i < array.Length; i++)
        {
            if (!dense && array.IsNull(i))
                continue;

            var value = buffer.Slice((offset + i) * 16, 16);
            ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(value);
            long hi = BinaryPrimitives.ReadInt64LittleEndian(value.Slice(8));
            if (minIdx < 0)
            {
                minIdx = maxIdx = i;
                (minHi, minLo, maxHi, maxLo) = (hi, lo, hi, lo);
                continue;
            }

            if ((hi < minHi) | ((hi == minHi) & (lo < minLo)))
            {
                minIdx = i;
                (minHi, minLo) = (hi, lo);
            }

            if ((hi > maxHi) | ((hi == maxHi) & (lo > maxLo)))
            {
                maxIdx = i;
                (maxHi, maxLo) = (hi, lo);
            }
        }

        return (minIdx, maxIdx);
    }

    /// <summary>Levels for <paramref name="levels"/>.Length rows of <paramref name="array"/> from
    /// <paramref name="start"/>: 1 where the validity bit is set, 0 where it is clear.</summary>
    internal static void ExpandValidity(IArrowArray array, int start, Span<int> levels)
    {
        ReadOnlySpan<byte> bitmap = array.Data.Buffers[0].Span;
        int bit = array.Data.Offset + start;
        for (int i = 0; i < levels.Length; i++, bit++)
            levels[i] = (bitmap[bit >> 3] >> (bit & 7)) & 1;
    }

    private static ReadOnlySpan<byte> Bytes(ReadOnlySpan<int> offsets, ReadOnlySpan<byte> data, int i) =>
        data.Slice(offsets[i], offsets[i + 1] - offsets[i]);

    /// <summary>IEEE 754 total-order key of a DOUBLE's bits; its own inverse.</summary>
    private static long DoubleKey(long bits) => bits ^ ((bits >> 63) & long.MaxValue);

    private static int FloatKey(int bits) => bits ^ ((bits >> 31) & int.MaxValue);

    private static float Int32BitsToSingle(int bits) =>
        BitConverter.ToSingle(BitConverter.GetBytes(bits), 0);
}
