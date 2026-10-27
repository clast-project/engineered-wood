// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Computes column-level statistics (min, max, null_count) from Arrow arrays.
/// </summary>
/// <summary>
/// One page's bounds and NaN count, in the physical type's encoding and untruncated. Both bounds are
/// null when the page has no bound to give.
/// </summary>
internal readonly record struct PageBounds(byte[]? Min, byte[]? Max, long NanCount);

internal static class StatisticsCollector
{
    /// <summary>
    /// Maximum byte length for binary min/max statistics before truncation.
    /// Prevents metadata bloat from long string values.
    /// </summary>
    private const int MaxBinaryStatLength = 64;

    /// <summary>
    /// Computes statistics for a column chunk.
    /// </summary>
    /// <param name="floatingPointTotalOrder">
    /// When <see langword="true"/>, FLOAT/DOUBLE min/max follow IEEE 754 total
    /// order: an all-NaN chunk records its smallest and largest NaN (in that order)
    /// as its bounds rather than omitting them. When <see langword="false"/> (the TYPE_ORDER default) an
    /// all-NaN chunk omits min/max. NaN is excluded from the bounds either way,
    /// and a <c>nan_count</c> is always recorded for FLOAT/DOUBLE columns.
    /// </param>
    /// <param name="order">How the values are ordered; see <see cref="StatisticsOrders.For"/>.</param>
    public static Statistics Compute(
        IArrowArray array,
        PhysicalType physicalType,
        int typeLength,
        int[]? defLevels,
        int nonNullCount,
        int rowCount,
        bool floatingPointTotalOrder = false,
        StatisticsOrder order = StatisticsOrder.Default)
    {
        long nullCount = rowCount - nonNullCount;
        bool isFloatingPoint = IsFloatingPoint(physicalType, order);

        if (nonNullCount == 0)
        {
            // No values ⇒ no NaN. nan_count is mandatory for FP columns even at zero.
            return new Statistics { NullCount = nullCount, NanCount = isFloatingPoint ? 0L : null };
        }

        // Floating-point columns are scanned separately so the NaN count, min,
        // and max all come from a single pass.
        if (isFloatingPoint)
        {
            var (fpMin, fpMax, nanCount) = physicalType switch
            {
                PhysicalType.Float => ComputeFloatMinMax(array, defLevels, floatingPointTotalOrder, 0, array.Length),
                PhysicalType.Double => ComputeDoubleMinMax(array, defLevels, floatingPointTotalOrder, 0, array.Length),
                _ => ComputeFloat16MinMax(array, defLevels, floatingPointTotalOrder, 0, array.Length),
            };

            var fpStats = new Statistics
            {
                NullCount = nullCount,
                Min = fpMin,
                Max = fpMax,
                MinValue = fpMin,
                MaxValue = fpMax,
                IsMinValueExact = fpMin != null ? true : (bool?)null,
                IsMaxValueExact = fpMax != null ? true : (bool?)null,
                NanCount = nanCount,
            };

            GC.KeepAlive(array);
            return fpStats;
        }

        var (minBytes, maxBytes, minExact, maxExact) = physicalType switch
        {
            PhysicalType.Boolean => WithExact(ComputeBooleanMinMax(array, defLevels, 0, array.Length)),
            PhysicalType.Int32 => WithExact(ComputeMinMax<int>(array, defLevels, Int32Comparison(order), 0, array.Length)),
            PhysicalType.Int64 => WithExact(ComputeMinMax<long>(array, defLevels, Int64Comparison(order), 0, array.Length)),
            PhysicalType.ByteArray => ComputeByteArrayMinMaxTruncated(array, defLevels),
            PhysicalType.FixedLenByteArray
                => WithExact(ComputeFlbaMinMax(array, defLevels, typeLength, order, 0, array.Length)),
            _ => (null, null, true, true),
        };

        var stats = new Statistics
        {
            NullCount = nullCount,
            Min = minBytes,
            Max = maxBytes,
            MinValue = minBytes,
            MaxValue = maxBytes,
            IsMinValueExact = minBytes != null ? minExact : null,
            IsMaxValueExact = maxBytes != null ? maxExact : null,
        };

        // The min/max scans above read raw spans off the array's buffers, a full pass over the column.
        // Roots it for the duration; see doc/arrow-span-lifetime.md.
        GC.KeepAlive(array);
        return stats;
    }

    /// <summary>
    /// The bounds and NaN count of the values in level positions
    /// <paramref name="start"/> .. <paramref name="start"/> + <paramref name="count"/> — one page's
    /// worth — for a page index.
    /// </summary>
    /// <remarks>
    /// Runs the same scans <see cref="Compute"/> runs over the whole chunk, so a page's bounds are
    /// ordered exactly as the chunk's are. Unlike <see cref="Compute"/>, BYTE_ARRAY bounds come back in
    /// full: a page index truncates by its own rule. Call only for a page with at least one non-null
    /// value. <see cref="PageBounds.Min"/> is null when there are no bounds to give, which for a
    /// FLOAT/DOUBLE page under TYPE_ORDER means every value is NaN.
    /// </remarks>
    public static PageBounds ComputePageBounds(
        IArrowArray array,
        PhysicalType physicalType,
        int typeLength,
        int[]? defLevels,
        int start,
        int count,
        bool floatingPointTotalOrder,
        StatisticsOrder order)
    {
        int end = start + count;
        PageBounds bounds;
        switch (physicalType)
        {
            case PhysicalType.FixedLenByteArray when order == StatisticsOrder.Float16:
            {
                var (min, max, nanCount) = ComputeFloat16MinMax(array, defLevels, floatingPointTotalOrder, start, end);
                bounds = new PageBounds(min, max, nanCount);
                break;
            }
            case PhysicalType.Float:
            {
                var (min, max, nanCount) = ComputeFloatMinMax(array, defLevels, floatingPointTotalOrder, start, end);
                bounds = new PageBounds(min, max, nanCount);
                break;
            }
            case PhysicalType.Double:
            {
                var (min, max, nanCount) = ComputeDoubleMinMax(array, defLevels, floatingPointTotalOrder, start, end);
                bounds = new PageBounds(min, max, nanCount);
                break;
            }
            case PhysicalType.Boolean:
            {
                var (min, max) = ComputeBooleanMinMax(array, defLevels, start, end);
                bounds = new PageBounds(min, max, 0);
                break;
            }
            case PhysicalType.Int32:
            {
                var (min, max) = ComputeMinMax<int>(array, defLevels, Int32Comparison(order), start, end);
                bounds = new PageBounds(min, max, 0);
                break;
            }
            case PhysicalType.Int64:
            {
                var (min, max) = ComputeMinMax<long>(array, defLevels, Int64Comparison(order), start, end);
                bounds = new PageBounds(min, max, 0);
                break;
            }
            case PhysicalType.ByteArray:
            {
                var data = array.Data;
                ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
                ReadOnlySpan<byte> rawData = data.Buffers[2].Span;
                var (minIdx, maxIdx) = FindByteArrayMinMax(array, defLevels, start, end);
                bounds = minIdx < 0
                    ? default
                    : new PageBounds(
                        rawData.Slice(offsets[minIdx], offsets[minIdx + 1] - offsets[minIdx]).ToArray(),
                        rawData.Slice(offsets[maxIdx], offsets[maxIdx + 1] - offsets[maxIdx]).ToArray(),
                        0);
                break;
            }
            case PhysicalType.FixedLenByteArray:
            {
                var (min, max) = ComputeFlbaMinMax(array, defLevels, typeLength, order, start, end);
                bounds = new PageBounds(min, max, 0);
                break;
            }
            default:
                bounds = default;
                break;
        }

        GC.KeepAlive(array);
        return bounds;
    }

    /// <summary>
    /// The chunk statistics <see cref="Compute"/> would return, built from bounds already found
    /// page by page — so a column whose pages were scanned for a page index is not scanned again.
    /// </summary>
    /// <param name="min">The least non-NaN value, untruncated; null if every value is NaN.</param>
    /// <param name="max">The greatest non-NaN value, untruncated.</param>
    /// <param name="minNaN">The smallest NaN in IEEE 754 total order, for an all-NaN chunk under total order.</param>
    /// <param name="maxNaN">The largest NaN in IEEE 754 total order, likewise.</param>
    /// <param name="nanCount">NaN values in the chunk, for FLOAT/DOUBLE.</param>
    /// <remarks>
    /// Mirrors <see cref="Compute"/> field for field, including the 64-byte BYTE_ARRAY truncation and
    /// the exactness flags; tests hold the two to byte-identical output. Call only for a chunk with at
    /// least one non-null value, whose every page's bounds reached the fold.
    /// </remarks>
    public static Statistics FromBounds(
        PhysicalType physicalType,
        byte[]? min,
        byte[]? max,
        byte[]? minNaN,
        byte[]? maxNaN,
        long nanCount,
        long nullCount,
        bool floatingPointTotalOrder,
        StatisticsOrder order = StatisticsOrder.Default)
    {
        if (IsFloatingPoint(physicalType, order))
        {
            if (min is null && floatingPointTotalOrder && nanCount > 0)
            {
                min = minNaN;
                max = maxNaN;
            }

            return new Statistics
            {
                NullCount = nullCount,
                Min = min,
                Max = max,
                MinValue = min,
                MaxValue = max,
                IsMinValueExact = min != null ? true : (bool?)null,
                IsMaxValueExact = max != null ? true : (bool?)null,
                NanCount = nanCount,
            };
        }

        var (minBytes, maxBytes, minExact, maxExact) = physicalType == PhysicalType.ByteArray && min is not null
            ? TruncateBinaryStats(min, max!)
            : (min, max, true, true);

        return new Statistics
        {
            NullCount = nullCount,
            Min = minBytes,
            Max = maxBytes,
            MinValue = minBytes,
            MaxValue = maxBytes,
            IsMinValueExact = minBytes != null ? minExact : null,
            IsMaxValueExact = maxBytes != null ? maxExact : null,
        };
    }

    /// <summary>
    /// Orders two encoded values of one column the way the scans here order them: the comparator a
    /// dictionary-encoded page's bounds and a page index's boundary order are decided with. Both
    /// values must be non-NaN.
    /// </summary>
    public static int CompareValues(
        ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, PhysicalType physicalType, StatisticsOrder order)
        => physicalType switch
        {
            PhysicalType.Boolean => left[0].CompareTo(right[0]),
            PhysicalType.Int32 => Int32Comparison(order)(MemoryMarshal.Read<int>(left), MemoryMarshal.Read<int>(right)),
            PhysicalType.Int64 => Int64Comparison(order)(MemoryMarshal.Read<long>(left), MemoryMarshal.Read<long>(right)),
            PhysicalType.Float => CompareFloat(MemoryMarshal.Read<float>(left), MemoryMarshal.Read<float>(right)),
            PhysicalType.Double => CompareDouble(MemoryMarshal.Read<double>(left), MemoryMarshal.Read<double>(right)),
            PhysicalType.FixedLenByteArray => CompareFlba(left, right, order),
            _ => left.SequenceCompareTo(right),
        };

    /// <summary>
    /// Numeric order with -0 below +0: the rule the FLOAT scans apply when they pick a bound.
    /// </summary>
    private static int CompareFloat(float a, float b)
    {
        if (a < b) return -1;
        if (a > b) return 1;
#if NET8_0_OR_GREATER
        return a == 0f ? float.IsNegative(b).CompareTo(float.IsNegative(a)) : 0;
#else
        return a == 0f ? IsNegativeFloat(b).CompareTo(IsNegativeFloat(a)) : 0;
#endif
    }

    private static int CompareDouble(double a, double b)
    {
        if (a < b) return -1;
        if (a > b) return 1;
#if NET8_0_OR_GREATER
        return a == 0.0 ? double.IsNegative(b).CompareTo(double.IsNegative(a)) : 0;
#else
        return a == 0.0 ? IsNegativeDouble(b).CompareTo(IsNegativeDouble(a)) : 0;
#endif
    }

    /// <summary>
    /// Computes statistics from dictionary entries only (O(unique) instead of O(total)).
    /// For dict-encoded ByteArray columns, min/max of all values equals min/max of unique entries.
    /// </summary>
    public static Statistics ComputeFromDictEntries(
        byte[] dictPageData,
        int dictCount,
        PhysicalType physicalType,
        int typeLength,
        long nullCount,
        StatisticsOrder order = StatisticsOrder.Default)
    {
        if (dictCount == 0)
            return new Statistics { NullCount = nullCount };

        var (minBytes, maxBytes, minExact, maxExact) = physicalType switch
        {
            PhysicalType.ByteArray => ComputeByteArrayMinMaxFromDict(dictPageData, dictCount),
            PhysicalType.FixedLenByteArray
                => ComputeFlbaMinMaxFromDict(dictPageData, dictCount, typeLength, order),
            _ => ComputeFixedMinMaxFromDict(dictPageData, dictCount, physicalType, typeLength, order),
        };

        return new Statistics
        {
            NullCount = nullCount,
            Min = minBytes,
            Max = maxBytes,
            MinValue = minBytes,
            MaxValue = maxBytes,
            IsMinValueExact = minBytes != null ? minExact : null,
            IsMaxValueExact = maxBytes != null ? maxExact : null,
        };
    }

    /// <summary>
    /// Computes statistics for a dictionary-encoded FLOAT/DOUBLE column from its dictionary entries and
    /// its per-row indices — the shape <see cref="BufferedParquetWriter"/> has at flush time, where the
    /// column's Arrow values are long gone and <see cref="Compute"/>'s full scan is not available.
    /// </summary>
    /// <remarks>
    /// The bounds come from the entries alone, which is exact because every entry of a dictionary is a
    /// value some row holds: min/max over the distinct values IS min/max over the values.
    /// <para>
    /// <c>nan_count</c> is the one part that cannot come from the entries, because it counts VALUES and
    /// not distinct values — so it is derived from the indices. That scan is skipped entirely when no
    /// entry is NaN, which is the ordinary case; the cost is only paid by a column that actually has
    /// NaNs in it. The count is exact either way, which is the point: a wrong nan_count is worse than an
    /// absent one, and the field is mandatory for FLOAT/DOUBLE.
    /// </para>
    /// </remarks>
    public static Statistics ComputeFloatingPointFromDictEntries(
        in DictionaryEncoder.DictionaryResult dictResult,
        PhysicalType physicalType,
        long nullCount,
        bool floatingPointTotalOrder)
    {
        int dictCount = dictResult.DictionaryCount;
        if (dictCount == 0)
        {
            // No values ⇒ no NaN. nan_count is mandatory for FP columns even at zero.
            return new Statistics { NullCount = nullCount, NanCount = 0L };
        }

        int elementSize = physicalType switch
        {
            PhysicalType.Float => 4,
            PhysicalType.Double => 8,
            _ => 2, // FLOAT16, the only FIXED_LEN_BYTE_ARRAY routed here
        };
        var span = dictResult.DictionaryPageData.AsSpan();

        // NaN is excluded from the bounds, exactly as the full scan excludes it.
        var (minBytes, maxBytes, _, _) = elementSize switch
        {
            4 => ComputeFixedMinMaxTypedFloat(span, dictCount, elementSize),
            8 => ComputeFixedMinMaxTypedDouble(span, dictCount, elementSize),
            _ => ComputeFixedMinMaxHalf(span, dictCount),
        };

        var nanEntries = FindNaNEntries(span, dictCount, elementSize);
        long nanCount = 0;
        int minNaNEntry = -1, maxNaNEntry = -1;
        if (nanEntries != null)
            nanCount = CountNaNRows(dictResult, nanEntries, span, physicalType, out minNaNEntry, out maxNaNEntry);

        if (minBytes is null && floatingPointTotalOrder && nanCount > 0)
        {
            // No non-NaN values. Under IEEE 754 total order, an all-NaN chunk records its smallest and
            // largest NaN as the bounds — the ones the full scan picks; under TYPE_ORDER, min/max are
            // omitted.
            minBytes = span.Slice(minNaNEntry * elementSize, elementSize).ToArray();
            maxBytes = span.Slice(maxNaNEntry * elementSize, elementSize).ToArray();
        }

        return new Statistics
        {
            NullCount = nullCount,
            Min = minBytes,
            Max = maxBytes,
            MinValue = minBytes,
            MaxValue = maxBytes,
            IsMinValueExact = minBytes != null ? true : (bool?)null,
            IsMaxValueExact = maxBytes != null ? true : (bool?)null,
            NanCount = nanCount,
        };
    }

    /// <summary>
    /// Flags which dictionary entries are NaN, or returns <see langword="null"/> when none is — the
    /// signal to the caller that no index scan is needed at all.
    /// </summary>
    private static bool[]? FindNaNEntries(ReadOnlySpan<byte> dictPage, int dictCount, int elementSize)
    {
        bool[]? flags = null;
        if (elementSize == 2)
        {
            var values = MemoryMarshal.Cast<byte, ushort>(dictPage[..(dictCount * 2)]);
            for (int i = 0; i < dictCount; i++)
            {
                if (!IsHalfNaN(values[i])) continue;
                flags ??= new bool[dictCount];
                flags[i] = true;
            }
        }
        else if (elementSize == 4)
        {
            var values = MemoryMarshal.Cast<byte, float>(dictPage[..(dictCount * 4)]);
            for (int i = 0; i < dictCount; i++)
            {
                if (!float.IsNaN(values[i])) continue;
                flags ??= new bool[dictCount];
                flags[i] = true;
            }
        }
        else
        {
            var values = MemoryMarshal.Cast<byte, double>(dictPage[..(dictCount * 8)]);
            for (int i = 0; i < dictCount; i++)
            {
                if (!double.IsNaN(values[i])) continue;
                flags ??= new bool[dictCount];
                flags[i] = true;
            }
        }

        return flags;
    }

    /// <summary>
    /// Counts the rows whose index points at a NaN entry, and reports the referenced NaN entries that
    /// are smallest and largest in IEEE 754 total order. Reads whichever index form the dictionary
    /// result carries.
    /// </summary>
    private static long CountNaNRows(
        in DictionaryEncoder.DictionaryResult dictResult, bool[] nanEntries, ReadOnlySpan<byte> dictPage,
        PhysicalType physicalType, out int minNaNEntry, out int maxNaNEntry)
    {
        int elementSize = physicalType switch
        {
            PhysicalType.Float => 4,
            PhysicalType.Double => 8,
            _ => 2,
        };
        long nanCount = 0;
        int minEntry = -1, maxEntry = -1;
        long minKey = 0, maxKey = 0;

        void See(int entry, ReadOnlySpan<byte> page)
        {
            long key = TotalOrderKey(page.Slice(entry * elementSize, elementSize), physicalType);
            if (minEntry < 0 || key < minKey) { minEntry = entry; minKey = key; }
            if (maxEntry < 0 || key > maxKey) { maxEntry = entry; maxKey = key; }
        }

        if (dictResult.Indices is { } indices)
        {
            foreach (int index in indices)
            {
                if (!nanEntries[index]) continue;
                See(index, dictPage);
                nanCount++;
            }
        }
        else if (dictResult.IndexRuns is { } runs)
        {
            for (int r = 0; r < runs.Values.Length; r++)
            {
                if (!nanEntries[runs.Values[r]]) continue;
                See(runs.Values[r], dictPage);
                nanCount += runs.Lengths[r];
            }
        }

        minNaNEntry = minEntry;
        maxNaNEntry = maxEntry;
        return nanCount;
    }

    private static (byte[]?, byte[]?, bool, bool) WithExact((byte[]? min, byte[]? max) result)
        => (result.min, result.max, true, true);

    // ── Dict entry statistics ──

    private static (byte[]?, byte[]?, bool, bool) ComputeByteArrayMinMaxFromDict(
        byte[] dictPage, int dictCount)
    {
        // PLAIN-encoded ByteArray dict: 4-byte LE length prefix + data per entry
        int pos = 0;
        int minOffset = 0, minLen = 0;
        int maxOffset = 0, maxLen = 0;
        var span = dictPage.AsSpan();

        for (int i = 0; i < dictCount; i++)
        {
            int len = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(pos));
            int dataOffset = pos + 4;

            if (i == 0)
            {
                minOffset = maxOffset = dataOffset;
                minLen = maxLen = len;
            }
            else
            {
                var val = span.Slice(dataOffset, len);
                if (val.SequenceCompareTo(span.Slice(minOffset, minLen)) < 0)
                {
                    minOffset = dataOffset;
                    minLen = len;
                }
                if (val.SequenceCompareTo(span.Slice(maxOffset, maxLen)) > 0)
                {
                    maxOffset = dataOffset;
                    maxLen = len;
                }
            }

            pos = dataOffset + len;
        }

        return TruncateBinaryStats(span.Slice(minOffset, minLen), span.Slice(maxOffset, maxLen));
    }

    private static (byte[]?, byte[]?, bool, bool) ComputeFlbaMinMaxFromDict(
        byte[] dictPage, int dictCount, int typeLength, StatisticsOrder order)
    {
        var span = dictPage.AsSpan();
        int minIdx = 0, maxIdx = 0;

        for (int i = 1; i < dictCount; i++)
        {
            var val = span.Slice(i * typeLength, typeLength);
            if (CompareFlba(val, span.Slice(minIdx * typeLength, typeLength), order) < 0)
                minIdx = i;
            if (CompareFlba(val, span.Slice(maxIdx * typeLength, typeLength), order) > 0)
                maxIdx = i;
        }

        // FLBA values are typically short (16 or 32 bytes for decimals), so no truncation
        return (
            span.Slice(minIdx * typeLength, typeLength).ToArray(),
            span.Slice(maxIdx * typeLength, typeLength).ToArray(),
            true, true);
    }

    private static (byte[]?, byte[]?, bool, bool) ComputeFixedMinMaxFromDict(
        byte[] dictPage, int dictCount, PhysicalType physicalType, int typeLength, StatisticsOrder order)
    {
        // Fixed-width types: PLAIN-encoded, elementSize bytes per entry
        int elementSize = physicalType switch
        {
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.Int96 => 12,
            _ => typeLength,
        };

        var span = dictPage.AsSpan();

        return physicalType switch
        {
            PhysicalType.Int32 => ComputeFixedMinMaxTyped<int>(span, dictCount, elementSize, Int32Comparison(order)),
            PhysicalType.Int64 => ComputeFixedMinMaxTyped<long>(span, dictCount, elementSize, Int64Comparison(order)),
            PhysicalType.Float => ComputeFixedMinMaxTypedFloat(span, dictCount, elementSize),
            PhysicalType.Double => ComputeFixedMinMaxTypedDouble(span, dictCount, elementSize),
            _ => (span[..elementSize].ToArray(), span[..elementSize].ToArray(), true, true),
        };
    }

    private static (byte[]?, byte[]?, bool, bool) ComputeFixedMinMaxTyped<T>(
        ReadOnlySpan<byte> span, int count, int elementSize, Comparison<T> compare)
        where T : unmanaged
    {
        var values = MemoryMarshal.Cast<byte, T>(span[..(count * elementSize)]);
        T min = values[0], max = values[0];
        for (int i = 1; i < count; i++)
        {
            if (compare(values[i], min) < 0) min = values[i];
            if (compare(values[i], max) > 0) max = values[i];
        }
        var minBytes = new byte[elementSize];
        var maxBytes = new byte[elementSize];
#if NET8_0_OR_GREATER
        MemoryMarshal.Write(minBytes, in min);
        MemoryMarshal.Write(maxBytes, in max);
#else
        MemoryMarshal.Write(minBytes, ref min);
        MemoryMarshal.Write(maxBytes, ref max);
#endif
        return (minBytes, maxBytes, true, true);
    }

    private static (byte[]?, byte[]?, bool, bool) ComputeFixedMinMaxTypedFloat(
        ReadOnlySpan<byte> span, int count, int elementSize)
    {
        var values = MemoryMarshal.Cast<byte, float>(span[..(count * elementSize)]);
        bool first = true;
        float min = 0, max = 0;
        for (int i = 0; i < count; i++)
        {
            if (float.IsNaN(values[i])) continue;
            if (first) { min = max = values[i]; first = false; }
            else
            {
#if NET8_0_OR_GREATER
                if (values[i] < min || (values[i] == 0f && min == 0f && float.IsNegative(values[i]))) min = values[i];
                if (values[i] > max || (values[i] == 0f && max == 0f && !float.IsNegative(values[i]))) max = values[i];
#else
                if (values[i] < min || (values[i] == 0f && min == 0f && IsNegativeFloat(values[i]))) min = values[i];
                if (values[i] > max || (values[i] == 0f && max == 0f && !IsNegativeFloat(values[i]))) max = values[i];
#endif
            }
        }
        if (first) return (null, null, true, true);
        var minB = new byte[4]; var maxB = new byte[4];
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteSingleLittleEndian(minB, min);
        BinaryPrimitives.WriteSingleLittleEndian(maxB, max);
#else
        BitConverter.GetBytes(min).CopyTo(minB, 0);
        BitConverter.GetBytes(max).CopyTo(maxB, 0);
#endif
        return (minB, maxB, true, true);
    }

    /// <summary>The non-NaN FLOAT16 dictionary entries' least and greatest, by total-order key.</summary>
    private static (byte[]?, byte[]?, bool, bool) ComputeFixedMinMaxHalf(ReadOnlySpan<byte> span, int count)
    {
        var values = MemoryMarshal.Cast<byte, ushort>(span[..(count * 2)]);
        int min = -1, max = -1, minKey = 0, maxKey = 0;
        for (int i = 0; i < count; i++)
        {
            int bits = values[i];
            if (IsHalfNaN(bits)) continue;
            int key = HalfKey(bits);
            if (min < 0 || key < minKey) { min = bits; minKey = key; }
            if (max < 0 || key > maxKey) { max = bits; maxKey = key; }
        }
        return min < 0 ? (null, null, true, true) : (Half16Bytes(min), Half16Bytes(max), true, true);
    }

    private static (byte[]?, byte[]?, bool, bool) ComputeFixedMinMaxTypedDouble(
        ReadOnlySpan<byte> span, int count, int elementSize)
    {
        var values = MemoryMarshal.Cast<byte, double>(span[..(count * elementSize)]);
        bool first = true;
        double min = 0, max = 0;
        for (int i = 0; i < count; i++)
        {
            if (double.IsNaN(values[i])) continue;
            if (first) { min = max = values[i]; first = false; }
            else
            {
#if NET8_0_OR_GREATER
                if (values[i] < min || (values[i] == 0.0 && min == 0.0 && double.IsNegative(values[i]))) min = values[i];
                if (values[i] > max || (values[i] == 0.0 && max == 0.0 && !double.IsNegative(values[i]))) max = values[i];
#else
                if (values[i] < min || (values[i] == 0.0 && min == 0.0 && IsNegativeDouble(values[i]))) min = values[i];
                if (values[i] > max || (values[i] == 0.0 && max == 0.0 && !IsNegativeDouble(values[i]))) max = values[i];
#endif
            }
        }
        if (first) return (null, null, true, true);
        var minB = new byte[8]; var maxB = new byte[8];
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteDoubleLittleEndian(minB, min);
        BinaryPrimitives.WriteDoubleLittleEndian(maxB, max);
#else
        BitConverter.GetBytes(min).CopyTo(minB, 0);
        BitConverter.GetBytes(max).CopyTo(maxB, 0);
#endif
        return (minB, maxB, true, true);
    }

    // ── Full-scan statistics ──

    private static (byte[]?, byte[]?) ComputeBooleanMinMax(IArrowArray array, int[]? defLevels, int start, int end)
    {
        var boolArray = (BooleanArray)array;
        bool hasTrue = false, hasFalse = false;

        for (int i = start; i < end; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            if (boolArray.GetValue(i) == true) hasTrue = true;
            else hasFalse = true;
            if (hasTrue && hasFalse) break;
        }

        byte minByte = hasFalse ? (byte)0 : (byte)1;
        byte maxByte = hasTrue ? (byte)1 : (byte)0;
        return ([minByte], [maxByte]);
    }

    private static (byte[]?, byte[]?) ComputeMinMax<T>(
        IArrowArray array, int[]? defLevels, Comparison<T> compare, int start, int end)
        where T : unmanaged
    {
        var valueBuffer = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span);
        bool first = true;
        T min = default, max = default;

        for (int i = start; i < end; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            T val = valueBuffer[i];
            if (first)
            {
                min = max = val;
                first = false;
            }
            else
            {
                if (compare(val, min) < 0) min = val;
                if (compare(val, max) > 0) max = val;
            }
        }

        if (first) return (null, null);

        int size = Marshal.SizeOf<T>();
        var minBytes = new byte[size];
        var maxBytes = new byte[size];
#if NET8_0_OR_GREATER
        MemoryMarshal.Write(minBytes, in min);
        MemoryMarshal.Write(maxBytes, in max);
#else
        MemoryMarshal.Write(minBytes, ref min);
        MemoryMarshal.Write(maxBytes, ref max);
#endif
        return (minBytes, maxBytes);
    }

    private static (byte[]?, byte[]?, long) ComputeFloatMinMax(
        IArrowArray array, int[]? defLevels, bool totalOrder, int start, int end)
    {
        var valueBuffer = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span);
        var bitsBuffer = MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span);
        bool first = true;
        float min = 0, max = 0;
        long nanCount = 0;
        var nans = default(NaNRange);

        for (int i = start; i < end; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            float val = valueBuffer[i];
            if (float.IsNaN(val))
            {
                if (totalOrder)
                {
                    int bits = bitsBuffer[i];
                    nans.Add(bits, bits ^ ((bits >> 31) & 0x7FFF_FFFF));
                }
                nanCount++;
                continue; // NaN excluded from min/max
            }
            if (first)
            {
                min = max = val;
                first = false;
            }
            else
            {
#if NET8_0_OR_GREATER
                if (val < min || (val == 0f && min == 0f && float.IsNegative(val)))
                    min = val;
                if (val > max || (val == 0f && max == 0f && !float.IsNegative(val)))
                    max = val;
#else
                if (val < min || (val == 0f && min == 0f && IsNegativeFloat(val)))
                    min = val;
                if (val > max || (val == 0f && max == 0f && !IsNegativeFloat(val)))
                    max = val;
#endif
            }
        }

        if (first)
        {
            // No non-NaN values. Under IEEE 754 total order, an all-NaN chunk records its smallest
            // and largest NaN as the bounds; under TYPE_ORDER, min/max are omitted.
            if (nans.Any)
                return (Int32BitsToBytes(nans.MinBits), Int32BitsToBytes(nans.MaxBits), nanCount);
            return (null, null, nanCount);
        }

        return (FloatToBytes(min), FloatToBytes(max), nanCount);
    }

    /// <summary>
    /// The FLOAT16 counterpart of <see cref="ComputeFloatMinMax"/>, on the raw 16 bits (System.Half is
    /// not on every target). Non-NaN values are ordered by total-order key, which is numeric order with
    /// -0 below +0 — the tie-break the FLOAT scan applies.
    /// </summary>
    private static (byte[]?, byte[]?, long) ComputeFloat16MinMax(
        IArrowArray array, int[]? defLevels, bool totalOrder, int start, int end)
    {
        var bitsBuffer = MemoryMarshal.Cast<byte, ushort>(array.Data.Buffers[1].Span);
        int min = -1, max = -1, minKey = 0, maxKey = 0;
        long nanCount = 0;
        var nans = default(NaNRange);

        for (int i = start; i < end; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            int bits = bitsBuffer[i];
            int key = HalfKey(bits);
            if (IsHalfNaN(bits))
            {
                if (totalOrder)
                    nans.Add(bits, key);
                nanCount++;
                continue;
            }
            if (min < 0 || key < minKey) { min = bits; minKey = key; }
            if (max < 0 || key > maxKey) { max = bits; maxKey = key; }
        }

        if (min < 0)
        {
            if (nans.Any)
                return (Half16Bytes(nans.MinBits), Half16Bytes(nans.MaxBits), nanCount);
            return (null, null, nanCount);
        }

        return (Half16Bytes(min), Half16Bytes(max), nanCount);
    }

    private static byte[] Half16Bytes(long bits)
    {
        var bytes = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, (ushort)bits);
        return bytes;
    }

    private static (byte[]?, byte[]?, long) ComputeDoubleMinMax(
        IArrowArray array, int[]? defLevels, bool totalOrder, int start, int end)
    {
        var valueBuffer = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span);
        var bitsBuffer = MemoryMarshal.Cast<byte, long>(array.Data.Buffers[1].Span);
        bool first = true;
        double min = 0, max = 0;
        long nanCount = 0;
        var nans = default(NaNRange);

        for (int i = start; i < end; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            double val = valueBuffer[i];
            if (double.IsNaN(val))
            {
                if (totalOrder)
                {
                    long bits = bitsBuffer[i];
                    nans.Add(bits, bits ^ ((bits >> 63) & 0x7FFF_FFFF_FFFF_FFFF));
                }
                nanCount++;
                continue;
            }
            if (first)
            {
                min = max = val;
                first = false;
            }
            else
            {
#if NET8_0_OR_GREATER
                if (val < min || (val == 0.0 && min == 0.0 && double.IsNegative(val)))
                    min = val;
                if (val > max || (val == 0.0 && max == 0.0 && !double.IsNegative(val)))
                    max = val;
#else
                if (val < min || (val == 0.0 && min == 0.0 && IsNegativeDouble(val)))
                    min = val;
                if (val > max || (val == 0.0 && max == 0.0 && !IsNegativeDouble(val)))
                    max = val;
#endif
            }
        }

        if (first)
        {
            if (nans.Any)
                return (Int64BitsToBytes(nans.MinBits), Int64BitsToBytes(nans.MaxBits), nanCount);
            return (null, null, nanCount);
        }

        return (DoubleToBytes(min), DoubleToBytes(max), nanCount);
    }

    /// <summary>
    /// TYPE_ORDER's rule for signed zeros, for a FLOAT/DOUBLE lower bound: a zero minimum is written as
    /// <c>-0.0</c> whichever zero the values held. TYPE_ORDER does not say whether -0 sorts below +0, so
    /// the bound must hold under either reading; a reader that orders them would otherwise prune
    /// <c>x = -0.0</c> from a chunk of <c>+0.0</c>. IEEE 754 total order needs no such rule: it orders
    /// the zeros, and its bounds are exact. Returns <paramref name="bound"/> itself when nothing changes.
    /// </summary>
    internal static byte[] TypeOrderLowerBound(byte[] bound, PhysicalType physicalType) =>
        IsZero(bound, physicalType) ? SignedZero(physicalType, negative: true) : bound;

    /// <summary>The upper-bound counterpart of <see cref="TypeOrderLowerBound"/>: a zero maximum is
    /// written as <c>+0.0</c>.</summary>
    internal static byte[] TypeOrderUpperBound(byte[] bound, PhysicalType physicalType) =>
        IsZero(bound, physicalType) ? SignedZero(physicalType, negative: false) : bound;

    /// <summary>
    /// <paramref name="stats"/> with <see cref="TypeOrderLowerBound"/> and
    /// <see cref="TypeOrderUpperBound"/> applied to its bounds; itself when the column is not
    /// FLOAT/DOUBLE, is written in IEEE 754 total order, or has no zero bound.
    /// </summary>
    internal static Statistics ApplyTypeOrderZeroRule(
        Statistics stats, PhysicalType physicalType, bool totalOrder, StatisticsOrder order = StatisticsOrder.Default)
    {
        if (totalOrder || !IsFloatingPoint(physicalType, order))
            return stats;

        byte[]? Lower(byte[]? b) => b is null ? null : TypeOrderLowerBound(b, physicalType);
        byte[]? Upper(byte[]? b) => b is null ? null : TypeOrderUpperBound(b, physicalType);
        var min = Lower(stats.Min);
        var max = Upper(stats.Max);
        var minValue = Lower(stats.MinValue);
        var maxValue = Upper(stats.MaxValue);
        if (min == stats.Min && max == stats.Max && minValue == stats.MinValue && maxValue == stats.MaxValue)
            return stats;

        return new Statistics
        {
            NullCount = stats.NullCount,
            DistinctCount = stats.DistinctCount,
            Min = min,
            Max = max,
            MinValue = minValue,
            MaxValue = maxValue,
            IsMinValueExact = stats.IsMinValueExact,
            IsMaxValueExact = stats.IsMaxValueExact,
            NanCount = stats.NanCount,
        };
    }

    private static bool IsZero(byte[] bound, PhysicalType physicalType) => physicalType switch
    {
        PhysicalType.Float => bound.Length == 4 && MemoryMarshal.Read<float>(bound) == 0f,
        PhysicalType.Double => bound.Length == 8 && MemoryMarshal.Read<double>(bound) == 0.0,
        PhysicalType.FixedLenByteArray => bound.Length == 2 && (BinaryPrimitives.ReadUInt16LittleEndian(bound) & 0x7FFF) == 0,
        _ => false,
    };

    private static byte[] SignedZero(PhysicalType physicalType, bool negative) => physicalType switch
    {
        PhysicalType.Float => FloatToBytes(negative ? -0.0f : 0.0f),
        PhysicalType.Double => DoubleToBytes(negative ? -0.0 : 0.0),
        _ => Half16Bytes(negative ? 0x8000 : 0x0000),
    };

    /// <summary>
    /// Whether a column's statistics follow the floating-point rules — NaN kept out of the bounds and
    /// counted, signed zeros, IEEE 754 total order: FLOAT, DOUBLE, and FLOAT16 (a FIXED_LEN_BYTE_ARRAY(2)
    /// written with <see cref="StatisticsOrder.Float16"/>).
    /// </summary>
    internal static bool IsFloatingPoint(PhysicalType physicalType, StatisticsOrder order) =>
        physicalType is PhysicalType.Float or PhysicalType.Double || order == StatisticsOrder.Float16;

    /// <summary>Whether a FLOAT, DOUBLE or FLOAT16 value is a NaN; FLOAT16 is told apart by width.</summary>
    internal static bool IsNaN(ReadOnlySpan<byte> value, PhysicalType physicalType) => value.Length switch
    {
        2 => IsHalfNaN(BinaryPrimitives.ReadUInt16LittleEndian(value)),
        _ when physicalType == PhysicalType.Float => float.IsNaN(MemoryMarshal.Read<float>(value)),
        _ => double.IsNaN(MemoryMarshal.Read<double>(value)),
    };

    /// <summary>Whether a half-precision float's bits are a NaN: all-ones exponent, non-zero mantissa.</summary>
    private static bool IsHalfNaN(int bits) => (bits & 0x7C00) == 0x7C00 && (bits & 0x03FF) != 0;

    /// <summary>A half-precision float's total-order key; see <see cref="TotalOrderKey"/>.</summary>
    private static int HalfKey(int bits)
    {
        int signed = (short)bits;
        return signed ^ ((signed >> 15) & 0x7FFF);
    }

    /// <summary>
    /// A FLOAT, DOUBLE or FLOAT16 value's position in IEEE 754 total order, as a signed integer: the
    /// bits, with a negative value's magnitude bits flipped so that more negative sorts lower. Orders
    /// NaNs too — by sign, then payload — which is what an all-NaN chunk's bounds need under total
    /// order. For non-NaN values it is numeric order with -0 below +0. FLOAT16 is told apart by width.
    /// </summary>
    internal static long TotalOrderKey(ReadOnlySpan<byte> value, PhysicalType physicalType)
    {
        if (value.Length == 2)
            return HalfKey(BinaryPrimitives.ReadUInt16LittleEndian(value));

        if (physicalType == PhysicalType.Float)
        {
            int bits = BinaryPrimitives.ReadInt32LittleEndian(value);
            return bits ^ ((bits >> 31) & 0x7FFF_FFFF);
        }

        long bits64 = BinaryPrimitives.ReadInt64LittleEndian(value);
        return bits64 ^ ((bits64 >> 63) & 0x7FFF_FFFF_FFFF_FFFF);
    }

    /// <summary>
    /// Tracks the smallest and largest NaN in IEEE 754 total order, kept as raw bits so payloads
    /// survive. Under total order these are an all-NaN chunk's or page's bounds (parquet.thrift).
    /// </summary>
    private struct NaNRange
    {
        private long _minKey, _maxKey;
        public long MinBits, MaxBits;
        public bool Any;

        public void Add(long bits, long key)
        {
            if (!Any || key < _minKey) { _minKey = key; MinBits = bits; }
            if (!Any || key > _maxKey) { _maxKey = key; MaxBits = bits; }
            Any = true;
        }
    }

    private static byte[] Int32BitsToBytes(long bits)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, (int)bits);
        return bytes;
    }

    private static byte[] Int64BitsToBytes(long bits)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, bits);
        return bytes;
    }

    private static byte[] FloatToBytes(float value)
    {
        var bytes = new byte[4];
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteSingleLittleEndian(bytes, value);
#else
        BitConverter.GetBytes(value).CopyTo(bytes, 0);
#endif
        return bytes;
    }

    private static byte[] DoubleToBytes(double value)
    {
        var bytes = new byte[8];
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteDoubleLittleEndian(bytes, value);
#else
        BitConverter.GetBytes(value).CopyTo(bytes, 0);
#endif
        return bytes;
    }

    private static (byte[]?, byte[]?, bool, bool) ComputeByteArrayMinMaxTruncated(
        IArrowArray array, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> rawData = data.Buffers[2].Span;

        var (minIdx, maxIdx) = FindByteArrayMinMax(array, defLevels, 0, array.Length);
        if (minIdx < 0) return (null, null, true, true);

        return TruncateBinaryStats(
            rawData.Slice(offsets[minIdx], offsets[minIdx + 1] - offsets[minIdx]),
            rawData.Slice(offsets[maxIdx], offsets[maxIdx + 1] - offsets[maxIdx]));
    }

    /// <summary>Positions of the least and greatest values in the range, or -1 if all are null.</summary>
    private static (int MinIdx, int MaxIdx) FindByteArrayMinMax(
        IArrowArray array, int[]? defLevels, int start, int end)
    {
        var data = array.Data;
        ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> rawData = data.Buffers[2].Span;

        int minIdx = -1, maxIdx = -1;

        for (int i = start; i < end; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            if (minIdx < 0)
            {
                minIdx = maxIdx = i;
            }
            else
            {
                var val = rawData.Slice(offsets[i], offsets[i + 1] - offsets[i]);
                if (val.SequenceCompareTo(rawData.Slice(offsets[minIdx], offsets[minIdx + 1] - offsets[minIdx])) < 0)
                    minIdx = i;
                if (val.SequenceCompareTo(rawData.Slice(offsets[maxIdx], offsets[maxIdx + 1] - offsets[maxIdx])) > 0)
                    maxIdx = i;
            }
        }

        return (minIdx, maxIdx);
    }

    private static (byte[]?, byte[]?) ComputeFlbaMinMax(
        IArrowArray array, int[]? defLevels, int typeLength, StatisticsOrder order, int start, int end)
    {
        var valueBuffer = array.Data.Buffers[1].Span;
        int minIdx = -1, maxIdx = -1;

        for (int i = start; i < end; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            if (minIdx < 0)
            {
                minIdx = maxIdx = i;
            }
            else
            {
                var val = valueBuffer.Slice(i * typeLength, typeLength);
                if (CompareFlba(val, valueBuffer.Slice(minIdx * typeLength, typeLength), order) < 0)
                    minIdx = i;
                if (CompareFlba(val, valueBuffer.Slice(maxIdx * typeLength, typeLength), order) > 0)
                    maxIdx = i;
            }
        }

        if (minIdx < 0) return (null, null);

        // FLBA values are typically short (16/32 bytes for decimals), no truncation needed
        return (
            valueBuffer.Slice(minIdx * typeLength, typeLength).ToArray(),
            valueBuffer.Slice(maxIdx * typeLength, typeLength).ToArray());
    }

    /// <summary>
    /// Orders two FIXED_LEN_BYTE_ARRAY values for statistics.
    /// </summary>
    /// <remarks>
    /// Unsigned lexicographic is right for UUIDs and plain fixed-size binary, and wrong for two
    /// carriers of signed integers, where it sorts every negative value (a leading 0xFF) above every
    /// positive one. DECIMAL is big-endian two's complement (ColumnChunkWriter has rewritten the buffer
    /// before this runs), so it compares as a signed number: the first byte signed, the rest unsigned.
    /// The extended-precision timestamp carrier (apache/parquet-format#600) is LITTLE-endian, so its
    /// most significant byte is last.
    /// </remarks>
    private static int CompareFlba(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, StatisticsOrder order)
        => order switch
        {
            StatisticsOrder.ExtendedTimestamp => ExtendedTimestamp.Compare(left, right),
            StatisticsOrder.SignedBigEndian => CompareSignedBigEndian(left, right),
            // NaN never reaches here: the scans keep it out of the bounds.
            StatisticsOrder.Float16 => HalfKey(BinaryPrimitives.ReadUInt16LittleEndian(left))
                .CompareTo(HalfKey(BinaryPrimitives.ReadUInt16LittleEndian(right))),
            _ => left.SequenceCompareTo(right),
        };

    /// <summary>Orders two equal-width big-endian two's-complement integers by value.</summary>
    private static int CompareSignedBigEndian(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        int bySign = ((sbyte)left[0]).CompareTo((sbyte)right[0]);
        return bySign != 0 ? bySign : left[1..].SequenceCompareTo(right[1..]);
    }

    // ── Truncation helpers ──

    /// <summary>
    /// Truncates binary min/max to <see cref="MaxBinaryStatLength"/> bytes.
    /// Min is truncated by taking the prefix.
    /// Max is truncated by taking the prefix and incrementing the last byte
    /// to maintain the upper-bound guarantee.
    /// </summary>
    private static (byte[]?, byte[]?, bool, bool) TruncateBinaryStats(
        ReadOnlySpan<byte> minValue, ReadOnlySpan<byte> maxValue)
    {
        byte[] minBytes;
        bool minExact;
        if (minValue.Length <= MaxBinaryStatLength)
        {
            minBytes = minValue.ToArray();
            minExact = true;
        }
        else
        {
            minBytes = minValue[..MaxBinaryStatLength].ToArray();
            minExact = false;
        }

        byte[] maxBytes;
        bool maxExact;
        if (maxValue.Length <= MaxBinaryStatLength)
        {
            maxBytes = maxValue.ToArray();
            maxExact = true;
        }
        else
        {
            maxBytes = TruncateMax(maxValue);
            maxExact = false;
        }

        return (minBytes, maxBytes, minExact, maxExact);
    }

    /// <summary>
    /// Truncates a max value by taking a prefix and incrementing the last byte.
    /// If the last byte is 0xFF, walks backwards to find an incrementable byte.
    /// Falls back to the full value if all bytes are 0xFF.
    /// </summary>
    private static byte[] TruncateMax(ReadOnlySpan<byte> value)
    {
        var result = value[..MaxBinaryStatLength].ToArray();
        for (int i = result.Length - 1; i >= 0; i--)
        {
            if (result[i] < 0xFF)
            {
                result[i]++;
                // Zero out bytes after the incremented position
                for (int j = i + 1; j < result.Length; j++)
                    result[j] = 0;
                return result;
            }
        }
        // All 0xFF in prefix — can't increment, return full value
        return value.ToArray();
    }

    private static Comparison<int> Int32Comparison(StatisticsOrder order) =>
        order == StatisticsOrder.Unsigned ? CompareUInt32 : CompareInt32;

    private static Comparison<long> Int64Comparison(StatisticsOrder order) =>
        order == StatisticsOrder.Unsigned ? CompareUInt64 : CompareInt64;

    private static int CompareInt32(int a, int b) => a.CompareTo(b);
    private static int CompareInt64(long a, long b) => a.CompareTo(b);
    private static int CompareUInt32(int a, int b) => unchecked((uint)a).CompareTo(unchecked((uint)b));
    private static int CompareUInt64(long a, long b) => unchecked((ulong)a).CompareTo(unchecked((ulong)b));

#if !NET8_0_OR_GREATER
    private static bool IsNegativeFloat(float value) =>
        value < 0 || (value == 0 && float.IsNegativeInfinity(1.0f / value));

    private static bool IsNegativeDouble(double value) =>
        value < 0 || (value == 0 && double.IsNegativeInfinity(1.0 / value));
#endif
}
