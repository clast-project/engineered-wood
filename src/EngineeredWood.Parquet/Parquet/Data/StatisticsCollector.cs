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
    /// order: an all-NaN chunk records the NaN value as its bounds rather than
    /// omitting them. When <see langword="false"/> (the TYPE_ORDER default) an
    /// all-NaN chunk omits min/max. NaN is excluded from the bounds either way,
    /// and a <c>nan_count</c> is always recorded for FLOAT/DOUBLE columns.
    /// </param>
    public static Statistics Compute(
        IArrowArray array,
        PhysicalType physicalType,
        int typeLength,
        int[]? defLevels,
        int nonNullCount,
        int rowCount,
        bool floatingPointTotalOrder = false)
    {
        long nullCount = rowCount - nonNullCount;
        bool isFloatingPoint = physicalType is PhysicalType.Float or PhysicalType.Double;

        if (nonNullCount == 0)
        {
            // No values ⇒ no NaN. nan_count is mandatory for FP columns even at zero.
            return new Statistics { NullCount = nullCount, NanCount = isFloatingPoint ? 0L : null };
        }

        // Floating-point columns are scanned separately so the NaN count, min,
        // and max all come from a single pass.
        if (isFloatingPoint)
        {
            var (fpMin, fpMax, nanCount) = physicalType == PhysicalType.Float
                ? ComputeFloatMinMax(array, defLevels, floatingPointTotalOrder)
                : ComputeDoubleMinMax(array, defLevels, floatingPointTotalOrder);

            return new Statistics
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
        }

        var (minBytes, maxBytes, minExact, maxExact) = physicalType switch
        {
            PhysicalType.Boolean => WithExact(ComputeBooleanMinMax(array, defLevels)),
            PhysicalType.Int32 => WithExact(ComputeMinMax<int>(array, defLevels, CompareInt32)),
            PhysicalType.Int64 => WithExact(ComputeMinMax<long>(array, defLevels, CompareInt64)),
            PhysicalType.ByteArray => ComputeByteArrayMinMaxTruncated(array, defLevels),
            PhysicalType.FixedLenByteArray => ComputeFlbaMinMaxTruncated(array, defLevels, typeLength),
            _ => (null, null, true, true),
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
    /// Computes statistics from dictionary entries only (O(unique) instead of O(total)).
    /// For dict-encoded ByteArray columns, min/max of all values equals min/max of unique entries.
    /// </summary>
    public static Statistics ComputeFromDictEntries(
        byte[] dictPageData,
        int dictCount,
        PhysicalType physicalType,
        int typeLength,
        long nullCount)
    {
        if (dictCount == 0)
            return new Statistics { NullCount = nullCount };

        var (minBytes, maxBytes, minExact, maxExact) = physicalType switch
        {
            PhysicalType.ByteArray => ComputeByteArrayMinMaxFromDict(dictPageData, dictCount),
            PhysicalType.FixedLenByteArray => ComputeFlbaMinMaxFromDict(dictPageData, dictCount, typeLength),
            _ => ComputeFixedMinMaxFromDict(dictPageData, dictCount, physicalType, typeLength),
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
        byte[] dictPage, int dictCount, int typeLength)
    {
        var span = dictPage.AsSpan();
        int minIdx = 0, maxIdx = 0;

        for (int i = 1; i < dictCount; i++)
        {
            var val = span.Slice(i * typeLength, typeLength);
            if (val.SequenceCompareTo(span.Slice(minIdx * typeLength, typeLength)) < 0)
                minIdx = i;
            if (val.SequenceCompareTo(span.Slice(maxIdx * typeLength, typeLength)) > 0)
                maxIdx = i;
        }

        // FLBA values are typically short (16 or 32 bytes for decimals), so no truncation
        return (
            span.Slice(minIdx * typeLength, typeLength).ToArray(),
            span.Slice(maxIdx * typeLength, typeLength).ToArray(),
            true, true);
    }

    private static (byte[]?, byte[]?, bool, bool) ComputeFixedMinMaxFromDict(
        byte[] dictPage, int dictCount, PhysicalType physicalType, int typeLength)
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
            PhysicalType.Int32 => ComputeFixedMinMaxTyped<int>(span, dictCount, elementSize, CompareInt32),
            PhysicalType.Int64 => ComputeFixedMinMaxTyped<long>(span, dictCount, elementSize, CompareInt64),
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

    private static (byte[]?, byte[]?) ComputeBooleanMinMax(IArrowArray array, int[]? defLevels)
    {
        var boolArray = (BooleanArray)array;
        bool hasTrue = false, hasFalse = false;

        for (int i = 0; i < array.Length; i++)
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
        IArrowArray array, int[]? defLevels, Comparison<T> compare)
        where T : unmanaged
    {
        var valueBuffer = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span);
        bool first = true;
        T min = default, max = default;

        for (int i = 0; i < array.Length; i++)
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
        IArrowArray array, int[]? defLevels, bool totalOrder)
    {
        var valueBuffer = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span);
        bool first = true;
        float min = 0, max = 0;
        long nanCount = 0;
        float firstNaN = 0;

        for (int i = 0; i < array.Length; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            float val = valueBuffer[i];
            if (float.IsNaN(val))
            {
                if (nanCount == 0) firstNaN = val;
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
            // No non-NaN values. Under IEEE 754 total order, an all-NaN chunk
            // records the NaN as both bounds; under TYPE_ORDER, min/max are omitted.
            if (totalOrder && nanCount > 0)
                return (FloatToBytes(firstNaN), FloatToBytes(firstNaN), nanCount);
            return (null, null, nanCount);
        }

        return (FloatToBytes(min), FloatToBytes(max), nanCount);
    }

    private static (byte[]?, byte[]?, long) ComputeDoubleMinMax(
        IArrowArray array, int[]? defLevels, bool totalOrder)
    {
        var valueBuffer = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span);
        bool first = true;
        double min = 0, max = 0;
        long nanCount = 0;
        double firstNaN = 0;

        for (int i = 0; i < array.Length; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            double val = valueBuffer[i];
            if (double.IsNaN(val))
            {
                if (nanCount == 0) firstNaN = val;
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
            if (totalOrder && nanCount > 0)
                return (DoubleToBytes(firstNaN), DoubleToBytes(firstNaN), nanCount);
            return (null, null, nanCount);
        }

        return (DoubleToBytes(min), DoubleToBytes(max), nanCount);
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

        int minIdx = -1, maxIdx = -1;

        for (int i = 0; i < array.Length; i++)
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

        if (minIdx < 0) return (null, null, true, true);

        return TruncateBinaryStats(
            rawData.Slice(offsets[minIdx], offsets[minIdx + 1] - offsets[minIdx]),
            rawData.Slice(offsets[maxIdx], offsets[maxIdx + 1] - offsets[maxIdx]));
    }

    private static (byte[]?, byte[]?, bool, bool) ComputeFlbaMinMaxTruncated(
        IArrowArray array, int[]? defLevels, int typeLength)
    {
        var valueBuffer = array.Data.Buffers[1].Span;
        int minIdx = -1, maxIdx = -1;

        for (int i = 0; i < array.Length; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            if (minIdx < 0)
            {
                minIdx = maxIdx = i;
            }
            else
            {
                var val = valueBuffer.Slice(i * typeLength, typeLength);
                if (val.SequenceCompareTo(valueBuffer.Slice(minIdx * typeLength, typeLength)) < 0)
                    minIdx = i;
                if (val.SequenceCompareTo(valueBuffer.Slice(maxIdx * typeLength, typeLength)) > 0)
                    maxIdx = i;
            }
        }

        if (minIdx < 0) return (null, null, true, true);

        // FLBA values are typically short (16/32 bytes for decimals), no truncation needed
        return (
            valueBuffer.Slice(minIdx * typeLength, typeLength).ToArray(),
            valueBuffer.Slice(maxIdx * typeLength, typeLength).ToArray(),
            true, true);
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

    private static int CompareInt32(int a, int b) => a.CompareTo(b);
    private static int CompareInt64(long a, long b) => a.CompareTo(b);

#if !NET8_0_OR_GREATER
    private static bool IsNegativeFloat(float value) =>
        value < 0 || (value == 0 && float.IsNegativeInfinity(1.0f / value));

    private static bool IsNegativeDouble(double value) =>
        value < 0 || (value == 0 && double.IsNegativeInfinity(1.0 / value));
#endif
}
