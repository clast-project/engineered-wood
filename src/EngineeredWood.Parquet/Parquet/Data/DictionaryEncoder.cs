// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Analyzes an Arrow column and builds a dictionary if cardinality is sufficiently low.
/// Used by the write path for dictionary encoding (analyze-before-write strategy).
/// </summary>
internal static class DictionaryEncoder
{
    internal const float CardinalityThreshold = 0.20f;

    internal readonly struct DictionaryResult
    {
        /// <summary>PLAIN-encoded dictionary page body (unique values in index order).</summary>
        public required byte[] DictionaryPageData { get; init; }

        /// <summary>Number of unique values in the dictionary.</summary>
        public required int DictionaryCount { get; init; }

        /// <summary>Dictionary index for each non-null value (length == nonNullCount).</summary>
        public required int[] Indices { get; init; }
    }

    /// <summary>
    /// Attempts to build a dictionary for the given column.
    /// Returns null if dictionary encoding is disabled, not applicable, or thresholds are exceeded.
    /// </summary>
    public static DictionaryResult? TryEncode(
        IArrowArray array,
        PhysicalType physicalType,
        int typeLength,
        int[]? defLevels,
        int nonNullCount,
        ParquetWriteOptions options)
    {
        if (!options.DictionaryEnabled || physicalType == PhysicalType.Boolean)
            return null;

        // For very small columns, dictionary overhead isn't worthwhile
        if (nonNullCount == 0)
            return null;

        return physicalType switch
        {
            PhysicalType.Int32 => TryEncodeFixed<int>(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit),
            PhysicalType.Int64 => TryEncodeFixed<long>(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit),
            PhysicalType.Float => TryEncodeFixed<float>(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit),
            PhysicalType.Double => TryEncodeFixed<double>(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit),
            PhysicalType.ByteArray => TryEncodeByteArray(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit),
            PhysicalType.FixedLenByteArray => TryEncodeFixedLenByteArray(array, defLevels, nonNullCount, typeLength, options.DictionaryPageSizeLimit),
            _ => null,
        };
    }

    /// <summary>
    /// Calculates the bit width needed to encode dictionary indices.
    /// </summary>
    public static int GetIndexBitWidth(int dictionaryCount) =>
#if NET8_0_OR_GREATER
        dictionaryCount <= 1 ? 1 : 32 - int.LeadingZeroCount(dictionaryCount - 1);
#else
        dictionaryCount <= 1 ? 1 : 32 - BitPolyfills.LeadingZeroCount((uint)(dictionaryCount - 1));
#endif

    private static DictionaryResult? TryEncodeFixed<T>(
        IArrowArray array, int[]? defLevels, int nonNullCount, int pageSizeLimit)
        where T : unmanaged, IEquatable<T>
    {
        int rowCount = array.Length;
        int maxCardinality = Math.Max(1, (int)(nonNullCount * CardinalityThreshold));
        int elementSize = Marshal.SizeOf<T>();

        var dict = new Dictionary<T, int>();
        var indices = new int[nonNullCount];
        var valueBuffer = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span);
        int idx = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            T value = valueBuffer[i];
            if (!dict.TryGetValue(value, out int dictIdx))
            {
                if (dict.Count >= maxCardinality)
                    return null;

                dictIdx = dict.Count;
                dict[value] = dictIdx;

                if (dict.Count * elementSize > pageSizeLimit)
                    return null;
            }
            indices[idx++] = dictIdx;
        }

        // Produce PLAIN-encoded dictionary page: values in index order
        var entries = new T[dict.Count];
        foreach (var kvp in dict)
            entries[kvp.Value] = kvp.Key;

        var dictPage = new byte[dict.Count * elementSize];
        MemoryMarshal.AsBytes(entries.AsSpan()).CopyTo(dictPage);

        return new DictionaryResult
        {
            DictionaryPageData = dictPage,
            DictionaryCount = dict.Count,
            Indices = indices,
        };
    }

    private static DictionaryResult? TryEncodeByteArray(
        IArrowArray array, int[]? defLevels, int nonNullCount, int pageSizeLimit)
    {
        int rowCount = array.Length;
        int maxCardinality = Math.Max(1, (int)(nonNullCount * CardinalityThreshold));

        var data = array.Data;
        ReadOnlySpan<int> arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> arrowData = data.Buffers[2].Span;

        // Open-addressing hash table with linear probing
        var table = new BytesHashTable(Math.Max(16, maxCardinality * 2));
        var uniqueEntries = new List<byte[]>();
        var indices = new int[nonNullCount];
        int idx = 0;
        int totalDictBytes = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            int start = arrowOffsets[i];
            int len = arrowOffsets[i + 1] - start;
            ReadOnlySpan<byte> valueBytes = arrowData.Slice(start, len);

            int dictIdx = table.GetOrAdd(valueBytes, uniqueEntries.Count);

            if (dictIdx == uniqueEntries.Count)
            {
                // New entry
                if (uniqueEntries.Count >= maxCardinality)
                    return null;

                var copy = valueBytes.ToArray();
                uniqueEntries.Add(copy);
                totalDictBytes += 4 + len;

                if (totalDictBytes > pageSizeLimit)
                    return null;
            }

            indices[idx++] = dictIdx;
        }

        // Produce PLAIN-encoded dictionary page (4-byte LE length prefix per entry)
        var dictPage = new byte[totalDictBytes];
        int pos = 0;
        foreach (var entry in uniqueEntries)
        {
            BinaryPrimitives.WriteInt32LittleEndian(dictPage.AsSpan(pos), entry.Length);
            pos += 4;
            entry.CopyTo(dictPage.AsSpan(pos));
            pos += entry.Length;
        }

        return new DictionaryResult
        {
            DictionaryPageData = dictPage,
            DictionaryCount = uniqueEntries.Count,
            Indices = indices,
        };
    }

    private static DictionaryResult? TryEncodeFixedLenByteArray(
        IArrowArray array, int[]? defLevels, int nonNullCount, int typeLength, int pageSizeLimit)
    {
        int rowCount = array.Length;
        int maxCardinality = Math.Max(1, (int)(nonNullCount * CardinalityThreshold));

        var valueBuffer = array.Data.Buffers[1].Span;

        var table = new BytesHashTable(Math.Max(16, maxCardinality * 2));
        var uniqueEntries = new List<byte[]>();
        var indices = new int[nonNullCount];
        int idx = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            ReadOnlySpan<byte> valueBytes = valueBuffer.Slice(i * typeLength, typeLength);
            int dictIdx = table.GetOrAdd(valueBytes, uniqueEntries.Count);

            if (dictIdx == uniqueEntries.Count)
            {
                // New entry
                if (uniqueEntries.Count >= maxCardinality)
                    return null;

                uniqueEntries.Add(valueBytes.ToArray());

                if (uniqueEntries.Count * typeLength > pageSizeLimit)
                    return null;
            }

            indices[idx++] = dictIdx;
        }

        // Produce PLAIN-encoded dictionary page (concatenated, no length prefix for FLBA)
        var dictPage = new byte[uniqueEntries.Count * typeLength];
        int pos = 0;
        foreach (var entry in uniqueEntries)
        {
            entry.CopyTo(dictPage.AsSpan(pos));
            pos += typeLength;
        }

        return new DictionaryResult
        {
            DictionaryPageData = dictPage,
            DictionaryCount = uniqueEntries.Count,
            Indices = indices,
        };
    }

    /// <summary>
    /// Open-addressing hash table with linear probing for byte sequences.
    /// More cache-friendly than Dictionary&lt;int, List&lt;...&gt;&gt; with collision chains.
    /// Uses FNV-1a for hashing.
    /// </summary>
    private sealed class BytesHashTable
    {
        private readonly int[] _hashes;    // 0 = empty slot
        private readonly byte[]?[] _keys;
        private readonly int[] _values;
        private readonly int _mask;

        public BytesHashTable(int capacity)
        {
            // Round up to next power of 2
            int size = 1;
            while (size < capacity) size <<= 1;
            _hashes = new int[size];
            _keys = new byte[]?[size];
            _values = new int[size];
            _mask = size - 1;
        }

        /// <summary>
        /// Returns the existing index for the key, or inserts <paramref name="nextIndex"/>
        /// and returns it if the key is new.
        /// </summary>
        public int GetOrAdd(ReadOnlySpan<byte> key, int nextIndex)
        {
            uint h = Fnv1a(key);
            // Ensure hash is non-zero (0 = empty sentinel)
            int hash = (int)(h | 1);
            int slot = hash & _mask;

            while (true)
            {
                if (_hashes[slot] == 0)
                {
                    // Empty slot — insert
                    _hashes[slot] = hash;
                    _keys[slot] = key.ToArray();
                    _values[slot] = nextIndex;
                    return nextIndex;
                }

                if (_hashes[slot] == hash && key.SequenceEqual(_keys[slot]))
                {
                    return _values[slot];
                }

                slot = (slot + 1) & _mask;
            }
        }

        private static uint Fnv1a(ReadOnlySpan<byte> data)
        {
            uint hash = 2166136261u;
            for (int i = 0; i < data.Length; i++)
            {
                hash ^= data[i];
                hash *= 16777619u;
            }
            return hash;
        }
    }
}
