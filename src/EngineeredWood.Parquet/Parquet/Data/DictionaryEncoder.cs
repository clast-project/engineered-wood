// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

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

        /// <summary>
        /// Dictionary index for each non-null value (length == nonNullCount), or null when
        /// <see cref="IndexRuns"/> carries the same sequence in run form instead.
        /// </summary>
        public required int[]? Indices { get; init; }

        /// <summary>
        /// The indices as runs, for a column that arrived run-end encoded. Set INSTEAD of
        /// <see cref="Indices"/>: materializing one index per row is exactly the O(N) allocation such a
        /// column exists to avoid, and the page encoder consumes runs directly.
        /// </summary>
        public DictionaryIndexRuns? IndexRuns { get; init; }
    }

    /// <summary>
    /// Dictionary indices in run form — <c>Values[r]</c> repeated <c>Lengths[r]</c> times, concatenated.
    /// The sequence covers NON-NULL rows only, exactly as <see cref="DictionaryResult.Indices"/> does, so a
    /// run of nulls contributes nothing here rather than a run of some placeholder index.
    /// </summary>
    internal readonly struct DictionaryIndexRuns
    {
        public required int[] Values { get; init; }

        public required int[] Lengths { get; init; }
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

        // A run-end encoded column is dictionary-encoded from its RUNS. Everything below reads one value
        // slot per row, which such a column does not have.
        if (array is RunEndEncodedArray ree)
            return TryEncodeRuns(ree, physicalType, typeLength, nonNullCount, options.DictionaryPageSizeLimit);

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

        // The fixed-width twin of the constant probe in TryEncodeByteArray, and cheaper: every value slot is
        // the same width, so "all rows equal" is exactly "the value buffer is periodic with period
        // elementSize" — one vectorized compare that stops at the first differing byte.
        //
        // Byte equality is stricter than the Dictionary<T,int> below, which compares by value. That only
        // ever costs a missed fast path (+0.0 and -0.0 are equal but differ in bytes, so such a column
        // falls through to the loop), never a wrong answer: byte-identical values read back identical.
        if (defLevels is null && rowCount > 0
            && IsConstantFixedWidth(array.Data.Buffers[1].Span, rowCount, elementSize))
        {
            if (elementSize > pageSizeLimit)
                return null;

            var constPage = array.Data.Buffers[1].Span.Slice(0, elementSize).ToArray();
            return new DictionaryResult
            {
                DictionaryPageData = constPage,
                DictionaryCount = 1,
                Indices = new int[nonNullCount],
            };
        }

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

        // A column holding one value in every row is a common shape on the write path — a materialized
        // partition value, a CDF _change_type, anything ArrowCompute.Repeat produced — and hashing every row
        // to discover that it has one distinct value is what dominates encoding it. Recognising it up front
        // replaces that with a length check and one comparison. The probe costs almost nothing when it
        // fails: see TryDescribeConstantValue.
        if (defLevels is null &&
            TryDescribeConstantValue(arrowOffsets, arrowData, rowCount, out int constStart, out int constLen))
        {
            // The dictionary page has the same PLAIN shape as below, with one entry.
            if (4 + constLen > pageSizeLimit)
                return null;

            var constPage = new byte[4 + constLen];
            BinaryPrimitives.WriteInt32LittleEndian(constPage, constLen);
            arrowData.Slice(constStart, constLen).CopyTo(constPage.AsSpan(4));

            return new DictionaryResult
            {
                DictionaryPageData = constPage,
                DictionaryCount = 1,
                // Every row maps to entry 0, and a fresh int[] is already zeroed — nothing to write per row.
                Indices = new int[nonNullCount],
            };
        }

        // Open-addressing hash table with linear probing
        var table = new BytesHashTable();
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

            int dictIdx = table.GetOrAdd(valueBytes, uniqueEntries.Count, out byte[]? inserted);

            if (inserted is not null)
            {
                // New entry — the table's copy becomes the dictionary's, rather than a second one.
                if (uniqueEntries.Count >= maxCardinality)
                    return null;

                uniqueEntries.Add(inserted);
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

    /// <summary>
    /// Builds the dictionary for a run-end encoded column out of its RUNS: one hash per run rather than
    /// per row, and indices emitted in run form, so both the time and the allocation are O(runs). A
    /// constant column — one run — costs one lookup and a two-element index run whatever its row count.
    ///
    /// <para>Nulls come from the run's value, which is the only place a run-end encoded array keeps them
    /// (its own <c>IsNull</c> answers false for every row). A null run contributes no indices at all,
    /// matching what the per-row encoders do with a def level of 0.</para>
    ///
    /// <para>Values are compared as BYTES, like the constant probes above, where the fixed-width per-row
    /// path compares as values. The difference costs at most a duplicate dictionary entry (+0.0 and -0.0
    /// are equal but differ in bytes) and never a wrong value, since byte-identical entries read back
    /// identical.</para>
    /// </summary>
    private static DictionaryResult? TryEncodeRuns(
        RunEndEncodedArray array,
        PhysicalType physicalType,
        int typeLength,
        int nonNullCount,
        int pageSizeLimit)
    {
        var values = array.Values;

        // The run arm reads value slots straight out of the raw buffers, so it has to recognise the
        // LAYOUT and not merely the physical type. MapArrowType routes LargeString (64-bit offsets) and
        // StringView (a different layout entirely) to BYTE_ARRAY alongside String/Binary, and reading
        // either of those as 32-bit offsets is silent corruption rather than a failure. An unrecognised
        // layout declines here, which lands the column on the expansion path where the per-row encoders
        // handle it exactly as they do today.
        bool lengthPrefixed = physicalType == PhysicalType.ByteArray;
        int slotWidth = 0;
        if (lengthPrefixed
            ? values.Data.DataType is not (Apache.Arrow.Types.StringType or BinaryType)
            : !TryGetSlotWidth(values.Data.DataType, physicalType, typeLength, out slotWidth))
        {
            return null;
        }

        int maxCardinality = Math.Max(1, (int)(nonNullCount * CardinalityThreshold));

        // The runs are an EXACT upper bound on the distinct values — one run cannot hold two of them —
        // where the per-row arms have nothing to go on and must grow into the right size. Passing it
        // costs nothing and means a column whose bound is tight never rehashes at all.
        int distinctBound = Math.Min(maxCardinality, values.Length) + 1;
        var table = new BytesHashTable(distinctBound);
        var uniqueEntries = new List<byte[]>();
        var runIndices = new List<int>();
        var runLengths = new List<int>();
        int totalDictBytes = 0;
        long countedNonNull = 0;

        foreach (var run in RunEndEncoding.EnumerateRuns(array))
        {
            if (values.IsNull(run.PhysicalIndex))
                continue;

            countedNonNull += run.Length;

            var valueBytes = RunValueBytes(values, run.PhysicalIndex, lengthPrefixed, slotWidth);
            int dictIdx = table.GetOrAdd(valueBytes, uniqueEntries.Count, out byte[]? inserted);

            if (inserted is not null)
            {
                if (uniqueEntries.Count >= maxCardinality)
                    return null;

                uniqueEntries.Add(inserted);
                totalDictBytes += (lengthPrefixed ? 4 : 0) + valueBytes.Length;

                if (totalDictBytes > pageSizeLimit)
                    return null;
            }

            // Two runs that map to the same entry are one run in the index stream — they are adjacent
            // there whenever the runs between them were null, and the RLE encoder cannot merge what it is
            // handed as two.
            if (runIndices.Count > 0 && runIndices[runIndices.Count - 1] == dictIdx)
                runLengths[runLengths.Count - 1] += run.Length;
            else
            {
                runIndices.Add(dictIdx);
                runLengths.Add(run.Length);
            }
        }

        // The caller derived the definition levels from these same runs, so a disagreement means the two
        // views of this column have diverged — the array carries nulls the levels do not admit, or the
        // reverse. Declining sends it down the expansion path, where it is encoded on the caller's terms
        // rather than written with an index stream the levels cannot address.
        if (countedNonNull != nonNullCount)
            return null;

        var dictPage = new byte[totalDictBytes];
        int pos = 0;
        foreach (var entry in uniqueEntries)
        {
            if (lengthPrefixed)
            {
                BinaryPrimitives.WriteInt32LittleEndian(dictPage.AsSpan(pos), entry.Length);
                pos += 4;
            }

            entry.CopyTo(dictPage.AsSpan(pos));
            pos += entry.Length;
        }

        return new DictionaryResult
        {
            DictionaryPageData = dictPage,
            DictionaryCount = uniqueEntries.Count,
            Indices = null,
            IndexRuns = new DictionaryIndexRuns
            {
                Values = runIndices.ToArray(),
                Lengths = runLengths.ToArray(),
            },
        };
    }

    /// <summary>One value slot of a run-end encoded column's values child, as raw bytes.</summary>
    private static ReadOnlySpan<byte> RunValueBytes(
        IArrowArray values, int index, bool lengthPrefixed, int slotWidth)
    {
        var data = values.Data;
        int slot = data.Offset + index;

        if (!lengthPrefixed)
            return data.Buffers[1].Span.Slice(slot * slotWidth, slotWidth);

        var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        int start = offsets[slot];
        return data.Buffers[2].Span.Slice(start, offsets[slot + 1] - start);
    }

    /// <summary>
    /// The byte width of one value slot, and whether the Arrow type actually LAYS OUT its values at that
    /// width. Both halves matter: Int8 through Int16 are written as the 4-byte INT32 physical type, and
    /// reading their 1- and 2-byte buffers as 4-byte slots packs several rows into each value. The caller
    /// widens such a column before this point; the check is what keeps that a requirement rather than an
    /// assumption.
    /// </summary>
    private static bool TryGetSlotWidth(
        IArrowType valueType, PhysicalType physicalType, int typeLength, out int width)
    {
        switch (physicalType)
        {
            case PhysicalType.Int32:
                width = 4;
                return valueType is Int32Type or UInt32Type or Date32Type or Time32Type or Decimal32Type;
            case PhysicalType.Int64:
                width = 8;
                return valueType is Int64Type or UInt64Type or Time64Type or TimestampType
                    or DurationType or Decimal64Type;
            case PhysicalType.Float:
                width = 4;
                return valueType is FloatType;
            case PhysicalType.Double:
                width = 8;
                return valueType is DoubleType;
            case PhysicalType.FixedLenByteArray:
                // Decimal128Type and Decimal256Type derive from FixedSizeBinaryType, so both are covered.
                width = typeLength;
                return typeLength > 0 && valueType is FixedSizeBinaryType or HalfFloatType;
            default:
                width = 0;
                return false;
        }
    }

    /// <summary>
    /// Whether a fixed-width value buffer holds <paramref name="rowCount"/> copies of one value — i.e. is
    /// periodic with period <paramref name="elementSize"/>. A single-row column is trivially constant.
    /// </summary>
    private static bool IsConstantFixedWidth(ReadOnlySpan<byte> buffer, int rowCount, int elementSize)
    {
        var body = buffer.Slice(0, rowCount * elementSize);
        return body.Length <= elementSize
            || body.Slice(elementSize).SequenceEqual(body.Slice(0, body.Length - elementSize));
    }

    /// <summary>
    /// Tests whether every row of a variable-width column holds the same bytes, reporting that value's span
    /// when so. Callers must have established that the column has no nulls, since this reads every row.
    ///
    /// <para>The checks are ordered so that a column which is NOT constant — the common case, and the one
    /// that must not pay for this — is rejected as early as possible:</para>
    /// <list type="number">
    ///   <item>Values of differing length make the total length disagree with <c>len * rowCount</c>. That is
    ///   O(1) and rejects most real columns outright.</item>
    ///   <item>Otherwise the value buffer must be <paramref name="rowCount"/> repetitions of one value, i.e.
    ///   periodic with period <c>len</c>. Comparing it against itself shifted by one value settles that in a
    ///   single vectorized compare that stops at the first differing byte — immediately, for a column whose
    ///   rows differ at all early.</item>
    ///   <item>Only a column that has passed both pays the offset walk, which is what actually proves each
    ///   row is one whole value. It is needed: lengths <c>[2,1,3]</c> over <c>"aaaaaa"</c> satisfy both
    ///   checks above while holding three DIFFERENT values.</item>
    /// </list>
    /// </summary>
    private static bool TryDescribeConstantValue(
        ReadOnlySpan<int> offsets, ReadOnlySpan<byte> data, int rowCount, out int start, out int length)
    {
        start = 0;
        length = 0;
        if (rowCount <= 0)
            return false;

        start = offsets[0];
        length = offsets[1] - start;
        int total = offsets[rowCount] - start;

        // (1) Uniform length is necessary, and cheap to refute.
        if ((long)length * rowCount != total)
            return false;

        // (2) Periodicity. An all-empty column (length 0) is constant and lands here with empty slices.
        if (length > 0 && rowCount > 1)
        {
            var body = data.Slice(start, total);
            if (!body.Slice(length).SequenceEqual(body.Slice(0, total - length)))
                return false;
        }

        // (3) Every row is exactly one period into the buffer.
        for (int i = 1; i < rowCount; i++)
        {
            if (offsets[i] - start != (long)i * length)
                return false;
        }

        return true;
    }

    private static DictionaryResult? TryEncodeFixedLenByteArray(
        IArrowArray array, int[]? defLevels, int nonNullCount, int typeLength, int pageSizeLimit)
    {
        int rowCount = array.Length;
        int maxCardinality = Math.Max(1, (int)(nonNullCount * CardinalityThreshold));

        var valueBuffer = array.Data.Buffers[1].Span;

        var table = new BytesHashTable();
        var uniqueEntries = new List<byte[]>();
        var indices = new int[nonNullCount];
        int idx = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            ReadOnlySpan<byte> valueBytes = valueBuffer.Slice(i * typeLength, typeLength);
            int dictIdx = table.GetOrAdd(valueBytes, uniqueEntries.Count, out byte[]? inserted);

            if (inserted is not null)
            {
                // New entry — the table's copy becomes the dictionary's, rather than a second one.
                if (uniqueEntries.Count >= maxCardinality)
                    return null;

                uniqueEntries.Add(inserted);

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
    ///
    /// <para><b>Grows geometrically rather than being sized up front.</b> It used to be built from the
    /// caller's cardinality CAP — a fifth of the row count — so a 1M-row string column allocated 8.4 MB of
    /// table before hashing a single row, identically whether the column went on to hold twelve distinct
    /// values or a million. That is per column and concurrent, since the writer runs columns under
    /// <c>Parallel.For</c>: twenty string columns was ~168 MB of table live at once, independent of the
    /// data. It now starts at 16 slots and doubles.</para>
    ///
    /// <para>The load factor ceiling stays at one half, which is exactly what the old sizing guaranteed
    /// (<c>maxCardinality * 2</c> rounded up to a power of two, against at most <c>maxCardinality</c>
    /// entries). Linear probing degrades sharply as a table fills, so holding the same ceiling keeps probe
    /// lengths what they were rather than trading the memory for lookup cost. The worst case — a column
    /// that really does reach the cap — ends at the same size it was allocated at before, having paid an
    /// amortized two moves per entry to get there.</para>
    /// </summary>
    private sealed class BytesHashTable
    {
        /// <summary>Smallest table. A column with a handful of distinct values never outgrows it.</summary>
        private const int MinimumSize = 16;

        /// <summary>
        /// Largest table. <c>_hashes.Length &lt;&lt; 1</c> overflows to negative past this, and a table
        /// this size is 12 GB across the three arrays — long past where the column itself would have
        /// exhausted memory.
        /// </summary>
        private const int MaximumSize = 1 << 30;

        // Empty until the constructor's Allocate call, which every path goes through; the initializers
        // are here only so the nullable analysis can see that they are never null.
        private int[] _hashes = [];    // 0 = empty slot
        private byte[]?[] _keys = [];
        private int[] _values = [];
        private int _mask;
        private int _count;
        private int _growAt;

        /// <param name="expectedEntries">
        /// An upper bound on the distinct values, for a caller that has one — the run-end encoded arm
        /// does, since a run cannot hold two distinct values. Sizing to it means an exact bound never
        /// rehashes. Zero (the default) starts at the minimum and grows, which is the right answer for a
        /// caller that cannot know.
        /// </param>
        public BytesHashTable(int expectedEntries = 0)
        {
            int size = MinimumSize;
            // Two slots per entry, matching the load-factor ceiling below. Compared as long: a bound near
            // int.MaxValue would otherwise double past the sign bit and spin here.
            while (size < (long)expectedEntries * 2 && size < MaximumSize)
                size <<= 1;

            Allocate(size);
        }

        /// <summary>
        /// Returns the existing index for the key, or inserts <paramref name="nextIndex"/>
        /// and returns it if the key is new.
        /// </summary>
        /// <param name="inserted">
        /// The copy of the key this table now holds, when the key was new; null when it was already
        /// present. Callers keep their own copy of each distinct value for the dictionary page, and
        /// handing them this one is what stops every distinct value being copied twice. It is the table's
        /// comparison key, so a caller that mutated it would corrupt lookups — every caller here only ever
        /// reads it, on the way into the dictionary page.
        /// </param>
        public int GetOrAdd(ReadOnlySpan<byte> key, int nextIndex, out byte[]? inserted)
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
                    var copy = key.ToArray();
                    _hashes[slot] = hash;
                    _keys[slot] = copy;
                    _values[slot] = nextIndex;
                    inserted = copy;

                    // Grown AFTER the insert, so the slot this landed in does not have to survive the
                    // rehash — nothing here returns a slot, only the caller's own index.
                    if (++_count >= _growAt)
                        Grow();

                    return nextIndex;
                }

                if (_hashes[slot] == hash && key.SequenceEqual(_keys[slot]))
                {
                    inserted = null;
                    return _values[slot];
                }

                slot = (slot + 1) & _mask;
            }
        }

        private void Allocate(int size)
        {
            _hashes = new int[size];
            _keys = new byte[]?[size];
            _values = new int[size];
            _mask = size - 1;
            _growAt = size / 2;
        }

        /// <summary>
        /// Doubles the table and reinserts. The stored hash is reused rather than recomputed — it is the
        /// full FNV-1a value with the low bit forced, so the wider mask selects the new slot directly and
        /// no key is hashed twice.
        /// </summary>
        private void Grow()
        {
            var oldHashes = _hashes;
            var oldKeys = _keys;
            var oldValues = _values;

            if (oldHashes.Length >= MaximumSize)
            {
                // Unreachable in practice — the caller declines at a fifth of the row count long before,
                // and half a billion live keys would have exhausted memory first. Stated as a failure
                // rather than left implicit: a table that cannot grow eventually fills, and a full
                // open-addressed table makes GetOrAdd spin forever rather than return.
                throw new InvalidOperationException(
                    $"The dictionary hash table cannot grow past {MaximumSize} slots.");
            }

            Allocate(oldHashes.Length << 1);

            for (int i = 0; i < oldHashes.Length; i++)
            {
                int hash = oldHashes[i];
                if (hash == 0)
                    continue;

                int slot = hash & _mask;
                while (_hashes[slot] != 0)
                    slot = (slot + 1) & _mask;

                _hashes[slot] = hash;
                _keys[slot] = oldKeys[i];
                _values[slot] = oldValues[i];
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
