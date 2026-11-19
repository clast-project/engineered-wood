// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.IO.Hashing;
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
    /// <summary>
    /// The share of distinct values above which the run-end encoded arm declines a dictionary. The per-row
    /// arms and the buffered writer decide on the first page instead (<see cref="FirstPagePays"/>).
    /// </summary>
    internal const float CardinalityThreshold = 0.20f;

    /// <summary>
    /// The sample the per-row arms decide on, as parquet-mr decides on its first page: the values up to this many
    /// bytes written plain (parquet-mr's default page size), or all of them. If the dictionary page and those
    /// values' bit-packed indices are no smaller than the values written plain, the dictionary is declined for the
    /// whole chunk. Past that only the dictionary page size limit stops it, and then partway
    /// (<see cref="DictionaryResult.PartialLength"/>).
    /// </summary>
    /// <remarks>
    /// Fixed rather than <see cref="ParquetWriteOptions.DataPageSize"/>, so that a small page setting does not
    /// shrink the sample to a few values that are all distinct. And in bytes rather than EW's 20,000-row pages:
    /// for 100,000 distinct INT32 values in random order, 20,000 values are nearly all distinct and the test
    /// fails, where over the chunk the dictionary is 2.5 MB against 4 MB plain; 1 MiB (262,144 values) sees that
    /// it pays.
    /// </remarks>
    internal const int FirstPageBytes = 1 << 20;

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

        /// <summary>
        /// Null when the dictionary covers the whole chunk. Otherwise the dictionary page filled up at this
        /// level position: the positions before it are dictionary-encoded and <see cref="Indices"/> holds their
        /// non-null values' indices (only those; the array may be longer), and the rest of the chunk is written
        /// in the column's own encoding, as parquet-cpp and parquet-mr write a column whose dictionary fills.
        /// The writer moves it back to a record boundary.
        /// </summary>
        public int? PartialLength { get; init; }
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
        {
            var runResult = TryEncodeRuns(ree, physicalType, typeLength, nonNullCount, options.DictionaryPageSizeLimit);
            GC.KeepAlive(array);
            return runResult;
        }

        // Every arm below reads raw spans off this array's buffers and then loops, allocating, for as long
        // as the column is. `array` is not referenced again once an arm has taken its spans, and a span is
        // not a GC reference to what it points at — so without the KeepAlive the array is collectable from
        // that moment, and for a caller-built array that means its buffers are freed by a finalizer while
        // the arms are still reading them. See doc/arrow-span-lifetime.md.
        var result = physicalType switch
        {
            // The type argument is the BIT PATTERN of a value slot, not the value — see TryEncodeFixed.
            // FLOAT and DOUBLE go in as uint/ulong so that the dictionary is keyed on bits.
            PhysicalType.Int32 => TryEncodeFixed<int>(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit, FirstPageBytes),
            PhysicalType.Int64 => TryEncodeFixed<long>(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit, FirstPageBytes),
            PhysicalType.Float => TryEncodeFixed<uint>(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit, FirstPageBytes),
            PhysicalType.Double => TryEncodeFixed<ulong>(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit, FirstPageBytes),
            PhysicalType.ByteArray => TryEncodeByteArray(array, defLevels, nonNullCount, options.DictionaryPageSizeLimit, FirstPageBytes),
            PhysicalType.FixedLenByteArray => TryEncodeFixedLenByteArray(array, defLevels, nonNullCount, typeLength, options.DictionaryPageSizeLimit, FirstPageBytes),
            _ => null,
        };

        GC.KeepAlive(array);
        return result;
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

    /// <summary>
    /// Dictionary-encodes a fixed-width column, keyed on the BIT PATTERN of each value slot.
    /// </summary>
    /// <typeparam name="TBits">
    /// An integer type the width of one value slot — never the value's own type for FLOAT or DOUBLE, which
    /// come in as <see cref="uint"/> and <see cref="ulong"/>. Nothing here interprets a slot, so reading the
    /// buffer as an integer of the same width changes only the equality used to fold slots together, and
    /// the dictionary page comes out byte-for-byte the same.
    /// </typeparam>
    /// <remarks>
    /// A dictionary entry is a set of BYTES: two slots belong in the same entry exactly when their encoded
    /// bytes match. Keying on the VALUE is LOOSER than that for floating point and merges slots that are not
    /// the same — <c>IEquatable&lt;double&gt;.Equals(-0.0, 0.0)</c> is <c>true</c>, so both zeros landed in
    /// one entry and every index for the second one pointed at the first one's bytes. Whichever zero came
    /// first won and the other was silently rewritten (issue #154).
    /// <para>
    /// Two consequences worth naming. NaNs with different payloads are now separate entries rather than one,
    /// which is the same rule applied consistently. And separating the zeros costs a dictionary entry, so a
    /// column sitting exactly on the cardinality threshold can now fall out of dictionary encoding — the
    /// values are right either way, since declining just writes them plain.
    /// </para>
    /// </remarks>
    private static DictionaryResult? TryEncodeFixed<TBits>(
        IArrowArray array, int[]? defLevels, int nonNullCount, int pageSizeLimit, int firstPageBytes)
        where TBits : unmanaged, IEquatable<TBits>
    {
        int rowCount = array.Length;
        int elementSize = Marshal.SizeOf<TBits>();
        // The values the first page holds written plain, or all of them.
        int firstPage = (int)Math.Min(nonNullCount, Math.Max(1, ((long)firstPageBytes + elementSize - 1) / elementSize));

        // The fixed-width twin of the constant probe in TryEncodeByteArray, and cheaper: every value slot is
        // the same width, so "all rows equal" is exactly "the value buffer is periodic with period
        // elementSize" — one vectorized compare that stops at the first differing byte.
        //
        // This probe compares BYTES, which is now the same equality the dictionary below uses; the two agree
        // rather than one being stricter than the other.
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

        var dict = new Dictionary<TBits, int>();
        var indices = new int[nonNullCount];
        var valueBuffer = MemoryMarshal.Cast<byte, TBits>(array.Data.Buffers[1].Span);
        int idx = 0;
        int? partialAt = null;

        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            TBits value = valueBuffer[i];
            if (!dict.TryGetValue(value, out int dictIdx))
            {
                dictIdx = dict.Count;
                dict[value] = dictIdx;

                if (dict.Count * elementSize > pageSizeLimit)
                {
                    // Full before the first page decided: decline. After: keep what is encoded.
                    if (idx < firstPage)
                        return null;
                    dict.Remove(value);
                    partialAt = i;
                    break;
                }
            }

            indices[idx++] = dictIdx;
            if (idx == firstPage && !Pays(dict.Count * (long)elementSize, idx, dict.Count, idx * (long)elementSize))
                return null;
        }

        // Produce PLAIN-encoded dictionary page: values in index order
        var entries = new TBits[dict.Count];
        foreach (var kvp in dict)
            entries[kvp.Value] = kvp.Key;

        var dictPage = new byte[dict.Count * elementSize];
        MemoryMarshal.AsBytes(entries.AsSpan()).CopyTo(dictPage);

        return new DictionaryResult
        {
            DictionaryPageData = dictPage,
            DictionaryCount = dict.Count,
            Indices = indices,
            PartialLength = partialAt,
        };
    }

    /// <summary>
    /// The first-page test the per-row arms apply as they build a dictionary, applied to one already built: by
    /// the buffered writer, which accumulates its dictionary as rows arrive and decides at flush. Entries are
    /// numbered in order of first appearance there too, so the entries the first page's values use are the first
    /// (highest index among them + 1).
    /// </summary>
    internal static bool FirstPagePays(
        DictionaryResult dictionary, int nonNullCount, PhysicalType physicalType, int typeLength, int firstPageBytes)
    {
        var indices = dictionary.Indices;
        if (indices is null || nonNullCount == 0)
            return true;

        ReadOnlySpan<byte> page = dictionary.DictionaryPageData;
        int width = physicalType switch
        {
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.Int96 => 12,
            PhysicalType.FixedLenByteArray => typeLength,
            _ => 0, // BYTE_ARRAY: each entry's length is in the page
        };

        int[]? lengths = null;
        if (width == 0)
        {
            lengths = new int[dictionary.DictionaryCount];
            for (int e = 0, pos = 0; e < lengths.Length; e++)
            {
                lengths[e] = BinaryPrimitives.ReadInt32LittleEndian(page.Slice(pos));
                pos += 4 + lengths[e];
            }
        }

        long plainBytes = 0;
        int values = 0, entries = 0;
        while (values < nonNullCount && plainBytes < firstPageBytes)
        {
            int index = indices[values++];
            plainBytes += width == 0 ? 4 + lengths![index] : width;
            entries = Math.Max(entries, index + 1);
        }

        long dictionaryBytes = 0;
        for (int e = 0; e < entries; e++)
            dictionaryBytes += width == 0 ? 4 + lengths![e] : width;

        return Pays(dictionaryBytes, values, entries, plainBytes);
    }

    /// <summary>
    /// Whether a dictionary pays on the first page: its page of <paramref name="dictionaryBytes"/> and the
    /// <paramref name="values"/> indices bit-packed at the width <paramref name="entries"/> needs, against those
    /// values written plain (<paramref name="plainBytes"/>). This is parquet-mr's test.
    /// </summary>
    private static bool Pays(long dictionaryBytes, int values, int entries, long plainBytes) =>
        dictionaryBytes + ((long)values * GetIndexBitWidth(entries) + 7) / 8 < plainBytes;

    private static DictionaryResult? TryEncodeByteArray(
        IArrowArray array, int[]? defLevels, int nonNullCount, int pageSizeLimit, int firstPageBytes)
    {
        int rowCount = array.Length;

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

        // The table keeps the distinct values as the dictionary page itself (PLAIN: a 4-byte LE length prefix
        // per entry), so the page is a copy of what it holds.
        var table = new BytesHashTable(lengthPrefixed: true);
        var indices = new int[nonNullCount];
        int idx = 0;
        long plainBytes = 0;
        bool decided = false;
        int? partialAt = null;

        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            int start = arrowOffsets[i];
            ReadOnlySpan<byte> valueBytes = arrowData.Slice(start, arrowOffsets[i + 1] - start);

            int dictIdx = table.GetOrAdd(valueBytes, out bool inserted);
            if (inserted && table.ByteCount > pageSizeLimit)
            {
                // Full before the first page decided: decline. After: keep what is encoded.
                if (!decided)
                    return null;
                partialAt = i;
                break;
            }

            indices[idx++] = dictIdx;
            if (!decided)
            {
                // The first page: the values up to its size written plain, or all of them.
                plainBytes += 4 + valueBytes.Length;
                if (plainBytes >= firstPageBytes || idx == nonNullCount)
                {
                    if (!Pays(table.ByteCount, idx, table.Count, plainBytes))
                        return null;
                    decided = true;
                }
            }
        }

        // The entry that overflowed is not part of a partial dictionary.
        int entries = partialAt is null ? table.Count : table.Count - 1;
        return new DictionaryResult
        {
            DictionaryPageData = table.ToPage(entries),
            DictionaryCount = entries,
            Indices = indices,
            PartialLength = partialAt,
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
        var table = new BytesHashTable(lengthPrefixed, distinctBound);
        var runIndices = new List<int>();
        var runLengths = new List<int>();
        long countedNonNull = 0;

        foreach (var run in RunEndEncoding.EnumerateRuns(array))
        {
            if (values.IsNull(run.PhysicalIndex))
                continue;

            countedNonNull += run.Length;

            var valueBytes = RunValueBytes(values, run.PhysicalIndex, lengthPrefixed, slotWidth);
            int dictIdx = table.GetOrAdd(valueBytes, out bool inserted);
            if (inserted && (table.Count > maxCardinality || table.ByteCount > pageSizeLimit))
                return null;

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

        return new DictionaryResult
        {
            DictionaryPageData = table.ToPage(),
            DictionaryCount = table.Count,
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
        IArrowArray array, int[]? defLevels, int nonNullCount, int typeLength, int pageSizeLimit, int firstPageBytes)
    {
        int rowCount = array.Length;
        int firstPage = (int)Math.Min(nonNullCount, Math.Max(1, ((long)firstPageBytes + typeLength - 1) / typeLength));

        var valueBuffer = array.Data.Buffers[1].Span;

        // The page is the values concatenated (PLAIN for FLBA has no length prefix), which the table holds.
        var table = new BytesHashTable(lengthPrefixed: false);
        var indices = new int[nonNullCount];
        int idx = 0;
        int? partialAt = null;

        for (int i = 0; i < rowCount; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;

            ReadOnlySpan<byte> valueBytes = valueBuffer.Slice(i * typeLength, typeLength);
            int dictIdx = table.GetOrAdd(valueBytes, out bool inserted);
            if (inserted && table.ByteCount > pageSizeLimit)
            {
                // Full before the first page decided: decline. After: keep what is encoded.
                if (idx < firstPage)
                    return null;
                partialAt = i;
                break;
            }

            indices[idx++] = dictIdx;
            if (idx == firstPage && !Pays(table.ByteCount, idx, table.Count, idx * (long)typeLength))
                return null;
        }

        int entries = partialAt is null ? table.Count : table.Count - 1;
        return new DictionaryResult
        {
            DictionaryPageData = table.ToPage(entries),
            DictionaryCount = entries,
            Indices = indices,
            PartialLength = partialAt,
        };
    }

    /// <summary>
    /// The distinct values of a column, numbered in the order they first appear, found with an open-addressing
    /// hash table (linear probing) and kept as the PLAIN dictionary page they will be written as.
    /// </summary>
    /// <remarks>
    /// <para><b>The values live in the page.</b> Each new value is appended to one growing buffer in the page's
    /// layout: a 4-byte little-endian length and then the bytes for BYTE_ARRAY, the bytes alone for
    /// FIXED_LEN_BYTE_ARRAY. A slot is 32 bytes holding everything a lookup compares (hash, length, entry number,
    /// and a short key's words), so a probe reads one cache line; only a long key is compared against its bytes
    /// in the page. The page is a copy of the buffer: no array per entry, and no second pass
    /// to lay the page out.</para>
    /// <para><b>Short keys are two words.</b> A key of at most 16 bytes (codes, categories, most dictionary
    /// columns) is read into two words, hashed with a multiply-and-mix of them, and compared as them, kept beside
    /// the entry: no call per value and no read of the page. Longer keys are hashed with <b>XxHash3</b> and
    /// compared byte by byte. Either replaces FNV-1a, a multiply per byte. The slot comes from the hash; whether a
    /// slot is taken comes from its entry number (0 = empty).
    /// The table this replaced forced the hash's low bit to mark a slot taken and chose the slot from the same
    /// bits, so it only ever used the odd slots.</para>
    /// <para><b>Grows geometrically</b> from 16 slots, with a load factor ceiling of one half: a 1M-row column
    /// with twelve distinct values allocates no more than one with twelve rows, and linear probing stays short.
    /// The writer runs columns in parallel, so sizing to a worst case would be paid per column at once.</para>
    /// <para>MEASURED per million values (<c>dict-profile</c>), building the dictionary of a 1,000-value string
    /// column: 19.6 ms with FNV-1a and an array per entry, 9.3 now. What is left is each lookup's dependent chain
    /// (mix, load the slot, compare, branch), about 8 ns; the hash's spread is as random (796 of 2,048 slots for
    /// 1,000 keys).</para>
    /// <para>The length comparison before the words is what makes a 32-bit hash collision harmless: keys that
    /// differ only in trailing zero bytes share words, and a long key's byte comparison would otherwise read past
    /// a shorter entry.</para>
    /// </remarks>
    private sealed class BytesHashTable
    {
        /// <summary>Smallest table. A column with a handful of distinct values never outgrows it.</summary>
        private const int MinimumSize = 16;

        /// <summary>Largest table: past this the slot count would overflow when doubled.</summary>
        private const int MaximumSize = 1 << 30;

        private readonly bool _lengthPrefixed;

        /// <summary>
        /// One slot: everything a lookup compares, in 32 bytes, so a probe reads one cache line rather than
        /// following the entry number into arrays of its own.
        /// </summary>
        private struct Slot
        {
            public ulong Word0, Word1; // a key of at most 16 bytes (see Words); unused for a longer one
            public int Hash;
            public int Entry;          // the entry's number plus one; 0 = empty
            public int Length;
        }

        private Slot[] _table = [];
        private int _mask;
        private int _growAt;

        // Per entry, in order: where its bytes start in the page.
        private int[] _starts = new int[MinimumSize];
        private int _count;

        private byte[] _page = new byte[256];
        private int _pageLength;

        /// <param name="lengthPrefixed">Whether the page prefixes each entry with its length (BYTE_ARRAY).</param>
        /// <param name="expectedEntries">
        /// An upper bound on the distinct values, for a caller that has one, as the run-end encoded arm does: a
        /// run cannot hold two distinct values. Sizing to it means an exact bound never rehashes. Zero starts at
        /// the minimum and grows.
        /// </param>
        public BytesHashTable(bool lengthPrefixed, int expectedEntries = 0)
        {
            _lengthPrefixed = lengthPrefixed;
            int size = MinimumSize;
            // Two slots per entry, matching the load-factor ceiling. Compared as long: a bound near
            // int.MaxValue would otherwise double past the sign bit and spin here.
            while (size < (long)expectedEntries * 2 && size < MaximumSize)
                size <<= 1;

            Allocate(size);
        }

        /// <summary>The distinct values so far.</summary>
        public int Count => _count;

        /// <summary>The bytes of the dictionary page so far.</summary>
        public int ByteCount => _pageLength;

        /// <summary>The PLAIN dictionary page: every entry, in order.</summary>
        public byte[] ToPage() => _page.AsSpan(0, _pageLength).ToArray();

        /// <summary>The PLAIN dictionary page of the first <paramref name="entries"/> entries.</summary>
        public byte[] ToPage(int entries) => entries == _count
            ? ToPage()
            : _page.AsSpan(0, _starts[entries] - (_lengthPrefixed ? 4 : 0)).ToArray();

        /// <summary>
        /// The number of <paramref name="key"/>'s entry, adding it as the next one when it is new
        /// (<paramref name="inserted"/>).
        /// </summary>
        public int GetOrAdd(ReadOnlySpan<byte> key, out bool inserted)
        {
            // A key of at most 16 bytes is two words, hashed and compared as such; a longer one is hashed with
            // XxHash3 and compared byte by byte.
            bool shortKey = key.Length <= ShortKey;
            ulong w0 = 0, w1 = 0;
            int hash;
            if (shortKey)
            {
                Words(key, out w0, out w1);
                hash = Mix(w0, w1, key.Length);
            }
            else
            {
                hash = (int)XxHash3.HashToUInt64(key);
            }

            int slot = hash & _mask;
            while (true)
            {
                ref Slot s = ref _table[slot];
                if (s.Entry == 0)
                {
                    int entry = Add(key);
                    s.Word0 = w0;
                    s.Word1 = w1;
                    s.Hash = hash;
                    s.Length = key.Length;
                    s.Entry = entry + 1;
                    inserted = true;

                    // Grown AFTER the insert, so the slot this landed in does not have to survive the rehash.
                    if (_count >= _growAt)
                        Grow();

                    return entry;
                }

                if (s.Hash == hash && s.Length == key.Length
                    && (shortKey
                        ? s.Word0 == w0 && s.Word1 == w1
                        : key.SequenceEqual(_page.AsSpan(_starts[s.Entry - 1], key.Length))))
                {
                    inserted = false;
                    return s.Entry - 1;
                }

                slot = (slot + 1) & _mask;
            }
        }

        /// <summary>The longest key compared as words.</summary>
        private const int ShortKey = 16;

        /// <summary>
        /// A key of at most 16 bytes as two words. For a given length the mapping is one to one: every byte
        /// lands in some word at a position fixed by the length (reads may overlap, never miss), and the callers
        /// compare lengths too. Only bytes of the key are read.
        /// </summary>
        private static void Words(ReadOnlySpan<byte> key, out ulong w0, out ulong w1)
        {
            int n = key.Length;
            if (n >= 8)
            {
                w0 = BinaryPrimitives.ReadUInt64LittleEndian(key);
                // Bytes 8 .. n-1: the last eight bytes, shifted so the ones before byte 8 fall off.
                w1 = n == 8 ? 0 : BinaryPrimitives.ReadUInt64LittleEndian(key.Slice(n - 8)) >> (8 * (16 - n));
            }
            else if (n >= 4)
            {
                w0 = BinaryPrimitives.ReadUInt32LittleEndian(key)
                    | ((ulong)BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(n - 4)) << 32);
                w1 = 0;
            }
            else
            {
                w0 = 0;
                for (int i = 0; i < n; i++)
                    w0 |= (ulong)key[i] << (8 * i);
                w1 = 0;
            }
        }

        /// <summary>A 32-bit hash of a short key's words and length (multiply, rotate, and a final avalanche).</summary>
        private static int Mix(ulong w0, ulong w1, int length)
        {
            ulong h = (w0 * 0x9E3779B97F4A7C15UL) ^ RotateLeft(w1 * 0xC2B2AE3D27D4EB4FUL, 31) ^ (ulong)length;
            h ^= h >> 33;
            h *= 0xFF51AFD7ED558CCDUL;
            h ^= h >> 33;
            return (int)h;
        }

        private static ulong RotateLeft(ulong value, int offset) => (value << offset) | (value >> (64 - offset));

        /// <summary>Appends <paramref name="key"/> to the page as the next entry.</summary>
        private int Add(ReadOnlySpan<byte> key)
        {
            int needed = _pageLength + (_lengthPrefixed ? 4 : 0) + key.Length;
            if (needed > _page.Length)
                System.Array.Resize(ref _page, (int)Math.Min(Math.Max((long)_page.Length * 2, needed), int.MaxValue));

            if (_lengthPrefixed)
            {
                BinaryPrimitives.WriteInt32LittleEndian(_page.AsSpan(_pageLength), key.Length);
                _pageLength += 4;
            }

            if (_count == _starts.Length)
                System.Array.Resize(ref _starts, _count * 2);

            _starts[_count] = _pageLength;
            key.CopyTo(_page.AsSpan(_pageLength));
            _pageLength += key.Length;
            return _count++;
        }

        private void Allocate(int size)
        {
            _table = new Slot[size];
            _mask = size - 1;
            _growAt = size / 2;
        }

        /// <summary>
        /// Doubles the table and reinserts each entry at the slot its stored hash picks under the wider mask;
        /// no key is hashed twice.
        /// </summary>
        private void Grow()
        {
            var old = _table;

            if (old.Length >= MaximumSize)
            {
                // Unreachable in practice: the callers decline at a fifth of the row count long before. Stated
                // as a failure rather than left implicit, because a full open-addressed table makes GetOrAdd
                // spin forever.
                throw new InvalidOperationException(
                    $"The dictionary hash table cannot grow past {MaximumSize} slots.");
            }

            Allocate(old.Length << 1);

            for (int i = 0; i < old.Length; i++)
            {
                if (old[i].Entry == 0)
                    continue;

                int slot = old[i].Hash & _mask;
                while (_table[slot].Entry != 0)
                    slot = (slot + 1) & _mask;

                _table[slot] = old[i];
            }
        }
    }
}
