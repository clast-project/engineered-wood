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
    /// Attempts to build a dictionary for the given column: a <see cref="DictionaryBuilder"/> fed the whole
    /// column once, with its first-page test taken at the end when the values never reached it.
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
        var builder = DictionaryBuilder.Create(physicalType, typeLength, options);
        if (builder is null)
            return null;

        var feed = builder.Feed(array, defLevels, nonNullCount);
        return feed.Outcome switch
        {
            DictionaryFeed.Accepted => builder.ToResult(feed),
            DictionaryFeed.Full => builder.ToResult(feed),
            DictionaryFeed.Pending => builder.Decide() ? builder.ToResult(feed) : null,
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

    /// <summary>What a <see cref="DictionaryBuilder.Feed"/> did with the values it was given.</summary>
    internal enum DictionaryFeed
    {
        /// <summary>Every value has an entry, and the first-page test is still to come.</summary>
        Pending,

        /// <summary>Every value has an entry, and the dictionary has passed its first-page test.</summary>
        Accepted,

        /// <summary>The dictionary was declined, for the whole chunk: it filled before its first-page test, or failed it.</summary>
        Declined,

        /// <summary>
        /// The dictionary filled after passing its test. The values before <see cref="FeedResult.FullAt"/> have
        /// entries; the dictionary takes no more.
        /// </summary>
        Full,

        /// <summary>
        /// A run-end encoded column the run arm cannot read, after values have already been fed: the caller expands it
        /// and feeds the rows.
        /// </summary>
        UseRows,
    }

    /// <summary>The outcome of one <see cref="DictionaryBuilder.Feed"/>, and the entries of the values it took.</summary>
    internal readonly struct FeedResult
    {
        public DictionaryFeed Outcome { get; init; }

        /// <summary>
        /// The entry of each value taken, in order (<see cref="Taken"/> of them are set), or null when
        /// <see cref="Runs"/> carries them instead or nothing was taken.
        /// </summary>
        public int[]? Indices { get; init; }

        /// <summary>The entries in run form, for a run-end encoded column.</summary>
        public DictionaryIndexRuns? Runs { get; init; }

        /// <summary>Values given an entry.</summary>
        public int Taken { get; init; }

        /// <summary>For <see cref="DictionaryFeed.Full"/>: the level position of the first value without an entry.</summary>
        public int FullAt { get; init; }
    }

    /// <summary>A dictionary's entries, as the PLAIN bytes the dictionary page holds for each.</summary>
    internal interface IDictionaryEntries
    {
        int Count { get; }

        ReadOnlySpan<byte> Entry(int entry);
    }

    /// <summary>The entries of a dictionary already built: a page and its entry count.</summary>
    internal sealed class PageEntries : IDictionaryEntries
    {
        private readonly byte[] _page;
        private readonly int[] _starts;
        private readonly int[] _lengths;

        public PageEntries(byte[] page, int count, PhysicalType physicalType, int typeLength)
        {
            _page = page;
            _starts = new int[count];
            _lengths = new int[count];
            if (physicalType == PhysicalType.ByteArray)
            {
                // PLAIN BYTE_ARRAY: a 4-byte little-endian length before each entry.
                for (int i = 0, position = 0; i < count; i++)
                {
                    _lengths[i] = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(position));
                    _starts[i] = position + 4;
                    position += 4 + _lengths[i];
                }
            }
            else
            {
                int width = physicalType switch
                {
                    PhysicalType.Int32 or PhysicalType.Float => 4,
                    PhysicalType.Int64 or PhysicalType.Double => 8,
                    PhysicalType.Int96 => 12,
                    PhysicalType.Boolean => 1,
                    _ => typeLength,
                };
                for (int i = 0; i < count; i++)
                {
                    _starts[i] = i * width;
                    _lengths[i] = width;
                }
            }
        }

        public int Count => _starts.Length;

        public ReadOnlySpan<byte> Entry(int entry) => _page.AsSpan(_starts[entry], _lengths[entry]);
    }

    /// <summary>
    /// A column chunk's dictionary, built as its values arrive: one feed per append, the hash table kept between
    /// them. Entries are numbered in the order their values first appear.
    /// </summary>
    /// <remarks>
    /// <para><b>The first-page test</b> (as parquet-mr decides on its first page) is taken once
    /// <see cref="FirstPageBytes"/> of values written plain have been fed, or by <see cref="Decide"/> when the chunk
    /// ends short of that. If the dictionary page and those values' bit-packed indices are no smaller than the values
    /// written plain, the dictionary is declined for the whole chunk. Until then the caller holds the values, so that
    /// a declined dictionary can write them plain. Past the test only <see cref="ParquetWriteOptions.DictionaryPageSizeLimit"/>
    /// stops it, and then partway (<see cref="DictionaryFeed.Full"/>); a dictionary that fills before its test is
    /// declined.</para>
    /// <para><b>One feed of a whole chunk is today's single-pass encoder</b> (<see cref="TryEncode"/> is exactly that),
    /// so a row group written from one batch gets the dictionary it always did. Three of its shortcuts are kept as they
    /// were: a column with no nulls whose values are all one value takes one lookup (and no test); and a run-end
    /// encoded column is fed by its runs, and when it is the first to bring values, decided by
    /// <see cref="CardinalityThreshold"/> over them.</para>
    /// <para><b>Fixed-width values are keyed on their BIT PATTERN</b>: an integer type the width of one value slot,
    /// never the value's own type for FLOAT or DOUBLE, which come in as <see cref="uint"/> and <see cref="ulong"/>.
    /// A dictionary entry is a set of BYTES: two slots belong in the same entry exactly when their encoded bytes
    /// match. Keying on the VALUE is looser than that for floating point and merged -0.0 into 0.0 (issue #154). NaNs
    /// with different payloads are separate entries, by the same rule.</para>
    /// </remarks>
    internal sealed class DictionaryBuilder : IDictionaryEntries
    {
        private readonly PhysicalType _physicalType;
        private readonly int _typeLength;
        private readonly int _pageSizeLimit;

        /// <summary>A value's PLAIN width, or 0 for BYTE_ARRAY, whose values are a 4-byte length and their bytes.</summary>
        private readonly int _elementSize;

        private EntryTable? _table;

        /// <summary>Values given an entry so far.</summary>
        private long _values;

        /// <summary>The values fed before the test was taken, written plain: the test's yardstick.</summary>
        private long _plainBytes;

        /// <summary>Whether every value so far came through a constant probe, which takes no test.</summary>
        private bool _constantOnly;

        /// <summary>The entries once the dictionary is full: the one that overflowed is not among them.</summary>
        private int? _frozenCount;

        private DictionaryBuilder(PhysicalType physicalType, int typeLength, int elementSize, int pageSizeLimit)
        {
            _physicalType = physicalType;
            _typeLength = typeLength;
            _elementSize = elementSize;
            _pageSizeLimit = pageSizeLimit;
        }

        /// <summary>A builder for a column chunk, or null when the column gets no dictionary at all.</summary>
        public static DictionaryBuilder? Create(PhysicalType physicalType, int typeLength, ParquetWriteOptions options)
        {
            if (!options.DictionaryEnabled)
                return null;

            int elementSize = physicalType switch
            {
                PhysicalType.Int32 or PhysicalType.Float => 4,
                PhysicalType.Int64 or PhysicalType.Double => 8,
                PhysicalType.ByteArray => 0,
                PhysicalType.FixedLenByteArray => typeLength,
                _ => -1, // BOOLEAN, INT96
            };
            return elementSize < 0
                ? null
                : new DictionaryBuilder(physicalType, typeLength, elementSize, options.DictionaryPageSizeLimit);
        }

        /// <summary>Whether the dictionary has passed its first-page test.</summary>
        public bool Decided { get; private set; }

        /// <summary>Whether the dictionary was declined; it takes nothing more.</summary>
        public bool Declined { get; private set; }

        /// <summary>Whether the dictionary is full; it takes nothing more, and keeps the entries it has.</summary>
        public bool Frozen => _frozenCount is not null;

        /// <summary>The entries the dictionary page holds.</summary>
        public int Count => _frozenCount ?? _table?.Count ?? 0;

        /// <summary>The dictionary page's bytes so far, uncompressed.</summary>
        public long ByteCount => _table?.ByteCount ?? 0;

        public ReadOnlySpan<byte> Entry(int entry) => _table!.Entry(entry);

        /// <summary>The PLAIN dictionary page.</summary>
        public byte[] ToPage() => _table is null ? [] : _table.ToPage(Count);

        /// <summary>The dictionary as a <see cref="DictionaryResult"/>, with the entries of <paramref name="feed"/>.</summary>
        public DictionaryResult ToResult(in FeedResult feed) => new()
        {
            DictionaryPageData = ToPage(),
            DictionaryCount = Count,
            Indices = feed.Runs is null ? feed.Indices ?? [] : null,
            IndexRuns = feed.Runs,
            PartialLength = feed.Outcome == DictionaryFeed.Full ? feed.FullAt : null,
        };

        /// <summary>
        /// Takes the first-page test now, over what has been fed, for a chunk that ends (or must be decided) before
        /// its values reach <see cref="FirstPageBytes"/>. Returns whether the dictionary is kept.
        /// </summary>
        public bool Decide()
        {
            if (Declined)
                return false;
            if (!Decided)
            {
                // A constant probe never took the test; one entry fed only through probes is kept as it was.
                if ((_constantOnly && Count == 1) || Pays(ByteCount, _values, Count, _plainBytes))
                    Decided = true;
                else
                    Declined = true;
            }
            return Decided;
        }

        /// <summary>
        /// Gives the non-null values of <paramref name="array"/> their entries, adding the new ones.
        /// </summary>
        /// <param name="defLevels">0 for a level without a value, or null when every level has one.</param>
        public FeedResult Feed(IArrowArray array, int[]? defLevels, int nonNullCount)
        {
            if (Declined || Frozen)
                throw new InvalidOperationException("The dictionary takes no more values.");

            if (nonNullCount == 0)
                return new FeedResult { Outcome = Decided ? DictionaryFeed.Accepted : DictionaryFeed.Pending };

            // Every arm below reads raw spans off this array's buffers and then loops, allocating, for as long as
            // the column is. A span is not a GC reference to what it points at, so without the KeepAlive a
            // caller-built array is collectable mid-loop. See doc/arrow-span-lifetime.md.
            var result = array is RunEndEncodedArray ree
                ? FeedRuns(ree, nonNullCount)
                : _physicalType switch
                {
                    // The type argument is the BIT PATTERN of a value slot, not the value.
                    PhysicalType.Int32 => FeedFixed<int>(array, defLevels, nonNullCount),
                    PhysicalType.Int64 => FeedFixed<long>(array, defLevels, nonNullCount),
                    PhysicalType.Float => FeedFixed<uint>(array, defLevels, nonNullCount),
                    PhysicalType.Double => FeedFixed<ulong>(array, defLevels, nonNullCount),
                    PhysicalType.ByteArray => FeedByteArray(array, defLevels, nonNullCount),
                    _ => FeedFixedLenByteArray(array, defLevels, nonNullCount),
                };

            GC.KeepAlive(array);
            return result;
        }

        private FeedResult FeedFixed<TBits>(IArrowArray array, int[]? defLevels, int nonNullCount)
            where TBits : unmanaged, IEquatable<TBits>
        {
            var table = (FixedTable<TBits>)(_table ??= new FixedTable<TBits>());
            int rowCount = array.Length;
            int elementSize = _elementSize;
            var buffer = array.Data.Buffers[1].Span;

            // "All rows equal" is exactly "the value buffer is periodic with period elementSize": one vectorized
            // compare that stops at the first differing byte. It compares BYTES, the equality the table uses.
            if (defLevels is null && rowCount > 0 && IsConstantFixedWidth(buffer, rowCount, elementSize))
                return FeedConstant(buffer.Slice(0, elementSize), nonNullCount, elementSize);

            var valueBuffer = MemoryMarshal.Cast<byte, TBits>(buffer);
            var indices = new int[nonNullCount];
            int idx = 0;
            long values = _values, plainBytes = _plainBytes;
            bool decided = Decided;

            for (int i = 0; i < rowCount; i++)
            {
                if (defLevels != null && defLevels[i] == 0) continue;

                int entry = table.GetOrAdd(valueBuffer[i], out bool inserted);
                if (inserted && table.ByteCount > _pageSizeLimit)
                    return Overflow(indices, idx, i, values, plainBytes);

                indices[idx++] = entry;
                values++;
                if (!decided)
                {
                    plainBytes += elementSize;
                    if (plainBytes >= FirstPageBytes)
                    {
                        if (!Pays(table.ByteCount, values, table.Count, plainBytes))
                            return Decline();
                        decided = Decided = true;
                    }
                }
            }

            return Fed(indices, idx, values, plainBytes);
        }

        private FeedResult FeedByteArray(IArrowArray array, int[]? defLevels, int nonNullCount)
        {
            var table = (BytesTable)(_table ??= new BytesTable(lengthPrefixed: true));
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
                return FeedConstant(arrowData.Slice(constStart, constLen), nonNullCount, 4 + constLen);
            }

            var indices = new int[nonNullCount];
            int idx = 0;
            long values = _values, plainBytes = _plainBytes;
            bool decided = Decided;

            for (int i = 0; i < rowCount; i++)
            {
                if (defLevels != null && defLevels[i] == 0) continue;

                int start = arrowOffsets[i];
                ReadOnlySpan<byte> valueBytes = arrowData.Slice(start, arrowOffsets[i + 1] - start);

                int entry = table.GetOrAddBytes(valueBytes, out bool inserted);
                if (inserted && table.ByteCount > _pageSizeLimit)
                    return Overflow(indices, idx, i, values, plainBytes);

                indices[idx++] = entry;
                values++;
                if (!decided)
                {
                    // The first page: the values up to its size written plain.
                    plainBytes += 4 + valueBytes.Length;
                    if (plainBytes >= FirstPageBytes)
                    {
                        if (!Pays(table.ByteCount, values, table.Count, plainBytes))
                            return Decline();
                        decided = Decided = true;
                    }
                }
            }

            return Fed(indices, idx, values, plainBytes);
        }

        private FeedResult FeedFixedLenByteArray(IArrowArray array, int[]? defLevels, int nonNullCount)
        {
            int typeLength = _typeLength;
            // The page is the values concatenated (PLAIN for FLBA has no length prefix), which the table holds.
            var table = (BytesTable)(_table ??= new BytesTable(lengthPrefixed: false));
            int rowCount = array.Length;
            var valueBuffer = array.Data.Buffers[1].Span;

            var indices = new int[nonNullCount];
            int idx = 0;
            long values = _values, plainBytes = _plainBytes;
            bool decided = Decided;

            for (int i = 0; i < rowCount; i++)
            {
                if (defLevels != null && defLevels[i] == 0) continue;

                int entry = table.GetOrAddBytes(valueBuffer.Slice(i * typeLength, typeLength), out bool inserted);
                if (inserted && table.ByteCount > _pageSizeLimit)
                    return Overflow(indices, idx, i, values, plainBytes);

                indices[idx++] = entry;
                values++;
                if (!decided)
                {
                    plainBytes += typeLength;
                    if (plainBytes >= FirstPageBytes)
                    {
                        if (!Pays(table.ByteCount, values, table.Count, plainBytes))
                            return Decline();
                        decided = Decided = true;
                    }
                }
            }

            return Fed(indices, idx, values, plainBytes);
        }

        /// <summary>
        /// A column of one value in every row (and no nulls): one lookup, and one entry for every row. Such a feed
        /// takes no first-page test, as the single-pass encoder never did.
        /// </summary>
        private FeedResult FeedConstant(ReadOnlySpan<byte> value, int count, int plainSize)
        {
            bool first = _values == 0;
            int entry = _table!.GetOrAddBytes(value, out bool inserted);
            if (inserted && _table.ByteCount > _pageSizeLimit)
                return Overflow([], 0, 0, _values, _plainBytes);

            var indices = new int[count];
            if (entry != 0)
                indices.AsSpan().Fill(entry);

            _constantOnly = first || _constantOnly;
            _values += count;
            if (!Decided)
            {
                _plainBytes += (long)count * plainSize;
                if (_plainBytes >= FirstPageBytes && !Decide())
                    return Decline();
            }

            return new FeedResult
            {
                Outcome = Decided ? DictionaryFeed.Accepted : DictionaryFeed.Pending,
                Indices = indices,
                Taken = count,
            };
        }

        /// <summary>
        /// Feeds a run-end encoded column by its RUNS: one hash per run rather than per row, and entries in run form,
        /// so both the time and the allocation are O(runs). A constant column — one run — costs one lookup and a
        /// two-element index run whatever its row count.
        /// </summary>
        /// <remarks>
        /// <para>Nulls come from the run's value, which is the only place a run-end encoded array keeps them (its own
        /// <c>IsNull</c> answers false for every row). A null run contributes no entries at all, matching what the
        /// per-row arms do with a def level of 0.</para>
        /// <para>The first feed to bring values decides by runs, as the single-pass encoder did: the dictionary is
        /// declined when its distinct values exceed <see cref="CardinalityThreshold"/> of the values, or its page the
        /// size limit. A later feed follows the per-row rules, with each run counted as its rows.</para>
        /// <para>Values are compared as BYTES, like the per-row arms.</para>
        /// </remarks>
        private FeedResult FeedRuns(RunEndEncodedArray array, int nonNullCount)
        {
            bool first = _values == 0 && !Decided;
            var values = array.Values;

            // The run arm reads value slots straight out of the raw buffers, so it has to recognise the LAYOUT and not
            // merely the physical type. LargeString (64-bit offsets) and StringView reach BYTE_ARRAY too, and reading
            // either as 32-bit offsets is silent corruption rather than a failure. An unrecognised layout is fed by
            // its rows instead, or declines on a first feed, as the single-pass encoder did.
            bool lengthPrefixed = _physicalType == PhysicalType.ByteArray;
            int slotWidth = 0;
            if (lengthPrefixed
                ? values.Data.DataType is not (Apache.Arrow.Types.StringType or BinaryType)
                : !TryGetSlotWidth(values.Data.DataType, _physicalType, _typeLength, out slotWidth))
            {
                if (!first)
                    return new FeedResult { Outcome = DictionaryFeed.UseRows };
                return Decline();
            }

            int maxCardinality = Math.Max(1, (int)(nonNullCount * CardinalityThreshold));
            if (_table is null)
            {
                // The runs are an EXACT upper bound on the distinct values — one run cannot hold two of them. Sizing
                // the table to it means a column whose bound is tight never rehashes.
                int distinctBound = first ? Math.Min(maxCardinality, values.Length) + 1 : 0;
                _table = NewTable(distinctBound);
            }

            var table = _table;
            var runIndices = new List<int>();
            var runLengths = new List<int>();
            long counted = 0;
            int levelPosition = 0;
            long plainBytes = _plainBytes;

            foreach (var run in RunEndEncoding.EnumerateRuns(array))
            {
                int runStart = levelPosition;
                levelPosition += run.Length;
                if (values.IsNull(run.PhysicalIndex))
                    continue;

                var valueBytes = RunValueBytes(values, run.PhysicalIndex, lengthPrefixed, slotWidth);
                int entry = table.GetOrAddBytes(valueBytes, out bool inserted);
                if (inserted)
                {
                    if (first ? table.Count > maxCardinality || table.ByteCount > _pageSizeLimit
                              : table.ByteCount > _pageSizeLimit)
                    {
                        if (first || !Decided)
                            return Decline();
                        _frozenCount = table.Count - 1;
                        _values += counted;
                        return new FeedResult
                        {
                            Outcome = DictionaryFeed.Full,
                            Runs = Runs(runIndices, runLengths),
                            Taken = checked((int)counted),
                            FullAt = runStart,
                        };
                    }
                }

                counted += run.Length;

                // Two runs that map to the same entry are one run in the index stream — they are adjacent there
                // whenever the runs between them were null, and the RLE encoder cannot merge what it is handed as two.
                if (runIndices.Count > 0 && runIndices[runIndices.Count - 1] == entry)
                    runLengths[runLengths.Count - 1] += run.Length;
                else
                {
                    runIndices.Add(entry);
                    runLengths.Add(run.Length);
                }

                if (!first && !Decided)
                {
                    plainBytes += (long)run.Length * (lengthPrefixed ? 4 + valueBytes.Length : slotWidth);
                    if (plainBytes >= FirstPageBytes)
                    {
                        if (!Pays(table.ByteCount, _values + counted, table.Count, plainBytes))
                            return Decline();
                        Decided = true;
                    }
                }
            }

            // The caller derived the definition levels from these same runs, so a disagreement means the two views of
            // this column have diverged. A first feed declines, which writes the column on the caller's terms; later,
            // entries have been added for values the levels do not admit.
            if (counted != nonNullCount)
            {
                if (first)
                    return Decline();
                throw new InvalidOperationException(
                    "A run-end encoded column's runs disagree with its definition levels.");
            }

            if (first)
                Decided = true;
            _constantOnly = false;
            _values += counted;
            _plainBytes = plainBytes;
            return new FeedResult
            {
                Outcome = Decided ? DictionaryFeed.Accepted : DictionaryFeed.Pending,
                Runs = Runs(runIndices, runLengths),
                Taken = checked((int)counted),
            };
        }

        private static DictionaryIndexRuns Runs(List<int> values, List<int> lengths) => new()
        {
            Values = values.ToArray(),
            Lengths = lengths.ToArray(),
        };

        private EntryTable NewTable(int expectedEntries) => _physicalType switch
        {
            PhysicalType.Int32 => new FixedTable<int>(expectedEntries),
            PhysicalType.Int64 => new FixedTable<long>(expectedEntries),
            PhysicalType.Float => new FixedTable<uint>(expectedEntries),
            PhysicalType.Double => new FixedTable<ulong>(expectedEntries),
            PhysicalType.ByteArray => new BytesTable(lengthPrefixed: true, expectedEntries),
            _ => new BytesTable(lengthPrefixed: false, expectedEntries),
        };

        /// <summary>A value overflowed the page: declined before the test, full after it.</summary>
        private FeedResult Overflow(int[] indices, int taken, int levelPosition, long values, long plainBytes)
        {
            if (!Decided)
                return Decline();

            _frozenCount = _table!.Count - 1;
            _values = values;
            _plainBytes = plainBytes;
            _constantOnly = false;
            return new FeedResult { Outcome = DictionaryFeed.Full, Indices = indices, Taken = taken, FullAt = levelPosition };
        }

        private FeedResult Fed(int[] indices, int taken, long values, long plainBytes)
        {
            _values = values;
            _plainBytes = plainBytes;
            _constantOnly = false;
            return new FeedResult
            {
                Outcome = Decided ? DictionaryFeed.Accepted : DictionaryFeed.Pending,
                Indices = indices,
                Taken = taken,
            };
        }

        private FeedResult Decline()
        {
            Declined = true;
            _table = null;
            return new FeedResult { Outcome = DictionaryFeed.Declined };
        }
    }

    /// <summary>A builder's distinct values and their entry numbers.</summary>
    private abstract class EntryTable : IDictionaryEntries
    {
        public abstract int Count { get; }

        /// <summary>The bytes of the dictionary page of every entry.</summary>
        public abstract long ByteCount { get; }

        public abstract int GetOrAddBytes(ReadOnlySpan<byte> key, out bool inserted);

        public abstract ReadOnlySpan<byte> Entry(int entry);

        /// <summary>The PLAIN dictionary page of the first <paramref name="entries"/> entries.</summary>
        public abstract byte[] ToPage(int entries);
    }

    /// <summary>Fixed-width entries keyed on their bits (see <see cref="DictionaryBuilder"/>).</summary>
    private sealed class FixedTable<TBits>(int capacity = 0) : EntryTable
        where TBits : unmanaged, IEquatable<TBits>
    {
        private readonly Dictionary<TBits, int> _map = new(capacity);
        private TBits[] _entries = new TBits[16];
        private int _count;

        public override int Count => _count;

        public override long ByteCount => (long)_count * Size;

        private static readonly int Size = Marshal.SizeOf<TBits>();

        public int GetOrAdd(TBits value, out bool inserted)
        {
            if (_map.TryGetValue(value, out int entry))
            {
                inserted = false;
                return entry;
            }

            entry = _count;
            _map[value] = entry;
            if (_count == _entries.Length)
                System.Array.Resize(ref _entries, _count * 2);
            _entries[_count++] = value;
            inserted = true;
            return entry;
        }

        public override int GetOrAddBytes(ReadOnlySpan<byte> key, out bool inserted) =>
            GetOrAdd(MemoryMarshal.Read<TBits>(key), out inserted);

        public override ReadOnlySpan<byte> Entry(int entry) => MemoryMarshal.AsBytes(_entries.AsSpan(entry, 1));

        public override byte[] ToPage(int entries) => MemoryMarshal.AsBytes(_entries.AsSpan(0, entries)).ToArray();
    }

    /// <summary>Byte entries in a <see cref="BytesHashTable"/>, which holds them as the page itself.</summary>
    private sealed class BytesTable(bool lengthPrefixed, int expectedEntries = 0) : EntryTable
    {
        private readonly BytesHashTable _table = new(lengthPrefixed, expectedEntries);

        public override int Count => _table.Count;

        public override long ByteCount => _table.ByteCount;

        public override int GetOrAddBytes(ReadOnlySpan<byte> key, out bool inserted) => _table.GetOrAdd(key, out inserted);

        public override ReadOnlySpan<byte> Entry(int entry) => _table.Entry(entry);

        public override byte[] ToPage(int entries) => _table.ToPage(entries);
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
    private static bool Pays(long dictionaryBytes, long values, int entries, long plainBytes) =>
        dictionaryBytes + (values * GetIndexBitWidth(entries) + 7) / 8 < plainBytes;

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
    internal sealed class BytesHashTable
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

        /// <summary>Entry <paramref name="entry"/>'s bytes, without a length prefix.</summary>
        public ReadOnlySpan<byte> Entry(int entry) => _page.AsSpan(_starts[entry], EntryLength(entry));

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
            int hash = Hash(key, out ulong w0, out ulong w1);

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

        /// <summary>Slots in the bounded cache <see cref="AppendRecent"/> deduplicates against.</summary>
        private const int RecentSlots = 1024;

        // Per recent slot: the entry's number plus one (0 = empty), and its hash.
        private int[]? _recentEntries;
        private int[]? _recentHashes;

        /// <summary>
        /// The number of <paramref name="key"/>'s entry for a caller that has stopped deduplicating fully (the buffered
        /// writer, once a column's dictionary passes the page size limit, or has none): the entry of an equal key
        /// among the last that hashed to its slot in a small cache (<see cref="RecentSlots"/> slots), else a new entry
        /// appended without a slot in the table, which no longer grows. Repeats of a small working set of values then
        /// cost one entry each, as they did when every value was looked up, and the memory spent finding them is
        /// bounded.
        /// </summary>
        public int AppendRecent(ReadOnlySpan<byte> key, out bool added)
        {
            _recentEntries ??= new int[RecentSlots];
            _recentHashes ??= new int[RecentSlots];

            int hash = Hash(key, out _, out _);
            int slot = (int)((uint)hash * 0x9E3779B9u >> 22); // the top 10 bits: RecentSlots = 1 << 10
            int entry = _recentEntries[slot] - 1;
            if (entry >= 0 && _recentHashes[slot] == hash && EntryLength(entry) == key.Length
                && key.SequenceEqual(_page.AsSpan(_starts[entry], key.Length)))
            {
                added = false;
                return entry;
            }

            entry = Add(key);
            _recentEntries[slot] = entry + 1;
            _recentHashes[slot] = hash;
            added = true;
            return entry;
        }

        /// <summary>Entry <paramref name="entry"/>'s length in bytes, from where it and the next one start.</summary>
        private int EntryLength(int entry) =>
            (entry + 1 < _count ? _starts[entry + 1] - (_lengthPrefixed ? 4 : 0) : _pageLength) - _starts[entry];

        /// <summary>The hash a key is found by, and for a short key its words (see <see cref="Words"/>).</summary>
        private static int Hash(ReadOnlySpan<byte> key, out ulong w0, out ulong w1)
        {
            if (key.Length <= ShortKey)
            {
                Words(key, out w0, out w1);
                return Mix(w0, w1, key.Length);
            }

            w0 = w1 = 0;
            return (int)XxHash3.HashToUInt64(key);
        }

        /// <summary>The largest array .NET allocates (<c>Array.MaxLength</c>, which netstandard2.0 lacks).</summary>
        internal const int MaxArrayLength = 0x7FFFFFC7;

        /// <summary>Appends <paramref name="key"/> to the page as the next entry.</summary>
        private int Add(ReadOnlySpan<byte> key)
        {
            // In long: past 2 GiB the int sum wraps. The file writer stops at the dictionary page size limit; the
            // buffered writer's appends past it do not.
            long needed = (long)_pageLength + (_lengthPrefixed ? 4 : 0) + key.Length;
            if (needed > _page.Length)
            {
                if (needed > MaxArrayLength)
                    throw new InvalidOperationException(
                        "A column's values in one row group exceed the largest array .NET can allocate; flush row groups sooner.");
                System.Array.Resize(ref _page, (int)Math.Min(MaxArrayLength, Math.Max((long)_page.Length * 2, needed)));
            }

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
