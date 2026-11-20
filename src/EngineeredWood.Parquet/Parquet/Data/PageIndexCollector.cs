// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// A column chunk's page index as the encoder produces it: page locations relative to the start of
/// the chunk, which <see cref="ParquetFileAssembler"/> turns into file offsets once the chunk is
/// placed.
/// </summary>
/// <param name="PageLocations">One entry per data page; <see cref="PageLocation.Offset"/> is from the chunk start.</param>
/// <param name="ColumnIndex">The page bounds, or null when this chunk gets no ColumnIndex.</param>
/// <param name="UnencodedByteArrayDataBytes">
/// Each data page's BYTE_ARRAY value bytes, length prefixes excluded: the OffsetIndex's
/// <c>unencoded_byte_array_data_bytes</c>. Null for a chunk of another physical type, or when
/// <see cref="ParquetWriteOptions.WriteSizeStatistics"/> is off.
/// </param>
internal sealed record ChunkPageIndex(
    IReadOnlyList<PageLocation> PageLocations, ColumnIndex? ColumnIndex, IReadOnlyList<long>? UnencodedByteArrayDataBytes);

/// <summary>
/// Collects a column chunk's page index, one data page at a time, as <see cref="ColumnChunkWriter"/>
/// emits the pages.
/// </summary>
internal sealed class PageIndexCollector
{
    private readonly PhysicalType _physicalType;
    private readonly int _typeLength;
    private readonly StatisticsOrder _order;
    private readonly int? _truncateLength;
    private readonly bool _utf8;
    private readonly bool _floatingPoint;

    private readonly List<PageLocation> _locations = new();
    private readonly List<bool> _nullPages = new();
    private readonly List<byte[]> _mins = new();
    private readonly List<byte[]> _maxes = new();
    private readonly List<long> _nullCounts = new();
    private readonly List<long> _nanCounts = new();
    private List<long>? _unencodedBytes;
    private long _firstRowIndex;

    // The chunk's bounds, folded from the untruncated page bounds as the pages arrive, so the chunk
    // statistics need not scan the values a second time.
    private readonly BoundsFold _fold;

    /// <param name="writeColumnIndex">
    /// Whether the chunk may get a ColumnIndex at all; false for a column without statistics or of a
    /// type whose bounds are not ordered correctly. The OffsetIndex is written either way.
    /// </param>
    /// <param name="utf8">Whether BYTE_ARRAY values are UTF-8, and so must be truncated on code points.</param>
    /// <param name="recordsUnencodedBytes">
    /// Whether each page passes its BYTE_ARRAY value bytes to <see cref="AddPage"/>: a BYTE_ARRAY chunk's
    /// OffsetIndex carries them under <see cref="ParquetWriteOptions.WriteSizeStatistics"/>.
    /// </param>
    public PageIndexCollector(
        PhysicalType physicalType,
        int typeLength,
        StatisticsOrder order,
        bool floatingPointTotalOrder,
        int? truncateLength,
        bool utf8,
        bool writeColumnIndex,
        bool recordsUnencodedBytes)
    {
        if (recordsUnencodedBytes && physicalType != PhysicalType.ByteArray)
            throw new ArgumentException($"A {physicalType} chunk has no BYTE_ARRAY value bytes.", nameof(recordsUnencodedBytes));
        RecordsUnencodedBytes = recordsUnencodedBytes;
        _physicalType = physicalType;
        _typeLength = typeLength;
        _order = order;
        FloatingPointTotalOrder = floatingPointTotalOrder;
        _truncateLength = physicalType == PhysicalType.ByteArray ? truncateLength : null;
        _utf8 = utf8;
        _floatingPoint = StatisticsCollector.IsFloatingPoint(physicalType, order);
        WritesColumnIndex = writeColumnIndex;
        FoldsBounds = writeColumnIndex;
        _fold = new BoundsFold(physicalType, order, floatingPointTotalOrder);
    }

    /// <summary>Whether the chunk still gets a ColumnIndex: false once it is known to get none.</summary>
    public bool WritesColumnIndex { get; private set; }

    /// <summary>
    /// Whether page bounds are needed: for the ColumnIndex, and for the chunk statistics folded from them. A chunk
    /// that drops its ColumnIndex for an all-NaN page keeps folding, so that its statistics still cover every page
    /// (a chunk of several appends has no other pass over the earlier ones).
    /// </summary>
    public bool FoldsBounds { get; }

    public bool FloatingPointTotalOrder { get; }

    /// <summary>Whether each page's BYTE_ARRAY value bytes are recorded for the OffsetIndex.</summary>
    public bool RecordsUnencodedBytes { get; }

    public PhysicalType PhysicalType => _physicalType;

    public int TypeLength => _typeLength;

    public StatisticsOrder Order => _order;

    /// <summary>
    /// Records a data page just written.
    /// </summary>
    /// <param name="page">Where <see cref="ColumnChunkOutput.EmitPage(PageHeader, ReadOnlySpan{byte})"/> put it.</param>
    /// <param name="rows">Rows that start in the page. Every page starts a row, so this sums to the chunk's rows.</param>
    /// <param name="nullCount">Level entries in the page that hold no value.</param>
    /// <param name="bounds">The page's bounds; ignored unless <see cref="FoldsBounds"/>.</param>
    /// <param name="unencodedBytes">
    /// The page's BYTE_ARRAY value bytes, length prefixes excluded; null unless
    /// <see cref="RecordsUnencodedBytes"/>.
    /// </param>
    public void AddPage(
        EmittedPage page, int rows, int nullCount, int nonNullCount, in PageBounds bounds, long? unencodedBytes)
    {
        if (page.Ordinal != _locations.Count)
            throw new InvalidOperationException($"Data page {page.Ordinal} recorded as page {_locations.Count}.");
        if (unencodedBytes.HasValue != RecordsUnencodedBytes)
            throw new InvalidOperationException($"A page's unencoded byte count was {(unencodedBytes.HasValue ? "given" : "omitted")}.");

        _locations.Add(new PageLocation(page.Offset, page.Size, _firstRowIndex));
        _firstRowIndex += rows;
        if (unencodedBytes is { } bytes)
            (_unencodedBytes ??= new()).Add(bytes);

        if (!FoldsBounds)
            return;

        bool nullPage = nonNullCount == 0;
        if (!nullPage)
        {
            // Values but no bounds: every value is NaN under TYPE_ORDER. The format then requires the
            // chunk to have no ColumnIndex at all, since no bound would be a valid one. Its NaNs still count.
            if (bounds.Min is null)
                WritesColumnIndex = false;
            _fold.Add(bounds);
        }

        if (!WritesColumnIndex)
            return;

        _nullPages.Add(nullPage);
        _mins.Add(nullPage ? [] : Truncate(LowerBound(bounds.Min!), max: false));
        _maxes.Add(nullPage ? [] : Truncate(UpperBound(bounds.Max!), max: true));
        _nullCounts.Add(nullCount);
        if (_floatingPoint)
            _nanCounts.Add(nullPage ? 0 : bounds.NanCount);
    }

    /// <summary>
    /// Whether <see cref="ChunkStatistics"/> will answer for a chunk of <paramref name="nonNullCount"/> values:
    /// the pages' bounds are folded and there is a value.
    /// </summary>
    public bool HasChunkStatistics(int nonNullCount) => FoldsBounds && nonNullCount > 0;

    /// <summary>
    /// The chunk statistics, from the page bounds, or null when they do not cover the chunk and the
    /// caller must scan: no bounds were kept, or there are no values at all.
    /// </summary>
    public Statistics? ChunkStatistics(int nonNullCount, int levelCount)
    {
        if (!HasChunkStatistics(nonNullCount))
            return null;

        return _fold.ToStatistics(levelCount - nonNullCount);
    }

    /// <summary>The chunk's page index, with page offsets still relative to the chunk.</summary>
    /// <param name="pageOffsetBase">
    /// Where the pages' stream begins in the chunk: the length of the pages written ahead of the data pages
    /// (dictionary or symbol table) when those were not written to the same stream, 0 when they were.
    /// </param>
    public ChunkPageIndex Build(int pageOffsetBase = 0)
    {
        var locations = _locations.ToArray();
        if (pageOffsetBase != 0)
        {
            for (int i = 0; i < locations.Length; i++)
                locations[i] = locations[i] with { Offset = locations[i].Offset + pageOffsetBase };
        }

        ColumnIndex? columnIndex = null;
        if (WritesColumnIndex)
        {
            columnIndex = new ColumnIndex
            {
                NullPages = _nullPages.ToArray(),
                MinValues = _mins.ToArray(),
                MaxValues = _maxes.ToArray(),
                BoundaryOrder = ComputeBoundaryOrder(),
                NullCounts = _nullCounts.ToArray(),
                NanCounts = _floatingPoint ? _nanCounts.ToArray() : null,
            };
        }

        return new ChunkPageIndex(
            locations, columnIndex, RecordsUnencodedBytes ? _unencodedBytes?.ToArray() ?? [] : null);
    }

    private byte[] Truncate(byte[] value, bool max)
    {
        if (_truncateLength is not { } limit)
            return value;

        return max
            ? PageIndexTruncation.TruncateMax(value, limit, _utf8)
            : PageIndexTruncation.TruncateMin(value, limit, _utf8);
    }

    /// <summary>
    /// ASCENDING when both the minimums and the maximums never decrease from one non-null page to the
    /// next, DESCENDING when they never increase, UNORDERED otherwise. Decided on the bounds as
    /// written, since those are what a reader searches.
    /// </summary>
    private BoundaryOrder ComputeBoundaryOrder()
    {
        bool ascending = true, descending = true;
        int previous = -1;
        for (int page = 0; page < _nullPages.Count; page++)
        {
            if (_nullPages[page])
                continue;

            // A NaN bound (IEEE 754 total order, all-NaN page) has no place in the numeric order the
            // comparator implements, so it promises nothing.
            if (_floatingPoint && (IsNaN(_mins[page]) || IsNaN(_maxes[page])))
                return BoundaryOrder.Unordered;

            if (previous >= 0)
            {
                int byMin = Compare(_mins[previous], _mins[page]);
                int byMax = Compare(_maxes[previous], _maxes[page]);
                if (byMin > 0 || byMax > 0) ascending = false;
                if (byMin < 0 || byMax < 0) descending = false;
                if (!ascending && !descending)
                    return BoundaryOrder.Unordered;
            }

            previous = page;
        }

        return ascending ? BoundaryOrder.Ascending : BoundaryOrder.Descending;
    }

    private int Compare(byte[] left, byte[] right) =>
        StatisticsCollector.CompareValues(left, right, _physicalType, _order);

    // TYPE_ORDER's signed-zero rule (see StatisticsCollector.TypeOrderLowerBound). The chunk fold above
    // keeps the exact bounds; the chunk statistics get the same rule where they are written.
    private byte[] LowerBound(byte[] min) =>
        _floatingPoint && !FloatingPointTotalOrder ? StatisticsCollector.TypeOrderLowerBound(min, _physicalType) : min;

    private byte[] UpperBound(byte[] max) =>
        _floatingPoint && !FloatingPointTotalOrder ? StatisticsCollector.TypeOrderUpperBound(max, _physicalType) : max;

    private bool IsNaN(byte[] value) => StatisticsCollector.IsNaN(value, _physicalType);
}

/// <summary>
/// The bounds of a set of values, folded from the bounds of its parts (pages for a page index, appends for a
/// chunk's scanned statistics) into the statistics one scan of the whole would give.
/// </summary>
/// <remarks>
/// Strict comparisons keep the first of equal values, as the full scan does. A part with values but no bounds (all
/// NaN under TYPE_ORDER) adds only its NaN count.
/// </remarks>
internal sealed class BoundsFold(PhysicalType physicalType, StatisticsOrder order, bool floatingPointTotalOrder)
{
    private readonly bool _floatingPoint = StatisticsCollector.IsFloatingPoint(physicalType, order);
    private byte[]? _min, _max, _minNaN, _maxNaN;
    private long _nanCount;

    public void Add(in PageBounds bounds)
    {
        _nanCount += bounds.NanCount;
        if (bounds.Min is null)
            return;

        // A NaN bound is an all-NaN part under total order, its smallest and largest NaN; they take part
        // only if nothing else does, and then as the smallest and largest NaN.
        if (_floatingPoint && StatisticsCollector.IsNaN(bounds.Min, physicalType))
        {
            if (_minNaN is null || TotalOrderKey(bounds.Min) < TotalOrderKey(_minNaN))
                _minNaN = bounds.Min;
            if (_maxNaN is null || TotalOrderKey(bounds.Max!) > TotalOrderKey(_maxNaN))
                _maxNaN = bounds.Max;
            return;
        }

        if (_min is null || Compare(bounds.Min, _min) < 0)
            _min = bounds.Min;
        if (_max is null || Compare(bounds.Max!, _max) > 0)
            _max = bounds.Max;
    }

    /// <summary>The statistics of a set with at least one value.</summary>
    public Statistics ToStatistics(long nullCount) =>
        StatisticsCollector.FromBounds(
            physicalType, _min, _max, _minNaN, _maxNaN, _nanCount, nullCount, floatingPointTotalOrder, order);

    private int Compare(byte[] left, byte[] right) =>
        StatisticsCollector.CompareValues(left, right, physicalType, order);

    private long TotalOrderKey(byte[] value) => StatisticsCollector.TotalOrderKey(value, physicalType);
}

/// <summary>
/// A dictionary-encoded page's bounds, found from the dictionary entries its indices reference rather
/// than from the values, which the dictionary path no longer has.
/// </summary>
/// <remarks>
/// Each distinct entry a page references is compared once, with
/// <see cref="StatisticsCollector.CompareValues"/>, the comparator the chunk statistics use. NaN
/// entries are counted, not compared by value; an all-NaN page under IEEE 754 total order takes its
/// smallest and largest NaN as its bounds, as the full scan does.
/// </remarks>
internal sealed class DictionaryPageBounds
{
    private readonly DictionaryEncoder.IDictionaryEntries _entries;
    private readonly PhysicalType _physicalType;
    private readonly StatisticsOrder _order;
    private readonly bool _totalOrder;
    private readonly bool _floatingPoint;

    // Per entry, as far as the dictionary has been looked at: it can grow between pages.
    private bool[]? _nanEntries;
    private int _known;

    // _seen[entry] == _page marks an entry already compared for the current page, so a page costs
    // one comparison per distinct entry, not per value, and nothing has to be cleared between pages.
    private int[] _seen = [];
    private int _page;

    private int _min, _max, _minNaN, _maxNaN;
    private long _nanCount;

    public DictionaryPageBounds(in DictionaryEncoder.DictionaryResult dictionary, PageIndexCollector collector)
        : this(
            new DictionaryEncoder.PageEntries(
                dictionary.DictionaryPageData, dictionary.DictionaryCount, collector.PhysicalType, collector.TypeLength),
            collector.PhysicalType, collector.Order, collector.FloatingPointTotalOrder)
    {
    }

    /// <param name="entries">The dictionary, which may gain entries between pages.</param>
    public DictionaryPageBounds(
        DictionaryEncoder.IDictionaryEntries entries, PhysicalType physicalType, StatisticsOrder order, bool totalOrder)
    {
        _entries = entries;
        _physicalType = physicalType;
        _order = order;
        _totalOrder = totalOrder;
        _floatingPoint = StatisticsCollector.IsFloatingPoint(physicalType, order);
    }

    /// <summary>Starts a new page.</summary>
    public void Begin()
    {
        // The entries added since the last page, which this page's values may reference.
        int count = _entries.Count;
        if (count > _known)
        {
            if (_seen.Length < count)
                System.Array.Resize(ref _seen, Math.Max(count, _seen.Length * 2));
            if (_floatingPoint)
            {
                for (int i = _known; i < count; i++)
                {
                    if (!StatisticsCollector.IsNaN(Entry(i), _physicalType)) continue;
                    if (_nanEntries is null || _nanEntries.Length < count)
                        System.Array.Resize(ref _nanEntries, Math.Max(count, (_nanEntries?.Length ?? 0) * 2));
                    _nanEntries[i] = true;
                }
            }
            _known = count;
        }

        _page++;
        _min = _max = _minNaN = _maxNaN = -1;
        _nanCount = 0;
    }

    /// <summary>Adds <paramref name="count"/> consecutive values that all reference <paramref name="entry"/>.</summary>
    public void Add(int entry, int count)
    {
        if (_nanEntries is not null && entry < _nanEntries.Length && _nanEntries[entry])
        {
            if (_totalOrder)
            {
                long key = StatisticsCollector.TotalOrderKey(Entry(entry), _physicalType);
                if (_minNaN < 0 || key < StatisticsCollector.TotalOrderKey(Entry(_minNaN), _physicalType))
                    _minNaN = entry;
                if (_maxNaN < 0 || key > StatisticsCollector.TotalOrderKey(Entry(_maxNaN), _physicalType))
                    _maxNaN = entry;
            }
            _nanCount += count;
            return;
        }

        if (_seen[entry] == _page)
            return;
        _seen[entry] = _page;

        if (_min < 0)
        {
            _min = _max = entry;
            return;
        }

        if (Compare(entry, _min) < 0) _min = entry;
        if (Compare(entry, _max) > 0) _max = entry;
    }

    /// <summary>Adds each value's entry in turn.</summary>
    public void Add(ReadOnlySpan<int> entries)
    {
        foreach (int entry in entries)
            Add(entry, 1);
    }

    /// <summary>The bounds of the values added since <see cref="Begin"/>.</summary>
    public PageBounds End()
    {
        if (_min >= 0)
            return new PageBounds(Entry(_min).ToArray(), Entry(_max).ToArray(), _nanCount);

        if (_minNaN >= 0)
            return new PageBounds(Entry(_minNaN).ToArray(), Entry(_maxNaN).ToArray(), _nanCount);

        return new PageBounds(null, null, _nanCount);
    }

    private ReadOnlySpan<byte> Entry(int entry) => _entries.Entry(entry);

    private int Compare(int left, int right) =>
        StatisticsCollector.CompareValues(Entry(left), Entry(right), _physicalType, _order);
}
