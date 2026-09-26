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
internal sealed record ChunkPageIndex(IReadOnlyList<PageLocation> PageLocations, ColumnIndex? ColumnIndex);

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
    private long _firstRowIndex;

    /// <param name="writeColumnIndex">
    /// Whether the chunk may get a ColumnIndex at all; false for a column without statistics or of a
    /// type whose bounds are not ordered correctly. The OffsetIndex is written either way.
    /// </param>
    /// <param name="utf8">Whether BYTE_ARRAY values are UTF-8, and so must be truncated on code points.</param>
    public PageIndexCollector(
        PhysicalType physicalType,
        int typeLength,
        StatisticsOrder order,
        bool floatingPointTotalOrder,
        int? truncateLength,
        bool utf8,
        bool writeColumnIndex)
    {
        _physicalType = physicalType;
        _typeLength = typeLength;
        _order = order;
        FloatingPointTotalOrder = floatingPointTotalOrder;
        _truncateLength = physicalType == PhysicalType.ByteArray ? truncateLength : null;
        _utf8 = utf8;
        _floatingPoint = physicalType is PhysicalType.Float or PhysicalType.Double;
        WritesColumnIndex = writeColumnIndex;
    }

    /// <summary>Whether page bounds are needed: false once the chunk is known to get no ColumnIndex.</summary>
    public bool WritesColumnIndex { get; private set; }

    public bool FloatingPointTotalOrder { get; }

    public PhysicalType PhysicalType => _physicalType;

    public int TypeLength => _typeLength;

    public StatisticsOrder Order => _order;

    /// <summary>
    /// Records a data page just written.
    /// </summary>
    /// <param name="page">Where <see cref="ColumnChunkOutput.EmitPage(PageHeader, ReadOnlySpan{byte})"/> put it.</param>
    /// <param name="rows">Rows that start in the page. Every page starts a row, so this sums to the chunk's rows.</param>
    /// <param name="nullCount">Level entries in the page that hold no value.</param>
    /// <param name="bounds">The page's bounds; ignored unless <see cref="WritesColumnIndex"/>.</param>
    public void AddPage(EmittedPage page, int rows, int nullCount, int nonNullCount, in PageBounds bounds)
    {
        if (page.Ordinal != _locations.Count)
            throw new InvalidOperationException($"Data page {page.Ordinal} recorded as page {_locations.Count}.");

        _locations.Add(new PageLocation(page.Offset, page.Size, _firstRowIndex));
        _firstRowIndex += rows;

        if (!WritesColumnIndex)
            return;

        bool nullPage = nonNullCount == 0;
        if (!nullPage && bounds.Min is null)
        {
            // Values but no bounds: every value is NaN under TYPE_ORDER. The format then requires the
            // chunk to have no ColumnIndex at all, since no bound would be a valid one.
            WritesColumnIndex = false;
            return;
        }

        _nullPages.Add(nullPage);
        _mins.Add(nullPage ? [] : Truncate(bounds.Min!, max: false));
        _maxes.Add(nullPage ? [] : Truncate(bounds.Max!, max: true));
        _nullCounts.Add(nullCount);
        if (_floatingPoint)
            _nanCounts.Add(nullPage ? 0 : bounds.NanCount);
    }

    /// <summary>The chunk's page index, with page offsets still relative to the chunk.</summary>
    public ChunkPageIndex Build()
    {
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

        return new ChunkPageIndex(_locations.ToArray(), columnIndex);
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

    private bool IsNaN(byte[] value) => _physicalType == PhysicalType.Float
        ? float.IsNaN(MemoryMarshal.Read<float>(value))
        : double.IsNaN(MemoryMarshal.Read<double>(value));
}

/// <summary>
/// A dictionary-encoded page's bounds, found from the dictionary entries its indices reference rather
/// than from the values, which the dictionary path no longer has.
/// </summary>
/// <remarks>
/// Each distinct entry a page references is compared once, with
/// <see cref="StatisticsCollector.CompareValues"/>, the comparator the chunk statistics use. NaN
/// entries are counted, not compared; an all-NaN page under IEEE 754 total order takes the first NaN
/// in row order as both bounds, as the full scan does.
/// </remarks>
internal sealed class DictionaryPageBounds
{
    private readonly byte[] _dictionary;
    private readonly int[] _entryStart;
    private readonly int[] _entryLength;
    private readonly bool[]? _nanEntries;
    private readonly PhysicalType _physicalType;
    private readonly StatisticsOrder _order;
    private readonly bool _totalOrder;

    // _seen[entry] == _page marks an entry already compared for the current page, so a page costs
    // one comparison per distinct entry, not per value, and nothing has to be cleared between pages.
    private readonly int[] _seen;
    private int _page;

    private int _min, _max, _firstNaN;
    private long _nanCount;

    public DictionaryPageBounds(in DictionaryEncoder.DictionaryResult dictionary, PageIndexCollector collector)
    {
        _dictionary = dictionary.DictionaryPageData;
        _physicalType = collector.PhysicalType;
        _order = collector.Order;
        _totalOrder = collector.FloatingPointTotalOrder;

        int count = dictionary.DictionaryCount;
        _entryStart = new int[count];
        _entryLength = new int[count];
        _seen = new int[count];

        if (_physicalType == PhysicalType.ByteArray)
        {
            // PLAIN BYTE_ARRAY: a 4-byte little-endian length before each entry.
            int position = 0;
            for (int i = 0; i < count; i++)
            {
                int length = BinaryPrimitives.ReadInt32LittleEndian(_dictionary.AsSpan(position));
                _entryStart[i] = position + 4;
                _entryLength[i] = length;
                position += 4 + length;
            }
        }
        else
        {
            int width = _physicalType switch
            {
                PhysicalType.Int32 or PhysicalType.Float => 4,
                PhysicalType.Int64 or PhysicalType.Double => 8,
                PhysicalType.Int96 => 12,
                PhysicalType.Boolean => 1,
                _ => collector.TypeLength,
            };
            for (int i = 0; i < count; i++)
            {
                _entryStart[i] = i * width;
                _entryLength[i] = width;
            }
        }

        if (_physicalType is PhysicalType.Float or PhysicalType.Double)
        {
            for (int i = 0; i < count; i++)
            {
                var entry = Entry(i);
                bool nan = _physicalType == PhysicalType.Float
                    ? float.IsNaN(MemoryMarshal.Read<float>(entry))
                    : double.IsNaN(MemoryMarshal.Read<double>(entry));
                if (!nan) continue;
                _nanEntries ??= new bool[count];
                _nanEntries[i] = true;
            }
        }
    }

    /// <summary>Starts a new page.</summary>
    public void Begin()
    {
        _page++;
        _min = _max = _firstNaN = -1;
        _nanCount = 0;
    }

    /// <summary>Adds <paramref name="count"/> consecutive values that all reference <paramref name="entry"/>.</summary>
    public void Add(int entry, int count)
    {
        if (_nanEntries is not null && _nanEntries[entry])
        {
            if (_firstNaN < 0) _firstNaN = entry;
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

        if (_firstNaN >= 0 && _totalOrder)
        {
            byte[] nan = Entry(_firstNaN).ToArray();
            return new PageBounds(nan, nan, _nanCount);
        }

        return new PageBounds(null, null, _nanCount);
    }

    private ReadOnlySpan<byte> Entry(int entry) => _dictionary.AsSpan(_entryStart[entry], _entryLength[entry]);

    private int Compare(int left, int right) =>
        StatisticsCollector.CompareValues(Entry(left), Entry(right), _physicalType, _order);
}
