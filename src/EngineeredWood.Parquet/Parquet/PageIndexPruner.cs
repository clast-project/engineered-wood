// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// Page pruning (doc/parquet-page-index.md, R-2): the rows of one row group that a predicate might
/// match, judged from the ColumnIndex of each column it references.
/// </summary>
/// <remarks>
/// <para>The predicate columns' page boundaries, merged, cut the row group into elementary intervals,
/// inside each of which every indexed column is covered by exactly one page. Each interval is judged
/// by the unchanged <see cref="StatisticsEvaluator"/>, over an accessor that answers with the covering
/// page's bounds and counts, so AND/OR/NOT, IN, NaN and the inverted-bounds guard (#412) all behave as
/// they do for a row group. An interval judged AlwaysFalse is dropped; the rest coalesce into ranges.</para>
/// <para>Only columns that are not repeated take part. For a repeated leaf, the ColumnIndex counts
/// values rather than rows, so a predicate on one stays Unknown here, as it is for a row group. An
/// index that fails to decode, or cannot tile the row group, is ignored rather than trusted: this is an
/// optimisation, and a read that does not consult the index would not fail on it.</para>
/// </remarks>
internal static class PageIndexPruner
{
    /// <summary>
    /// The leaves of <paramref name="filter"/> whose page index can narrow it in
    /// <paramref name="rowGroup"/>: flat, with both indexes (offset and length each; a footer with only
    /// one of the two has no index), and not stored in another file. A chunk
    /// with a <c>file_path</c> counts as stored here under <see cref="ColumnChunkFilePathKind.Ignore"/>,
    /// as it does for every other read (#405). In schema order.
    /// </summary>
    internal static List<int> PrunableColumns(
        Predicate filter, RowGroup rowGroup, SchemaDescriptor schema, ColumnChunkFilePathKind filePath)
    {
        var names = ParquetStatisticsAccessor.BuildNameIndex(schema);
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        var variantCalls = new HashSet<VariantGetCall>();
        CollectColumns(filter, referenced, variantCalls);

        var leaves = new SortedSet<int>();
        foreach (string name in referenced)
        {
            if (names.TryGetValue(name, out int leaf) && Indexed(leaf))
                leaves.Add(leaf);
        }

        // A variant path prunes pages by its typed_value leaf, where its residual is null throughout the
        // interval: by the residual's own page index, or for every interval when its chunk is null throughout.
        foreach (var call in variantCalls)
        {
            if (ShreddedVariantPath.Resolve(schema, call) is not { } path || !Indexed(path.Leaf))
                continue;
            if (path.Residual is int residual && !ResidualNullThroughout(rowGroup, residual))
            {
                if (!Indexed(residual))
                    continue;
                leaves.Add(residual);
            }
            leaves.Add(path.Leaf);
        }

        return [.. leaves];

        bool Indexed(int leaf)
        {
            if (leaf >= rowGroup.Columns.Count)
                return false;
            var chunk = rowGroup.Columns[leaf];
            return schema.Columns[leaf].MaxRepetitionLevel == 0
                && (chunk.FilePath is null || filePath == ColumnChunkFilePathKind.Ignore)
                && chunk.ColumnIndexOffset is not null && chunk.ColumnIndexLength is not null
                && chunk.OffsetIndexOffset is not null && chunk.OffsetIndexLength is not null;
        }
    }

    /// <summary>Whether a residual <c>value</c> column's chunk statistics say it is null in every row.</summary>
    private static bool ResidualNullThroughout(RowGroup rowGroup, int residual) =>
        residual < rowGroup.Columns.Count
        && rowGroup.Columns[residual].MetaData?.Statistics?.NullCount == rowGroup.NumRows;

    /// <summary>
    /// The ranges of <paramref name="rowGroup"/> that might hold a row matching <paramref name="filter"/>,
    /// ascending and disjoint, given the page indexes of its <see cref="PrunableColumns"/>.
    /// </summary>
    internal static IReadOnlyList<RowRange> SelectRows(
        Predicate filter, RowGroup rowGroup, SchemaDescriptor schema, IReadOnlyList<ColumnChunkPageIndex> indexes)
    {
        long rows = rowGroup.NumRows;
        if (rows <= 0)
            return [];

        var columns = new List<IndexedColumn>();
        foreach (var index in indexes)
        {
            if (Usable(index, schema.Columns[index.Column].MaxDefinitionLevel == 0, rows, out var offsetIndex, out var columnIndex))
                columns.Add(new IndexedColumn(index.Column, schema.Columns[index.Column], offsetIndex!, columnIndex!));
        }

        if (columns.Count == 0)
            return [new RowRange(0, rows)];

        // The elementary intervals: every column's page starts, merged. Each column's list is already
        // ascending (Usable checks), so a sort of the union is all it takes.
        var cuts = new List<long>();
        foreach (var column in columns)
        {
            foreach (var page in column.Pages)
                cuts.Add(page.FirstRowIndex);
        }

        cuts.Sort();
        cuts.Add(rows);

        var accessor = new IntervalAccessor(schema, rowGroup, columns);
        var interval = new Interval(columns.Count);
        var result = new List<RowRange>();
        for (int i = 0; i + 1 < cuts.Count; i++)
        {
            long start = cuts[i], end = cuts[i + 1];
            if (start == end)
                continue; // the same boundary from more than one column

            interval.Start = start;
            interval.End = end;
            for (int slot = 0; slot < columns.Count; slot++)
            {
                var pages = columns[slot].Pages;
                ref int page = ref interval.Pages[slot];
                while (page + 1 < pages.Count && pages[page + 1].FirstRowIndex <= start)
                    page++;
            }

            if (StatisticsEvaluator.Evaluate(filter, interval, accessor) == FilterResult.AlwaysFalse)
                continue;

            if (result.Count > 0 && result[^1].End == start)
                result[^1] = result[^1] with { End = end };
            else
                result.Add(new RowRange(start, end));
        }

        return result;
    }

    /// <summary>
    /// Whether a column's page index can be trusted to describe the row group: both indexes decode,
    /// the pages start at row 0 and ascend strictly within the row group, every per-page list of the
    /// ColumnIndex has one entry per page, and no count is impossible (negative, more nulls and NaNs
    /// together than rows, a null page in a required column, with fewer nulls than rows, or with a NaN).
    /// </summary>
    private static bool Usable(
        ColumnChunkPageIndex index, bool required, long rows, out OffsetIndex? offsetIndex, out ColumnIndex? columnIndex)
    {
        try
        {
            offsetIndex = index.OffsetIndex;
            columnIndex = index.ColumnIndex;
        }
        catch (ParquetFormatException)
        {
            offsetIndex = null;
            columnIndex = null;
            return false;
        }

        if (offsetIndex is null || columnIndex is null)
            return false;

        var pages = offsetIndex.PageLocations;
        int count = pages.Count;
        if (count == 0 || pages[0].FirstRowIndex != 0)
            return false;
        for (int p = 1; p < count; p++)
        {
            if (pages[p].FirstRowIndex <= pages[p - 1].FirstRowIndex)
                return false;
        }

        if (pages[count - 1].FirstRowIndex >= rows
            || columnIndex.NullPages.Count != count
            || columnIndex.MinValues.Count != count
            || columnIndex.MaxValues.Count != count
            || (columnIndex.NullCounts is not null && columnIndex.NullCounts.Count != count)
            || (columnIndex.NanCounts is not null && columnIndex.NanCounts.Count != count))
        {
            return false;
        }

        // Counts that cannot be true. parquet-mr 1.13 wrote the datapage_v1-*-checksum fixtures with
        // every page marked null, null counts of -1 and empty bounds, for required columns full of
        // values: a placeholder index, trusted, drops every row of `a IS NOT NULL`. An impossible NaN
        // count could only keep rows, but it says the same thing about the index as an impossible null
        // count does, and the index's bounds are what drop them. In a flat column each row is one
        // value, null or not, and a NaN is a non-null value, so a page's nulls and NaNs share its rows.
        for (int p = 0; p < count; p++)
        {
            long pageRows = (p + 1 < count ? pages[p + 1].FirstRowIndex : rows) - pages[p].FirstRowIndex;
            long? nulls = columnIndex.NullCounts?[p];
            long? nans = columnIndex.NanCounts?[p];
            if (nulls is < 0 || nulls > pageRows || nans is < 0 || nans > pageRows || nulls + nans > pageRows)
                return false;
            if (columnIndex.NullPages[p] && (required || nulls is { } n && n != pageRows || nans > 0))
                return false;
        }

        return true;
    }

    private static void CollectColumns(Predicate predicate, HashSet<string> sink, HashSet<VariantGetCall> calls)
    {
        switch (predicate)
        {
            case AndPredicate and:
                foreach (var child in and.Children) CollectColumns(child, sink, calls);
                break;
            case OrPredicate or:
                foreach (var child in or.Children) CollectColumns(child, sink, calls);
                break;
            case NotPredicate not:
                CollectColumns(not.Child, sink, calls);
                break;
            case ComparisonPredicate comparison:
                CollectColumns(comparison.Left, sink, calls);
                CollectColumns(comparison.Right, sink, calls);
                break;
            case UnaryPredicate unary:
                CollectColumns(unary.Operand, sink, calls);
                break;
            case SetPredicate set:
                CollectColumns(set.Operand, sink, calls);
                foreach (var value in set.Values) CollectColumns(value, sink, calls);
                break;
        }
    }

    private static void CollectColumns(Expression expression, HashSet<string> sink, HashSet<VariantGetCall> calls)
    {
        switch (expression)
        {
            case UnboundReference reference: sink.Add(reference.Name); break;
            case BoundReference reference: sink.Add(reference.Name); break;
            case FunctionCall call when VariantGetCall.TryMatch(call, out var variantGet):
                calls.Add(variantGet);
                break;
            case FunctionCall call:
                foreach (var argument in call.Arguments) CollectColumns(argument, sink, calls);
                break;
            case Predicate predicate: CollectColumns(predicate, sink, calls); break;
        }
    }

    private sealed record IndexedColumn(int Leaf, ColumnDescriptor Descriptor, OffsetIndex OffsetIndex, ColumnIndex ColumnIndex)
    {
        public IReadOnlyList<PageLocation> Pages => OffsetIndex.PageLocations;
    }

    /// <summary>One elementary interval, and the page covering it in each indexed column (by slot).</summary>
    private sealed class Interval(int columns)
    {
        public long Start;
        public long End;
        public readonly int[] Pages = new int[columns];
    }

    /// <summary>
    /// Answers for an interval with its covering page's ColumnIndex entry. Bounds are never exact (a
    /// page's bounds may be truncated, and need not be values it holds). A null count is given only
    /// when it holds for any part of the page: all of it for a null page, none when the page has no
    /// nulls. Otherwise it is unknown, since the page's count need not fall in this interval.
    /// </summary>
    private sealed class IntervalAccessor
        : IStatisticsAccessor<Interval>, INanCountAccessor<Interval>, IVariantStatisticsAccessor<Interval>
    {
        private readonly SchemaDescriptor _schema;
        private readonly RowGroup _rowGroup;
        private readonly List<IndexedColumn> _columns;
        private readonly Dictionary<string, int> _slots = new(StringComparer.Ordinal);
        private readonly Dictionary<int, int> _slotOfLeaf = new();
        private readonly Dictionary<VariantGetCall, IStatisticsAccessor<Interval>?> _variantPaths = new();

        // The last page decoded per slot: a page usually spans several intervals.
        private readonly int[] _decodedPage;
        private readonly LiteralValue?[] _min;
        private readonly LiteralValue?[] _max;

        public IntervalAccessor(SchemaDescriptor schema, RowGroup rowGroup, List<IndexedColumn> columns)
        {
            _schema = schema;
            _rowGroup = rowGroup;
            _columns = columns;
            for (int slot = 0; slot < columns.Count; slot++)
                _slotOfLeaf[columns[slot].Leaf] = slot;
            foreach (var entry in ParquetStatisticsAccessor.BuildNameIndex(schema))
            {
                if (_slotOfLeaf.TryGetValue(entry.Value, out int slot))
                    _slots[entry.Key] = slot;
            }

            _decodedPage = new int[columns.Count];
            _decodedPage.AsSpan().Fill(-1);
            _min = new LiteralValue?[columns.Count];
            _max = new LiteralValue?[columns.Count];
        }

        public LiteralValue? GetMinValue(Interval interval, string column) =>
            Decode(interval, column, out int slot) ? _min[slot] : null;

        public LiteralValue? GetMaxValue(Interval interval, string column) =>
            Decode(interval, column, out int slot) ? _max[slot] : null;

        public long? GetNullCount(Interval interval, string column)
        {
            if (!_slots.TryGetValue(column, out int slot))
                return null;
            int page = interval.Pages[slot];
            var index = _columns[slot].ColumnIndex;
            if (index.NullPages[page])
                return interval.End - interval.Start;
            return index.NullCounts is { } counts && counts[page] == 0 ? 0 : null;
        }

        public long? GetNanCount(Interval interval, string column)
        {
            if (!_slots.TryGetValue(column, out int slot))
                return null;
            int page = interval.Pages[slot];
            var index = _columns[slot].ColumnIndex;
            if (index.NullPages[page])
                return 0;
            return index.NanCounts is { } counts && counts[page] == 0 ? 0 : null;
        }

        public long? GetValueCount(Interval interval, string column) => interval.End - interval.Start;

        public bool IsMinExact(Interval interval, string column) => false;

        public bool IsMaxExact(Interval interval, string column) => false;

        /// <summary>
        /// A variant path's statistics in an interval, from its <c>typed_value</c> leaf's covering page, where the
        /// residual is null throughout the interval (see <see cref="ShreddedVariantPath"/>). The two columns' pages
        /// need not line up: an interval lies inside one page of each.
        /// </summary>
        public IStatisticsAccessor<Interval>? ForVariantGet(VariantGetCall call)
        {
            if (!_variantPaths.TryGetValue(call, out var accessor))
            {
                accessor = null;
                if (ShreddedVariantPath.Resolve(_schema, call) is { } path
                    && _slotOfLeaf.TryGetValue(path.Leaf, out int leaf))
                {
                    // No slot for a residual its chunk shows null throughout; without one for any other, no bounds.
                    int? residual = null;
                    if (path.Residual is int r && !ResidualNullThroughout(_rowGroup, r))
                        residual = _slotOfLeaf.TryGetValue(r, out int slot) ? slot : -1;
                    if (residual is not -1)
                        accessor = new VariantPathAccessor(this, path, leaf, residual);
                }
                _variantPaths[call] = accessor;
            }
            return accessor;
        }

        private long PageRows(int slot, int page)
        {
            var pages = _columns[slot].Pages;
            return (page + 1 < pages.Count ? pages[page + 1].FirstRowIndex : _rowGroup.NumRows) - pages[page].FirstRowIndex;
        }

        /// <summary>A variant path's leaf pages, read where the residual's covering page (if any) is all null.</summary>
        private sealed class VariantPathAccessor(IntervalAccessor owner, ShreddedVariantPath path, int leaf, int? residual)
            : IStatisticsAccessor<Interval>, INanCountAccessor<Interval>
        {
            private ColumnIndex Index => owner._columns[leaf].ColumnIndex;

            // The leaf's covering page, or -1 when the residual's covering page holds a value.
            private int Page(Interval interval)
            {
                if (residual is int slot)
                {
                    int page = interval.Pages[slot];
                    var index = owner._columns[slot].ColumnIndex;
                    if (!index.NullPages[page] && index.NullCounts?[page] != owner.PageRows(slot, page))
                        return -1;
                }
                return interval.Pages[leaf];
            }

            public LiteralValue? GetMinValue(Interval interval, string column) =>
                Page(interval) is var page and >= 0 && !Index.NullPages[page]
                    ? path.LowerBound(Index.MinValues[page], Index.MaxValues[page])
                    : null;

            public LiteralValue? GetMaxValue(Interval interval, string column) =>
                Page(interval) is var page and >= 0 && !Index.NullPages[page]
                    ? path.UpperBound(Index.MaxValues[page])
                    : null;

            // As for a row group (ParquetStatisticsAccessor.ForVariantGet), but only a count that holds for any part
            // of the page: all of it null, or none of it.
            public long? GetNullCount(Interval interval, string column)
            {
                int page = Page(interval);
                if (page < 0)
                    return null;
                if (Index.NullPages[page] || Index.NullCounts?[page] == owner.PageRows(leaf, page))
                    return interval.End - interval.Start;
                return Index.NullCounts?[page] == 0
                    && GetMinValue(interval, column) is not null && GetMaxValue(interval, column) is not null
                    ? 0
                    : null;
            }

            public long? GetNanCount(Interval interval, string column)
            {
                int page = Page(interval);
                if (page < 0)
                    return null;
                if (Index.NullPages[page])
                    return 0;
                return Index.NanCounts is { } counts && counts[page] == 0 ? 0 : null;
            }

            public long? GetValueCount(Interval interval, string column) => interval.End - interval.Start;

            public bool IsMinExact(Interval interval, string column) => false;

            public bool IsMaxExact(Interval interval, string column) => false;
        }

        /// <summary>Decodes the covering page's bounds into the slot's cache; false for an unindexed column.</summary>
        private bool Decode(Interval interval, string column, out int slot)
        {
            if (!_slots.TryGetValue(column, out slot))
                return false;
            int page = interval.Pages[slot];
            if (_decodedPage[slot] != page)
            {
                var indexed = _columns[slot];
                var index = indexed.ColumnIndex;
                if (index.NullPages[page])
                {
                    // A null page's bounds are placeholders; its null count is what prunes it.
                    _min[slot] = null;
                    _max[slot] = null;
                }
                else
                {
                    _min[slot] = ParquetStatisticsAccessor.DecodeLowerBound(indexed.Descriptor, index.MinValues[page], index.MaxValues[page]);
                    _max[slot] = ParquetStatisticsAccessor.DecodeUpperBound(indexed.Descriptor, index.MaxValues[page], index.MinValues[page]);
                }

                _decodedPage[slot] = page;
            }

            return true;
        }
    }
}
