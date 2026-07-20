// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Lance.Table.Indices;

/// <summary>
/// Reader for a Lance BTREE scalar index. A BTREE index lives in
/// <c>{dataset}/_indices/{uuid}/</c> as two Lance files:
/// <list type="bullet">
///   <item><c>page_data.lance</c> with schema
///   <c>(values: T, ids: uint64)</c>: one row per indexed value paired
///   with its stable row id, sorted ascending by <c>values</c>.</item>
///   <item><c>page_lookup.lance</c> with schema
///   <c>(min: T, max: T, null_count: uint32, page_idx: uint32)</c>:
///   one row per "page" of <c>page_data</c>. The <c>batch_size</c>
///   metadata key gives the page size (default 4096).</item>
/// </list>
///
/// <para>Row ids are 64-bit values: high 32 bits are the fragment id,
/// low 32 bits are the row offset within that fragment. This reader
/// surfaces them already split via <see cref="IndexedRowAddress"/>.</para>
///
/// <para>This slice handles the value type <see cref="Int32Type"/>
/// (the most common scalar-index column type for BTREE in pylance).
/// Other primitive types (<see cref="Int64Type"/>, <see cref="StringType"/>,
/// etc.) follow the same wire format and can be added as needed.</para>
///
/// <para>Auto-discovery from the manifest is not yet implemented —
/// callers locate index directories by listing <c>_indices/</c>. The
/// schema of the opened index tells the caller which value type the
/// index covers, but not which column name (that lives in the
/// <c>IndexMetadata</c> proto inside the dataset's transaction file,
/// which we don't yet parse).</para>
/// </summary>
public sealed class BTreeIndex : IAsyncDisposable
{
    private readonly LanceFileReader _pageData;
    private readonly LanceFileReader _pageLookup;
    private LookupRow[]? _cachedLookup;

    private BTreeIndex(LanceFileReader pageData, LanceFileReader pageLookup)
    {
        _pageData = pageData;
        _pageLookup = pageLookup;
    }

    /// <summary>The Arrow type of the indexed value column.</summary>
    public IArrowType ValueType =>
        _pageData.Schema.GetFieldByName("values").DataType;

    /// <summary>
    /// Open a BTREE index from an index directory using a local filesystem.
    /// </summary>
    public static ValueTask<BTreeIndex> OpenAsync(
        string indexDirectory, CancellationToken cancellationToken = default)
    {
        var fs = new LocalTableFileSystem(indexDirectory);
        return OpenAsync(fs, ".", cancellationToken);
    }

    /// <summary>
    /// Open a BTREE index from a caller-provided filesystem rooted at the
    /// dataset, given the relative path to the index directory (e.g.
    /// <c>"_indices/{uuid}"</c>).
    /// </summary>
    public static async ValueTask<BTreeIndex> OpenAsync(
        ITableFileSystem fs, string indexDirectory,
        CancellationToken cancellationToken = default)
    {
        string dataPath = JoinPath(indexDirectory, "page_data.lance");
        string lookupPath = JoinPath(indexDirectory, "page_lookup.lance");
        IRandomAccessFile? dataFile = null;
        IRandomAccessFile? lookupFile = null;
        LanceFileReader? dataReader = null;
        LanceFileReader? lookupReader = null;
        try
        {
            dataFile = await fs.OpenReadAsync(dataPath, cancellationToken).ConfigureAwait(false);
            dataReader = await LanceFileReader.OpenAsync(dataFile, ownsReader: true, cancellationToken).ConfigureAwait(false);
            dataFile = null;  // ownership transferred
            lookupFile = await fs.OpenReadAsync(lookupPath, cancellationToken).ConfigureAwait(false);
            lookupReader = await LanceFileReader.OpenAsync(lookupFile, ownsReader: true, cancellationToken).ConfigureAwait(false);
            lookupFile = null;

            var dataReaderTaken = dataReader; dataReader = null;
            var lookupReaderTaken = lookupReader; lookupReader = null;
            return new BTreeIndex(dataReaderTaken, lookupReaderTaken);
        }
        finally
        {
            if (lookupReader is not null) await lookupReader.DisposeAsync().ConfigureAwait(false);
            if (dataReader is not null) await dataReader.DisposeAsync().ConfigureAwait(false);
            if (lookupFile is not null) await lookupFile.DisposeAsync().ConfigureAwait(false);
            if (dataFile is not null) await dataFile.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Find every row whose indexed value equals <paramref name="value"/>.
    /// Returns the row-addresses (fragment id + row offset) in the order
    /// the index stores them. Empty list if no match.
    /// </summary>
    public Task<IReadOnlyList<IndexedRowAddress>> QueryEqualAsync(
        int value, CancellationToken cancellationToken = default)
        => QueryRangeAsync(value, value, includeMin: true, includeMax: true, cancellationToken);

    /// <summary>
    /// Find every row whose indexed value lies in
    /// <c>[min, max]</c> (inclusive on both ends by default). Pass
    /// <c>null</c> for an open end.
    /// </summary>
    public async Task<IReadOnlyList<IndexedRowAddress>> QueryRangeAsync(
        int? min, int? max,
        bool includeMin = true, bool includeMax = true,
        CancellationToken cancellationToken = default)
    {
        if (ValueType is not Int32Type)
            throw new NotSupportedException(
                $"BTreeIndex.QueryRangeAsync(int) requires an Int32 index; this index is {ValueType}.");

        var lookup = await EnsureLookupAsync(cancellationToken).ConfigureAwait(false);
        var matches = new List<IndexedRowAddress>();

        for (int p = 0; p < lookup.Length; p++)
        {
            var row = lookup[p];
            // Skip pages whose [min, max] range can't possibly contain
            // values in [min, max]. The page's null_count doesn't affect
            // value-range pruning.
            if (max is int maxV && (includeMax ? row.PageMin > maxV : row.PageMin >= maxV))
                continue;
            if (min is int minV && (includeMin ? row.PageMax < minV : row.PageMax <= minV))
                continue;

            // Read page p of page_data and scan it.
            var values = (Int32Array)await _pageData.ReadColumnAsync(0, cancellationToken).ConfigureAwait(false);
            var ids = (UInt64Array)await _pageData.ReadColumnAsync(1, cancellationToken).ConfigureAwait(false);
            // page_data is sorted by values; we could binary-search, but
            // for a first slice, scan linearly. Lance writes one page = one
            // contiguous batch, so values has length = batch size for that
            // page (and we currently fetch the whole column — multi-page
            // page_data files would need pagewise reads).
            // For now, assume one page (= one Lance page).
            int n = values.Length;
            for (int i = 0; i < n; i++)
            {
                int v = values.GetValue(i)!.Value;
                if (min is int mn)
                {
                    if (includeMin ? v < mn : v <= mn) continue;
                }
                if (max is int mx)
                {
                    if (includeMax ? v > mx : v >= mx) continue;
                }
                ulong id = ids.GetValue(i)!.Value;
                matches.Add(new IndexedRowAddress(
                    FragmentId: checked((uint)(id >> 32)),
                    RowOffset: checked((uint)(id & 0xFFFF_FFFFUL))));
            }
        }
        return matches;
    }

    private async Task<LookupRow[]> EnsureLookupAsync(CancellationToken cancellationToken)
    {
        if (_cachedLookup is not null) return _cachedLookup;

        var schema = _pageLookup.Schema;
        if (schema.GetFieldByName("min") is null
            || schema.GetFieldByName("max") is null
            || schema.GetFieldByName("page_idx") is null)
            throw new LanceTableFormatException(
                "page_lookup.lance is missing one of (min, max, page_idx).");
        if (schema.GetFieldByName("min").DataType is not Int32Type)
            throw new NotSupportedException(
                $"BTreeIndex page_lookup with non-Int32 min/max ({schema.GetFieldByName("min").DataType}) is not yet supported.");

        var minArr = (Int32Array)await _pageLookup.ReadColumnAsync(0, cancellationToken).ConfigureAwait(false);
        var maxArr = (Int32Array)await _pageLookup.ReadColumnAsync(1, cancellationToken).ConfigureAwait(false);
        // null_count column at index 2; page_idx at index 3.
        var pageIdxArr = (UInt32Array)await _pageLookup.ReadColumnAsync(3, cancellationToken).ConfigureAwait(false);

        int n = minArr.Length;
        var rows = new LookupRow[n];
        for (int i = 0; i < n; i++)
        {
            rows[i] = new LookupRow(
                PageMin: minArr.GetValue(i)!.Value,
                PageMax: maxArr.GetValue(i)!.Value,
                PageIdx: pageIdxArr.GetValue(i)!.Value);
        }
        _cachedLookup = rows;
        return rows;
    }

    public async ValueTask DisposeAsync()
    {
        await _pageData.DisposeAsync().ConfigureAwait(false);
        await _pageLookup.DisposeAsync().ConfigureAwait(false);
    }

    private static string JoinPath(string a, string b) =>
        a == "." || a == string.Empty
            ? b
            : a.EndsWith("/", StringComparison.Ordinal) ? a + b : a + "/" + b;

    private readonly record struct LookupRow(int PageMin, int PageMax, uint PageIdx);
}

/// <summary>
/// A row address returned by a scalar index query: split into the
/// fragment that owns the row and the row's offset within the fragment.
/// </summary>
public readonly record struct IndexedRowAddress(uint FragmentId, uint RowOffset);
