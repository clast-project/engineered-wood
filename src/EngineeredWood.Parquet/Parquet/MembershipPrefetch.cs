// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// Reads ahead what row-group pruning will ask of one membership source (dictionary pages or Bloom
/// filters): many row groups to a request, instead of one request per row group.
/// </summary>
/// <remarks>
/// <para>Measured by <c>dictpruning-ab</c> with 20 ms added per request, asking each row group's dictionary
/// in its own request made dictionary pruning a loss on object storage: a skipped group traded its data
/// request for a dictionary request (same round trips, fewer bytes), and a group that was not skipped
/// paid both, nearly doubling the read. Here the first group to need its dictionaries loads a WINDOW:
/// its own and those of the groups after it that statistics leave undecided, up to a byte budget, in
/// one <see cref="IRandomAccessFile.ReadRangesAsync"/>. The next group outside the window loads the next.
/// Memory holds one window at a time.</para>
/// <para>What that saves is ROUND TRIPS, not requests. Object stores have no multi-range GET, and the
/// cloud readers' <see cref="CoalescingFileReader"/> sends the window's distant ranges as separate GETs
/// issued concurrently. So on S3, Azure or GCS a window costs about one GET per page but only one round
/// trip of waiting, where asking group by group cost a round trip per group.</para>
/// <para>Only the columns an equality or IN leaf names are read, and only chunks
/// <see cref="MembershipPredicateEvaluator.TryGetRange"/> accepts, the same test the single read
/// applies. A group this has no entry for is left to the evaluator, which reads what it needs itself, so
/// a gap here costs a request and never a wrong answer.</para>
/// <para>Each source has its own prefetch, and so its own windows. The Bloom step only runs for a group
/// the dictionary step left undecided, but a Bloom window is chosen by statistics alone, so with both
/// sources on it may also read the filters of groups a dictionary then rules out: bytes, not round
/// trips, since they arrive in the same window.</para>
/// </remarks>
internal sealed class MembershipPrefetch
{
    /// <summary>
    /// The bytes one window may ask for. Well above a default dictionary page or Bloom filter
    /// (~1 MiB), so a window usually spans many row groups, and small enough to hold in memory.
    /// </summary>
    internal const long DefaultBudgetBytes = 32L * 1024 * 1024;

    /// <summary>
    /// The most ranges (pages or filters) one window asks for. On object storage a window is not one
    /// HTTP request: <see cref="CoalescingFileReader"/> merges only ranges under a small gap apart, and
    /// these sit between data chunks, so it issues about one GET per range, all at once.
    /// The window saves round trips because those GETs run concurrently; this bounds how many.
    /// </summary>
    internal const int MaxRangesPerWindow = 64;

    private readonly MembershipSource _source;
    private readonly Predicate _filter;
    private readonly FileMetaData _metadata;
    private readonly ParquetStatisticsAccessor _accessor;
    private readonly IRandomAccessFile _file;
    private readonly long _fileLength;
    private readonly ColumnChunkFilePathKind _filePath;
    private readonly bool _validateChecksums;
    private readonly long _budgetBytes;
    private readonly List<(int Index, ColumnDescriptor Descriptor)> _columns;

    /// <summary>The current window: each covered row group's value sets by column index.</summary>
    private readonly Dictionary<int, Dictionary<int, MembershipPredicateEvaluator.IValueSet?>> _window = new();

    /// <summary>The first row group after the current window.</summary>
    private int _windowEnd;

    public MembershipPrefetch(
        MembershipSource source,
        Predicate filter, FileMetaData metadata, SchemaDescriptor schema, ParquetStatisticsAccessor accessor,
        IRandomAccessFile file, long fileLength, ColumnChunkFilePathKind filePath, bool validateChecksums,
        long budgetBytes)
    {
        _source = source;
        _filter = filter;
        _metadata = metadata;
        _accessor = accessor;
        _file = file;
        _fileLength = fileLength;
        _filePath = filePath;
        _validateChecksums = validateChecksums;
        _budgetBytes = budgetBytes;
        _columns = MembershipPredicateEvaluator.MembershipColumns(filter, schema);
    }

    /// <summary>
    /// The value sets of <paramref name="rowGroup"/>'s predicate columns (null for a chunk that cannot
    /// answer), loading the window that starts at it when it is past the current one. Call in ascending
    /// row-group order, and only for a group statistics left undecided.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<int, MembershipPredicateEvaluator.IValueSet?>?> ForRowGroupAsync(
        int rowGroup, CancellationToken cancellationToken)
    {
        if (_columns.Count == 0)
            return null;

        if (rowGroup >= _windowEnd)
            await LoadWindowAsync(rowGroup, cancellationToken).ConfigureAwait(false);

        return _window.TryGetValue(rowGroup, out var sets) ? sets : null;
    }

    private async ValueTask LoadWindowAsync(int first, CancellationToken cancellationToken)
    {
        _window.Clear();
        var ranges = new List<FileRange>();
        var owners = new List<(int RowGroup, int Column, ColumnDescriptor Descriptor, CompressionCodec Codec)>();
        long bytes = 0;

        int group = first;
        for (; group < _metadata.RowGroups.Count; group++)
        {
            var rowGroup = _metadata.RowGroups[group];

            // Groups statistics decide never reach a membership step, so they cost nothing here. The
            // caller has already found the first group undecided.
            if (group != first && StatisticsEvaluator.Evaluate(_filter, rowGroup, _accessor) != FilterResult.Unknown)
                continue;

            var sets = new Dictionary<int, MembershipPredicateEvaluator.IValueSet?>();
            var groupRanges = new List<(int Column, ColumnDescriptor Descriptor, CompressionCodec Codec, FileRange Range)>();
            long groupBytes = 0;
            foreach (var (index, descriptor) in _columns)
            {
                var chunk = rowGroup.Columns[index];
                if (MembershipPredicateEvaluator.TryGetRange(
                        _source, chunk, descriptor, _fileLength, _filePath, out var range))
                {
                    groupRanges.Add((index, descriptor, chunk.MetaData!.Codec, range));
                    groupBytes += range.Length;
                }
                else
                {
                    sets[index] = null; // cannot answer: never read
                }
            }

            // The first group is always loaded, whatever it costs; a later one that would overrun the
            // budget, or the cap on concurrent pages, starts the next window instead.
            if (group != first
                && (bytes + groupBytes > _budgetBytes || ranges.Count + groupRanges.Count > MaxRangesPerWindow))
                break;

            bytes += groupBytes;
            foreach (var (column, descriptor, codec, range) in groupRanges)
            {
                ranges.Add(range);
                owners.Add((group, column, descriptor, codec));
            }
            _window[group] = sets;
        }
        _windowEnd = group;

        if (ranges.Count == 0)
            return;

        var buffers = await _file.ReadRangesAsync(ranges, cancellationToken).ConfigureAwait(false);
        try
        {
            for (int i = 0; i < owners.Count; i++)
            {
                var (rowGroup, column, descriptor, codec) = owners[i];
                _window[rowGroup][column] = MembershipPredicateEvaluator.Decode(
                    _source, buffers[i].Memory.Span, codec, descriptor, _validateChecksums);
            }
        }
        finally
        {
            foreach (var buffer in buffers)
                buffer.Dispose();
        }
    }
}
