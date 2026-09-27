// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// Reads the dictionary pages that row-group pruning will ask for ahead of time, many row groups to a
/// request, instead of one request per row group.
/// </summary>
/// <remarks>
/// <para>Measured by <c>dictpruning-ab</c> with 20 ms added per request, asking each row group's dictionary
/// in its own request made dictionary pruning a loss on object storage: a skipped group traded its data
/// request for a dictionary request (same round trips, fewer bytes), and a group that was not skipped
/// paid both, nearly doubling the read. Here the first group to need its dictionaries loads a WINDOW:
/// its own and those of the groups after it that statistics leave undecided, up to a byte budget, in
/// one <see cref="IRandomAccessFile.ReadRangesAsync"/>. The next group outside the window loads the next.
/// Memory holds one window at a time.</para>
/// <para>Only the columns an equality or IN leaf names are read, and only chunks
/// <see cref="MembershipPredicateEvaluator.TryGetDictionaryRange"/> accepts, the same test the single
/// read applies. A group this has no entry for is left to the evaluator, which reads what it needs
/// itself, so a gap here costs a request and never a wrong answer.</para>
/// </remarks>
internal sealed class DictionaryPrefetch
{
    /// <summary>
    /// The bytes of dictionary pages one window may ask for. Well above a default dictionary page
    /// (~1 MiB), so a window usually spans many row groups, and small enough to hold in memory.
    /// </summary>
    internal const long DefaultBudgetBytes = 32L * 1024 * 1024;

    private readonly Predicate _filter;
    private readonly FileMetaData _metadata;
    private readonly ParquetStatisticsAccessor _accessor;
    private readonly IRandomAccessFile _file;
    private readonly long _fileLength;
    private readonly ColumnChunkFilePathKind _filePath;
    private readonly bool _validateChecksums;
    private readonly long _budgetBytes;
    private readonly List<(int Index, ColumnDescriptor Descriptor)> _columns;

    /// <summary>The current window: each covered row group's dictionaries by column index.</summary>
    private readonly Dictionary<int, Dictionary<int, HashSet<byte[]>?>> _window = new();

    /// <summary>The first row group after the current window.</summary>
    private int _windowEnd;

    public DictionaryPrefetch(
        Predicate filter, FileMetaData metadata, SchemaDescriptor schema, ParquetStatisticsAccessor accessor,
        IRandomAccessFile file, long fileLength, ColumnChunkFilePathKind filePath, bool validateChecksums,
        long budgetBytes)
    {
        _filter = filter;
        _metadata = metadata;
        _accessor = accessor;
        _file = file;
        _fileLength = fileLength;
        _filePath = filePath;
        _validateChecksums = validateChecksums;
        _budgetBytes = budgetBytes;
        _columns = MembershipPredicateEvaluator.DictionaryColumns(filter, schema);
    }

    /// <summary>
    /// The dictionaries of <paramref name="rowGroup"/>'s predicate columns (null for a chunk that cannot
    /// answer), loading the window that starts at it when it is past the current one. Call in ascending
    /// row-group order, and only for a group statistics left undecided.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<int, HashSet<byte[]>?>?> ForRowGroupAsync(
        int rowGroup, CancellationToken cancellationToken)
    {
        if (_columns.Count == 0)
            return null;

        if (rowGroup >= _windowEnd)
            await LoadWindowAsync(rowGroup, cancellationToken).ConfigureAwait(false);

        return _window.TryGetValue(rowGroup, out var dictionaries) ? dictionaries : null;
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

            // Groups statistics decide never reach the dictionary step, so they cost nothing here. The
            // caller has already found the first group undecided.
            if (group != first && StatisticsEvaluator.Evaluate(_filter, rowGroup, _accessor) != FilterResult.Unknown)
                continue;

            var dictionaries = new Dictionary<int, HashSet<byte[]>?>();
            var groupRanges = new List<(int Column, ColumnDescriptor Descriptor, CompressionCodec Codec, FileRange Range)>();
            long groupBytes = 0;
            foreach (var (index, descriptor) in _columns)
            {
                var chunk = rowGroup.Columns[index];
                if (MembershipPredicateEvaluator.TryGetDictionaryRange(
                        chunk, descriptor, _fileLength, _filePath, out var range))
                {
                    groupRanges.Add((index, descriptor, chunk.MetaData!.Codec, range));
                    groupBytes += range.Length;
                }
                else
                {
                    dictionaries[index] = null; // cannot answer: never read
                }
            }

            // The first group is always loaded, whatever it costs; a later one that would overrun the
            // budget starts the next window instead.
            if (group != first && bytes + groupBytes > _budgetBytes)
                break;

            bytes += groupBytes;
            foreach (var (column, descriptor, codec, range) in groupRanges)
            {
                ranges.Add(range);
                owners.Add((group, column, descriptor, codec));
            }
            _window[group] = dictionaries;
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
                _window[rowGroup][column] = MembershipPredicateEvaluator.DecodeDictionary(
                    buffers[i].Memory.Span, codec, descriptor, _validateChecksums);
            }
        }
        finally
        {
            foreach (var buffer in buffers)
                buffer.Dispose();
        }
    }
}
